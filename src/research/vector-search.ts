import fs from "node:fs";
import path from "node:path";
import { loadSqliteVecExtension } from "../memory/sqlite-vec.js";
import { openResearchDb, type ResearchDb } from "./db.js";
import {
  createHashEmbeddingProvider,
  createResearchEmbeddingProvider,
  type ResearchEmbeddingProvider,
} from "./embeddings-provider.js";

const VECTOR_LIMIT_SCAN = 12_000;
const DEFAULT_CANDIDATE_MULTIPLIER = 24;

type SearchTable = "chunks" | "repo_chunks";

type SearchRow = {
  id: number;
  embedding: string;
  text: string;
  metadata?: string;
  source_table?: string;
  citation_url?: string;
};

export type SearchHit = {
  table: SearchTable;
  id: number;
  score: number;
  vectorScore: number;
  lexicalScore: number;
  sourceQualityScore: number;
  freshnessScore: number;
  text: string;
  meta?: string;
  sourceTable?: string;
  citationUrl?: string;
};

const toBlob = (vec: number[]): Buffer => Buffer.from(new Float32Array(vec).buffer);

const cosine = (a: number[], b: number[]): number => {
  if (a.length !== b.length || a.length === 0) return 0;
  let dot = 0;
  let na = 0;
  let nb = 0;
  for (let i = 0; i < a.length; i += 1) {
    dot += a[i]! * b[i]!;
    na += a[i]! * a[i]!;
    nb += b[i]! * b[i]!;
  }
  if (na <= 1e-9 || nb <= 1e-9) return 0;
  return dot / (Math.sqrt(na) * Math.sqrt(nb));
};

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

export const tokenizeForRank = (text: string): string[] =>
  text
    .toLowerCase()
    .split(/[^a-z0-9_]+/)
    .map((token) => token.trim())
    .filter((token) => token.length >= 2);

export const buildFinancialQueryExpansion = (query: string): string => {
  const lowered = query.toLowerCase();
  const terms = new Set(tokenizeForRank(query));
  const add = (items: string[]) => {
    for (const item of items) terms.add(item);
  };
  if (/\brevenue|sales|topline|turnover\b/.test(lowered)) {
    add(["revenue", "sales", "topline", "turnover"]);
  }
  if (/\bnet income|earnings|profit|eps\b/.test(lowered)) {
    add(["net", "income", "earnings", "profit", "eps"]);
  }
  if (/\bmargin|gross|operating\b/.test(lowered)) {
    add(["margin", "gross", "operating", "profitability"]);
  }
  if (/\bfcf|cash flow|cashflow\b/.test(lowered)) {
    add(["cash", "flow", "free", "fcf", "liquidity"]);
  }
  if (/\brisks?|risk factors?|10-k\b/.test(lowered)) {
    add(["risk", "factors", "10-k", "disclosure"]);
  }
  if (/\bvaluation|multiple|ev|ebitda|p\/e\b/.test(lowered)) {
    add(["valuation", "multiple", "ev", "ebitda", "pe"]);
  }
  return [query, ...Array.from(terms)].join(" ").trim();
};

export const lexicalCoverageScore = (query: string, text: string, meta?: string): number => {
  const queryTokens = new Set(tokenizeForRank(query));
  if (queryTokens.size === 0) return 0;
  const corpusTokens = new Set(tokenizeForRank(`${text} ${meta ?? ""}`));
  let matched = 0;
  for (const token of queryTokens) {
    if (corpusTokens.has(token)) matched += 1;
  }
  const coverage = matched / queryTokens.size;
  const loweredCorpus = `${text} ${meta ?? ""}`.toLowerCase();
  const phraseBonus =
    query.toLowerCase().includes("risk factors") && loweredCorpus.includes("risk factors")
      ? 0.1
      : 0;
  return clamp01(coverage * 0.9 + phraseBonus);
};

const parseMetadata = (raw?: string): Record<string, unknown> | null => {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object") return null;
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
};

const parseDateMs = (value: unknown): number | null => {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  const isoLike = /^\d{4}-\d{2}-\d{2}$/.test(trimmed) ? `${trimmed}T00:00:00.000Z` : trimmed;
  const ms = Date.parse(isoLike);
  return Number.isFinite(ms) ? ms : null;
};

const resolveRecencyDate = (row: SearchRow): number | null => {
  const meta = parseMetadata(row.metadata);
  if (meta) {
    for (const key of [
      "asOfDate",
      "periodEnd",
      "filingDate",
      "filed",
      "event_date",
      "eventDate",
      "acceptedAt",
    ]) {
      const parsed = parseDateMs(meta[key]);
      if (parsed) return parsed;
    }
  }
  const fromText = row.text.match(/\b\d{4}-\d{2}-\d{2}\b/)?.[0];
  return parseDateMs(fromText ?? null);
};

const freshnessScore = (row: SearchRow): number => {
  const timestamp = resolveRecencyDate(row);
  if (!timestamp) return 0.5;
  const ageDays = Math.max(0, (Date.now() - timestamp) / 86_400_000);
  return clamp01(Math.exp(-ageDays / 730));
};

const sourceQualityScore = (row: SearchRow, source: "research" | "code"): number => {
  if (source === "code") return 0.72;
  const table = row.source_table ?? "";
  const base =
    table === "fundamental_facts"
      ? 1
      : table === "filings"
        ? 0.95
        : table === "transcripts"
          ? 0.78
          : table === "earnings_expectations"
            ? 0.85
            : 0.6;
  const citationBonus = row.citation_url ? 0.03 : 0;
  return clamp01(base + citationBonus);
};

export const blendRankSignals = (scores: {
  vector: number;
  lexical: number;
  sourceQuality: number;
  freshness: number;
}): number =>
  clamp01(
    0.66 * clamp01(scores.vector) +
      0.22 * clamp01(scores.lexical) +
      0.08 * clamp01(scores.sourceQuality) +
      0.04 * clamp01(scores.freshness),
  );

const tryLoadVecExtension = async (db: ResearchDb): Promise<boolean> => {
  const loaded = await loadSqliteVecExtension({ db });
  return loaded.ok;
};

const ensureVecTables = (db: ResearchDb, dims: number) => {
  db.exec(
    `CREATE VIRTUAL TABLE IF NOT EXISTS chunks_vec USING vec0(id INTEGER PRIMARY KEY, embedding FLOAT[${dims}])`,
  );
  db.exec(
    `CREATE VIRTUAL TABLE IF NOT EXISTS repo_chunks_vec USING vec0(id INTEGER PRIMARY KEY, embedding FLOAT[${dims}])`,
  );
};

const syncTable = async (params: {
  db: ResearchDb;
  table: "chunks" | "repo_chunks";
  vecTable: "chunks_vec" | "repo_chunks_vec";
  limit?: number;
  useVec: boolean;
  provider: ResearchEmbeddingProvider;
}) => {
  const { db, table, vecTable, useVec, provider } = params;
  const limit = params.limit ?? 2000;
  const rows = db
    .prepare(
      `SELECT t.id, t.text
       FROM ${table} t
       LEFT JOIN research_vectors v
         ON v.source_table = ? AND v.row_id = t.id
       WHERE t.pending_embedding = 1
          OR v.row_id IS NULL
          OR COALESCE(v.provider, '') != ?
          OR COALESCE(v.model, '') != ?
          OR v.dims != ?
       ORDER BY t.id ASC
       LIMIT ?`,
    )
    .all(table, provider.id, provider.model, provider.dims, limit) as Array<{
    id: number;
    text: string;
  }>;
  if (!rows.length) return 0;

  const vectors = await provider.embedBatch(rows.map((row) => `document: ${row.text}`));
  const upsertFallback = db.prepare(
    `INSERT INTO research_vectors (source_table, row_id, dims, provider, model, embedding, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(source_table, row_id) DO UPDATE SET
       dims=excluded.dims,
       provider=excluded.provider,
       model=excluded.model,
       embedding=excluded.embedding,
       updated_at=excluded.updated_at`,
  );
  const mark = db.prepare(`UPDATE ${table} SET pending_embedding = 0 WHERE id = ?`);
  const upsertVec = useVec
    ? db.prepare(`INSERT OR REPLACE INTO ${vecTable}(id, embedding) VALUES(?, ?)`)
    : null;

  db.exec("BEGIN");
  try {
    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i];
      const vec = vectors[i] ?? [];
      upsertFallback.run(
        table,
        row?.id,
        provider.dims,
        provider.id,
        provider.model,
        JSON.stringify(vec),
        Date.now(),
      );
      if (upsertVec) {
        try {
          upsertVec.run(row?.id, toBlob(vec));
        } catch {
          // vec path is optional; fallback store remains source of truth.
        }
      }
      mark.run(row?.id);
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return rows.length;
};

export const syncEmbeddings = async (dbPath?: string) => {
  const db = openResearchDb(dbPath);
  const selection = await createResearchEmbeddingProvider();
  const provider = selection.provider;
  const useVec = await tryLoadVecExtension(db);
  if (useVec) {
    ensureVecTables(db, provider.dims);
  }
  const chunkCount = await syncTable({
    db,
    table: "chunks",
    vecTable: "chunks_vec",
    useVec,
    provider,
  });
  const repoCount = await syncTable({
    db,
    table: "repo_chunks",
    vecTable: "repo_chunks_vec",
    useVec,
    provider,
  });
  return {
    chunkCount,
    repoCount,
    useVec,
    provider: provider.id,
    model: provider.model,
    dims: provider.dims,
    warning: selection.warning,
  };
};

const fetchCandidates = (params: {
  db: ResearchDb;
  ticker?: string;
  source: "research" | "code";
  candidateLimit: number;
}): SearchRow[] => {
  const { db, ticker, source, candidateLimit } = params;
  if (source === "code") {
    return db
      .prepare(
        `SELECT v.row_id AS id, v.embedding AS embedding, r.text AS text,
                NULL AS metadata, 'repo_chunks' AS source_table, NULL AS citation_url
           FROM research_vectors v
           JOIN repo_chunks r ON r.id = v.row_id
          WHERE v.source_table='repo_chunks'
          LIMIT ?`,
      )
      .all(candidateLimit) as SearchRow[];
  }

  const sql = `SELECT
      v.row_id AS id,
      v.embedding AS embedding,
      c.text AS text,
      c.metadata AS metadata,
      c.source_table AS source_table,
      CASE
        WHEN c.source_table='filings' THEN (SELECT url FROM filings WHERE id=c.ref_id)
        WHEN c.source_table='transcripts' THEN (SELECT url FROM transcripts WHERE id=c.ref_id)
        WHEN c.source_table='fundamental_facts' THEN (SELECT source_url FROM fundamental_facts WHERE id=c.ref_id)
        WHEN c.source_table='earnings_expectations' THEN (SELECT source_url FROM earnings_expectations WHERE id=c.ref_id)
        ELSE NULL
      END AS citation_url
     FROM research_vectors v
     JOIN chunks c ON c.id = v.row_id
    WHERE v.source_table='chunks'
      ${
        ticker
          ? `AND (
          (c.source_table='filings' AND c.ref_id IN (
            SELECT f.id FROM filings f JOIN instruments i ON i.id=f.instrument_id WHERE i.ticker=?
          ))
          OR
          (c.source_table='transcripts' AND c.ref_id IN (
            SELECT t.id FROM transcripts t JOIN instruments i ON i.id=t.instrument_id WHERE i.ticker=?
          ))
          OR
          (c.source_table='fundamental_facts' AND c.ref_id IN (
            SELECT ff.id FROM fundamental_facts ff JOIN instruments i ON i.id=ff.instrument_id WHERE i.ticker=?
          ))
          OR
          (c.source_table='earnings_expectations' AND c.ref_id IN (
            SELECT ee.id FROM earnings_expectations ee JOIN instruments i ON i.id=ee.instrument_id WHERE i.ticker=?
          ))
        )`
          : ""
      }
    LIMIT ?`;
  return db
    .prepare(sql)
    .all(
      ...(ticker ? [ticker, ticker, ticker, ticker, candidateLimit] : [candidateLimit]),
    ) as SearchRow[];
};

const scoreCandidates = (params: {
  rows: SearchRow[];
  queryVec: number[];
  query: string;
  source: "research" | "code";
}): SearchHit[] => {
  const { rows, queryVec, query, source } = params;
  const hits: SearchHit[] = [];
  for (const row of rows) {
    let embedding: number[];
    try {
      embedding = JSON.parse(row.embedding) as number[];
    } catch {
      continue;
    }
    if (!Array.isArray(embedding) || embedding.length !== queryVec.length) continue;
    const vectorScore = cosine(queryVec, embedding);
    const lexicalScore = lexicalCoverageScore(query, row.text, row.metadata);
    const sourceScore = sourceQualityScore(row, source);
    const recencyScore = freshnessScore(row);
    const score = blendRankSignals({
      vector: vectorScore,
      lexical: lexicalScore,
      sourceQuality: sourceScore,
      freshness: recencyScore,
    });
    hits.push({
      table: source === "code" ? "repo_chunks" : "chunks",
      id: row.id,
      score,
      vectorScore,
      lexicalScore,
      sourceQualityScore: sourceScore,
      freshnessScore: recencyScore,
      text: row.text,
      meta: row.metadata,
      sourceTable: row.source_table,
      citationUrl: row.citation_url,
    });
  }
  return hits.toSorted((a, b) => b.score - a.score);
};

const searchWithProvider = async (params: {
  db: ResearchDb;
  provider: ResearchEmbeddingProvider;
  query: string;
  ticker?: string;
  source: "research" | "code";
  limit: number;
  candidateMultiplier: number;
}): Promise<SearchHit[]> => {
  const expandedQuery = buildFinancialQueryExpansion(params.query);
  const queryVec = await params.provider.embedQuery(`query: ${expandedQuery}`);
  const candidateLimit = Math.max(
    200,
    Math.min(VECTOR_LIMIT_SCAN, Math.floor(params.limit * params.candidateMultiplier)),
  );
  const rows = fetchCandidates({
    db: params.db,
    ticker: params.ticker,
    source: params.source,
    candidateLimit,
  });
  return scoreCandidates({
    rows,
    queryVec,
    query: expandedQuery,
    source: params.source,
  }).slice(0, params.limit);
};

export const searchResearch = async (params: {
  query: string;
  limit?: number;
  ticker?: string;
  dbPath?: string;
  source?: "research" | "code";
  candidateMultiplier?: number;
}) => {
  const db = openResearchDb(params.dbPath);
  const limit = Math.max(1, params.limit ?? 8);
  const source = params.source ?? "research";
  const candidateMultiplier = Math.max(
    4,
    params.candidateMultiplier ?? DEFAULT_CANDIDATE_MULTIPLIER,
  );
  const ticker = params.ticker?.trim().toUpperCase();

  const selection = await createResearchEmbeddingProvider();
  let hits = await searchWithProvider({
    db,
    provider: selection.provider,
    query: params.query,
    ticker,
    source,
    limit,
    candidateMultiplier,
  });

  if (!hits.length && selection.provider.id !== "hash") {
    const hashProvider = createHashEmbeddingProvider();
    hits = await searchWithProvider({
      db,
      provider: hashProvider,
      query: params.query,
      ticker,
      source,
      limit,
      candidateMultiplier,
    });
  }
  return hits;
};

export const writeBackup = (params: { dbPath?: string; destDir: string }) => {
  const dbPath = path.resolve(params.dbPath ?? path.join(process.cwd(), "data", "research.db"));
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const out = path.resolve(params.destDir, `research-${ts}.db`);
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.copyFileSync(dbPath, out);
  return out;
};

export const __testOnly = {
  buildFinancialQueryExpansion,
  lexicalCoverageScore,
  tokenizeForRank,
  blendRankSignals,
};
