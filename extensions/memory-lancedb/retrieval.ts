import type { Table } from "@lancedb/lancedb";
import type { RetrievedSnippet, RetrievalBudget, RetrievalFilters } from "./types.js";

export type EmbeddingClient = {
  embed(text: string): Promise<number[]>;
};

export type HybridSearchOptions = {
  query: string;
  table: Table;
  embeddings: EmbeddingClient;
  filters?: RetrievalFilters;
  budget: RetrievalBudget;
  vectorLimit?: number;
  ftsLimit?: number;
  rrfK?: number;
  rewriteCount?: number;
};

type RankedRow = {
  id: string;
  rank: number;
  score: number;
  row: Record<string, unknown>;
};

const STOP_WORDS = new Set([
  "the",
  "a",
  "an",
  "of",
  "for",
  "and",
  "to",
  "in",
  "on",
  "with",
  "vs",
  "is",
  "are",
  "was",
  "were",
  "from",
  "by",
  "at",
  "as",
]);

const METRIC_SYNONYMS: Record<string, string[]> = {
  revenue: ["sales", "top line"],
  margin: ["profitability", "gross margin", "operating margin"],
  growth: ["cagr", "yoy", "year over year"],
  guidance: ["outlook", "forecast"],
  capex: ["capital expenditure"],
  freecashflow: ["fcf", "free cash flow"],
};

function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .split(/\s+/)
    .filter(Boolean);
}

function normalizeForDuplicateCheck(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildBigrams(text: string): Set<string> {
  const out = new Set<string>();
  for (let i = 0; i < text.length - 1; i++) {
    out.add(text.slice(i, i + 2));
  }
  return out;
}

function nearDuplicate(a: string, b: string, threshold = 0.9): boolean {
  const na = normalizeForDuplicateCheck(a);
  const nb = normalizeForDuplicateCheck(b);
  if (!na || !nb) {
    return false;
  }
  if (na === nb) {
    return true;
  }
  if (na.length > 40 && (na.includes(nb) || nb.includes(na))) {
    return true;
  }
  const ba = buildBigrams(na);
  const bb = buildBigrams(nb);
  if (ba.size === 0 || bb.size === 0) {
    return false;
  }
  let intersect = 0;
  for (const gram of ba) {
    if (bb.has(gram)) {
      intersect++;
    }
  }
  const union = ba.size + bb.size - intersect;
  return union > 0 ? intersect / union >= threshold : false;
}

function truncateToApproxTokens(text: string, maxTokens: number): string {
  if (maxTokens <= 0) {
    return "";
  }
  const words = text.trim().split(/\s+/).filter(Boolean);
  if (words.length <= maxTokens) {
    return words.join(" ");
  }
  return `${words.slice(0, maxTokens).join(" ")} …`;
}

function pickRowId(row: Record<string, unknown>): string {
  const chunkId =
    typeof row.chunk_id === "string" && row.chunk_id.trim() ? row.chunk_id.trim() : undefined;
  if (chunkId) {
    return chunkId;
  }
  const rowId =
    typeof row._rowid === "number"
      ? `rowid:${row._rowid}`
      : typeof row.id === "string"
        ? row.id
        : "";
  return rowId || `row:${Math.random().toString(36).slice(2)}`;
}

function quote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function buildFilterSql(filters?: RetrievalFilters): string | null {
  if (!filters) {
    return null;
  }
  const clauses: string[] = [];
  if (filters.tickers && filters.tickers.length > 0) {
    const tickers = filters.tickers.map((value) => quote(value.toUpperCase()));
    clauses.push(`ticker IN (${tickers.join(",")})`);
  }
  if (filters.docTypes && filters.docTypes.length > 0) {
    const docTypes = filters.docTypes.map((value) => quote(value.toLowerCase()));
    clauses.push(`doc_type IN (${docTypes.join(",")})`);
  }
  if (filters.sourceUrls && filters.sourceUrls.length > 0) {
    const sourceUrls = filters.sourceUrls.map((value) => quote(value));
    clauses.push(`source_url IN (${sourceUrls.join(",")})`);
  }
  if (typeof filters.publishedAtMin === "number") {
    clauses.push(`published_at >= ${Math.floor(filters.publishedAtMin)}`);
  }
  if (typeof filters.publishedAtMax === "number") {
    clauses.push(`published_at <= ${Math.floor(filters.publishedAtMax)}`);
  }
  return clauses.length > 0 ? clauses.join(" AND ") : null;
}

function shouldUsePrefilter(filters?: RetrievalFilters): boolean {
  if (!filters) {
    return false;
  }
  return Boolean(
    (filters.tickers && filters.tickers.length > 0) ||
      (filters.docTypes && filters.docTypes.length > 0) ||
      (filters.sourceUrls && filters.sourceUrls.length > 0),
  );
}

function rowMatchesFilters(row: Record<string, unknown>, filters?: RetrievalFilters): boolean {
  if (!filters) {
    return true;
  }
  if (filters.tickers && filters.tickers.length > 0) {
    const ticker = String(row.ticker ?? "").toUpperCase();
    if (!filters.tickers.some((value) => value.toUpperCase() === ticker)) {
      return false;
    }
  }
  if (filters.docTypes && filters.docTypes.length > 0) {
    const docType = String(row.doc_type ?? "").toLowerCase();
    if (!filters.docTypes.some((value) => value.toLowerCase() === docType)) {
      return false;
    }
  }
  if (filters.sourceUrls && filters.sourceUrls.length > 0) {
    const sourceUrl = String(row.source_url ?? "");
    if (!filters.sourceUrls.includes(sourceUrl)) {
      return false;
    }
  }
  const publishedAt = Number(row.published_at ?? 0);
  if (typeof filters.publishedAtMin === "number" && publishedAt < filters.publishedAtMin) {
    return false;
  }
  if (typeof filters.publishedAtMax === "number" && publishedAt > filters.publishedAtMax) {
    return false;
  }
  return true;
}

export function buildQueryRewrites(query: string, rewriteCount = 5): string[] {
  const base = query.trim();
  if (!base) {
    return [];
  }
  const variants = new Set<string>([base]);
  const tokens = tokenize(base).filter((token) => !STOP_WORDS.has(token));
  if (tokens.length > 0) {
    variants.add(tokens.join(" "));
  }

  const upperTicker = base.match(/\b[A-Z]{1,5}\b/g) ?? [];
  if (upperTicker.length > 0) {
    variants.add(`${upperTicker.join(" ")} ${tokens.join(" ")}`.trim());
  }

  for (const token of tokens) {
    const synonyms = METRIC_SYNONYMS[token.replace(/\s+/g, "")];
    if (!synonyms) {
      continue;
    }
    variants.add(`${base} ${synonyms.join(" ")}`.trim());
  }

  const yearMatches = base.match(/\b20\d{2}\b/g) ?? [];
  if (yearMatches.length > 0) {
    variants.add(`${tokens.join(" ")} ${yearMatches.join(" ")}`.trim());
  }

  return Array.from(variants).filter(Boolean).slice(0, Math.max(3, Math.min(5, rewriteCount)));
}

function rankRows(
  rows: Record<string, unknown>[],
  scoreField: "_distance" | "_score",
  desc = false,
): RankedRow[] {
  const sorted = [...rows].sort((a, b) => {
    const av = Number(a[scoreField] ?? 0);
    const bv = Number(b[scoreField] ?? 0);
    if (!Number.isFinite(av) && !Number.isFinite(bv)) {
      return 0;
    }
    if (!Number.isFinite(av)) {
      return 1;
    }
    if (!Number.isFinite(bv)) {
      return -1;
    }
    return desc ? bv - av : av - bv;
  });

  return sorted.map((row, idx) => ({
    id: pickRowId(row),
    rank: idx + 1,
    score: Number(row[scoreField] ?? 0),
    row,
  }));
}

function applyRrf(
  rankedGroups: RankedRow[][],
  rrfK: number,
): Array<{ id: string; score: number; row: Record<string, unknown> }> {
  const acc = new Map<string, { score: number; row: Record<string, unknown> }>();
  for (const ranked of rankedGroups) {
    for (const item of ranked) {
      const current = acc.get(item.id) ?? { score: 0, row: item.row };
      current.score += 1 / (rrfK + item.rank);
      current.row = item.row;
      acc.set(item.id, current);
    }
  }
  return Array.from(acc, ([id, value]) => ({ id, score: value.score, row: value.row })).sort(
    (a, b) => b.score - a.score,
  );
}

export async function hybridSearch(options: HybridSearchOptions): Promise<RetrievedSnippet[]> {
  const {
    query,
    table,
    embeddings,
    filters,
    budget,
    vectorLimit = 40,
    ftsLimit = 40,
    rrfK = 60,
    rewriteCount = 5,
  } = options;

  const rewrites = buildQueryRewrites(query, rewriteCount);
  if (rewrites.length === 0) {
    return [];
  }

  const filterSql = buildFilterSql(filters);
  const prefilter = shouldUsePrefilter(filters) && Boolean(filterSql);
  const rankedGroups: RankedRow[][] = [];

  await Promise.all(
    rewrites.map(async (rewrite) => {
      const vector = await embeddings.embed(rewrite);

      let vectorQuery = table.vectorSearch(vector).withRowId().limit(vectorLimit);
      let ftsQuery = table.search(rewrite, "fts", "text").withRowId().limit(ftsLimit);

      if (prefilter && filterSql) {
        vectorQuery = vectorQuery.where(filterSql);
        ftsQuery = ftsQuery.where(filterSql);
      }

      const [vectorRowsRaw, ftsRowsRaw] = await Promise.all([vectorQuery.toArray(), ftsQuery.toArray()]);
      const vectorRows = vectorRowsRaw as Record<string, unknown>[];
      const ftsRows = ftsRowsRaw as Record<string, unknown>[];

      rankedGroups.push(rankRows(vectorRows, "_distance", false));
      rankedGroups.push(rankRows(ftsRows, "_score", true));
    }),
  );

  const fused = applyRrf(rankedGroups, rrfK);
  const finalRows = prefilter ? fused : fused.filter((item) => rowMatchesFilters(item.row, filters));

  const dedupSourceHash = new Set<string>();
  const chosen: RetrievedSnippet[] = [];

  for (const item of finalRows) {
    const row = item.row;
    const text = String(row.text ?? "").trim();
    if (!text) {
      continue;
    }

    const sourceUrl = String(row.source_url ?? "");
    const chunkHash = String(row.chunk_hash ?? "");
    const sourceHashKey = `${sourceUrl}::${chunkHash}`;
    if (sourceHashKey !== "::" && dedupSourceHash.has(sourceHashKey)) {
      continue;
    }
    if (sourceHashKey !== "::") {
      dedupSourceHash.add(sourceHashKey);
    }

    if (chosen.some((existing) => nearDuplicate(existing.text, text))) {
      continue;
    }

    const chunkId = String(row.chunk_id ?? item.id);
    const page = Number(row.page ?? 0);
    const charStart = Number(row.char_start ?? 0);
    const charEnd = Number(row.char_end ?? 0);
    const citationKey =
      String(row.citation_key ?? "").trim() ||
      `${sourceUrl || "local://memory"}#${chunkId}:p${page}:c${charStart}-${charEnd}`;

    chosen.push({
      chunkId,
      text: truncateToApproxTokens(text, budget.maxTokensPerSnippet),
      score: item.score,
      sourceUrl,
      company: String(row.company ?? ""),
      ticker: String(row.ticker ?? ""),
      docType: String(row.doc_type ?? ""),
      section: String(row.section ?? ""),
      page,
      publishedAt: Number(row.published_at ?? 0),
      chunkHash,
      citation: {
        key: citationKey,
        chunkId,
        sourceUrl,
        page,
        charStart,
        charEnd,
      },
    });

    if (chosen.length >= budget.maxResults) {
      break;
    }
  }

  return chosen;
}

