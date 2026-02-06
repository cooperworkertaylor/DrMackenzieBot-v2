import fs from "node:fs";
import path from "node:path";
import { loadSqliteVecExtension } from "../memory/sqlite-vec.js";
import { openResearchDb, type ResearchDb } from "./db.js";

const DIMS = 256;
const VECTOR_LIMIT_SCAN = 8000;

type SearchHit = {
  table: "chunks" | "repo_chunks";
  id: number;
  score: number;
  text: string;
  meta?: string;
};

const toBlob = (vec: number[]): Buffer => Buffer.from(new Float32Array(vec).buffer);

const l2Normalize = (vec: number[]): number[] => {
  const mag = Math.sqrt(vec.reduce((s, n) => s + n * n, 0));
  if (!Number.isFinite(mag) || mag <= 1e-9) return vec.map(() => 0);
  return vec.map((v) => v / mag);
};

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

export const embedText = (text: string, dims = DIMS): number[] => {
  const out = Array.from({ length: dims }, () => 0);
  const tokens = text
    .toLowerCase()
    .split(/[^a-z0-9_]+/)
    .filter(Boolean);
  for (const token of tokens) {
    let h = 2166136261;
    for (let i = 0; i < token.length; i += 1) {
      h ^= token.charCodeAt(i);
      h += (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24);
    }
    const idx = Math.abs(h) % dims;
    out[idx] += 1;
  }
  return l2Normalize(out);
};

const tryLoadVecExtension = async (db: ResearchDb): Promise<boolean> => {
  const loaded = await loadSqliteVecExtension({ db });
  return loaded.ok;
};

const ensureVecTables = (db: ResearchDb) => {
  db.exec(
    `CREATE VIRTUAL TABLE IF NOT EXISTS chunks_vec USING vec0(id INTEGER PRIMARY KEY, embedding FLOAT[${DIMS}])`,
  );
  db.exec(
    `CREATE VIRTUAL TABLE IF NOT EXISTS repo_chunks_vec USING vec0(id INTEGER PRIMARY KEY, embedding FLOAT[${DIMS}])`,
  );
};

const syncTable = async (params: {
  db: ResearchDb;
  table: "chunks" | "repo_chunks";
  vecTable: "chunks_vec" | "repo_chunks_vec";
  limit?: number;
  useVec: boolean;
}) => {
  const { db, table, vecTable, useVec } = params;
  const limit = params.limit ?? 2000;
  const rows = db
    .prepare(`SELECT id, text FROM ${table} WHERE pending_embedding = 1 ORDER BY id ASC LIMIT ?`)
    .all(limit) as Array<{ id: number; text: string }>;
  if (!rows.length) return 0;

  const upsertFallback = db.prepare(
    `INSERT INTO research_vectors (source_table, row_id, dims, embedding, updated_at)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(source_table, row_id) DO UPDATE SET
       dims=excluded.dims,
       embedding=excluded.embedding,
       updated_at=excluded.updated_at`,
  );
  const mark = db.prepare(`UPDATE ${table} SET pending_embedding = 0 WHERE id = ?`);
  const upsertVec = useVec
    ? db.prepare(`INSERT OR REPLACE INTO ${vecTable}(id, embedding) VALUES(?, ?)`)
    : null;

  db.exec("BEGIN");
  try {
    for (const row of rows) {
      const vec = embedText(row.text);
      upsertFallback.run(table, row.id, DIMS, JSON.stringify(vec), Date.now());
      if (upsertVec) {
        try {
          upsertVec.run(row.id, toBlob(vec));
        } catch {
          // vec path is optional; fallback store remains source of truth.
        }
      }
      mark.run(row.id);
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
  const useVec = await tryLoadVecExtension(db);
  if (useVec) {
    ensureVecTables(db);
  }
  const chunkCount = await syncTable({
    db,
    table: "chunks",
    vecTable: "chunks_vec",
    useVec,
  });
  const repoCount = await syncTable({
    db,
    table: "repo_chunks",
    vecTable: "repo_chunks_vec",
    useVec,
  });
  return { chunkCount, repoCount, useVec };
};

const searchFallback = (params: {
  db: ResearchDb;
  queryVec: number[];
  limit: number;
  ticker?: string;
  source: "research" | "code";
}): SearchHit[] => {
  const { db, queryVec, limit, ticker, source } = params;
  const rows =
    source === "code"
      ? (db
          .prepare(
            `SELECT v.row_id AS id, v.embedding AS embedding, r.text AS text
             FROM research_vectors v
             JOIN repo_chunks r ON r.id = v.row_id
             WHERE v.source_table='repo_chunks'
             LIMIT ?`,
          )
          .all(VECTOR_LIMIT_SCAN) as Array<{ id: number; embedding: string; text: string }>)
      : (db
          .prepare(
            `SELECT v.row_id AS id, v.embedding AS embedding, c.text AS text, c.metadata AS metadata
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
                )`
                   : ""
               }
             LIMIT ?`,
          )
          .all(...(ticker ? [ticker, ticker, VECTOR_LIMIT_SCAN] : [VECTOR_LIMIT_SCAN])) as Array<{
          id: number;
          embedding: string;
          text: string;
          metadata?: string;
        }>);

  const scored = rows
    .map((row) => {
      const emb = JSON.parse(row.embedding) as number[];
      return {
        table: source === "code" ? ("repo_chunks" as const) : ("chunks" as const),
        id: row.id,
        score: cosine(queryVec, emb),
        text: row.text,
        meta: "metadata" in row && typeof row.metadata === "string" ? row.metadata : undefined,
      };
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
  return scored;
};

export const searchResearch = async (params: {
  query: string;
  limit?: number;
  ticker?: string;
  dbPath?: string;
  source?: "research" | "code";
}) => {
  const db = openResearchDb(params.dbPath);
  const limit = Math.max(1, params.limit ?? 8);
  const source = params.source ?? "research";
  const queryVec = embedText(params.query);
  return searchFallback({
    db,
    queryVec,
    limit,
    ticker: params.ticker?.trim().toUpperCase(),
    source,
  });
};

export const writeBackup = (params: { dbPath?: string; destDir: string }) => {
  const dbPath = path.resolve(params.dbPath ?? path.join(process.cwd(), "data", "research.db"));
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const out = path.resolve(params.destDir, `research-${ts}.db`);
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.copyFileSync(dbPath, out);
  return out;
};
