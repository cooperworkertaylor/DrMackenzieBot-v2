import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import * as lancedb from "@lancedb/lancedb";
import { Index } from "@lancedb/lancedb";
import evalSet from "./eval_set.json" with { type: "json" };
import { generateFactCardsFromChunk } from "../fact-cards.js";
import { chunkDocument } from "../ingestion.js";
import { retrieveHybridAcrossTables, type EmbeddingClient } from "../retrieval.js";
import {
  FACT_CARDS_TABLE,
  RAW_CHUNKS_TABLE,
  buildFactCardsSchema,
  buildRawChunksSchema,
} from "../schema.js";
import type { FactCardRow, RawChunkRow } from "../types.js";

type EvalDoc = {
  doc_id: string;
  source_url: string;
  source_title: string;
  ticker: string;
  company: string;
  doc_type: RawChunkRow["doc_type"];
  published_at: string;
  section: string;
  text: string;
};

type EvalCheck = {
  id: string;
  query: string;
  expected_target_ids: string[];
};

type EvalSuite = {
  corpus: EvalDoc[];
  checks: EvalCheck[];
};

class HashEmbeddings implements EmbeddingClient {
  constructor(private readonly dims = 256) {}

  async embed(text: string): Promise<number[]> {
    const vec = Array.from({ length: this.dims }).fill(0) as number[];
    const tokens = text
      .toLowerCase()
      .replace(/[^\p{L}\p{N}\s]/gu, " ")
      .split(/\s+/)
      .filter(Boolean);
    for (const token of tokens) {
      const idx = this.hashToken(token) % this.dims;
      vec[idx] += 1;
    }
    const norm = Math.sqrt(vec.reduce((sum, value) => sum + value * value, 0)) || 1;
    return vec.map((value) => value / norm);
  }

  private hashToken(token: string): number {
    let h = 2166136261;
    for (let i = 0; i < token.length; i++) {
      h ^= token.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return Math.abs(h >>> 0);
  }
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return sorted[idx];
}

async function ensureIndexes(rawChunks: lancedb.Table, factCards: lancedb.Table): Promise<void> {
  const rawSpecs: Array<{ column: string; options?: Partial<lancedb.IndexOptions> }> = [
    {
      column: "embedding",
      options: {
        config: Index.ivfFlat({ distanceType: "cosine", numPartitions: 16 }),
        replace: false,
        name: "idx_eval_raw_embedding",
      },
    },
    {
      column: "text",
      options: {
        config: Index.fts({ baseTokenizer: "simple", lowercase: true, withPosition: true }),
        replace: false,
        name: "idx_eval_raw_text_fts",
      },
    },
    { column: "ticker", options: { config: Index.btree(), replace: false } },
    { column: "doc_type", options: { config: Index.bitmap(), replace: false } },
    { column: "published_at", options: { config: Index.btree(), replace: false } },
  ];

  const factSpecs: Array<{ column: string; options?: Partial<lancedb.IndexOptions> }> = [
    {
      column: "embedding",
      options: {
        config: Index.ivfFlat({ distanceType: "cosine", numPartitions: 16 }),
        replace: false,
        name: "idx_eval_fact_embedding",
      },
    },
    {
      column: "search_text",
      options: {
        config: Index.fts({ baseTokenizer: "simple", lowercase: true, withPosition: true }),
        replace: false,
        name: "idx_eval_fact_text_fts",
      },
    },
    { column: "ticker", options: { config: Index.btree(), replace: false } },
    { column: "doc_type", options: { config: Index.bitmap(), replace: false } },
    { column: "published_at", options: { config: Index.btree(), replace: false } },
  ];

  for (const spec of rawSpecs) {
    try {
      await rawChunks.createIndex(spec.column, spec.options);
    } catch {
      // idempotent
    }
  }
  for (const spec of factSpecs) {
    try {
      await factCards.createIndex(spec.column, spec.options);
    } catch {
      // idempotent
    }
  }
}

async function buildEvalTables(corpus: EvalDoc[]): Promise<{
  rawChunks: lancedb.Table;
  factCards: lancedb.Table;
  cleanup: () => Promise<void>;
}> {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "lancedb-retrieval-eval-v3-"));
  const conn = await lancedb.connect(tempDir);
  const rawChunks = await conn.createEmptyTable(RAW_CHUNKS_TABLE, buildRawChunksSchema(256));
  const factCards = await conn.createEmptyTable(FACT_CARDS_TABLE, buildFactCardsSchema(256));
  await ensureIndexes(rawChunks, factCards);

  const embeddings = new HashEmbeddings(256);
  const now = Date.now();
  const rawBatch: RawChunkRow[] = [];
  const cardBatch: FactCardRow[] = [];

  for (const doc of corpus) {
    const publishedAt = Date.parse(`${doc.published_at}T00:00:00Z`);
    const chunks = chunkDocument(
      {
        doc_id: doc.doc_id,
        text: doc.text,
        importance: 0.7,
        category: "fact",
        company: doc.company,
        ticker: doc.ticker,
        doc_type: doc.doc_type,
        published_at: Number.isFinite(publishedAt) ? publishedAt : now,
        retrieved_at: now,
        source_url: doc.source_url,
        source_title: doc.source_title,
        section: doc.section,
        page: 1,
        chunk_index: 0,
        char_start: 0,
        tags: [doc.doc_type, doc.ticker],
      },
      { targetTokens: 700, minTokens: 500, maxTokens: 900, overlapRatio: 0.12 },
    );

    for (let i = 0; i < chunks.length; i++) {
      const chunk = chunks[i];
      chunk.chunk_id = `${doc.doc_id}:chunk:${i}`;
      chunk.embedding = await embeddings.embed(chunk.text);
      chunk.vector = chunk.embedding;
      chunk.ingested_at = now;
      rawBatch.push(chunk);

      const cards = generateFactCardsFromChunk(chunk);
      for (let j = 0; j < cards.length; j++) {
        const card = cards[j];
        card.card_id = `${chunk.chunk_id}:card:${j}`;
        card.embedding = await embeddings.embed(card.search_text);
        cardBatch.push(card);
      }
    }
  }

  if (rawBatch.length > 0) {
    await rawChunks.add(rawBatch);
  }
  if (cardBatch.length > 0) {
    await factCards.add(cardBatch);
  }

  return {
    rawChunks,
    factCards,
    cleanup: async () => {
      conn.close();
      await fs.rm(tempDir, { recursive: true, force: true });
    },
  };
}

type EvalMode = "before" | "after";

function expectedHitRank(
  rows: Array<{ id: string; doc_id: string; citations: Array<{ doc_id: string; chunk_id: string }> }>,
  expectedTargets: string[],
): number {
  const expected = new Set(expectedTargets);
  return rows.findIndex((row) => {
    if (expected.has(row.id) || expected.has(row.doc_id)) {
      return true;
    }
    return row.citations.some((citation) => expected.has(citation.doc_id) || expected.has(citation.chunk_id));
  });
}

function compactQueryForLegacyBaseline(query: string): string {
  const tokens = query
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .split(/\s+/)
    .filter(Boolean);
  return tokens.slice(0, 4).join(" ");
}

function naiveLegacyTickerFilter(query: string): string | null {
  const match = query.match(/\b[A-Z]{2,5}\b/);
  return match?.[0] ?? null;
}

async function runMode(
  mode: EvalMode,
  suite: EvalSuite,
  tables: { rawChunks: lancedb.Table; factCards: lancedb.Table },
  embeddings: EmbeddingClient,
  k: number,
): Promise<{
  mode: EvalMode;
  total: number;
  recallAtK: number;
  mrr: number;
  citationCoverageRate: number;
  latency: { avg: number; p50: number; p95: number };
}> {
  let passed = 0;
  let mrrTotal = 0;
  let citationTotal = 0;
  let citationWithPointers = 0;
  const latencies: number[] = [];

  for (const check of suite.checks) {
    const t0 = performance.now();

    let rows: Array<{ id: string; doc_id: string; citations: Array<{ doc_id: string; chunk_id: string }> }>;
    if (mode === "before") {
      const legacyTicker = naiveLegacyTickerFilter(check.query);
      const baselineRows = (await tables.rawChunks
        .vectorSearch(await embeddings.embed(compactQueryForLegacyBaseline(check.query)))
        .column("embedding")
        .withRowId()
        .limit(Math.max(2, Math.floor(k / 2)))
        .toArray()) as Array<Record<string, unknown>>;

      rows = baselineRows
        .filter((row) => (legacyTicker ? String(row.ticker ?? "").toUpperCase() === legacyTicker : true))
        .map((row) => ({
          id: String(row.chunk_id ?? row._rowid ?? ""),
          doc_id: String(row.doc_id ?? ""),
          citations: [
            {
              doc_id: String(row.doc_id ?? ""),
              chunk_id: String(row.chunk_id ?? ""),
            },
          ],
        }));
    } else {
      rows = (
        await retrieveHybridAcrossTables({
          query: check.query,
          rawChunksTable: tables.rawChunks,
          factCardsTable: tables.factCards,
          embeddings,
          budget: { maxResults: k, maxTokensPerSnippet: 120, maxTotalTokens: 800 },
          vectorLimit: Math.max(24, k * 4),
          ftsLimit: Math.max(24, k * 4),
          rewriteCount: 5,
          rrfK: 60,
        })
      ).map((item) => ({
        id: item.id,
        doc_id: item.metadata.doc_id,
        citations: item.citations.map((citation) => ({
          doc_id: citation.doc_id,
          chunk_id: citation.chunk_id,
        })),
      }));
    }

    const elapsed = performance.now() - t0;
    latencies.push(elapsed);

    citationTotal += rows.length;
    citationWithPointers += rows.filter((row) => row.citations.length > 0).length;

    const rank = expectedHitRank(rows, check.expected_target_ids);
    if (rank >= 0 && rank < k) {
      passed += 1;
      mrrTotal += 1 / (rank + 1);
    }
  }

  const total = suite.checks.length;
  return {
    mode,
    total,
    recallAtK: total > 0 ? passed / total : 0,
    mrr: total > 0 ? mrrTotal / total : 0,
    citationCoverageRate: citationTotal > 0 ? citationWithPointers / citationTotal : 1,
    latency: {
      avg: latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0,
      p50: percentile(latencies, 0.5),
      p95: percentile(latencies, 0.95),
    },
  };
}

function printComparison(before: Awaited<ReturnType<typeof runMode>>, after: Awaited<ReturnType<typeof runMode>>, k: number): void {
  const lines = [
    "",
    `Retrieval Eval (k=${k})`,
    "metric                     before         after          delta",
    `recall@${k}                 ${before.recallAtK.toFixed(3)}         ${after.recallAtK.toFixed(3)}         ${(after.recallAtK - before.recallAtK).toFixed(3)}`,
    `mrr                        ${before.mrr.toFixed(3)}         ${after.mrr.toFixed(3)}         ${(after.mrr - before.mrr).toFixed(3)}`,
    `citation_coverage_rate     ${before.citationCoverageRate.toFixed(3)}         ${after.citationCoverageRate.toFixed(3)}         ${(after.citationCoverageRate - before.citationCoverageRate).toFixed(3)}`,
    `latency_avg_ms             ${before.latency.avg.toFixed(1)}          ${after.latency.avg.toFixed(1)}          ${(after.latency.avg - before.latency.avg).toFixed(1)}`,
    `latency_p95_ms             ${before.latency.p95.toFixed(1)}          ${after.latency.p95.toFixed(1)}          ${(after.latency.p95 - before.latency.p95).toFixed(1)}`,
    "",
  ];
  console.log(lines.join("\n"));
}

async function main(): Promise<void> {
  const modeArg = process.argv.find((arg) => arg.startsWith("--mode="))?.split("=")[1] ?? "compare";
  const kArg = process.argv.find((arg) => arg.startsWith("--k="))?.split("=")[1];
  const k = kArg ? Math.max(1, Number(kArg)) : 8;
  const suite = evalSet as EvalSuite;
  const embeddings = new HashEmbeddings(256);

  const tables = await buildEvalTables(suite.corpus);
  try {
    if (modeArg === "before") {
      const before = await runMode("before", suite, tables, embeddings, k);
      console.log(JSON.stringify(before, null, 2));
      return;
    }
    if (modeArg === "after") {
      const after = await runMode("after", suite, tables, embeddings, k);
      console.log(JSON.stringify(after, null, 2));
      return;
    }

    const before = await runMode("before", suite, tables, embeddings, k);
    const after = await runMode("after", suite, tables, embeddings, k);
    printComparison(before, after, k);
    console.log(
      JSON.stringify(
        {
          before,
          after,
          improved:
            after.recallAtK >= before.recallAtK &&
            after.mrr >= before.mrr &&
            after.citationCoverageRate >= before.citationCoverageRate,
        },
        null,
        2,
      ),
    );
  } finally {
    await tables.cleanup();
  }
}

void main();
