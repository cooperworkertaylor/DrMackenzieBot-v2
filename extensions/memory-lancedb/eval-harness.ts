import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import * as lancedb from "@lancedb/lancedb";
import { Index } from "@lancedb/lancedb";
import benchmark from "./eval/benchmark.retrieval.json" with { type: "json" };
import { chunkDocument } from "./ingestion.js";
import { hybridSearch, type EmbeddingClient } from "./retrieval.js";
import type { MemoryChunkRow } from "./types.js";

type BenchmarkDoc = {
  source_id: string;
  source_url: string;
  ticker: string;
  company: string;
  doc_type: MemoryChunkRow["doc_type"];
  published_at: string;
  section: string;
  text: string;
};

type BenchmarkCheck = {
  id: string;
  query: string;
  expected_source_ids: string[];
};

type BenchmarkFile = {
  corpus: BenchmarkDoc[];
  checks: BenchmarkCheck[];
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

async function ensureIndices(table: lancedb.Table): Promise<void> {
  const specs: Array<{ column: string; options?: Partial<lancedb.IndexOptions> }> = [
    {
      column: "vector",
      options: {
        config: Index.ivfFlat({ distanceType: "cosine", numPartitions: 16 }),
        replace: false,
        name: "idx_eval_vector",
      },
    },
    {
      column: "text",
      options: {
        config: Index.fts({
          baseTokenizer: "simple",
          lowercase: true,
          withPosition: true,
        }),
        replace: false,
        name: "idx_eval_text_fts",
      },
    },
    { column: "ticker", options: { config: Index.btree(), replace: false } },
    { column: "doc_type", options: { config: Index.bitmap(), replace: false } },
    { column: "published_at", options: { config: Index.btree(), replace: false } },
    { column: "source_url", options: { config: Index.btree(), replace: false } },
  ];

  for (const spec of specs) {
    try {
      await table.createIndex(spec.column, spec.options);
    } catch {
      // best-effort for local eval environments
    }
  }
}

async function buildEvalTable(corpus: BenchmarkDoc[]): Promise<{
  table: lancedb.Table;
  cleanup: () => Promise<void>;
}> {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "lancedb-retrieval-eval-"));
  const conn = await lancedb.connect(tempDir);
  const table = await conn.createTable("memories_v2", [
    {
      chunk_id: "__schema__",
      doc_id: "__schema__",
      text: "",
      text_norm: "",
      vector: Array.from({ length: 256 }).fill(0),
      importance: 0,
      category: "other",
      company: "",
      ticker: "",
      doc_type: "other",
      published_at: 0,
      ingested_at: 0,
      source_url: "",
      section: "",
      page: 0,
      chunk_index: 0,
      chunk_hash: "",
      char_start: 0,
      char_end: 0,
      token_count: 0,
      citation_key: "",
    },
  ]);
  await table.delete('chunk_id = "__schema__"');
  await ensureIndices(table);

  const embeddings = new HashEmbeddings(256);
  const now = Date.now();
  const batch: MemoryChunkRow[] = [];
  for (const doc of corpus) {
    const publishedAt = Date.parse(`${doc.published_at}T00:00:00Z`);
    const seedChunks = chunkDocument(
      {
        doc_id: doc.source_id,
        text: doc.text,
        importance: 0.7,
        category: "fact",
        company: doc.company,
        ticker: doc.ticker,
        doc_type: doc.doc_type,
        published_at: Number.isFinite(publishedAt) ? publishedAt : now,
        source_url: doc.source_url,
        section: doc.section,
        page: 1,
        chunk_index: 0,
        char_start: 0,
      },
      { targetTokens: 700, minTokens: 500, maxTokens: 900, overlapRatio: 0.12 },
    );

    for (let i = 0; i < seedChunks.length; i++) {
      const chunk = seedChunks[i];
      chunk.chunk_id = `${doc.source_id}:chunk:${i}`;
      chunk.vector = await embeddings.embed(chunk.text);
      chunk.ingested_at = now;
      batch.push(chunk);
    }
  }
  if (batch.length > 0) {
    await table.add(batch);
  }

  return {
    table,
    cleanup: async () => {
      conn.close();
      await fs.rm(tempDir, { recursive: true, force: true });
    },
  };
}

async function run(): Promise<void> {
  const args = process.argv.slice(2);
  const kArg = args.find((arg) => arg.startsWith("--k="));
  const failRecallArg = args.find((arg) => arg.startsWith("--fail-recall="));
  const failMrrArg = args.find((arg) => arg.startsWith("--fail-mrr="));
  const k = kArg ? Math.max(1, Number(kArg.split("=")[1])) : 8;
  const failRecall = failRecallArg ? Number(failRecallArg.split("=")[1]) : null;
  const failMrr = failMrrArg ? Number(failMrrArg.split("=")[1]) : null;

  const suite = benchmark as BenchmarkFile;
  const sourceLookup = new Map(suite.corpus.map((doc) => [doc.source_id, doc.source_url]));
  const embeddings = new HashEmbeddings(256);
  const { table, cleanup } = await buildEvalTable(suite.corpus);

  let passed = 0;
  let mrrTotal = 0;
  let citationTotal = 0;
  let citationGood = 0;
  const latencyMs: number[] = [];

  try {
    for (const check of suite.checks) {
      const expectedUrls = check.expected_source_ids
        .map((sourceId) => sourceLookup.get(sourceId))
        .filter((value): value is string => Boolean(value));

      const t0 = performance.now();
      const results = await hybridSearch({
        query: check.query,
        table,
        embeddings,
        budget: { maxResults: k, maxTokensPerSnippet: 220 },
        vectorLimit: Math.max(20, k * 4),
        ftsLimit: Math.max(20, k * 4),
        rewriteCount: 5,
        rrfK: 60,
      });
      const elapsed = performance.now() - t0;
      latencyMs.push(elapsed);

      citationTotal += results.length;
      citationGood += results.filter((row) => row.citation.key && row.sourceUrl).length;

      const rank = results.findIndex((result) => expectedUrls.includes(result.sourceUrl));
      if (rank >= 0 && rank < k) {
        passed++;
        mrrTotal += 1 / (rank + 1);
      }
    }
  } finally {
    await cleanup();
  }

  const total = suite.checks.length;
  const recallAtK = total > 0 ? passed / total : 0;
  const mrr = total > 0 ? mrrTotal / total : 0;
  const citationCoverageRate = citationTotal > 0 ? citationGood / citationTotal : 1;
  const avgLatencyMs =
    latencyMs.length > 0 ? latencyMs.reduce((a, b) => a + b, 0) / latencyMs.length : 0;

  const report = {
    total,
    k,
    passed,
    recallAtK,
    mrr,
    citationCoverageRate,
    latencyMs: {
      avg: avgLatencyMs,
      p50: percentile(latencyMs, 0.5),
      p95: percentile(latencyMs, 0.95),
    },
  };

  console.log(JSON.stringify(report, null, 2));

  if (failRecall !== null && recallAtK < failRecall) {
    process.exitCode = 1;
  }
  if (failMrr !== null && mrr < failMrr) {
    process.exitCode = 1;
  }
}

run().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

