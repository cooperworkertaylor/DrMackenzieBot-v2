import { describe, expect, it } from "vitest";
import type { Table } from "@lancedb/lancedb";
import { buildQueryRewrites, hybridSearch } from "./retrieval.js";

function mockQuery(rows: Record<string, unknown>[]) {
  return {
    where: () => mockQuery(rows),
    withRowId: () => mockQuery(rows),
    limit: () => mockQuery(rows),
    toArray: async () => rows,
  };
}

describe("memory-lancedb retrieval", () => {
  it("builds 3-5 rewrite variants", () => {
    const rewrites = buildQueryRewrites("NVDA revenue growth guidance 2025");
    expect(rewrites.length).toBeGreaterThanOrEqual(3);
    expect(rewrites.length).toBeLessThanOrEqual(5);
    expect(new Set(rewrites).size).toBe(rewrites.length);
  });

  it("deduplicates and enforces retrieval budget", async () => {
    const vectorRows: Record<string, unknown>[] = [
      {
        chunk_id: "a1",
        text: "NVIDIA revenue grew fast in 2025",
        _distance: 0.1,
        source_url: "https://a",
        chunk_hash: "h1",
        ticker: "NVDA",
        doc_type: "memo",
        page: 1,
        char_start: 0,
        char_end: 20,
      },
      {
        chunk_id: "a2",
        text: "NVIDIA revenue grew fast in 2025",
        _distance: 0.2,
        source_url: "https://a",
        chunk_hash: "h1",
        ticker: "NVDA",
        doc_type: "memo",
        page: 1,
        char_start: 0,
        char_end: 20,
      },
    ];
    const ftsRows: Record<string, unknown>[] = [
      {
        chunk_id: "b1",
        text: "Gross margin guidance stayed near 75 percent for NVIDIA",
        _score: 9.5,
        source_url: "https://b",
        chunk_hash: "h2",
        ticker: "NVDA",
        doc_type: "transcript",
        page: 3,
        char_start: 12,
        char_end: 55,
      },
    ];

    const table = {
      vectorSearch: () => mockQuery(vectorRows),
      search: () => mockQuery(ftsRows),
    } as unknown as Table;

    const snippets = await hybridSearch({
      query: "NVDA margin",
      table,
      embeddings: { embed: async () => [0.1, 0.2, 0.3] },
      budget: { maxResults: 2, maxTokensPerSnippet: 6 },
      vectorLimit: 10,
      ftsLimit: 10,
      rewriteCount: 3,
      rrfK: 60,
    });

    expect(snippets.length).toBe(2);
    expect(snippets[0]?.citation.key.length).toBeGreaterThan(0);
    expect(snippets.some((row) => row.chunkId === "a2")).toBe(false);
    expect(snippets.every((row) => row.text.split(/\s+/).length <= 7)).toBe(true);
  });
});

