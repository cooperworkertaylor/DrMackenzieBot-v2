import { describe, expect, it } from "vitest";
import type { Table } from "@lancedb/lancedb";
import { hybridSearch, retrieveHybridAcrossTables } from "./retrieval.js";
import { planQuery } from "./query-planner.js";

function mockQuery(rows: Record<string, unknown>[]) {
  return {
    where: () => mockQuery(rows),
    postfilter: () => mockQuery(rows),
    withRowId: () => mockQuery(rows),
    column: () => mockQuery(rows),
    fullTextSearch: () => mockQuery(rows),
    rerank: () => mockQuery(rows),
    limit: () => mockQuery(rows),
    toArray: async () => rows,
  };
}

describe("memory-lancedb retrieval", () => {
  it("builds query plan with rewrites and keeps inferred ticker loose", () => {
    const plan = planQuery("NVDA revenue growth guidance 2025");
    expect(plan.rewrites.length).toBeGreaterThanOrEqual(3);
    expect(plan.rewrites.length).toBeLessThanOrEqual(5);
    expect(plan.ticker).toBe("NVDA");
    expect(plan.strictFilters.tickers).toBeUndefined();
    expect(plan.looseFilters.tickers).toEqual(["NVDA"]);
  });

  it("deduplicates and enforces retrieval budget", async () => {
    const vectorRows: Record<string, unknown>[] = [
      {
        chunk_id: "a1",
        doc_id: "doc:a",
        text: "NVIDIA revenue grew fast in 2025",
        _distance: 0.1,
        source_url: "https://a",
        text_norm_hash: "h1",
        ticker: "NVDA",
        doc_type: "memo",
        source_title: "A",
        page: 1,
        char_start: 0,
        char_end: 20,
        section: "overview",
        published_at: 1,
      },
      {
        chunk_id: "a2",
        doc_id: "doc:a",
        text: "NVIDIA revenue grew fast in 2025",
        _distance: 0.2,
        source_url: "https://a",
        text_norm_hash: "h1",
        ticker: "NVDA",
        doc_type: "memo",
        source_title: "A",
        page: 1,
        char_start: 0,
        char_end: 20,
        section: "overview",
        published_at: 1,
      },
    ];
    const ftsRows: Record<string, unknown>[] = [
      {
        chunk_id: "b1",
        doc_id: "doc:b",
        text: "Gross margin guidance stayed near 75 percent for NVIDIA",
        _score: 9.5,
        source_url: "https://b",
        text_norm_hash: "h2",
        ticker: "NVDA",
        doc_type: "transcript",
        source_title: "B",
        page: 3,
        char_start: 12,
        char_end: 55,
        section: "guidance",
        published_at: 2,
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
      budget: { maxResults: 2, maxTokensPerSnippet: 6, maxTotalTokens: 20 },
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

  it("prefers fact cards and falls back to raw chunks", async () => {
    const factRows: Record<string, unknown>[] = [
      {
        card_id: "c1",
        doc_id: "doc:1",
        chunk_id: "chunk:1",
        claim: "NVDA datacenter revenue grew 112% YoY.",
        evidence_text: "- Data center revenue grew 112% year over year.",
        source_url: "https://example.com/nvda",
        source_title: "NVDA call",
        ticker: "NVDA",
        doc_type: "transcript",
        section: "datacenter",
        page: 1,
        published_at: 1,
        text_norm_hash: "f1",
        citations: [{ doc_id: "doc:1", chunk_id: "chunk:1", url: "https://example.com/nvda" }],
      },
    ];
    const rawRows: Record<string, unknown>[] = [
      {
        chunk_id: "chunk:2",
        doc_id: "doc:2",
        text: "Raw fallback text",
        source_url: "https://example.com/raw",
        source_title: "Raw",
        ticker: "NVDA",
        doc_type: "memo",
        section: "body",
        page: 1,
        published_at: 1,
        text_norm_hash: "r1",
      },
    ];
    const factTable = { vectorSearch: () => mockQuery(factRows), search: () => mockQuery(factRows) } as unknown as Table;
    const rawTable = { vectorSearch: () => mockQuery(rawRows), search: () => mockQuery(rawRows) } as unknown as Table;

    const results = await retrieveHybridAcrossTables({
      query: "NVDA revenue growth",
      factCardsTable: factTable,
      rawChunksTable: rawTable,
      embeddings: { embed: async () => [0.1, 0.2, 0.3] },
      budget: { maxResults: 3, maxTokensPerSnippet: 50, maxTotalTokens: 100 },
      vectorLimit: 5,
      ftsLimit: 5,
      rewriteCount: 3,
      rrfK: 60,
    });

    expect(results.length).toBeGreaterThan(0);
    expect(results[0]?.type).toBe("fact_card");
    expect(results[0]?.citations.length).toBeGreaterThan(0);
  });
});
