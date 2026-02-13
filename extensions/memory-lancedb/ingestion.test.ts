import { describe, expect, it } from "vitest";
import { chunkDocument, removeBoilerplate } from "./ingestion.js";

describe("memory-lancedb ingestion", () => {
  it("removes common boilerplate lines", () => {
    const cleaned = removeBoilerplate(`
      Accept all cookies
      Main analysis starts here
      Privacy policy
      Main analysis starts here
      Revenue growth improved
    `);
    expect(cleaned).toContain("Main analysis starts here");
    expect(cleaned).toContain("Revenue growth improved");
    expect(cleaned.toLowerCase()).not.toContain("cookies");
    expect(cleaned.toLowerCase()).not.toContain("privacy policy");
  });

  it("chunks long text with overlap and token bounds", () => {
    const longText = Array.from({ length: 1800 })
      .map((_, idx) => `token${idx}`)
      .join(" ");
    const chunks = chunkDocument(
      {
        doc_id: "doc:test",
        text: longText,
        importance: 0.7,
        category: "fact",
        company: "Test Co",
        ticker: "TST",
        doc_type: "memo",
        published_at: Date.now(),
        source_url: "https://example.com/doc",
        section: "Overview",
        page: 1,
        chunk_index: 0,
        char_start: 0,
      },
      {
        targetTokens: 700,
        minTokens: 500,
        maxTokens: 900,
        overlapRatio: 0.12,
      },
    );

    expect(chunks.length).toBeGreaterThan(1);
    expect(chunks.every((chunk) => chunk.token_count > 0)).toBe(true);
    expect(new Set(chunks.map((chunk) => chunk.chunk_hash)).size).toBe(chunks.length);
  });
});

