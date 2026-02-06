import { describe, expect, it } from "vitest";
import { __testOnly } from "./vector-search.js";

describe("research vector search ranking helpers", () => {
  it("expands finance queries with related terms", () => {
    const expanded = __testOnly
      .buildFinancialQueryExpansion("AAPL revenue growth and risk factors")
      .toLowerCase();
    expect(expanded).toContain("sales");
    expect(expanded).toContain("topline");
    expect(expanded).toContain("risk");
    expect(expanded).toContain("disclosure");
  });

  it("assigns higher lexical coverage to relevant text", () => {
    const query = "AAPL free cash flow trend";
    const relevant = __testOnly.lexicalCoverageScore(
      query,
      "Apple free cash flow improved year over year.",
      '{"label":"Cash flow statement"}',
    );
    const irrelevant = __testOnly.lexicalCoverageScore(
      query,
      "Weather outlook and vacation itinerary.",
      '{"label":"Travel"}',
    );
    expect(relevant).toBeGreaterThan(irrelevant);
    expect(relevant).toBeGreaterThan(0.3);
    expect(irrelevant).toBeLessThan(0.3);
  });

  it("blends vector and lexical/source signals into bounded score", () => {
    const strong = __testOnly.blendRankSignals({
      vector: 0.9,
      lexical: 0.8,
      sourceQuality: 1,
      freshness: 0.7,
    });
    const weak = __testOnly.blendRankSignals({
      vector: 0.2,
      lexical: 0.1,
      sourceQuality: 0.4,
      freshness: 0.2,
    });
    expect(strong).toBeGreaterThan(weak);
    expect(strong).toBeLessThanOrEqual(1);
    expect(weak).toBeGreaterThanOrEqual(0);
  });
});
