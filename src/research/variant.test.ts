import { describe, expect, it } from "vitest";
import { deriveVariantScores } from "./variant.js";

describe("variant perception scoring", () => {
  it("produces positive variant when fundamentals outpace expectations", () => {
    const result = deriveVariantScores({
      surprisesPct: [-1, -2, 0],
      estimatedEps: [1.5, 1.6, 1.7, 1.8],
      revenueSeries: [140, 130, 120, 110, 100],
      marginSeries: [0.27, 0.24, 0.22, 0.2],
    });
    expect(result.fundamentalScore).toBeGreaterThan(result.expectationScore);
    expect(result.variantGapScore).toBeGreaterThan(0.5);
  });

  it("produces negative variant when expectations exceed weakening fundamentals", () => {
    const result = deriveVariantScores({
      surprisesPct: [8, 9, 7],
      estimatedEps: [2.2, 2.0, 1.8, 1.6],
      revenueSeries: [90, 100, 110, 120, 130],
      marginSeries: [0.12, 0.15, 0.17, 0.2],
    });
    expect(result.expectationScore).toBeGreaterThan(result.fundamentalScore);
    expect(result.variantGapScore).toBeLessThan(0.5);
  });
});
