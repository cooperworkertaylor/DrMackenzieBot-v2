import { describe, expect, it } from "vitest";
import { __testOnly } from "./embeddings-provider.js";

describe("embeddings-provider chunk safeguards", () => {
  it("splits oversized text into bounded segments", () => {
    const text = Array.from({ length: 23 }, (_, index) => `tok${index + 1}`).join(" ");
    const segments = __testOnly.splitTextForEmbedding(text, 10);
    expect(segments.length).toBe(3);
    expect(segments[0]?.split(/\s+/).length).toBeLessThanOrEqual(10);
    expect(segments[1]?.split(/\s+/).length).toBeLessThanOrEqual(10);
    expect(segments[2]?.split(/\s+/).length).toBeLessThanOrEqual(10);
  });

  it("returns weighted average vectors with normalization", () => {
    const merged = __testOnly.averageVectors(
      [
        [1, 0, 0],
        [0, 2, 0],
      ],
      [1, 3],
    );
    expect(merged.length).toBe(3);
    expect(merged[1]).toBeGreaterThan(merged[0] ?? 0);
    const magnitude = Math.sqrt(merged.reduce((sum, value) => sum + value * value, 0));
    expect(magnitude).toBeCloseTo(1, 6);
  });
});
