import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  __testOnly,
  addCatalyst,
  getCatalystSummary,
  listCatalysts,
  resolveCatalyst,
} from "./catalyst.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-research-${name}-${Date.now()}-${Math.random()}.db`);

describe("catalyst engine", () => {
  it("normalizes date values for mixed formats", () => {
    expect(__testOnly.normalizeDate("2026-01-02")).toBe("2026-01-02");
    expect(__testOnly.normalizeDate("Jan 2 2026")).toBe("2026-01-02");
    expect(__testOnly.normalizeDate("not-a-date")).toBe("");
  });

  it("summarizes open catalysts and excludes resolved entries", () => {
    const dbPath = testDbPath("catalyst-summary");
    const firstId = addCatalyst({
      ticker: "aapl",
      name: "Product launch",
      category: "company",
      probability: 0.6,
      impactBps: 300,
      confidence: 0.8,
      dateWindowStart: "2026-03-01",
      dbPath,
    });
    addCatalyst({
      ticker: "AAPL",
      name: "Regulatory fine",
      category: "regulatory",
      probability: 0.25,
      impactBps: -200,
      confidence: 0.7,
      dateWindowStart: "2026-04-01",
      dbPath,
    });
    resolveCatalyst({
      catalystId: firstId,
      occurred: true,
      realizedImpactBps: 280,
      dbPath,
    });

    const open = listCatalysts({ ticker: "AAPL", status: "open", dbPath });
    expect(open).toHaveLength(1);
    expect(open[0]?.name).toBe("Regulatory fine");
    const summary = getCatalystSummary({ ticker: "AAPL", dbPath });
    expect(summary.openCount).toBe(1);
    expect(summary.expectedImpactBps).toBeCloseTo(-35, 6);
    expect(summary.expectedImpactPct).toBeCloseTo(-0.0035, 6);
    expect(summary.weightedConfidence).toBeCloseTo(0.7, 6);
    expect(summary.highImpactCount).toBe(0);
    expect(summary.nearestCatalystDate).toBe("2026-04-01");
  });
});
