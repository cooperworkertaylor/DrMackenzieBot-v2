import { describe, expect, it } from "vitest";
import {
  buildScenarioDrivers,
  deriveImpliedExpectations,
  type ValuationInputs,
} from "./valuation.js";

describe("valuation engine", () => {
  it("builds three scenarios with monotonic growth assumptions", () => {
    const inputs: ValuationInputs = {
      revenueTtm: 100_000,
      operatingMarginTtm: 0.24,
      recentRevenueGrowth: 0.08,
      expectationTrend: 0.05,
      expectationObservationCount: 8,
      sharesOutstanding: 10_000,
      currentPrice: 150,
    };
    const scenarios = buildScenarioDrivers(inputs);
    expect(scenarios).toHaveLength(3);
    const bear = scenarios.find((s) => s.name === "bear");
    const base = scenarios.find((s) => s.name === "base");
    const bull = scenarios.find((s) => s.name === "bull");
    expect(bear).toBeDefined();
    expect(base).toBeDefined();
    expect(bull).toBeDefined();
    expect((bear?.revenueGrowth ?? 0) < (base?.revenueGrowth ?? 0)).toBeTruthy();
    expect((base?.revenueGrowth ?? 0) < (bull?.revenueGrowth ?? 0)).toBeTruthy();
  });

  it("derives market-implied stance from valuation gaps", () => {
    const implied = deriveImpliedExpectations({
      marketCap: 1_500_000,
      revenueTtm: 100_000,
      modelRevenueGrowth: 0.12,
      modelOperatingMargin: 0.24,
      wacc: 0.1,
      terminalGrowth: 0.03,
      taxRate: 0.21,
      fcfConversion: 0.8,
    });
    expect(Number.isFinite(implied.impliedRevenueGrowth)).toBeTruthy();
    expect(Number.isFinite(implied.impliedOperatingMargin)).toBeTruthy();
    expect(["market-too-bearish", "market-too-bullish", "aligned"]).toContain(implied.stance);
  });
});
