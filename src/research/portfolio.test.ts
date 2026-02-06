import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { computePortfolioPlan } from "./portfolio.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-research-${name}-${Date.now()}-${Math.random()}.db`);

describe("portfolio planner", () => {
  it("returns an insufficient-evidence stance when research coverage is missing", () => {
    const dbPath = testDbPath("portfolio-empty");
    const plan = computePortfolioPlan({ ticker: "AAPL", dbPath });
    expect(plan.stance).toBe("insufficient-evidence");
    expect(plan.recommendedWeightPct).toBe(0);
    expect(plan.maxRiskBudgetPct).toBe(0);
    expect(plan.reviewTriggers.length).toBeGreaterThanOrEqual(3);
  });
});
