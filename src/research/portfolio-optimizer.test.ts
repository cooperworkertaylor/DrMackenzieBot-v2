import { describe, expect, it } from "vitest";
import type { PortfolioDecisionResult } from "./portfolio-decision.js";
import { composePortfolioOptimization } from "./portfolio-optimizer.js";

const decisionStub = (params: {
  ticker: string;
  stance: "long" | "short" | "watch";
  recommendation: "enter" | "watch" | "avoid";
  expectedReturnPct: number;
  decisionScore: number;
  confidence: number;
  baseWeightPct: number;
  baseRiskBudgetPct: number;
  bearReturnPct: number;
  baseReturnPct: number;
  bullReturnPct: number;
}): PortfolioDecisionResult =>
  ({
    generatedAt: "2026-02-06T00:00:00.000Z",
    ticker: params.ticker,
    question: "portfolio question",
    recommendation: params.recommendation,
    finalStance: params.stance,
    decisionScore: params.decisionScore,
    confidence: params.confidence,
    constraints: {
      maxSingleNameWeightPct: 8,
      maxRiskBudgetPct: 4,
      maxStopLossPct: 0.24,
      minConfidence: 0.58,
      requiredDebateCoverage: 0.7,
      maxDownsideLossPct: 2.2,
    },
    expectedReturnPct: params.expectedReturnPct,
    downsideRiskPct: Math.abs(Math.min(0, params.bearReturnPct * (params.baseWeightPct / 100))),
    riskBreaches: [],
    stress: [
      {
        scenario: "bear",
        probability: 0.25,
        returnPct: params.bearReturnPct,
        weightedReturnPct: params.bearReturnPct * 0.25,
        pnlPct: params.bearReturnPct * (params.baseWeightPct / 100),
        breachesRiskBudget: false,
      },
      {
        scenario: "base",
        probability: 0.5,
        returnPct: params.baseReturnPct,
        weightedReturnPct: params.baseReturnPct * 0.5,
        pnlPct: params.baseReturnPct * (params.baseWeightPct / 100),
        breachesRiskBudget: false,
      },
      {
        scenario: "bull",
        probability: 0.25,
        returnPct: params.bullReturnPct,
        weightedReturnPct: params.bullReturnPct * 0.25,
        pnlPct: params.bullReturnPct * (params.baseWeightPct / 100),
        breachesRiskBudget: false,
      },
    ],
    sizeCandidates: [
      {
        label: "conservative",
        weightPct: params.baseWeightPct * 0.65,
        riskBudgetPct: params.baseRiskBudgetPct * 0.65,
        expectedPnlPct: 0,
        downsidePnlPct: 0,
        score: params.decisionScore * 0.9,
        recommendation: params.recommendation === "avoid" ? "avoid" : "watch",
        notes: [],
      },
      {
        label: "base",
        weightPct: params.baseWeightPct,
        riskBudgetPct: params.baseRiskBudgetPct,
        expectedPnlPct: 0,
        downsidePnlPct: 0,
        score: params.decisionScore,
        recommendation: params.recommendation,
        notes: [],
      },
      {
        label: "aggressive",
        weightPct: params.baseWeightPct * 1.2,
        riskBudgetPct: params.baseRiskBudgetPct * 1.2,
        expectedPnlPct: 0,
        downsidePnlPct: 0,
        score: params.decisionScore * 1.05,
        recommendation: params.recommendation === "avoid" ? "avoid" : "enter",
        notes: [],
      },
    ],
    rationale: ["test rationale"],
    portfolio: {} as PortfolioDecisionResult["portfolio"],
    valuation: {} as PortfolioDecisionResult["valuation"],
    variant: {} as PortfolioDecisionResult["variant"],
    researchCell: {} as PortfolioDecisionResult["researchCell"],
  }) as PortfolioDecisionResult;

describe("portfolio optimizer", () => {
  it("enforces gross/net/sector constraints and applies correlation penalty", () => {
    const decisions = [
      decisionStub({
        ticker: "AAPL",
        stance: "long",
        recommendation: "enter",
        expectedReturnPct: 18,
        decisionScore: 0.82,
        confidence: 0.79,
        baseWeightPct: 7,
        baseRiskBudgetPct: 2.8,
        bearReturnPct: -18,
        baseReturnPct: 12,
        bullReturnPct: 30,
      }),
      decisionStub({
        ticker: "MSFT",
        stance: "long",
        recommendation: "enter",
        expectedReturnPct: 16,
        decisionScore: 0.8,
        confidence: 0.77,
        baseWeightPct: 6.5,
        baseRiskBudgetPct: 2.6,
        bearReturnPct: -16,
        baseReturnPct: 10,
        bullReturnPct: 26,
      }),
      decisionStub({
        ticker: "XOM",
        stance: "short",
        recommendation: "enter",
        expectedReturnPct: -10,
        decisionScore: 0.73,
        confidence: 0.7,
        baseWeightPct: 5.5,
        baseRiskBudgetPct: 2.1,
        bearReturnPct: -14,
        baseReturnPct: -2,
        bullReturnPct: 10,
      }),
    ];
    const result = composePortfolioOptimization({
      tickers: ["AAPL", "MSFT", "XOM"],
      question: "Optimize three names",
      decisions,
      constraints: {
        maxGrossExposurePct: 12,
        maxNetExposurePct: 4,
        maxPortfolioRiskBudgetPct: 4,
        maxSectorExposurePct: 6,
        maxSingleNameWeightPct: 8,
        maxPairwiseCorrelation: 0.7,
        maxWeightedCorrelation: 0.7,
      },
      sectors: {
        AAPL: "Technology",
        MSFT: "Technology",
        XOM: "Energy",
      },
      pairwiseCorrelation: [
        { left: "AAPL", right: "MSFT", correlation: 0.9, overlapDays: 140 },
        { left: "AAPL", right: "XOM", correlation: 0.2, overlapDays: 140 },
        { left: "MSFT", right: "XOM", correlation: 0.24, overlapDays: 140 },
      ],
    });
    expect(result.positions.length).toBeGreaterThanOrEqual(2);
    expect(result.metrics.grossExposurePct).toBeLessThanOrEqual(12.0001);
    expect(Math.abs(result.metrics.netExposurePct)).toBeLessThanOrEqual(4.0001);
    expect(result.sectorExposurePct["Technology"] ?? 0).toBeLessThanOrEqual(6.0001);
    expect(result.positions.some((position) => position.correlationPenalty < 1)).toBe(true);
    expect(result.constraintBreaches).toHaveLength(0);
  });

  it("drops non-actionable and negative directional-return names", () => {
    const decisions = [
      decisionStub({
        ticker: "TSLA",
        stance: "watch",
        recommendation: "watch",
        expectedReturnPct: 4,
        decisionScore: 0.55,
        confidence: 0.5,
        baseWeightPct: 3,
        baseRiskBudgetPct: 1.2,
        bearReturnPct: -20,
        baseReturnPct: 3,
        bullReturnPct: 25,
      }),
      decisionStub({
        ticker: "IBM",
        stance: "short",
        recommendation: "enter",
        expectedReturnPct: 8,
        decisionScore: 0.71,
        confidence: 0.69,
        baseWeightPct: 4.2,
        baseRiskBudgetPct: 1.6,
        bearReturnPct: -12,
        baseReturnPct: 4,
        bullReturnPct: 14,
      }),
      decisionStub({
        ticker: "NVDA",
        stance: "long",
        recommendation: "enter",
        expectedReturnPct: 14,
        decisionScore: 0.78,
        confidence: 0.76,
        baseWeightPct: 5,
        baseRiskBudgetPct: 2,
        bearReturnPct: -15,
        baseReturnPct: 10,
        bullReturnPct: 25,
      }),
    ];
    const result = composePortfolioOptimization({
      tickers: ["TSLA", "IBM", "NVDA"],
      question: "Filter candidates",
      decisions,
      pairwiseCorrelation: [],
    });
    expect(result.positions).toHaveLength(1);
    expect(result.positions[0]?.ticker).toBe("NVDA");
    expect(result.dropped.some((row) => row.ticker === "TSLA")).toBe(true);
    expect(result.dropped.some((row) => row.ticker === "IBM")).toBe(true);
  });

  it("flags stress-loss breaches when downside exceeds portfolio limit", () => {
    const decisions = [
      decisionStub({
        ticker: "AMD",
        stance: "long",
        recommendation: "enter",
        expectedReturnPct: 17,
        decisionScore: 0.8,
        confidence: 0.77,
        baseWeightPct: 8,
        baseRiskBudgetPct: 3,
        bearReturnPct: -35,
        baseReturnPct: 12,
        bullReturnPct: 34,
      }),
      decisionStub({
        ticker: "AMZN",
        stance: "long",
        recommendation: "enter",
        expectedReturnPct: 13,
        decisionScore: 0.74,
        confidence: 0.72,
        baseWeightPct: 7,
        baseRiskBudgetPct: 2.8,
        bearReturnPct: -30,
        baseReturnPct: 9,
        bullReturnPct: 23,
      }),
    ];
    const result = composePortfolioOptimization({
      tickers: ["AMD", "AMZN"],
      question: "Stress check",
      decisions,
      constraints: {
        maxStressLossPct: 1,
        maxGrossExposurePct: 20,
        maxNetExposurePct: 20,
      },
      pairwiseCorrelation: [{ left: "AMD", right: "AMZN", correlation: 0.45, overlapDays: 120 }],
    });
    expect(result.scenarioStress.some((row) => row.breachesStressLossLimit)).toBe(true);
    expect(result.constraintBreaches.some((breach) => breach.includes("Worst stress loss"))).toBe(
      true,
    );
  });
});
