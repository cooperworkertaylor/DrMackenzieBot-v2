import { describe, expect, it } from "vitest";
import { composePortfolioDecision } from "./portfolio-decision.js";

const buildStrongInput = () => ({
  ticker: "NVDA",
  question: "Should we take a position now?",
  portfolio: {
    ticker: "NVDA",
    generatedAt: "2026-02-01T00:00:00.000Z",
    confidence: 0.76,
    expectedUpsidePct: 0.2,
    catalystExpectedImpactPct: 0.03,
    recommendedWeightPct: 6,
    maxRiskBudgetPct: 2.7,
    stopLossPct: 0.11,
    timeHorizonDays: 90,
    stance: "long" as const,
    reviewTriggers: ["Refresh after next print."],
    rationale: ["Variant + valuation remain aligned."],
  },
  valuation: {
    ticker: "NVDA",
    computedAt: "2026-02-01T00:00:00.000Z",
    currentPrice: 100,
    currentPriceDate: "2026-01-31",
    expectationObservationCount: 8,
    scenarios: [
      {
        name: "bear" as const,
        probability: 0.25,
        revenueGrowth: 0.06,
        operatingMargin: 0.19,
        wacc: 0.105,
        terminalGrowth: 0.025,
        taxRate: 0.21,
        fcfConversion: 0.75,
        nextRevenue: 100,
        nextEbit: 19,
        nextFcf: 14,
        enterpriseValue: 900,
        impliedSharePrice: 88,
        upsidePct: -0.12,
      },
      {
        name: "base" as const,
        probability: 0.5,
        revenueGrowth: 0.13,
        operatingMargin: 0.24,
        wacc: 0.095,
        terminalGrowth: 0.03,
        taxRate: 0.21,
        fcfConversion: 0.78,
        nextRevenue: 120,
        nextEbit: 29,
        nextFcf: 22,
        enterpriseValue: 1200,
        impliedSharePrice: 115,
        upsidePct: 0.15,
      },
      {
        name: "bull" as const,
        probability: 0.25,
        revenueGrowth: 0.22,
        operatingMargin: 0.29,
        wacc: 0.09,
        terminalGrowth: 0.03,
        taxRate: 0.21,
        fcfConversion: 0.8,
        nextRevenue: 150,
        nextEbit: 43,
        nextFcf: 34,
        enterpriseValue: 1650,
        impliedSharePrice: 145,
        upsidePct: 0.45,
      },
    ],
    expectedSharePrice: 118,
    expectedUpsidePct: 0.18,
    expectedUpsideWithCatalystsPct: 0.2,
    confidence: 0.74,
    notes: [],
  },
  variant: {
    ticker: "NVDA",
    computedAt: "2026-02-01T00:00:00.000Z",
    expectationScore: 0.71,
    fundamentalScore: 0.77,
    variantGapScore: 0.67,
    confidence: 0.72,
    stance: "positive-variant" as const,
    expectationObservations: 8,
    fundamentalObservations: 7,
    metrics: {
      avgSurprisePct: 6.1,
      estimateTrend: 0.1,
      revenueGrowth: 0.14,
      marginDelta: 0.03,
    },
    notes: [],
  },
  researchCell: {
    generatedAt: "2026-02-01T00:00:00.000Z",
    ticker: "NVDA",
    question: "Should we take a position now?",
    thesis: {
      role: "thesis" as const,
      stance: "bullish" as const,
      score: 0.78,
      confidence: 0.77,
      summary: "Thesis remains constructive.",
      keyPoints: ["Momentum improving."],
    },
    skeptic: {
      role: "skeptic" as const,
      stance: "neutral" as const,
      score: 0.32,
      confidence: 0.61,
      summary: "No fatal contradiction yet.",
      keyPoints: ["Valuation requires monitoring."],
    },
    riskManager: {
      role: "risk_manager" as const,
      stance: "neutral" as const,
      score: 0.64,
      confidence: 0.7,
      summary: "Risk controls in place.",
      keyPoints: ["Stop and budget enforced."],
    },
    allocator: {
      role: "allocator" as const,
      finalStance: "long" as const,
      score: 0.73,
      confidence: 0.8,
      recommendedWeightPct: 6,
      maxRiskBudgetPct: 2.7,
      stopLossPct: 0.11,
      summary: "Enter with measured size.",
      keyPoints: ["Strong expected return with acceptable downside."],
    },
    debate: {
      consensusScore: 0.71,
      adversarialCoverageScore: 0.82,
      disconfirmingEvidence: ["Bear case still has -12% return."],
      majorDisagreements: [],
      unresolvedRisks: [],
      riskControls: ["Stop-loss", "Risk budget"],
      requiredFollowUps: ["Re-run after next filing."],
      passed: true,
    },
  },
});

describe("portfolio decision", () => {
  it("recommends enter when score and controls are strong", () => {
    const input = buildStrongInput();
    const decision = composePortfolioDecision(input);
    expect(decision.recommendation).toBe("enter");
    expect(decision.riskBreaches).toHaveLength(0);
    expect(decision.finalStance).toBe("long");
    expect(decision.decisionScore).toBeGreaterThanOrEqual(0.68);
    expect(decision.sizeCandidates.some((candidate) => candidate.recommendation === "enter")).toBe(
      true,
    );
  });

  it("downgrades to avoid when stance is non-actionable and confidence is weak", () => {
    const input = buildStrongInput();
    input.researchCell.allocator.finalStance = "watch";
    input.researchCell.allocator.confidence = 0.35;
    input.researchCell.debate.adversarialCoverageScore = 0.42;
    input.portfolio.confidence = 0.4;
    input.variant.confidence = 0.35;
    input.valuation.confidence = 0.35;
    input.valuation.expectedUpsideWithCatalystsPct = -0.3;

    const decision = composePortfolioDecision(input);
    expect(decision.recommendation).toBe("avoid");
    expect(decision.riskBreaches.some((issue) => issue.includes("non-actionable"))).toBe(true);
    expect(decision.riskBreaches.some((issue) => issue.includes("Confidence"))).toBe(true);
    expect(decision.riskBreaches.some((issue) => issue.includes("Adversarial coverage"))).toBe(
      true,
    );
    expect(decision.sizeCandidates.every((candidate) => candidate.recommendation === "avoid")).toBe(
      true,
    );
  });

  it("flags downside and stress breaches under tight constraints", () => {
    const input = buildStrongInput();
    const decision = composePortfolioDecision({
      ...input,
      constraints: {
        maxRiskBudgetPct: 1,
        maxDownsideLossPct: 0.5,
      },
    });
    expect(decision.riskBreaches.some((issue) => issue.includes("Worst scenario loss"))).toBe(true);
    expect(
      decision.riskBreaches.some((issue) => issue.includes("scenarios exceed current risk budget")),
    ).toBe(true);
    expect(decision.stress.some((row) => row.breachesRiskBudget)).toBe(true);
  });
});
