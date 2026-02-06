import { describe, expect, it } from "vitest";
import { runAdversarialResearchCell } from "./research-cell.js";

const baseSnapshot = {
  ticker: "NVDA",
  entityId: 1,
  entityName: "NVIDIA Corp",
  asOfDate: "2026-02-01",
  windowStartDate: "2024-02-01",
  events: [
    {
      id: 1,
      entityId: 1,
      eventType: "filing_10_q",
      eventTime: Date.parse("2026-01-31T00:00:00.000Z"),
      periodStart: "2025-11-01",
      periodEnd: "2026-01-31",
      sourceTable: "filings",
      sourceRefId: 101,
      sourceUrl: "https://www.sec.gov/example",
      title: "Quarterly report",
      payload: {},
      eventHash: "a",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    },
  ],
  facts: [],
  metrics: [
    {
      metricKey: "us-gaap.revenues",
      metricKind: "numeric",
      unit: "USD",
      samples: 6,
      latestAsOfDate: "2026-01-31",
      latestValueNum: 100,
      previousValueNum: 90,
      deltaValueNum: 10,
    },
    {
      metricKey: "earnings.surprise_pct",
      metricKind: "numeric",
      unit: "pct",
      samples: 4,
      latestAsOfDate: "2026-01-31",
      latestValueNum: 3.2,
      previousValueNum: 2.5,
      deltaValueNum: 0.7,
    },
  ],
};

const baseInput = () => ({
  ticker: "NVDA",
  question: "Is the next 12 months risk/reward attractive?",
  claims: [
    { claim: "Revenue growth remains strong in the latest quarter.", citationIds: [1, 2] },
    { claim: "Operating leverage is improving.", citationIds: [2, 3] },
    { claim: "Estimate revisions are positive.", citationIds: [3, 4] },
    { claim: "Demand catalysts remain open.", citationIds: [4, 1] },
  ],
  citations: [
    {
      id: 1,
      source_table: "filings",
      ref_id: 10,
      url: "https://www.sec.gov/Archives/example-10q",
    },
    {
      id: 2,
      source_table: "transcripts",
      ref_id: 11,
      url: "https://www.fool.com/transcripts/example",
    },
    {
      id: 3,
      source_table: "fundamental_facts",
      ref_id: 12,
      url: "https://www.macrotrends.net/example",
    },
    {
      id: 4,
      source_table: "earnings_expectations",
      ref_id: 13,
      url: "https://www.alpha-vantage.co/query?function=EARNINGS",
    },
  ],
  variant: {
    ticker: "NVDA",
    computedAt: new Date().toISOString(),
    expectationScore: 0.74,
    fundamentalScore: 0.79,
    variantGapScore: 0.66,
    confidence: 0.73,
    stance: "positive-variant" as const,
    expectationObservations: 8,
    fundamentalObservations: 7,
    metrics: {
      avgSurprisePct: 8.2,
      estimateTrend: 0.12,
      revenueGrowth: 0.18,
      marginDelta: 0.04,
    },
    notes: [],
  },
  valuation: {
    ticker: "NVDA",
    computedAt: new Date().toISOString(),
    expectationObservationCount: 8,
    scenarios: [
      {
        name: "bear" as const,
        probability: 0.2,
        revenueGrowth: 0.08,
        operatingMargin: 0.2,
        wacc: 0.1,
        terminalGrowth: 0.02,
        taxRate: 0.21,
        fcfConversion: 0.75,
        nextRevenue: 100,
        nextEbit: 20,
        nextFcf: 15,
        enterpriseValue: 400,
        impliedSharePrice: 90,
        upsidePct: -0.1,
      },
      {
        name: "base" as const,
        probability: 0.6,
        revenueGrowth: 0.15,
        operatingMargin: 0.24,
        wacc: 0.095,
        terminalGrowth: 0.025,
        taxRate: 0.21,
        fcfConversion: 0.78,
        nextRevenue: 120,
        nextEbit: 28,
        nextFcf: 22,
        enterpriseValue: 520,
        impliedSharePrice: 125,
        upsidePct: 0.14,
      },
      {
        name: "bull" as const,
        probability: 0.2,
        revenueGrowth: 0.22,
        operatingMargin: 0.28,
        wacc: 0.09,
        terminalGrowth: 0.03,
        taxRate: 0.21,
        fcfConversion: 0.8,
        nextRevenue: 145,
        nextEbit: 40,
        nextFcf: 32,
        enterpriseValue: 700,
        impliedSharePrice: 165,
        upsidePct: 0.5,
      },
    ],
    expectedUpsideWithCatalystsPct: 0.18,
    confidence: 0.72,
    notes: [],
  },
  portfolio: {
    ticker: "NVDA",
    generatedAt: new Date().toISOString(),
    confidence: 0.71,
    expectedUpsidePct: 0.18,
    catalystExpectedImpactPct: 0.03,
    recommendedWeightPct: 6,
    maxRiskBudgetPct: 2.5,
    stopLossPct: 0.11,
    timeHorizonDays: 90,
    stance: "long" as const,
    reviewTriggers: [
      "Re-evaluate if confidence drops.",
      "Re-evaluate if stop-loss is breached.",
      "Re-evaluate if catalysts weaken.",
      "Re-evaluate if valuation stance flips.",
    ],
    rationale: ["Upside remains favorable."],
  },
  diagnostics: {
    contradictions: [] as Array<{ severity: "high" | "medium"; detail: string }>,
    falsificationTriggers: [
      "Two negative surprise prints.",
      "Estimate trend turns negative.",
      "Revenue growth below threshold.",
      "Upside turns negative.",
    ],
  },
  graphSnapshot: baseSnapshot,
});

describe("adversarial research cell", () => {
  it("produces debate outputs and passes coverage on strong setup", () => {
    const result = runAdversarialResearchCell(baseInput());
    expect(result.ticker).toBe("NVDA");
    expect(result.thesis.score).toBeGreaterThan(0.55);
    expect(result.allocator.finalStance).toBe("long");
    expect(result.debate.adversarialCoverageScore).toBeGreaterThanOrEqual(0.7);
    expect(result.debate.disconfirmingEvidence.length).toBeGreaterThanOrEqual(1);
    expect(result.debate.passed).toBe(true);
  });

  it("shifts to watch when contradictions dominate", () => {
    const input = baseInput();
    input.diagnostics.contradictions = [
      { severity: "high", detail: "Valuation and stance conflict." },
      { severity: "medium", detail: "Confidence too low for current sizing." },
    ];
    input.valuation.impliedExpectations = {
      impliedRevenueGrowth: 0.24,
      impliedOperatingMargin: 0.31,
      impliedNextFcf: 30,
      modelRevenueGrowth: 0.15,
      modelOperatingMargin: 0.24,
      growthGap: 0.09,
      marginGap: 0.07,
      stance: "market-too-bullish",
    };
    input.graphSnapshot.metrics = [
      {
        metricKey: "us-gaap.revenues",
        metricKind: "numeric",
        unit: "USD",
        samples: 6,
        latestAsOfDate: "2026-01-31",
        latestValueNum: 80,
        previousValueNum: 92,
        deltaValueNum: -12,
      },
    ];
    const result = runAdversarialResearchCell(input);
    expect(result.skeptic.score).toBeGreaterThan(result.thesis.score);
    expect(result.allocator.finalStance).toBe("watch");
    expect(result.debate.disconfirmingEvidence.length).toBeGreaterThanOrEqual(2);
  });
});
