import { describe, expect, it } from "vitest";
import { gradeInstitutionalMemo } from "./grade.js";

const isoNow = new Date().toISOString();

const baseInputs = () => ({
  hitsCount: 12,
  claims: [
    { claim: "Revenue growth accelerated in the latest quarter.", citationIds: [1, 2] },
    { claim: "Margin expansion supports upside to consensus.", citationIds: [2, 3] },
    { claim: "Management commentary points to durable demand.", citationIds: [3, 4] },
    { claim: "Catalyst path is visible over the next two quarters.", citationIds: [4, 1] },
  ],
  citations: [
    {
      id: 1,
      source_table: "filings",
      ref_id: 101,
      url: "https://www.sec.gov/ixviewer/ix.html?doc=/Archives/data/000000/2025-11-05-10q.htm",
      metadata: JSON.stringify({ filingDate: "2025-11-05" }),
    },
    {
      id: 2,
      source_table: "transcripts",
      ref_id: 201,
      url: "https://www.fool.com/earnings/call-transcripts/2025/11/07/example/",
      metadata: JSON.stringify({ eventDate: "2025-11-07" }),
    },
    {
      id: 3,
      source_table: "fundamental_facts",
      ref_id: 301,
      url: "https://www.macrotrends.net/stocks/charts/EXM/example/revenue",
      metadata: JSON.stringify({ asOfDate: "2025-10-31" }),
    },
    {
      id: 4,
      source_table: "earnings_expectations",
      ref_id: 401,
      url: "https://www.alpha-vantage.co/query?function=EARNINGS",
      metadata: JSON.stringify({ reportedDate: "2025-11-04" }),
    },
  ],
  variant: {
    ticker: "EXM",
    computedAt: isoNow,
    expectationScore: 0.64,
    fundamentalScore: 0.72,
    variantGapScore: 0.61,
    confidence: 0.72,
    stance: "positive-variant" as const,
    expectationObservations: 6,
    fundamentalObservations: 6,
    metrics: {
      avgSurprisePct: 6.2,
      estimateTrend: 0.08,
      revenueGrowth: 0.12,
      marginDelta: 0.03,
    },
    notes: [],
  },
  valuation: {
    ticker: "EXM",
    computedAt: isoNow,
    expectationObservationCount: 6,
    scenarios: [
      {
        name: "bear" as const,
        probability: 0.2,
        revenueGrowth: 0.04,
        operatingMargin: 0.15,
        wacc: 0.1,
        terminalGrowth: 0.02,
        taxRate: 0.21,
        fcfConversion: 0.72,
        nextRevenue: 100,
        nextEbit: 15,
        nextFcf: 11,
        enterpriseValue: 200,
        impliedSharePrice: 42,
        upsidePct: -0.1,
      },
      {
        name: "base" as const,
        probability: 0.6,
        revenueGrowth: 0.08,
        operatingMargin: 0.19,
        wacc: 0.095,
        terminalGrowth: 0.025,
        taxRate: 0.21,
        fcfConversion: 0.76,
        nextRevenue: 110,
        nextEbit: 21,
        nextFcf: 15,
        enterpriseValue: 260,
        impliedSharePrice: 56,
        upsidePct: 0.12,
      },
      {
        name: "bull" as const,
        probability: 0.2,
        revenueGrowth: 0.12,
        operatingMargin: 0.23,
        wacc: 0.09,
        terminalGrowth: 0.03,
        taxRate: 0.21,
        fcfConversion: 0.8,
        nextRevenue: 120,
        nextEbit: 27.6,
        nextFcf: 21,
        enterpriseValue: 320,
        impliedSharePrice: 68,
        upsidePct: 0.32,
      },
    ],
    expectedUpsideWithCatalystsPct: 0.14,
    confidence: 0.74,
    notes: [],
  },
  portfolio: {
    ticker: "EXM",
    generatedAt: isoNow,
    confidence: 0.71,
    expectedUpsidePct: 0.14,
    catalystExpectedImpactPct: 0.02,
    recommendedWeightPct: 5,
    maxRiskBudgetPct: 2,
    stopLossPct: 0.12,
    timeHorizonDays: 90,
    stance: "long" as const,
    reviewTriggers: [
      "Re-evaluate if confidence drops below 0.55.",
      "Re-evaluate if downside exceeds 12%.",
      "Re-evaluate if catalyst impact weakens.",
      "Re-evaluate if implied expectations flip.",
    ],
    rationale: ["Scenario-weighted upside is positive."],
  },
  diagnostics: {
    contradictions: [],
    falsificationTriggers: [
      "Two quarters of negative EPS surprise.",
      "Estimate trend turns negative.",
      "Revenue growth signal weakens.",
      "Expected upside turns negative.",
    ],
  },
  minScore: 0.82,
  calibrationOverride: {
    mode: "historical" as const,
    sampleCount: 40,
    score: 0.78,
    mae: 0.14,
    directionalAccuracy: 0.59,
    confidenceOutcomeMae: 0.13,
  },
});

describe("institutional memo grading", () => {
  it("passes high-quality memo inputs", () => {
    const grade = gradeInstitutionalMemo(baseInputs());
    expect(grade.passed).toBe(true);
    expect(grade.score).toBeGreaterThanOrEqual(0.82);
    expect(grade.requiredFailures).toEqual([]);
  });

  it("fails when claim-level evidence coverage is weak", () => {
    const params = baseInputs();
    params.claims = params.claims.map((claim) => ({
      ...claim,
      citationIds: [claim.citationIds[0]!],
    }));
    const grade = gradeInstitutionalMemo(params);
    expect(grade.passed).toBe(false);
    expect(grade.requiredFailures).toContain("claim_evidence_coverage");
  });

  it("fails when actionability is insufficient", () => {
    const params = baseInputs();
    params.portfolio.stance = "insufficient-evidence";
    params.portfolio.recommendedWeightPct = 0;
    params.portfolio.maxRiskBudgetPct = 0;
    const grade = gradeInstitutionalMemo(params);
    expect(grade.passed).toBe(false);
    expect(grade.requiredFailures).toContain("actionability");
  });

  it("fails when adversarial debate coverage is weak", () => {
    const params = baseInputs();
    params.researchCell = {
      coverageScore: 0.31,
      dissentCount: 0,
      disconfirmingEvidenceCount: 0,
      riskControlCount: 1,
      unresolvedRiskCount: 3,
      finalStance: "long",
      finalConfidence: 0.41,
      passed: false,
    };
    const grade = gradeInstitutionalMemo(params);
    expect(grade.passed).toBe(false);
    expect(grade.requiredFailures).toContain("adversarial_debate");
  });
});
