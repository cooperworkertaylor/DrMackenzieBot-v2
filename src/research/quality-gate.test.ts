import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import type { InstitutionalGradeResult } from "./grade.js";
import {
  evaluateCrossSectionQualityGate,
  evaluateMemoQualityGate,
  evaluateQualityGateRegression,
  listQualityGateRuns,
  recordQualityGateRun,
  summarizeQualityGateRuns,
} from "./quality-gate.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-quality-gate-${name}-${Date.now()}-${Math.random()}.db`);

const strongGrade = (): InstitutionalGradeResult => ({
  score: 0.9,
  minScore: 0.82,
  passed: true,
  checks: [],
  requiredFailures: [],
  calibration: {
    mode: "historical",
    sampleCount: 24,
    score: 0.81,
    passed: true,
    detail: "ok",
    mae: 0.12,
    directionalAccuracy: 0.74,
    confidenceOutcomeMae: 0.11,
  },
  actionabilityScore: 0.86,
  adversarialCoverageScore: 0.84,
});

describe("quality gate", () => {
  it("passes high-quality memo gate inputs", () => {
    const gate = evaluateMemoQualityGate({
      artifactId: "AAPL:test",
      minScore: 0.82,
      claims: [
        { claim: "Revenue accelerated in the latest quarter.", citationIds: [1, 2] },
        { claim: "Operating leverage improved year over year.", citationIds: [3, 4] },
        { claim: "Guidance implies durable demand.", citationIds: [5, 6] },
      ],
      citations: [
        {
          id: 1,
          source_table: "filings",
          ref_id: 101,
          metadata: JSON.stringify({ filingDate: "2025-11-01" }),
          url: "https://www.sec.gov/ixviewer/ix.html",
        },
        {
          id: 2,
          source_table: "transcripts",
          ref_id: 102,
          metadata: JSON.stringify({ eventDate: "2025-11-05" }),
          url: "https://seekingalpha.com/transcript/abc",
        },
        {
          id: 3,
          source_table: "fundamental_facts",
          ref_id: 103,
          metadata: JSON.stringify({ asOfDate: "2025-10-01" }),
          url: "https://www.sec.gov/data/companyfacts.json",
        },
        {
          id: 4,
          source_table: "earnings_expectations",
          ref_id: 104,
          metadata: JSON.stringify({ reportedDate: "2025-10-25" }),
          url: "https://www.alphavantage.co/query",
        },
        {
          id: 5,
          source_table: "filings",
          ref_id: 105,
          metadata: JSON.stringify({ filingDate: "2025-09-10" }),
          url: "https://www.sec.gov/Archives/edgar/data/...",
        },
        {
          id: 6,
          source_table: "transcripts",
          ref_id: 106,
          metadata: JSON.stringify({ eventDate: "2025-10-30" }),
          url: "https://www.fool.com/earnings/call-transcripts/...",
        },
      ],
      diagnostics: {
        contradictions: [],
        falsificationTriggers: ["a", "b", "c", "d"],
      },
      valuation: {
        ticker: "AAPL",
        confidence: 0.78,
        currentPrice: 190,
        currentPriceDate: "2025-11-05",
        expectedSharePrice: 214,
        expectedUpsidePct: 0.126,
        expectedUpsideWithCatalystsPct: 0.14,
        scenarios: [
          {
            name: "bear",
            probability: 0.25,
            revenueGrowth: 0.02,
            operatingMargin: 0.2,
            taxRate: 0.18,
            wacc: 0.1,
            terminalGrowth: 0.02,
            impliedSharePrice: 168,
          },
          {
            name: "base",
            probability: 0.5,
            revenueGrowth: 0.06,
            operatingMargin: 0.25,
            taxRate: 0.18,
            wacc: 0.095,
            terminalGrowth: 0.025,
            impliedSharePrice: 214,
          },
          {
            name: "bull",
            probability: 0.25,
            revenueGrowth: 0.1,
            operatingMargin: 0.28,
            taxRate: 0.18,
            wacc: 0.09,
            terminalGrowth: 0.03,
            impliedSharePrice: 242,
          },
        ],
        notes: [],
        impliedExpectations: {
          impliedRevenueGrowth: 0.055,
          impliedOperatingMargin: 0.24,
          modelRevenueGrowth: 0.06,
          modelOperatingMargin: 0.25,
          stance: "aligned",
        },
      },
      grade: strongGrade(),
    });
    expect(gate.passed).toBe(true);
    expect(gate.requiredFailures).toHaveLength(0);
    expect(gate.score).toBeGreaterThanOrEqual(0.82);
  });

  it("fails memo gate when memo grade has required failures", () => {
    const grade = strongGrade();
    grade.passed = false;
    grade.requiredFailures = ["actionability"];
    const gate = evaluateMemoQualityGate({
      artifactId: "NU:test",
      minScore: 0.82,
      claims: [
        { claim: "Revenue accelerated in the latest quarter.", citationIds: [1, 2] },
        { claim: "Operating leverage improved year over year.", citationIds: [3, 4] },
        { claim: "Guidance implies durable demand.", citationIds: [5, 6] },
      ],
      citations: [
        {
          id: 1,
          source_table: "filings",
          ref_id: 101,
          metadata: JSON.stringify({ filingDate: "2025-11-01" }),
          url: "https://www.sec.gov/ixviewer/ix.html",
        },
        {
          id: 2,
          source_table: "transcripts",
          ref_id: 102,
          metadata: JSON.stringify({ eventDate: "2025-11-05" }),
          url: "https://seekingalpha.com/transcript/abc",
        },
        {
          id: 3,
          source_table: "fundamental_facts",
          ref_id: 103,
          metadata: JSON.stringify({ asOfDate: "2025-10-01" }),
          url: "https://www.sec.gov/data/companyfacts.json",
        },
        {
          id: 4,
          source_table: "earnings_expectations",
          ref_id: 104,
          metadata: JSON.stringify({ reportedDate: "2025-10-25" }),
          url: "https://www.alphavantage.co/query",
        },
        {
          id: 5,
          source_table: "filings",
          ref_id: 105,
          metadata: JSON.stringify({ filingDate: "2025-09-10" }),
          url: "https://www.sec.gov/Archives/edgar/data/...",
        },
        {
          id: 6,
          source_table: "transcripts",
          ref_id: 106,
          metadata: JSON.stringify({ eventDate: "2025-10-30" }),
          url: "https://www.fool.com/earnings/call-transcripts/...",
        },
      ],
      diagnostics: {
        contradictions: [],
        falsificationTriggers: ["a", "b", "c", "d"],
      },
      valuation: {
        ticker: "NU",
        confidence: 0.78,
        currentPrice: 190,
        currentPriceDate: "2025-11-05",
        expectedSharePrice: 214,
        expectedUpsidePct: 0.126,
        expectedUpsideWithCatalystsPct: 0.14,
        scenarios: [
          {
            name: "bear",
            probability: 0.25,
            revenueGrowth: 0.02,
            operatingMargin: 0.2,
            taxRate: 0.18,
            wacc: 0.1,
            terminalGrowth: 0.02,
            impliedSharePrice: 168,
          },
          {
            name: "base",
            probability: 0.5,
            revenueGrowth: 0.06,
            operatingMargin: 0.25,
            taxRate: 0.18,
            wacc: 0.095,
            terminalGrowth: 0.025,
            impliedSharePrice: 214,
          },
          {
            name: "bull",
            probability: 0.25,
            revenueGrowth: 0.1,
            operatingMargin: 0.28,
            taxRate: 0.18,
            wacc: 0.09,
            terminalGrowth: 0.03,
            impliedSharePrice: 242,
          },
        ],
        notes: [],
        impliedExpectations: {
          impliedRevenueGrowth: 0.055,
          impliedOperatingMargin: 0.24,
          modelRevenueGrowth: 0.06,
          modelOperatingMargin: 0.25,
          stance: "aligned",
        },
      },
      grade,
    });
    const modelQuality = gate.checks.find((check) => check.name === "model_quality");
    expect(gate.passed).toBe(false);
    expect(gate.requiredFailures).toContain("model_quality");
    expect(modelQuality?.passed).toBe(false);
    expect(modelQuality?.score).toBe(0);
  });

  it("records runs and detects recent quality regression", () => {
    const dbPath = testDbPath("regression");
    const baselineGate = evaluateCrossSectionQualityGate({
      artifactType: "theme_report",
      artifactId: "theme-a",
      evidenceCoverageScore: 0.9,
      institutionalReadinessScore: 0.88,
      avgVariantConfidence: 0.82,
      avgValuationConfidence: 0.8,
      avgPortfolioConfidence: 0.79,
      benchmarkContextScore: 0.9,
      scenarioCoverageRatio: 0.95,
      riskFlagCount: 0,
      uniqueGroupCount: 5,
      factorStabilityScore: 0.86,
      macroCoveragePct: 92,
      generatedAt: new Date().toISOString(),
      minScore: 0.82,
    });
    const recentGate = evaluateCrossSectionQualityGate({
      artifactType: "theme_report",
      artifactId: "theme-a",
      evidenceCoverageScore: 0.58,
      institutionalReadinessScore: 0.54,
      avgVariantConfidence: 0.5,
      avgValuationConfidence: 0.48,
      avgPortfolioConfidence: 0.46,
      benchmarkContextScore: 0.35,
      scenarioCoverageRatio: 0.3,
      riskFlagCount: 4,
      uniqueGroupCount: 1,
      factorStabilityScore: 0.3,
      macroCoveragePct: 42,
      generatedAt: new Date().toISOString(),
      minScore: 0.82,
    });
    const now = Date.now();
    for (let index = 0; index < 12; index += 1) {
      recordQualityGateRun({
        evaluation: baselineGate,
        createdAt: now - (25 + index) * 86_400_000,
        dbPath,
      });
    }
    for (let index = 0; index < 10; index += 1) {
      recordQualityGateRun({
        evaluation: recentGate,
        createdAt: now - (2 + index) * 86_400_000,
        dbPath,
      });
    }
    const runs = listQualityGateRuns({
      artifactType: "theme_report",
      days: 90,
      limit: 200,
      dbPath,
    });
    const summary = summarizeQualityGateRuns(runs);
    expect(summary.total).toBe(22);
    expect(summary.passRate).toBeLessThan(0.6);
    const regression = evaluateQualityGateRegression({
      artifactType: "theme_report",
      lookbackDays: 90,
      recentDays: 14,
      minRecentSamples: 8,
      minRecentPassRate: 0.75,
      minRecentAvgScore: 0.8,
      maxPassRateDrop: 0.08,
      maxAvgScoreDrop: 0.05,
      dbPath,
    });
    expect(regression.passed).toBe(false);
    expect(regression.reasons.length).toBeGreaterThan(0);
  });
});
