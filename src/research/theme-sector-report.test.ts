import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import type {
  SectorResearchResult,
  ThemeResearchResult,
  CrossSectionConstituentSnapshot,
} from "./theme-sector.js";
import { openResearchDb } from "./db.js";
import {
  buildSectorInstitutionalReport,
  buildThemeInstitutionalReport,
} from "./theme-sector-report.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-theme-sector-report-${name}-${Date.now()}-${Math.random()}.db`);

const constituent = (ticker: string): CrossSectionConstituentSnapshot => ({
  ticker,
  companyName: `${ticker} Inc`,
  sector: "Technology",
  industry: "Software",
  currentPrice: 100,
  return20dPct: 3,
  return63dPct: 8,
  return126dPct: 14,
  drawdown63dPct: -5,
  volatility63dPct: 24,
  avgDailyDollarVolumeUsd: 25_000_000,
  avgDailyShareVolume: 600_000,
  variantStance: "positive-variant",
  variantGapScore: 0.7,
  variantConfidence: 0.72,
  valuationConfidence: 0.68,
  expectedUpsidePct: 12,
  expectedUpsideWithCatalystsPct: 14,
  impliedValuationStance: "aligned",
  portfolioStance: "watch",
  portfolioConfidence: 0.66,
  catalystOpenCount: 2,
  catalystExpectedImpactPct: 2.5,
  compositeScore: 0.71,
  conviction: "long-bias",
  notes: [],
});

describe("institutional theme/sector report builder", () => {
  it("renders schema-v3 thematic report with minimum exhibits", () => {
    const dbPath = testDbPath("theme");
    openResearchDb(dbPath);
    const result: ThemeResearchResult = {
      generatedAt: new Date().toISOString(),
      theme: "ai-inference-stack",
      themeVersion: 3,
      usedThemeRegistry: true,
      membershipMinScore: 0.55,
      benchmarkRelative: {
        benchmarkTicker: "QQQ",
        sampleSize: 120,
        lookbackDays: 365,
        themeReturnPct: 22,
        benchmarkReturnPct: 16,
        relativeReturnPct: 6,
        activeHitRatePct: 57,
        annualizedActiveReturnPct: 5,
        annualizedAlphaPct: 2,
        beta: 1.1,
        correlation: 0.76,
        trackingErrorPct: 9,
        informationRatio: 0.4,
        upsideCapturePct: 105,
        downsideCapturePct: 92,
      },
      factorAttribution: undefined,
      tickers: ["AAA", "BBB", "CCC"],
      lookbackDays: 365,
      metrics: {
        constituentCount: 3,
        breadthPositive20dPct: 0.66,
        breadthPositive63dPct: 0.67,
        medianReturn20dPct: 2.4,
        medianReturn63dPct: 7.1,
        medianExpectedUpsidePct: 11.3,
        medianVariantGapScore: 0.62,
        avgVariantConfidence: 0.7,
        avgValuationConfidence: 0.68,
        avgPortfolioConfidence: 0.65,
        dispersion63dPct: 10,
        averagePairwiseCorrelation63d: 0.2,
        avgVolatility63dPct: 24,
        totalOpenCatalysts: 4,
        avgCatalystExpectedImpactPct: 1.9,
        concentrationHhi: 0.39,
        longBiasPct: 0.66,
        shortBiasPct: 0,
        neutralPct: 0.34,
        evidenceCoverageScore: 0.82,
        institutionalReadinessScore: 0.81,
        regime: "rotation",
      },
      factorDecomposition: {
        model: "cross_sectional_style_proxy_v1",
        dominantFactor: "quality",
        concentration: 0.44,
        exposures: [
          {
            factor: "quality",
            exposureZ: 0.9,
            weightedRaw: 0.4,
            dispersionZ: 0.5,
            topPositiveTicker: "AAA",
            topNegativeTicker: "CCC",
          },
          {
            factor: "value",
            exposureZ: 0.4,
            weightedRaw: 0.2,
            dispersionZ: 0.4,
            topPositiveTicker: "BBB",
            topNegativeTicker: "CCC",
          },
          {
            factor: "momentum",
            exposureZ: 0.2,
            weightedRaw: 0.1,
            dispersionZ: 0.3,
            topPositiveTicker: "AAA",
            topNegativeTicker: "BBB",
          },
          {
            factor: "size",
            exposureZ: -0.1,
            weightedRaw: -0.2,
            dispersionZ: 0.2,
            topPositiveTicker: "CCC",
            topNegativeTicker: "AAA",
          },
        ],
      },
      catalystCalendar: {
        asOfDate: new Date().toISOString().slice(0, 10),
        horizonDays: 90,
        totalOpenEvents: 4,
        scheduledEvents: 3,
        unscheduledEvents: 1,
        eventsInHorizon: 3,
        nearTermEvents: 2,
        highImpactEvents: 1,
        avgConfidence: 0.72,
        weightedExpectedImpactBps: 115,
        weightedDownsideSharePct: 31,
        crowdingScore: 0.35,
        maxSameDayEvents: 1,
        topEventWindows: [{ date: "2026-03-15", eventCount: 2, weightedExpectedImpactBps: 80 }],
        tickerLoad: [],
      },
      sectorExposure: [
        { sector: "Technology", count: 2, sharePct: 0.66, avgCompositeScore: 0.74 },
        { sector: "Industrials", count: 1, sharePct: 0.34, avgCompositeScore: 0.61 },
      ],
      leaders: [constituent("AAA"), constituent("BBB")],
      laggards: [constituent("CCC")],
      constituents: [constituent("AAA"), constituent("BBB"), constituent("CCC")],
      riskFlags: ["Style concentration remains elevated."],
      insightSummary: ["Signal quality improving."],
    };

    const report = buildThemeInstitutionalReport({ result, dbPath });
    expect(report.exhibits.length).toBeGreaterThanOrEqual(8);
    expect(report.markdown).toContain("## A. Cover");
    expect(report.markdown).toContain("## C. The Story");
    expect(report.markdown).toContain("Takeaway:");
    expect(report.quality.exhibitCount).toBeGreaterThanOrEqual(8);
  });

  it("renders schema-v3 sector report with capital allocation section", () => {
    const dbPath = testDbPath("sector");
    openResearchDb(dbPath);
    const result: SectorResearchResult = {
      generatedAt: new Date().toISOString(),
      sector: "Semiconductors",
      tickers: ["NVDA", "AMD"],
      lookbackDays: 365,
      metrics: {
        constituentCount: 2,
        breadthPositive20dPct: 0.5,
        breadthPositive63dPct: 0.5,
        medianReturn20dPct: 1.2,
        medianReturn63dPct: 5.4,
        medianExpectedUpsidePct: 8.2,
        medianVariantGapScore: 0.58,
        avgVariantConfidence: 0.64,
        avgValuationConfidence: 0.63,
        avgPortfolioConfidence: 0.61,
        dispersion63dPct: 8.4,
        averagePairwiseCorrelation63d: 0.34,
        avgVolatility63dPct: 28,
        totalOpenCatalysts: 2,
        avgCatalystExpectedImpactPct: 1.2,
        concentrationHhi: 0.52,
        longBiasPct: 0.5,
        shortBiasPct: 0,
        neutralPct: 0.5,
        evidenceCoverageScore: 0.78,
        institutionalReadinessScore: 0.76,
        regime: "range-bound",
      },
      factorDecomposition: {
        model: "cross_sectional_style_proxy_v1",
        dominantFactor: "momentum",
        concentration: 0.41,
        exposures: [
          {
            factor: "momentum",
            exposureZ: 0.6,
            weightedRaw: 0.3,
            dispersionZ: 0.4,
            topPositiveTicker: "NVDA",
            topNegativeTicker: "AMD",
          },
          {
            factor: "value",
            exposureZ: 0.3,
            weightedRaw: 0.1,
            dispersionZ: 0.3,
            topPositiveTicker: "AMD",
            topNegativeTicker: "NVDA",
          },
          {
            factor: "quality",
            exposureZ: 0.2,
            weightedRaw: 0.1,
            dispersionZ: 0.2,
            topPositiveTicker: "NVDA",
            topNegativeTicker: "AMD",
          },
          {
            factor: "size",
            exposureZ: -0.2,
            weightedRaw: -0.1,
            dispersionZ: 0.2,
            topPositiveTicker: "AMD",
            topNegativeTicker: "NVDA",
          },
        ],
      },
      catalystCalendar: {
        asOfDate: new Date().toISOString().slice(0, 10),
        horizonDays: 90,
        totalOpenEvents: 2,
        scheduledEvents: 2,
        unscheduledEvents: 0,
        eventsInHorizon: 2,
        nearTermEvents: 1,
        highImpactEvents: 1,
        avgConfidence: 0.7,
        weightedExpectedImpactBps: 90,
        weightedDownsideSharePct: 40,
        crowdingScore: 0.2,
        maxSameDayEvents: 1,
        topEventWindows: [{ date: "2026-03-20", eventCount: 1, weightedExpectedImpactBps: 55 }],
        tickerLoad: [],
      },
      factorAttribution: undefined,
      leaders: [constituent("NVDA")],
      laggards: [constituent("AMD")],
      constituents: [constituent("NVDA"), constituent("AMD")],
      riskFlags: ["Scenario skew narrowed."],
      insightSummary: ["Selective setup."],
    };

    const report = buildSectorInstitutionalReport({ result, dbPath });
    expect(report.markdown).toContain("## G. Capital Allocation Playbook");
    expect(report.markdown).toContain("## I. Timeline and Checkpoints");
  });
});
