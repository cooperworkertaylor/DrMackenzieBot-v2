import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { addCatalyst } from "./catalyst.js";
import { openResearchDb } from "./db.js";
import { upsertMacroFactorObservations } from "./macro-factors.js";
import { refreshThemeMembership, upsertThemeDefinition } from "./theme-ontology.js";
import { computeSectorResearch, computeThemeResearch } from "./theme-sector.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-theme-sector-${name}-${Date.now()}-${Math.random()}.db`);

const generateDates = (count: number, startDate: string): string[] => {
  const start = new Date(`${startDate}T00:00:00.000Z`);
  const out: string[] = [];
  for (let index = 0; index < count; index += 1) {
    out.push(new Date(start.getTime() + index * 24 * 60 * 60 * 1000).toISOString().slice(0, 10));
  }
  return out;
};

const futureDate = (daysAhead: number): string =>
  new Date(Date.now() + daysAhead * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

const seedMacroFactors = (params: { dbPath: string; dates: string[] }) => {
  const seriesDates = params.dates.slice(1);
  const factorBase: Record<string, number> = {
    rates: 0.0002,
    credit_spread: -0.0001,
    dollar: 0.00015,
    oil: 0.00025,
    vix: 0.0003,
  };
  for (const [factorKey, base] of Object.entries(factorBase)) {
    upsertMacroFactorObservations({
      factorKey,
      observations: seriesDates.map((date, index) => ({
        date,
        value: base * Math.cos((index + 3) / 11),
        source: "test_macro",
        sourceUrl: "https://example.test/macro",
      })),
      dbPath: params.dbPath,
    });
  }
};

const seedTicker = (params: {
  dbPath: string;
  ticker: string;
  name: string;
  sector: string;
  industry: string;
  dates: string[];
  basePrice: number;
  drift: number;
  phase: number;
}) => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  db.prepare(
    `INSERT OR REPLACE INTO instruments
      (ticker, name, sector, industry, updated_at)
     VALUES (?, ?, ?, ?, ?)`,
  ).run(params.ticker, params.name, params.sector, params.industry, now);
  const row = db.prepare(`SELECT id FROM instruments WHERE ticker=?`).get(params.ticker) as {
    id: number;
  };
  let price = params.basePrice;
  const insertPrice = db.prepare(
    `INSERT OR REPLACE INTO prices
      (instrument_id, date, open, high, low, close, volume, source, fetched_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  params.dates.forEach((date, index) => {
    const cyclical = 0.0035 * Math.sin((index + params.phase) / 9);
    const ret = params.drift + cyclical;
    price = Math.max(5, price * (1 + ret));
    const volume = 850_000 + (index % 15) * 22_000 + params.phase * 5_000;
    insertPrice.run(
      row.id,
      date,
      price * 0.996,
      price * 1.01,
      price * 0.99,
      price,
      volume,
      "test",
      now + index,
    );
  });
};

describe("theme and sector research", () => {
  it("builds a sector cross-sectional report with leaders and risk flags", () => {
    const dbPath = testDbPath("sector");
    const dates = generateDates(260, "2024-01-01");
    seedTicker({
      dbPath,
      ticker: "AAPL",
      name: "Apple Inc",
      sector: "Technology",
      industry: "Consumer Electronics",
      dates,
      basePrice: 120,
      drift: 0.0012,
      phase: 2,
    });
    seedTicker({
      dbPath,
      ticker: "MSFT",
      name: "Microsoft Corp",
      sector: "Technology",
      industry: "Software",
      dates,
      basePrice: 90,
      drift: 0.0008,
      phase: 5,
    });
    seedTicker({
      dbPath,
      ticker: "XOM",
      name: "Exxon Mobil",
      sector: "Energy",
      industry: "Oil & Gas",
      dates,
      basePrice: 70,
      drift: -0.0001,
      phase: 8,
    });
    seedTicker({
      dbPath,
      ticker: "QQQ",
      name: "Invesco QQQ Trust",
      sector: "ETF",
      industry: "Index Fund",
      dates,
      basePrice: 300,
      drift: 0.0007,
      phase: 6,
    });
    seedMacroFactors({ dbPath, dates });

    const result = computeSectorResearch({
      sector: "Technology",
      benchmarkTicker: "QQQ",
      lookbackDays: 1200,
      topN: 2,
      dbPath,
    });
    expect(result.sector).toBe("Technology");
    expect(result.metrics.constituentCount).toBe(2);
    expect(result.leaders.length).toBeGreaterThan(0);
    expect(result.laggards.length).toBeGreaterThan(0);
    expect(result.constituents.length).toBe(2);
    expect(result.metrics.institutionalReadinessScore).toBeGreaterThanOrEqual(0);
    expect(result.metrics.institutionalReadinessScore).toBeLessThanOrEqual(1);
    expect(result.metrics.evidenceCoverageScore).toBeGreaterThanOrEqual(0);
    expect(result.metrics.evidenceCoverageScore).toBeLessThanOrEqual(1);
    expect(result.factorDecomposition.exposures).toHaveLength(4);
    expect(result.catalystCalendar.totalOpenEvents).toBe(0);
    expect(result.factorAttribution?.benchmarkTicker).toBe("QQQ");
    expect(result.factorAttribution?.sampleSize).toBeGreaterThanOrEqual(40);
    expect(result.factorAttribution?.factorBetas.benchmark).toBeTypeOf("number");
    expect(result.factorAttribution?.macroFactors.length).toBe(5);
    expect(result.factorAttribution?.rollingWindows.length).toBeGreaterThan(0);
    expect(result.factorAttribution?.macroFactors[0]?.coveragePct).toBeGreaterThan(90);
    expect(result.insightSummary.length).toBeGreaterThanOrEqual(2);
  });

  it("builds a thematic report with sector exposure decomposition from theme registry", () => {
    const dbPath = testDbPath("theme");
    const dates = generateDates(260, "2024-01-01");
    seedTicker({
      dbPath,
      ticker: "AAPL",
      name: "Apple Inc",
      sector: "Technology",
      industry: "Consumer Electronics",
      dates,
      basePrice: 120,
      drift: 0.001,
      phase: 2,
    });
    seedTicker({
      dbPath,
      ticker: "NVDA",
      name: "NVIDIA Corp",
      sector: "Technology",
      industry: "Semiconductors",
      dates,
      basePrice: 100,
      drift: 0.0015,
      phase: 7,
    });
    seedTicker({
      dbPath,
      ticker: "XOM",
      name: "Exxon Mobil",
      sector: "Energy",
      industry: "Oil & Gas",
      dates,
      basePrice: 70,
      drift: 0.0001,
      phase: 4,
    });
    seedTicker({
      dbPath,
      ticker: "QQQ",
      name: "Invesco QQQ Trust",
      sector: "ETF",
      industry: "Index Fund",
      dates,
      basePrice: 300,
      drift: 0.0007,
      phase: 6,
    });
    seedMacroFactors({ dbPath, dates });

    upsertThemeDefinition({
      theme: "ai-compute-energy",
      displayName: "AI Compute + Energy",
      benchmark: "QQQ",
      rules: {
        tickerAllowlist: ["AAPL", "NVDA", "XOM"],
        minMembershipScore: 0.5,
      },
      activate: true,
      dbPath,
    });
    refreshThemeMembership({
      theme: "ai-compute-energy",
      dbPath,
    });
    addCatalyst({
      ticker: "NVDA",
      name: "AI platform launch",
      probability: 0.8,
      impactBps: 420,
      confidence: 0.75,
      direction: "up",
      dateWindowStart: futureDate(9),
      dateWindowEnd: futureDate(12),
      dbPath,
    });
    addCatalyst({
      ticker: "XOM",
      name: "Commodity downside shock",
      probability: 0.65,
      impactBps: 380,
      confidence: 0.8,
      direction: "down",
      dateWindowStart: futureDate(15),
      dateWindowEnd: futureDate(20),
      dbPath,
    });
    addCatalyst({
      ticker: "AAPL",
      name: "Regulatory review",
      probability: 0.35,
      impactBps: 240,
      confidence: 0.6,
      direction: "down",
      dbPath,
    });

    const result = computeThemeResearch({
      theme: "ai-compute-energy",
      lookbackDays: 1200,
      topN: 2,
      dbPath,
    });
    expect(result.theme).toBe("ai-compute-energy");
    expect(result.usedThemeRegistry).toBe(true);
    expect(result.metrics.constituentCount).toBe(3);
    expect(result.benchmarkRelative?.benchmarkTicker).toBe("QQQ");
    expect(result.benchmarkRelative?.sampleSize).toBeGreaterThanOrEqual(30);
    expect(result.benchmarkRelative?.themeReturnPct).toBeTypeOf("number");
    expect(result.benchmarkRelative?.benchmarkReturnPct).toBeTypeOf("number");
    expect(result.factorDecomposition.exposures).toHaveLength(4);
    expect(result.catalystCalendar.totalOpenEvents).toBeGreaterThanOrEqual(3);
    expect(result.catalystCalendar.eventsInHorizon).toBeGreaterThanOrEqual(2);
    expect(result.catalystCalendar.weightedDownsideSharePct).toBeGreaterThan(0);
    expect(result.factorAttribution?.benchmarkTicker).toBe("QQQ");
    expect(result.factorAttribution?.sampleSize).toBeGreaterThanOrEqual(40);
    expect(result.factorAttribution?.annualizedAlphaPct).toBeTypeOf("number");
    expect(result.factorAttribution?.macroFactors.length).toBe(5);
    expect(result.factorAttribution?.rollingWindows.length).toBeGreaterThan(0);
    expect(result.factorAttribution?.macroFactors[0]?.coveragePct).toBeGreaterThan(90);
    expect(result.sectorExposure.length).toBeGreaterThanOrEqual(2);
    expect(result.sectorExposure.reduce((sum, row) => sum + row.sharePct, 0)).toBeCloseTo(1, 5);
    expect(result.leaders.length).toBeGreaterThan(0);
    expect(result.laggards.length).toBeGreaterThan(0);
    expect(result.insightSummary.length).toBeGreaterThanOrEqual(2);
  });

  it("bootstraps theme membership on first run when registry has no active members", () => {
    const dbPath = testDbPath("theme-bootstrap");
    const dates = generateDates(260, "2024-01-01");
    seedTicker({
      dbPath,
      ticker: "AAPL",
      name: "Apple Inc",
      sector: "Technology",
      industry: "Consumer Electronics",
      dates,
      basePrice: 120,
      drift: 0.0011,
      phase: 2,
    });
    seedTicker({
      dbPath,
      ticker: "NVDA",
      name: "NVIDIA Corp",
      sector: "Technology",
      industry: "Semiconductors",
      dates,
      basePrice: 100,
      drift: 0.0014,
      phase: 7,
    });
    seedTicker({
      dbPath,
      ticker: "QQQ",
      name: "Invesco QQQ Trust",
      sector: "ETF",
      industry: "Index Fund",
      dates,
      basePrice: 300,
      drift: 0.0007,
      phase: 6,
    });
    seedMacroFactors({ dbPath, dates });

    const result = computeThemeResearch({
      theme: "technology",
      lookbackDays: 1200,
      topN: 2,
      dbPath,
    });
    expect(result.tickers.length).toBeGreaterThan(0);
    expect(result.metrics.constituentCount).toBeGreaterThan(0);
    expect(result.themeVersion).toBeGreaterThanOrEqual(1);
    expect(result.usedThemeRegistry).toBe(true);
  });

  it("auto-registers theme membership from explicit tickers on first report", () => {
    const dbPath = testDbPath("theme-explicit-tickers");
    const dates = generateDates(260, "2024-01-01");
    seedTicker({
      dbPath,
      ticker: "NVDA",
      name: "NVIDIA Corp",
      sector: "Technology",
      industry: "Semiconductors",
      dates,
      basePrice: 100,
      drift: 0.0014,
      phase: 7,
    });
    seedTicker({
      dbPath,
      ticker: "MSFT",
      name: "Microsoft Corp",
      sector: "Technology",
      industry: "Software",
      dates,
      basePrice: 90,
      drift: 0.001,
      phase: 5,
    });
    seedTicker({
      dbPath,
      ticker: "QQQ",
      name: "Invesco QQQ Trust",
      sector: "ETF",
      industry: "Index Fund",
      dates,
      basePrice: 300,
      drift: 0.0007,
      phase: 6,
    });
    seedMacroFactors({ dbPath, dates });

    const first = computeThemeResearch({
      theme: "ai-infrastructure",
      tickers: ["NVDA", "MSFT"],
      lookbackDays: 1200,
      topN: 2,
      dbPath,
    });
    expect(first.metrics.constituentCount).toBe(2);
    expect(first.themeVersion).toBeGreaterThanOrEqual(1);
    expect(first.benchmarkTicker).toBe("QQQ");
    expect(first.benchmarkRelative?.benchmarkTicker).toBe("QQQ");
    expect(first.factorAttribution?.benchmarkTicker).toBe("QQQ");

    const second = computeThemeResearch({
      theme: "ai-infrastructure",
      lookbackDays: 1200,
      topN: 2,
      dbPath,
    });
    expect(second.usedThemeRegistry).toBe(true);
    expect(second.metrics.constituentCount).toBeGreaterThanOrEqual(2);
    expect(second.benchmarkTicker).toBe("QQQ");
  });
});
