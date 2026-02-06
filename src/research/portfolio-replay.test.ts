import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import { computePortfolioReplay } from "./portfolio-replay.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-replay-${name}-${Date.now()}-${Math.random()}.db`);

const generateDates = (count: number, startDate: string): string[] => {
  const start = new Date(`${startDate}T00:00:00.000Z`);
  const out: string[] = [];
  for (let index = 0; index < count; index += 1) {
    out.push(new Date(start.getTime() + index * 24 * 60 * 60 * 1000).toISOString().slice(0, 10));
  }
  return out;
};

const seedTicker = (params: {
  dbPath: string;
  ticker: string;
  sector: string;
  dates: string[];
  basePrice: number;
  drift: number;
  phase: number;
}) => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  db.prepare(
    `INSERT OR IGNORE INTO instruments (ticker, sector, updated_at)
     VALUES (?, ?, ?)`,
  ).run(params.ticker, params.sector, now);
  const instrument = db.prepare(`SELECT id FROM instruments WHERE ticker=?`).get(params.ticker) as {
    id: number;
  };
  let price = params.basePrice;
  const stmt = db.prepare(
    `INSERT OR REPLACE INTO prices
      (instrument_id, date, open, high, low, close, volume, source, fetched_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  params.dates.forEach((date, index) => {
    const cyclical = 0.004 * Math.sin((index + params.phase) / 8);
    const ret = params.drift + cyclical;
    price = Math.max(5, price * (1 + ret));
    const volume = 900_000 + (index % 17) * 18_000 + params.phase * 2_000;
    stmt.run(
      instrument.id,
      date,
      price * 0.995,
      price * 1.01,
      price * 0.99,
      price,
      volume,
      "test",
      now + index,
    );
  });
};

describe("portfolio replay", () => {
  it("produces replay windows, calibration metrics, and persists eval run", () => {
    const dbPath = testDbPath("baseline");
    const dates = generateDates(340, "2024-01-01");
    seedTicker({
      dbPath,
      ticker: "AAPL",
      sector: "Technology",
      dates,
      basePrice: 120,
      drift: 0.0011,
      phase: 1,
    });
    seedTicker({
      dbPath,
      ticker: "MSFT",
      sector: "Technology",
      dates,
      basePrice: 90,
      drift: 0.0007,
      phase: 4,
    });
    seedTicker({
      dbPath,
      ticker: "XOM",
      sector: "Energy",
      dates,
      basePrice: 70,
      drift: -0.0002,
      phase: 7,
    });

    const result = computePortfolioReplay({
      tickers: ["AAPL", "MSFT", "XOM"],
      dbPath,
      startDate: "2024-07-01",
      endDate: "2024-12-10",
      rebalanceEveryDays: 21,
      horizonDays: 21,
      lookbackSignalDays: 84,
      lookbackCorrelationDays: 126,
      constraints: {
        maxGrossExposurePct: 18,
        maxNetExposurePct: 8,
        maxTurnoverPct: 10,
        portfolioNavUsd: 150_000_000,
      },
    });

    expect(result.summary.sampleCount).toBeGreaterThanOrEqual(4);
    expect(result.windows.length).toBe(result.summary.sampleCount);
    expect(result.summary.maePct).toBeGreaterThanOrEqual(0);
    expect(result.summary.directionalAccuracy).toBeGreaterThanOrEqual(0);
    expect(result.summary.directionalAccuracy).toBeLessThanOrEqual(1);
    expect(result.windows.some((window) => window.transactionCostPct > 0)).toBe(true);
    expect(result.windows.some((window) => window.turnoverPct > 0)).toBe(true);
    expect(result.evaluationChecks.length).toBe(4);
    expect(result.latestOptimization?.positions.length ?? 0).toBeGreaterThan(0);

    const db = openResearchDb(dbPath);
    const evalRow = db
      .prepare(
        `SELECT run_type, score, passed, total
       FROM eval_runs
       ORDER BY id DESC
       LIMIT 1`,
      )
      .get() as { run_type: string; score: number; passed: number; total: number };
    expect(evalRow.run_type).toBe("portfolio_replay");
    expect(evalRow.total).toBe(4);
    expect(evalRow.score).toBeGreaterThanOrEqual(0);
  });

  it("fails with insufficient history window", () => {
    const dbPath = testDbPath("insufficient");
    const dates = generateDates(90, "2025-01-01");
    seedTicker({
      dbPath,
      ticker: "AAPL",
      sector: "Technology",
      dates,
      basePrice: 120,
      drift: 0.001,
      phase: 2,
    });
    seedTicker({
      dbPath,
      ticker: "MSFT",
      sector: "Technology",
      dates,
      basePrice: 90,
      drift: 0.0008,
      phase: 5,
    });
    expect(() =>
      computePortfolioReplay({
        tickers: ["AAPL", "MSFT"],
        dbPath,
        lookbackSignalDays: 84,
        lookbackCorrelationDays: 126,
      }),
    ).toThrowError();
  });
});
