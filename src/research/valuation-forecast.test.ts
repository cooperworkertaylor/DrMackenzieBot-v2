import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import {
  forecastDecisionMetrics,
  recordValuationForecast,
  resolveMatureForecasts,
} from "./valuation.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-research-${name}-${Date.now()}-${Math.random()}.db`);

describe("valuation forecast tracking", () => {
  it("deduplicates open forecasts and resolves matured outcomes", () => {
    const dbPath = testDbPath("valuation-forecast");
    const db = openResearchDb(dbPath);
    const now = Date.now();
    db.prepare(`INSERT INTO instruments (ticker, updated_at) VALUES (?, ?)`).run("AAPL", now);
    const id1 = recordValuationForecast({
      ticker: "AAPL",
      predictedReturn: 0.12,
      startPrice: 100,
      basePriceDate: "2025-01-01",
      horizonDays: 7,
      source: "memo",
      dbPath,
    });
    const id2 = recordValuationForecast({
      ticker: "AAPL",
      predictedReturn: 0.11,
      startPrice: 100,
      basePriceDate: "2025-01-01",
      horizonDays: 7,
      source: "memo",
      dbPath,
    });
    expect(id1).toBe(id2);

    db.prepare(`UPDATE thesis_forecasts SET created_at=? WHERE id=?`).run(
      now - 9 * 86_400_000,
      id1,
    );
    db.prepare(
      `INSERT INTO prices (instrument_id, date, close, source, fetched_at)
       VALUES ((SELECT id FROM instruments WHERE ticker='AAPL'), ?, ?, 'alpha_vantage', ?)`,
    ).run(new Date().toISOString().slice(0, 10), 108, now);

    const resolved = resolveMatureForecasts({ dbPath });
    expect(resolved.resolvedNow).toBe(1);

    const metrics = forecastDecisionMetrics({ dbPath });
    expect(metrics.count).toBe(1);
    expect(metrics.mae).toBeCloseTo(0.04, 6);
    expect(metrics.directionalAccuracy).toBe(1);
  });
});
