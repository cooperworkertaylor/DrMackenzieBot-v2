import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import { monitorTicker } from "./monitor.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-research-${name}-${Date.now()}-${Math.random()}.db`);

const isoDay = (daysAgo: number) => {
  const ts = Date.now() - daysAgo * 86_400_000;
  return new Date(ts).toISOString().slice(0, 10);
};

describe("thesis monitor", () => {
  it("creates alerts for deteriorating revisions and weak price regime", () => {
    const dbPath = testDbPath("monitor");
    const db = openResearchDb(dbPath);
    const now = Date.now();
    db.prepare(`INSERT INTO instruments (ticker, updated_at) VALUES (?, ?)`).run("AAPL", now);
    const instrument = db.prepare(`SELECT id FROM instruments WHERE ticker=?`).get("AAPL") as {
      id: number;
    };

    const expectationsInsert = db.prepare(
      `INSERT INTO earnings_expectations (
         instrument_id, ticker, period_type, fiscal_date_ending, reported_date,
         estimated_eps, surprise_pct, source, fetched_at
       ) VALUES (?, ?, 'quarterly', ?, ?, ?, ?, 'alpha_vantage', ?)`,
    );
    const estimated = [0.92, 0.95, 1.0, 1.25, 1.3, 1.35];
    const surprises = [-12, -9, -7, -2, 1, 2];
    for (let i = 0; i < estimated.length; i += 1) {
      expectationsInsert.run(
        instrument.id,
        "AAPL",
        isoDay(i * 90 + 10),
        isoDay(i * 90),
        estimated[i],
        surprises[i],
        now + i,
      );
    }

    const priceInsert = db.prepare(
      `INSERT INTO prices (instrument_id, date, close, source, fetched_at)
       VALUES (?, ?, ?, 'alpha_vantage', ?)`,
    );
    for (let i = 0; i < 80; i += 1) {
      priceInsert.run(instrument.id, isoDay(i), 100 + i, now + i);
    }

    const filingInsert = db.prepare(
      `INSERT INTO filings (
         instrument_id, cik, accession, form, filed, text, fetched_at
       ) VALUES (?, '0000320193', ?, '10-Q', ?, ?, ?)`,
    );
    filingInsert.run(
      instrument.id,
      "0000320193-26-000001",
      isoDay(2),
      `${"risk adverse uncertainty ".repeat(40)} management discussion`,
      now + 1,
    );
    filingInsert.run(
      instrument.id,
      "0000320193-25-000001",
      isoDay(95),
      `${"operations performance stability ".repeat(40)} risk management discussion`,
      now,
    );

    const result = monitorTicker({ ticker: "AAPL", dbPath });
    const alertTypes = new Set(result.alerts.map((alert) => alert.alertType));

    expect(result.persisted).toBe(result.alerts.length);
    expect(result.alerts.length).toBeGreaterThanOrEqual(3);
    expect(alertTypes.has("estimate-revision")).toBe(true);
    expect(alertTypes.has("earnings-surprise")).toBe(true);
    expect(alertTypes.has("price-regime")).toBe(true);
    expect(alertTypes.has("filing-risk-language")).toBe(true);
  });
});
