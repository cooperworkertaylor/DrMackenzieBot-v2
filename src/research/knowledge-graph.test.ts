import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import { buildTickerPointInTimeGraph, getTickerPointInTimeSnapshot } from "./knowledge-graph.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-knowledge-graph-${name}-${Date.now()}-${Math.random()}.db`);

describe("point-in-time knowledge graph", () => {
  it("builds ticker graph from source tables and returns stable as-of snapshot", () => {
    const dbPath = testDbPath("build");
    const db = openResearchDb(dbPath);
    const now = Date.now();

    db.prepare(
      `INSERT INTO instruments (ticker, cik, name, updated_at)
       VALUES (?, ?, ?, ?)`,
    ).run("MSFT", "0000789019", "Microsoft Corp", now);
    const instrument = db.prepare(`SELECT id FROM instruments WHERE ticker=?`).get("MSFT") as {
      id: number;
    };

    db.prepare(
      `INSERT INTO fundamental_facts (
         instrument_id, ticker, cik, entity_name, taxonomy, concept, label, unit, value, as_of_date,
         period_start, period_end, filing_date, accepted_at, accession, accession_nodash, form, frame,
         fiscal_year, fiscal_period, revision_number, is_latest, source, source_url, fetched_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      instrument.id,
      "MSFT",
      "0000789019",
      "Microsoft Corp",
      "us-gaap",
      "Revenues",
      "Revenues",
      "USD",
      62000000000,
      "2025-09-30",
      "2025-07-01",
      "2025-09-30",
      "2025-10-29",
      "2025-10-29T16:05:00.000Z",
      "0000950170-25-123456",
      "000095017025123456",
      "10-Q",
      "CY2025Q3",
      2025,
      "Q3",
      1,
      1,
      "sec_companyfacts",
      "https://www.sec.gov/Archives/edgar/data/789019/000095017025123456/msft-20250930x10q.htm",
      now,
    );

    db.prepare(
      `INSERT INTO earnings_expectations (
         instrument_id, ticker, period_type, fiscal_date_ending, reported_date, reported_eps,
         estimated_eps, surprise, surprise_pct, report_time, source, source_url, fetched_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      instrument.id,
      "MSFT",
      "quarterly",
      "2025-09-30",
      "2025-10-29",
      3.15,
      3.08,
      0.07,
      2.27,
      "post-market",
      "alpha_vantage",
      "https://www.alpha-vantage.co/query?function=EARNINGS&symbol=MSFT",
      now,
    );

    db.prepare(
      `INSERT INTO filings (
         instrument_id, cik, accession, accession_raw, form, is_amendment, filed, accepted_at,
         period_end, as_of_date, title, url, source_url, source, text, filing_hash, fetched_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      instrument.id,
      "0000789019",
      "0000950170-25-123456",
      "000095017025123456",
      "10-Q",
      0,
      "2025-10-29",
      "2025-10-29T16:05:00.000Z",
      "2025-09-30",
      "2025-09-30",
      "Quarterly report",
      "https://www.sec.gov/ixviewer/ix.html?doc=/Archives/edgar/data/789019/000095017025123456/msft-20250930x10q.htm",
      "https://www.sec.gov/Archives/edgar/data/789019/000095017025123456/msft-20250930x10q.htm",
      "sec_edgar",
      "sample filing text",
      "hash-10q",
      now,
    );

    db.prepare(
      `INSERT INTO transcripts (
         instrument_id, event_date, event_type, source, url, title, speakers, content, fetched_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      instrument.id,
      "2025-10-29",
      "earnings_call",
      "manual",
      "https://example.com/msft-q3-2025-transcript",
      "Q3 earnings transcript",
      "Satya Nadella;Amy Hood",
      "Transcript body",
      now,
    );

    db.prepare(
      `INSERT INTO catalysts (
         instrument_id, ticker, category, name, date_window_start, date_window_end, probability,
         impact_bps, confidence, direction, source, status, notes, created_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      instrument.id,
      "MSFT",
      "company",
      "Copilot enterprise ramp",
      "2025-11-01",
      "2026-01-31",
      0.6,
      180,
      0.68,
      "up",
      "manual",
      "open",
      "",
      now,
      now,
    );
    const catalyst = db.prepare(`SELECT id FROM catalysts WHERE ticker=? LIMIT 1`).get("MSFT") as {
      id: number;
    };
    db.prepare(
      `INSERT INTO catalyst_outcomes (
         catalyst_id, occurred, realized_impact_bps, resolved_at, notes
       ) VALUES (?, ?, ?, ?, ?)`,
    ).run(catalyst.id, 1, 210, now, "resolved positive");

    const firstBuild = buildTickerPointInTimeGraph({
      ticker: "MSFT",
      dbPath,
      maxFundamentalFacts: 100,
      maxExpectations: 100,
      maxFilings: 100,
      maxTranscripts: 100,
      maxCatalysts: 100,
    });
    expect(firstBuild.rowsScanned).toBeGreaterThan(0);
    expect(firstBuild.eventsInserted).toBeGreaterThan(0);
    expect(firstBuild.factsInserted).toBeGreaterThan(0);

    const firstEventCount = (
      db
        .prepare(`SELECT COUNT(*) AS count FROM research_events WHERE entity_id=?`)
        .get(firstBuild.entityId) as {
        count: number;
      }
    ).count;
    const firstFactCount = (
      db
        .prepare(`SELECT COUNT(*) AS count FROM research_facts WHERE entity_id=?`)
        .get(firstBuild.entityId) as {
        count: number;
      }
    ).count;
    expect(firstEventCount).toBeGreaterThan(0);
    expect(firstFactCount).toBeGreaterThan(0);

    const secondBuild = buildTickerPointInTimeGraph({
      ticker: "MSFT",
      dbPath,
    });
    expect(secondBuild.rowsScanned).toBeGreaterThan(0);
    const secondEventCount = (
      db
        .prepare(`SELECT COUNT(*) AS count FROM research_events WHERE entity_id=?`)
        .get(firstBuild.entityId) as {
        count: number;
      }
    ).count;
    const secondFactCount = (
      db
        .prepare(`SELECT COUNT(*) AS count FROM research_facts WHERE entity_id=?`)
        .get(firstBuild.entityId) as {
        count: number;
      }
    ).count;
    expect(secondEventCount).toBe(firstEventCount);
    expect(secondFactCount).toBe(firstFactCount);

    const snapshot = getTickerPointInTimeSnapshot({
      ticker: "MSFT",
      asOfDate: "2026-01-15",
      lookbackDays: 800,
      dbPath,
      metricLimit: 100,
    });
    expect(snapshot.entityId).toBe(firstBuild.entityId);
    expect(snapshot.events.length).toBeGreaterThan(0);
    expect(snapshot.facts.length).toBeGreaterThan(0);
    const metricKeys = new Set(snapshot.metrics.map((metric) => metric.metricKey));
    expect(metricKeys.has("us-gaap.revenues")).toBe(true);
    expect(metricKeys.has("earnings.reported_eps")).toBe(true);
    expect(metricKeys.has("catalyst.probability")).toBe(true);
  });
});
