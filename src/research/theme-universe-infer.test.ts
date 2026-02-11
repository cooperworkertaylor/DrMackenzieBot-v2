import crypto from "node:crypto";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import { inferThemeUniverseFromDb, normalizeThemeTickerUniverse } from "./theme-universe-infer.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-theme-universe-${name}-${Date.now()}-${Math.random()}.db`);

const seedInstrument = (params: {
  dbPath: string;
  ticker: string;
  name: string;
  sector?: string;
  industry?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  db.prepare(
    `INSERT OR REPLACE INTO instruments (ticker, name, sector, industry, updated_at)
     VALUES (?, ?, ?, ?, ?)`,
  ).run(
    params.ticker.toUpperCase(),
    params.name,
    params.sector ?? "Technology",
    params.industry ?? "Semiconductors",
    Date.now(),
  );
};

const seedExternalDoc = (params: {
  dbPath: string;
  title: string;
  subject: string;
  content: string;
  url: string;
}) => {
  const db = openResearchDb(params.dbPath);
  const contentHash = crypto.createHash("sha256").update(params.content).digest("hex");
  db.prepare(
    `INSERT INTO external_documents
      (source_type, provider, external_id, sender, title, subject, url, ticker, published_at, received_at, content, content_hash, metadata, fetched_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    "newsletter",
    "test",
    "",
    "analyst@test.local",
    params.title,
    params.subject,
    params.url,
    "",
    "2026-02-10",
    "2026-02-10",
    params.content,
    contentHash,
    "{}",
    Date.now(),
  );
};

describe("theme universe inference normalization", () => {
  it("canonicalizes aliases and filters non-ticker noise", () => {
    const dbPath = testDbPath("normalize");
    seedInstrument({ dbPath, ticker: "TSM", name: "Taiwan Semiconductor Manufacturing Co." });
    seedInstrument({ dbPath, ticker: "CIEN", name: "Ciena Corp" });
    seedInstrument({ dbPath, ticker: "LITE", name: "Lumentum Holdings Inc" });

    const normalized = normalizeThemeTickerUniverse({
      dbPath,
      tickers: ["TSMC", "CMOS", "CIEN", "cien", "LITE"],
    });

    expect(normalized.tickers).toEqual(["TSM", "CIEN", "LITE"]);
    expect(normalized.tickers).not.toContain("CMOS");
    expect(normalized.tickers).not.toContain("TSMC");
  });

  it("drops alias/noise tokens during DB inference", () => {
    const dbPath = testDbPath("inference");
    seedInstrument({ dbPath, ticker: "TSM", name: "Taiwan Semiconductor Manufacturing Co." });
    seedInstrument({ dbPath, ticker: "CIEN", name: "Ciena Corp" });
    seedExternalDoc({
      dbPath,
      title: "Optical networking stack update",
      subject: "optical networking weekly",
      content:
        "Optical networking recap: TSMC remains a foundry winner while CMOS sensors get discussed alongside CIEN systems.",
      url: "https://example.test/optical-networking",
    });

    const inferred = inferThemeUniverseFromDb({
      dbPath,
      theme: "optical networking",
      maxDocs: 20,
      maxTickers: 20,
    });

    expect(inferred.inferred_tickers).toContain("TSM");
    expect(inferred.inferred_tickers).toContain("CIEN");
    expect(inferred.inferred_tickers).not.toContain("TSMC");
    expect(inferred.inferred_tickers).not.toContain("CMOS");
  });
});
