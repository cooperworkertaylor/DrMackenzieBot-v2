import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { buildAgentResearchContext, extractTickersFromText } from "./agent-research-context.js";
import { openResearchDb } from "./db.js";

describe("agent research context", () => {
  it("extracts tickers conservatively", () => {
    expect(extractTickersFromText("AI is changing software")).toEqual([]);
    expect(extractTickersFromText("ticker: NU")).toEqual(["NU"]);
    expect(extractTickersFromText("Is $NVDA a 10-year compounder?")).toEqual(["NVDA"]);
  });

  it("builds a bounded context block from the research DB (filings)", async () => {
    const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "openclaw-agent-research-"));
    const dbPath = path.join(tmpRoot, "research.db");
    const db = openResearchDb(dbPath);
    try {
      const now = Date.now();
      db.prepare("insert into instruments (ticker, name, updated_at) values (?, ?, ?);").run(
        "NU",
        "Nubank",
        now,
      );
      const row = db.prepare("select id from instruments where ticker=?;").get("NU") as
        | { id: number }
        | undefined;
      expect(row?.id).toBeTypeOf("number");
      const instrumentId = row?.id ?? 1;

      db.prepare(
        "insert into filings (instrument_id, cik, accession, form, filed, url, fetched_at) values (?, ?, ?, ?, ?, ?, ?);",
      ).run(
        instrumentId,
        "0001691493",
        "0000000000-00-000000",
        "20-F",
        "2024-04-19",
        "https://www.sec.gov/Archives/edgar/data/1691493/000129281424001464/0001292814-24-001464-index.html",
        now,
      );
      const filing = db
        .prepare("select id from filings where accession=?;")
        .get("0000000000-00-000000") as { id: number } | undefined;
      expect(filing?.id).toBeTypeOf("number");
      const filingId = filing?.id ?? 1;

      db.prepare(
        "insert into chunks (source_table, ref_id, seq, text, pending_embedding, metadata) values (?, ?, ?, ?, ?, ?);",
      ).run(
        "filings",
        filingId,
        0,
        "Revenue grew year-over-year, and credit losses normalized in the quarter.",
        0,
        JSON.stringify({ filed: "2024-04-19", form: "20-F" }),
      );
    } finally {
      db.close();
    }

    const result = await buildAgentResearchContext({
      prompt: "Is NU a 10-year compounder?",
      dbPath,
      limit: 4,
    });
    expect(result).not.toBeNull();
    expect(result?.context).toContain("<research-context>");
    expect(result?.context).toContain("Tickers: NU");
    expect(result?.context).toContain("source_table=filings");
    expect(result?.context).toContain("sec.gov");
  });
});
