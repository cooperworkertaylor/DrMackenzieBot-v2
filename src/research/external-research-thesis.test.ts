import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ingestExternalResearchDocument } from "./external-research.js";
import { getLatestExternalResearchStructuredReport } from "./external-research-report.js";
import { getLatestExternalResearchThesis } from "./external-research-thesis.js";
import { openResearchDb } from "./db.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-external-research-thesis-${name}-${Date.now()}-${Math.random()}.db`);

describe("external research thesis engine", () => {
  it("versions theses, records diffs, and raises thesis-break alerts on contradiction", () => {
    const dbPath = testDbPath("break");

    const first = ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "cooptaylor1@gmail.com",
      title: "NVDA demand strength setup",
      subject: "RESEARCH NVDA demand strength setup",
      ticker: "NVDA",
      content: [
        "NVDA demand strength remains favorable because pricing discipline and demand strength are holding across accelerator programs.",
        "Pricing discipline and demand strength support revenue growth while guidance remains constructive.",
        "Gross margin could hold around 72% while operating margin remains above 45% if mix stays tilted toward high-end accelerators.",
      ].join(" "),
      url: "https://example.com/research/nvda-demand-strength",
      publishedAt: "2026-03-01T10:00:00Z",
    });

    expect(first.thesisId).toBeGreaterThan(0);
    expect(first.thesisDiffId).toBeGreaterThan(0);
    expect(first.thesisAlertId).toBeUndefined();

    const second = ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "semianalysis",
      sender: "digest@semianalysis.com",
      title: "NVDA pricing discipline breaks",
      subject: "NVDA pricing discipline breaks",
      ticker: "NVDA",
      content: [
        "Pricing discipline and demand strength are weakening, which contradicts the prior pricing discipline and demand strength assumptions.",
        "Competition risk is rising and margin pressure is increasing as custom silicon programs pressure valuation and guidance.",
        "Investors now face uncertainty because pricing discipline and demand strength may no longer hold in the next quarter.",
      ].join(" "),
      url: "https://example.com/research/nvda-pricing-breaks",
      publishedAt: "2026-03-04T11:00:00Z",
    });

    expect(second.thesisId).toBeGreaterThan(first.thesisId ?? 0);
    expect(second.thesisDiffId).toBeGreaterThan(first.thesisDiffId ?? 0);
    expect(second.thesisAlertId).toBeGreaterThan(0);
    const latestThesis = getLatestExternalResearchThesis({ ticker: "NVDA", dbPath });
    expect(latestThesis?.versionNumber).toBe(2);
    expect(latestThesis?.bearCase.length).toBeGreaterThan(0);

    const latestReport = getLatestExternalResearchStructuredReport({ ticker: "NVDA", dbPath });
    expect(latestReport?.diffFromPrevious?.newBearCase.length).toBeGreaterThan(0);
    expect(latestReport?.whatChanged.some((item) => item.startsWith("New risk:"))).toBe(true);

    const db = openResearchDb(dbPath);
    const thesisCount = db.prepare(`SELECT COUNT(*) AS count FROM research_theses`).get() as {
      count: number;
    };
    const diffRow = db.prepare(
      `SELECT thesis_break AS thesisBreak FROM research_thesis_diffs ORDER BY id DESC LIMIT 1`,
    ).get() as { thesisBreak: number };
    const alertRow = db.prepare(
      `SELECT alert_type AS alertType FROM thesis_alerts ORDER BY id DESC LIMIT 1`,
    ).get() as { alertType?: string } | undefined;

    expect(thesisCount.count).toBe(2);
    expect(diffRow.thesisBreak).toBe(1);
    expect(alertRow?.alertType).toBe("external-thesis-break");
  });
});
