import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ingestExternalResearchDocument } from "./external-research.js";
import {
  buildExternalResearchStructuredReport,
  getLatestExternalResearchStructuredReport,
  storeExternalResearchStructuredReport,
} from "./external-research-report.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-external-research-report-${name}-${Date.now()}-${Math.random()}.db`);

describe("external research structured reports", () => {
  it("builds and stores a source-backed ticker memo from external research evidence", () => {
    const dbPath = testDbPath("memo");

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "cooptaylor1@gmail.com",
      title: "NVDA earnings setup",
      subject: "RESEARCH NVDA earnings setup",
      ticker: "NVDA",
      content: [
        "NVDA earnings setup looks favorable because revenue growth could sustain 24% as supply constraints ease and pricing remains disciplined.",
        "Gross margin could hold around 72% while operating margin remains above 45% if mix stays tilted toward high-end accelerators.",
        "Management guidance and capex discipline suggest capex could stay near $2.5 billion while demand remains strong.",
      ].join(" "),
      url: "https://example.com/research/nvda-earnings-setup",
      publishedAt: "2026-03-01T10:00:00Z",
    });

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "semianalysis",
      sender: "digest@semianalysis.com",
      title: "NVDA competitive risk update",
      subject: "NVDA competitive risk update",
      ticker: "NVDA",
      content: [
        "Competition risk is rising because custom silicon programs could pressure pricing and stretch valuation assumptions.",
        "Guidance remains constructive, but investors should monitor whether gross margin slips below 70% as supply normalizes.",
      ].join(" "),
      url: "https://example.com/research/nvda-risk-update",
      publishedAt: "2026-03-03T12:00:00Z",
    });

    const report = buildExternalResearchStructuredReport({
      ticker: "NVDA",
      dbPath,
      lookbackDays: 90,
    });
    expect(report.ticker).toBe("NVDA");
    expect(report.sources.length).toBeGreaterThanOrEqual(2);
    expect(report.evidence.length).toBeGreaterThan(0);
    expect(report.whatChanged.length).toBeGreaterThan(0);
    expect(report.bullCase.length).toBeGreaterThan(0);
    expect(report.bearCase.length).toBeGreaterThan(0);
    expect(report.markdown).toContain("## What Changed");
    expect(report.markdown).toContain("## Sources");
    expect(report.confidence).toBeGreaterThan(0);

    const stored = storeExternalResearchStructuredReport({ report, dbPath });
    expect(stored.id).toBeGreaterThan(0);

    const latest = getLatestExternalResearchStructuredReport({ ticker: "NVDA", dbPath });
    expect(latest?.id).toBe(stored.id);
    expect(latest?.summary).toContain("NVDA");
    expect(latest?.confidenceRationale).toContain("providers=");
  });
});
