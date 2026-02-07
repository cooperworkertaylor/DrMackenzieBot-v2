import { describe, expect, it } from "vitest";
import { enforceInstitutionalOutputGateV3 } from "./institutional-output-gate.js";

const freshDate = new Date(Date.now() - 14 * 86_400_000).toISOString().slice(0, 10);

const buildThemeMarkdown = () =>
  [
    "# Institutional Theme Research",
    "",
    "## A. Cover",
    "- Theme: AI Infrastructure",
    "",
    "## B. Exec Summary",
    "- Why now: capex cycle is accelerating while supply bottlenecks persist.",
    "",
    "## C. The Story",
    "What changed structurally is that compute demand has moved from experimental to production-critical workloads. ".repeat(
      18,
    ),
    "",
    "## D. Mechanics of the Theme",
    "- Value chain and margin capture by subsector.",
    "",
    "## E. Exhibits",
    ...Array.from({ length: 8 }, (_, idx) =>
      [
        `### Exhibit ${idx + 1}: Exhibit ${idx + 1}`,
        "- Data line.",
        "Takeaway: This exhibit is directly tied to deployment decisions.",
        "",
      ].join("\n"),
    ),
    "## F. Winners, Losers, and Second-Order Effects",
    "- Market belief: consensus expects durable spend acceleration.",
    "- Variant view: consensus underprices margin dispersion in networking and memory.",
    "",
    "## G. Capital Allocation Playbook",
    "- Sizing tiers and trigger-based deployment.",
    "",
    "## H. Risks, Falsifiers, and Monitoring Dashboard",
    "- Falsifier: margin conversion fails.",
    "",
    "## I. Timeline & Checkpoints",
    "- 30/90/180/365 checkpoints.",
    "",
    "## J. Appendix",
    "- Source List (timestamped)",
    "- date=2026-02-01 host=sec.gov",
    "- Snapshot methodology notes",
  ].join("\n");

describe("institutional output gate v3", () => {
  it("fails hard on debug leakage", () => {
    const report = enforceInstitutionalOutputGateV3({
      kind: "theme_report",
      artifactId: "theme:test",
      markdown: `${buildThemeMarkdown()}\nrun_id=123 quality_refinement_retry`,
      minScore: 0.82,
      sources: [{ sourceTable: "filings", date: freshDate, url: "https://sec.gov/x" }],
    });
    expect(report.evaluation.hardFails).toContain("debug_or_telemetry_leak");
    expect(report.evaluation.passed).toBe(false);
  });

  it("passes a complete thematic schema with fresh primary evidence", () => {
    const report = enforceInstitutionalOutputGateV3({
      kind: "theme_report",
      artifactId: "theme:pass",
      markdown: buildThemeMarkdown(),
      minScore: 0.82,
      sources: [
        { sourceTable: "filings", date: freshDate, url: "https://sec.gov/a" },
        { sourceTable: "transcripts", date: freshDate, url: "https://example.com/b" },
        { sourceTable: "earnings_expectations", date: freshDate, url: "https://example.com/c" },
      ],
    });
    expect(report.evaluation.passed).toBe(true);
    expect(report.evaluation.score).toBeGreaterThanOrEqual(0.82);
    expect(report.evaluation.hardFails.length).toBe(0);
  });

  it("repairs missing exhibit takeaways and freshness justification", () => {
    const weak = [
      "# Weak",
      "## A. Cover",
      "## B. Exec Summary",
      "## C. The Story",
      "Short story.",
      "## E. Exhibits",
      "### Exhibit 1: Thin",
      "- Bare line",
      "## G. Capital Allocation Playbook",
      "- placeholder",
      "## J. Appendix",
      "- Source list",
    ].join("\n");

    const report = enforceInstitutionalOutputGateV3({
      kind: "theme_report",
      artifactId: "theme:repair",
      markdown: weak,
      minScore: 0.82,
      sources: [{ sourceTable: "filings", date: "2020-01-01", url: "https://sec.gov/z" }],
    });

    expect(report.repairs.length).toBeGreaterThan(0);
    expect(report.markdown).toContain("Takeaway:");
    expect(report.markdown).toContain("Freshness Justification:");
  });

  it("enforces single-name exhibit minimums", () => {
    const memo = [
      "# Institutional Single-Name Research: TEST",
      "## A. Cover",
      "## B. Exec Summary",
      "## C. The Story",
      "Why now is clear and variant is explicit. ".repeat(20),
      "## D. Business Model & Value Drivers",
      "## E. Variant View / Debate",
      "- Market belief: optimistic",
      "- Variant view: conservative",
      "## F. Valuation",
      "## G. Exhibits",
      "### Exhibit 1: Only one",
      "Takeaway: too few exhibits.",
      "## H. Risks / Kill Shots + Monitoring",
      "## I. Catalysts & Timeline",
      "## J. Positioning & Risk Controls",
      "## K. Appendix",
      "- Source List (timestamped)",
      "- Snapshot",
    ].join("\n");

    const report = enforceInstitutionalOutputGateV3({
      kind: "memo",
      artifactId: "memo:test",
      markdown: memo,
      minScore: 0.82,
      sources: [{ sourceTable: "filings", date: freshDate, url: "https://sec.gov/memo" }],
    });

    expect(report.evaluation.hardFails).not.toContain("single_name_exhibit_minimum");
    expect(report.markdown.match(/^###\s+Exhibit\s+\d+:/gim)?.length ?? 0).toBeGreaterThanOrEqual(
      6,
    );
  });
});
