import { describe, expect, it } from "vitest";
import { renderV2ReportMarkdown } from "./report-markdown.js";

describe("renderV2ReportMarkdown", () => {
  it("renders exhibits with explicit Exhibit N headings for PDF diagnostics", () => {
    const markdown = renderV2ReportMarkdown({
      kind: "theme",
      report: {
        version: 2,
        kind: "theme",
        run_id: "run-test",
        generated_at: "2026-02-12T14:45:00Z",
        subject: { theme_name: "optical networking" },
        plan: {},
        sources: [
          {
            id: "S1",
            title: "Example Source",
            publisher: "Example Publisher",
            date_published: "2026-02-10",
            accessed_at: "2026-02-12T14:40:00Z",
            url: "https://example.com/source",
            reliability_tier: 2,
            excerpt_or_key_points: ["x"],
            tags: ["theme:optical-networking"],
          },
        ],
        numeric_facts: [],
        sections: [
          {
            key: "executive_summary",
            title: "Executive Summary",
            blocks: [{ tag: "FACT", text: "Evidence-backed setup.", source_ids: ["S1"] }],
          },
        ],
        exhibits: [
          {
            id: "X1",
            title: "Revenue Bridge",
            question: "What drives growth?",
            data_summary: ["Top-line + margin expansion"],
            takeaway: "Growth is concentrated in data-center networking.",
            source_ids: ["S1"],
          },
        ],
        appendix: {
          evidence_table: [
            { claim: "Growth is concentrated", evidence_ids: ["S1"], source_ids: ["S1"] },
          ],
          whats_missing: [],
        },
      },
    });

    expect(markdown).toContain("### Exhibit 1: Revenue Bridge");
    expect(markdown).toContain("- Exhibit ID: X1");
  });
});
