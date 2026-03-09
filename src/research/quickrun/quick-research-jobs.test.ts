import { describe, expect, it } from "vitest";
import { buildQuickResearchTelegramSummary } from "./quick-research-jobs.js";

describe("buildQuickResearchTelegramSummary", () => {
  it("extracts an investor digest from the v2 report shape", () => {
    const text = buildQuickResearchTelegramSummary({
      kind: "company",
      subject: "NVDA",
      jobId: "job-123",
      runId: "run-456",
      builtAtEt: "2026-03-09 14:40 ET",
      pdfBytes: 1024,
      sha256: "abc123",
      report: {
        sections: [
          {
            key: "executive_summary",
            blocks: [
              {
                tag: "INTERPRETATION",
                text: "Demand remains strong as enterprise AI budgets expand.",
              },
            ],
          },
          {
            key: "thesis",
            blocks: [
              {
                tag: "FACT",
                text: "Gross margin could hold near 72% if pricing stays disciplined.",
              },
            ],
          },
          {
            key: "risks_premortem",
            blocks: [
              { tag: "INTERPRETATION", text: "Custom silicon competition could pressure pricing." },
            ],
          },
        ],
        appendix: {
          whats_missing: ["Need one primary filing to validate the margin bridge."],
        },
      },
    });

    expect(text).toContain("Company memo ready: NVDA");
    expect(text).toContain("Top line");
    expect(text).toContain("Demand remains strong as enterprise AI budgets expand.");
    expect(text).toContain("Thesis / variant");
    expect(text).toContain("Gross margin could hold near 72% if pricing stays disciplined.");
    expect(text).toContain("Risks / change-mind triggers");
    expect(text).toContain("Custom silicon competition could pressure pricing.");
    expect(text).toContain("Missing / next diligence");
    expect(text).toContain("Need one primary filing to validate the margin bridge.");
  });
});
