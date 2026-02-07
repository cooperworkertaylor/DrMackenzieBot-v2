import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { enforceInstitutionalOutputGateV3 } from "./institutional-output-gate.js";

const repoRoot = process.cwd();
const freshDate = new Date(Date.now() - 21 * 86_400_000).toISOString().slice(0, 10);

describe("institutional sample reports", () => {
  it("theme sample passes v3 output gate", () => {
    const markdown = fs.readFileSync(
      path.join(repoRoot, "research-corpus", "samples", "theme-ai-infrastructure.sample.md"),
      "utf8",
    );
    const result = enforceInstitutionalOutputGateV3({
      kind: "theme_report",
      artifactId: "sample:theme",
      markdown,
      minScore: 0.82,
      sources: [
        { sourceTable: "filings", date: freshDate, url: "https://sec.gov/theme" },
        { sourceTable: "transcripts", date: freshDate, url: "https://example.com/theme" },
        {
          sourceTable: "earnings_expectations",
          date: freshDate,
          url: "https://example.com/theme-expect",
        },
      ],
    });
    expect(result.evaluation.passed).toBe(true);
  });

  it("single-name sample passes v3 output gate", () => {
    const markdown = fs.readFileSync(
      path.join(repoRoot, "research-corpus", "samples", "single-name-nu.sample.md"),
      "utf8",
    );
    const result = enforceInstitutionalOutputGateV3({
      kind: "memo",
      artifactId: "sample:memo",
      markdown,
      minScore: 0.82,
      sources: [
        { sourceTable: "filings", date: freshDate, url: "https://sec.gov/nu" },
        { sourceTable: "transcripts", date: freshDate, url: "https://example.com/nu" },
        {
          sourceTable: "earnings_expectations",
          date: freshDate,
          url: "https://example.com/nu-expect",
        },
      ],
    });
    expect(result.evaluation.passed).toBe(true);
  });
});
