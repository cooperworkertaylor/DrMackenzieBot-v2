import { describe, expect, it } from "vitest";
import { computePdfDiagnostics, validatePdfDiagnosticsStrict } from "./pdf-diagnostics.js";

describe("pdf-diagnostics", () => {
  it("flags placeholder-only PDFs in strict mode", () => {
    const text = [
      "Agentic Commerce - Updated Report (2026 n 02 n 10)",
      "Exhibits (pre n labels; CSV/queries provided in appendix pass)",
      "Sources (to appear with CSV/queries in appendix)",
      "Risks: regulatory pressure on stablecoin rails.",
    ].join("\n");

    const metrics = computePdfDiagnostics(text);
    const errors = validatePdfDiagnosticsStrict({ metrics });

    expect(metrics.urlCount).toBe(0);
    expect(metrics.exhibitTokenCount).toBe(0);
    expect(metrics.placeholderTokenCount).toBeGreaterThan(0);
    expect(metrics.dashMojibakeDateCount).toBeGreaterThan(0);

    expect(errors).toEqual(
      expect.arrayContaining([
        expect.stringContaining("missing_urls"),
        expect.stringContaining("missing_exhibits"),
        expect.stringContaining("placeholder_language"),
        expect.stringContaining("dash_mojibake_dates"),
      ]),
    );
  });

  it("passes strict mode for a minimally-auditable PDF text", () => {
    const text = [
      "Exhibit 1 Revenue growth",
      "This is a factual statement. [S1]",
      "Sources",
      "- [S1] SEC 10-K | https://www.sec.gov/Archives/edgar/data/0000000000/0000000000-index.html",
    ].join("\n");

    const metrics = computePdfDiagnostics(text);
    const errors = validatePdfDiagnosticsStrict({ metrics });

    expect(errors).toEqual([]);
  });
});
