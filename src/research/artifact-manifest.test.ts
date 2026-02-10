import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { writeArtifactManifest } from "./artifact-manifest.js";

describe("artifact manifest", () => {
  it("writes a manifest and detects unchanged outputs", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-artifact-"));
    const outPath = path.join(tmpDir, "report.md");
    const markdown = [
      "# Title",
      "",
      "## E. Exhibits",
      "### Exhibit 1: Example",
      "Takeaway: Test.",
      "",
    ].join("\n");

    const first = await writeArtifactManifest({
      kind: "memo",
      outPath,
      markdown,
      metrics: { claims: 7, citations: 14 },
      gate: { gateName: "test", score: 0.9, passed: true },
    });
    expect(first.manifestPath).toBe(`${outPath}.artifact.json`);
    expect(first.manifest.sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(first.manifest.unchangedFromPrevious).toBe(false);
    expect(first.manifest.metrics.exhibitCount).toBe(1);
    expect(first.manifest.metrics.takeawayCount).toBe(1);

    const second = await writeArtifactManifest({
      kind: "memo",
      outPath,
      markdown,
    });
    expect(second.manifest.unchangedFromPrevious).toBe(true);
    expect(second.manifest.previousSha256).toBe(first.manifest.sha256);
  });

  it("can track unchanged artifacts across versioned out paths via a shared series manifest", async () => {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-artifact-series-"));
    const seriesManifestPath = path.join(tmpDir, "artifact.json");
    const markdown = [
      "# Title",
      "",
      "## E. Exhibits",
      "### Exhibit 1: Example",
      "Takeaway: Test.",
      "",
      "## K. Appendix",
      "### Source List (timestamped)",
      "- C1: source=filings ref=0000000000 date=2026-02-09 host=sec.gov | https://www.sec.gov/",
      "",
    ].join("\n");

    const firstOut = path.join(tmpDir, "report.v1.md");
    const first = await writeArtifactManifest({
      kind: "memo",
      outPath: firstOut,
      markdown,
      seriesKey: "report",
      seriesManifestPath,
    });
    expect(first.manifest.unchangedFromPrevious).toBe(false);

    const secondOut = path.join(tmpDir, "report.v2.md");
    const second = await writeArtifactManifest({
      kind: "memo",
      outPath: secondOut,
      markdown,
      seriesKey: "report",
      seriesManifestPath,
    });
    expect(second.manifest.unchangedFromPrevious).toBe(true);
    expect(second.manifest.previousSha256).toBe(first.manifest.sha256);

    const seriesRaw = await fs.readFile(seriesManifestPath, "utf8");
    const seriesParsed = JSON.parse(seriesRaw) as { outPath?: string };
    expect(seriesParsed.outPath).toBe(path.resolve(secondOut));
  });
});
