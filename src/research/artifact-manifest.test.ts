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
});
