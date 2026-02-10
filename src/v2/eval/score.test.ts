import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { scoreV2Report } from "./score.js";

const readJson = (p: string): unknown =>
  JSON.parse(fs.readFileSync(path.resolve(process.cwd(), p), "utf8")) as unknown;

describe("v2 eval harness", () => {
  it("scores the v2 company example above the threshold and with no gate errors", () => {
    const report = readJson("examples/v2_company_report.json");
    const scored = scoreV2Report({ kind: "company", report });
    expect(scored.gate_passed).toBe(true);
    expect(scored.issues.filter((i) => i.severity === "error")).toEqual([]);
    expect(scored.total).toBeGreaterThanOrEqual(70);
  });

  it("scores the v2 theme example above the threshold and with no gate errors", () => {
    const report = readJson("examples/v2_theme_report.json");
    const scored = scoreV2Report({ kind: "theme", report });
    expect(scored.gate_passed).toBe(true);
    expect(scored.issues.filter((i) => i.severity === "error")).toEqual([]);
    expect(scored.total).toBeGreaterThanOrEqual(70);
  });
});
