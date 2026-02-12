import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { resetSchemaBundleForTests, validateReportJsonSchema } from "./schema-validator.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../../");

const loadJson = (filePath: string): unknown => {
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw) as unknown;
};

describe("schema-validator schema path resolution", () => {
  it("finds schemas even when cwd is not the repo root", () => {
    const previousCwd = process.cwd();
    try {
      process.chdir("/tmp");
      resetSchemaBundleForTests();
      const report = loadJson(path.join(repoRoot, "examples/v2_theme_report.json"));
      const result = validateReportJsonSchema({ kind: "theme", report });
      expect(result.valid).toBe(true);
    } finally {
      process.chdir(previousCwd);
      resetSchemaBundleForTests();
    }
  });
});
