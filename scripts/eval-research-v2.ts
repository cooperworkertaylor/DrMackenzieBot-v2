import fs from "node:fs";
import path from "node:path";

import { scoreV2Report } from "../src/v2/eval/score.js";

type Rubric = {
  thresholds?: { min_total_score?: number };
};

const readJson = (p: string): unknown =>
  JSON.parse(fs.readFileSync(path.resolve(process.cwd(), p), "utf8")) as unknown;

const rubric = readJson("eval/rubric.v2.json") as Rubric;
const threshold = rubric.thresholds?.min_total_score ?? 70;

const examples: Array<{ kind: "company" | "theme"; path: string }> = [
  { kind: "company", path: "examples/v2_company_report.json" },
  { kind: "theme", path: "examples/v2_theme_report.json" },
];

let failed = false;

for (const ex of examples) {
  const report = readJson(ex.path);
  const scored = scoreV2Report({ kind: ex.kind, report });
  const errors = scored.issues.filter((i) => i.severity === "error");
  const ok = scored.gate_passed && scored.total >= threshold && errors.length === 0;
  if (!ok) failed = true;

  // Minimal, grep-friendly output.
  // Example: v2_eval kind=company passed=1 total=100 structure=20 sourcing=20 ...
  // If failures exist, print the first few issue codes.
  const subs = scored.subscores;
  const issueCodes = scored.issues.map((i) => i.code).slice(0, 12);
  console.log(
    [
      "v2_eval",
      `kind=${ex.kind}`,
      `passed=${ok ? 1 : 0}`,
      `total=${scored.total}`,
      `structure=${subs.structure}`,
      `sourcing=${subs.sourcing}`,
      `numeric=${subs.numeric_provenance}`,
      `consistency=${subs.consistency}`,
      `risk=${subs.risk_falsifiers}`,
      `clarity=${subs.clarity}`,
      issueCodes.length ? `issues=${issueCodes.join(",")}` : "",
    ]
      .filter(Boolean)
      .join(" "),
  );
}

if (failed) {
  process.exitCode = 1;
}

