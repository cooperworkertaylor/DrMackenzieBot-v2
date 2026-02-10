import type { QualityGateResult, QualityIssue, ReportKindV2 } from "./types.js";
import { validateReportJsonSchema } from "./schema-validator.js";
import { validateCitationCoverage } from "./validators/citation-validator.js";
import { validateConsistency } from "./validators/consistency-validator.js";
import { validateNumericProvenance } from "./validators/numeric-provenance-validator.js";
import { validateSectionCompleteness } from "./validators/section-completeness-validator.js";
import { validateStyle } from "./validators/style-validator.js";

export function runV2QualityGate(params: {
  kind: ReportKindV2;
  report: unknown;
}): QualityGateResult {
  const issues: QualityIssue[] = [];

  const schemaRes = validateReportJsonSchema({ kind: params.kind, report: params.report });
  if (!schemaRes.valid) {
    schemaRes.errors.forEach((err) => {
      issues.push({
        severity: "error",
        code: "schema_invalid",
        message: err,
        fix: "Fix the JSON structure to satisfy the v2 schema before attempting repair.",
      });
    });
    return { passed: false, issues };
  }

  issues.push(...validateSectionCompleteness({ kind: params.kind, report: params.report }));
  issues.push(...validateStyle(params.report));
  issues.push(...validateCitationCoverage(params.report));
  issues.push(...validateNumericProvenance(params.report));
  issues.push(...validateConsistency({ kind: params.kind, report: params.report }));

  const passed = issues.every((issue) => issue.severity !== "error");
  return { passed, issues };
}
