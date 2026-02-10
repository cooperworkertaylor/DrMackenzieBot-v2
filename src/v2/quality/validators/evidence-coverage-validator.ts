import type { EvidenceItem } from "../../evidence/evidence-store.js";
import type { QualityIssue, ReportKindV2 } from "../types.js";
import { evaluateEvidenceCoverage } from "../../evidence/evidence-coverage.js";

const asArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const asString = (value: unknown): string => (typeof value === "string" ? value : "");
const asObject = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

/**
 * Lane 1 (Evidence Depth): institutional memos fail closed if primary evidence coverage is missing.
 *
 * We intentionally make this conservative. If the evidence isn't in the library, we don't ship the memo.
 */
export function validateEvidenceCoverage(params: {
  kind: ReportKindV2;
  report: unknown;
}): QualityIssue[] {
  const root = asObject(params.report);
  const sources = asArray(root.sources) as EvidenceItem[];
  const subject = asObject(root.subject);
  return evaluateEvidenceCoverage({
    kind: params.kind,
    sources,
    subject: {
      ticker: asString(subject.ticker),
      universe: asArray(subject.universe).map(asString),
    },
  });
}
