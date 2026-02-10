import type { ReportKindV2, QualityIssue } from "../types.js";

const asArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const asString = (value: unknown): string => (typeof value === "string" ? value : "");
const asObject = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const REQUIRED_SECTION_KEYS: Record<ReportKindV2, string[]> = {
  company: [
    "executive_summary",
    "variant_perception",
    "thesis",
    "business_overview",
    "moat_competition",
    "financial_quality",
    "valuation_scenarios",
    "catalysts",
    "risks_premortem",
    "change_mind_triggers",
  ],
  theme: [
    "executive_summary",
    "what_it_is_isnt_why_now",
    "value_chain",
    "capture_ledger",
    "beneficiaries_vs_left_behind",
    "catalysts_timeline",
    "risks_falsifiers",
    "portfolio_posture",
  ],
};

export function validateSectionCompleteness(params: {
  kind: ReportKindV2;
  report: unknown;
}): QualityIssue[] {
  const issues: QualityIssue[] = [];
  const required = REQUIRED_SECTION_KEYS[params.kind];
  const root = asObject(params.report);
  const sections = asArray(root.sections).map((item) => asObject(item));
  const keys = sections.map((section) => asString(section.key)).filter(Boolean);

  const seen = new Set<string>();
  keys.forEach((key, idx) => {
    if (seen.has(key)) {
      issues.push({
        severity: "error",
        code: "section_duplicate",
        path: `/sections/${idx}/key`,
        message: `Duplicate section key ${JSON.stringify(key)}.`,
        fix: "Ensure each required section appears exactly once.",
      });
    }
    seen.add(key);
  });

  required.forEach((key) => {
    if (!keys.includes(key)) {
      issues.push({
        severity: "error",
        code: "section_missing",
        path: "/sections",
        message: `Missing required section: ${key}.`,
        fix: "Add the missing section in the required order. If not applicable, include it with na_reason explaining why.",
      });
    }
  });

  // Order check: keys must appear in exact required order (extra keys are disallowed by schema).
  const expected = required;
  const actual = keys.filter((k) => expected.includes(k));
  const outOfOrder = actual.some((key, idx) => expected[idx] !== key);
  if (outOfOrder) {
    issues.push({
      severity: "error",
      code: "section_order",
      path: "/sections",
      message: `Sections are out of order. Expected: ${expected.join(" -> ")}`,
      fix: "Reorder sections to match the required structure exactly.",
    });
  }

  return issues;
}
