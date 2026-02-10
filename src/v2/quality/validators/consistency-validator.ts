import type { ReportKindV2, QualityIssue } from "../types.js";

const asArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const asString = (value: unknown): string => (typeof value === "string" ? value : "");
const asObject = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const normalize = (value: string): string => value.trim().toLowerCase();

const hasTitleMatch = (titles: string[], patterns: string[]): boolean =>
  patterns.some((pattern) => titles.some((title) => normalize(title).includes(pattern)));

export function validateConsistency(params: {
  kind: ReportKindV2;
  report: unknown;
}): QualityIssue[] {
  const issues: QualityIssue[] = [];
  const root = asObject(params.report);
  const exhibits = asArray(root.exhibits).map(asObject);
  const exhibitTitles = exhibits.map((ex) => asString(ex.title)).filter(Boolean);

  if (params.kind === "company") {
    // Required exhibit heuristics (fail-closed if missing; the goal is reproducibility).
    const required = [
      { key: "kpi_table", patterns: ["kpi"] },
      { key: "margins", patterns: ["margin"] },
      { key: "fcf", patterns: ["fcf", "free cash"] },
      { key: "sbc_dilution", patterns: ["sbc", "dilution", "share count"] },
      { key: "scenario_drivers", patterns: ["scenario", "driver"] },
      { key: "sensitivity", patterns: ["sensitivity"] },
    ];
    required.forEach((item) => {
      if (!hasTitleMatch(exhibitTitles, item.patterns)) {
        issues.push({
          severity: "error",
          code: "required_exhibit_missing",
          path: "/exhibits",
          message: `Missing required company exhibit (${item.key}). Expected exhibit title to include one of: ${item.patterns.join(", ")}`,
          fix: "Add the exhibit or explicitly justify N/A in the relevant section with a data-gap plan.",
        });
      }
    });

    // Thesis count: 3-5 falsifiable statements.
    const sections = asArray(root.sections).map(asObject);
    const thesis = sections.find((s) => asString(s.key) === "thesis");
    if (thesis) {
      const blocks = asArray(thesis.blocks).map(asObject);
      const statements = blocks.map((b) => asString(b.text)).filter((t) => t.trim().length > 0);
      if (statements.length < 3 || statements.length > 5) {
        issues.push({
          severity: "error",
          code: "thesis_statement_count",
          path: "/sections(thesis)",
          message: `Thesis must contain 3-5 falsifiable statements; found ${statements.length}.`,
          fix: "Rewrite the thesis into 3-5 bullets; each should have an explicit falsifier.",
        });
      }
      // Encourage explicit falsifier marker (keeps the writer honest and parsable).
      statements.forEach((text, idx) => {
        if (!/falsifier\s*:/i.test(text)) {
          issues.push({
            severity: "error",
            code: "thesis_missing_falsifier",
            path: `/sections(thesis)/blocks/${idx}`,
            message:
              "Each thesis statement must include a measurable falsifier (use 'Falsifier: ...').",
            fix: "Add a falsifier line using measurable triggers (and reference numeric_facts if thresholds matter).",
          });
        }
      });
    }
  } else {
    const required = [
      { key: "value_chain_map", patterns: ["value chain"] },
      { key: "capture_scorecard", patterns: ["capture", "scorecard"] },
      { key: "adoption_dashboard", patterns: ["adoption", "dashboard"] },
      { key: "catalyst_calendar", patterns: ["catalyst", "calendar"] },
      { key: "risk_heatmap", patterns: ["risk", "heatmap"] },
    ];
    required.forEach((item) => {
      if (!hasTitleMatch(exhibitTitles, item.patterns)) {
        issues.push({
          severity: "error",
          code: "required_exhibit_missing",
          path: "/exhibits",
          message: `Missing required theme exhibit (${item.key}). Expected exhibit title to include one of: ${item.patterns.join(", ")}`,
          fix: "Add the exhibit or explicitly justify N/A in the relevant section with a data-gap plan.",
        });
      }
    });
  }

  return issues;
}
