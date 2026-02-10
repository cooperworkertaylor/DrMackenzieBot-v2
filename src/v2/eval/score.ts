import type { ReportKindV2 } from "../quality/types.js";
import { runV2QualityGate } from "../quality/quality-gate.js";

export type V2EvalSubscoreKey =
  | "structure"
  | "sourcing"
  | "numeric_provenance"
  | "consistency"
  | "risk_falsifiers"
  | "clarity";

export type V2EvalBreakdown = {
  kind: ReportKindV2;
  gate_passed: boolean;
  total: number;
  subscores: Record<V2EvalSubscoreKey, number>;
  issues: Array<{ severity: string; code: string; message: string; path?: string }>;
};

const hasAnyCode = (codes: string[], prefixes: string[]): boolean =>
  codes.some((code) => prefixes.some((prefix) => code === prefix || code.startsWith(prefix)));

export function scoreV2Report(params: { kind: ReportKindV2; report: unknown }): V2EvalBreakdown {
  const gate = runV2QualityGate({ kind: params.kind, report: params.report });
  const codes = gate.issues.map((i) => i.code);

  // Structure (20)
  const structure = hasAnyCode(codes, ["schema_invalid", "section_"]) ? 0 : 20;

  // Sourcing (20)
  const sourcing = hasAnyCode(codes, [
    "citation_",
    "exhibit_unknown_source",
    "appendix_unknown_source",
  ])
    ? 0
    : 20;

  // Numeric provenance (20)
  const numeric_provenance = hasAnyCode(codes, ["numeric_", "exhibit_numeric_"]) ? 0 : 20;

  // Consistency (15)
  const consistency = hasAnyCode(codes, ["required_exhibit_missing", "thesis_"]) ? 0 : 15;

  // Risk + falsifiers (15)
  // For now, proxy via thesis falsifier enforcement (company) or presence of risks_falsifiers section (theme).
  const risk_falsifiers =
    params.kind === "company"
      ? hasAnyCode(codes, ["thesis_missing_falsifier", "thesis_statement_count"])
        ? 0
        : 15
      : hasAnyCode(codes, ["section_missing", "section_order"])
        ? 0
        : 15;

  // Clarity (10)
  const clarity = hasAnyCode(codes, ["style_"]) ? 0 : 10;

  const subscores: Record<V2EvalSubscoreKey, number> = {
    structure,
    sourcing,
    numeric_provenance,
    consistency,
    risk_falsifiers,
    clarity,
  };

  const total = Object.values(subscores).reduce((sum, v) => sum + v, 0);

  return {
    kind: params.kind,
    gate_passed: gate.passed,
    total,
    subscores: { ...subscores },
    issues: gate.issues,
  };
}
