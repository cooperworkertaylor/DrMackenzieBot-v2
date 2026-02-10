import type { QualityGateResult, QualityIssue, ReportKindV2 } from "./types.js";
import { runV2QualityGate } from "./quality-gate.js";

export type V2RepairModel = {
  /**
   * Must return a full candidate report JSON object (not a patch).
   * The caller will re-validate schema + gates.
   */
  repair: (params: {
    kind: ReportKindV2;
    report: unknown;
    issues: QualityIssue[];
    attempt: number;
  }) => Promise<unknown>;
};

export type V2RepairLoopResult = {
  report: unknown;
  gate: QualityGateResult;
  attemptsUsed: number;
  repaired: boolean;
};

export async function runV2QualityGateWithRepair(params: {
  kind: ReportKindV2;
  report: unknown;
  repairModel: V2RepairModel;
  maxAttempts?: number;
}): Promise<V2RepairLoopResult> {
  const maxAttempts = Math.max(0, Math.min(2, params.maxAttempts ?? 2));
  let current = params.report;
  let gate = runV2QualityGate({ kind: params.kind, report: current });
  if (gate.passed) {
    return { report: current, gate, attemptsUsed: 0, repaired: false };
  }

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    current = await params.repairModel.repair({
      kind: params.kind,
      report: current,
      issues: gate.issues,
      attempt,
    });
    gate = runV2QualityGate({ kind: params.kind, report: current });
    if (gate.passed) {
      return { report: current, gate, attemptsUsed: attempt, repaired: true };
    }
  }

  return { report: current, gate, attemptsUsed: maxAttempts, repaired: maxAttempts > 0 };
}
