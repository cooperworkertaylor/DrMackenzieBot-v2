export type ReportKindV2 = "company" | "theme";

export type QualitySeverity = "error" | "warn";

export type QualityIssue = {
  severity: QualitySeverity;
  code: string;
  message: string;
  /** JSON pointer-ish path (best-effort). */
  path?: string;
  /** Concrete suggested fix. */
  fix?: string;
};

export type QualityGateResult = {
  passed: boolean;
  issues: QualityIssue[];
};
