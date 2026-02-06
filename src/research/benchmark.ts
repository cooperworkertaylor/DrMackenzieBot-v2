import type { LearningTaskType } from "./learning.js";
import { openResearchDb } from "./db.js";

export type BenchmarkRunMode = "champion_vs_challenger" | "all_policies" | "champion_only";

export type BenchmarkSuite = {
  id: number;
  name: string;
  taskType: LearningTaskType;
  taskArchetype: string;
  description: string;
  active: boolean;
  gatingMinSamples: number;
  gatingMinLift: number;
  gatingMaxRiskBreaches: number;
  canaryDropThreshold: number;
  metadata: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type BenchmarkCase = {
  id: number;
  suiteId: number;
  caseName: string;
  taskArchetype: string;
  ticker: string;
  repoRoot: string;
  inputSummary: string;
  promptText: string;
  expected: Record<string, unknown>;
  weight: number;
  active: boolean;
  createdAt: number;
  updatedAt: number;
};

type PolicyVariant = {
  policyName: string;
  status: "champion" | "challenger" | "retired";
  active: boolean;
  minSamples: number;
  minLift: number;
  maxQuarantineRate: number;
  maxCalibrationError: number;
};

export type BenchmarkCaseResult = {
  caseId: number;
  caseName: string;
  policyName: string;
  policyRole: "primary";
  sampleCount: number;
  score: number;
  win: boolean;
  riskBreach: boolean;
  metrics: Record<string, unknown>;
};

export type BenchmarkPolicySummary = {
  policyName: string;
  status: "champion" | "challenger" | "retired";
  caseCount: number;
  weightedScore: number;
  weightedWinRate: number;
  riskBreaches: number;
  totalSamples: number;
  avgCalibrationError?: number;
  avgGraderScore?: number;
  avgOutcomeSignal?: number;
};

export type BenchmarkGate = {
  championPolicy?: string;
  promoteCandidate?: string;
  promoteAllowed: boolean;
  promoteReason: string;
  canaryBreach: boolean;
  canaryDrop?: number;
  rollbackCandidate?: string;
  rollbackReason: string;
};

export type BenchmarkRunResult = {
  runId: number;
  suite: BenchmarkSuite;
  mode: BenchmarkRunMode;
  seed: string;
  lookbackDays: number;
  startedAt: number;
  completedAt: number;
  caseCount: number;
  policySummaries: BenchmarkPolicySummary[];
  gate: BenchmarkGate;
};

export type BenchmarkGovernanceDecision = {
  runId: number;
  applied: boolean;
  decisionType: "promote" | "rollback" | "hold";
  championBefore: string;
  championAfter: string;
  challenger: string;
  reason: string;
};

export type BenchmarkReport = {
  suite: BenchmarkSuite;
  generatedAt: string;
  runs: Array<{
    id: number;
    mode: BenchmarkRunMode;
    seed: string;
    startedAt: number;
    completedAt: number;
    summary: {
      lookbackDays: number;
      caseCount: number;
      policySummaries: BenchmarkPolicySummary[];
      gate: BenchmarkGate;
    };
  }>;
};

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const mean = (values: number[]): number | undefined => {
  if (!values.length) return undefined;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
};

const normalizeTaskType = (taskType?: string): LearningTaskType => {
  const normalized = taskType?.trim().toLowerCase();
  if (normalized === "investment") return "investment";
  if (normalized === "coding") return "coding";
  return "other";
};

const parseJsonObject = <T extends object>(value: unknown, fallback: T): T => {
  if (typeof value !== "string" || !value.trim()) return fallback;
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return fallback;
    return parsed as T;
  } catch {
    return fallback;
  }
};

const toFiniteOrUndefined = (value: unknown): number | undefined => {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return value;
};

const normalizeMode = (mode?: string): BenchmarkRunMode => {
  const normalized = mode?.trim().toLowerCase();
  if (normalized === "all_policies") return "all_policies";
  if (normalized === "champion_only") return "champion_only";
  return "champion_vs_challenger";
};

const parseSuiteRow = (row: {
  id: number;
  name: string;
  task_type: string;
  task_archetype: string;
  description: string;
  active: number;
  gating_min_samples: number;
  gating_min_lift: number;
  gating_max_risk_breaches: number;
  canary_drop_threshold: number;
  metadata?: string;
  created_at: number;
  updated_at: number;
}): BenchmarkSuite => ({
  id: row.id,
  name: row.name,
  taskType: normalizeTaskType(row.task_type),
  taskArchetype: row.task_archetype,
  description: row.description,
  active: row.active === 1,
  gatingMinSamples: Math.max(1, Math.round(row.gating_min_samples)),
  gatingMinLift: Math.max(0, row.gating_min_lift),
  gatingMaxRiskBreaches: Math.max(0, Math.round(row.gating_max_risk_breaches)),
  canaryDropThreshold: clamp(row.canary_drop_threshold, 0, 1),
  metadata: parseJsonObject<Record<string, unknown>>(row.metadata, {}),
  createdAt: row.created_at,
  updatedAt: row.updated_at,
});

const parseCaseRow = (row: {
  id: number;
  suite_id: number;
  case_name: string;
  task_archetype: string;
  ticker: string;
  repo_root: string;
  input_summary: string;
  prompt_text: string;
  expected?: string;
  weight: number;
  active: number;
  created_at: number;
  updated_at: number;
}): BenchmarkCase => ({
  id: row.id,
  suiteId: row.suite_id,
  caseName: row.case_name,
  taskArchetype: row.task_archetype,
  ticker: row.ticker,
  repoRoot: row.repo_root,
  inputSummary: row.input_summary,
  promptText: row.prompt_text,
  expected: parseJsonObject<Record<string, unknown>>(row.expected, {}),
  weight: Math.max(0.01, row.weight),
  active: row.active === 1,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
});

const parseVariantRows = (params: {
  taskType: LearningTaskType;
  taskArchetype: string;
  dbPath?: string;
}): PolicyVariant[] => {
  const db = openResearchDb(params.dbPath);
  const rows = db
    .prepare(
      `SELECT policy_name, status, active, min_samples, min_lift, max_quarantine_rate, max_calibration_error
       FROM policy_variants
       WHERE task_type=? AND task_archetype=?
       ORDER BY CASE status WHEN 'champion' THEN 0 WHEN 'challenger' THEN 1 ELSE 2 END, updated_at DESC`,
    )
    .all(params.taskType, params.taskArchetype) as Array<{
    policy_name: string;
    status: string;
    active: number;
    min_samples: number;
    min_lift: number;
    max_quarantine_rate: number;
    max_calibration_error: number;
  }>;
  return rows.map((row) => ({
    policyName: row.policy_name,
    status: row.status === "champion" || row.status === "retired" ? row.status : "challenger",
    active: row.active === 1,
    minSamples: Math.max(1, Math.round(row.min_samples)),
    minLift: Math.max(0, row.min_lift),
    maxQuarantineRate: clamp(row.max_quarantine_rate, 0, 1),
    maxCalibrationError: clamp(row.max_calibration_error, 0, 1),
  }));
};

const resolveSuite = (params: {
  suiteName: string;
  taskType?: LearningTaskType | string;
  taskArchetype?: string;
  dbPath?: string;
}): BenchmarkSuite => {
  const db = openResearchDb(params.dbPath);
  const taskType = params.taskType ? normalizeTaskType(params.taskType) : "";
  const taskArchetype = (params.taskArchetype ?? "").trim();
  const row = db
    .prepare(
      `SELECT
         id, name, task_type, task_archetype, description, active,
         gating_min_samples, gating_min_lift, gating_max_risk_breaches, canary_drop_threshold,
         metadata, created_at, updated_at
       FROM benchmark_suites
       WHERE name=?
         AND (? = '' OR task_type = ?)
         AND (? = '' OR task_archetype = ?)
       LIMIT 1`,
    )
    .get(params.suiteName.trim(), taskType, taskType, taskArchetype, taskArchetype) as
    | {
        id: number;
        name: string;
        task_type: string;
        task_archetype: string;
        description: string;
        active: number;
        gating_min_samples: number;
        gating_min_lift: number;
        gating_max_risk_breaches: number;
        canary_drop_threshold: number;
        metadata?: string;
        created_at: number;
        updated_at: number;
      }
    | undefined;
  if (!row) {
    throw new Error(`Benchmark suite not found: name=${params.suiteName}`);
  }
  return parseSuiteRow(row);
};

const listCasesForSuite = (params: {
  suiteId: number;
  activeOnly?: boolean;
  dbPath?: string;
}): BenchmarkCase[] => {
  const db = openResearchDb(params.dbPath);
  const rows = db
    .prepare(
      `SELECT
         id, suite_id, case_name, task_archetype, ticker, repo_root,
         input_summary, prompt_text, expected, weight, active, created_at, updated_at
       FROM benchmark_cases
       WHERE suite_id=?
         AND (? = 0 OR active=1)
       ORDER BY case_name ASC`,
    )
    .all(params.suiteId, params.activeOnly === false ? 0 : 1) as Array<{
    id: number;
    suite_id: number;
    case_name: string;
    task_archetype: string;
    ticker: string;
    repo_root: string;
    input_summary: string;
    prompt_text: string;
    expected?: string;
    weight: number;
    active: number;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map(parseCaseRow);
};

export const upsertBenchmarkSuite = (params: {
  name: string;
  taskType: LearningTaskType | string;
  taskArchetype?: string;
  description?: string;
  active?: boolean;
  gatingMinSamples?: number;
  gatingMinLift?: number;
  gatingMaxRiskBreaches?: number;
  canaryDropThreshold?: number;
  metadata?: Record<string, unknown>;
  dbPath?: string;
}): BenchmarkSuite => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const taskType = normalizeTaskType(params.taskType);
  const taskArchetype = (params.taskArchetype ?? "").trim();
  const name = params.name.trim();
  if (!name) throw new Error("Suite name is required");
  db.prepare(
    `INSERT INTO benchmark_suites (
       name, task_type, task_archetype, description, active,
       gating_min_samples, gating_min_lift, gating_max_risk_breaches, canary_drop_threshold,
       metadata, created_at, updated_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(name, task_type, task_archetype) DO UPDATE SET
       description=excluded.description,
       active=excluded.active,
       gating_min_samples=excluded.gating_min_samples,
       gating_min_lift=excluded.gating_min_lift,
       gating_max_risk_breaches=excluded.gating_max_risk_breaches,
       canary_drop_threshold=excluded.canary_drop_threshold,
       metadata=excluded.metadata,
       updated_at=excluded.updated_at`,
  ).run(
    name,
    taskType,
    taskArchetype,
    (params.description ?? "").trim(),
    params.active === false ? 0 : 1,
    Math.max(1, Math.round(params.gatingMinSamples ?? 25)),
    Math.max(0, params.gatingMinLift ?? 0.03),
    Math.max(0, Math.round(params.gatingMaxRiskBreaches ?? 0)),
    clamp(params.canaryDropThreshold ?? 0.07, 0, 1),
    JSON.stringify(params.metadata ?? {}),
    now,
    now,
  );
  return resolveSuite({
    suiteName: name,
    taskType,
    taskArchetype,
    dbPath: params.dbPath,
  });
};

export const listBenchmarkSuites = (
  params: {
    taskType?: LearningTaskType | string;
    taskArchetype?: string;
    activeOnly?: boolean;
    dbPath?: string;
  } = {},
): BenchmarkSuite[] => {
  const db = openResearchDb(params.dbPath);
  const taskType = params.taskType ? normalizeTaskType(params.taskType) : "";
  const taskArchetype = (params.taskArchetype ?? "").trim();
  const rows = db
    .prepare(
      `SELECT
         id, name, task_type, task_archetype, description, active,
         gating_min_samples, gating_min_lift, gating_max_risk_breaches, canary_drop_threshold,
         metadata, created_at, updated_at
       FROM benchmark_suites
       WHERE (? = '' OR task_type = ?)
         AND (? = '' OR task_archetype = ?)
         AND (? = 0 OR active=1)
       ORDER BY task_type ASC, task_archetype ASC, name ASC`,
    )
    .all(
      taskType,
      taskType,
      taskArchetype,
      taskArchetype,
      params.activeOnly === false ? 0 : 1,
    ) as Array<{
    id: number;
    name: string;
    task_type: string;
    task_archetype: string;
    description: string;
    active: number;
    gating_min_samples: number;
    gating_min_lift: number;
    gating_max_risk_breaches: number;
    canary_drop_threshold: number;
    metadata?: string;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map(parseSuiteRow);
};

export const upsertBenchmarkCase = (params: {
  suiteName: string;
  taskType?: LearningTaskType | string;
  taskArchetype?: string;
  caseName: string;
  caseArchetype?: string;
  ticker?: string;
  repoRoot?: string;
  inputSummary?: string;
  promptText?: string;
  expected?: Record<string, unknown>;
  weight?: number;
  active?: boolean;
  dbPath?: string;
}): BenchmarkCase => {
  const suite = resolveSuite({
    suiteName: params.suiteName,
    taskType: params.taskType,
    taskArchetype: params.taskArchetype,
    dbPath: params.dbPath,
  });
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const caseName = params.caseName.trim();
  if (!caseName) throw new Error("caseName is required");
  db.prepare(
    `INSERT INTO benchmark_cases (
       suite_id, case_name, task_archetype, ticker, repo_root, input_summary, prompt_text,
       expected, weight, active, created_at, updated_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(suite_id, case_name) DO UPDATE SET
       task_archetype=excluded.task_archetype,
       ticker=excluded.ticker,
       repo_root=excluded.repo_root,
       input_summary=excluded.input_summary,
       prompt_text=excluded.prompt_text,
       expected=excluded.expected,
       weight=excluded.weight,
       active=excluded.active,
       updated_at=excluded.updated_at`,
  ).run(
    suite.id,
    caseName,
    (params.caseArchetype ?? "").trim(),
    (params.ticker ?? "").trim().toUpperCase(),
    (params.repoRoot ?? "").trim(),
    (params.inputSummary ?? "").trim(),
    (params.promptText ?? "").trim(),
    JSON.stringify(params.expected ?? {}),
    Math.max(0.01, params.weight ?? 1),
    params.active === false ? 0 : 1,
    now,
    now,
  );

  const row = openResearchDb(params.dbPath)
    .prepare(
      `SELECT
         id, suite_id, case_name, task_archetype, ticker, repo_root,
         input_summary, prompt_text, expected, weight, active, created_at, updated_at
       FROM benchmark_cases
       WHERE suite_id=? AND case_name=?`,
    )
    .get(suite.id, caseName) as {
    id: number;
    suite_id: number;
    case_name: string;
    task_archetype: string;
    ticker: string;
    repo_root: string;
    input_summary: string;
    prompt_text: string;
    expected?: string;
    weight: number;
    active: number;
    created_at: number;
    updated_at: number;
  };
  return parseCaseRow(row);
};

export const listBenchmarkCases = (params: {
  suiteName: string;
  taskType?: LearningTaskType | string;
  taskArchetype?: string;
  activeOnly?: boolean;
  dbPath?: string;
}): BenchmarkCase[] => {
  const suite = resolveSuite({
    suiteName: params.suiteName,
    taskType: params.taskType,
    taskArchetype: params.taskArchetype,
    dbPath: params.dbPath,
  });
  return listCasesForSuite({
    suiteId: suite.id,
    activeOnly: params.activeOnly,
    dbPath: params.dbPath,
  });
};

const policyCandidatesForMode = (params: {
  suite: BenchmarkSuite;
  mode: BenchmarkRunMode;
  dbPath?: string;
}): PolicyVariant[] => {
  const variants = parseVariantRows({
    taskType: params.suite.taskType,
    taskArchetype: params.suite.taskArchetype,
    dbPath: params.dbPath,
  }).filter((variant) => variant.active && variant.status !== "retired");
  if (!variants.length) return [];
  const champion = variants.find((variant) => variant.status === "champion") ?? variants[0];
  if (params.mode === "champion_only") {
    return champion ? [champion] : [];
  }
  if (params.mode === "all_policies") {
    return variants;
  }
  const challengers = variants.filter((variant) => variant.policyName !== champion?.policyName);
  return champion ? [champion, ...challengers] : variants;
};

const loadTaskOutcomesForCase = (params: {
  suite: BenchmarkSuite;
  benchmarkCase: BenchmarkCase;
  policyName: string;
  lookbackDays: number;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  const since = Date.now() - Math.max(1, params.lookbackDays) * 86_400_000;
  const archetypeFilter =
    params.benchmarkCase.taskArchetype.trim() || params.suite.taskArchetype.trim();
  const rows = db
    .prepare(
      `SELECT
         grader_score, status, confidence, user_score, realized_outcome_score, citation_count
       FROM task_outcomes
       WHERE task_type=?
         AND policy_name=?
         AND created_at>=?
         AND (? = '' OR task_archetype = ?)
         AND (? = '' OR ticker = ?)
         AND (? = '' OR repo_root = ?)`,
    )
    .all(
      params.suite.taskType,
      params.policyName,
      since,
      archetypeFilter,
      archetypeFilter,
      params.benchmarkCase.ticker,
      params.benchmarkCase.ticker,
      params.benchmarkCase.repoRoot,
      params.benchmarkCase.repoRoot,
    ) as Array<{
    grader_score: number;
    status: string;
    confidence?: number;
    user_score?: number;
    realized_outcome_score?: number;
    citation_count?: number;
  }>;
  return rows;
};

const expectedNumber = (expected: Record<string, unknown>, key: string): number | undefined => {
  const value = expected[key];
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
};

const scoreCaseForPolicy = (params: {
  suite: BenchmarkSuite;
  benchmarkCase: BenchmarkCase;
  policy: PolicyVariant;
  lookbackDays: number;
  dbPath?: string;
}): BenchmarkCaseResult => {
  const rows = loadTaskOutcomesForCase({
    suite: params.suite,
    benchmarkCase: params.benchmarkCase,
    policyName: params.policy.policyName,
    lookbackDays: params.lookbackDays,
    dbPath: params.dbPath,
  });
  const sampleCount = rows.length;
  const graderScores = rows.map((row) => toFiniteOrUndefined(row.grader_score) ?? 0);
  const outcomeSignals = rows.map(
    (row) =>
      toFiniteOrUndefined(row.realized_outcome_score) ??
      toFiniteOrUndefined(row.user_score) ??
      toFiniteOrUndefined(row.grader_score) ??
      0,
  );
  const winRate = rows.length
    ? rows.filter((_, idx) => (outcomeSignals[idx] ?? 0) >= 0.7).length / rows.length
    : 0;
  const quarantineRate = rows.length
    ? rows.filter((row) => row.status === "quarantine").length / rows.length
    : 0;
  const calibrationErrors = rows
    .map((row, idx) => {
      const confidence = toFiniteOrUndefined(row.confidence);
      const outcome = outcomeSignals[idx];
      if (typeof confidence !== "number" || typeof outcome !== "number") return undefined;
      return Math.abs(confidence - outcome);
    })
    .filter((value): value is number => typeof value === "number");
  const calibrationError = mean(calibrationErrors);
  const citationsAvg = mean(
    rows
      .map((row) => toFiniteOrUndefined(row.citation_count))
      .filter((value): value is number => typeof value === "number"),
  );
  const avgGraderScore = mean(graderScores) ?? 0;
  const avgOutcomeSignal = mean(outcomeSignals) ?? 0;
  const calibrationScore =
    typeof calibrationError === "number" ? clamp(1 - calibrationError, 0, 1) : 0.6;
  const sampleQuality = clamp(
    sampleCount / Math.max(1, expectedNumber(params.benchmarkCase.expected, "min_samples") ?? 8),
    0,
    1,
  );
  const score = clamp(
    0.5 * avgGraderScore + 0.35 * avgOutcomeSignal + 0.1 * calibrationScore + 0.05 * sampleQuality,
    0,
    1,
  );

  const minSamples = Math.max(
    1,
    Math.round(
      expectedNumber(params.benchmarkCase.expected, "min_samples") ?? params.policy.minSamples,
    ),
  );
  const minScore = expectedNumber(params.benchmarkCase.expected, "min_score") ?? 0.65;
  const minWinRate = expectedNumber(params.benchmarkCase.expected, "min_win_rate") ?? 0.5;
  const maxQuarantineRate =
    expectedNumber(params.benchmarkCase.expected, "max_quarantine_rate") ??
    params.policy.maxQuarantineRate;
  const maxCalibrationError =
    expectedNumber(params.benchmarkCase.expected, "max_calibration_error") ??
    params.policy.maxCalibrationError;
  const minCitations = expectedNumber(params.benchmarkCase.expected, "min_citations");

  const riskReasons: string[] = [];
  if (sampleCount < minSamples) riskReasons.push("insufficient_samples");
  if (quarantineRate > maxQuarantineRate) riskReasons.push("quarantine_rate_high");
  if (typeof calibrationError === "number" && calibrationError > maxCalibrationError) {
    riskReasons.push("calibration_error_high");
  }
  if (typeof minCitations === "number" && (citationsAvg ?? 0) < minCitations) {
    riskReasons.push("citation_coverage_low");
  }

  const riskBreach = riskReasons.length > 0;
  const win = !riskBreach && score >= minScore && winRate >= minWinRate;

  return {
    caseId: params.benchmarkCase.id,
    caseName: params.benchmarkCase.caseName,
    policyName: params.policy.policyName,
    policyRole: "primary",
    sampleCount,
    score,
    win,
    riskBreach,
    metrics: {
      avg_grader_score: avgGraderScore,
      avg_outcome_signal: avgOutcomeSignal,
      win_rate: winRate,
      quarantine_rate: quarantineRate,
      calibration_error: calibrationError,
      avg_citations: citationsAvg,
      min_samples: minSamples,
      min_score: minScore,
      min_win_rate: minWinRate,
      max_quarantine_rate: maxQuarantineRate,
      max_calibration_error: maxCalibrationError,
      risk_reasons: riskReasons,
    },
  };
};

const summarizePolicyResults = (params: {
  policy: PolicyVariant;
  results: BenchmarkCaseResult[];
  casesById: Map<number, BenchmarkCase>;
}): BenchmarkPolicySummary => {
  if (!params.results.length) {
    return {
      policyName: params.policy.policyName,
      status: params.policy.status,
      caseCount: 0,
      weightedScore: 0,
      weightedWinRate: 0,
      riskBreaches: 0,
      totalSamples: 0,
    };
  }
  let totalWeight = 0;
  let scoreWeighted = 0;
  let winWeighted = 0;
  let riskBreaches = 0;
  let totalSamples = 0;
  let calibrationWeighted = 0;
  let calibrationWeight = 0;
  let graderWeighted = 0;
  let outcomeWeighted = 0;
  for (const result of params.results) {
    const benchmarkCase = params.casesById.get(result.caseId);
    const weight = benchmarkCase?.weight ?? 1;
    totalWeight += weight;
    scoreWeighted += result.score * weight;
    winWeighted += (result.win ? 1 : 0) * weight;
    riskBreaches += result.riskBreach ? 1 : 0;
    totalSamples += result.sampleCount;
    const calibration = toFiniteOrUndefined(result.metrics.calibration_error);
    if (typeof calibration === "number") {
      calibrationWeighted += calibration * weight;
      calibrationWeight += weight;
    }
    const grader = toFiniteOrUndefined(result.metrics.avg_grader_score);
    if (typeof grader === "number") graderWeighted += grader * weight;
    const outcome = toFiniteOrUndefined(result.metrics.avg_outcome_signal);
    if (typeof outcome === "number") outcomeWeighted += outcome * weight;
  }
  return {
    policyName: params.policy.policyName,
    status: params.policy.status,
    caseCount: params.results.length,
    weightedScore: totalWeight > 1e-9 ? scoreWeighted / totalWeight : 0,
    weightedWinRate: totalWeight > 1e-9 ? winWeighted / totalWeight : 0,
    riskBreaches,
    totalSamples,
    avgCalibrationError:
      calibrationWeight > 1e-9 ? calibrationWeighted / calibrationWeight : undefined,
    avgGraderScore: totalWeight > 1e-9 ? graderWeighted / totalWeight : undefined,
    avgOutcomeSignal: totalWeight > 1e-9 ? outcomeWeighted / totalWeight : undefined,
  };
};

const parseRunSummary = (
  value: unknown,
): {
  lookbackDays: number;
  caseCount: number;
  policySummaries: BenchmarkPolicySummary[];
  gate: BenchmarkGate;
} => {
  const parsed = parseJsonObject<Record<string, unknown>>(value, {});
  const lookbackDays = toFiniteOrUndefined(parsed.lookbackDays) ?? 0;
  const caseCount = toFiniteOrUndefined(parsed.caseCount) ?? 0;
  const policySummariesRaw = Array.isArray(parsed.policySummaries)
    ? (parsed.policySummaries as Array<Record<string, unknown>>)
    : [];
  const policySummaries: BenchmarkPolicySummary[] = policySummariesRaw.map((item) => ({
    policyName: typeof item.policyName === "string" ? item.policyName : "",
    status: item.status === "champion" || item.status === "retired" ? item.status : "challenger",
    caseCount: Math.max(0, Math.round(toFiniteOrUndefined(item.caseCount) ?? 0)),
    weightedScore: clamp(toFiniteOrUndefined(item.weightedScore) ?? 0, 0, 1),
    weightedWinRate: clamp(toFiniteOrUndefined(item.weightedWinRate) ?? 0, 0, 1),
    riskBreaches: Math.max(0, Math.round(toFiniteOrUndefined(item.riskBreaches) ?? 0)),
    totalSamples: Math.max(0, Math.round(toFiniteOrUndefined(item.totalSamples) ?? 0)),
    avgCalibrationError: toFiniteOrUndefined(item.avgCalibrationError),
    avgGraderScore: toFiniteOrUndefined(item.avgGraderScore),
    avgOutcomeSignal: toFiniteOrUndefined(item.avgOutcomeSignal),
  }));
  const gateRaw = parseJsonObject<Record<string, unknown>>(JSON.stringify(parsed.gate ?? {}), {});
  const gate: BenchmarkGate = {
    championPolicy: typeof gateRaw.championPolicy === "string" ? gateRaw.championPolicy : undefined,
    promoteCandidate:
      typeof gateRaw.promoteCandidate === "string" ? gateRaw.promoteCandidate : undefined,
    promoteAllowed: gateRaw.promoteAllowed === true,
    promoteReason: typeof gateRaw.promoteReason === "string" ? gateRaw.promoteReason : "",
    canaryBreach: gateRaw.canaryBreach === true,
    canaryDrop: toFiniteOrUndefined(gateRaw.canaryDrop),
    rollbackCandidate:
      typeof gateRaw.rollbackCandidate === "string" ? gateRaw.rollbackCandidate : undefined,
    rollbackReason: typeof gateRaw.rollbackReason === "string" ? gateRaw.rollbackReason : "",
  };
  return {
    lookbackDays: Math.max(0, Math.round(lookbackDays)),
    caseCount: Math.max(0, Math.round(caseCount)),
    policySummaries,
    gate,
  };
};

const getPreviousRunChampionScore = (params: {
  suiteId: number;
  runId: number;
  championPolicy: string;
  dbPath?: string;
}): number | undefined => {
  const db = openResearchDb(params.dbPath);
  const row = db
    .prepare(
      `SELECT summary
       FROM benchmark_runs
       WHERE suite_id=?
         AND id<>?
       ORDER BY created_at DESC
       LIMIT 1`,
    )
    .get(params.suiteId, params.runId) as { summary?: string } | undefined;
  if (!row?.summary) return undefined;
  const parsed = parseRunSummary(row.summary);
  return parsed.policySummaries.find((item) => item.policyName === params.championPolicy)
    ?.weightedScore;
};

const evaluateGate = (params: {
  suite: BenchmarkSuite;
  runId: number;
  policySummaries: BenchmarkPolicySummary[];
  variants: PolicyVariant[];
  dbPath?: string;
}): BenchmarkGate => {
  const championSummary =
    params.policySummaries.find((item) => item.status === "champion") ?? params.policySummaries[0];
  if (!championSummary) {
    return {
      promoteAllowed: false,
      promoteReason: "No benchmark policy summaries available.",
      canaryBreach: false,
      rollbackReason: "No champion available.",
    };
  }
  const championPolicy = championSummary.policyName;
  const championVariant = params.variants.find((variant) => variant.policyName === championPolicy);
  const challengerSummaries = params.policySummaries
    .filter((item) => item.policyName !== championPolicy)
    .toSorted((a, b) => b.weightedScore - a.weightedScore);
  const topChallenger = challengerSummaries[0];
  const challengerVariant = params.variants.find(
    (variant) => variant.policyName === topChallenger?.policyName,
  );

  let promoteAllowed = false;
  let promoteReason = "No eligible challenger.";
  if (topChallenger && challengerVariant) {
    const minSamples = Math.max(
      params.suite.gatingMinSamples,
      challengerVariant.minSamples,
      championVariant?.minSamples ?? 1,
    );
    const minLift = Math.max(
      params.suite.gatingMinLift,
      challengerVariant.minLift,
      championVariant?.minLift ?? 0,
    );
    const requiredRiskBreaches = Math.min(
      params.suite.gatingMaxRiskBreaches,
      Math.round((championVariant?.maxQuarantineRate ?? 1) * 10),
    );
    if (topChallenger.totalSamples < minSamples) {
      promoteReason = `Challenger samples ${topChallenger.totalSamples} < min ${minSamples}.`;
    } else if (topChallenger.weightedScore < championSummary.weightedScore + minLift) {
      promoteReason = `Challenger score ${topChallenger.weightedScore.toFixed(3)} < champion+lift ${(championSummary.weightedScore + minLift).toFixed(3)}.`;
    } else if (topChallenger.riskBreaches > requiredRiskBreaches) {
      promoteReason = `Challenger risk breaches ${topChallenger.riskBreaches} > allowed ${requiredRiskBreaches}.`;
    } else {
      promoteAllowed = true;
      promoteReason = "Challenger passed benchmark promotion gates.";
    }
  }

  const previousChampionScore = getPreviousRunChampionScore({
    suiteId: params.suite.id,
    runId: params.runId,
    championPolicy,
    dbPath: params.dbPath,
  });
  const canaryDrop =
    typeof previousChampionScore === "number"
      ? previousChampionScore - championSummary.weightedScore
      : undefined;
  const canaryBreach =
    typeof canaryDrop === "number" && canaryDrop > params.suite.canaryDropThreshold;
  let rollbackCandidate: string | undefined;
  let rollbackReason = "Champion canary stable.";
  if (canaryBreach) {
    const fallback = challengerSummaries.find((summary) => {
      const variant = params.variants.find((item) => item.policyName === summary.policyName);
      if (!variant) return false;
      const minSamples = Math.max(
        variant.minSamples,
        Math.floor(params.suite.gatingMinSamples / 2),
      );
      if (summary.totalSamples < minSamples) return false;
      if (summary.riskBreaches > params.suite.gatingMaxRiskBreaches) return false;
      return (
        summary.weightedScore >= championSummary.weightedScore - params.suite.gatingMinLift / 2
      );
    });
    if (fallback) {
      rollbackCandidate = fallback.policyName;
      rollbackReason =
        "Champion canary breached; fallback challenger satisfies rollback guardrails.";
    } else {
      rollbackReason = "Champion canary breached but no fallback challenger satisfies guardrails.";
    }
  }

  return {
    championPolicy,
    promoteCandidate: topChallenger?.policyName,
    promoteAllowed,
    promoteReason,
    canaryBreach,
    canaryDrop,
    rollbackCandidate,
    rollbackReason,
  };
};

export const runBenchmarkReplay = (params: {
  suiteName: string;
  taskType?: LearningTaskType | string;
  taskArchetype?: string;
  mode?: BenchmarkRunMode | string;
  lookbackDays?: number;
  seed?: string;
  dbPath?: string;
}): BenchmarkRunResult => {
  const db = openResearchDb(params.dbPath);
  const suite = resolveSuite({
    suiteName: params.suiteName,
    taskType: params.taskType,
    taskArchetype: params.taskArchetype,
    dbPath: params.dbPath,
  });
  const benchmarkCases = listCasesForSuite({
    suiteId: suite.id,
    activeOnly: true,
    dbPath: params.dbPath,
  });
  if (!benchmarkCases.length) {
    throw new Error(`No active benchmark cases found for suite ${suite.name}`);
  }
  const mode = normalizeMode(params.mode);
  const seed =
    params.seed?.trim() ||
    `${suite.name}:${suite.taskType}:${suite.taskArchetype || "default"}:${Date.now()}`;
  const lookbackDays = Math.max(1, params.lookbackDays ?? 90);
  const policies = policyCandidatesForMode({
    suite,
    mode,
    dbPath: params.dbPath,
  });
  if (!policies.length) {
    throw new Error(
      `No active policy variants for ${suite.taskType}:${suite.taskArchetype || "default"}`,
    );
  }

  const startedAt = Date.now();
  const runRow = db
    .prepare(
      `INSERT INTO benchmark_runs (
         suite_id, run_mode, seed, status, summary, started_at, completed_at, created_at
       ) VALUES (?, ?, ?, 'running', '{}', ?, ?, ?)
       RETURNING id`,
    )
    .get(suite.id, mode, seed, startedAt, startedAt, startedAt) as { id: number };
  const runId = runRow.id;

  const caseResults: BenchmarkCaseResult[] = [];
  for (const policy of policies) {
    for (const benchmarkCase of benchmarkCases) {
      caseResults.push(
        scoreCaseForPolicy({
          suite,
          benchmarkCase,
          policy,
          lookbackDays,
          dbPath: params.dbPath,
        }),
      );
    }
  }
  const casesById = new Map(benchmarkCases.map((item) => [item.id, item]));
  const policySummaries = policies
    .map((policy) =>
      summarizePolicyResults({
        policy,
        results: caseResults.filter((result) => result.policyName === policy.policyName),
        casesById,
      }),
    )
    .toSorted((a, b) => b.weightedScore - a.weightedScore);

  const gate = evaluateGate({
    suite,
    runId,
    policySummaries,
    variants: policies,
    dbPath: params.dbPath,
  });

  db.exec("BEGIN");
  try {
    const resultInsert = db.prepare(
      `INSERT INTO benchmark_results (
         run_id, case_id, task_type, task_archetype, policy_name, policy_role,
         sample_count, score, win, risk_breach, metrics, created_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    for (const result of caseResults) {
      const benchmarkCase = casesById.get(result.caseId);
      resultInsert.run(
        runId,
        result.caseId,
        suite.taskType,
        benchmarkCase?.taskArchetype || suite.taskArchetype,
        result.policyName,
        result.policyRole,
        result.sampleCount,
        result.score,
        result.win ? 1 : 0,
        result.riskBreach ? 1 : 0,
        JSON.stringify(result.metrics),
        Date.now(),
      );
    }
    const completedAt = Date.now();
    const summary = {
      lookbackDays,
      caseCount: benchmarkCases.length,
      policySummaries,
      gate,
    };
    db.prepare(
      `UPDATE benchmark_runs
       SET status='completed',
           summary=?,
           completed_at=?
       WHERE id=?`,
    ).run(JSON.stringify(summary), completedAt, runId);
    db.exec("COMMIT");
    return {
      runId,
      suite,
      mode,
      seed,
      lookbackDays,
      startedAt,
      completedAt,
      caseCount: benchmarkCases.length,
      policySummaries,
      gate,
    };
  } catch (err) {
    db.exec("ROLLBACK");
    db.prepare(`UPDATE benchmark_runs SET status='failed', completed_at=? WHERE id=?`).run(
      Date.now(),
      runId,
    );
    throw err;
  }
};

const parseDecisionRow = (row: {
  id: number;
  run_id: number;
  applied: number;
  decision_type: string;
  champion_before: string;
  champion_after: string;
  challenger: string;
  reason: string;
}) => ({
  runId: row.run_id,
  applied: row.applied === 1,
  decisionType:
    row.decision_type === "rollback"
      ? "rollback"
      : row.decision_type === "hold"
        ? "hold"
        : "promote",
  championBefore: row.champion_before,
  championAfter: row.champion_after,
  challenger: row.challenger,
  reason: row.reason,
});

const setChampionPolicy = (params: {
  taskType: LearningTaskType;
  taskArchetype: string;
  championName: string;
  previousChampionName?: string;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  db.exec("BEGIN");
  try {
    if (params.previousChampionName?.trim()) {
      db.prepare(
        `UPDATE policy_variants
         SET status='challenger',
             traffic_weight=MIN(traffic_weight, 0.3),
             updated_at=?
         WHERE task_type=? AND task_archetype=? AND policy_name=?`,
      ).run(Date.now(), params.taskType, params.taskArchetype, params.previousChampionName);
    }
    db.prepare(
      `UPDATE policy_variants
       SET status='champion',
           active=1,
           traffic_weight=MAX(traffic_weight, 0.8),
           updated_at=?
       WHERE task_type=? AND task_archetype=? AND policy_name=?`,
    ).run(Date.now(), params.taskType, params.taskArchetype, params.championName);
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
};

export const applyBenchmarkGovernance = (params: {
  runId: number;
  dbPath?: string;
}): BenchmarkGovernanceDecision => {
  const db = openResearchDb(params.dbPath);
  const runRow = db
    .prepare(
      `SELECT br.id, br.suite_id, br.summary, bs.task_type, bs.task_archetype
       FROM benchmark_runs br
       JOIN benchmark_suites bs ON bs.id=br.suite_id
       WHERE br.id=?`,
    )
    .get(params.runId) as
    | {
        id: number;
        suite_id: number;
        summary?: string;
        task_type: string;
        task_archetype: string;
      }
    | undefined;
  if (!runRow) throw new Error(`Benchmark run not found: id=${params.runId}`);
  const parsedSummary = parseRunSummary(runRow.summary);
  const gate = parsedSummary.gate;
  const championBefore = gate.championPolicy ?? "";
  let decisionType: "promote" | "rollback" | "hold" = "hold";
  let championAfter = championBefore;
  let challenger = "";
  let reason = "No benchmark governance action required.";
  let applied = false;

  if (gate.canaryBreach && gate.rollbackCandidate && gate.rollbackCandidate !== championBefore) {
    decisionType = "rollback";
    championAfter = gate.rollbackCandidate;
    challenger = gate.rollbackCandidate;
    reason = gate.rollbackReason || "Champion canary breached; rolling back to challenger.";
    applied = true;
  } else if (
    gate.promoteAllowed &&
    gate.promoteCandidate &&
    gate.promoteCandidate !== championBefore
  ) {
    decisionType = "promote";
    championAfter = gate.promoteCandidate;
    challenger = gate.promoteCandidate;
    reason = gate.promoteReason || "Benchmark gates passed; promoting challenger.";
    applied = true;
  } else if (gate.canaryBreach) {
    decisionType = "hold";
    reason = gate.rollbackReason || "Champion canary breached but no eligible rollback candidate.";
  } else {
    decisionType = "hold";
    reason = gate.promoteReason || "Promotion gate not satisfied.";
  }

  if (applied && championAfter) {
    setChampionPolicy({
      taskType: normalizeTaskType(runRow.task_type),
      taskArchetype: runRow.task_archetype,
      championName: championAfter,
      previousChampionName: championBefore,
      dbPath: params.dbPath,
    });
  }

  db.prepare(
    `INSERT INTO policy_decisions (
       task_type, task_archetype, decision_type, champion_before, champion_after,
       challenger, reason, metrics, created_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    runRow.task_type,
    runRow.task_archetype,
    decisionType,
    championBefore,
    championAfter,
    challenger,
    reason,
    JSON.stringify({
      source: "benchmark",
      run_id: params.runId,
      gate,
      benchmark_summary: {
        case_count: parsedSummary.caseCount,
        lookback_days: parsedSummary.lookbackDays,
      },
    }),
    Date.now(),
  );

  return {
    runId: params.runId,
    applied,
    decisionType,
    championBefore,
    championAfter,
    challenger,
    reason,
  };
};

export const benchmarkReport = (params: {
  suiteName: string;
  taskType?: LearningTaskType | string;
  taskArchetype?: string;
  limit?: number;
  dbPath?: string;
}): BenchmarkReport => {
  const suite = resolveSuite({
    suiteName: params.suiteName,
    taskType: params.taskType,
    taskArchetype: params.taskArchetype,
    dbPath: params.dbPath,
  });
  const db = openResearchDb(params.dbPath);
  const limit = Math.max(1, Math.round(params.limit ?? 20));
  const rows = db
    .prepare(
      `SELECT id, run_mode, seed, started_at, completed_at, summary
       FROM benchmark_runs
       WHERE suite_id=?
       ORDER BY created_at DESC
       LIMIT ?`,
    )
    .all(suite.id, limit) as Array<{
    id: number;
    run_mode: string;
    seed: string;
    started_at: number;
    completed_at: number;
    summary?: string;
  }>;
  return {
    suite,
    generatedAt: new Date().toISOString(),
    runs: rows.map((row) => ({
      id: row.id,
      mode: normalizeMode(row.run_mode),
      seed: row.seed,
      startedAt: row.started_at,
      completedAt: row.completed_at,
      summary: parseRunSummary(row.summary),
    })),
  };
};

export const runAllBenchmarksWithGovernance = (
  params: {
    taskType?: LearningTaskType | string;
    taskArchetype?: string;
    lookbackDays?: number;
    mode?: BenchmarkRunMode | string;
    dbPath?: string;
  } = {},
) => {
  const suites = listBenchmarkSuites({
    taskType: params.taskType,
    taskArchetype: params.taskArchetype,
    activeOnly: true,
    dbPath: params.dbPath,
  });
  const runs: BenchmarkRunResult[] = [];
  const decisions: BenchmarkGovernanceDecision[] = [];
  let failures = 0;
  for (const suite of suites) {
    try {
      const run = runBenchmarkReplay({
        suiteName: suite.name,
        taskType: suite.taskType,
        taskArchetype: suite.taskArchetype,
        mode: params.mode,
        lookbackDays: params.lookbackDays,
        dbPath: params.dbPath,
      });
      runs.push(run);
      decisions.push(
        applyBenchmarkGovernance({
          runId: run.runId,
          dbPath: params.dbPath,
        }),
      );
    } catch {
      failures += 1;
    }
  }
  return {
    suiteCount: suites.length,
    runCount: runs.length,
    failures,
    runs,
    decisions,
  };
};
