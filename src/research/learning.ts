import { createHash } from "node:crypto";
import { openResearchDb } from "./db.js";
import { appendProvenanceEvent } from "./provenance.js";
import { forecastDecisionMetrics, resolveMatureForecasts } from "./valuation.js";

export type LearningTaskType = "investment" | "coding" | "other";
export type LearningStatus = "trusted" | "pending" | "quarantine";

type NumericRecord = Record<string, number>;
type SourceMix = Record<string, number>;

export type TaskOutcomeRecord = {
  id: number;
  taskType: LearningTaskType;
  taskArchetype: string;
  policyName: string;
  policyRole: "primary" | "shadow";
  experimentGroup: string;
  ticker: string;
  repoRoot: string;
  inputSummary: string;
  outputHash: string;
  confidence?: number;
  citationCount?: number;
  latencyMs?: number;
  userScore?: number;
  realizedOutcomeScore?: number;
  outcomeLabel: string;
  sourceMix: SourceMix;
  gradingMetrics: NumericRecord;
  graderScore: number;
  graderDetails: NumericRecord;
  graderVersion: string;
  status: LearningStatus;
  statusReason: string;
  createdAt: number;
  updatedAt: number;
};

export type LogTaskOutcomeParams = {
  id?: number;
  taskType?: LearningTaskType | string;
  taskArchetype?: string;
  policyName?: string;
  policyRole?: "primary" | "shadow" | string;
  experimentGroup?: string;
  ticker?: string;
  repoRoot?: string;
  inputSummary?: string;
  outputText?: string;
  outputHash?: string;
  confidence?: number;
  citationCount?: number;
  latencyMs?: number;
  userScore?: number;
  realizedOutcomeScore?: number;
  outcomeLabel?: string;
  sourceMix?: SourceMix;
  gradingMetrics?: NumericRecord;
  dbPath?: string;
};

type GradeResult = {
  score: number;
  details: NumericRecord;
};

type LearningReportParams = {
  days?: number;
  taskType?: LearningTaskType | string;
  minSamples?: number;
  dbPath?: string;
};

type RoutingRecommendation = {
  taskType: LearningTaskType;
  bestArchetype?: string;
  archetypeSampleCount?: number;
  archetypeWinRate?: number;
  archetypeAvgScore?: number;
  topSources: Array<{ source: string; score: number; samples: number }>;
};

type LearningReport = {
  generatedAt: string;
  lookbackDays: number;
  totalTasks: number;
  avgGraderScore?: number;
  avgUserScore?: number;
  avgRealizedOutcomeScore?: number;
  trustedRate?: number;
  quarantineRate?: number;
  byTaskType: Array<{
    taskType: LearningTaskType;
    count: number;
    avgScore: number;
    trustedRate: number;
    quarantineRate: number;
    winRate: number;
  }>;
  routing: RoutingRecommendation[];
  sourceEffectiveness: Array<{
    source: string;
    score: number;
    samples: number;
  }>;
  rewardModels: Array<{
    taskType: LearningTaskType;
    taskArchetype: string;
    policyName: string;
    reward: number;
    samples: number;
    avgOutcome: number;
    trustRate: number;
    quarantineRate: number;
  }>;
  learningDynamics: {
    outcomeCoverageRate: number;
    pendingOutcomeRate: number;
    delayedFeedbackRate: number;
    avgFeedbackLagDays?: number;
    confidenceCalibrationMae?: number;
    lowConfidenceRate: number;
  };
  calibration: {
    forecastSampleCount: number;
    forecastMae?: number;
    forecastDirectionalAccuracy?: number;
    catalystSampleCount: number;
    catalystBrier?: number;
    catalystImpactMaeBps?: number;
    openHighAlerts: number;
  };
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

const normalizePolicyRole = (policyRole?: string): "primary" | "shadow" => {
  const normalized = policyRole?.trim().toLowerCase();
  if (normalized === "shadow") return "shadow";
  return "primary";
};

const normalizeScore = (value?: number): number | undefined => {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return clamp(value, 0, 1);
};

const normalizePositiveInt = (value?: number): number | undefined => {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  if (value < 0) return undefined;
  return Math.round(value);
};

const normalizeSourceMix = (value?: SourceMix): SourceMix => {
  if (!value || typeof value !== "object") return {};
  const out: SourceMix = {};
  for (const [source, raw] of Object.entries(value)) {
    if (!source.trim()) continue;
    if (typeof raw !== "number" || !Number.isFinite(raw) || raw <= 0) continue;
    out[source.trim()] = raw;
  }
  return out;
};

const normalizeMetrics = (value?: NumericRecord): NumericRecord => {
  if (!value || typeof value !== "object") return {};
  const out: NumericRecord = {};
  for (const [name, raw] of Object.entries(value)) {
    if (!name.trim()) continue;
    if (typeof raw !== "number" || !Number.isFinite(raw)) continue;
    out[name.trim()] = raw;
  }
  return out;
};

const hashOutput = (value: string): string => createHash("sha256").update(value).digest("hex");

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

const calibrationScore = (params: {
  confidence?: number;
  userScore?: number;
  realizedOutcomeScore?: number;
}): number => {
  if (typeof params.confidence !== "number") return 0.5;
  if (typeof params.realizedOutcomeScore === "number") {
    return clamp(1 - Math.abs(params.confidence - params.realizedOutcomeScore), 0, 1);
  }
  if (typeof params.userScore === "number") {
    return clamp(1 - Math.abs(params.confidence - params.userScore), 0, 1);
  }
  return 0.6;
};

const gradeInvestment = (params: {
  confidence?: number;
  citationCount?: number;
  userScore?: number;
  realizedOutcomeScore?: number;
  gradingMetrics: NumericRecord;
}): GradeResult => {
  const contradictions = params.gradingMetrics.contradictions;
  const falsificationCount = params.gradingMetrics.falsification_count;
  const calibrationError = params.gradingMetrics.calibration_error;

  const evidenceScore =
    typeof params.citationCount === "number" ? clamp(params.citationCount / 8, 0, 1) : 0.45;
  const contradictionScore =
    typeof contradictions === "number" ? clamp(1 - contradictions * 0.35, 0, 1) : 0.7;
  const falsificationScore =
    typeof falsificationCount === "number" ? clamp(falsificationCount / 5, 0, 1) : 0.5;
  const calibrationFromError =
    typeof calibrationError === "number" ? clamp(1 - calibrationError / 0.35, 0, 1) : undefined;
  const calibration =
    typeof calibrationFromError === "number" ? calibrationFromError : calibrationScore(params);
  const user = typeof params.userScore === "number" ? params.userScore : 0.5;
  const realized =
    typeof params.realizedOutcomeScore === "number" ? params.realizedOutcomeScore : user;
  const score = clamp(
    0.2 * evidenceScore +
      0.2 * contradictionScore +
      0.15 * falsificationScore +
      0.2 * calibration +
      0.15 * realized +
      0.1 * user,
    0,
    1,
  );

  return {
    score,
    details: {
      evidence_score: evidenceScore,
      contradiction_score: contradictionScore,
      falsification_score: falsificationScore,
      calibration_score: calibration,
      user_signal: user,
      realized_signal: realized,
    },
  };
};

const gradeCoding = (params: {
  confidence?: number;
  userScore?: number;
  realizedOutcomeScore?: number;
  gradingMetrics: NumericRecord;
}): GradeResult => {
  const testsPassRate = params.gradingMetrics.tests_pass_rate;
  const regressions = params.gradingMetrics.regressions;
  const reviewFindings = params.gradingMetrics.review_findings;
  const rollbackRate = params.gradingMetrics.rollback_rate;

  const testsScore = typeof testsPassRate === "number" ? clamp(testsPassRate, 0, 1) : 0.55;
  const regressionScore = typeof regressions === "number" ? clamp(1 - regressions / 3, 0, 1) : 0.65;
  const reviewScore =
    typeof reviewFindings === "number" ? clamp(1 - reviewFindings / 8, 0, 1) : 0.65;
  const rollbackScore = typeof rollbackRate === "number" ? clamp(1 - rollbackRate, 0, 1) : 0.7;
  const calibration = calibrationScore(params);
  const user = typeof params.userScore === "number" ? params.userScore : 0.5;

  const score = clamp(
    0.25 * testsScore +
      0.2 * regressionScore +
      0.15 * reviewScore +
      0.15 * rollbackScore +
      0.15 * calibration +
      0.1 * user,
    0,
    1,
  );
  return {
    score,
    details: {
      tests_score: testsScore,
      regression_score: regressionScore,
      review_score: reviewScore,
      rollback_score: rollbackScore,
      calibration_score: calibration,
      user_signal: user,
    },
  };
};

const gradeOther = (params: {
  confidence?: number;
  userScore?: number;
  realizedOutcomeScore?: number;
}): GradeResult => {
  const confidence = typeof params.confidence === "number" ? params.confidence : 0.5;
  const user = typeof params.userScore === "number" ? params.userScore : 0.5;
  const realized =
    typeof params.realizedOutcomeScore === "number" ? params.realizedOutcomeScore : user;
  const calibration = calibrationScore(params);
  const score = clamp(0.35 * confidence + 0.25 * user + 0.2 * realized + 0.2 * calibration, 0, 1);
  return {
    score,
    details: {
      confidence_signal: confidence,
      user_signal: user,
      realized_signal: realized,
      calibration_score: calibration,
    },
  };
};

const gradeTask = (params: {
  taskType: LearningTaskType;
  confidence?: number;
  citationCount?: number;
  userScore?: number;
  realizedOutcomeScore?: number;
  gradingMetrics: NumericRecord;
}): GradeResult => {
  if (params.taskType === "investment") {
    return gradeInvestment(params);
  }
  if (params.taskType === "coding") {
    return gradeCoding(params);
  }
  return gradeOther(params);
};

const deriveStatus = (params: {
  score: number;
  userScore?: number;
  realizedOutcomeScore?: number;
}): { status: LearningStatus; reason: string } => {
  const userScore = params.userScore;
  const realized = params.realizedOutcomeScore;
  if (
    params.score >= 0.82 &&
    (typeof userScore !== "number" || userScore >= 0.6) &&
    (typeof realized !== "number" || realized >= 0.6)
  ) {
    return {
      status: "trusted",
      reason: "High graded quality with no negative feedback signals.",
    };
  }
  if (
    params.score < 0.55 ||
    (typeof userScore === "number" && userScore < 0.4) ||
    (typeof realized === "number" && realized < 0.4)
  ) {
    return {
      status: "quarantine",
      reason: "Low quality score or adverse feedback/outcome signal.",
    };
  }
  return {
    status: "pending",
    reason: "Needs more feedback or realized-outcome evidence before promotion.",
  };
};

const parseTaskOutcomeRow = (row: {
  id: number;
  task_type: string;
  task_archetype: string;
  policy_name: string;
  policy_role: string;
  experiment_group: string;
  ticker: string;
  repo_root: string;
  input_summary: string;
  output_hash: string;
  confidence?: number;
  citation_count?: number;
  latency_ms?: number;
  user_score?: number;
  realized_outcome_score?: number;
  outcome_label: string;
  source_mix?: string;
  grading_metrics?: string;
  grader_score: number;
  grader_details?: string;
  grader_version: string;
  status: string;
  status_reason: string;
  created_at: number;
  updated_at: number;
}): TaskOutcomeRecord => ({
  id: row.id,
  taskType: normalizeTaskType(row.task_type),
  taskArchetype: row.task_archetype,
  policyName: row.policy_name,
  policyRole: normalizePolicyRole(row.policy_role),
  experimentGroup: row.experiment_group,
  ticker: row.ticker,
  repoRoot: row.repo_root,
  inputSummary: row.input_summary,
  outputHash: row.output_hash,
  confidence: normalizeScore(row.confidence),
  citationCount: normalizePositiveInt(row.citation_count),
  latencyMs: normalizePositiveInt(row.latency_ms),
  userScore: normalizeScore(row.user_score),
  realizedOutcomeScore: normalizeScore(row.realized_outcome_score),
  outcomeLabel: row.outcome_label,
  sourceMix: normalizeSourceMix(parseJsonObject<SourceMix>(row.source_mix, {})),
  gradingMetrics: normalizeMetrics(parseJsonObject<NumericRecord>(row.grading_metrics, {})),
  graderScore: clamp(row.grader_score, 0, 1),
  graderDetails: normalizeMetrics(parseJsonObject<NumericRecord>(row.grader_details, {})),
  graderVersion: row.grader_version,
  status: row.status === "trusted" || row.status === "quarantine" ? row.status : "pending",
  statusReason: row.status_reason,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
});

const loadTaskOutcome = (id: number, dbPath?: string): TaskOutcomeRecord | undefined => {
  const db = openResearchDb(dbPath);
  const row = db
    .prepare(
      `SELECT
         id, task_type, task_archetype, policy_name, policy_role, experiment_group,
         ticker, repo_root, input_summary, output_hash,
         confidence, citation_count, latency_ms, user_score, realized_outcome_score, outcome_label,
         source_mix, grading_metrics, grader_score, grader_details, grader_version,
         status, status_reason, created_at, updated_at
       FROM task_outcomes
       WHERE id=?`,
    )
    .get(id) as
    | {
        id: number;
        task_type: string;
        task_archetype: string;
        policy_name: string;
        policy_role: string;
        experiment_group: string;
        ticker: string;
        repo_root: string;
        input_summary: string;
        output_hash: string;
        confidence?: number;
        citation_count?: number;
        latency_ms?: number;
        user_score?: number;
        realized_outcome_score?: number;
        outcome_label: string;
        source_mix?: string;
        grading_metrics?: string;
        grader_score: number;
        grader_details?: string;
        grader_version: string;
        status: string;
        status_reason: string;
        created_at: number;
        updated_at: number;
      }
    | undefined;
  return row ? parseTaskOutcomeRow(row) : undefined;
};

const evaluateForStorage = (params: {
  taskType: LearningTaskType;
  confidence?: number;
  citationCount?: number;
  userScore?: number;
  realizedOutcomeScore?: number;
  gradingMetrics: NumericRecord;
}) => {
  const grade = gradeTask(params);
  const status = deriveStatus({
    score: grade.score,
    userScore: params.userScore,
    realizedOutcomeScore: params.realizedOutcomeScore,
  });
  return {
    graderScore: grade.score,
    graderDetails: grade.details,
    status: status.status,
    statusReason: status.reason,
  };
};

export const logTaskOutcome = (
  params: LogTaskOutcomeParams,
): {
  id: number;
  taskType: LearningTaskType;
  graderScore: number;
  status: LearningStatus;
  statusReason: string;
  outputHash: string;
} => {
  const writeProvenance = (payload: Record<string, unknown>) => {
    try {
      appendProvenanceEvent({
        eventType: "task_outcome",
        entityType: "task_outcomes",
        entityId: payload.id as number | string | undefined,
        payload,
        metadata: {
          task_type: payload.task_type,
          task_archetype: payload.task_archetype,
        },
        dbPath: params.dbPath,
      });
    } catch {
      // Provenance should not block critical task logging path.
    }
  };

  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const existing =
    typeof params.id === "number" ? loadTaskOutcome(params.id, params.dbPath) : undefined;
  if (typeof params.id === "number" && !existing) {
    throw new Error(`Task outcome not found: id=${params.id}`);
  }

  const taskType = normalizeTaskType(params.taskType ?? existing?.taskType);
  const taskArchetype = (params.taskArchetype ?? existing?.taskArchetype ?? "").trim();
  const policyName = (params.policyName ?? existing?.policyName ?? "default").trim() || "default";
  const policyRole = normalizePolicyRole(params.policyRole ?? existing?.policyRole);
  const experimentGroup = (params.experimentGroup ?? existing?.experimentGroup ?? "").trim();
  const ticker = (params.ticker ?? existing?.ticker ?? "").trim().toUpperCase();
  const repoRoot = (params.repoRoot ?? existing?.repoRoot ?? "").trim();
  const inputSummary = (params.inputSummary ?? existing?.inputSummary ?? "").trim();
  const outputHash =
    (params.outputHash ?? "").trim() ||
    (params.outputText ? hashOutput(params.outputText) : "") ||
    existing?.outputHash ||
    hashOutput(`${taskType}|${taskArchetype}|${inputSummary}|${now}`);
  const confidence = normalizeScore(params.confidence ?? existing?.confidence);
  const citationCount = normalizePositiveInt(params.citationCount ?? existing?.citationCount);
  const latencyMs = normalizePositiveInt(params.latencyMs ?? existing?.latencyMs);
  const userScore = normalizeScore(params.userScore ?? existing?.userScore);
  const realizedOutcomeScore = normalizeScore(
    params.realizedOutcomeScore ?? existing?.realizedOutcomeScore,
  );
  const outcomeLabel = (params.outcomeLabel ?? existing?.outcomeLabel ?? "").trim();
  const sourceMix = normalizeSourceMix(params.sourceMix ?? existing?.sourceMix ?? {});
  const gradingMetrics = normalizeMetrics(params.gradingMetrics ?? existing?.gradingMetrics ?? {});
  const evaluated = evaluateForStorage({
    taskType,
    confidence,
    citationCount,
    userScore,
    realizedOutcomeScore,
    gradingMetrics,
  });

  if (existing) {
    db.prepare(
      `UPDATE task_outcomes
       SET task_type=?,
           task_archetype=?,
           policy_name=?,
           policy_role=?,
           experiment_group=?,
           ticker=?,
           repo_root=?,
           input_summary=?,
           output_hash=?,
           confidence=?,
           citation_count=?,
           latency_ms=?,
           user_score=?,
           realized_outcome_score=?,
           outcome_label=?,
           source_mix=?,
           grading_metrics=?,
           grader_score=?,
           grader_details=?,
           grader_version='v1',
           status=?,
           status_reason=?,
           updated_at=?
       WHERE id=?`,
    ).run(
      taskType,
      taskArchetype,
      policyName,
      policyRole,
      experimentGroup,
      ticker,
      repoRoot,
      inputSummary,
      outputHash,
      confidence ?? null,
      citationCount ?? null,
      latencyMs ?? null,
      userScore ?? null,
      realizedOutcomeScore ?? null,
      outcomeLabel,
      JSON.stringify(sourceMix),
      JSON.stringify(gradingMetrics),
      evaluated.graderScore,
      JSON.stringify(evaluated.graderDetails),
      evaluated.status,
      evaluated.statusReason,
      now,
      existing.id,
    );
    writeProvenance({
      id: existing.id,
      mode: "update",
      task_type: taskType,
      task_archetype: taskArchetype,
      policy_name: policyName,
      policy_role: policyRole,
      experiment_group: experimentGroup,
      ticker,
      repo_root: repoRoot,
      output_hash: outputHash,
      grader_score: evaluated.graderScore,
      status: evaluated.status,
      status_reason: evaluated.statusReason,
      updated_at: now,
    });
    return {
      id: existing.id,
      taskType,
      graderScore: evaluated.graderScore,
      status: evaluated.status,
      statusReason: evaluated.statusReason,
      outputHash,
    };
  }

  const row = db
    .prepare(
      `INSERT INTO task_outcomes (
         task_type, task_archetype, policy_name, policy_role, experiment_group,
         ticker, repo_root, input_summary, output_hash,
         confidence, citation_count, latency_ms, user_score, realized_outcome_score, outcome_label,
         source_mix, grading_metrics, grader_score, grader_details, grader_version,
         status, status_reason, created_at, updated_at
       ) VALUES (
         ?, ?, ?, ?, ?,
         ?, ?, ?, ?,
         ?, ?, ?, ?, ?, ?,
         ?, ?, ?, ?, 'v1',
         ?, ?, ?, ?
       )
       RETURNING id`,
    )
    .get(
      taskType,
      taskArchetype,
      policyName,
      policyRole,
      experimentGroup,
      ticker,
      repoRoot,
      inputSummary,
      outputHash,
      confidence ?? null,
      citationCount ?? null,
      latencyMs ?? null,
      userScore ?? null,
      realizedOutcomeScore ?? null,
      outcomeLabel,
      JSON.stringify(sourceMix),
      JSON.stringify(gradingMetrics),
      evaluated.graderScore,
      JSON.stringify(evaluated.graderDetails),
      evaluated.status,
      evaluated.statusReason,
      now,
      now,
    ) as { id: number };

  writeProvenance({
    id: row.id,
    mode: "insert",
    task_type: taskType,
    task_archetype: taskArchetype,
    policy_name: policyName,
    policy_role: policyRole,
    experiment_group: experimentGroup,
    ticker,
    repo_root: repoRoot,
    output_hash: outputHash,
    grader_score: evaluated.graderScore,
    status: evaluated.status,
    status_reason: evaluated.statusReason,
    created_at: now,
  });

  return {
    id: row.id,
    taskType,
    graderScore: evaluated.graderScore,
    status: evaluated.status,
    statusReason: evaluated.statusReason,
    outputHash,
  };
};

export const refreshTaskOutcomeGrades = (
  params: {
    days?: number;
    dbPath?: string;
  } = {},
) => {
  const db = openResearchDb(params.dbPath);
  const lookbackMs = Math.max(1, params.days ?? 365) * 86_400_000;
  const cutoff = Date.now() - lookbackMs;
  const rows = db
    .prepare(
      `SELECT
         id, task_type, confidence, citation_count, user_score, realized_outcome_score,
         grading_metrics
       FROM task_outcomes
       WHERE created_at >= ?`,
    )
    .all(cutoff) as Array<{
    id: number;
    task_type: string;
    confidence?: number;
    citation_count?: number;
    user_score?: number;
    realized_outcome_score?: number;
    grading_metrics?: string;
  }>;

  let updated = 0;
  for (const row of rows) {
    const taskType = normalizeTaskType(row.task_type);
    const gradingMetrics = normalizeMetrics(
      parseJsonObject<NumericRecord>(row.grading_metrics, {}),
    );
    const evaluated = evaluateForStorage({
      taskType,
      confidence: normalizeScore(row.confidence),
      citationCount: normalizePositiveInt(row.citation_count),
      userScore: normalizeScore(row.user_score),
      realizedOutcomeScore: normalizeScore(row.realized_outcome_score),
      gradingMetrics,
    });
    db.prepare(
      `UPDATE task_outcomes
       SET grader_score=?,
           grader_details=?,
           status=?,
           status_reason=?,
           grader_version='v1',
           updated_at=?
       WHERE id=?`,
    ).run(
      evaluated.graderScore,
      JSON.stringify(evaluated.graderDetails),
      evaluated.status,
      evaluated.statusReason,
      Date.now(),
      row.id,
    );
    updated += 1;
  }

  return {
    scanned: rows.length,
    updated,
  };
};

type SourceAccumulator = {
  weightedScore: number;
  weightedCount: number;
};

const addSourceContribution = (
  acc: Map<string, SourceAccumulator>,
  sourceMix: SourceMix,
  score: number,
) => {
  const entries = Object.entries(sourceMix).filter(([, value]) => value > 0);
  if (!entries.length) return;
  const total = entries.reduce((sum, [, value]) => sum + value, 0);
  if (total <= 1e-9) return;
  for (const [source, value] of entries) {
    const weight = value / total;
    const current = acc.get(source) ?? { weightedScore: 0, weightedCount: 0 };
    current.weightedScore += weight * score;
    current.weightedCount += weight;
    acc.set(source, current);
  }
};

const sourceRanking = (
  acc: Map<string, SourceAccumulator>,
  limit: number,
): Array<{ source: string; score: number; samples: number }> =>
  Array.from(acc.entries())
    .map(([source, data]) => ({
      source,
      score: data.weightedCount > 1e-9 ? data.weightedScore / data.weightedCount : 0,
      samples: Math.round(data.weightedCount * 100) / 100,
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);

const loadRecentTaskOutcomes = (params: LearningReportParams): TaskOutcomeRecord[] => {
  const db = openResearchDb(params.dbPath);
  const lookbackDays = Math.max(1, params.days ?? 30);
  const cutoff = Date.now() - lookbackDays * 86_400_000;
  const rows = db
    .prepare(
      `SELECT
         id, task_type, task_archetype, policy_name, policy_role, experiment_group,
         ticker, repo_root, input_summary, output_hash,
         confidence, citation_count, latency_ms, user_score, realized_outcome_score, outcome_label,
         source_mix, grading_metrics, grader_score, grader_details, grader_version,
         status, status_reason, created_at, updated_at
       FROM task_outcomes
       WHERE created_at >= ?
         AND (? = '' OR task_type = ?)
       ORDER BY created_at DESC`,
    )
    .all(
      cutoff,
      params.taskType ? normalizeTaskType(params.taskType) : "",
      params.taskType ? normalizeTaskType(params.taskType) : "",
    ) as Array<{
    id: number;
    task_type: string;
    task_archetype: string;
    policy_name: string;
    policy_role: string;
    experiment_group: string;
    ticker: string;
    repo_root: string;
    input_summary: string;
    output_hash: string;
    confidence?: number;
    citation_count?: number;
    latency_ms?: number;
    user_score?: number;
    realized_outcome_score?: number;
    outcome_label: string;
    source_mix?: string;
    grading_metrics?: string;
    grader_score: number;
    grader_details?: string;
    grader_version: string;
    status: string;
    status_reason: string;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map(parseTaskOutcomeRow);
};

const loadCalibrationSnapshot = (dbPath?: string) => {
  const forecast = forecastDecisionMetrics({ dbPath });
  const db = openResearchDb(dbPath);
  const catalystRows = db
    .prepare(
      `SELECT c.probability, o.occurred, c.impact_bps, c.confidence, o.realized_impact_bps
       FROM catalysts c
       JOIN catalyst_outcomes o ON o.catalyst_id=c.id
       ORDER BY o.resolved_at DESC
       LIMIT 300`,
    )
    .all() as Array<{
    probability: number;
    occurred: number;
    impact_bps: number;
    confidence: number;
    realized_impact_bps?: number | null;
  }>;
  const catalystSampleCount = catalystRows.length;
  const catalystBrier =
    catalystRows.length > 0
      ? catalystRows.reduce((sum, row) => {
          const p = clamp(row.probability, 0, 1);
          const y = row.occurred ? 1 : 0;
          return sum + (p - y) ** 2;
        }, 0) / catalystRows.length
      : undefined;
  const realizedImpactRows = catalystRows.filter(
    (row) =>
      typeof row.realized_impact_bps === "number" && Number.isFinite(row.realized_impact_bps),
  );
  const catalystImpactMaeBps =
    realizedImpactRows.length > 0
      ? realizedImpactRows.reduce((sum, row) => {
          const predicted = row.impact_bps * row.confidence;
          return sum + Math.abs(predicted - (row.realized_impact_bps as number));
        }, 0) / realizedImpactRows.length
      : undefined;
  const openHighAlerts = (
    db
      .prepare(`SELECT COUNT(*) AS count FROM thesis_alerts WHERE resolved=0 AND severity='high'`)
      .get() as { count: number }
  ).count;

  return {
    forecastSampleCount: forecast.count,
    forecastMae: forecast.mae,
    forecastDirectionalAccuracy: forecast.directionalAccuracy,
    catalystSampleCount,
    catalystBrier,
    catalystImpactMaeBps,
    openHighAlerts,
  };
};

export const learningReport = (params: LearningReportParams = {}): LearningReport => {
  const lookbackDays = Math.max(1, params.days ?? 30);
  const minSamples = Math.max(1, params.minSamples ?? 3);
  const rows = loadRecentTaskOutcomes({ ...params, days: lookbackDays });
  const byType = new Map<LearningTaskType, TaskOutcomeRecord[]>();
  const byTypeArchetype = new Map<string, TaskOutcomeRecord[]>();
  const globalSourceAcc = new Map<string, SourceAccumulator>();
  const typeSourceAcc = new Map<LearningTaskType, Map<string, SourceAccumulator>>();

  for (const row of rows) {
    const typeRows = byType.get(row.taskType) ?? [];
    typeRows.push(row);
    byType.set(row.taskType, typeRows);

    const archetypeKey = `${row.taskType}|${row.taskArchetype || "default"}`;
    const archetypeRows = byTypeArchetype.get(archetypeKey) ?? [];
    archetypeRows.push(row);
    byTypeArchetype.set(archetypeKey, archetypeRows);

    const outcomeSignal = row.realizedOutcomeScore ?? row.userScore ?? row.graderScore;
    addSourceContribution(globalSourceAcc, row.sourceMix, outcomeSignal);
    const taskSource = typeSourceAcc.get(row.taskType) ?? new Map<string, SourceAccumulator>();
    addSourceContribution(taskSource, row.sourceMix, outcomeSignal);
    typeSourceAcc.set(row.taskType, taskSource);
  }

  const byTaskType = Array.from(byType.entries()).map(([taskType, taskRows]) => {
    const scores = taskRows.map((row) => row.graderScore);
    const trusted = taskRows.filter((row) => row.status === "trusted").length;
    const quarantined = taskRows.filter((row) => row.status === "quarantine").length;
    const wins = taskRows.filter(
      (row) => (row.realizedOutcomeScore ?? row.userScore ?? 0) >= 0.7,
    ).length;
    return {
      taskType,
      count: taskRows.length,
      avgScore: mean(scores) ?? 0,
      trustedRate: taskRows.length ? trusted / taskRows.length : 0,
      quarantineRate: taskRows.length ? quarantined / taskRows.length : 0,
      winRate: taskRows.length ? wins / taskRows.length : 0,
    };
  });

  const routing: RoutingRecommendation[] = Array.from(byType.entries()).map(
    ([taskType, taskRows]) => {
      const candidates = Array.from(byTypeArchetype.entries())
        .filter(([key]) => key.startsWith(`${taskType}|`))
        .map(([key, archetypeRows]) => {
          const archetype = key.split("|")[1] || "";
          const outcomeSignals = archetypeRows.map(
            (row) => row.realizedOutcomeScore ?? row.userScore ?? row.graderScore,
          );
          const wins = outcomeSignals.filter((signal) => signal >= 0.7).length;
          return {
            archetype,
            samples: archetypeRows.length,
            avgScore: mean(archetypeRows.map((row) => row.graderScore)) ?? 0,
            winRate: archetypeRows.length ? wins / archetypeRows.length : 0,
          };
        })
        .filter((entry) => entry.samples >= minSamples)
        .sort((a, b) => {
          if (b.winRate !== a.winRate) return b.winRate - a.winRate;
          return b.avgScore - a.avgScore;
        });
      const best = candidates[0];
      return {
        taskType,
        bestArchetype: best?.archetype,
        archetypeSampleCount: best?.samples,
        archetypeWinRate: best?.winRate,
        archetypeAvgScore: best?.avgScore,
        topSources: sourceRanking(typeSourceAcc.get(taskType) ?? new Map(), 4),
      };
    },
  );

  const avgGraderScore = mean(rows.map((row) => row.graderScore));
  const avgUserScore = mean(
    rows.map((row) => row.userScore).filter((value): value is number => typeof value === "number"),
  );
  const avgRealizedOutcomeScore = mean(
    rows
      .map((row) => row.realizedOutcomeScore)
      .filter((value): value is number => typeof value === "number"),
  );
  const trusted = rows.filter((row) => row.status === "trusted").length;
  const quarantined = rows.filter((row) => row.status === "quarantine").length;
  const rowsWithOutcome = rows.filter(
    (row) => typeof row.realizedOutcomeScore === "number" || typeof row.userScore === "number",
  );
  const rowsWithResolvedOutcome = rows.filter(
    (row) => typeof row.realizedOutcomeScore === "number",
  );
  const pendingOutcomeRate =
    rows.length > 0 ? (rows.length - rowsWithOutcome.length) / rows.length : 0;
  const delayedFeedbackRows = rowsWithResolvedOutcome.filter(
    (row) => row.updatedAt - row.createdAt > 6 * 60 * 60 * 1000,
  );
  const delayedFeedbackRate =
    rowsWithResolvedOutcome.length > 0
      ? delayedFeedbackRows.length / rowsWithResolvedOutcome.length
      : 0;
  const avgFeedbackLagDays = mean(
    rowsWithResolvedOutcome.map((row) => Math.max(0, row.updatedAt - row.createdAt) / 86_400_000),
  );
  const confidenceCalibrationMae = mean(
    rows
      .map((row) => {
        if (typeof row.confidence !== "number") return undefined;
        const outcome = row.realizedOutcomeScore ?? row.userScore;
        if (typeof outcome !== "number") return undefined;
        return Math.abs(row.confidence - outcome);
      })
      .filter((value): value is number => typeof value === "number"),
  );
  const lowConfidenceRate =
    rows.length > 0
      ? rows.filter((row) => (row.confidence ?? row.graderScore) < 0.55).length / rows.length
      : 0;

  const rewardBuckets = new Map<
    string,
    {
      taskType: LearningTaskType;
      taskArchetype: string;
      policyName: string;
      samples: number;
      outcomeSum: number;
      trust: number;
      quarantine: number;
      scoreSum: number;
    }
  >();
  for (const row of rows) {
    if (!row.policyName.trim()) continue;
    const key = `${row.taskType}|${row.taskArchetype || "default"}|${row.policyName}`;
    const bucket = rewardBuckets.get(key) ?? {
      taskType: row.taskType,
      taskArchetype: row.taskArchetype || "default",
      policyName: row.policyName,
      samples: 0,
      outcomeSum: 0,
      trust: 0,
      quarantine: 0,
      scoreSum: 0,
    };
    bucket.samples += 1;
    bucket.scoreSum += row.graderScore;
    bucket.outcomeSum += row.realizedOutcomeScore ?? row.userScore ?? row.graderScore;
    if (row.status === "trusted") bucket.trust += 1;
    if (row.status === "quarantine") bucket.quarantine += 1;
    rewardBuckets.set(key, bucket);
  }
  const rewardModels = Array.from(rewardBuckets.values())
    .filter((bucket) => bucket.samples >= minSamples)
    .map((bucket) => {
      const avgOutcome = bucket.outcomeSum / bucket.samples;
      const avgScore = bucket.scoreSum / bucket.samples;
      const trustRate = bucket.trust / bucket.samples;
      const quarantineRate = bucket.quarantine / bucket.samples;
      const reward = clamp(
        0.45 * avgScore + 0.35 * avgOutcome + 0.12 * trustRate + 0.08 * (1 - quarantineRate),
        0,
        1,
      );
      return {
        taskType: bucket.taskType,
        taskArchetype: bucket.taskArchetype,
        policyName: bucket.policyName,
        reward,
        samples: bucket.samples,
        avgOutcome,
        trustRate,
        quarantineRate,
      };
    })
    .sort((a, b) => b.reward - a.reward)
    .slice(0, 30);

  return {
    generatedAt: new Date().toISOString(),
    lookbackDays,
    totalTasks: rows.length,
    avgGraderScore,
    avgUserScore,
    avgRealizedOutcomeScore,
    trustedRate: rows.length ? trusted / rows.length : undefined,
    quarantineRate: rows.length ? quarantined / rows.length : undefined,
    byTaskType,
    routing,
    sourceEffectiveness: sourceRanking(globalSourceAcc, 8),
    rewardModels,
    learningDynamics: {
      outcomeCoverageRate: rows.length > 0 ? rowsWithOutcome.length / rows.length : 0,
      pendingOutcomeRate,
      delayedFeedbackRate,
      avgFeedbackLagDays,
      confidenceCalibrationMae,
      lowConfidenceRate,
    },
    calibration: loadCalibrationSnapshot(params.dbPath),
  };
};

export const runLearningCalibration = (
  params: {
    days?: number;
    minSamples?: number;
    dbPath?: string;
  } = {},
) => {
  const forecastResolution = resolveMatureForecasts({ dbPath: params.dbPath });
  const refresh = refreshTaskOutcomeGrades({ days: params.days ?? 365, dbPath: params.dbPath });
  const report = learningReport({
    days: params.days ?? 90,
    minSamples: params.minSamples ?? 3,
    dbPath: params.dbPath,
  });
  return {
    forecastResolution,
    refresh,
    report,
  };
};
