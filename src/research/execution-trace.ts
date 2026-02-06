import { createHash } from "node:crypto";
import type { LearningTaskType } from "./learning.js";
import { openResearchDb } from "./db.js";
import { appendProvenanceEvent } from "./provenance.js";

export type ExecutionTraceStepInput = {
  seq?: number;
  toolName: string;
  action?: string;
  status?: string;
  latencyMs?: number;
  retries?: number;
  errorType?: string;
  inputHash?: string;
  outputHash?: string;
  details?: Record<string, unknown>;
};

type NormalizedStep = {
  seq: number;
  toolName: string;
  action: string;
  status: string;
  latencyMs: number;
  retries: number;
  errorType: string;
  inputHash: string;
  outputHash: string;
  details: Record<string, unknown>;
};

export type LogExecutionTraceParams = {
  taskType?: LearningTaskType | string;
  taskArchetype?: string;
  policyName?: string;
  policyRole?: "primary" | "shadow" | string;
  experimentGroup?: string;
  ticker?: string;
  repoRoot?: string;
  seed?: string;
  traceHash?: string;
  outputText?: string;
  outputHash?: string;
  success?: boolean;
  retryCount?: number;
  errorCount?: number;
  timeoutCount?: number;
  totalLatencyMs?: number;
  metadata?: Record<string, unknown>;
  startedAt?: number;
  completedAt?: number;
  steps?: ExecutionTraceStepInput[];
  dbPath?: string;
};

export type ExecutionTraceRecord = {
  id: number;
  taskType: LearningTaskType;
  taskArchetype: string;
  policyName: string;
  policyRole: "primary" | "shadow";
  experimentGroup: string;
  ticker: string;
  repoRoot: string;
  seed: string;
  traceHash: string;
  outputHash: string;
  success: boolean;
  stepCount: number;
  retryCount: number;
  errorCount: number;
  timeoutCount: number;
  totalLatencyMs: number;
  metadata: Record<string, unknown>;
  startedAt: number;
  completedAt: number;
  createdAt: number;
};

export type ExecutionReliabilitySummary = {
  traceCount: number;
  completionRate: number;
  timeoutRate: number;
  reproducibilityScore: number;
  reproducibilitySampleCount: number;
  avgRetries: number;
  avgErrors: number;
  avgLatencyMs?: number;
};

export type ExecutionTraceReport = {
  generatedAt: string;
  lookbackDays: number;
  taskType: LearningTaskType;
  taskArchetype: string;
  policyName: string;
  ticker?: string;
  repoRoot?: string;
  summary: ExecutionReliabilitySummary;
  seedStats: Array<{
    seed: string;
    runCount: number;
    stableRatio: number;
    latestAt: number;
    latestOutputHash: string;
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

const normalizePolicyRole = (value?: string): "primary" | "shadow" => {
  const normalized = value?.trim().toLowerCase();
  if (normalized === "shadow") return "shadow";
  return "primary";
};

const normalizeInt = (value: unknown): number | undefined => {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  if (value < 0) return 0;
  return Math.round(value);
};

const normalizeObject = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
};

const normalizeStepStatus = (value?: string): string => {
  const normalized = (value ?? "").trim().toLowerCase();
  if (!normalized) return "ok";
  if (["ok", "error", "timeout", "retry", "skipped"].includes(normalized)) return normalized;
  return normalized;
};

const hashValue = (value: string): string => createHash("sha256").update(value).digest("hex");

const normalizeSteps = (steps: ExecutionTraceStepInput[] | undefined): NormalizedStep[] => {
  const safe = Array.isArray(steps) ? steps : [];
  return safe
    .map((step, index) => ({
      seq: normalizeInt(step.seq) ?? index + 1,
      toolName: (step.toolName ?? "").trim(),
      action: (step.action ?? "").trim(),
      status: normalizeStepStatus(step.status),
      latencyMs: normalizeInt(step.latencyMs) ?? 0,
      retries: normalizeInt(step.retries) ?? 0,
      errorType: (step.errorType ?? "").trim(),
      inputHash: (step.inputHash ?? "").trim(),
      outputHash: (step.outputHash ?? "").trim(),
      details: normalizeObject(step.details),
    }))
    .filter((step) => step.toolName);
};

const traceHashFor = (params: {
  taskType: LearningTaskType;
  taskArchetype: string;
  policyName: string;
  seed: string;
  outputHash: string;
  success: boolean;
  steps: NormalizedStep[];
}): string => {
  const payload = JSON.stringify({
    task_type: params.taskType,
    task_archetype: params.taskArchetype,
    policy_name: params.policyName,
    seed: params.seed,
    output_hash: params.outputHash,
    success: params.success,
    steps: params.steps.map((step) => ({
      seq: step.seq,
      tool: step.toolName,
      action: step.action,
      status: step.status,
      retries: step.retries,
      latency_ms: step.latencyMs,
      error_type: step.errorType,
      input_hash: step.inputHash,
      output_hash: step.outputHash,
    })),
  });
  return hashValue(payload);
};

const runStableRatio = (outputHashes: string[]): number => {
  if (!outputHashes.length) return 0;
  const counts = new Map<string, number>();
  for (const outputHash of outputHashes) {
    const key = outputHash || "missing";
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  const topCount = Math.max(...counts.values());
  return topCount / outputHashes.length;
};

const loadTraceRows = (params: {
  taskType: LearningTaskType;
  taskArchetype?: string;
  policyName: string;
  ticker?: string;
  repoRoot?: string;
  lookbackDays: number;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  const cutoff = Date.now() - Math.max(1, params.lookbackDays) * 86_400_000;
  return db
    .prepare(
      `SELECT
         id, seed, output_hash, success, retry_count, error_count, timeout_count,
         total_latency_ms, created_at, completed_at
       FROM execution_traces
       WHERE task_type=?
         AND policy_name=?
         AND created_at>=?
         AND (? = '' OR task_archetype = ?)
         AND (? = '' OR ticker = ?)
         AND (? = '' OR repo_root = ?)
       ORDER BY created_at DESC`,
    )
    .all(
      params.taskType,
      params.policyName,
      cutoff,
      (params.taskArchetype ?? "").trim(),
      (params.taskArchetype ?? "").trim(),
      (params.ticker ?? "").trim().toUpperCase(),
      (params.ticker ?? "").trim().toUpperCase(),
      (params.repoRoot ?? "").trim(),
      (params.repoRoot ?? "").trim(),
    ) as Array<{
    id: number;
    seed: string;
    output_hash: string;
    success: number;
    retry_count: number;
    error_count: number;
    timeout_count: number;
    total_latency_ms: number;
    created_at: number;
    completed_at: number;
  }>;
};

const summarizeTraceRows = (
  rows: Array<{
    seed: string;
    output_hash: string;
    success: number;
    retry_count: number;
    error_count: number;
    timeout_count: number;
    total_latency_ms: number;
  }>,
): ExecutionReliabilitySummary => {
  if (!rows.length) {
    return {
      traceCount: 0,
      completionRate: 0,
      timeoutRate: 0,
      reproducibilityScore: 0,
      reproducibilitySampleCount: 0,
      avgRetries: 0,
      avgErrors: 0,
      avgLatencyMs: undefined,
    };
  }

  const traceCount = rows.length;
  const completionRate = rows.filter((row) => row.success === 1).length / traceCount;
  const timeoutRate = rows.filter((row) => row.timeout_count > 0).length / traceCount;
  const avgRetries = mean(rows.map((row) => Math.max(0, row.retry_count))) ?? 0;
  const avgErrors = mean(rows.map((row) => Math.max(0, row.error_count))) ?? 0;
  const avgLatencyMs = mean(rows.map((row) => Math.max(0, row.total_latency_ms)));

  const bySeed = new Map<string, string[]>();
  for (const row of rows) {
    const seed = row.seed.trim();
    if (!seed) continue;
    const group = bySeed.get(seed) ?? [];
    group.push((row.output_hash ?? "").trim());
    bySeed.set(seed, group);
  }

  let reproducibilitySampleCount = 0;
  let reproducibilityWeighted = 0;
  for (const outputs of bySeed.values()) {
    if (outputs.length < 2) continue;
    const stableRatio = runStableRatio(outputs);
    reproducibilityWeighted += stableRatio * outputs.length;
    reproducibilitySampleCount += outputs.length;
  }
  const reproducibilityScore =
    reproducibilitySampleCount > 0
      ? clamp(reproducibilityWeighted / reproducibilitySampleCount, 0, 1)
      : rows.length > 0
        ? 0.5
        : 0;

  return {
    traceCount,
    completionRate: clamp(completionRate, 0, 1),
    timeoutRate: clamp(timeoutRate, 0, 1),
    reproducibilityScore,
    reproducibilitySampleCount,
    avgRetries: Math.max(0, avgRetries),
    avgErrors: Math.max(0, avgErrors),
    avgLatencyMs,
  };
};

export const logExecutionTrace = (params: LogExecutionTraceParams): ExecutionTraceRecord => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const taskType = normalizeTaskType(params.taskType);
  const taskArchetype = (params.taskArchetype ?? "").trim();
  const policyName = (params.policyName ?? "default").trim() || "default";
  const policyRole = normalizePolicyRole(params.policyRole);
  const experimentGroup = (params.experimentGroup ?? "").trim();
  const ticker = (params.ticker ?? "").trim().toUpperCase();
  const repoRoot = (params.repoRoot ?? "").trim();
  const seed = (params.seed ?? "").trim();
  const steps = normalizeSteps(params.steps);
  const retryCount =
    normalizeInt(params.retryCount) ??
    steps.reduce((sum, step) => sum + Math.max(0, step.retries), 0);
  const timeoutCount =
    normalizeInt(params.timeoutCount) ?? steps.filter((step) => step.status === "timeout").length;
  const errorCount =
    normalizeInt(params.errorCount) ?? steps.filter((step) => step.status === "error").length;
  const totalLatencyMs =
    normalizeInt(params.totalLatencyMs) ??
    steps.reduce((sum, step) => sum + Math.max(0, step.latencyMs), 0);
  const outputHash =
    (params.outputHash ?? "").trim() ||
    (params.outputText?.trim() ? hashValue(params.outputText.trim()) : "");
  const success = params.success ?? (errorCount === 0 && timeoutCount === 0);
  const traceHash =
    (params.traceHash ?? "").trim() ||
    traceHashFor({
      taskType,
      taskArchetype,
      policyName,
      seed,
      outputHash,
      success,
      steps,
    });
  const startedAt =
    normalizeInt(params.startedAt) ??
    (normalizeInt(params.completedAt) ?? now) - Math.max(0, totalLatencyMs);
  const completedAt = Math.max(startedAt, normalizeInt(params.completedAt) ?? now);
  const metadata = normalizeObject(params.metadata);

  db.exec("BEGIN");
  try {
    const row = db
      .prepare(
        `INSERT INTO execution_traces (
           task_type, task_archetype, policy_name, policy_role, experiment_group,
           ticker, repo_root, seed, trace_hash, output_hash, success, step_count,
           retry_count, error_count, timeout_count, total_latency_ms, metadata,
           started_at, completed_at, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        seed,
        traceHash,
        outputHash,
        success ? 1 : 0,
        steps.length,
        retryCount,
        errorCount,
        timeoutCount,
        totalLatencyMs,
        JSON.stringify(metadata),
        startedAt,
        completedAt,
        now,
      ) as { id: number };
    const traceId = row.id;

    const insertStep = db.prepare(
      `INSERT INTO execution_trace_steps (
         trace_id, seq, tool_name, action, status, latency_ms, retries, error_type,
         input_hash, output_hash, details, created_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    steps.forEach((step, index) => {
      insertStep.run(
        traceId,
        step.seq || index + 1,
        step.toolName,
        step.action,
        step.status,
        step.latencyMs,
        step.retries,
        step.errorType,
        step.inputHash,
        step.outputHash,
        JSON.stringify(step.details),
        now,
      );
    });

    db.exec("COMMIT");
    try {
      appendProvenanceEvent({
        eventType: "execution_trace",
        entityType: "execution_traces",
        entityId: traceId,
        payload: {
          trace_id: traceId,
          task_type: taskType,
          task_archetype: taskArchetype,
          policy_name: policyName,
          policy_role: policyRole,
          experiment_group: experimentGroup,
          ticker,
          repo_root: repoRoot,
          seed,
          trace_hash: traceHash,
          output_hash: outputHash,
          success: success ? 1 : 0,
          step_count: steps.length,
          retry_count: retryCount,
          error_count: errorCount,
          timeout_count: timeoutCount,
          total_latency_ms: totalLatencyMs,
          completed_at: completedAt,
        },
        metadata: {
          source: "execution_trace",
        },
        dbPath: params.dbPath,
      });
    } catch {
      // Keep trace pipeline non-blocking if provenance write fails.
    }
    return {
      id: traceId,
      taskType,
      taskArchetype,
      policyName,
      policyRole,
      experimentGroup,
      ticker,
      repoRoot,
      seed,
      traceHash,
      outputHash,
      success,
      stepCount: steps.length,
      retryCount,
      errorCount,
      timeoutCount,
      totalLatencyMs,
      metadata,
      startedAt,
      completedAt,
      createdAt: now,
    };
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
};

export const executionReliabilitySummary = (params: {
  taskType: LearningTaskType | string;
  taskArchetype?: string;
  policyName: string;
  ticker?: string;
  repoRoot?: string;
  lookbackDays?: number;
  dbPath?: string;
}): ExecutionReliabilitySummary => {
  const taskType = normalizeTaskType(params.taskType);
  const rows = loadTraceRows({
    taskType,
    taskArchetype: params.taskArchetype,
    policyName: params.policyName.trim(),
    ticker: params.ticker,
    repoRoot: params.repoRoot,
    lookbackDays: Math.max(1, params.lookbackDays ?? 90),
    dbPath: params.dbPath,
  });
  return summarizeTraceRows(rows);
};

export const executionTraceReport = (params: {
  taskType: LearningTaskType | string;
  taskArchetype?: string;
  policyName: string;
  ticker?: string;
  repoRoot?: string;
  lookbackDays?: number;
  seedLimit?: number;
  dbPath?: string;
}): ExecutionTraceReport => {
  const taskType = normalizeTaskType(params.taskType);
  const lookbackDays = Math.max(1, params.lookbackDays ?? 90);
  const rows = loadTraceRows({
    taskType,
    taskArchetype: params.taskArchetype,
    policyName: params.policyName.trim(),
    ticker: params.ticker,
    repoRoot: params.repoRoot,
    lookbackDays,
    dbPath: params.dbPath,
  });
  const seedMap = new Map<
    string,
    {
      outputs: string[];
      latestAt: number;
      latestOutputHash: string;
    }
  >();
  for (const row of rows) {
    const seed = row.seed.trim();
    if (!seed) continue;
    const current = seedMap.get(seed) ?? {
      outputs: [],
      latestAt: 0,
      latestOutputHash: "",
    };
    current.outputs.push((row.output_hash ?? "").trim());
    if (row.completed_at >= current.latestAt) {
      current.latestAt = row.completed_at;
      current.latestOutputHash = (row.output_hash ?? "").trim();
    }
    seedMap.set(seed, current);
  }
  const seedLimit = Math.max(1, normalizeInt(params.seedLimit) ?? 20);
  const seedStats = Array.from(seedMap.entries())
    .map(([seed, value]) => ({
      seed,
      runCount: value.outputs.length,
      stableRatio: runStableRatio(value.outputs),
      latestAt: value.latestAt,
      latestOutputHash: value.latestOutputHash,
    }))
    .sort((a, b) => b.latestAt - a.latestAt)
    .slice(0, seedLimit);

  return {
    generatedAt: new Date().toISOString(),
    lookbackDays,
    taskType,
    taskArchetype: (params.taskArchetype ?? "").trim(),
    policyName: params.policyName.trim(),
    ticker: (params.ticker ?? "").trim().toUpperCase() || undefined,
    repoRoot: (params.repoRoot ?? "").trim() || undefined,
    summary: summarizeTraceRows(rows),
    seedStats,
  };
};
