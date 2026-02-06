import { createHash } from "node:crypto";
import type { LearningTaskType } from "./learning.js";
import { openResearchDb } from "./db.js";

export type PolicyVariantStatus = "champion" | "challenger" | "retired";
export type PolicyRole = "primary" | "shadow";

export type PolicyVariant = {
  id: number;
  taskType: LearningTaskType;
  taskArchetype: string;
  policyName: string;
  status: PolicyVariantStatus;
  active: boolean;
  trafficWeight: number;
  shadowWeight: number;
  minSamples: number;
  minLift: number;
  maxQuarantineRate: number;
  maxCalibrationError: number;
  metadata: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type RegisterPolicyVariantParams = {
  taskType: LearningTaskType | string;
  taskArchetype?: string;
  policyName: string;
  status?: PolicyVariantStatus;
  active?: boolean;
  trafficWeight?: number;
  shadowWeight?: number;
  minSamples?: number;
  minLift?: number;
  maxQuarantineRate?: number;
  maxCalibrationError?: number;
  metadata?: Record<string, unknown>;
  dbPath?: string;
};

type VariantPerformance = {
  taskType: LearningTaskType;
  taskArchetype: string;
  policyName: string;
  status: PolicyVariantStatus;
  sampleCount: number;
  primarySampleCount: number;
  shadowSampleCount: number;
  score?: number;
  winRate?: number;
  quarantineRate?: number;
  calibrationError?: number;
  avgGraderScore?: number;
  avgOutcomeSignal?: number;
};

type PolicyDecision = {
  id: number;
  taskType: LearningTaskType;
  taskArchetype: string;
  decisionType: "promote" | "rollback" | "hold";
  championBefore: string;
  championAfter: string;
  challenger: string;
  reason: string;
  metrics: Record<string, unknown>;
  createdAt: number;
};

type GovernanceResult = {
  promoted: number;
  rolledBack: number;
  held: number;
  decisions: PolicyDecision[];
};

type PolicyReport = {
  generatedAt: string;
  lookbackDays: number;
  taskType: LearningTaskType;
  taskArchetype: string;
  champion?: string;
  variants: VariantPerformance[];
  recentDecisions: PolicyDecision[];
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

const normalizeStatus = (status?: string): PolicyVariantStatus => {
  const normalized = status?.trim().toLowerCase();
  if (normalized === "champion") return "champion";
  if (normalized === "retired") return "retired";
  return "challenger";
};

const parseMetadata = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
};

const parseJsonMetadata = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "string" || !value.trim()) return {};
  try {
    const parsed = JSON.parse(value) as unknown;
    return parseMetadata(parsed);
  } catch {
    return {};
  }
};

const parseRole = (value?: string): PolicyRole => {
  const normalized = value?.trim().toLowerCase();
  if (normalized === "shadow") return "shadow";
  return "primary";
};

const toFiniteOrUndefined = (value: unknown): number | undefined => {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return value;
};

const toUnit = (seed: string): number => {
  const hex = createHash("sha256").update(seed).digest("hex").slice(0, 12);
  const value = Number.parseInt(hex, 16);
  return value / 0xffffffffffff;
};

const parseVariantRow = (row: {
  id: number;
  task_type: string;
  task_archetype: string;
  policy_name: string;
  status: string;
  active: number;
  traffic_weight: number;
  shadow_weight: number;
  min_samples: number;
  min_lift: number;
  max_quarantine_rate: number;
  max_calibration_error: number;
  metadata?: string;
  created_at: number;
  updated_at: number;
}): PolicyVariant => ({
  id: row.id,
  taskType: normalizeTaskType(row.task_type),
  taskArchetype: row.task_archetype,
  policyName: row.policy_name,
  status: normalizeStatus(row.status),
  active: row.active === 1,
  trafficWeight: clamp(row.traffic_weight, 0, 1),
  shadowWeight: clamp(row.shadow_weight, 0, 1),
  minSamples: Math.max(1, Math.round(row.min_samples)),
  minLift: Math.max(0, row.min_lift),
  maxQuarantineRate: clamp(row.max_quarantine_rate, 0, 1),
  maxCalibrationError: clamp(row.max_calibration_error, 0, 1),
  metadata: parseJsonMetadata(row.metadata),
  createdAt: row.created_at,
  updatedAt: row.updated_at,
});

const loadVariants = (params: {
  taskType: LearningTaskType;
  taskArchetype: string;
  includeRetired?: boolean;
  dbPath?: string;
}): PolicyVariant[] => {
  const db = openResearchDb(params.dbPath);
  const rows = db
    .prepare(
      `SELECT
         id, task_type, task_archetype, policy_name, status, active,
         traffic_weight, shadow_weight, min_samples, min_lift,
         max_quarantine_rate, max_calibration_error, metadata, created_at, updated_at
       FROM policy_variants
       WHERE task_type=?
         AND task_archetype=?
         AND (? = 1 OR status <> 'retired')
       ORDER BY
         CASE status WHEN 'champion' THEN 0 WHEN 'challenger' THEN 1 ELSE 2 END,
         updated_at DESC`,
    )
    .all(params.taskType, params.taskArchetype, params.includeRetired ? 1 : 0) as Array<{
    id: number;
    task_type: string;
    task_archetype: string;
    policy_name: string;
    status: string;
    active: number;
    traffic_weight: number;
    shadow_weight: number;
    min_samples: number;
    min_lift: number;
    max_quarantine_rate: number;
    max_calibration_error: number;
    metadata?: string;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map(parseVariantRow);
};

export const registerPolicyVariant = (params: RegisterPolicyVariantParams): PolicyVariant => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const taskType = normalizeTaskType(params.taskType);
  const taskArchetype = (params.taskArchetype ?? "").trim();
  const policyName = params.policyName.trim();
  if (!policyName) throw new Error("policyName is required");

  const status = normalizeStatus(params.status);
  const active = params.active ?? true;
  const trafficWeight =
    typeof params.trafficWeight === "number"
      ? clamp(params.trafficWeight, 0, 1)
      : status === "champion"
        ? 1
        : 0.2;
  const shadowWeight =
    typeof params.shadowWeight === "number"
      ? clamp(params.shadowWeight, 0, 1)
      : status === "challenger"
        ? 1
        : 0;
  const minSamples = Math.max(1, Math.round(params.minSamples ?? 25));
  const minLift = Math.max(0, params.minLift ?? 0.03);
  const maxQuarantineRate = clamp(params.maxQuarantineRate ?? 0.2, 0, 1);
  const maxCalibrationError = clamp(params.maxCalibrationError ?? 0.25, 0, 1);
  const metadata = parseMetadata(params.metadata ?? {});

  db.prepare(
    `INSERT INTO policy_variants (
       task_type, task_archetype, policy_name, status, active,
       traffic_weight, shadow_weight, min_samples, min_lift,
       max_quarantine_rate, max_calibration_error, metadata, created_at, updated_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(task_type, task_archetype, policy_name) DO UPDATE SET
       status=excluded.status,
       active=excluded.active,
       traffic_weight=excluded.traffic_weight,
       shadow_weight=excluded.shadow_weight,
       min_samples=excluded.min_samples,
       min_lift=excluded.min_lift,
       max_quarantine_rate=excluded.max_quarantine_rate,
       max_calibration_error=excluded.max_calibration_error,
       metadata=excluded.metadata,
       updated_at=excluded.updated_at`,
  ).run(
    taskType,
    taskArchetype,
    policyName,
    status,
    active ? 1 : 0,
    trafficWeight,
    shadowWeight,
    minSamples,
    minLift,
    maxQuarantineRate,
    maxCalibrationError,
    JSON.stringify(metadata),
    now,
    now,
  );

  return (
    loadVariants({ taskType, taskArchetype, includeRetired: true, dbPath: params.dbPath }).find(
      (variant) => variant.policyName === policyName,
    ) ?? {
      id: 0,
      taskType,
      taskArchetype,
      policyName,
      status,
      active,
      trafficWeight,
      shadowWeight,
      minSamples,
      minLift,
      maxQuarantineRate,
      maxCalibrationError,
      metadata,
      createdAt: now,
      updatedAt: now,
    }
  );
};

export const listPolicyVariants = (params: {
  taskType: LearningTaskType | string;
  taskArchetype?: string;
  includeRetired?: boolean;
  dbPath?: string;
}) =>
  loadVariants({
    taskType: normalizeTaskType(params.taskType),
    taskArchetype: (params.taskArchetype ?? "").trim(),
    includeRetired: params.includeRetired ?? false,
    dbPath: params.dbPath,
  });

const pickWeighted = <T extends { weight: number }>(rows: T[], unit: number): T | undefined => {
  const total = rows.reduce((sum, row) => sum + Math.max(0, row.weight), 0);
  if (total <= 1e-9) return rows[0];
  let cursor = clamp(unit, 0, 1) * total;
  for (const row of rows) {
    cursor -= Math.max(0, row.weight);
    if (cursor <= 0) return row;
  }
  return rows[rows.length - 1];
};

const outcomeCoverage = (rows: Array<{ user_score?: number; realized_outcome_score?: number }>) => {
  if (!rows.length) return 0;
  const covered = rows.filter(
    (row) =>
      typeof toFiniteOrUndefined(row.realized_outcome_score) === "number" ||
      typeof toFiniteOrUndefined(row.user_score) === "number",
  ).length;
  return covered / rows.length;
};

const policyUncertaintyScore = (params: {
  variant?: PolicyVariant;
  dbPath?: string;
  lookbackDays?: number;
}): number => {
  if (!params.variant) return 1;
  const lookbackDays = Math.max(7, params.lookbackDays ?? 45);
  const endMs = Date.now();
  const startMs = endMs - lookbackDays * 86_400_000;
  const rows = performanceRows({
    taskType: params.variant.taskType,
    taskArchetype: params.variant.taskArchetype,
    policyName: params.variant.policyName,
    startMs,
    endMs,
    dbPath: params.dbPath,
  });
  if (!rows.length) return 1;
  const sampleFactor = clamp(rows.length / 30, 0, 1);
  const coverage = outcomeCoverage(rows);
  const calibrationRows = rows
    .map((row) => {
      const confidence = toFiniteOrUndefined(row.confidence);
      const realized = toFiniteOrUndefined(row.realized_outcome_score);
      const user = toFiniteOrUndefined(row.user_score);
      const outcome = realized ?? user;
      if (typeof confidence !== "number" || typeof outcome !== "number") return undefined;
      return Math.abs(confidence - outcome);
    })
    .filter((value): value is number => typeof value === "number");
  const calibrationError = mean(calibrationRows) ?? 0.2;
  const calibrationPenalty = clamp(calibrationError / 0.3, 0, 1);
  return clamp(0.4 * (1 - sampleFactor) + 0.35 * (1 - coverage) + 0.25 * calibrationPenalty, 0, 1);
};

export const routePolicyAssignment = (params: {
  taskType: LearningTaskType | string;
  taskArchetype?: string;
  explorationRate?: number;
  maxShadows?: number;
  seed?: string;
  dbPath?: string;
}): {
  taskType: LearningTaskType;
  taskArchetype: string;
  primary?: PolicyVariant;
  shadows: PolicyVariant[];
  experimentGroup: string;
  dynamicExplorationRate: number;
  uncertaintyScore: number;
} => {
  const taskType = normalizeTaskType(params.taskType);
  const taskArchetype = (params.taskArchetype ?? "").trim();
  const variants = loadVariants({
    taskType,
    taskArchetype,
    includeRetired: false,
    dbPath: params.dbPath,
  }).filter((variant) => variant.active);
  const experimentGroup = `${taskType}:${taskArchetype || "default"}`;
  if (!variants.length) {
    return {
      taskType,
      taskArchetype,
      primary: undefined,
      shadows: [],
      experimentGroup,
      dynamicExplorationRate: 0,
      uncertaintyScore: 1,
    };
  }

  const champion =
    variants.find((variant) => variant.status === "champion") ??
    variants.toSorted((a, b) => b.trafficWeight - a.trafficWeight)[0];
  const challengers = variants.filter((variant) => variant.policyName !== champion?.policyName);

  const seed = params.seed?.trim() || `${experimentGroup}:${Date.now()}`;
  const primaryUnit = toUnit(`${seed}:primary`);
  const explicitExplorationRate = typeof params.explorationRate === "number";
  const baseExplorationRate = clamp(params.explorationRate ?? 0.15, 0, 1);
  const uncertaintyScore = policyUncertaintyScore({
    variant: champion,
    dbPath: params.dbPath,
  });
  const dynamicExplorationRate = explicitExplorationRate
    ? baseExplorationRate
    : clamp(baseExplorationRate + uncertaintyScore * 0.25, 0, 0.6);
  const explore = challengers.length > 0 && primaryUnit < dynamicExplorationRate;

  const primary = explore
    ? pickWeighted(
        challengers.map((variant) => {
          const perf = computeVariantPerformance({
            variant,
            days: 60,
            dbPath: params.dbPath,
          });
          const rewardModel = clamp(
            0.45 * (perf.score ?? 0.5) +
              0.25 * (perf.winRate ?? 0.5) +
              0.2 * (1 - (perf.calibrationError ?? 0.25)) +
              0.1 * (1 - (perf.quarantineRate ?? 0.1)),
            0,
            1,
          );
          return {
            variant,
            weight: (variant.trafficWeight > 0 ? variant.trafficWeight : 1) * (0.5 + rewardModel),
          };
        }),
        toUnit(`${seed}:explore`),
      )?.variant
    : champion;

  const shadowCandidates = challengers
    .filter((variant) => variant.shadowWeight > 0)
    .toSorted((a, b) => b.shadowWeight - a.shadowWeight);
  const maxShadows = Math.max(0, Math.round(params.maxShadows ?? 2));
  const shadows = shadowCandidates.slice(0, maxShadows);

  return {
    taskType,
    taskArchetype,
    primary,
    shadows,
    experimentGroup,
    dynamicExplorationRate,
    uncertaintyScore,
  };
};

const performanceRows = (params: {
  taskType: LearningTaskType;
  taskArchetype: string;
  policyName: string;
  startMs: number;
  endMs?: number;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  return db
    .prepare(
      `SELECT
         policy_role, grader_score, status, confidence, user_score, realized_outcome_score
       FROM task_outcomes
       WHERE task_type=?
         AND task_archetype=?
         AND policy_name=?
         AND created_at >= ?
         AND (? IS NULL OR created_at < ?)`,
    )
    .all(
      params.taskType,
      params.taskArchetype,
      params.policyName,
      params.startMs,
      params.endMs ?? null,
      params.endMs ?? null,
    ) as Array<{
    policy_role: string;
    grader_score: number;
    status: string;
    confidence?: number;
    user_score?: number;
    realized_outcome_score?: number;
  }>;
};

const computeVariantPerformance = (params: {
  variant: PolicyVariant;
  days: number;
  dbPath?: string;
  endMs?: number;
}): VariantPerformance => {
  const now = Date.now();
  const startMs = now - Math.max(1, params.days) * 86_400_000;
  const rows = performanceRows({
    taskType: params.variant.taskType,
    taskArchetype: params.variant.taskArchetype,
    policyName: params.variant.policyName,
    startMs,
    endMs: params.endMs,
    dbPath: params.dbPath,
  });
  const sampleCount = rows.length;
  const primaryRows = rows.filter((row) => parseRole(row.policy_role) === "primary");
  const shadowRows = rows.filter((row) => parseRole(row.policy_role) === "shadow");
  if (!sampleCount) {
    return {
      taskType: params.variant.taskType,
      taskArchetype: params.variant.taskArchetype,
      policyName: params.variant.policyName,
      status: params.variant.status,
      sampleCount: 0,
      primarySampleCount: 0,
      shadowSampleCount: 0,
    };
  }
  let totalWeight = 0;
  let scoreSum = 0;
  let wins = 0;
  let quarantine = 0;
  let graderSum = 0;
  let outcomeSum = 0;
  let calibrationSum = 0;
  let calibrationCount = 0;
  for (const row of rows) {
    const role = parseRole(row.policy_role);
    const roleWeight = role === "shadow" ? 0.8 : 1;
    totalWeight += roleWeight;
    const grader = toFiniteOrUndefined(row.grader_score) ?? 0;
    const realized = toFiniteOrUndefined(row.realized_outcome_score);
    const user = toFiniteOrUndefined(row.user_score);
    const outcome = realized ?? user ?? grader;
    scoreSum += roleWeight * (0.4 * grader + 0.6 * outcome);
    graderSum += roleWeight * grader;
    outcomeSum += roleWeight * outcome;
    if (outcome >= 0.7) wins += roleWeight;
    if (row.status === "quarantine") quarantine += roleWeight;
    const confidence = toFiniteOrUndefined(row.confidence);
    if (typeof confidence === "number" && typeof (realized ?? user) === "number") {
      calibrationSum += roleWeight * Math.abs(confidence - (realized ?? user)!);
      calibrationCount += roleWeight;
    }
  }
  return {
    taskType: params.variant.taskType,
    taskArchetype: params.variant.taskArchetype,
    policyName: params.variant.policyName,
    status: params.variant.status,
    sampleCount,
    primarySampleCount: primaryRows.length,
    shadowSampleCount: shadowRows.length,
    score: totalWeight > 1e-9 ? scoreSum / totalWeight : undefined,
    winRate: totalWeight > 1e-9 ? wins / totalWeight : undefined,
    quarantineRate: totalWeight > 1e-9 ? quarantine / totalWeight : undefined,
    calibrationError: calibrationCount > 1e-9 ? calibrationSum / calibrationCount : undefined,
    avgGraderScore: totalWeight > 1e-9 ? graderSum / totalWeight : undefined,
    avgOutcomeSignal: totalWeight > 1e-9 ? outcomeSum / totalWeight : undefined,
  };
};

const insertDecision = (params: {
  taskType: LearningTaskType;
  taskArchetype: string;
  decisionType: "promote" | "rollback" | "hold";
  championBefore: string;
  championAfter: string;
  challenger: string;
  reason: string;
  metrics: Record<string, unknown>;
  dbPath?: string;
}): PolicyDecision => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const row = db
    .prepare(
      `INSERT INTO policy_decisions (
         task_type, task_archetype, decision_type, champion_before, champion_after,
         challenger, reason, metrics, created_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      params.taskType,
      params.taskArchetype,
      params.decisionType,
      params.championBefore,
      params.championAfter,
      params.challenger,
      params.reason,
      JSON.stringify(params.metrics),
      now,
    ) as { id: number };
  return {
    id: row.id,
    taskType: params.taskType,
    taskArchetype: params.taskArchetype,
    decisionType: params.decisionType,
    championBefore: params.championBefore,
    championAfter: params.championAfter,
    challenger: params.challenger,
    reason: params.reason,
    metrics: params.metrics,
    createdAt: now,
  };
};

const setChampion = (params: {
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
         SET status='challenger', traffic_weight=MIN(traffic_weight, 0.3), updated_at=?
         WHERE task_type=? AND task_archetype=? AND policy_name=?`,
      ).run(Date.now(), params.taskType, params.taskArchetype, params.previousChampionName);
    }
    db.prepare(
      `UPDATE policy_variants
       SET status='champion', active=1, traffic_weight=MAX(traffic_weight, 0.8), updated_at=?
       WHERE task_type=? AND task_archetype=? AND policy_name=?`,
    ).run(Date.now(), params.taskType, params.taskArchetype, params.championName);
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
};

export const policyPerformanceReport = (params: {
  taskType: LearningTaskType | string;
  taskArchetype?: string;
  days?: number;
  dbPath?: string;
}): PolicyReport => {
  const taskType = normalizeTaskType(params.taskType);
  const taskArchetype = (params.taskArchetype ?? "").trim();
  const days = Math.max(1, params.days ?? 60);
  const variants = loadVariants({
    taskType,
    taskArchetype,
    includeRetired: true,
    dbPath: params.dbPath,
  });
  const champion = variants.find((variant) => variant.status === "champion")?.policyName;
  const performance = variants.map((variant) =>
    computeVariantPerformance({ variant, days, dbPath: params.dbPath }),
  );
  const db = openResearchDb(params.dbPath);
  const decisions = db
    .prepare(
      `SELECT
         id, task_type, task_archetype, decision_type, champion_before, champion_after,
         challenger, reason, metrics, created_at
       FROM policy_decisions
       WHERE task_type=? AND task_archetype=?
       ORDER BY created_at DESC
       LIMIT 20`,
    )
    .all(taskType, taskArchetype) as Array<{
    id: number;
    task_type: string;
    task_archetype: string;
    decision_type: "promote" | "rollback" | "hold";
    champion_before: string;
    champion_after: string;
    challenger: string;
    reason: string;
    metrics?: string;
    created_at: number;
  }>;

  return {
    generatedAt: new Date().toISOString(),
    lookbackDays: days,
    taskType,
    taskArchetype,
    champion,
    variants: performance.toSorted((a, b) => (b.score ?? 0) - (a.score ?? 0)),
    recentDecisions: decisions.map((row) => ({
      id: row.id,
      taskType: normalizeTaskType(row.task_type),
      taskArchetype: row.task_archetype,
      decisionType: row.decision_type,
      championBefore: row.champion_before,
      championAfter: row.champion_after,
      challenger: row.challenger,
      reason: row.reason,
      metrics: parseJsonMetadata(row.metrics),
      createdAt: row.created_at,
    })),
  };
};

const governanceForGroup = (params: {
  taskType: LearningTaskType;
  taskArchetype: string;
  days: number;
  recentDays: number;
  minSamples: number;
  dbPath?: string;
}): GovernanceResult => {
  const variants = loadVariants({
    taskType: params.taskType,
    taskArchetype: params.taskArchetype,
    includeRetired: false,
    dbPath: params.dbPath,
  }).filter((variant) => variant.active);
  if (!variants.length) {
    return { promoted: 0, rolledBack: 0, held: 0, decisions: [] };
  }

  const decisions: PolicyDecision[] = [];
  let promoted = 0;
  let rolledBack = 0;
  let held = 0;
  let champion = variants.find((variant) => variant.status === "champion");
  const challengers = () =>
    loadVariants({
      taskType: params.taskType,
      taskArchetype: params.taskArchetype,
      includeRetired: false,
      dbPath: params.dbPath,
    }).filter((variant) => variant.active && variant.status === "challenger");

  if (!champion) {
    const fallback = variants[0];
    setChampion({
      taskType: params.taskType,
      taskArchetype: params.taskArchetype,
      championName: fallback.policyName,
      dbPath: params.dbPath,
    });
    const decision = insertDecision({
      taskType: params.taskType,
      taskArchetype: params.taskArchetype,
      decisionType: "promote",
      championBefore: "",
      championAfter: fallback.policyName,
      challenger: fallback.policyName,
      reason: "No champion existed; promoted first active variant.",
      metrics: {},
      dbPath: params.dbPath,
    });
    decisions.push(decision);
    promoted += 1;
    champion = fallback;
  }

  if (!champion) {
    return { promoted, rolledBack, held, decisions };
  }
  const championVariant = champion;

  const championPerf = computeVariantPerformance({
    variant: championVariant,
    days: params.days,
    dbPath: params.dbPath,
  });
  const challengerPerf = challengers()
    .map((variant) => ({
      variant,
      performance: computeVariantPerformance({ variant, days: params.days, dbPath: params.dbPath }),
    }))
    .filter(
      (entry) =>
        entry.performance.sampleCount >= Math.max(params.minSamples, entry.variant.minSamples),
    )
    .toSorted((a, b) => (b.performance.score ?? 0) - (a.performance.score ?? 0));

  const promotionCandidate = challengerPerf.find((entry) => {
    if (championPerf.sampleCount < Math.max(params.minSamples, championVariant.minSamples)) {
      return false;
    }
    if (typeof entry.performance.score !== "number" || typeof championPerf.score !== "number") {
      return false;
    }
    const minLift = Math.max(championVariant.minLift, entry.variant.minLift);
    if (entry.performance.score < championPerf.score + minLift) return false;
    if (
      typeof entry.performance.winRate === "number" &&
      typeof championPerf.winRate === "number" &&
      entry.performance.winRate < championPerf.winRate
    ) {
      return false;
    }
    if (
      typeof entry.performance.quarantineRate === "number" &&
      entry.performance.quarantineRate > entry.variant.maxQuarantineRate
    ) {
      return false;
    }
    if (
      typeof entry.performance.calibrationError === "number" &&
      entry.performance.calibrationError > entry.variant.maxCalibrationError
    ) {
      return false;
    }
    return true;
  });

  if (promotionCandidate) {
    setChampion({
      taskType: params.taskType,
      taskArchetype: params.taskArchetype,
      championName: promotionCandidate.variant.policyName,
      previousChampionName: championVariant.policyName,
      dbPath: params.dbPath,
    });
    const decision = insertDecision({
      taskType: params.taskType,
      taskArchetype: params.taskArchetype,
      decisionType: "promote",
      championBefore: championVariant.policyName,
      championAfter: promotionCandidate.variant.policyName,
      challenger: promotionCandidate.variant.policyName,
      reason: "Challenger outperformed champion over required sample and guardrails.",
      metrics: {
        champion_score: championPerf.score,
        challenger_score: promotionCandidate.performance.score,
        champion_win_rate: championPerf.winRate,
        challenger_win_rate: promotionCandidate.performance.winRate,
      },
      dbPath: params.dbPath,
    });
    decisions.push(decision);
    promoted += 1;
    champion = promotionCandidate.variant;
  }

  const currentChampionPerf = computeVariantPerformance({
    variant: champion,
    days: params.days,
    dbPath: params.dbPath,
  });
  const recentChampionPerf = computeVariantPerformance({
    variant: champion,
    days: params.recentDays,
    dbPath: params.dbPath,
  });
  const minRecentSamples = Math.max(8, Math.floor(champion.minSamples / 3));
  const degradeScore =
    typeof recentChampionPerf.score === "number" &&
    typeof currentChampionPerf.score === "number" &&
    recentChampionPerf.score + 0.05 < currentChampionPerf.score;
  const degradeWinRate =
    typeof recentChampionPerf.winRate === "number" &&
    typeof currentChampionPerf.winRate === "number" &&
    recentChampionPerf.winRate + 0.07 < currentChampionPerf.winRate;
  const degradeCalibration =
    typeof recentChampionPerf.calibrationError === "number" &&
    typeof currentChampionPerf.calibrationError === "number" &&
    recentChampionPerf.calibrationError > currentChampionPerf.calibrationError + 0.07;
  const degraded =
    recentChampionPerf.sampleCount >= minRecentSamples &&
    (degradeScore || degradeWinRate || degradeCalibration);

  if (degraded) {
    const fallback = challengers()
      .map((variant) => ({
        variant,
        performance: computeVariantPerformance({
          variant,
          days: params.days,
          dbPath: params.dbPath,
        }),
      }))
      .filter(
        (entry) =>
          entry.performance.sampleCount >= Math.max(params.minSamples, entry.variant.minSamples),
      )
      .find((entry) => {
        if (
          typeof recentChampionPerf.score === "number" &&
          typeof entry.performance.score === "number" &&
          entry.performance.score <= recentChampionPerf.score + 0.01
        ) {
          return false;
        }
        if (
          typeof recentChampionPerf.winRate === "number" &&
          typeof entry.performance.winRate === "number" &&
          entry.performance.winRate < recentChampionPerf.winRate
        ) {
          return false;
        }
        if (
          typeof entry.performance.quarantineRate === "number" &&
          entry.performance.quarantineRate > entry.variant.maxQuarantineRate
        ) {
          return false;
        }
        return true;
      });

    if (fallback) {
      setChampion({
        taskType: params.taskType,
        taskArchetype: params.taskArchetype,
        championName: fallback.variant.policyName,
        previousChampionName: champion.policyName,
        dbPath: params.dbPath,
      });
      const decision = insertDecision({
        taskType: params.taskType,
        taskArchetype: params.taskArchetype,
        decisionType: "rollback",
        championBefore: champion.policyName,
        championAfter: fallback.variant.policyName,
        challenger: fallback.variant.policyName,
        reason: "Champion degraded on recent window and fallback outperformed guardrails.",
        metrics: {
          champion_recent_score: recentChampionPerf.score,
          champion_baseline_score: currentChampionPerf.score,
          fallback_score: fallback.performance.score,
          champion_recent_win_rate: recentChampionPerf.winRate,
          fallback_win_rate: fallback.performance.winRate,
        },
        dbPath: params.dbPath,
      });
      decisions.push(decision);
      rolledBack += 1;
    } else {
      const decision = insertDecision({
        taskType: params.taskType,
        taskArchetype: params.taskArchetype,
        decisionType: "hold",
        championBefore: champion.policyName,
        championAfter: champion.policyName,
        challenger: "",
        reason: "Champion degraded recently but no eligible fallback met guardrails.",
        metrics: {
          champion_recent_score: recentChampionPerf.score,
          champion_baseline_score: currentChampionPerf.score,
        },
        dbPath: params.dbPath,
      });
      decisions.push(decision);
      held += 1;
    }
  }

  return {
    promoted,
    rolledBack,
    held,
    decisions,
  };
};

export const runPolicyGovernance = (
  params: {
    taskType?: LearningTaskType | string;
    taskArchetype?: string;
    days?: number;
    recentDays?: number;
    minSamples?: number;
    dbPath?: string;
  } = {},
): GovernanceResult => {
  const db = openResearchDb(params.dbPath);
  const groups = db
    .prepare(
      `SELECT DISTINCT task_type, task_archetype
       FROM policy_variants
       WHERE active=1
         AND (? = '' OR task_type = ?)
         AND (? = '' OR task_archetype = ?)`,
    )
    .all(
      params.taskType ? normalizeTaskType(params.taskType) : "",
      params.taskType ? normalizeTaskType(params.taskType) : "",
      (params.taskArchetype ?? "").trim(),
      (params.taskArchetype ?? "").trim(),
    ) as Array<{ task_type: string; task_archetype: string }>;

  const decisions: PolicyDecision[] = [];
  let promoted = 0;
  let rolledBack = 0;
  let held = 0;
  for (const group of groups) {
    const result = governanceForGroup({
      taskType: normalizeTaskType(group.task_type),
      taskArchetype: group.task_archetype,
      days: Math.max(7, params.days ?? 60),
      recentDays: Math.max(3, params.recentDays ?? 14),
      minSamples: Math.max(1, params.minSamples ?? 25),
      dbPath: params.dbPath,
    });
    promoted += result.promoted;
    rolledBack += result.rolledBack;
    held += result.held;
    decisions.push(...result.decisions);
  }
  return {
    promoted,
    rolledBack,
    held,
    decisions: decisions.toSorted((a, b) => b.createdAt - a.createdAt),
  };
};
