import type {
  InstitutionalGradeResult,
  MemoCitation,
  MemoDiagnostics,
  MemoEvidenceClaim,
} from "./grade.js";
import type { ValuationResult } from "./valuation.js";
import { openResearchDb } from "./db.js";

export type QualityGateArtifactType = "memo" | "sector_report" | "theme_report";

export type QualityGateCheck = {
  name: string;
  passed: boolean;
  detail: string;
  weight: number;
  score: number;
  required: boolean;
};

export type QualityGateEvaluation = {
  gateName: string;
  artifactType: QualityGateArtifactType;
  artifactId: string;
  score: number;
  minScore: number;
  passed: boolean;
  requiredFailures: string[];
  checks: QualityGateCheck[];
  metrics: Record<string, string | number | boolean | null>;
};

export type QualityGateRun = {
  id: number;
  gateName: string;
  artifactType: QualityGateArtifactType;
  artifactId: string;
  score: number;
  minScore: number;
  passed: boolean;
  requiredFailures: string[];
  checks: QualityGateCheck[];
  metrics: Record<string, unknown>;
  metadata: Record<string, unknown>;
  createdAt: number;
};

export type QualityGateSummary = {
  total: number;
  passed: number;
  passRate: number;
  avgScore: number;
  avgMinScore: number;
};

export type QualityGateRegressionResult = {
  passed: boolean;
  artifactType?: QualityGateArtifactType;
  lookbackDays: number;
  recentDays: number;
  baselineCount: number;
  recentCount: number;
  baselinePassRate: number;
  recentPassRate: number;
  baselineAvgScore: number;
  recentAvgScore: number;
  reasons: string[];
};

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const mean = (values: number[]): number => {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
};

const parseJsonObject = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "string" || !value.trim()) return {};
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
};

const toDateMs = (value: unknown): number | undefined => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T00:00:00.000Z` : value;
  const ts = Date.parse(normalized);
  return Number.isFinite(ts) ? ts : undefined;
};

const citationRecencyMs = (citation: MemoCitation): number | undefined => {
  const metadata = parseJsonObject(citation.metadata);
  for (const key of [
    "asOfDate",
    "periodEnd",
    "filingDate",
    "filed",
    "event_date",
    "eventDate",
    "acceptedAt",
    "reportedDate",
    "fetchedAt",
    "fetched_at",
  ]) {
    const ts = toDateMs(metadata[key]);
    if (typeof ts === "number") return ts;
  }
  const fromUrl = citation.url?.match(/\b\d{4}-\d{2}-\d{2}\b/)?.[0];
  return fromUrl ? toDateMs(fromUrl) : undefined;
};

const extractCitationHost = (url?: string): string | undefined => {
  if (!url?.trim()) return undefined;
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return undefined;
  }
};

const PRIMARY_SOURCE_TABLES = new Set([
  "filings",
  "fundamental_facts",
  "earnings_expectations",
  "transcripts",
  "research_events",
  "research_facts",
]);

const makeCheck = (params: {
  name: string;
  detail: string;
  weight: number;
  score: number;
  passThreshold: number;
  required?: boolean;
}): QualityGateCheck => {
  const score = clamp01(params.score);
  return {
    name: params.name,
    detail: params.detail,
    weight: params.weight,
    score,
    passed: score >= params.passThreshold,
    required: Boolean(params.required),
  };
};

const finalizeChecks = (params: {
  gateName: string;
  artifactType: QualityGateArtifactType;
  artifactId: string;
  minScore: number;
  checks: QualityGateCheck[];
  metrics: Record<string, string | number | boolean | null>;
}): QualityGateEvaluation => {
  const totalWeight = params.checks.reduce((sum, check) => sum + check.weight, 0);
  const score =
    totalWeight > 1e-9
      ? params.checks.reduce((sum, check) => sum + check.score * check.weight, 0) / totalWeight
      : 0;
  const requiredFailures = params.checks
    .filter((check) => check.required && !check.passed)
    .map((check) => check.name);
  return {
    gateName: params.gateName,
    artifactType: params.artifactType,
    artifactId: params.artifactId,
    score,
    minScore: params.minScore,
    passed: score >= params.minScore && requiredFailures.length === 0,
    requiredFailures,
    checks: params.checks,
    metrics: params.metrics,
  };
};

export const evaluateMemoQualityGate = (params: {
  artifactId: string;
  claims: MemoEvidenceClaim[];
  citations: MemoCitation[];
  diagnostics: MemoDiagnostics;
  valuation: ValuationResult;
  grade: InstitutionalGradeResult;
  minScore?: number;
}): QualityGateEvaluation => {
  const minScore = params.minScore ?? Math.max(0.84, params.grade.minScore);
  const claimCount = params.claims.length;
  const claimCoverage =
    claimCount > 0
      ? params.claims.filter((claim) => claim.citationIds.length >= 2).length / claimCount
      : 0;
  const uniqueHosts = new Set(
    params.citations
      .map((citation) => extractCitationHost(citation.url))
      .filter((value): value is string => typeof value === "string"),
  ).size;
  const uniqueSourceTables = new Set(params.citations.map((citation) => citation.source_table))
    .size;
  const primarySourceCount = params.citations.filter((citation) =>
    PRIMARY_SOURCE_TABLES.has(citation.source_table),
  ).length;
  const primarySourceRatio = params.citations.length
    ? primarySourceCount / params.citations.length
    : 0;
  const datedCitations = params.citations
    .map((citation) => citationRecencyMs(citation))
    .filter((value): value is number => typeof value === "number");
  const freshCutoff = Date.now() - 730 * 86_400_000;
  const freshnessRatio =
    datedCitations.length > 0
      ? datedCitations.filter((value) => value >= freshCutoff).length / datedCitations.length
      : 0.5;
  const highContradictions = params.diagnostics.contradictions.filter(
    (item) => item.severity === "high",
  ).length;
  const mediumContradictions = params.diagnostics.contradictions.filter(
    (item) => item.severity === "medium",
  ).length;
  const contradictionScore =
    highContradictions > 0
      ? 0
      : mediumContradictions === 0
        ? 1
        : mediumContradictions === 1
          ? 0.75
          : 0.3;
  const pricedScenarioCount = params.valuation.scenarios.filter(
    (scenario) => typeof scenario.impliedSharePrice === "number",
  ).length;
  const benchmarkContextScore = params.valuation.impliedExpectations ? 1 : 0.25;
  const modelQualityScore = params.grade.passed
    ? params.grade.score
    : Math.min(params.grade.score, 0.55);
  const checks: QualityGateCheck[] = [
    makeCheck({
      name: "citation_coverage",
      detail: `claim_coverage=${(claimCoverage * 100).toFixed(1)}% claims=${claimCount}`,
      weight: 0.18,
      score: claimCoverage,
      passThreshold: 0.98,
      required: true,
    }),
    makeCheck({
      name: "source_diversity",
      detail: `unique_hosts=${uniqueHosts} unique_source_tables=${uniqueSourceTables}`,
      weight: 0.1,
      score: Math.max(clamp01(uniqueHosts / 3), clamp01(uniqueSourceTables / 4)),
      passThreshold: 0.67,
    }),
    makeCheck({
      name: "primary_source_ratio",
      detail: `primary_ratio=${(primarySourceRatio * 100).toFixed(1)}% primary=${primarySourceCount} total=${params.citations.length}`,
      weight: 0.12,
      score: primarySourceRatio,
      passThreshold: 0.6,
      required: true,
    }),
    makeCheck({
      name: "evidence_freshness",
      detail: `fresh_ratio=${(freshnessRatio * 100).toFixed(1)}% dated=${datedCitations.length}`,
      weight: 0.1,
      score: freshnessRatio,
      passThreshold: 0.6,
    }),
    makeCheck({
      name: "contradiction_check",
      detail: `high=${highContradictions} medium=${mediumContradictions}`,
      weight: 0.14,
      score: contradictionScore,
      passThreshold: 0.75,
      required: true,
    }),
    makeCheck({
      name: "benchmark_relative_context",
      detail: `implied_expectations=${params.valuation.impliedExpectations ? 1 : 0}`,
      weight: 0.1,
      score: benchmarkContextScore,
      passThreshold: 0.6,
    }),
    makeCheck({
      name: "scenario_coverage",
      detail: `priced_scenarios=${pricedScenarioCount} total_scenarios=${params.valuation.scenarios.length}`,
      weight: 0.1,
      score: clamp01(pricedScenarioCount / 3),
      passThreshold: 0.67,
      required: true,
    }),
    makeCheck({
      name: "confidence_calibration",
      detail: `mode=${params.grade.calibration.mode} score=${params.grade.calibration.score.toFixed(2)} sample=${params.grade.calibration.sampleCount}`,
      weight: 0.12,
      score: params.grade.calibration.score,
      passThreshold: 0.65,
      required: true,
    }),
    makeCheck({
      name: "model_quality",
      detail: `memo_grade_score=${params.grade.score.toFixed(2)} memo_grade_min=${params.grade.minScore.toFixed(2)} memo_grade_passed=${params.grade.passed ? 1 : 0}`,
      weight: 0.14,
      score: modelQualityScore,
      passThreshold: params.grade.minScore,
      required: true,
    }),
  ];
  return finalizeChecks({
    gateName: "institutional_memo_v2",
    artifactType: "memo",
    artifactId: params.artifactId,
    minScore,
    checks,
    metrics: {
      claim_count: claimCount,
      claim_coverage: claimCoverage,
      unique_hosts: uniqueHosts,
      unique_source_tables: uniqueSourceTables,
      primary_source_ratio: primarySourceRatio,
      freshness_ratio: freshnessRatio,
      high_contradictions: highContradictions,
      medium_contradictions: mediumContradictions,
      priced_scenario_count: pricedScenarioCount,
      implied_expectations: Boolean(params.valuation.impliedExpectations),
      calibration_score: params.grade.calibration.score,
      memo_grade_score: params.grade.score,
    },
  });
};

export const evaluateCrossSectionQualityGate = (params: {
  artifactType: "sector_report" | "theme_report";
  artifactId: string;
  evidenceCoverageScore: number;
  institutionalReadinessScore: number;
  avgVariantConfidence: number;
  avgValuationConfidence: number;
  avgPortfolioConfidence: number;
  benchmarkContextScore: number;
  scenarioCoverageRatio: number;
  riskFlagCount: number;
  uniqueGroupCount: number;
  factorStabilityScore: number;
  macroCoveragePct?: number;
  generatedAt: string;
  minScore?: number;
}): QualityGateEvaluation => {
  const minScore = params.minScore ?? 0.82;
  const generatedAtMs = Date.parse(params.generatedAt);
  const ageDays = Number.isFinite(generatedAtMs)
    ? (Date.now() - generatedAtMs) / 86_400_000
    : Number.POSITIVE_INFINITY;
  const confidenceComposite = mean([
    params.avgVariantConfidence,
    params.avgValuationConfidence,
    params.avgPortfolioConfidence,
  ]);
  const contradictionScore =
    params.riskFlagCount <= 0
      ? 1
      : params.riskFlagCount === 1
        ? 0.85
        : params.riskFlagCount === 2
          ? 0.7
          : params.riskFlagCount === 3
            ? 0.5
            : 0.2;
  const checks: QualityGateCheck[] = [
    makeCheck({
      name: "evidence_coverage",
      detail: `evidence_coverage=${params.evidenceCoverageScore.toFixed(2)}`,
      weight: 0.16,
      score: params.evidenceCoverageScore,
      passThreshold: 0.7,
      required: true,
    }),
    makeCheck({
      name: "institutional_readiness",
      detail: `institutional_readiness=${params.institutionalReadinessScore.toFixed(2)}`,
      weight: 0.18,
      score: params.institutionalReadinessScore,
      passThreshold: 0.75,
      required: true,
    }),
    makeCheck({
      name: "benchmark_relative_context",
      detail: `benchmark_context_score=${params.benchmarkContextScore.toFixed(2)}`,
      weight: 0.15,
      score: params.benchmarkContextScore,
      passThreshold: 0.65,
      required: true,
    }),
    makeCheck({
      name: "scenario_coverage",
      detail: `scenario_coverage=${(params.scenarioCoverageRatio * 100).toFixed(1)}%`,
      weight: 0.12,
      score: params.scenarioCoverageRatio,
      passThreshold: 0.67,
      required: true,
    }),
    makeCheck({
      name: "confidence_calibration",
      detail: `avg_variant=${params.avgVariantConfidence.toFixed(2)} avg_valuation=${params.avgValuationConfidence.toFixed(2)} avg_portfolio=${params.avgPortfolioConfidence.toFixed(2)}`,
      weight: 0.12,
      score: clamp01((confidenceComposite - 0.45) / 0.4),
      passThreshold: 0.62,
      required: true,
    }),
    makeCheck({
      name: "contradiction_check",
      detail: `risk_flags=${params.riskFlagCount}`,
      weight: 0.1,
      score: contradictionScore,
      passThreshold: 0.7,
      required: true,
    }),
    makeCheck({
      name: "source_diversity",
      detail: `unique_groups=${params.uniqueGroupCount}`,
      weight: 0.09,
      score: clamp01(params.uniqueGroupCount / 4),
      passThreshold: 0.6,
    }),
    makeCheck({
      name: "factor_stability",
      detail: `factor_stability=${params.factorStabilityScore.toFixed(2)}`,
      weight: 0.08,
      score: params.factorStabilityScore,
      passThreshold: 0.55,
    }),
    makeCheck({
      name: "deliverable_freshness",
      detail: `age_days=${Number.isFinite(ageDays) ? ageDays.toFixed(2) : "n/a"}`,
      weight: 0.08,
      score: Number.isFinite(ageDays) ? clamp01(1 - ageDays / 7) : 0,
      passThreshold: 0.8,
    }),
  ];
  if (typeof params.macroCoveragePct === "number") {
    checks.push(
      makeCheck({
        name: "macro_factor_coverage",
        detail: `macro_coverage_pct=${params.macroCoveragePct.toFixed(1)}`,
        weight: 0.1,
        score: clamp01(params.macroCoveragePct / 100),
        passThreshold: 0.7,
      }),
    );
  }
  return finalizeChecks({
    gateName: "institutional_cross_section_v1",
    artifactType: params.artifactType,
    artifactId: params.artifactId,
    minScore,
    checks,
    metrics: {
      evidence_coverage: params.evidenceCoverageScore,
      institutional_readiness: params.institutionalReadinessScore,
      benchmark_context_score: params.benchmarkContextScore,
      scenario_coverage_ratio: params.scenarioCoverageRatio,
      avg_variant_confidence: params.avgVariantConfidence,
      avg_valuation_confidence: params.avgValuationConfidence,
      avg_portfolio_confidence: params.avgPortfolioConfidence,
      risk_flag_count: params.riskFlagCount,
      unique_group_count: params.uniqueGroupCount,
      factor_stability_score: params.factorStabilityScore,
      macro_coverage_pct: params.macroCoveragePct ?? null,
    },
  });
};

export const recordQualityGateRun = (params: {
  evaluation: QualityGateEvaluation;
  metadata?: Record<string, unknown>;
  createdAt?: number;
  dbPath?: string;
}): QualityGateRun => {
  const db = openResearchDb(params.dbPath);
  const createdAt = Math.max(0, Math.round(params.createdAt ?? Date.now()));
  const result = db
    .prepare(
      `INSERT INTO quality_gate_runs (
         artifact_type,
         artifact_id,
         gate_name,
         score,
         min_score,
         passed,
         required_failures,
         checks,
         metrics,
         metadata,
         created_at
       )
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    )
    .run(
      params.evaluation.artifactType,
      params.evaluation.artifactId,
      params.evaluation.gateName,
      params.evaluation.score,
      params.evaluation.minScore,
      params.evaluation.passed ? 1 : 0,
      JSON.stringify(params.evaluation.requiredFailures),
      JSON.stringify(params.evaluation.checks),
      JSON.stringify(params.evaluation.metrics),
      JSON.stringify(params.metadata ?? {}),
      createdAt,
    ) as { lastInsertRowid?: number | bigint };
  return {
    id: Number(result.lastInsertRowid ?? 0),
    gateName: params.evaluation.gateName,
    artifactType: params.evaluation.artifactType,
    artifactId: params.evaluation.artifactId,
    score: params.evaluation.score,
    minScore: params.evaluation.minScore,
    passed: params.evaluation.passed,
    requiredFailures: [...params.evaluation.requiredFailures],
    checks: [...params.evaluation.checks],
    metrics: { ...params.evaluation.metrics },
    metadata: params.metadata ?? {},
    createdAt,
  };
};

export const listQualityGateRuns = (
  params: {
    artifactType?: QualityGateArtifactType;
    artifactId?: string;
    days?: number;
    limit?: number;
    dbPath?: string;
  } = {},
): QualityGateRun[] => {
  const db = openResearchDb(params.dbPath);
  const artifactType = params.artifactType?.trim() ?? "";
  const artifactId = params.artifactId?.trim() ?? "";
  const limit = Math.max(1, Math.round(params.limit ?? 200));
  const cutoffMs =
    typeof params.days === "number" && params.days > 0
      ? Date.now() - Math.round(params.days * 86_400_000)
      : 0;
  const rows = db
    .prepare(
      `SELECT
         id,
         artifact_type,
         artifact_id,
         gate_name,
         score,
         min_score,
         passed,
         required_failures,
         checks,
         metrics,
         metadata,
         created_at
       FROM quality_gate_runs
       WHERE (? = '' OR artifact_type = ?)
         AND (? = '' OR artifact_id = ?)
         AND (? = 0 OR created_at >= ?)
       ORDER BY created_at DESC, id DESC
       LIMIT ?`,
    )
    .all(artifactType, artifactType, artifactId, artifactId, cutoffMs, cutoffMs, limit) as Array<{
    id: number;
    artifact_type: string;
    artifact_id: string;
    gate_name: string;
    score: number;
    min_score: number;
    passed: number;
    required_failures: string;
    checks: string;
    metrics: string;
    metadata: string;
    created_at: number;
  }>;
  return rows.map((row) => ({
    id: row.id,
    gateName: row.gate_name,
    artifactType: row.artifact_type as QualityGateArtifactType,
    artifactId: row.artifact_id,
    score: row.score,
    minScore: row.min_score,
    passed: Boolean(row.passed),
    requiredFailures: (() => {
      try {
        const parsed = JSON.parse(row.required_failures) as unknown;
        return Array.isArray(parsed) ? parsed.map((value) => String(value)) : [];
      } catch {
        return [];
      }
    })(),
    checks: (() => {
      try {
        const parsed = JSON.parse(row.checks) as unknown;
        return Array.isArray(parsed) ? (parsed as QualityGateCheck[]) : [];
      } catch {
        return [];
      }
    })(),
    metrics: parseJsonObject(row.metrics),
    metadata: parseJsonObject(row.metadata),
    createdAt: row.created_at,
  }));
};

export const summarizeQualityGateRuns = (runs: QualityGateRun[]): QualityGateSummary => {
  const total = runs.length;
  const passed = runs.filter((run) => run.passed).length;
  const passRate = total > 0 ? passed / total : 0;
  const avgScore = total > 0 ? mean(runs.map((run) => run.score)) : 0;
  const avgMinScore = total > 0 ? mean(runs.map((run) => run.minScore)) : 0;
  return {
    total,
    passed,
    passRate,
    avgScore,
    avgMinScore,
  };
};

export const evaluateQualityGateRegression = (
  params: {
    artifactType?: QualityGateArtifactType;
    lookbackDays?: number;
    recentDays?: number;
    minRecentSamples?: number;
    minRecentPassRate?: number;
    minRecentAvgScore?: number;
    maxPassRateDrop?: number;
    maxAvgScoreDrop?: number;
    dbPath?: string;
  } = {},
): QualityGateRegressionResult => {
  const lookbackDays = Math.max(7, Math.round(params.lookbackDays ?? 90));
  const recentDays = Math.max(1, Math.min(lookbackDays - 1, Math.round(params.recentDays ?? 14)));
  const minRecentSamples = Math.max(1, Math.round(params.minRecentSamples ?? 10));
  const minRecentPassRate = clamp01(params.minRecentPassRate ?? 0.8);
  const minRecentAvgScore = clamp01(params.minRecentAvgScore ?? 0.82);
  const maxPassRateDrop = clamp01(params.maxPassRateDrop ?? 0.08);
  const maxAvgScoreDrop = clamp01(params.maxAvgScoreDrop ?? 0.05);

  const runs = listQualityGateRuns({
    artifactType: params.artifactType,
    days: lookbackDays,
    limit: 5000,
    dbPath: params.dbPath,
  });
  const cutoffRecent = Date.now() - recentDays * 86_400_000;
  const recentRuns = runs.filter((run) => run.createdAt >= cutoffRecent);
  const baselineRuns = runs.filter((run) => run.createdAt < cutoffRecent);
  const recent = summarizeQualityGateRuns(recentRuns);
  const baseline = summarizeQualityGateRuns(baselineRuns);

  const reasons: string[] = [];
  if (recent.total < minRecentSamples) {
    reasons.push(`recent_samples=${recent.total} required>=${minRecentSamples}`);
  }
  if (recent.passRate < minRecentPassRate) {
    reasons.push(
      `recent_pass_rate=${recent.passRate.toFixed(3)} required>=${minRecentPassRate.toFixed(3)}`,
    );
  }
  if (recent.avgScore < minRecentAvgScore) {
    reasons.push(
      `recent_avg_score=${recent.avgScore.toFixed(3)} required>=${minRecentAvgScore.toFixed(3)}`,
    );
  }
  if (baseline.total >= minRecentSamples) {
    const passRateDrop = baseline.passRate - recent.passRate;
    const avgScoreDrop = baseline.avgScore - recent.avgScore;
    if (passRateDrop > maxPassRateDrop) {
      reasons.push(`pass_rate_drop=${passRateDrop.toFixed(3)} max=${maxPassRateDrop.toFixed(3)}`);
    }
    if (avgScoreDrop > maxAvgScoreDrop) {
      reasons.push(`score_drop=${avgScoreDrop.toFixed(3)} max=${maxAvgScoreDrop.toFixed(3)}`);
    }
  }
  return {
    passed: reasons.length === 0,
    artifactType: params.artifactType,
    lookbackDays,
    recentDays,
    baselineCount: baseline.total,
    recentCount: recent.total,
    baselinePassRate: baseline.passRate,
    recentPassRate: recent.passRate,
    baselineAvgScore: baseline.avgScore,
    recentAvgScore: recent.avgScore,
    reasons,
  };
};
