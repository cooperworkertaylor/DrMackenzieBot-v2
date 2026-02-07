import type { PortfolioPlan } from "./portfolio.js";
import type { VariantPerceptionResult } from "./variant.js";
import { openResearchDb } from "./db.js";
import { forecastDecisionMetrics, type ValuationResult } from "./valuation.js";

export type MemoEvidenceClaim = {
  claim: string;
  citationIds: number[];
};

export type MemoCitation = {
  id: number;
  source_table: string;
  ref_id: number;
  metadata?: string;
  url?: string;
};

export type MemoContradiction = {
  severity: "high" | "medium";
  detail: string;
};

export type MemoDiagnostics = {
  contradictions: MemoContradiction[];
  falsificationTriggers: string[];
};

export type InstitutionalGradeCheck = {
  name: string;
  passed: boolean;
  detail: string;
  weight: number;
  score: number;
  required: boolean;
};

export type InstitutionalCalibration = {
  mode: "historical" | "proxy";
  sampleCount: number;
  score: number;
  passed: boolean;
  detail: string;
  mae?: number;
  directionalAccuracy?: number;
  confidenceOutcomeMae?: number;
};

export type InstitutionalGradeResult = {
  score: number;
  minScore: number;
  passed: boolean;
  checks: InstitutionalGradeCheck[];
  requiredFailures: string[];
  calibration: InstitutionalCalibration;
  actionabilityScore: number;
  adversarialCoverageScore: number;
};

type CalibrationOverride = {
  mode: "historical" | "proxy";
  sampleCount: number;
  score: number;
  mae?: number;
  directionalAccuracy?: number;
  confidenceOutcomeMae?: number;
};

export type AdversarialDebateAssessment = {
  coverageScore: number;
  dissentCount: number;
  disconfirmingEvidenceCount: number;
  riskControlCount: number;
  unresolvedRiskCount: number;
  finalStance: string;
  finalConfidence: number;
  passed: boolean;
};

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

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

const toDateMs = (value: unknown): number | undefined => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T00:00:00.000Z` : value;
  const ts = Date.parse(normalized);
  return Number.isFinite(ts) ? ts : undefined;
};

const citationRecencyMs = (citation: MemoCitation): number | undefined => {
  const metadata = parseJsonObject<Record<string, unknown>>(citation.metadata, {});
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
  return typeof fromUrl === "string" ? toDateMs(fromUrl) : undefined;
};

const extractCitationHost = (url?: string): string | undefined => {
  if (!url?.trim()) return undefined;
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return undefined;
  }
};

const mean = (values: number[]): number | undefined => {
  if (!values.length) return undefined;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
};

const evaluateActionability = (params: {
  portfolio: PortfolioPlan;
  valuation: ValuationResult;
  variant: VariantPerceptionResult;
}): { score: number; passed: boolean; detail: string } => {
  const stanceActionable =
    params.portfolio.stance === "long" || params.portfolio.stance === "short" ? 1 : 0;
  const hasExpectedValue =
    typeof params.valuation.expectedUpsideWithCatalystsPct === "number" ? 1 : 0;
  const sizingDiscipline =
    params.portfolio.recommendedWeightPct > 0 &&
    params.portfolio.maxRiskBudgetPct > 0 &&
    params.portfolio.maxRiskBudgetPct <= Math.max(4, params.portfolio.recommendedWeightPct * 0.7)
      ? 1
      : 0;
  const stopLossDiscipline =
    params.portfolio.stopLossPct >= 0.06 && params.portfolio.stopLossPct <= 0.25 ? 1 : 0;
  const triggerCoverage = clamp01(params.portfolio.reviewTriggers.length / 4);
  const confidenceQuality = clamp01(
    (Math.min(params.portfolio.confidence, params.valuation.confidence, params.variant.confidence) -
      0.4) /
      0.45,
  );

  const score = clamp01(
    0.32 * stanceActionable +
      0.18 * hasExpectedValue +
      0.2 * sizingDiscipline +
      0.1 * stopLossDiscipline +
      0.1 * triggerCoverage +
      0.1 * confidenceQuality,
  );
  const passed = score >= 0.7 && stanceActionable === 1;
  return {
    score,
    passed,
    detail: `score=${score.toFixed(2)} stance=${params.portfolio.stance} weight=${params.portfolio.recommendedWeightPct.toFixed(2)} risk_budget=${params.portfolio.maxRiskBudgetPct.toFixed(2)} stop_loss=${(params.portfolio.stopLossPct * 100).toFixed(1)}% triggers=${params.portfolio.reviewTriggers.length}`,
  };
};

const evaluateCalibrationHistorical = (params: {
  dbPath?: string;
}): {
  sampleCount: number;
  score: number;
  detail: string;
  mae?: number;
  directionalAccuracy?: number;
  confidenceOutcomeMae?: number;
} => {
  const forecast = forecastDecisionMetrics({ dbPath: params.dbPath });
  const db = openResearchDb(params.dbPath);
  const confidenceRows = db
    .prepare(
      `SELECT confidence, COALESCE(realized_outcome_score, user_score) AS outcome
       FROM task_outcomes
       WHERE task_type='investment'
         AND confidence IS NOT NULL
         AND (realized_outcome_score IS NOT NULL OR user_score IS NOT NULL)
       ORDER BY created_at DESC
       LIMIT 500`,
    )
    .all() as Array<{ confidence?: number; outcome?: number }>;
  const confidenceDeltas = confidenceRows
    .map((row) => {
      if (typeof row.confidence !== "number" || typeof row.outcome !== "number") return undefined;
      return Math.abs(row.confidence - row.outcome);
    })
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  const confidenceOutcomeMae = mean(confidenceDeltas);
  const maeScore = typeof forecast.mae === "number" ? clamp01(1 - forecast.mae / 0.35) : undefined;
  const directionalScore =
    typeof forecast.directionalAccuracy === "number"
      ? clamp01((forecast.directionalAccuracy - 0.45) / 0.2)
      : undefined;
  const confidenceScore =
    typeof confidenceOutcomeMae === "number" ? clamp01(1 - confidenceOutcomeMae / 0.3) : undefined;
  const components = [maeScore, directionalScore, confidenceScore].filter(
    (value): value is number => typeof value === "number",
  );
  const score = mean(components) ?? 0;
  const sampleCount = Math.max(forecast.count, confidenceDeltas.length);
  return {
    sampleCount,
    score,
    detail: `sample=${sampleCount} forecast_mae=${typeof forecast.mae === "number" ? forecast.mae.toFixed(3) : "n/a"} directional=${typeof forecast.directionalAccuracy === "number" ? forecast.directionalAccuracy.toFixed(3) : "n/a"} confidence_mae=${typeof confidenceOutcomeMae === "number" ? confidenceOutcomeMae.toFixed(3) : "n/a"}`,
    mae: forecast.mae,
    directionalAccuracy: forecast.directionalAccuracy,
    confidenceOutcomeMae,
  };
};

const evaluateCalibrationProxy = (params: {
  variant: VariantPerceptionResult;
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
}): {
  score: number;
  detail: string;
} => {
  const confidenceGap = Math.abs(params.portfolio.confidence - params.variant.confidence);
  const confidenceFloor = Math.min(
    params.portfolio.confidence,
    params.valuation.confidence,
    params.variant.confidence,
  );
  const score = clamp01(
    0.55 * clamp01(1 - confidenceGap / 0.35) + 0.45 * clamp01((confidenceFloor - 0.4) / 0.35),
  );
  return {
    score,
    detail: `proxy_gap=${confidenceGap.toFixed(3)} confidence_floor=${confidenceFloor.toFixed(3)}`,
  };
};

const evaluateCalibration = (params: {
  variant: VariantPerceptionResult;
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
  dbPath?: string;
  calibrationOverride?: CalibrationOverride;
}): InstitutionalCalibration => {
  if (params.calibrationOverride) {
    const passed =
      params.calibrationOverride.score >= 0.65 && params.calibrationOverride.sampleCount >= 8;
    return {
      mode: params.calibrationOverride.mode,
      sampleCount: params.calibrationOverride.sampleCount,
      score: clamp01(params.calibrationOverride.score),
      passed,
      detail: `override mode=${params.calibrationOverride.mode} sample=${params.calibrationOverride.sampleCount} score=${params.calibrationOverride.score.toFixed(2)}`,
      mae: params.calibrationOverride.mae,
      directionalAccuracy: params.calibrationOverride.directionalAccuracy,
      confidenceOutcomeMae: params.calibrationOverride.confidenceOutcomeMae,
    };
  }

  const historical = evaluateCalibrationHistorical({ dbPath: params.dbPath });
  if (historical.sampleCount >= 12) {
    const passed = historical.score >= 0.65;
    return {
      mode: "historical",
      sampleCount: historical.sampleCount,
      score: historical.score,
      passed,
      detail: historical.detail,
      mae: historical.mae,
      directionalAccuracy: historical.directionalAccuracy,
      confidenceOutcomeMae: historical.confidenceOutcomeMae,
    };
  }

  const proxy = evaluateCalibrationProxy({
    variant: params.variant,
    valuation: params.valuation,
    portfolio: params.portfolio,
  });
  const passed = proxy.score >= 0.62;
  return {
    mode: "proxy",
    sampleCount: historical.sampleCount,
    score: proxy.score,
    passed,
    detail: `${proxy.detail} historical_sample=${historical.sampleCount}`,
    mae: historical.mae,
    directionalAccuracy: historical.directionalAccuracy,
    confidenceOutcomeMae: historical.confidenceOutcomeMae,
  };
};

const check = (params: {
  name: string;
  detail: string;
  weight: number;
  score: number;
  passThreshold: number;
  required?: boolean;
}): InstitutionalGradeCheck => {
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

export const gradeInstitutionalMemo = (params: {
  hitsCount: number;
  claims: MemoEvidenceClaim[];
  citations: MemoCitation[];
  variant: VariantPerceptionResult;
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
  diagnostics: MemoDiagnostics;
  researchCell?: AdversarialDebateAssessment;
  minScore?: number;
  dbPath?: string;
  calibrationOverride?: CalibrationOverride;
}): InstitutionalGradeResult => {
  const minScore = params.minScore ?? 0.82;
  const citationSet = new Set(params.citations.map((citation) => citation.id));
  const claimCount = params.claims.length;
  const totalClaimCitations = params.claims.reduce(
    (sum, claim) => sum + claim.citationIds.length,
    0,
  );
  const citationsPerClaim = claimCount > 0 ? totalClaimCitations / claimCount : 0;
  const claimCoverage =
    claimCount > 0
      ? params.claims.filter((claim) => claim.citationIds.length >= 2).length / claimCount
      : 0;
  const totalCitationRefs = Math.max(1, totalClaimCitations);
  const unresolvedCitations = params.claims
    .flatMap((claim) => claim.citationIds)
    .filter((citationId) => !citationSet.has(citationId)).length;
  const traceabilityScore = clamp01(1 - unresolvedCitations / totalCitationRefs);

  const uniqueSourceTables = new Set(params.citations.map((citation) => citation.source_table))
    .size;
  const uniqueHosts = new Set(
    params.citations
      .map((citation) => extractCitationHost(citation.url))
      .filter((value): value is string => typeof value === "string"),
  ).size;

  const datedCitations = params.citations
    .map((citation) => citationRecencyMs(citation))
    .filter((value): value is number => typeof value === "number");
  const freshCutoff = Date.now() - 730 * 86_400_000;
  const freshRatio =
    datedCitations.length > 0
      ? datedCitations.filter((timestamp) => timestamp >= freshCutoff).length /
        datedCitations.length
      : 0.5;

  const highContradictions = params.diagnostics.contradictions.filter(
    (entry) => entry.severity === "high",
  ).length;
  const mediumContradictions = params.diagnostics.contradictions.filter(
    (entry) => entry.severity === "medium",
  ).length;
  const contradictionScore =
    highContradictions > 0
      ? 0
      : mediumContradictions === 0
        ? 1
        : mediumContradictions === 1
          ? 0.75
          : 0.2;

  const calibration = evaluateCalibration({
    variant: params.variant,
    valuation: params.valuation,
    portfolio: params.portfolio,
    dbPath: params.dbPath,
    calibrationOverride: params.calibrationOverride,
  });
  const actionability = evaluateActionability({
    portfolio: params.portfolio,
    valuation: params.valuation,
    variant: params.variant,
  });
  const adversarialCoverageScore = params.researchCell
    ? clamp01(
        0.55 * clamp01(params.researchCell.coverageScore) +
          0.2 * clamp01(params.researchCell.disconfirmingEvidenceCount / 2) +
          0.15 * clamp01(params.researchCell.riskControlCount / 4) +
          0.1 * clamp01(params.researchCell.finalConfidence),
      )
    : 0.8;

  const checks: InstitutionalGradeCheck[] = [
    check({
      name: "claim_count",
      detail: `claims=${claimCount} required>=4`,
      weight: 0.1,
      score: clamp01(claimCount / 6),
      passThreshold: 4 / 6,
    }),
    check({
      name: "claim_evidence_coverage",
      detail: `coverage=${(claimCoverage * 100).toFixed(1)}% avg_citations=${citationsPerClaim.toFixed(2)} required=100% coverage and avg>=2.0`,
      weight: 0.14,
      score: clamp01(0.6 * claimCoverage + 0.4 * clamp01(citationsPerClaim / 2)),
      passThreshold: 0.98,
      required: true,
    }),
    check({
      name: "evidence_traceability",
      detail: `unresolved_citations=${unresolvedCitations} total_refs=${totalCitationRefs}`,
      weight: 0.1,
      score: traceabilityScore,
      passThreshold: 0.99,
      required: true,
    }),
    check({
      name: "source_independence",
      detail: `unique_hosts=${uniqueHosts} unique_source_tables=${uniqueSourceTables} required hosts>=2 or source_tables>=3`,
      weight: 0.08,
      score: Math.max(clamp01(uniqueHosts / 3), clamp01(uniqueSourceTables / 4)),
      passThreshold: 0.67,
    }),
    check({
      name: "evidence_freshness",
      detail: `fresh_ratio=${(freshRatio * 100).toFixed(1)}% dated_citations=${datedCitations.length} required>=60%`,
      weight: 0.08,
      score: freshRatio,
      passThreshold: 0.6,
    }),
    check({
      name: "retrieval_depth",
      detail: `hits=${params.hitsCount} required>=8`,
      weight: 0.06,
      score: clamp01(params.hitsCount / 12),
      passThreshold: 8 / 12,
    }),
    check({
      name: "contradiction_handling",
      detail: `high=${highContradictions} medium=${mediumContradictions} required high=0 and medium<=1`,
      weight: 0.12,
      score: contradictionScore,
      passThreshold: 0.75,
      required: true,
    }),
    check({
      name: "falsification_depth",
      detail: `triggers=${params.diagnostics.falsificationTriggers.length} required>=4`,
      weight: 0.07,
      score: clamp01(params.diagnostics.falsificationTriggers.length / 6),
      passThreshold: 4 / 6,
    }),
    check({
      name: "calibration_quality",
      detail: calibration.detail,
      weight: 0.13,
      score: calibration.score,
      passThreshold: 0.65,
      required: true,
    }),
    check({
      name: "valuation_confidence",
      detail: `valuation_confidence=${params.valuation.confidence.toFixed(2)} priced_scenarios=${params.valuation.scenarios.filter((scenario) => typeof scenario.impliedSharePrice === "number").length}`,
      weight: 0.06,
      score: clamp01(
        0.6 * params.valuation.confidence +
          0.4 *
            clamp01(
              params.valuation.scenarios.filter(
                (scenario) => typeof scenario.impliedSharePrice === "number",
              ).length / 3,
            ),
      ),
      passThreshold: 0.6,
    }),
    check({
      name: "actionability",
      detail: actionability.detail,
      weight: 0.12,
      score: actionability.score,
      passThreshold: 0.7,
      required: true,
    }),
    check({
      name: "adversarial_debate",
      detail: params.researchCell
        ? `coverage=${params.researchCell.coverageScore.toFixed(2)} disconfirming=${params.researchCell.disconfirmingEvidenceCount} risk_controls=${params.researchCell.riskControlCount} dissent=${params.researchCell.dissentCount} unresolved=${params.researchCell.unresolvedRiskCount} stance=${params.researchCell.finalStance} passed=${params.researchCell.passed ? 1 : 0}`
        : "research_cell=missing (fallback score applied)",
      weight: 0.08,
      score: adversarialCoverageScore,
      passThreshold: params.researchCell ? 0.72 : 0.55,
      required: Boolean(params.researchCell),
    }),
    check({
      name: "variant_support",
      detail: `variant_confidence=${params.variant.confidence.toFixed(2)} expectation_obs=${params.variant.expectationObservations} fundamental_obs=${params.variant.fundamentalObservations}`,
      weight: 0.04,
      score: clamp01(
        0.6 * params.variant.confidence +
          0.2 * clamp01(params.variant.expectationObservations / 6) +
          0.2 * clamp01(params.variant.fundamentalObservations / 6),
      ),
      passThreshold: 0.58,
    }),
  ];

  const totalWeight = checks.reduce((sum, item) => sum + item.weight, 0);
  const weightedScore =
    totalWeight > 1e-9
      ? checks.reduce((sum, item) => sum + item.score * item.weight, 0) / totalWeight
      : 0;
  const requiredFailures = checks
    .filter((item) => item.required && !item.passed)
    .map((item) => item.name);
  return {
    score: weightedScore,
    minScore,
    passed: weightedScore >= minScore && requiredFailures.length === 0,
    checks,
    requiredFailures,
    calibration,
    actionabilityScore: actionability.score,
    adversarialCoverageScore,
  };
};
