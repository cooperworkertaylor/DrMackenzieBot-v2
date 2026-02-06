import type { MemoCitation, MemoDiagnostics, MemoEvidenceClaim } from "./grade.js";
import type { PointInTimeSnapshot } from "./knowledge-graph.js";
import type { PortfolioPlan } from "./portfolio.js";
import type { ValuationResult } from "./valuation.js";
import type { VariantPerceptionResult } from "./variant.js";

export type ResearchCellStance = "bullish" | "bearish" | "neutral";

export type ResearchCellRoleOutput = {
  role: "thesis" | "skeptic" | "risk_manager";
  stance: ResearchCellStance;
  score: number;
  confidence: number;
  summary: string;
  keyPoints: string[];
};

export type ResearchCellAllocatorOutput = {
  role: "allocator";
  finalStance: PortfolioPlan["stance"];
  score: number;
  confidence: number;
  recommendedWeightPct: number;
  maxRiskBudgetPct: number;
  stopLossPct: number;
  summary: string;
  keyPoints: string[];
};

export type ResearchCellDebate = {
  consensusScore: number;
  adversarialCoverageScore: number;
  disconfirmingEvidence: string[];
  majorDisagreements: string[];
  unresolvedRisks: string[];
  riskControls: string[];
  requiredFollowUps: string[];
  passed: boolean;
};

export type ResearchCellResult = {
  generatedAt: string;
  ticker: string;
  question: string;
  thesis: ResearchCellRoleOutput;
  skeptic: ResearchCellRoleOutput;
  riskManager: ResearchCellRoleOutput;
  allocator: ResearchCellAllocatorOutput;
  debate: ResearchCellDebate;
};

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const toFixedPct = (value?: number): string =>
  typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(1)}%` : "n/a";

const uniqueStrings = (values: string[]): string[] => Array.from(new Set(values.filter(Boolean)));

const stanceFromScore = (score: number): ResearchCellStance => {
  if (score >= 0.58) return "bullish";
  if (score <= 0.42) return "bearish";
  return "neutral";
};

const parseCitationDomains = (citations: MemoCitation[]): Set<string> => {
  const domains = new Set<string>();
  citations.forEach((citation) => {
    const url = citation.url?.trim();
    if (!url) return;
    try {
      const host = new URL(url).hostname.replace(/^www\./, "").toLowerCase();
      if (host) domains.add(host);
    } catch {
      // Ignore malformed URLs.
    }
  });
  return domains;
};

const computeGraphMomentum = (
  snapshot: PointInTimeSnapshot,
): {
  score: number;
  positives: string[];
  negatives: string[];
} => {
  const positives: string[] = [];
  const negatives: string[] = [];
  const keyMetrics = snapshot.metrics.filter((metric) => typeof metric.deltaValueNum === "number");
  for (const metric of keyMetrics) {
    const delta = metric.deltaValueNum as number;
    if (delta > 0) {
      positives.push(`${metric.metricKey} improved (${delta.toFixed(4)})`);
    } else if (delta < 0) {
      negatives.push(`${metric.metricKey} weakened (${delta.toFixed(4)})`);
    }
  }
  if (!keyMetrics.length) {
    return {
      score: 0.5,
      positives,
      negatives,
    };
  }
  const score = clamp01(
    (positives.length - negatives.length + keyMetrics.length) / (2 * keyMetrics.length),
  );
  return {
    score,
    positives,
    negatives,
  };
};

export const runAdversarialResearchCell = (params: {
  ticker: string;
  question: string;
  claims: MemoEvidenceClaim[];
  citations: MemoCitation[];
  variant: VariantPerceptionResult;
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
  diagnostics: MemoDiagnostics;
  graphSnapshot: PointInTimeSnapshot;
}): ResearchCellResult => {
  const ticker = params.ticker.trim().toUpperCase();
  const expectedUpside =
    params.valuation.expectedUpsideWithCatalystsPct ?? params.valuation.expectedUpsidePct ?? 0;
  const variantScore =
    params.variant.stance === "positive-variant"
      ? 0.78
      : params.variant.stance === "negative-variant"
        ? 0.28
        : params.variant.stance === "in-line"
          ? 0.5
          : 0.2;
  const valuationScore = clamp01(0.5 + expectedUpside / 0.25);
  const citationBreadth = clamp01(parseCitationDomains(params.citations).size / 3);
  const claimDepth = clamp01(params.claims.length / 6);
  const graphMomentum = computeGraphMomentum(params.graphSnapshot);

  const thesisScore = clamp01(
    0.34 * variantScore +
      0.28 * valuationScore +
      0.2 * graphMomentum.score +
      0.1 * citationBreadth +
      0.08 * claimDepth,
  );
  const highContradictions = params.diagnostics.contradictions.filter(
    (entry) => entry.severity === "high",
  ).length;
  const mediumContradictions = params.diagnostics.contradictions.filter(
    (entry) => entry.severity === "medium",
  ).length;
  const impliedBullishRisk =
    params.valuation.impliedExpectations?.stance === "market-too-bullish" && expectedUpside > 0
      ? 1
      : 0;
  const lowConfidenceRisk =
    Math.min(params.variant.confidence, params.valuation.confidence, params.portfolio.confidence) <
    0.55
      ? 1
      : 0;
  const skepticScore = clamp01(
    0.48 * (highContradictions > 0 ? 1 : 0) +
      0.2 * clamp01(mediumContradictions / 2) +
      0.16 * impliedBullishRisk +
      0.08 * lowConfidenceRisk +
      0.08 * clamp01(graphMomentum.negatives.length / 3),
  );

  const riskControlSignals = [
    `Stop-loss hard limit ${(params.portfolio.stopLossPct * 100).toFixed(1)}%.`,
    `Risk budget capped at ${params.portfolio.maxRiskBudgetPct.toFixed(2)}% NAV.`,
    ...params.portfolio.reviewTriggers.slice(0, 3),
  ];
  const riskControls = uniqueStrings(riskControlSignals);
  const riskScore = clamp01(
    0.4 * clamp01((0.25 - params.portfolio.stopLossPct) / 0.17) +
      0.3 * clamp01(riskControls.length / 4) +
      0.3 *
        clamp01(
          params.portfolio.maxRiskBudgetPct > 0 ? 1 - params.portfolio.maxRiskBudgetPct / 6 : 0,
        ),
  );

  let finalStance: PortfolioPlan["stance"] = params.portfolio.stance;
  if (
    (highContradictions > 0 && skepticScore >= thesisScore) ||
    skepticScore - thesisScore >= 0.2
  ) {
    finalStance = "watch";
  } else if (thesisScore - skepticScore >= 0.2) {
    if (params.portfolio.stance === "insufficient-evidence") {
      finalStance = expectedUpside >= 0 ? "long" : "short";
    }
  } else if (params.portfolio.stance === "insufficient-evidence") {
    finalStance = "watch";
  }

  const weightScalar = clamp(1 - 0.55 * skepticScore + 0.25 * thesisScore, 0.15, 1.15);
  const recommendedWeightPct =
    finalStance === "long" || finalStance === "short"
      ? clamp(params.portfolio.recommendedWeightPct * weightScalar, 0.35, 10)
      : 0;
  const maxRiskBudgetPct =
    recommendedWeightPct > 0 ? clamp(recommendedWeightPct * 0.45, 0.35, 4) : 0;
  const stopLossPct =
    finalStance === "short"
      ? clamp(params.portfolio.stopLossPct + skepticScore * 0.02, 0.08, 0.24)
      : clamp(params.portfolio.stopLossPct + skepticScore * 0.015, 0.08, 0.24);
  const allocatorScore = clamp01(0.5 * thesisScore + 0.2 * riskScore + 0.3 * (1 - skepticScore));
  const allocatorConfidence = clamp01(
    0.45 * allocatorScore +
      0.25 * params.portfolio.confidence +
      0.15 * params.valuation.confidence +
      0.15 * params.variant.confidence,
  );

  const bearScenario = params.valuation.scenarios.find(
    (scenario) => scenario.name === "bear" && typeof scenario.upsidePct === "number",
  );
  const disconfirmingEvidence = uniqueStrings([
    ...params.diagnostics.contradictions.map((entry) => entry.detail),
    ...(impliedBullishRisk
      ? [
          "Implied expectations indicate market-too-bullish stance despite positive headline upside.",
        ]
      : []),
    ...(typeof bearScenario?.upsidePct === "number"
      ? [`Bear scenario still implies ${toFixedPct(bearScenario.upsidePct)} downside.`]
      : []),
    ...graphMomentum.negatives,
  ]);
  const majorDisagreements = uniqueStrings([
    ...(params.portfolio.stance === "long" && skepticScore > thesisScore
      ? ["Skeptic score exceeds thesis while portfolio defaults long."]
      : []),
    ...(params.portfolio.stance === "short" && thesisScore > skepticScore
      ? ["Thesis score exceeds skeptic while portfolio defaults short."]
      : []),
    ...(params.variant.stance === "positive-variant" &&
    params.valuation.impliedExpectations?.stance === "market-too-bullish"
      ? ["Variant-positive signal conflicts with expensive implied valuation."]
      : []),
  ]);
  const unresolvedRisks = uniqueStrings([
    ...(mediumContradictions > 0
      ? [`${mediumContradictions} medium-severity contradiction(s) need follow-up.`]
      : []),
    ...(allocatorConfidence < 0.62
      ? ["Allocator confidence is below institutional comfort level."]
      : []),
    ...(params.graphSnapshot.metrics.length < 6
      ? ["Sparse graph metrics; improve longitudinal coverage before sizing up."]
      : []),
  ]);
  const requiredFollowUps = uniqueStrings([
    "Re-run graph build after next filing/transcript ingestion.",
    ...(disconfirmingEvidence.length === 0
      ? ["Add at least one explicit disconfirming datapoint."]
      : []),
    ...(params.valuation.impliedExpectations?.stance === "market-too-bullish"
      ? ["Validate valuation downside via bear-case assumptions and margin compression stress."]
      : []),
    ...(params.graphSnapshot.events.length < 8
      ? [
          "Increase event history depth (filings/transcripts/catalysts) for stronger temporal context.",
        ]
      : []),
  ]);

  const coverageScore = clamp01(
    0.26 +
      0.22 * clamp01(disconfirmingEvidence.length / 2) +
      0.2 * clamp01(riskControls.length / 4) +
      0.16 * clamp01(params.graphSnapshot.metrics.length / 10) +
      0.16 * clamp01(params.claims.length / 6),
  );
  const consensusScore = clamp01(1 - Math.abs(thesisScore - skepticScore));
  const debatePassed =
    coverageScore >= 0.7 && disconfirmingEvidence.length >= 1 && riskControls.length >= 3;

  return {
    generatedAt: new Date().toISOString(),
    ticker,
    question: params.question,
    thesis: {
      role: "thesis",
      stance: stanceFromScore(thesisScore),
      score: thesisScore,
      confidence: clamp01(0.65 * thesisScore + 0.35 * params.variant.confidence),
      summary: `Thesis agent sees ${toFixedPct(expectedUpside)} expected upside with variant stance ${params.variant.stance}.`,
      keyPoints: uniqueStrings([
        `Expected upside (with catalysts): ${toFixedPct(expectedUpside)}.`,
        `Variant gap score ${params.variant.variantGapScore.toFixed(2)} with confidence ${params.variant.confidence.toFixed(2)}.`,
        ...graphMomentum.positives.slice(0, 2),
      ]),
    },
    skeptic: {
      role: "skeptic",
      stance: stanceFromScore(1 - skepticScore),
      score: skepticScore,
      confidence: clamp01(0.7 * skepticScore + 0.3 * (1 - params.variant.confidence)),
      summary: `Skeptic agent flags ${highContradictions + mediumContradictions} contradiction(s) and ${graphMomentum.negatives.length} negative momentum signal(s).`,
      keyPoints: uniqueStrings([
        ...(params.diagnostics.contradictions.length
          ? params.diagnostics.contradictions.slice(0, 3).map((entry) => entry.detail)
          : ["No explicit contradictions detected."]),
        ...(graphMomentum.negatives.length
          ? graphMomentum.negatives.slice(0, 2)
          : ["No negative graph momentum detected in tracked metrics."]),
      ]),
    },
    riskManager: {
      role: "risk_manager",
      stance: "neutral",
      score: riskScore,
      confidence: clamp01(0.5 * riskScore + 0.5 * params.portfolio.confidence),
      summary: `Risk manager sets stop-loss at ${(stopLossPct * 100).toFixed(1)}% with max risk budget ${maxRiskBudgetPct.toFixed(2)}%.`,
      keyPoints: riskControls,
    },
    allocator: {
      role: "allocator",
      finalStance,
      score: allocatorScore,
      confidence: allocatorConfidence,
      recommendedWeightPct,
      maxRiskBudgetPct,
      stopLossPct,
      summary: `Allocator chooses ${finalStance} with ${recommendedWeightPct.toFixed(2)}% weight at confidence ${allocatorConfidence.toFixed(2)}.`,
      keyPoints: uniqueStrings([
        `Consensus score ${consensusScore.toFixed(2)} and coverage score ${coverageScore.toFixed(2)}.`,
        `Thesis score ${thesisScore.toFixed(2)} vs skeptic score ${skepticScore.toFixed(2)}.`,
        ...(majorDisagreements.length
          ? majorDisagreements.slice(0, 2)
          : ["No major disagreement requiring override."]),
      ]),
    },
    debate: {
      consensusScore,
      adversarialCoverageScore: coverageScore,
      disconfirmingEvidence,
      majorDisagreements,
      unresolvedRisks,
      riskControls,
      requiredFollowUps,
      passed: debatePassed,
    },
  };
};
