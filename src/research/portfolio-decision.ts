import type { MemoCitation, MemoDiagnostics, MemoEvidenceClaim } from "./grade.js";
import { getTickerPointInTimeSnapshot, type PointInTimeSnapshot } from "./knowledge-graph.js";
import { computePortfolioPlan, type PortfolioPlan } from "./portfolio.js";
import { runAdversarialResearchCell, type ResearchCellResult } from "./research-cell.js";
import { computeValuation, type ValuationResult } from "./valuation.js";
import { computeVariantPerception, type VariantPerceptionResult } from "./variant.js";

export type PortfolioDecisionConstraints = {
  maxSingleNameWeightPct: number;
  maxRiskBudgetPct: number;
  maxStopLossPct: number;
  minConfidence: number;
  requiredDebateCoverage: number;
  maxDownsideLossPct: number;
};

export type PortfolioStressCase = {
  scenario: string;
  probability: number;
  returnPct: number;
  weightedReturnPct: number;
  pnlPct: number;
  breachesRiskBudget: boolean;
};

export type PositionSizeCandidate = {
  label: "conservative" | "base" | "aggressive";
  weightPct: number;
  riskBudgetPct: number;
  expectedPnlPct: number;
  downsidePnlPct: number;
  score: number;
  recommendation: "enter" | "watch" | "avoid";
  notes: string[];
};

export type PortfolioDecisionResult = {
  generatedAt: string;
  ticker: string;
  question: string;
  recommendation: "enter" | "watch" | "avoid";
  finalStance: PortfolioPlan["stance"];
  decisionScore: number;
  confidence: number;
  constraints: PortfolioDecisionConstraints;
  expectedReturnPct: number;
  downsideRiskPct: number;
  riskBreaches: string[];
  stress: PortfolioStressCase[];
  sizeCandidates: PositionSizeCandidate[];
  rationale: string[];
  portfolio: PortfolioPlan;
  valuation: ValuationResult;
  variant: VariantPerceptionResult;
  researchCell: ResearchCellResult;
};

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const parseNumeric = (value: unknown, fallback: number): number =>
  typeof value === "number" && Number.isFinite(value) ? value : fallback;

const defaultConstraints = (): PortfolioDecisionConstraints => ({
  maxSingleNameWeightPct: 8,
  maxRiskBudgetPct: 4,
  maxStopLossPct: 0.24,
  minConfidence: 0.58,
  requiredDebateCoverage: 0.7,
  maxDownsideLossPct: 2.2,
});

const normalizeConstraints = (
  constraints?: Partial<PortfolioDecisionConstraints>,
): PortfolioDecisionConstraints => {
  const defaults = defaultConstraints();
  return {
    maxSingleNameWeightPct: clamp(
      parseNumeric(constraints?.maxSingleNameWeightPct, defaults.maxSingleNameWeightPct),
      0.25,
      15,
    ),
    maxRiskBudgetPct: clamp(
      parseNumeric(constraints?.maxRiskBudgetPct, defaults.maxRiskBudgetPct),
      0.25,
      8,
    ),
    maxStopLossPct: clamp(
      parseNumeric(constraints?.maxStopLossPct, defaults.maxStopLossPct),
      0.06,
      0.35,
    ),
    minConfidence: clamp01(parseNumeric(constraints?.minConfidence, defaults.minConfidence)),
    requiredDebateCoverage: clamp01(
      parseNumeric(constraints?.requiredDebateCoverage, defaults.requiredDebateCoverage),
    ),
    maxDownsideLossPct: clamp(
      parseNumeric(constraints?.maxDownsideLossPct, defaults.maxDownsideLossPct),
      0.5,
      6,
    ),
  };
};

const buildProxyClaims = (snapshot: PointInTimeSnapshot): MemoEvidenceClaim[] =>
  snapshot.metrics.slice(0, 6).map((metric, index) => {
    const delta =
      typeof metric.deltaValueNum === "number" ? ` delta=${metric.deltaValueNum.toFixed(4)}` : "";
    const latest =
      typeof metric.latestValueNum === "number"
        ? metric.latestValueNum.toFixed(4)
        : (metric.latestValueText ?? "n/a");
    return {
      claim: `Metric ${metric.metricKey} latest=${latest}${delta} as_of=${metric.latestAsOfDate}.`,
      citationIds: [index + 1],
    };
  });

const buildProxyCitations = (snapshot: PointInTimeSnapshot): MemoCitation[] =>
  snapshot.events.slice(0, 8).map((event, index) => ({
    id: index + 1,
    source_table: event.sourceTable,
    ref_id: event.sourceRefId,
    metadata: JSON.stringify({
      eventType: event.eventType,
      eventTime: new Date(event.eventTime).toISOString(),
    }),
    url: event.sourceUrl,
  }));

const buildProxyDiagnostics = (params: {
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
  variant: VariantPerceptionResult;
}): MemoDiagnostics => {
  const contradictions: MemoDiagnostics["contradictions"] = [];
  if (
    params.valuation.impliedExpectations?.stance === "market-too-bullish" &&
    params.variant.stance === "positive-variant"
  ) {
    contradictions.push({
      severity: "high",
      detail: "Positive variant signal conflicts with market-too-bullish implied expectations.",
    });
  }
  const expectedUpside =
    params.valuation.expectedUpsideWithCatalystsPct ?? params.valuation.expectedUpsidePct;
  if (typeof expectedUpside === "number") {
    if (expectedUpside > 0.12 && params.portfolio.stance === "short") {
      contradictions.push({
        severity: "high",
        detail: "Short stance conflicts with strongly positive expected upside.",
      });
    } else if (expectedUpside < -0.12 && params.portfolio.stance === "long") {
      contradictions.push({
        severity: "high",
        detail: "Long stance conflicts with strongly negative expected upside.",
      });
    }
  }
  if (params.portfolio.confidence < 0.55 && params.portfolio.recommendedWeightPct >= 4.5) {
    contradictions.push({
      severity: "medium",
      detail: "Sizing appears too high for current confidence level.",
    });
  }
  const falsificationTriggers = [
    "Expected upside flips negative on next valuation refresh.",
    "Two consecutive negative EPS surprise prints.",
    "Estimate trend drops below prior quarter trend.",
    `Stop-loss breach at ${(params.portfolio.stopLossPct * 100).toFixed(1)}%.`,
  ];
  return {
    contradictions,
    falsificationTriggers,
  };
};

const scenarioReturnPct = (
  scenario: ValuationResult["scenarios"][number],
  currentPrice?: number,
): number => {
  if (typeof scenario.upsidePct === "number" && Number.isFinite(scenario.upsidePct)) {
    return scenario.upsidePct * 100;
  }
  if (
    typeof scenario.impliedSharePrice === "number" &&
    Number.isFinite(scenario.impliedSharePrice) &&
    typeof currentPrice === "number" &&
    Number.isFinite(currentPrice) &&
    Math.abs(currentPrice) > 1e-9
  ) {
    return (scenario.impliedSharePrice / currentPrice - 1) * 100;
  }
  return 0;
};

export const composePortfolioDecision = (params: {
  ticker: string;
  question: string;
  portfolio: PortfolioPlan;
  valuation: ValuationResult;
  variant: VariantPerceptionResult;
  researchCell: ResearchCellResult;
  constraints?: Partial<PortfolioDecisionConstraints>;
}): PortfolioDecisionResult => {
  const constraints = normalizeConstraints(params.constraints);
  const expectedReturnPct =
    (params.valuation.expectedUpsideWithCatalystsPct ?? params.valuation.expectedUpsidePct ?? 0) *
    100;
  const finalStance = params.researchCell.allocator.finalStance;
  const confidence = clamp01(
    0.45 * params.researchCell.allocator.confidence +
      0.25 * params.portfolio.confidence +
      0.2 * params.valuation.confidence +
      0.1 * params.variant.confidence,
  );
  const baseWeightRaw = params.researchCell.allocator.recommendedWeightPct;
  const baseWeightPct =
    finalStance === "long" || finalStance === "short"
      ? clamp(baseWeightRaw, 0, constraints.maxSingleNameWeightPct)
      : 0;
  const baseRiskBudgetPct =
    baseWeightPct > 0
      ? clamp(params.researchCell.allocator.maxRiskBudgetPct, 0.2, constraints.maxRiskBudgetPct)
      : 0;
  const stopLossPct = clamp(
    params.researchCell.allocator.stopLossPct,
    0.06,
    constraints.maxStopLossPct,
  );

  const stress = params.valuation.scenarios.map((scenario) => {
    const returnPct = scenarioReturnPct(scenario, params.valuation.currentPrice);
    const weightedReturnPct = returnPct * clamp01(scenario.probability);
    const pnlPct = (baseWeightPct / 100) * returnPct;
    return {
      scenario: scenario.name,
      probability: clamp01(scenario.probability),
      returnPct,
      weightedReturnPct,
      pnlPct,
      breachesRiskBudget: Math.abs(pnlPct) > baseRiskBudgetPct,
    } as PortfolioStressCase;
  });
  const downsideLossPct = Math.abs(
    stress
      .filter((row) => row.returnPct < 0)
      .reduce((worst, row) => Math.min(worst, row.pnlPct), 0),
  );

  const riskBreaches: string[] = [];
  if (finalStance === "watch" || finalStance === "insufficient-evidence") {
    riskBreaches.push("Final stance is non-actionable.");
  }
  if (confidence < constraints.minConfidence) {
    riskBreaches.push(
      `Confidence ${confidence.toFixed(2)} is below minimum ${constraints.minConfidence.toFixed(2)}.`,
    );
  }
  if (params.researchCell.debate.adversarialCoverageScore < constraints.requiredDebateCoverage) {
    riskBreaches.push(
      `Adversarial coverage ${params.researchCell.debate.adversarialCoverageScore.toFixed(2)} below ${constraints.requiredDebateCoverage.toFixed(2)}.`,
    );
  }
  if (downsideLossPct > constraints.maxDownsideLossPct) {
    riskBreaches.push(
      `Worst scenario loss ${downsideLossPct.toFixed(2)}% exceeds ${constraints.maxDownsideLossPct.toFixed(2)}%.`,
    );
  }
  if (stress.some((row) => row.breachesRiskBudget)) {
    riskBreaches.push("One or more scenarios exceed current risk budget.");
  }

  const decisionScore = clamp01(
    0.32 * clamp01((expectedReturnPct + 25) / 50) +
      0.24 * confidence +
      0.2 * params.researchCell.debate.adversarialCoverageScore +
      0.14 * (1 - clamp01(downsideLossPct / Math.max(0.5, constraints.maxDownsideLossPct * 1.4))) +
      0.1 * (riskBreaches.length === 0 ? 1 : clamp01(1 - riskBreaches.length / 4)),
  );

  let recommendation: PortfolioDecisionResult["recommendation"] = "watch";
  if (finalStance === "long" || finalStance === "short") {
    if (riskBreaches.length === 0 && decisionScore >= 0.68) {
      recommendation = "enter";
    } else if (riskBreaches.length >= 2 || decisionScore < 0.48) {
      recommendation = "avoid";
    }
  } else if (decisionScore < 0.4) {
    recommendation = "avoid";
  }

  const sizeScales: Array<{ label: PositionSizeCandidate["label"]; scale: number }> = [
    { label: "conservative", scale: 0.65 },
    { label: "base", scale: 1 },
    { label: "aggressive", scale: 1.2 },
  ];
  const expectedWeightedReturnPct = stress.reduce((sum, row) => sum + row.weightedReturnPct, 0);
  const worstScenarioReturnPct = Math.min(0, ...stress.map((row) => row.returnPct));
  const sizeCandidates = sizeScales.map((entry) => {
    const weightPct = clamp(baseWeightPct * entry.scale, 0, constraints.maxSingleNameWeightPct);
    const riskBudgetPct =
      weightPct > 0 ? clamp(weightPct * 0.45, 0.2, constraints.maxRiskBudgetPct) : 0;
    const expectedPnlPct = (weightPct / 100) * expectedWeightedReturnPct;
    const downsidePnlPct = Math.abs((weightPct / 100) * worstScenarioReturnPct);
    const score = clamp01(
      0.45 * decisionScore +
        0.25 * (weightPct > 0 ? clamp01(expectedPnlPct / 2.2) : 0) +
        0.3 * (1 - clamp01(downsidePnlPct / Math.max(0.5, constraints.maxDownsideLossPct * 1.2))),
    );
    let candidateRecommendation: PositionSizeCandidate["recommendation"] = "watch";
    if (weightPct <= 0 || recommendation === "avoid") {
      candidateRecommendation = "avoid";
    } else if (
      downsidePnlPct <= constraints.maxDownsideLossPct &&
      score >= 0.64 &&
      recommendation === "enter"
    ) {
      candidateRecommendation = "enter";
    }
    const notes = [
      `weight=${weightPct.toFixed(2)}%`,
      `risk_budget=${riskBudgetPct.toFixed(2)}%`,
      `expected_pnl=${expectedPnlPct.toFixed(2)}%`,
      `downside_pnl=${downsidePnlPct.toFixed(2)}%`,
    ];
    return {
      label: entry.label,
      weightPct,
      riskBudgetPct,
      expectedPnlPct,
      downsidePnlPct,
      score,
      recommendation: candidateRecommendation,
      notes,
    };
  });

  const rationale = [
    `Final stance from allocator: ${finalStance}.`,
    `Decision score ${decisionScore.toFixed(2)} with confidence ${confidence.toFixed(2)}.`,
    `Expected return ${expectedReturnPct.toFixed(2)}% and downside risk ${downsideLossPct.toFixed(2)}%.`,
    `Adversarial coverage ${params.researchCell.debate.adversarialCoverageScore.toFixed(2)} with ${params.researchCell.debate.disconfirmingEvidence.length} disconfirming evidence items.`,
  ];

  return {
    generatedAt: new Date().toISOString(),
    ticker: params.ticker.trim().toUpperCase(),
    question: params.question,
    recommendation,
    finalStance,
    decisionScore,
    confidence,
    constraints,
    expectedReturnPct,
    downsideRiskPct: downsideLossPct,
    riskBreaches,
    stress,
    sizeCandidates,
    rationale,
    portfolio: params.portfolio,
    valuation: params.valuation,
    variant: params.variant,
    researchCell: params.researchCell,
  };
};

export const computePortfolioDecision = (params: {
  ticker: string;
  question?: string;
  constraints?: Partial<PortfolioDecisionConstraints>;
  dbPath?: string;
}): PortfolioDecisionResult => {
  const ticker = params.ticker.trim().toUpperCase();
  if (!ticker) throw new Error("ticker is required");
  const question = params.question?.trim() || "Portfolio sizing and scenario stress decision.";
  const valuation = computeValuation({
    ticker,
    dbPath: params.dbPath,
  });
  const variant = computeVariantPerception({
    ticker,
    dbPath: params.dbPath,
  });
  const portfolio = computePortfolioPlan({
    ticker,
    dbPath: params.dbPath,
  });
  const graphSnapshot = getTickerPointInTimeSnapshot({
    ticker,
    dbPath: params.dbPath,
    lookbackDays: 730,
    eventLimit: 24,
    factLimit: 200,
    metricLimit: 16,
  });
  const proxyClaims = buildProxyClaims(graphSnapshot);
  const proxyCitations = buildProxyCitations(graphSnapshot);
  const diagnostics = buildProxyDiagnostics({
    valuation,
    portfolio,
    variant,
  });
  const researchCell = runAdversarialResearchCell({
    ticker,
    question,
    claims: proxyClaims,
    citations: proxyCitations,
    variant,
    valuation,
    portfolio,
    diagnostics,
    graphSnapshot,
  });
  return composePortfolioDecision({
    ticker,
    question,
    portfolio,
    valuation,
    variant,
    researchCell,
    constraints: params.constraints,
  });
};
