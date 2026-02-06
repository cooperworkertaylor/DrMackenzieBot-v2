import { openResearchDb } from "./db.js";
import {
  computePortfolioDecision,
  type PortfolioDecisionConstraints,
  type PortfolioDecisionResult,
} from "./portfolio-decision.js";

export type PortfolioOptimizerConstraints = {
  maxGrossExposurePct: number;
  maxNetExposurePct: number;
  maxPortfolioRiskBudgetPct: number;
  maxSectorExposurePct: number;
  maxSingleNameWeightPct: number;
  maxPairwiseCorrelation: number;
  maxWeightedCorrelation: number;
  minPositionWeightPct: number;
  minCorrelationHistoryDays: number;
  maxStressLossPct: number;
  portfolioNavUsd: number;
  minAvgDailyDollarVolumeUsd: number;
  maxAdvParticipationPct: number;
  maxTurnoverPct: number;
  spreadBps: number;
  impactBpsAtMaxParticipation: number;
  liquidityLookbackDays: number;
};

export type PortfolioLiquiditySnapshot = {
  ticker: string;
  avgDailyDollarVolumeUsd?: number;
  avgDailyShareVolume?: number;
  observationCount: number;
  lookbackDays: number;
};

export type PortfolioOptimizerPosition = {
  ticker: string;
  sector: string;
  stance: "long" | "short";
  recommendation: "enter" | "watch";
  decisionScore: number;
  confidence: number;
  directionalExpectedReturnPct: number;
  signedWeightPct: number;
  absWeightPct: number;
  riskBudgetPct: number;
  expectedPnlPct: number;
  worstScenarioPnlPct: number;
  volatilityAnnualizedPct?: number;
  correlationPenalty: number;
  currentSignedWeightPct: number;
  turnoverTradePct: number;
  avgDailyDollarVolumeUsd?: number;
  advParticipationPct?: number;
  estimatedTransactionCostBps: number;
  estimatedTransactionCostPct: number;
  expectedNetPnlPct: number;
  rationale: string[];
};

export type PortfolioCorrelationEdge = {
  left: string;
  right: string;
  correlation: number;
  overlapDays: number;
};

export type PortfolioScenarioAggregate = {
  scenario: string;
  portfolioPnlPct: number;
  breachesStressLossLimit: boolean;
};

export type PortfolioOptimizationResult = {
  generatedAt: string;
  question: string;
  tickers: string[];
  constraints: PortfolioOptimizerConstraints;
  positions: PortfolioOptimizerPosition[];
  dropped: Array<{ ticker: string; reason: string }>;
  metrics: {
    grossExposurePct: number;
    netExposurePct: number;
    portfolioRiskBudgetPct: number;
    expectedReturnPct: number;
    expectedPnlPct: number;
    expectedNetPnlPct: number;
    worstScenarioPnlPct: number;
    diversificationScore: number;
    effectiveNames: number;
    weightedCorrelation: number;
    turnoverPct: number;
    transactionCostPct: number;
    liquidityCoveragePct: number;
  };
  scenarioStress: PortfolioScenarioAggregate[];
  sectorExposurePct: Record<string, number>;
  pairwiseCorrelation: PortfolioCorrelationEdge[];
  constraintBreaches: string[];
  liquidity: Record<string, PortfolioLiquiditySnapshot>;
};

type ReturnSeries = {
  ticker: string;
  returnsByDate: Map<string, number>;
  volatilityAnnualizedPct?: number;
};

type RiskModel = {
  returnSeriesByTicker: Map<string, ReturnSeries>;
  pairwiseCorrelation: PortfolioCorrelationEdge[];
  sectors: Map<string, string>;
  liquidityByTicker: Map<string, PortfolioLiquiditySnapshot>;
};

type OptimizerCandidate = {
  ticker: string;
  sector: string;
  stance: "long" | "short";
  recommendation: "enter" | "watch";
  direction: 1 | -1;
  decisionScore: number;
  confidence: number;
  expectedReturnPct: number;
  directionalExpectedReturnPct: number;
  baseWeightPct: number;
  baseRiskBudgetPct: number;
  worstScenarioReturnPct: number;
  initialWeightPct: number;
  correlationPenalty: number;
  weightPct: number;
  riskBudgetPct: number;
  volatilityAnnualizedPct?: number;
  currentSignedWeightPct: number;
  turnoverTradePct: number;
  avgDailyDollarVolumeUsd?: number;
  advParticipationPct?: number;
  estimatedTransactionCostBps: number;
  estimatedTransactionCostPct: number;
  expectedNetPnlPct: number;
  rationale: string[];
};

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const clamp01 = (value: number): number => clamp(value, 0, 1);

const parseNumeric = (value: unknown, fallback: number): number =>
  typeof value === "number" && Number.isFinite(value) ? value : fallback;

const mean = (values: number[]): number =>
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const stddev = (values: number[]): number => {
  if (values.length < 2) return 0;
  const avg = mean(values);
  const variance = values.reduce((sum, value) => sum + (value - avg) ** 2, 0) / (values.length - 1);
  return Math.sqrt(Math.max(0, variance));
};

const covariance = (left: number[], right: number[]): number => {
  if (left.length < 2 || right.length < 2 || left.length !== right.length) return 0;
  const leftMean = mean(left);
  const rightMean = mean(right);
  let sum = 0;
  for (let index = 0; index < left.length; index += 1) {
    sum += (left[index] - leftMean) * (right[index] - rightMean);
  }
  return sum / (left.length - 1);
};

const defaultConstraints = (): PortfolioOptimizerConstraints => ({
  maxGrossExposurePct: 24,
  maxNetExposurePct: 10,
  maxPortfolioRiskBudgetPct: 7,
  maxSectorExposurePct: 9,
  maxSingleNameWeightPct: 8,
  maxPairwiseCorrelation: 0.72,
  maxWeightedCorrelation: 0.6,
  minPositionWeightPct: 0.35,
  minCorrelationHistoryDays: 40,
  maxStressLossPct: 3.5,
  portfolioNavUsd: 25_000_000,
  minAvgDailyDollarVolumeUsd: 5_000_000,
  maxAdvParticipationPct: 0.12,
  maxTurnoverPct: 18,
  spreadBps: 6,
  impactBpsAtMaxParticipation: 35,
  liquidityLookbackDays: 20,
});

const normalizeConstraints = (
  constraints?: Partial<PortfolioOptimizerConstraints>,
): PortfolioOptimizerConstraints => {
  const defaults = defaultConstraints();
  return {
    maxGrossExposurePct: clamp(
      parseNumeric(constraints?.maxGrossExposurePct, defaults.maxGrossExposurePct),
      2,
      120,
    ),
    maxNetExposurePct: clamp(
      parseNumeric(constraints?.maxNetExposurePct, defaults.maxNetExposurePct),
      0,
      80,
    ),
    maxPortfolioRiskBudgetPct: clamp(
      parseNumeric(constraints?.maxPortfolioRiskBudgetPct, defaults.maxPortfolioRiskBudgetPct),
      0.25,
      40,
    ),
    maxSectorExposurePct: clamp(
      parseNumeric(constraints?.maxSectorExposurePct, defaults.maxSectorExposurePct),
      0.5,
      60,
    ),
    maxSingleNameWeightPct: clamp(
      parseNumeric(constraints?.maxSingleNameWeightPct, defaults.maxSingleNameWeightPct),
      0.25,
      25,
    ),
    maxPairwiseCorrelation: clamp01(
      parseNumeric(constraints?.maxPairwiseCorrelation, defaults.maxPairwiseCorrelation),
    ),
    maxWeightedCorrelation: clamp01(
      parseNumeric(constraints?.maxWeightedCorrelation, defaults.maxWeightedCorrelation),
    ),
    minPositionWeightPct: clamp(
      parseNumeric(constraints?.minPositionWeightPct, defaults.minPositionWeightPct),
      0.05,
      4,
    ),
    minCorrelationHistoryDays: Math.round(
      clamp(
        parseNumeric(constraints?.minCorrelationHistoryDays, defaults.minCorrelationHistoryDays),
        10,
        252,
      ),
    ),
    maxStressLossPct: clamp(
      parseNumeric(constraints?.maxStressLossPct, defaults.maxStressLossPct),
      0.25,
      20,
    ),
    portfolioNavUsd: clamp(
      parseNumeric(constraints?.portfolioNavUsd, defaults.portfolioNavUsd),
      100_000,
      10_000_000_000,
    ),
    minAvgDailyDollarVolumeUsd: clamp(
      parseNumeric(constraints?.minAvgDailyDollarVolumeUsd, defaults.minAvgDailyDollarVolumeUsd),
      0,
      10_000_000_000,
    ),
    maxAdvParticipationPct: clamp(
      parseNumeric(constraints?.maxAdvParticipationPct, defaults.maxAdvParticipationPct),
      0.01,
      1,
    ),
    maxTurnoverPct: clamp(
      parseNumeric(constraints?.maxTurnoverPct, defaults.maxTurnoverPct),
      0.5,
      200,
    ),
    spreadBps: clamp(parseNumeric(constraints?.spreadBps, defaults.spreadBps), 0, 150),
    impactBpsAtMaxParticipation: clamp(
      parseNumeric(constraints?.impactBpsAtMaxParticipation, defaults.impactBpsAtMaxParticipation),
      0,
      500,
    ),
    liquidityLookbackDays: Math.round(
      clamp(
        parseNumeric(constraints?.liquidityLookbackDays, defaults.liquidityLookbackDays),
        5,
        252,
      ),
    ),
  };
};

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

const mapScenarioReturns = (decision: PortfolioDecisionResult): Map<string, number> => {
  const direction = decision.finalStance === "short" ? -1 : decision.finalStance === "long" ? 1 : 0;
  const map = new Map<string, number>();
  if (direction === 0) return map;
  for (const scenario of decision.stress) {
    map.set(scenario.scenario, scenario.returnPct * direction);
  }
  return map;
};

const computeDirectionalExpectedReturn = (decision: PortfolioDecisionResult): number => {
  if (decision.finalStance === "short") return -decision.expectedReturnPct;
  if (decision.finalStance === "long") return decision.expectedReturnPct;
  return 0;
};

const buildCandidates = (params: {
  decisions: PortfolioDecisionResult[];
  constraints: PortfolioOptimizerConstraints;
  sectors: Map<string, string>;
  volatilityByTicker: Map<string, number | undefined>;
  liquidityByTicker: Map<string, PortfolioLiquiditySnapshot>;
  currentWeightsSignedPct: Map<string, number>;
}): {
  candidates: OptimizerCandidate[];
  dropped: Array<{ ticker: string; reason: string }>;
} => {
  const dropped: Array<{ ticker: string; reason: string }> = [];
  const candidates: OptimizerCandidate[] = [];
  for (const decision of params.decisions) {
    const ticker = decision.ticker;
    const sector = params.sectors.get(ticker) ?? "Unknown";
    const liquidity = params.liquidityByTicker.get(ticker);
    const avgDailyDollarVolumeUsd = liquidity?.avgDailyDollarVolumeUsd;
    if (
      typeof avgDailyDollarVolumeUsd === "number" &&
      avgDailyDollarVolumeUsd < params.constraints.minAvgDailyDollarVolumeUsd
    ) {
      dropped.push({
        ticker,
        reason: `Average daily dollar volume $${avgDailyDollarVolumeUsd.toFixed(0)} below minimum $${params.constraints.minAvgDailyDollarVolumeUsd.toFixed(0)}.`,
      });
      continue;
    }
    if (decision.recommendation === "avoid") {
      dropped.push({ ticker, reason: "Decision layer returned avoid." });
      continue;
    }
    if (decision.finalStance !== "long" && decision.finalStance !== "short") {
      dropped.push({ ticker, reason: "Non-actionable stance (watch/insufficient-evidence)." });
      continue;
    }
    const baseSize =
      decision.sizeCandidates.find((candidate) => candidate.label === "base") ??
      decision.sizeCandidates[0];
    if (!baseSize || baseSize.weightPct <= 0) {
      dropped.push({ ticker, reason: "No actionable base size candidate." });
      continue;
    }
    const directionalExpectedReturnPct = computeDirectionalExpectedReturn(decision);
    if (directionalExpectedReturnPct <= -1.5) {
      dropped.push({ ticker, reason: "Directional expected return is materially negative." });
      continue;
    }
    const recommendationScale = decision.recommendation === "enter" ? 1 : 0.55;
    const qualityScore = clamp01(
      0.55 * decision.decisionScore +
        0.25 * decision.confidence +
        0.2 * clamp01((directionalExpectedReturnPct + 20) / 45),
    );
    const initialWeightPct = clamp(
      baseSize.weightPct * recommendationScale * clamp(0.45 + qualityScore, 0.3, 1.2),
      0,
      params.constraints.maxSingleNameWeightPct,
    );
    if (initialWeightPct < params.constraints.minPositionWeightPct) {
      dropped.push({ ticker, reason: "Initial weight falls below minimum tradable size." });
      continue;
    }
    const worstScenarioReturnPct =
      decision.finalStance === "short"
        ? Math.min(0, ...decision.stress.map((row) => -row.returnPct))
        : Math.min(0, ...decision.stress.map((row) => row.returnPct));
    const direction = decision.finalStance === "long" ? 1 : -1;
    const baseRiskBudgetPct = clamp(
      baseSize.riskBudgetPct,
      0,
      decision.constraints.maxRiskBudgetPct,
    );
    const currentSignedWeightPct = params.currentWeightsSignedPct.get(ticker) ?? 0;
    candidates.push({
      ticker,
      sector,
      stance: decision.finalStance,
      recommendation: decision.recommendation,
      direction,
      decisionScore: decision.decisionScore,
      confidence: decision.confidence,
      expectedReturnPct: decision.expectedReturnPct,
      directionalExpectedReturnPct,
      baseWeightPct: baseSize.weightPct,
      baseRiskBudgetPct,
      worstScenarioReturnPct,
      initialWeightPct,
      correlationPenalty: 1,
      weightPct: initialWeightPct,
      riskBudgetPct: 0,
      volatilityAnnualizedPct: params.volatilityByTicker.get(ticker),
      currentSignedWeightPct,
      turnoverTradePct: 0,
      avgDailyDollarVolumeUsd,
      advParticipationPct: undefined,
      estimatedTransactionCostBps: 0,
      estimatedTransactionCostPct: 0,
      expectedNetPnlPct: 0,
      rationale: [
        ...decision.rationale,
        ...(typeof avgDailyDollarVolumeUsd !== "number"
          ? ["Liquidity history missing; applying conservative execution cost proxy."]
          : []),
      ],
    });
  }
  return { candidates, dropped };
};

const computeCorrelationMatrix = (
  tickers: string[],
  seriesByTicker: Map<string, ReturnSeries>,
  minOverlapDays: number,
): PortfolioCorrelationEdge[] => {
  const edges: PortfolioCorrelationEdge[] = [];
  for (let i = 0; i < tickers.length; i += 1) {
    for (let j = i + 1; j < tickers.length; j += 1) {
      const left = tickers[i]!;
      const right = tickers[j]!;
      const leftSeries = seriesByTicker.get(left);
      const rightSeries = seriesByTicker.get(right);
      if (!leftSeries || !rightSeries) {
        edges.push({ left, right, correlation: 0.35, overlapDays: 0 });
        continue;
      }
      const leftReturns: number[] = [];
      const rightReturns: number[] = [];
      for (const [date, leftValue] of leftSeries.returnsByDate) {
        const rightValue = rightSeries.returnsByDate.get(date);
        if (typeof rightValue !== "number") continue;
        leftReturns.push(leftValue);
        rightReturns.push(rightValue);
      }
      if (leftReturns.length < minOverlapDays) {
        edges.push({ left, right, correlation: 0.35, overlapDays: leftReturns.length });
        continue;
      }
      const leftVol = stddev(leftReturns);
      const rightVol = stddev(rightReturns);
      if (leftVol <= 1e-9 || rightVol <= 1e-9) {
        edges.push({ left, right, correlation: 0, overlapDays: leftReturns.length });
        continue;
      }
      const corr = covariance(leftReturns, rightReturns) / (leftVol * rightVol);
      edges.push({
        left,
        right,
        correlation: clamp(corr, -1, 1),
        overlapDays: leftReturns.length,
      });
    }
  }
  return edges;
};

const applyPairwiseCorrelationPenalty = (params: {
  candidates: OptimizerCandidate[];
  pairwiseCorrelation: PortfolioCorrelationEdge[];
  constraints: PortfolioOptimizerConstraints;
}) => {
  const byTicker = new Map(params.candidates.map((candidate) => [candidate.ticker, candidate]));
  for (const edge of params.pairwiseCorrelation) {
    if (edge.correlation <= params.constraints.maxPairwiseCorrelation) continue;
    const left = byTicker.get(edge.left);
    const right = byTicker.get(edge.right);
    if (!left || !right) continue;
    if (left.direction !== right.direction) continue;
    const overage = edge.correlation - params.constraints.maxPairwiseCorrelation;
    const penalty = clamp(1 - overage * 0.65, 0.5, 1);
    const weaker =
      left.decisionScore <= right.decisionScore
        ? left
        : right.decisionScore < left.decisionScore
          ? right
          : left.confidence <= right.confidence
            ? left
            : right;
    weaker.correlationPenalty = Math.min(weaker.correlationPenalty, penalty);
  }
  for (const candidate of params.candidates) {
    candidate.weightPct = clamp(
      candidate.initialWeightPct * candidate.correlationPenalty,
      0,
      params.constraints.maxSingleNameWeightPct,
    );
  }
};

const signedWeight = (candidate: OptimizerCandidate): number =>
  candidate.weightPct * candidate.direction;

const applySignedWeight = (candidate: OptimizerCandidate, signed: number) => {
  candidate.direction = signed >= 0 ? 1 : -1;
  candidate.stance = candidate.direction > 0 ? "long" : "short";
  candidate.weightPct = Math.abs(signed);
};

const updateTradeLiquidityAndCosts = (params: {
  candidates: OptimizerCandidate[];
  constraints: PortfolioOptimizerConstraints;
}) => {
  for (const candidate of params.candidates) {
    const targetSignedWeight = signedWeight(candidate);
    const turnoverTradePct = Math.abs(targetSignedWeight - candidate.currentSignedWeightPct);
    candidate.turnoverTradePct = turnoverTradePct;
    const adv = candidate.avgDailyDollarVolumeUsd;
    const hasLiquidity = typeof adv === "number" && adv > 1;
    const participationPct = hasLiquidity
      ? (params.constraints.portfolioNavUsd * (turnoverTradePct / 100)) / adv
      : undefined;
    candidate.advParticipationPct = participationPct;
    const normalizedParticipation =
      typeof participationPct === "number" && participationPct > 0
        ? clamp(participationPct / params.constraints.maxAdvParticipationPct, 0, 4)
        : 1;
    const transactionCostBps =
      params.constraints.spreadBps +
      params.constraints.impactBpsAtMaxParticipation * Math.sqrt(normalizedParticipation);
    candidate.estimatedTransactionCostBps = transactionCostBps;
    candidate.estimatedTransactionCostPct = (turnoverTradePct * transactionCostBps) / 10_000;
    candidate.expectedNetPnlPct =
      (candidate.weightPct / 100) * candidate.directionalExpectedReturnPct -
      candidate.estimatedTransactionCostPct;
  }
};

const enforceAdvParticipation = (params: {
  candidates: OptimizerCandidate[];
  constraints: PortfolioOptimizerConstraints;
}) => {
  for (const candidate of params.candidates) {
    if (
      typeof candidate.advParticipationPct !== "number" ||
      candidate.advParticipationPct <= params.constraints.maxAdvParticipationPct
    ) {
      continue;
    }
    const targetSignedWeight = signedWeight(candidate);
    const delta = targetSignedWeight - candidate.currentSignedWeightPct;
    const scale = clamp(
      params.constraints.maxAdvParticipationPct / candidate.advParticipationPct,
      0,
      1,
    );
    const adjustedSigned = candidate.currentSignedWeightPct + delta * scale;
    applySignedWeight(candidate, adjustedSigned);
  }
  updateTradeLiquidityAndCosts(params);
};

const enforceTurnoverCap = (params: {
  candidates: OptimizerCandidate[];
  constraints: PortfolioOptimizerConstraints;
}) => {
  const totalTurnover = params.candidates.reduce(
    (sum, candidate) => sum + candidate.turnoverTradePct,
    0,
  );
  if (totalTurnover <= params.constraints.maxTurnoverPct || totalTurnover <= 1e-9) return;
  const scaling = clamp(params.constraints.maxTurnoverPct / totalTurnover, 0, 1);
  for (const candidate of params.candidates) {
    const targetSigned = signedWeight(candidate);
    const delta = targetSigned - candidate.currentSignedWeightPct;
    const adjustedSigned = candidate.currentSignedWeightPct + delta * scaling;
    applySignedWeight(candidate, adjustedSigned);
  }
  updateTradeLiquidityAndCosts(params);
};

const enforceSingleNameCap = (params: {
  candidates: OptimizerCandidate[];
  constraints: PortfolioOptimizerConstraints;
}) => {
  for (const candidate of params.candidates) {
    candidate.weightPct = clamp(candidate.weightPct, 0, params.constraints.maxSingleNameWeightPct);
  }
};

const scaleAllWeights = (candidates: OptimizerCandidate[], factor: number) => {
  for (const candidate of candidates) {
    candidate.weightPct *= factor;
  }
};

const grossExposure = (candidates: OptimizerCandidate[]): number =>
  candidates.reduce((sum, candidate) => sum + Math.abs(candidate.weightPct), 0);

const netExposure = (candidates: OptimizerCandidate[]): number =>
  candidates.reduce((sum, candidate) => sum + candidate.weightPct * candidate.direction, 0);

const enforceGrossAndNet = (params: {
  candidates: OptimizerCandidate[];
  constraints: PortfolioOptimizerConstraints;
}) => {
  const gross = grossExposure(params.candidates);
  if (gross > params.constraints.maxGrossExposurePct && gross > 1e-9) {
    scaleAllWeights(params.candidates, params.constraints.maxGrossExposurePct / gross);
  }
  const net = netExposure(params.candidates);
  if (Math.abs(net) <= params.constraints.maxNetExposurePct || Math.abs(net) <= 1e-9) return;
  const dominantDirection: 1 | -1 = net > 0 ? 1 : -1;
  const dominant = params.candidates.filter(
    (candidate) => candidate.direction === dominantDirection,
  );
  const dominantExposure = dominant.reduce((sum, candidate) => sum + candidate.weightPct, 0);
  if (dominantExposure <= 1e-9) return;
  const overage = Math.abs(net) - params.constraints.maxNetExposurePct;
  const reductionFactor = clamp((dominantExposure - overage) / dominantExposure, 0, 1);
  for (const candidate of dominant) {
    candidate.weightPct *= reductionFactor;
  }
};

const enforceSectorCap = (params: {
  candidates: OptimizerCandidate[];
  constraints: PortfolioOptimizerConstraints;
}) => {
  const bySector = new Map<string, OptimizerCandidate[]>();
  for (const candidate of params.candidates) {
    const group = bySector.get(candidate.sector) ?? [];
    group.push(candidate);
    bySector.set(candidate.sector, group);
  }
  for (const [, group] of bySector) {
    const exposure = group.reduce((sum, candidate) => sum + Math.abs(candidate.weightPct), 0);
    if (exposure <= params.constraints.maxSectorExposurePct || exposure <= 1e-9) continue;
    const scaling = params.constraints.maxSectorExposurePct / exposure;
    for (const candidate of group) {
      candidate.weightPct *= scaling;
    }
  }
};

const applyRiskBudget = (params: {
  candidates: OptimizerCandidate[];
  constraints: PortfolioOptimizerConstraints;
}) => {
  for (const candidate of params.candidates) {
    if (candidate.baseWeightPct <= 1e-9) {
      candidate.riskBudgetPct = 0;
      continue;
    }
    candidate.riskBudgetPct = clamp(
      candidate.baseRiskBudgetPct * (candidate.weightPct / candidate.baseWeightPct),
      0,
      params.constraints.maxPortfolioRiskBudgetPct,
    );
  }
  const totalRiskBudget = params.candidates.reduce(
    (sum, candidate) => sum + candidate.riskBudgetPct,
    0,
  );
  if (totalRiskBudget <= params.constraints.maxPortfolioRiskBudgetPct || totalRiskBudget <= 1e-9)
    return;
  const scaling = params.constraints.maxPortfolioRiskBudgetPct / totalRiskBudget;
  for (const candidate of params.candidates) {
    candidate.weightPct *= scaling;
    candidate.riskBudgetPct *= scaling;
  }
};

const pruneTinyPositions = (params: {
  candidates: OptimizerCandidate[];
  constraints: PortfolioOptimizerConstraints;
  dropped: Array<{ ticker: string; reason: string }>;
}): OptimizerCandidate[] => {
  const kept: OptimizerCandidate[] = [];
  for (const candidate of params.candidates) {
    if (candidate.weightPct + 1e-9 >= params.constraints.minPositionWeightPct) {
      kept.push(candidate);
    } else {
      params.dropped.push({
        ticker: candidate.ticker,
        reason: `Optimized weight ${candidate.weightPct.toFixed(2)}% below minimum tradable size.`,
      });
    }
  }
  return kept;
};

const buildSectorExposure = (candidates: OptimizerCandidate[]): Record<string, number> => {
  const out: Record<string, number> = {};
  for (const candidate of candidates) {
    out[candidate.sector] = (out[candidate.sector] ?? 0) + Math.abs(candidate.weightPct);
  }
  return out;
};

const computeWeightedCorrelation = (
  candidates: OptimizerCandidate[],
  edges: PortfolioCorrelationEdge[],
): number => {
  const gross = grossExposure(candidates);
  if (gross <= 1e-9 || candidates.length < 2) return 0;
  const normalized = new Map<string, number>();
  for (const candidate of candidates) {
    normalized.set(candidate.ticker, Math.abs(candidate.weightPct) / gross);
  }
  let numerator = 0;
  let denominator = 0;
  for (const edge of edges) {
    const leftWeight = normalized.get(edge.left);
    const rightWeight = normalized.get(edge.right);
    if (typeof leftWeight !== "number" || typeof rightWeight !== "number") continue;
    const pairWeight = leftWeight * rightWeight;
    numerator += pairWeight * edge.correlation;
    denominator += pairWeight;
  }
  if (denominator <= 1e-9) return 0;
  return numerator / denominator;
};

const summarizeDiversification = (
  candidates: OptimizerCandidate[],
): {
  diversificationScore: number;
  effectiveNames: number;
} => {
  const gross = grossExposure(candidates);
  if (gross <= 1e-9 || candidates.length === 0) {
    return { diversificationScore: 0, effectiveNames: 0 };
  }
  const hhi = candidates.reduce((sum, candidate) => {
    const share = Math.abs(candidate.weightPct) / gross;
    return sum + share ** 2;
  }, 0);
  const effectiveNames = hhi > 1e-9 ? 1 / hhi : 0;
  const diversificationScore = clamp01(
    candidates.length <= 1 ? 0 : (effectiveNames - 1) / (candidates.length - 1),
  );
  return { diversificationScore, effectiveNames };
};

const buildScenarioStress = (params: {
  candidates: OptimizerCandidate[];
  decisionByTicker: Map<string, PortfolioDecisionResult>;
  constraints: PortfolioOptimizerConstraints;
}): PortfolioScenarioAggregate[] => {
  const scenarioNames = new Set<string>();
  const scenarioByTicker = new Map<string, Map<string, number>>();
  for (const candidate of params.candidates) {
    const decision = params.decisionByTicker.get(candidate.ticker);
    if (!decision) continue;
    const returns = mapScenarioReturns(decision);
    scenarioByTicker.set(candidate.ticker, returns);
    for (const scenario of returns.keys()) {
      scenarioNames.add(scenario);
    }
  }
  const rows: PortfolioScenarioAggregate[] = [];
  for (const scenario of scenarioNames) {
    let portfolioPnlPct = 0;
    for (const candidate of params.candidates) {
      const scenarioMap = scenarioByTicker.get(candidate.ticker);
      const directionalReturn = scenarioMap?.get(scenario) ?? 0;
      portfolioPnlPct += (candidate.weightPct / 100) * directionalReturn;
    }
    rows.push({
      scenario,
      portfolioPnlPct,
      breachesStressLossLimit:
        Math.abs(Math.min(0, portfolioPnlPct)) > params.constraints.maxStressLossPct,
    });
  }
  return rows.sort((left, right) => left.scenario.localeCompare(right.scenario));
};

const buildConstraintBreaches = (params: {
  candidates: OptimizerCandidate[];
  constraints: PortfolioOptimizerConstraints;
  sectorExposurePct: Record<string, number>;
  weightedCorrelation: number;
  scenarioStress: PortfolioScenarioAggregate[];
}): string[] => {
  const breaches: string[] = [];
  const gross = grossExposure(params.candidates);
  const net = netExposure(params.candidates);
  const riskBudget = params.candidates.reduce((sum, candidate) => sum + candidate.riskBudgetPct, 0);
  const turnover = params.candidates.reduce(
    (sum, candidate) => sum + candidate.turnoverTradePct,
    0,
  );
  if (gross > params.constraints.maxGrossExposurePct + 1e-6) {
    breaches.push(
      `Gross exposure ${gross.toFixed(2)}% exceeds ${params.constraints.maxGrossExposurePct.toFixed(2)}%.`,
    );
  }
  if (Math.abs(net) > params.constraints.maxNetExposurePct + 1e-6) {
    breaches.push(
      `Net exposure ${net.toFixed(2)}% exceeds +/-${params.constraints.maxNetExposurePct.toFixed(2)}%.`,
    );
  }
  if (riskBudget > params.constraints.maxPortfolioRiskBudgetPct + 1e-6) {
    breaches.push(
      `Portfolio risk budget ${riskBudget.toFixed(2)}% exceeds ${params.constraints.maxPortfolioRiskBudgetPct.toFixed(2)}%.`,
    );
  }
  if (turnover > params.constraints.maxTurnoverPct + 1e-6) {
    breaches.push(
      `Turnover ${turnover.toFixed(2)}% exceeds ${params.constraints.maxTurnoverPct.toFixed(2)}%.`,
    );
  }
  for (const [sector, exposure] of Object.entries(params.sectorExposurePct)) {
    if (exposure <= params.constraints.maxSectorExposurePct + 1e-6) continue;
    breaches.push(
      `Sector ${sector} exposure ${exposure.toFixed(2)}% exceeds ${params.constraints.maxSectorExposurePct.toFixed(2)}%.`,
    );
  }
  if (params.weightedCorrelation > params.constraints.maxWeightedCorrelation + 1e-6) {
    breaches.push(
      `Weighted correlation ${params.weightedCorrelation.toFixed(2)} exceeds ${params.constraints.maxWeightedCorrelation.toFixed(2)}.`,
    );
  }
  const worstStress = Math.abs(
    params.scenarioStress.reduce((worst, row) => Math.min(worst, row.portfolioPnlPct), 0),
  );
  if (worstStress > params.constraints.maxStressLossPct + 1e-6) {
    breaches.push(
      `Worst stress loss ${worstStress.toFixed(2)}% exceeds ${params.constraints.maxStressLossPct.toFixed(2)}%.`,
    );
  }
  for (const candidate of params.candidates) {
    if (
      typeof candidate.advParticipationPct === "number" &&
      candidate.advParticipationPct > params.constraints.maxAdvParticipationPct + 1e-6
    ) {
      breaches.push(
        `${candidate.ticker} ADV participation ${(candidate.advParticipationPct * 100).toFixed(2)}% exceeds ${(params.constraints.maxAdvParticipationPct * 100).toFixed(2)}%.`,
      );
    }
  }
  return breaches;
};

export const composePortfolioOptimization = (params: {
  tickers: string[];
  question: string;
  decisions: PortfolioDecisionResult[];
  constraints?: Partial<PortfolioOptimizerConstraints>;
  sectors?: Record<string, string>;
  pairwiseCorrelation?: PortfolioCorrelationEdge[];
  volatilityAnnualizedPctByTicker?: Record<string, number | undefined>;
  currentWeightsSignedPct?: Record<string, number>;
  liquidityByTicker?: Record<string, PortfolioLiquiditySnapshot>;
}): PortfolioOptimizationResult => {
  const constraints = normalizeConstraints(params.constraints);
  const sectors = new Map<string, string>(
    Object.entries(params.sectors ?? {}).map(([ticker, sector]) => [
      normalizeTicker(ticker),
      sector,
    ]),
  );
  const volatilityByTicker = new Map<string, number | undefined>(
    Object.entries(params.volatilityAnnualizedPctByTicker ?? {}).map(([ticker, value]) => [
      normalizeTicker(ticker),
      value,
    ]),
  );
  const currentWeightsSignedPct = new Map<string, number>(
    Object.entries(params.currentWeightsSignedPct ?? {}).map(([ticker, value]) => [
      normalizeTicker(ticker),
      Number.isFinite(value) ? value : 0,
    ]),
  );
  const liquidityByTicker = new Map<string, PortfolioLiquiditySnapshot>(
    Object.entries(params.liquidityByTicker ?? {}).map(([ticker, value]) => [
      normalizeTicker(ticker),
      {
        ticker: normalizeTicker(value.ticker || ticker),
        avgDailyDollarVolumeUsd:
          typeof value.avgDailyDollarVolumeUsd === "number" &&
          Number.isFinite(value.avgDailyDollarVolumeUsd)
            ? value.avgDailyDollarVolumeUsd
            : undefined,
        avgDailyShareVolume:
          typeof value.avgDailyShareVolume === "number" &&
          Number.isFinite(value.avgDailyShareVolume)
            ? value.avgDailyShareVolume
            : undefined,
        observationCount:
          typeof value.observationCount === "number" && Number.isFinite(value.observationCount)
            ? value.observationCount
            : 0,
        lookbackDays:
          typeof value.lookbackDays === "number" && Number.isFinite(value.lookbackDays)
            ? value.lookbackDays
            : constraints.liquidityLookbackDays,
      },
    ]),
  );
  const decisions = params.decisions.map((decision) => ({
    ...decision,
    ticker: normalizeTicker(decision.ticker),
  }));
  const decisionByTicker = new Map(decisions.map((decision) => [decision.ticker, decision]));
  const { candidates: seededCandidates, dropped } = buildCandidates({
    decisions,
    constraints,
    sectors,
    volatilityByTicker,
    liquidityByTicker,
    currentWeightsSignedPct,
  });
  if (!seededCandidates.length) {
    throw new Error("No actionable positions after applying single-name decision filters.");
  }
  const pairwiseCorrelation = params.pairwiseCorrelation ?? [];
  applyPairwiseCorrelationPenalty({
    candidates: seededCandidates,
    pairwiseCorrelation,
    constraints,
  });
  enforceSingleNameCap({ candidates: seededCandidates, constraints });
  updateTradeLiquidityAndCosts({
    candidates: seededCandidates,
    constraints,
  });
  enforceAdvParticipation({
    candidates: seededCandidates,
    constraints,
  });
  enforceSectorCap({ candidates: seededCandidates, constraints });
  enforceGrossAndNet({ candidates: seededCandidates, constraints });
  applyRiskBudget({ candidates: seededCandidates, constraints });
  updateTradeLiquidityAndCosts({
    candidates: seededCandidates,
    constraints,
  });
  enforceTurnoverCap({
    candidates: seededCandidates,
    constraints,
  });
  enforceSingleNameCap({ candidates: seededCandidates, constraints });
  enforceSectorCap({ candidates: seededCandidates, constraints });
  enforceGrossAndNet({ candidates: seededCandidates, constraints });
  applyRiskBudget({ candidates: seededCandidates, constraints });
  updateTradeLiquidityAndCosts({
    candidates: seededCandidates,
    constraints,
  });
  const candidates = pruneTinyPositions({
    candidates: seededCandidates,
    constraints,
    dropped,
  });
  if (!candidates.length) {
    throw new Error("All candidate positions were pruned by portfolio constraints.");
  }
  const sectorExposurePct = buildSectorExposure(candidates);
  const weightedCorrelation = computeWeightedCorrelation(candidates, pairwiseCorrelation);
  const scenarioStress = buildScenarioStress({
    candidates,
    decisionByTicker,
    constraints,
  });
  const expectedPnlPct = candidates.reduce(
    (sum, candidate) => sum + (candidate.weightPct / 100) * candidate.directionalExpectedReturnPct,
    0,
  );
  const worstScenarioPnlPct = scenarioStress.reduce(
    (worst, row) => Math.min(worst, row.portfolioPnlPct),
    0,
  );
  const diversification = summarizeDiversification(candidates);
  const positions: PortfolioOptimizerPosition[] = candidates
    .map((candidate) => ({
      ticker: candidate.ticker,
      sector: candidate.sector,
      stance: candidate.stance,
      recommendation: candidate.recommendation,
      decisionScore: candidate.decisionScore,
      confidence: candidate.confidence,
      directionalExpectedReturnPct: candidate.directionalExpectedReturnPct,
      signedWeightPct: candidate.weightPct * candidate.direction,
      absWeightPct: candidate.weightPct,
      riskBudgetPct: candidate.riskBudgetPct,
      expectedPnlPct: (candidate.weightPct / 100) * candidate.directionalExpectedReturnPct,
      worstScenarioPnlPct: (candidate.weightPct / 100) * candidate.worstScenarioReturnPct,
      volatilityAnnualizedPct: candidate.volatilityAnnualizedPct,
      correlationPenalty: candidate.correlationPenalty,
      currentSignedWeightPct: candidate.currentSignedWeightPct,
      turnoverTradePct: candidate.turnoverTradePct,
      avgDailyDollarVolumeUsd: candidate.avgDailyDollarVolumeUsd,
      advParticipationPct: candidate.advParticipationPct,
      estimatedTransactionCostBps: candidate.estimatedTransactionCostBps,
      estimatedTransactionCostPct: candidate.estimatedTransactionCostPct,
      expectedNetPnlPct: candidate.expectedNetPnlPct,
      rationale: candidate.rationale,
    }))
    .sort((left, right) => Math.abs(right.signedWeightPct) - Math.abs(left.signedWeightPct));
  const expectedNetPnlPct = candidates.reduce(
    (sum, candidate) => sum + candidate.expectedNetPnlPct,
    0,
  );
  const turnoverPct = candidates.reduce((sum, candidate) => sum + candidate.turnoverTradePct, 0);
  const transactionCostPct = candidates.reduce(
    (sum, candidate) => sum + candidate.estimatedTransactionCostPct,
    0,
  );
  const liquidityCoveragePct = candidates.length
    ? candidates.filter((candidate) => typeof candidate.avgDailyDollarVolumeUsd === "number")
        .length / candidates.length
    : 0;
  const metrics = {
    grossExposurePct: grossExposure(candidates),
    netExposurePct: netExposure(candidates),
    portfolioRiskBudgetPct: candidates.reduce((sum, candidate) => sum + candidate.riskBudgetPct, 0),
    expectedReturnPct:
      grossExposure(candidates) > 1e-9 ? (expectedPnlPct / grossExposure(candidates)) * 100 : 0,
    expectedPnlPct,
    expectedNetPnlPct,
    worstScenarioPnlPct,
    diversificationScore: diversification.diversificationScore,
    effectiveNames: diversification.effectiveNames,
    weightedCorrelation,
    turnoverPct,
    transactionCostPct,
    liquidityCoveragePct,
  };
  const constraintBreaches = buildConstraintBreaches({
    candidates,
    constraints,
    sectorExposurePct,
    weightedCorrelation,
    scenarioStress,
  });
  return {
    generatedAt: new Date().toISOString(),
    question: params.question,
    tickers: params.tickers.map(normalizeTicker),
    constraints,
    positions,
    dropped,
    metrics,
    scenarioStress,
    sectorExposurePct,
    pairwiseCorrelation: pairwiseCorrelation
      .filter((edge) => edge.overlapDays >= constraints.minCorrelationHistoryDays)
      .toSorted((left, right) => Math.abs(right.correlation) - Math.abs(left.correlation)),
    constraintBreaches,
    liquidity: Object.fromEntries(
      Array.from(liquidityByTicker.entries()).map(([ticker, snapshot]) => [ticker, snapshot]),
    ),
  };
};

const loadSectors = (tickers: string[], dbPath?: string): Map<string, string> => {
  const out = new Map<string, string>();
  if (!tickers.length) return out;
  const db = openResearchDb(dbPath);
  const placeholders = tickers.map(() => "?").join(",");
  const rows = db
    .prepare(
      `SELECT UPPER(ticker) AS ticker, COALESCE(NULLIF(TRIM(sector), ''), 'Unknown') AS sector
       FROM instruments
       WHERE UPPER(ticker) IN (${placeholders})`,
    )
    .all(...tickers) as Array<{ ticker: string; sector: string }>;
  for (const row of rows) {
    out.set(normalizeTicker(row.ticker), row.sector);
  }
  return out;
};

const loadLiquiditySnapshots = (params: {
  tickers: string[];
  lookbackDays: number;
  dbPath?: string;
}): Map<string, PortfolioLiquiditySnapshot> => {
  const out = new Map<string, PortfolioLiquiditySnapshot>();
  if (!params.tickers.length) return out;
  const db = openResearchDb(params.dbPath);
  const cutoff = new Date(Date.now() - params.lookbackDays * 24 * 60 * 60 * 1000)
    .toISOString()
    .slice(0, 10);
  const placeholders = params.tickers.map(() => "?").join(",");
  const rows = db
    .prepare(
      `SELECT UPPER(i.ticker) AS ticker, p.close, p.volume
       FROM prices p
       JOIN instruments i ON i.id = p.instrument_id
       WHERE UPPER(i.ticker) IN (${placeholders})
         AND p.date >= ?
         AND p.close IS NOT NULL
         AND p.volume IS NOT NULL
       ORDER BY p.date ASC`,
    )
    .all(...params.tickers, cutoff) as Array<{
    ticker: string;
    close: number;
    volume: number;
  }>;
  const dollarsByTicker = new Map<string, number[]>();
  const sharesByTicker = new Map<string, number[]>();
  for (const row of rows) {
    if (!Number.isFinite(row.close) || !Number.isFinite(row.volume)) continue;
    const ticker = normalizeTicker(row.ticker);
    const dollars = dollarsByTicker.get(ticker) ?? [];
    const shares = sharesByTicker.get(ticker) ?? [];
    dollars.push(row.close * row.volume);
    shares.push(row.volume);
    dollarsByTicker.set(ticker, dollars);
    sharesByTicker.set(ticker, shares);
  }
  for (const ticker of params.tickers) {
    const dollarRows = dollarsByTicker.get(ticker) ?? [];
    const shareRows = sharesByTicker.get(ticker) ?? [];
    out.set(ticker, {
      ticker,
      avgDailyDollarVolumeUsd: dollarRows.length ? mean(dollarRows) : undefined,
      avgDailyShareVolume: shareRows.length ? mean(shareRows) : undefined,
      observationCount: dollarRows.length,
      lookbackDays: params.lookbackDays,
    });
  }
  return out;
};

const loadReturnSeries = (params: {
  tickers: string[];
  lookbackDays: number;
  dbPath?: string;
}): Map<string, ReturnSeries> => {
  const out = new Map<string, ReturnSeries>();
  if (!params.tickers.length) return out;
  const db = openResearchDb(params.dbPath);
  const cutoff = new Date(Date.now() - params.lookbackDays * 24 * 60 * 60 * 1000)
    .toISOString()
    .slice(0, 10);
  const placeholders = params.tickers.map(() => "?").join(",");
  const rows = db
    .prepare(
      `SELECT UPPER(i.ticker) AS ticker, p.date, p.close
       FROM prices p
       JOIN instruments i ON i.id = p.instrument_id
       WHERE UPPER(i.ticker) IN (${placeholders})
         AND p.date >= ?
         AND p.close IS NOT NULL
       ORDER BY p.date ASC`,
    )
    .all(...params.tickers, cutoff) as Array<{ ticker: string; date: string; close: number }>;
  const priceRowsByTicker = new Map<string, Array<{ date: string; close: number }>>();
  for (const row of rows) {
    const ticker = normalizeTicker(row.ticker);
    const list = priceRowsByTicker.get(ticker) ?? [];
    list.push({ date: row.date, close: row.close });
    priceRowsByTicker.set(ticker, list);
  }
  for (const ticker of params.tickers) {
    const rowsForTicker = priceRowsByTicker.get(ticker) ?? [];
    const returnsByDate = new Map<string, number>();
    const returnValues: number[] = [];
    for (let index = 1; index < rowsForTicker.length; index += 1) {
      const current = rowsForTicker[index]!;
      const prior = rowsForTicker[index - 1]!;
      if (
        !Number.isFinite(current.close) ||
        !Number.isFinite(prior.close) ||
        Math.abs(prior.close) <= 1e-9
      ) {
        continue;
      }
      const ret = current.close / prior.close - 1;
      if (!Number.isFinite(ret)) continue;
      returnsByDate.set(current.date, ret);
      returnValues.push(ret);
    }
    const annualizedVol =
      returnValues.length >= 20 ? stddev(returnValues) * Math.sqrt(252) * 100 : undefined;
    out.set(ticker, {
      ticker,
      returnsByDate,
      volatilityAnnualizedPct: annualizedVol,
    });
  }
  return out;
};

const buildRiskModel = (params: {
  tickers: string[];
  dbPath?: string;
  lookbackDays: number;
  minOverlapDays: number;
  liquidityLookbackDays: number;
}): RiskModel => {
  const returnSeriesByTicker = loadReturnSeries({
    tickers: params.tickers,
    lookbackDays: params.lookbackDays,
    dbPath: params.dbPath,
  });
  const pairwiseCorrelation = computeCorrelationMatrix(
    params.tickers,
    returnSeriesByTicker,
    params.minOverlapDays,
  );
  const sectors = loadSectors(params.tickers, params.dbPath);
  const liquidityByTicker = loadLiquiditySnapshots({
    tickers: params.tickers,
    lookbackDays: params.liquidityLookbackDays,
    dbPath: params.dbPath,
  });
  return {
    returnSeriesByTicker,
    pairwiseCorrelation,
    sectors,
    liquidityByTicker,
  };
};

export const computePortfolioOptimization = (params: {
  tickers: string[];
  question?: string;
  decisionConstraints?: Partial<PortfolioDecisionConstraints>;
  constraints?: Partial<PortfolioOptimizerConstraints>;
  currentWeightsSignedPct?: Record<string, number>;
  dbPath?: string;
  lookbackDays?: number;
}): PortfolioOptimizationResult => {
  const uniqueTickers = Array.from(new Set(params.tickers.map(normalizeTicker).filter(Boolean)));
  if (!uniqueTickers.length) throw new Error("At least one ticker is required.");
  const constraints = normalizeConstraints(params.constraints);
  const decisions = uniqueTickers.map((ticker) =>
    computePortfolioDecision({
      ticker,
      question: params.question,
      constraints: params.decisionConstraints,
      dbPath: params.dbPath,
    }),
  );
  const riskModel = buildRiskModel({
    tickers: uniqueTickers,
    dbPath: params.dbPath,
    lookbackDays: Math.max(60, params.lookbackDays ?? 252),
    minOverlapDays: constraints.minCorrelationHistoryDays,
    liquidityLookbackDays: constraints.liquidityLookbackDays,
  });
  const volatilityAnnualizedPctByTicker: Record<string, number | undefined> = {};
  for (const [ticker, series] of riskModel.returnSeriesByTicker) {
    volatilityAnnualizedPctByTicker[ticker] = series.volatilityAnnualizedPct;
  }
  const sectorMap: Record<string, string> = {};
  for (const ticker of uniqueTickers) {
    sectorMap[ticker] = riskModel.sectors.get(ticker) ?? "Unknown";
  }
  const question =
    params.question?.trim() ||
    "Construct a constrained multi-name portfolio with institutional risk controls.";
  return composePortfolioOptimization({
    tickers: uniqueTickers,
    question,
    decisions,
    constraints,
    sectors: sectorMap,
    pairwiseCorrelation: riskModel.pairwiseCorrelation,
    volatilityAnnualizedPctByTicker,
    currentWeightsSignedPct: params.currentWeightsSignedPct,
    liquidityByTicker: Object.fromEntries(
      Array.from(riskModel.liquidityByTicker.entries()).map(([ticker, snapshot]) => [
        ticker,
        snapshot,
      ]),
    ),
  });
};
