import type { PortfolioDecisionResult } from "./portfolio-decision.js";
import type { PortfolioDecisionConstraints } from "./portfolio-decision.js";
import { openResearchDb } from "./db.js";
import {
  composePortfolioOptimization,
  type PortfolioCorrelationEdge,
  type PortfolioLiquiditySnapshot,
  type PortfolioOptimizerConstraints,
  type PortfolioOptimizationResult,
} from "./portfolio-optimizer.js";
import { appendProvenanceEvent } from "./provenance.js";

type PricePoint = {
  date: string;
  close: number;
  volume?: number;
};

type ReplaySignalInput = {
  ticker: string;
  prices: PricePoint[];
  asOfIndex: number;
  lookbackShortDays: number;
  lookbackLongDays: number;
  maxSingleNameWeightPct: number;
};

export type PortfolioReplayWindow = {
  rebalanceDate: string;
  exitDate: string;
  expectedPnlPct: number;
  expectedNetPnlPct: number;
  realizedPnlPct: number;
  realizedNetPnlPct: number;
  transactionCostPct: number;
  turnoverPct: number;
  positions: number;
  grossExposurePct: number;
  netExposurePct: number;
  weightedCorrelation: number;
  directionMatched: boolean;
};

export type PortfolioReplaySummary = {
  sampleCount: number;
  meanExpectedNetPnlPct: number;
  meanRealizedNetPnlPct: number;
  maePct: number;
  rmsePct: number;
  directionalAccuracy: number;
  winRate: number;
  expectedRealizedCorrelation: number;
  score: number;
  passed: boolean;
};

export type PortfolioReplayResult = {
  generatedAt: string;
  tickers: string[];
  question: string;
  rebalanceEveryDays: number;
  horizonDays: number;
  lookbackSignalDays: number;
  lookbackCorrelationDays: number;
  windows: PortfolioReplayWindow[];
  summary: PortfolioReplaySummary;
  latestOptimization?: PortfolioOptimizationResult;
  evaluationChecks: Array<{ name: string; passed: boolean; detail: string }>;
};

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const clamp01 = (value: number): number => clamp(value, 0, 1);

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

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

const parseDateLike = (value: string | undefined): string | undefined => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const normalized = value.trim().slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(normalized) ? normalized : undefined;
};

const findIndexByDate = (series: PricePoint[], date: string): number => {
  for (let index = 0; index < series.length; index += 1) {
    if (series[index]?.date === date) return index;
  }
  return -1;
};

const buildReturnSeries = (
  prices: PricePoint[],
  asOfIndex: number,
  lookbackDays: number,
): Array<{ date: string; value: number }> => {
  const out: Array<{ date: string; value: number }> = [];
  const start = Math.max(1, asOfIndex - lookbackDays + 1);
  for (let index = start; index <= asOfIndex; index += 1) {
    const current = prices[index];
    const prior = prices[index - 1];
    if (!current || !prior || Math.abs(prior.close) <= 1e-9) continue;
    const ret = current.close / prior.close - 1;
    if (!Number.isFinite(ret)) continue;
    out.push({ date: current.date, value: ret });
  }
  return out;
};

const correlationEdgesForDate = (params: {
  tickers: string[];
  pricesByTicker: Map<string, PricePoint[]>;
  asOfDate: string;
  lookbackDays: number;
  minOverlapDays: number;
}): {
  edges: PortfolioCorrelationEdge[];
  volatilityAnnualizedPctByTicker: Record<string, number | undefined>;
} => {
  const returnsByTicker = new Map<string, Array<{ date: string; value: number }>>();
  const volatilityAnnualizedPctByTicker: Record<string, number | undefined> = {};
  for (const ticker of params.tickers) {
    const prices = params.pricesByTicker.get(ticker) ?? [];
    const asOfIndex = findIndexByDate(prices, params.asOfDate);
    if (asOfIndex <= 1) {
      returnsByTicker.set(ticker, []);
      volatilityAnnualizedPctByTicker[ticker] = undefined;
      continue;
    }
    const returns = buildReturnSeries(prices, asOfIndex, params.lookbackDays);
    returnsByTicker.set(ticker, returns);
    const values = returns.map((row) => row.value);
    volatilityAnnualizedPctByTicker[ticker] =
      values.length >= 20 ? stddev(values) * Math.sqrt(252) * 100 : undefined;
  }

  const edges: PortfolioCorrelationEdge[] = [];
  for (let leftIndex = 0; leftIndex < params.tickers.length; leftIndex += 1) {
    for (let rightIndex = leftIndex + 1; rightIndex < params.tickers.length; rightIndex += 1) {
      const left = params.tickers[leftIndex]!;
      const right = params.tickers[rightIndex]!;
      const leftRows = returnsByTicker.get(left) ?? [];
      const rightRows = returnsByTicker.get(right) ?? [];
      const rightByDate = new Map(rightRows.map((row) => [row.date, row.value]));
      const leftValues: number[] = [];
      const rightValues: number[] = [];
      for (const row of leftRows) {
        const rightValue = rightByDate.get(row.date);
        if (typeof rightValue !== "number") continue;
        leftValues.push(row.value);
        rightValues.push(rightValue);
      }
      if (leftValues.length < params.minOverlapDays) {
        edges.push({ left, right, correlation: 0.35, overlapDays: leftValues.length });
        continue;
      }
      const leftVol = stddev(leftValues);
      const rightVol = stddev(rightValues);
      if (leftVol <= 1e-9 || rightVol <= 1e-9) {
        edges.push({ left, right, correlation: 0, overlapDays: leftValues.length });
        continue;
      }
      const corr = covariance(leftValues, rightValues) / (leftVol * rightVol);
      edges.push({ left, right, correlation: clamp(corr, -1, 1), overlapDays: leftValues.length });
    }
  }
  return { edges, volatilityAnnualizedPctByTicker };
};

const liquiditySnapshotForDate = (params: {
  tickers: string[];
  pricesByTicker: Map<string, PricePoint[]>;
  asOfDate: string;
  lookbackDays: number;
}): Record<string, PortfolioLiquiditySnapshot> => {
  const out: Record<string, PortfolioLiquiditySnapshot> = {};
  for (const ticker of params.tickers) {
    const prices = params.pricesByTicker.get(ticker) ?? [];
    const asOfIndex = findIndexByDate(prices, params.asOfDate);
    if (asOfIndex < 0) {
      out[ticker] = {
        ticker,
        avgDailyDollarVolumeUsd: undefined,
        avgDailyShareVolume: undefined,
        observationCount: 0,
        lookbackDays: params.lookbackDays,
      };
      continue;
    }
    const start = Math.max(0, asOfIndex - params.lookbackDays + 1);
    const dollars: number[] = [];
    const shares: number[] = [];
    for (let index = start; index <= asOfIndex; index += 1) {
      const row = prices[index];
      if (!row) continue;
      if (typeof row.volume === "number" && Number.isFinite(row.volume) && row.volume > 0) {
        shares.push(row.volume);
        dollars.push(row.volume * row.close);
      }
    }
    out[ticker] = {
      ticker,
      avgDailyDollarVolumeUsd: dollars.length ? mean(dollars) : undefined,
      avgDailyShareVolume: shares.length ? mean(shares) : undefined,
      observationCount: dollars.length,
      lookbackDays: params.lookbackDays,
    };
  }
  return out;
};

const buildSyntheticDecision = (
  params: ReplaySignalInput,
  decisionConstraints?: Partial<PortfolioDecisionConstraints>,
): PortfolioDecisionResult | undefined => {
  if (params.asOfIndex < params.lookbackLongDays + 2) return undefined;
  const priceNow = params.prices[params.asOfIndex]?.close;
  const priceShort = params.prices[params.asOfIndex - params.lookbackShortDays]?.close;
  const priceLong = params.prices[params.asOfIndex - params.lookbackLongDays]?.close;
  if (
    typeof priceNow !== "number" ||
    typeof priceShort !== "number" ||
    typeof priceLong !== "number" ||
    Math.abs(priceShort) <= 1e-9 ||
    Math.abs(priceLong) <= 1e-9
  ) {
    return undefined;
  }
  const retShort = priceNow / priceShort - 1;
  const retLong = priceNow / priceLong - 1;
  const returns = buildReturnSeries(params.prices, params.asOfIndex, params.lookbackLongDays).map(
    (row) => row.value,
  );
  const vol = stddev(returns);
  const signal = 0.6 * retLong + 0.4 * retShort;
  const signalAbs = Math.abs(signal);
  const stance: "long" | "short" = signal >= 0 ? "long" : "short";
  const recommendation: "enter" | "watch" = signalAbs >= 0.025 ? "enter" : "watch";
  const decisionScore = clamp01(0.44 + signalAbs * 3.6 - vol * 2.2);
  const confidence = clamp01(0.45 + signalAbs * 2.8 + clamp(returns.length / 120, 0, 0.2));
  const maxSingleNameWeightPct = clamp(
    typeof decisionConstraints?.maxSingleNameWeightPct === "number"
      ? decisionConstraints.maxSingleNameWeightPct
      : params.maxSingleNameWeightPct,
    0.25,
    20,
  );
  const baseWeightPct = clamp(1.2 + signalAbs * 42, 0.35, maxSingleNameWeightPct);
  const riskBudgetPct = clamp(baseWeightPct * 0.45, 0.25, 6);
  const stopLossPct = clamp(0.08 + vol * 2.3, 0.08, 0.28);
  const expectedReturnPct = signal * 100;
  const bearReturnPct = expectedReturnPct - vol * 100 * 2;
  const bullReturnPct = expectedReturnPct + vol * 100 * 2;
  const baseReturnPct = expectedReturnPct;
  const constraints = {
    maxSingleNameWeightPct,
    maxRiskBudgetPct: clamp(
      typeof decisionConstraints?.maxRiskBudgetPct === "number"
        ? decisionConstraints.maxRiskBudgetPct
        : 4,
      0.25,
      10,
    ),
    maxStopLossPct: clamp(
      typeof decisionConstraints?.maxStopLossPct === "number"
        ? decisionConstraints.maxStopLossPct
        : 0.25,
      0.06,
      0.4,
    ),
    minConfidence: clamp(
      typeof decisionConstraints?.minConfidence === "number"
        ? decisionConstraints.minConfidence
        : 0.55,
      0,
      1,
    ),
    requiredDebateCoverage: clamp(
      typeof decisionConstraints?.requiredDebateCoverage === "number"
        ? decisionConstraints.requiredDebateCoverage
        : 0.65,
      0,
      1,
    ),
    maxDownsideLossPct: clamp(
      typeof decisionConstraints?.maxDownsideLossPct === "number"
        ? decisionConstraints.maxDownsideLossPct
        : 3,
      0.5,
      12,
    ),
  };

  return {
    generatedAt: new Date().toISOString(),
    ticker: params.ticker,
    question: "Replay synthetic allocation signal.",
    recommendation,
    finalStance: stance,
    decisionScore,
    confidence,
    constraints,
    expectedReturnPct,
    downsideRiskPct: Math.abs(Math.min(0, bearReturnPct) * (baseWeightPct / 100)),
    riskBreaches: [],
    stress: [
      {
        scenario: "bear",
        probability: 0.25,
        returnPct: bearReturnPct,
        weightedReturnPct: bearReturnPct * 0.25,
        pnlPct: (baseWeightPct / 100) * bearReturnPct,
        breachesRiskBudget: false,
      },
      {
        scenario: "base",
        probability: 0.5,
        returnPct: baseReturnPct,
        weightedReturnPct: baseReturnPct * 0.5,
        pnlPct: (baseWeightPct / 100) * baseReturnPct,
        breachesRiskBudget: false,
      },
      {
        scenario: "bull",
        probability: 0.25,
        returnPct: bullReturnPct,
        weightedReturnPct: bullReturnPct * 0.25,
        pnlPct: (baseWeightPct / 100) * bullReturnPct,
        breachesRiskBudget: false,
      },
    ],
    sizeCandidates: [
      {
        label: "conservative",
        weightPct: baseWeightPct * 0.65,
        riskBudgetPct: riskBudgetPct * 0.65,
        expectedPnlPct: (baseWeightPct * 0.65 * expectedReturnPct) / 100,
        downsidePnlPct: Math.abs((baseWeightPct * 0.65 * Math.min(0, bearReturnPct)) / 100),
        score: clamp01(decisionScore * 0.9),
        recommendation: recommendation === "enter" ? "watch" : "watch",
        notes: ["synthetic replay candidate"],
      },
      {
        label: "base",
        weightPct: baseWeightPct,
        riskBudgetPct: riskBudgetPct,
        expectedPnlPct: (baseWeightPct * expectedReturnPct) / 100,
        downsidePnlPct: Math.abs((baseWeightPct * Math.min(0, bearReturnPct)) / 100),
        score: decisionScore,
        recommendation,
        notes: ["synthetic replay candidate"],
      },
      {
        label: "aggressive",
        weightPct: baseWeightPct * 1.2,
        riskBudgetPct: riskBudgetPct * 1.2,
        expectedPnlPct: (baseWeightPct * 1.2 * expectedReturnPct) / 100,
        downsidePnlPct: Math.abs((baseWeightPct * 1.2 * Math.min(0, bearReturnPct)) / 100),
        score: clamp01(decisionScore * 1.04),
        recommendation,
        notes: ["synthetic replay candidate"],
      },
    ],
    rationale: [
      `Signal short=${(retShort * 100).toFixed(2)}% long=${(retLong * 100).toFixed(2)}%.`,
      `Synthetic signal=${(signal * 100).toFixed(2)}% vol=${(vol * 100).toFixed(2)}%.`,
    ],
    portfolio: {} as PortfolioDecisionResult["portfolio"],
    valuation: {} as PortfolioDecisionResult["valuation"],
    variant: {} as PortfolioDecisionResult["variant"],
    researchCell: {} as PortfolioDecisionResult["researchCell"],
  };
};

const loadPriceHistory = (params: {
  tickers: string[];
  dbPath?: string;
}): Map<string, PricePoint[]> => {
  const out = new Map<string, PricePoint[]>();
  if (!params.tickers.length) return out;
  const db = openResearchDb(params.dbPath);
  const placeholders = params.tickers.map(() => "?").join(",");
  const rows = db
    .prepare(
      `SELECT UPPER(i.ticker) AS ticker, p.date, p.close, p.volume
       FROM prices p
       JOIN instruments i ON i.id = p.instrument_id
       WHERE UPPER(i.ticker) IN (${placeholders})
         AND p.close IS NOT NULL
       ORDER BY p.date ASC`,
    )
    .all(...params.tickers) as Array<{
    ticker: string;
    date: string;
    close: number;
    volume?: number | null;
  }>;
  for (const ticker of params.tickers) {
    out.set(ticker, []);
  }
  for (const row of rows) {
    if (!Number.isFinite(row.close)) continue;
    const ticker = normalizeTicker(row.ticker);
    const list = out.get(ticker) ?? [];
    list.push({
      date: row.date,
      close: row.close,
      volume:
        typeof row.volume === "number" && Number.isFinite(row.volume) ? row.volume : undefined,
    });
    out.set(ticker, list);
  }
  return out;
};

const loadSectors = (tickers: string[], dbPath?: string): Record<string, string> => {
  if (!tickers.length) return {};
  const db = openResearchDb(dbPath);
  const placeholders = tickers.map(() => "?").join(",");
  const rows = db
    .prepare(
      `SELECT UPPER(ticker) AS ticker, COALESCE(NULLIF(TRIM(sector), ''), 'Unknown') AS sector
       FROM instruments
       WHERE UPPER(ticker) IN (${placeholders})`,
    )
    .all(...tickers) as Array<{ ticker: string; sector: string }>;
  const out: Record<string, string> = {};
  for (const ticker of tickers) out[ticker] = "Unknown";
  for (const row of rows) out[normalizeTicker(row.ticker)] = row.sector;
  return out;
};

const commonDates = (tickers: string[], pricesByTicker: Map<string, PricePoint[]>): string[] => {
  if (!tickers.length) return [];
  const first = pricesByTicker.get(tickers[0]!) ?? [];
  if (!first.length) return [];
  const sets = tickers.slice(1).map((ticker) => {
    const dates = pricesByTicker.get(ticker)?.map((row) => row.date) ?? [];
    return new Set(dates);
  });
  return first
    .map((row) => row.date)
    .filter((date) => sets.every((setValue) => setValue.has(date)));
};

const correlation = (left: number[], right: number[]): number | undefined => {
  if (left.length < 2 || right.length < 2 || left.length !== right.length) return undefined;
  const leftVol = stddev(left);
  const rightVol = stddev(right);
  if (leftVol <= 1e-9 || rightVol <= 1e-9) return undefined;
  return clamp(covariance(left, right) / (leftVol * rightVol), -1, 1);
};

const summarizeReplay = (windows: PortfolioReplayWindow[]): PortfolioReplaySummary => {
  if (!windows.length) {
    return {
      sampleCount: 0,
      meanExpectedNetPnlPct: 0,
      meanRealizedNetPnlPct: 0,
      maePct: 0,
      rmsePct: 0,
      directionalAccuracy: 0,
      winRate: 0,
      expectedRealizedCorrelation: 0,
      score: 0,
      passed: false,
    };
  }
  const expected = windows.map((window) => window.expectedNetPnlPct);
  const realized = windows.map((window) => window.realizedNetPnlPct);
  const errors = windows.map((window) =>
    Math.abs(window.expectedNetPnlPct - window.realizedNetPnlPct),
  );
  const squared = windows.map(
    (window) => (window.expectedNetPnlPct - window.realizedNetPnlPct) ** 2,
  );
  const maePct = mean(errors);
  const rmsePct = Math.sqrt(mean(squared));
  const directionalAccuracy = mean(windows.map((window) => (window.directionMatched ? 1 : 0)));
  const winRate = mean(windows.map((window) => (window.realizedNetPnlPct > 0 ? 1 : 0)));
  const corr = correlation(expected, realized) ?? 0;
  const score = clamp01(
    0.45 * directionalAccuracy + 0.35 * clamp01(1 - maePct / 3) + 0.2 * clamp01((corr + 1) / 2),
  );
  const passed = windows.length >= 8 && score >= 0.56;
  return {
    sampleCount: windows.length,
    meanExpectedNetPnlPct: mean(expected),
    meanRealizedNetPnlPct: mean(realized),
    maePct,
    rmsePct,
    directionalAccuracy,
    winRate,
    expectedRealizedCorrelation: corr,
    score,
    passed,
  };
};

const persistReplayEval = (params: {
  summary: PortfolioReplaySummary;
  windows: PortfolioReplayWindow[];
  tickers: string[];
  rebalanceEveryDays: number;
  horizonDays: number;
  dbPath?: string;
}) => {
  const checks = [
    {
      name: "sample_count",
      passed: params.summary.sampleCount >= 8,
      detail: `sample_count=${params.summary.sampleCount} (threshold>=8)`,
    },
    {
      name: "directional_accuracy",
      passed: params.summary.directionalAccuracy >= 0.52,
      detail: `directional_accuracy=${params.summary.directionalAccuracy.toFixed(3)} (threshold>=0.520)`,
    },
    {
      name: "mae_pct",
      passed: params.summary.maePct <= 2.5,
      detail: `mae_pct=${params.summary.maePct.toFixed(3)} (threshold<=2.500)`,
    },
    {
      name: "mean_realized_net_pnl_pct",
      passed: params.summary.meanRealizedNetPnlPct >= -0.2,
      detail: `mean_realized_net_pnl_pct=${params.summary.meanRealizedNetPnlPct.toFixed(3)} (threshold>=-0.200)`,
    },
  ];
  const passed = checks.filter((check) => check.passed).length;
  const total = checks.length;
  const db = openResearchDb(params.dbPath);
  db.prepare(
    `INSERT INTO eval_runs (run_type, score, passed, total, details, created_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
  ).run(
    "portfolio_replay",
    params.summary.score,
    passed,
    total,
    JSON.stringify({
      tickers: params.tickers,
      rebalance_every_days: params.rebalanceEveryDays,
      horizon_days: params.horizonDays,
      summary: params.summary,
      checks,
      windows: params.windows.slice(-40),
    }),
    Date.now(),
  );
  return checks;
};

export const computePortfolioReplay = (params: {
  tickers: string[];
  question?: string;
  dbPath?: string;
  startDate?: string;
  endDate?: string;
  rebalanceEveryDays?: number;
  horizonDays?: number;
  lookbackSignalDays?: number;
  lookbackCorrelationDays?: number;
  decisionConstraints?: Partial<PortfolioDecisionConstraints>;
  constraints?: Partial<PortfolioOptimizerConstraints>;
}) => {
  const tickers = Array.from(new Set(params.tickers.map(normalizeTicker).filter(Boolean)));
  if (!tickers.length) throw new Error("At least one ticker is required.");

  const question =
    params.question?.trim() || "Replay portfolio allocation policy on realized forward returns.";
  const rebalanceEveryDays = Math.max(5, Math.round(params.rebalanceEveryDays ?? 21));
  const horizonDays = Math.max(5, Math.round(params.horizonDays ?? 21));
  const lookbackSignalDays = Math.max(40, Math.round(params.lookbackSignalDays ?? 84));
  const lookbackCorrelationDays = Math.max(40, Math.round(params.lookbackCorrelationDays ?? 126));
  const startDate = parseDateLike(params.startDate);
  const endDate = parseDateLike(params.endDate);

  const pricesByTicker = loadPriceHistory({
    tickers,
    dbPath: params.dbPath,
  });
  const dates = commonDates(tickers, pricesByTicker);
  if (!dates.length) throw new Error("No overlapping price history for selected tickers.");

  const firstEligibleIndex = Math.max(lookbackSignalDays + 2, lookbackCorrelationDays + 2);
  let startIndex = firstEligibleIndex;
  if (startDate) {
    const idx = dates.findIndex((date) => date >= startDate);
    if (idx >= 0) startIndex = Math.max(startIndex, idx);
  }
  let lastEligibleIndex = dates.length - 2;
  if (endDate) {
    const idx = dates.findLastIndex((date) => date <= endDate);
    if (idx >= 0) lastEligibleIndex = Math.min(lastEligibleIndex, idx);
  }
  if (lastEligibleIndex - startIndex < horizonDays) {
    throw new Error("Insufficient history for replay window selection.");
  }

  const sectors = loadSectors(tickers, params.dbPath);
  const windows: PortfolioReplayWindow[] = [];
  let currentWeightsSignedPct: Record<string, number> = Object.fromEntries(
    tickers.map((ticker) => [ticker, 0]),
  );
  let latestOptimization: PortfolioOptimizationResult | undefined;

  for (
    let rebalanceIndex = startIndex;
    rebalanceIndex <= lastEligibleIndex - 1;
    rebalanceIndex += rebalanceEveryDays
  ) {
    const rebalanceDate = dates[rebalanceIndex]!;
    const nextRebalanceIndex = Math.min(rebalanceIndex + rebalanceEveryDays, lastEligibleIndex);
    const exitIndex = Math.min(rebalanceIndex + horizonDays, nextRebalanceIndex, dates.length - 1);
    if (exitIndex <= rebalanceIndex) continue;
    const exitDate = dates[exitIndex]!;

    const decisions: PortfolioDecisionResult[] = [];
    for (const ticker of tickers) {
      const prices = pricesByTicker.get(ticker) ?? [];
      const asOfIndex = findIndexByDate(prices, rebalanceDate);
      if (asOfIndex < 0) continue;
      const decision = buildSyntheticDecision(
        {
          ticker,
          prices,
          asOfIndex,
          lookbackShortDays: Math.max(10, Math.round(lookbackSignalDays / 4)),
          lookbackLongDays: lookbackSignalDays,
          maxSingleNameWeightPct: 8,
        },
        params.decisionConstraints,
      );
      if (decision) decisions.push(decision);
    }
    if (decisions.length < 2) continue;

    const corrModel = correlationEdgesForDate({
      tickers,
      pricesByTicker,
      asOfDate: rebalanceDate,
      lookbackDays: lookbackCorrelationDays,
      minOverlapDays: 30,
    });
    const liquidityByTicker = liquiditySnapshotForDate({
      tickers,
      pricesByTicker,
      asOfDate: rebalanceDate,
      lookbackDays:
        typeof params.constraints?.liquidityLookbackDays === "number"
          ? params.constraints.liquidityLookbackDays
          : 20,
    });

    const optimization = composePortfolioOptimization({
      tickers,
      question,
      decisions,
      constraints: params.constraints,
      sectors,
      pairwiseCorrelation: corrModel.edges,
      volatilityAnnualizedPctByTicker: corrModel.volatilityAnnualizedPctByTicker,
      currentWeightsSignedPct,
      liquidityByTicker,
    });
    latestOptimization = optimization;

    let realizedPnlPct = 0;
    for (const position of optimization.positions) {
      const prices = pricesByTicker.get(position.ticker) ?? [];
      const entryIndex = findIndexByDate(prices, rebalanceDate);
      const finalIndex = findIndexByDate(prices, exitDate);
      if (entryIndex < 0 || finalIndex < 0) continue;
      const entryPrice = prices[entryIndex]?.close;
      const exitPrice = prices[finalIndex]?.close;
      if (
        typeof entryPrice !== "number" ||
        typeof exitPrice !== "number" ||
        Math.abs(entryPrice) <= 1e-9
      ) {
        continue;
      }
      const assetReturnPct = (exitPrice / entryPrice - 1) * 100;
      realizedPnlPct += (position.signedWeightPct / 100) * assetReturnPct;
    }
    const realizedNetPnlPct = realizedPnlPct - optimization.metrics.transactionCostPct;
    const expectedNet = optimization.metrics.expectedNetPnlPct;
    const directionMatched =
      Math.sign(Math.abs(expectedNet) <= 1e-6 ? 0 : expectedNet) ===
      Math.sign(Math.abs(realizedNetPnlPct) <= 1e-6 ? 0 : realizedNetPnlPct);

    windows.push({
      rebalanceDate,
      exitDate,
      expectedPnlPct: optimization.metrics.expectedPnlPct,
      expectedNetPnlPct: expectedNet,
      realizedPnlPct,
      realizedNetPnlPct,
      transactionCostPct: optimization.metrics.transactionCostPct,
      turnoverPct: optimization.metrics.turnoverPct,
      positions: optimization.positions.length,
      grossExposurePct: optimization.metrics.grossExposurePct,
      netExposurePct: optimization.metrics.netExposurePct,
      weightedCorrelation: optimization.metrics.weightedCorrelation,
      directionMatched,
    });

    currentWeightsSignedPct = Object.fromEntries(tickers.map((ticker) => [ticker, 0]));
    optimization.positions.forEach((position) => {
      currentWeightsSignedPct[position.ticker] = position.signedWeightPct;
    });
  }

  const summary = summarizeReplay(windows);
  const checks = persistReplayEval({
    summary,
    windows,
    tickers,
    rebalanceEveryDays,
    horizonDays,
    dbPath: params.dbPath,
  });
  try {
    appendProvenanceEvent({
      eventType: "portfolio_replay_eval",
      entityType: "portfolio_replay",
      entityId: `${tickers.join(",")}:${rebalanceEveryDays}:${horizonDays}`,
      payload: {
        tickers,
        summary,
        window_count: windows.length,
      },
      metadata: {
        question,
        rebalance_every_days: rebalanceEveryDays,
        horizon_days: horizonDays,
      },
      dbPath: params.dbPath,
    });
  } catch {
    // Keep replay path resilient to provenance write failures.
  }

  const result: PortfolioReplayResult = {
    generatedAt: new Date().toISOString(),
    tickers,
    question,
    rebalanceEveryDays,
    horizonDays,
    lookbackSignalDays,
    lookbackCorrelationDays,
    windows,
    summary,
    latestOptimization,
    evaluationChecks: checks,
  };
  return result;
};
