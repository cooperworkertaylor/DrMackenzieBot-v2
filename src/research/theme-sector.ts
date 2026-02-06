import { getCatalystSummary } from "./catalyst.js";
import { openResearchDb } from "./db.js";
import { computePortfolioPlan } from "./portfolio.js";
import { appendProvenanceEvent } from "./provenance.js";
import { getThemeConstituents, listThemeDefinitions } from "./theme-ontology.js";
import { computeValuation } from "./valuation.js";
import { computeVariantPerception } from "./variant.js";

type PriceRow = {
  date: string;
  close: number;
  volume?: number;
};

type CatalystEventInternal = {
  ticker: string;
  dateWindowStart: string;
  dateWindowEnd: string;
  probability: number;
  impactBps: number;
  confidence: number;
  direction: "up" | "down" | "both";
};

type CrossSectionConstituentInternal = {
  ticker: string;
  companyName?: string;
  sector: string;
  industry: string;
  currentPrice?: number;
  return20dPct?: number;
  return63dPct?: number;
  return126dPct?: number;
  drawdown63dPct?: number;
  volatility63dPct?: number;
  avgDailyDollarVolumeUsd?: number;
  avgDailyShareVolume?: number;
  variantStance: string;
  variantGapScore: number;
  variantConfidence: number;
  valuationConfidence: number;
  expectedUpsidePct?: number;
  expectedUpsideWithCatalystsPct?: number;
  impliedValuationStance: string;
  portfolioStance: string;
  portfolioConfidence: number;
  catalystOpenCount: number;
  catalystExpectedImpactPct: number;
  compositeScore: number;
  conviction: "long-bias" | "short-bias" | "neutral";
  notes: string[];
  returnsByDate63d: Map<string, number>;
  returnsByDateLookback: Map<string, number>;
};

export type CrossSectionConstituentSnapshot = Omit<
  CrossSectionConstituentInternal,
  "returnsByDate63d" | "returnsByDateLookback"
>;

export type CrossSectionMetrics = {
  constituentCount: number;
  breadthPositive20dPct: number;
  breadthPositive63dPct: number;
  medianReturn20dPct?: number;
  medianReturn63dPct?: number;
  medianExpectedUpsidePct?: number;
  medianVariantGapScore?: number;
  avgVariantConfidence: number;
  avgValuationConfidence: number;
  avgPortfolioConfidence: number;
  dispersion63dPct?: number;
  averagePairwiseCorrelation63d?: number;
  avgVolatility63dPct?: number;
  totalOpenCatalysts: number;
  avgCatalystExpectedImpactPct: number;
  concentrationHhi: number;
  longBiasPct: number;
  shortBiasPct: number;
  neutralPct: number;
  evidenceCoverageScore: number;
  institutionalReadinessScore: number;
  regime: "expansion" | "contraction" | "rotation" | "range-bound";
};

export type SectorResearchResult = {
  generatedAt: string;
  sector: string;
  tickers: string[];
  lookbackDays: number;
  metrics: CrossSectionMetrics;
  factorDecomposition: FactorDecomposition;
  catalystCalendar: CatalystCalendarMetrics;
  factorAttribution?: CrossSectionFactorAttribution;
  leaders: CrossSectionConstituentSnapshot[];
  laggards: CrossSectionConstituentSnapshot[];
  constituents: CrossSectionConstituentSnapshot[];
  riskFlags: string[];
  insightSummary: string[];
};

export type ThemeResearchResult = {
  generatedAt: string;
  theme: string;
  themeVersion?: number;
  usedThemeRegistry: boolean;
  membershipMinScore?: number;
  benchmarkRelative?: ThemeBenchmarkRelativeMetrics;
  factorAttribution?: CrossSectionFactorAttribution;
  tickers: string[];
  lookbackDays: number;
  metrics: CrossSectionMetrics;
  factorDecomposition: FactorDecomposition;
  catalystCalendar: CatalystCalendarMetrics;
  sectorExposure: Array<{
    sector: string;
    count: number;
    sharePct: number;
    avgCompositeScore: number;
  }>;
  leaders: CrossSectionConstituentSnapshot[];
  laggards: CrossSectionConstituentSnapshot[];
  constituents: CrossSectionConstituentSnapshot[];
  riskFlags: string[];
  insightSummary: string[];
};

export type ThemeBenchmarkRelativeMetrics = {
  benchmarkTicker: string;
  sampleSize: number;
  lookbackDays: number;
  themeReturnPct: number;
  benchmarkReturnPct: number;
  relativeReturnPct: number;
  activeHitRatePct: number;
  annualizedActiveReturnPct?: number;
  annualizedAlphaPct?: number;
  beta?: number;
  correlation?: number;
  trackingErrorPct?: number;
  informationRatio?: number;
  upsideCapturePct?: number;
  downsideCapturePct?: number;
};

export type CrossSectionFactor = "momentum" | "value" | "quality" | "size";

export type FactorExposure = {
  factor: CrossSectionFactor;
  exposureZ: number;
  weightedRaw: number;
  dispersionZ: number;
  topPositiveTicker?: string;
  topNegativeTicker?: string;
};

export type FactorDecomposition = {
  model: string;
  dominantFactor: CrossSectionFactor;
  concentration: number;
  exposures: FactorExposure[];
};

export type CatalystCalendarTickerLoad = {
  ticker: string;
  openEvents: number;
  scheduledEvents: number;
  nearTermEvents: number;
  weightedExpectedImpactBps: number;
};

export type CatalystCalendarWindow = {
  date: string;
  eventCount: number;
  weightedExpectedImpactBps: number;
};

export type CatalystCalendarMetrics = {
  asOfDate: string;
  horizonDays: number;
  totalOpenEvents: number;
  scheduledEvents: number;
  unscheduledEvents: number;
  eventsInHorizon: number;
  nearTermEvents: number;
  highImpactEvents: number;
  avgConfidence: number;
  weightedExpectedImpactBps: number;
  weightedDownsideSharePct: number;
  crowdingScore: number;
  maxSameDayEvents: number;
  topEventWindows: CatalystCalendarWindow[];
  tickerLoad: CatalystCalendarTickerLoad[];
};

export type CrossSectionFactorAttribution = {
  benchmarkTicker: string;
  sampleSize: number;
  annualizedActiveReturnPct: number;
  annualizedAlphaPct: number;
  annualizedResidualPct: number;
  rSquared: number;
  factorBetas: {
    benchmark: number;
    momentum: number;
    value: number;
    quality: number;
    size: number;
  };
  annualizedContributionPct: {
    benchmark: number;
    momentum: number;
    value: number;
    quality: number;
    size: number;
  };
  dominantContributionFactor: "benchmark" | CrossSectionFactor;
};

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const clamp01 = (value: number): number => clamp(value, 0, 1);

const mean = (values: number[]): number =>
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const median = (values: number[]): number | undefined => {
  if (!values.length) return undefined;
  const sorted = [...values].sort((left, right) => left - right);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1]! + sorted[mid]!) / 2;
};

const stddev = (values: number[]): number | undefined => {
  if (values.length < 2) return undefined;
  const avg = mean(values);
  const variance = values.reduce((sum, value) => sum + (value - avg) ** 2, 0) / (values.length - 1);
  return Math.sqrt(Math.max(0, variance));
};

const compoundedReturnPct = (returns: number[]): number | undefined => {
  if (!returns.length) return undefined;
  let compounded = 1;
  for (const ret of returns) {
    if (!Number.isFinite(ret)) continue;
    compounded *= 1 + ret;
  }
  if (!Number.isFinite(compounded) || compounded <= 0) return undefined;
  return (compounded - 1) * 100;
};

const covariance = (left: number[], right: number[]): number | undefined => {
  if (left.length < 2 || right.length < 2 || left.length !== right.length) return undefined;
  const leftMean = mean(left);
  const rightMean = mean(right);
  let sum = 0;
  for (let index = 0; index < left.length; index += 1) {
    sum += (left[index] - leftMean) * (right[index] - rightMean);
  }
  return sum / (left.length - 1);
};

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

const toFinite = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isFinite(value) ? value : undefined;

const toIsoDate = (value: Date): string => value.toISOString().slice(0, 10);

const parseIsoDate = (value?: string): Date | undefined => {
  if (!value || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return undefined;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) ? parsed : undefined;
};

const daysBetween = (left: Date, right: Date): number =>
  Math.floor((right.getTime() - left.getTime()) / (24 * 60 * 60 * 1000));

const signedImpactBps = (event: CatalystEventInternal): number => {
  const base = event.probability * event.impactBps * event.confidence;
  if (event.direction === "up") return Math.abs(base);
  if (event.direction === "down") return -Math.abs(base);
  return base;
};

const inferRegime = (params: {
  breadthPositive63dPct: number;
  medianReturn63dPct?: number;
  dispersion63dPct?: number;
  averagePairwiseCorrelation63d?: number;
}): CrossSectionMetrics["regime"] => {
  const median63 = params.medianReturn63dPct ?? 0;
  const dispersion = params.dispersion63dPct ?? 0;
  const corr = params.averagePairwiseCorrelation63d ?? 0;
  if (params.breadthPositive63dPct >= 0.66 && median63 >= 4) return "expansion";
  if (params.breadthPositive63dPct <= 0.34 && median63 <= -4) return "contraction";
  if (dispersion >= 12 || corr <= 0.12) return "rotation";
  return "range-bound";
};

const loadUniverse = (params: {
  sector?: string;
  tickers?: string[];
  dbPath?: string;
}): Array<{ ticker: string; name?: string; sector: string; industry: string }> => {
  const db = openResearchDb(params.dbPath);
  if (params.tickers?.length) {
    const normalized = Array.from(new Set(params.tickers.map(normalizeTicker).filter(Boolean)));
    if (!normalized.length) return [];
    const placeholders = normalized.map(() => "?").join(",");
    const rows = db
      .prepare(
        `SELECT
           UPPER(i.ticker) AS ticker,
           NULLIF(TRIM(i.name), '') AS name,
           COALESCE(NULLIF(TRIM(i.sector), ''), 'Unknown') AS sector,
           COALESCE(NULLIF(TRIM(i.industry), ''), 'Unknown') AS industry
         FROM instruments i
         WHERE UPPER(i.ticker) IN (${placeholders})
         ORDER BY i.ticker ASC`,
      )
      .all(...normalized) as Array<{
      ticker: string;
      name?: string | null;
      sector: string;
      industry: string;
    }>;
    const existing = new Set(rows.map((row) => normalizeTicker(row.ticker)));
    const missing = normalized.filter((ticker) => !existing.has(ticker));
    const fallback = missing.map((ticker) => ({
      ticker,
      name: undefined,
      sector: "Unknown",
      industry: "Unknown",
    }));
    return [...rows, ...fallback].map((row) => ({
      ticker: normalizeTicker(row.ticker),
      name: row.name ?? undefined,
      sector: row.sector,
      industry: row.industry,
    }));
  }
  const sector = params.sector?.trim();
  if (!sector) throw new Error("Provide either sector or tickers.");
  const rows = db
    .prepare(
      `SELECT
         UPPER(i.ticker) AS ticker,
         NULLIF(TRIM(i.name), '') AS name,
         COALESCE(NULLIF(TRIM(i.sector), ''), 'Unknown') AS sector,
         COALESCE(NULLIF(TRIM(i.industry), ''), 'Unknown') AS industry
       FROM instruments i
       WHERE LOWER(TRIM(i.sector)) = LOWER(TRIM(?))
       ORDER BY i.ticker ASC`,
    )
    .all(sector) as Array<{
    ticker: string;
    name?: string | null;
    sector: string;
    industry: string;
  }>;
  return rows.map((row) => ({
    ticker: normalizeTicker(row.ticker),
    name: row.name ?? undefined,
    sector: row.sector,
    industry: row.industry,
  }));
};

const loadPrices = (params: {
  ticker: string;
  lookbackDays: number;
  dbPath?: string;
}): PriceRow[] => {
  const db = openResearchDb(params.dbPath);
  const cutoff = new Date(Date.now() - params.lookbackDays * 24 * 60 * 60 * 1000)
    .toISOString()
    .slice(0, 10);
  const rows = db
    .prepare(
      `SELECT p.date, p.close, p.volume
       FROM prices p
       JOIN instruments i ON i.id = p.instrument_id
       WHERE UPPER(i.ticker) = ?
         AND p.date >= ?
         AND p.close IS NOT NULL
       ORDER BY p.date ASC`,
    )
    .all(params.ticker, cutoff) as Array<{
    date: string;
    close: number;
    volume?: number | null;
  }>;
  return rows.map((row) => ({
    date: row.date,
    close: row.close,
    volume: typeof row.volume === "number" ? row.volume : undefined,
  }));
};

const nthReturnPct = (prices: PriceRow[], periods: number): number | undefined => {
  if (prices.length <= periods) return undefined;
  const last = prices[prices.length - 1]?.close;
  const prior = prices[prices.length - 1 - periods]?.close;
  if (
    typeof last !== "number" ||
    typeof prior !== "number" ||
    !Number.isFinite(last) ||
    !Number.isFinite(prior) ||
    Math.abs(prior) <= 1e-9
  ) {
    return undefined;
  }
  return (last / prior - 1) * 100;
};

const derivePriceSignals = (
  prices: PriceRow[],
): {
  currentPrice?: number;
  return20dPct?: number;
  return63dPct?: number;
  return126dPct?: number;
  drawdown63dPct?: number;
  volatility63dPct?: number;
  avgDailyDollarVolumeUsd?: number;
  avgDailyShareVolume?: number;
  returnsByDate63d: Map<string, number>;
  returnsByDateLookback: Map<string, number>;
} => {
  const currentPrice = prices[prices.length - 1]?.close;
  const return20dPct = nthReturnPct(prices, 20);
  const return63dPct = nthReturnPct(prices, 63);
  const return126dPct = nthReturnPct(prices, 126);
  const window63 = prices.slice(Math.max(0, prices.length - 63));
  const maxClose63 = window63.length ? Math.max(...window63.map((row) => row.close)) : undefined;
  const drawdown63dPct =
    typeof maxClose63 === "number" &&
    typeof currentPrice === "number" &&
    Math.abs(maxClose63) > 1e-9
      ? (currentPrice / maxClose63 - 1) * 100
      : undefined;
  const returnsByDate63d = new Map<string, number>();
  const returnsByDateLookback = new Map<string, number>();
  const returnValues63d: number[] = [];
  const startIndex63 = Math.max(1, prices.length - 63);
  for (let index = 1; index < prices.length; index += 1) {
    const current = prices[index];
    const prior = prices[index - 1];
    if (!current || !prior || Math.abs(prior.close) <= 1e-9) continue;
    const ret = current.close / prior.close - 1;
    if (!Number.isFinite(ret)) continue;
    returnsByDateLookback.set(current.date, ret);
    if (index >= startIndex63) {
      returnsByDate63d.set(current.date, ret);
      returnValues63d.push(ret);
    }
  }
  const volatility63dPct =
    returnValues63d.length >= 15
      ? (stddev(returnValues63d) ?? 0) * Math.sqrt(252) * 100
      : undefined;

  const dollarVolumes: number[] = [];
  const shareVolumes: number[] = [];
  for (const row of window63) {
    if (typeof row.volume !== "number" || !Number.isFinite(row.volume) || row.volume <= 0) continue;
    shareVolumes.push(row.volume);
    dollarVolumes.push(row.volume * row.close);
  }
  return {
    currentPrice: toFinite(currentPrice),
    return20dPct,
    return63dPct,
    return126dPct,
    drawdown63dPct,
    volatility63dPct,
    avgDailyDollarVolumeUsd: dollarVolumes.length ? mean(dollarVolumes) : undefined,
    avgDailyShareVolume: shareVolumes.length ? mean(shareVolumes) : undefined,
    returnsByDate63d,
    returnsByDateLookback,
  };
};

const averagePairwiseCorrelation63d = (
  rows: CrossSectionConstituentInternal[],
): number | undefined => {
  if (rows.length < 2) return undefined;
  const correlations: number[] = [];
  for (let leftIndex = 0; leftIndex < rows.length; leftIndex += 1) {
    for (let rightIndex = leftIndex + 1; rightIndex < rows.length; rightIndex += 1) {
      const left = rows[leftIndex]!;
      const right = rows[rightIndex]!;
      const rightByDate = right.returnsByDate63d;
      const leftSeries: number[] = [];
      const rightSeries: number[] = [];
      for (const [date, leftRet] of left.returnsByDate63d) {
        const rightRet = rightByDate.get(date);
        if (typeof rightRet !== "number") continue;
        leftSeries.push(leftRet);
        rightSeries.push(rightRet);
      }
      const leftVol = stddev(leftSeries);
      const rightVol = stddev(rightSeries);
      const cov = covariance(leftSeries, rightSeries);
      if (
        typeof leftVol !== "number" ||
        typeof rightVol !== "number" ||
        typeof cov !== "number" ||
        leftVol <= 1e-9 ||
        rightVol <= 1e-9
      ) {
        continue;
      }
      correlations.push(clamp(cov / (leftVol * rightVol), -1, 1));
    }
  }
  return correlations.length ? mean(correlations) : undefined;
};

const computeCompositeScore = (params: {
  return63dPct?: number;
  expectedUpsidePct?: number;
  variantGapScore: number;
  variantConfidence: number;
  valuationConfidence: number;
  portfolioConfidence: number;
  volatility63dPct?: number;
  drawdown63dPct?: number;
  catalystExpectedImpactPct: number;
}): number => {
  const momentumNorm = clamp01(((params.return63dPct ?? 0) + 18) / 36);
  const upsideNorm = clamp01(((params.expectedUpsidePct ?? 0) + 20) / 40);
  const confidenceNorm = clamp01(
    (params.variantConfidence + params.valuationConfidence + params.portfolioConfidence) / 3,
  );
  const volatilityPenalty = clamp01((params.volatility63dPct ?? 20) / 75);
  const drawdownPenalty = clamp01(Math.abs(Math.min(0, params.drawdown63dPct ?? 0)) / 35);
  const catalystNorm = clamp01((params.catalystExpectedImpactPct + 12) / 24);
  return clamp01(
    0.25 * momentumNorm +
      0.22 * upsideNorm +
      0.21 * params.variantGapScore +
      0.2 * confidenceNorm +
      0.08 * catalystNorm -
      0.08 * volatilityPenalty -
      0.04 * drawdownPenalty,
  );
};

const classifyConviction = (score: number): CrossSectionConstituentSnapshot["conviction"] => {
  if (score >= 0.62) return "long-bias";
  if (score <= 0.38) return "short-bias";
  return "neutral";
};

const buildConstituentSnapshot = (params: {
  ticker: string;
  companyName?: string;
  sector: string;
  industry: string;
  lookbackDays: number;
  dbPath?: string;
}): CrossSectionConstituentInternal => {
  const prices = loadPrices({
    ticker: params.ticker,
    lookbackDays: params.lookbackDays,
    dbPath: params.dbPath,
  });
  const priceSignals = derivePriceSignals(prices);
  const variant = computeVariantPerception({ ticker: params.ticker, dbPath: params.dbPath });
  const valuation = computeValuation({ ticker: params.ticker, dbPath: params.dbPath });
  const portfolio = computePortfolioPlan({ ticker: params.ticker, dbPath: params.dbPath });
  const catalyst = getCatalystSummary({ ticker: params.ticker, dbPath: params.dbPath });
  const expectedUpsideWithCatalystsPct =
    typeof valuation.expectedUpsideWithCatalystsPct === "number"
      ? valuation.expectedUpsideWithCatalystsPct * 100
      : undefined;
  const expectedUpsidePct =
    typeof valuation.expectedUpsidePct === "number" ? valuation.expectedUpsidePct * 100 : undefined;
  const compositeScore = computeCompositeScore({
    return63dPct: priceSignals.return63dPct,
    expectedUpsidePct: expectedUpsideWithCatalystsPct ?? expectedUpsidePct,
    variantGapScore: variant.variantGapScore,
    variantConfidence: variant.confidence,
    valuationConfidence: valuation.confidence,
    portfolioConfidence: portfolio.confidence,
    volatility63dPct: priceSignals.volatility63dPct,
    drawdown63dPct: priceSignals.drawdown63dPct,
    catalystExpectedImpactPct: catalyst.expectedImpactPct * 100,
  });
  const conviction = classifyConviction(compositeScore);
  const notes: string[] = [];
  if (variant.notes.length) notes.push(...variant.notes.slice(0, 2));
  if (valuation.notes.length) notes.push(...valuation.notes.slice(0, 2));
  return {
    ticker: params.ticker,
    companyName: params.companyName,
    sector: params.sector,
    industry: params.industry,
    currentPrice: priceSignals.currentPrice,
    return20dPct: priceSignals.return20dPct,
    return63dPct: priceSignals.return63dPct,
    return126dPct: priceSignals.return126dPct,
    drawdown63dPct: priceSignals.drawdown63dPct,
    volatility63dPct: priceSignals.volatility63dPct,
    avgDailyDollarVolumeUsd: priceSignals.avgDailyDollarVolumeUsd,
    avgDailyShareVolume: priceSignals.avgDailyShareVolume,
    variantStance: variant.stance,
    variantGapScore: variant.variantGapScore,
    variantConfidence: variant.confidence,
    valuationConfidence: valuation.confidence,
    expectedUpsidePct,
    expectedUpsideWithCatalystsPct,
    impliedValuationStance: valuation.impliedExpectations?.stance ?? "insufficient-evidence",
    portfolioStance: portfolio.stance,
    portfolioConfidence: portfolio.confidence,
    catalystOpenCount: catalyst.openCount,
    catalystExpectedImpactPct: catalyst.expectedImpactPct * 100,
    compositeScore,
    conviction,
    notes,
    returnsByDate63d: priceSignals.returnsByDate63d,
    returnsByDateLookback: priceSignals.returnsByDateLookback,
  };
};

const toPublicSnapshot = (
  row: CrossSectionConstituentInternal,
): CrossSectionConstituentSnapshot => ({
  ticker: row.ticker,
  companyName: row.companyName,
  sector: row.sector,
  industry: row.industry,
  currentPrice: row.currentPrice,
  return20dPct: row.return20dPct,
  return63dPct: row.return63dPct,
  return126dPct: row.return126dPct,
  drawdown63dPct: row.drawdown63dPct,
  volatility63dPct: row.volatility63dPct,
  avgDailyDollarVolumeUsd: row.avgDailyDollarVolumeUsd,
  avgDailyShareVolume: row.avgDailyShareVolume,
  variantStance: row.variantStance,
  variantGapScore: row.variantGapScore,
  variantConfidence: row.variantConfidence,
  valuationConfidence: row.valuationConfidence,
  expectedUpsidePct: row.expectedUpsidePct,
  expectedUpsideWithCatalystsPct: row.expectedUpsideWithCatalystsPct,
  impliedValuationStance: row.impliedValuationStance,
  portfolioStance: row.portfolioStance,
  portfolioConfidence: row.portfolioConfidence,
  catalystOpenCount: row.catalystOpenCount,
  catalystExpectedImpactPct: row.catalystExpectedImpactPct,
  compositeScore: row.compositeScore,
  conviction: row.conviction,
  notes: row.notes,
});

const computeCrossSectionMetrics = (
  rows: CrossSectionConstituentInternal[],
): CrossSectionMetrics => {
  const returns20 = rows
    .map((row) => row.return20dPct)
    .filter((value): value is number => typeof value === "number");
  const returns63 = rows
    .map((row) => row.return63dPct)
    .filter((value): value is number => typeof value === "number");
  const expectedUpside = rows
    .map((row) => row.expectedUpsideWithCatalystsPct ?? row.expectedUpsidePct)
    .filter((value): value is number => typeof value === "number");
  const variantGap = rows.map((row) => row.variantGapScore);
  const volatility = rows
    .map((row) => row.volatility63dPct)
    .filter((value): value is number => typeof value === "number");
  const breadthPositive20dPct = rows.length
    ? rows.filter((row) => typeof row.return20dPct === "number" && row.return20dPct > 0).length /
      rows.length
    : 0;
  const breadthPositive63dPct = rows.length
    ? rows.filter((row) => typeof row.return63dPct === "number" && row.return63dPct > 0).length /
      rows.length
    : 0;
  const longBiasPct = rows.length
    ? rows.filter((row) => row.conviction === "long-bias").length / rows.length
    : 0;
  const shortBiasPct = rows.length
    ? rows.filter((row) => row.conviction === "short-bias").length / rows.length
    : 0;
  const neutralPct = rows.length
    ? rows.filter((row) => row.conviction === "neutral").length / rows.length
    : 0;
  const avgPairwiseCorrelation63d = averagePairwiseCorrelation63d(rows);
  const dispersion63dPct = stddev(returns63);
  const weights = rows.map((row) => Math.max(row.compositeScore, 0.001));
  const totalWeight = weights.reduce((sum, value) => sum + value, 0);
  const concentrationHhi =
    totalWeight > 1e-9
      ? rows.reduce((sum, row) => {
          const share = Math.max(row.compositeScore, 0.001) / totalWeight;
          return sum + share ** 2;
        }, 0)
      : 1;
  const evidenceCoverageScore = clamp01(
    0.4 * clamp01(rows.length / 12) +
      0.3 * mean(rows.map((row) => row.variantConfidence)) +
      0.3 * mean(rows.map((row) => row.valuationConfidence)),
  );
  const institutionalReadinessScore = clamp01(
    0.28 * evidenceCoverageScore +
      0.2 * (1 - clamp01((concentrationHhi - 0.2) / 0.8)) +
      0.2 *
        (avgPairwiseCorrelation63d !== undefined
          ? clamp01(1 - (avgPairwiseCorrelation63d + 1) / 2)
          : 0.5) +
      0.17 * breadthPositive63dPct +
      0.15 * (expectedUpside.length ? clamp01((mean(expectedUpside) + 15) / 30) : 0.5),
  );
  const regime = inferRegime({
    breadthPositive63dPct,
    medianReturn63dPct: median(returns63),
    dispersion63dPct,
    averagePairwiseCorrelation63d: avgPairwiseCorrelation63d,
  });
  return {
    constituentCount: rows.length,
    breadthPositive20dPct,
    breadthPositive63dPct,
    medianReturn20dPct: median(returns20),
    medianReturn63dPct: median(returns63),
    medianExpectedUpsidePct: median(expectedUpside),
    medianVariantGapScore: median(variantGap),
    avgVariantConfidence: mean(rows.map((row) => row.variantConfidence)),
    avgValuationConfidence: mean(rows.map((row) => row.valuationConfidence)),
    avgPortfolioConfidence: mean(rows.map((row) => row.portfolioConfidence)),
    dispersion63dPct,
    averagePairwiseCorrelation63d: avgPairwiseCorrelation63d,
    avgVolatility63dPct: volatility.length ? mean(volatility) : undefined,
    totalOpenCatalysts: rows.reduce((sum, row) => sum + row.catalystOpenCount, 0),
    avgCatalystExpectedImpactPct: mean(rows.map((row) => row.catalystExpectedImpactPct)),
    concentrationHhi,
    longBiasPct,
    shortBiasPct,
    neutralPct,
    evidenceCoverageScore,
    institutionalReadinessScore,
    regime,
  };
};

const factorRawValue = (
  row: CrossSectionConstituentInternal,
  factor: CrossSectionFactor,
): number => {
  if (factor === "momentum") return row.return126dPct ?? row.return63dPct ?? row.return20dPct ?? 0;
  if (factor === "value") return row.expectedUpsideWithCatalystsPct ?? row.expectedUpsidePct ?? 0;
  if (factor === "quality")
    return (
      (0.45 * row.variantConfidence +
        0.35 * row.valuationConfidence +
        0.2 * row.portfolioConfidence -
        0.5) *
      100
    );
  return -Math.log(Math.max(row.avgDailyDollarVolumeUsd ?? 5_000_000, 1));
};

const computeFactorDecomposition = (
  rows: CrossSectionConstituentInternal[],
): FactorDecomposition => {
  const factors: CrossSectionFactor[] = ["momentum", "value", "quality", "size"];
  const weights = rows.map((row) => Math.max(0.001, row.compositeScore));
  const totalWeight = weights.reduce((sum, value) => sum + value, 0);
  const exposures = factors.map((factor) => {
    const raw = rows.map((row) => factorRawValue(row, factor));
    const rawMean = mean(raw);
    const rawStd = stddev(raw) ?? 0;
    const z = raw.map((value) => (rawStd > 1e-9 ? (value - rawMean) / rawStd : 0));
    const exposureZ =
      totalWeight > 1e-9
        ? z.reduce((sum, value, index) => sum + value * (weights[index] ?? 0), 0) / totalWeight
        : 0;
    const weightedRaw =
      totalWeight > 1e-9
        ? raw.reduce((sum, value, index) => sum + value * (weights[index] ?? 0), 0) / totalWeight
        : rawMean;
    let topPositiveTicker: string | undefined;
    let topNegativeTicker: string | undefined;
    if (rows.length) {
      let maxIndex = 0;
      let minIndex = 0;
      for (let index = 1; index < z.length; index += 1) {
        if ((z[index] ?? 0) > (z[maxIndex] ?? 0)) maxIndex = index;
        if ((z[index] ?? 0) < (z[minIndex] ?? 0)) minIndex = index;
      }
      topPositiveTicker = rows[maxIndex]?.ticker;
      topNegativeTicker = rows[minIndex]?.ticker;
    }
    return {
      factor,
      exposureZ,
      weightedRaw,
      dispersionZ: stddev(z) ?? 0,
      topPositiveTicker,
      topNegativeTicker,
    };
  });
  const absolute = exposures.map((row) => Math.abs(row.exposureZ));
  const sumAbs = absolute.reduce((sum, value) => sum + value, 0);
  const concentration =
    sumAbs > 1e-9
      ? absolute.reduce((sum, value) => {
          const share = value / sumAbs;
          return sum + share ** 2;
        }, 0)
      : 0;
  const dominant = exposures
    .slice()
    .sort((left, right) => Math.abs(right.exposureZ) - Math.abs(left.exposureZ))[0]?.factor;
  return {
    model: "cross_sectional_style_proxy_v1",
    dominantFactor: dominant ?? "momentum",
    concentration,
    exposures,
  };
};

const buildThemeReturnByDate = (rows: CrossSectionConstituentInternal[]): Map<string, number> => {
  const bucket = new Map<string, number[]>();
  for (const row of rows) {
    for (const [date, value] of row.returnsByDateLookback) {
      const existing = bucket.get(date) ?? [];
      existing.push(value);
      bucket.set(date, existing);
    }
  }
  const out = new Map<string, number>();
  for (const [date, values] of bucket) {
    if (!values.length) continue;
    out.set(date, mean(values));
  }
  return out;
};

const factorExposureZByTicker = (
  rows: CrossSectionConstituentInternal[],
  factor: CrossSectionFactor,
): Map<string, number> => {
  const raw = rows.map((row) => factorRawValue(row, factor));
  const rawMean = mean(raw);
  const rawStd = stddev(raw) ?? 0;
  const out = new Map<string, number>();
  rows.forEach((row, index) => {
    const value = raw[index] ?? 0;
    const z = rawStd > 1e-9 ? (value - rawMean) / rawStd : 0;
    out.set(row.ticker, z);
  });
  return out;
};

const buildFactorProxyReturnByDate = (
  rows: CrossSectionConstituentInternal[],
  factor: CrossSectionFactor,
): Map<string, number> => {
  const exposureByTicker = factorExposureZByTicker(rows, factor);
  const allDates = new Set<string>();
  const numerators = new Map<string, number>();
  const denominators = new Map<string, number>();
  for (const row of rows) {
    const z = exposureByTicker.get(row.ticker) ?? 0;
    for (const [date, ret] of row.returnsByDateLookback) {
      allDates.add(date);
      if (!Number.isFinite(z) || Math.abs(z) <= 1e-9) continue;
      numerators.set(date, (numerators.get(date) ?? 0) + z * ret);
      denominators.set(date, (denominators.get(date) ?? 0) + Math.abs(z));
    }
  }
  const out = new Map<string, number>();
  for (const date of allDates) {
    const numerator = numerators.get(date) ?? 0;
    const denom = denominators.get(date) ?? 0;
    out.set(date, denom > 1e-9 ? numerator / denom : 0);
  }
  return out;
};

const solveLinearSystem = (matrix: number[][], vector: number[]): number[] | undefined => {
  const n = matrix.length;
  if (!n || vector.length !== n || matrix.some((row) => row.length !== n)) return undefined;
  const augmented = matrix.map((row, index) => [...row, vector[index] ?? 0]);
  for (let pivot = 0; pivot < n; pivot += 1) {
    let best = pivot;
    let bestAbs = Math.abs(augmented[pivot]?.[pivot] ?? 0);
    for (let row = pivot + 1; row < n; row += 1) {
      const abs = Math.abs(augmented[row]?.[pivot] ?? 0);
      if (abs > bestAbs) {
        best = row;
        bestAbs = abs;
      }
    }
    if (bestAbs <= 1e-12) return undefined;
    if (best !== pivot) {
      const temp = augmented[pivot];
      augmented[pivot] = augmented[best]!;
      augmented[best] = temp!;
    }
    const pivotValue = augmented[pivot]?.[pivot] ?? 0;
    for (let col = pivot; col <= n; col += 1) {
      augmented[pivot]![col] = (augmented[pivot]?.[col] ?? 0) / pivotValue;
    }
    for (let row = 0; row < n; row += 1) {
      if (row === pivot) continue;
      const factor = augmented[row]?.[pivot] ?? 0;
      if (Math.abs(factor) <= 1e-12) continue;
      for (let col = pivot; col <= n; col += 1) {
        augmented[row]![col] =
          (augmented[row]?.[col] ?? 0) - factor * (augmented[pivot]?.[col] ?? 0);
      }
    }
  }
  return augmented.map((row) => row[n] ?? 0);
};

const runOls = (
  y: number[],
  predictors: number[][],
): { coefficients: number[]; rSquared: number; residuals: number[] } | undefined => {
  const n = y.length;
  if (!n || predictors.some((series) => series.length !== n)) return undefined;
  const p = predictors.length + 1;
  const xtx = Array.from({ length: p }, () => Array.from({ length: p }, () => 0));
  const xty = Array.from({ length: p }, () => 0);
  for (let row = 0; row < n; row += 1) {
    const xRow = [1, ...predictors.map((series) => series[row] ?? 0)];
    const target = y[row] ?? 0;
    for (let i = 0; i < p; i += 1) {
      xty[i] = (xty[i] ?? 0) + (xRow[i] ?? 0) * target;
      for (let j = 0; j < p; j += 1) {
        xtx[i]![j] = (xtx[i]?.[j] ?? 0) + (xRow[i] ?? 0) * (xRow[j] ?? 0);
      }
    }
  }
  for (let i = 1; i < p; i += 1) {
    xtx[i]![i] = (xtx[i]?.[i] ?? 0) + 1e-6;
  }
  const coefficients = solveLinearSystem(xtx, xty);
  if (!coefficients) return undefined;
  const residuals: number[] = [];
  const predictions: number[] = [];
  for (let row = 0; row < n; row += 1) {
    let prediction = coefficients[0] ?? 0;
    for (let col = 1; col < p; col += 1) {
      prediction += (coefficients[col] ?? 0) * (predictors[col - 1]?.[row] ?? 0);
    }
    predictions.push(prediction);
    residuals.push((y[row] ?? 0) - prediction);
  }
  const yMean = mean(y);
  const sse = residuals.reduce((sum, value) => sum + value ** 2, 0);
  const sst = y.reduce((sum, value) => sum + (value - yMean) ** 2, 0);
  const rSquared = sst > 1e-12 ? clamp01(1 - sse / sst) : 0;
  return {
    coefficients,
    rSquared,
    residuals,
  };
};

const computeCrossSectionFactorAttribution = (params: {
  rows: CrossSectionConstituentInternal[];
  benchmarkTicker: string;
  lookbackDays: number;
  dbPath?: string;
}): CrossSectionFactorAttribution | undefined => {
  const benchmarkTicker = normalizeTicker(params.benchmarkTicker);
  if (!benchmarkTicker || params.rows.length < 2) return undefined;
  const themeByDate = buildThemeReturnByDate(params.rows);
  if (!themeByDate.size) return undefined;
  const benchmarkByDate = derivePriceSignals(
    loadPrices({
      ticker: benchmarkTicker,
      lookbackDays: params.lookbackDays,
      dbPath: params.dbPath,
    }),
  ).returnsByDateLookback;
  if (!benchmarkByDate.size) return undefined;
  const momentumByDate = buildFactorProxyReturnByDate(params.rows, "momentum");
  const valueByDate = buildFactorProxyReturnByDate(params.rows, "value");
  const qualityByDate = buildFactorProxyReturnByDate(params.rows, "quality");
  const sizeByDate = buildFactorProxyReturnByDate(params.rows, "size");
  const dates = Array.from(themeByDate.keys())
    .filter((date) => benchmarkByDate.has(date))
    .sort((left, right) => left.localeCompare(right));
  if (dates.length < 30) return undefined;
  const themeSeries = dates.map((date) => themeByDate.get(date) ?? 0);
  const benchmarkSeries = dates.map((date) => benchmarkByDate.get(date) ?? 0);
  const momentumSeries = dates.map((date) => momentumByDate.get(date) ?? 0);
  const valueSeries = dates.map((date) => valueByDate.get(date) ?? 0);
  const qualitySeries = dates.map((date) => qualityByDate.get(date) ?? 0);
  const sizeSeries = dates.map((date) => sizeByDate.get(date) ?? 0);
  const activeSeries = themeSeries.map((value, index) => value - (benchmarkSeries[index] ?? 0));
  const regression = runOls(activeSeries, [
    benchmarkSeries,
    momentumSeries,
    valueSeries,
    qualitySeries,
    sizeSeries,
  ]);
  if (!regression) return undefined;
  const alphaDaily = regression.coefficients[0] ?? 0;
  const betaBenchmark = regression.coefficients[1] ?? 0;
  const betaMomentum = regression.coefficients[2] ?? 0;
  const betaValue = regression.coefficients[3] ?? 0;
  const betaQuality = regression.coefficients[4] ?? 0;
  const betaSize = regression.coefficients[5] ?? 0;
  const annualizedBenchmark = betaBenchmark * mean(benchmarkSeries) * 252 * 100;
  const annualizedMomentum = betaMomentum * mean(momentumSeries) * 252 * 100;
  const annualizedValue = betaValue * mean(valueSeries) * 252 * 100;
  const annualizedQuality = betaQuality * mean(qualitySeries) * 252 * 100;
  const annualizedSize = betaSize * mean(sizeSeries) * 252 * 100;
  const annualizedActiveReturnPct = mean(activeSeries) * 252 * 100;
  const annualizedAlphaPct = alphaDaily * 252 * 100;
  const annualizedResidualPct = mean(regression.residuals) * 252 * 100;
  const dominantEntries: Array<["benchmark" | CrossSectionFactor, number]> = [
    ["benchmark", annualizedBenchmark],
    ["momentum", annualizedMomentum],
    ["value", annualizedValue],
    ["quality", annualizedQuality],
    ["size", annualizedSize],
  ];
  const dominantContributionFactor =
    dominantEntries.sort((left, right) => Math.abs(right[1]) - Math.abs(left[1]))[0]?.[0] ??
    "benchmark";
  return {
    benchmarkTicker,
    sampleSize: dates.length,
    annualizedActiveReturnPct,
    annualizedAlphaPct,
    annualizedResidualPct,
    rSquared: regression.rSquared,
    factorBetas: {
      benchmark: betaBenchmark,
      momentum: betaMomentum,
      value: betaValue,
      quality: betaQuality,
      size: betaSize,
    },
    annualizedContributionPct: {
      benchmark: annualizedBenchmark,
      momentum: annualizedMomentum,
      value: annualizedValue,
      quality: annualizedQuality,
      size: annualizedSize,
    },
    dominantContributionFactor,
  };
};

const loadOpenCatalystEvents = (params: {
  tickers: string[];
  dbPath?: string;
}): CatalystEventInternal[] => {
  const tickers = Array.from(new Set(params.tickers.map(normalizeTicker).filter(Boolean)));
  if (!tickers.length) return [];
  const db = openResearchDb(params.dbPath);
  const placeholders = tickers.map(() => "?").join(",");
  const rows = db
    .prepare(
      `SELECT
         UPPER(ticker) AS ticker,
         date_window_start,
         date_window_end,
         probability,
         impact_bps,
         confidence,
         direction
       FROM catalysts
       WHERE status='open'
         AND UPPER(ticker) IN (${placeholders})`,
    )
    .all(...tickers) as Array<{
    ticker: string;
    date_window_start: string;
    date_window_end: string;
    probability: number;
    impact_bps: number;
    confidence: number;
    direction: string;
  }>;
  return rows.map((row) => ({
    ticker: normalizeTicker(row.ticker),
    dateWindowStart: row.date_window_start || "",
    dateWindowEnd: row.date_window_end || "",
    probability: clamp01(toFinite(row.probability) ?? 0),
    impactBps: toFinite(row.impact_bps) ?? 0,
    confidence: clamp01(toFinite(row.confidence) ?? 0),
    direction:
      row.direction === "up" || row.direction === "down" || row.direction === "both"
        ? row.direction
        : "both",
  }));
};

const computeCatalystCalendar = (params: {
  rows: CrossSectionConstituentInternal[];
  dbPath?: string;
  horizonDays?: number;
  asOf?: Date;
}): CatalystCalendarMetrics => {
  const asOf = params.asOf ?? new Date();
  const asOfDate = toIsoDate(asOf);
  const horizonDays = Math.max(7, Math.round(params.horizonDays ?? 90));
  const events = loadOpenCatalystEvents({
    tickers: params.rows.map((row) => row.ticker),
    dbPath: params.dbPath,
  });
  if (!events.length) {
    return {
      asOfDate,
      horizonDays,
      totalOpenEvents: 0,
      scheduledEvents: 0,
      unscheduledEvents: 0,
      eventsInHorizon: 0,
      nearTermEvents: 0,
      highImpactEvents: 0,
      avgConfidence: 0,
      weightedExpectedImpactBps: 0,
      weightedDownsideSharePct: 0,
      crowdingScore: 0,
      maxSameDayEvents: 0,
      topEventWindows: [],
      tickerLoad: [],
    };
  }
  let scheduledEvents = 0;
  let unscheduledEvents = 0;
  let eventsInHorizon = 0;
  let nearTermEvents = 0;
  let highImpactEvents = 0;
  let weightedExpectedImpactBps = 0;
  let downsideWeight = 0;
  let absWeight = 0;
  const windowMap = new Map<string, { eventCount: number; weightedExpectedImpactBps: number }>();
  const tickerMap = new Map<
    string,
    {
      openEvents: number;
      scheduledEvents: number;
      nearTermEvents: number;
      weightedExpectedImpactBps: number;
    }
  >();
  for (const event of events) {
    const signedImpact = signedImpactBps(event);
    weightedExpectedImpactBps += signedImpact;
    absWeight += Math.abs(signedImpact);
    if (signedImpact < 0) downsideWeight += Math.abs(signedImpact);
    if (Math.abs(signedImpact) >= 150) highImpactEvents += 1;
    const tickerBucket = tickerMap.get(event.ticker) ?? {
      openEvents: 0,
      scheduledEvents: 0,
      nearTermEvents: 0,
      weightedExpectedImpactBps: 0,
    };
    tickerBucket.openEvents += 1;
    tickerBucket.weightedExpectedImpactBps += signedImpact;
    const start = parseIsoDate(event.dateWindowStart);
    if (!start) {
      unscheduledEvents += 1;
      tickerMap.set(event.ticker, tickerBucket);
      continue;
    }
    scheduledEvents += 1;
    tickerBucket.scheduledEvents += 1;
    const daysUntil = daysBetween(asOf, start);
    if (daysUntil >= 0 && daysUntil <= horizonDays) {
      eventsInHorizon += 1;
      const windowKey = event.dateWindowStart;
      const windowBucket = windowMap.get(windowKey) ?? {
        eventCount: 0,
        weightedExpectedImpactBps: 0,
      };
      windowBucket.eventCount += 1;
      windowBucket.weightedExpectedImpactBps += signedImpact;
      windowMap.set(windowKey, windowBucket);
    }
    if (daysUntil >= 0 && daysUntil <= 14) {
      nearTermEvents += 1;
      tickerBucket.nearTermEvents += 1;
    }
    tickerMap.set(event.ticker, tickerBucket);
  }
  const tickerLoad = Array.from(tickerMap.entries())
    .map(([ticker, row]) => ({
      ticker,
      openEvents: row.openEvents,
      scheduledEvents: row.scheduledEvents,
      nearTermEvents: row.nearTermEvents,
      weightedExpectedImpactBps: row.weightedExpectedImpactBps,
    }))
    .sort((left, right) => {
      const impactDiff =
        Math.abs(right.weightedExpectedImpactBps) - Math.abs(left.weightedExpectedImpactBps);
      if (Math.abs(impactDiff) > 1e-9) return impactDiff;
      return right.openEvents - left.openEvents;
    });
  const topEventWindows = Array.from(windowMap.entries())
    .map(([date, row]) => ({
      date,
      eventCount: row.eventCount,
      weightedExpectedImpactBps: row.weightedExpectedImpactBps,
    }))
    .sort((left, right) => {
      if (right.eventCount !== left.eventCount) return right.eventCount - left.eventCount;
      return Math.abs(right.weightedExpectedImpactBps) - Math.abs(left.weightedExpectedImpactBps);
    })
    .slice(0, 5);
  const maxSameDayEvents = topEventWindows[0]?.eventCount ?? 0;
  const universeSize = Math.max(1, params.rows.length);
  const crowdingScore = clamp01(
    0.45 * clamp01(eventsInHorizon / (universeSize * 2)) +
      0.35 * clamp01(maxSameDayEvents / universeSize) +
      0.2 * clamp01(nearTermEvents / universeSize),
  );
  return {
    asOfDate,
    horizonDays,
    totalOpenEvents: events.length,
    scheduledEvents,
    unscheduledEvents,
    eventsInHorizon,
    nearTermEvents,
    highImpactEvents,
    avgConfidence: mean(events.map((event) => event.confidence)),
    weightedExpectedImpactBps,
    weightedDownsideSharePct: absWeight > 1e-9 ? (downsideWeight / absWeight) * 100 : 0,
    crowdingScore,
    maxSameDayEvents,
    topEventWindows,
    tickerLoad,
  };
};

const buildRiskFlags = (params: {
  metrics: CrossSectionMetrics;
  rows: CrossSectionConstituentInternal[];
  factorDecomposition?: FactorDecomposition;
  catalystCalendar?: CatalystCalendarMetrics;
  factorAttribution?: CrossSectionFactorAttribution;
  benchmarkRelative?: ThemeBenchmarkRelativeMetrics;
}): string[] => {
  const out: string[] = [];
  if (
    params.metrics.breadthPositive20dPct >= 0.65 &&
    typeof params.metrics.medianExpectedUpsidePct === "number" &&
    params.metrics.medianExpectedUpsidePct < 0
  ) {
    out.push(
      "Breadth is strong while median valuation upside is negative; potential crowding risk.",
    );
  }
  if (
    params.metrics.breadthPositive63dPct <= 0.35 &&
    typeof params.metrics.medianVariantGapScore === "number" &&
    params.metrics.medianVariantGapScore >= 0.58
  ) {
    out.push("Weak tape but positive variant gap suggests possible under-owned rebound setup.");
  }
  if (
    typeof params.metrics.averagePairwiseCorrelation63d === "number" &&
    params.metrics.averagePairwiseCorrelation63d >= 0.7 &&
    typeof params.metrics.dispersion63dPct === "number" &&
    params.metrics.dispersion63dPct >= 10
  ) {
    out.push("High correlation with high dispersion indicates fragile, factor-driven leadership.");
  }
  if (params.metrics.avgValuationConfidence < 0.5 || params.metrics.avgVariantConfidence < 0.5) {
    out.push(
      "Core confidence is low; increase filing/transcript/expectation coverage before scaling.",
    );
  }
  const liquidityTail = params.rows.filter(
    (row) =>
      typeof row.avgDailyDollarVolumeUsd === "number" && row.avgDailyDollarVolumeUsd < 5_000_000,
  );
  if (liquidityTail.length >= Math.max(2, Math.ceil(params.rows.length * 0.25))) {
    out.push("Liquidity tail is material; execution slippage risk may dominate alpha at size.");
  }
  if (params.factorDecomposition) {
    const dominant = params.factorDecomposition.exposures.find(
      (row) => row.factor === params.factorDecomposition?.dominantFactor,
    );
    if (
      typeof dominant?.exposureZ === "number" &&
      Math.abs(dominant.exposureZ) >= 0.9 &&
      params.factorDecomposition.concentration >= 0.42
    ) {
      out.push(
        `Style concentration is elevated in ${dominant.factor} (z=${dominant.exposureZ.toFixed(2)}, concentration=${params.factorDecomposition.concentration.toFixed(2)}).`,
      );
    }
    const sizeExposure = params.factorDecomposition.exposures.find((row) => row.factor === "size");
    if (
      typeof sizeExposure?.exposureZ === "number" &&
      sizeExposure.exposureZ >= 0.85 &&
      params.metrics.avgVolatility63dPct !== undefined &&
      params.metrics.avgVolatility63dPct >= 30
    ) {
      out.push(
        "Small-cap/liquidity style tilt is high while volatility is elevated; position sizing discipline is critical.",
      );
    }
  }
  if (params.catalystCalendar) {
    if (
      params.catalystCalendar.eventsInHorizon >= Math.max(4, Math.ceil(params.rows.length * 0.8)) &&
      params.catalystCalendar.crowdingScore >= 0.55
    ) {
      out.push(
        "Catalyst calendar is crowded in the next quarter; gap-risk and correlation spikes around event windows are likely.",
      );
    }
    if (
      params.catalystCalendar.weightedExpectedImpactBps < 0 &&
      params.catalystCalendar.weightedDownsideSharePct >= 65
    ) {
      out.push(
        "Catalyst skew is net downside with high downside share; scenario analysis should stress adverse outcomes.",
      );
    }
  }
  if (params.factorAttribution) {
    if (
      params.factorAttribution.rSquared >= 0.72 &&
      Math.abs(params.factorAttribution.annualizedAlphaPct) <= 3
    ) {
      out.push(
        "Active return is largely explained by benchmark/style betas with limited stock-picking alpha.",
      );
    }
    if (
      params.factorAttribution.factorBetas.benchmark >= 0.25 &&
      params.factorAttribution.annualizedContributionPct.benchmark < 0
    ) {
      out.push(
        "Benchmark beta drift contributed negatively; hedge ratio and beta targeting need tightening.",
      );
    }
  }
  if (params.benchmarkRelative) {
    const relative = params.benchmarkRelative;
    if (relative.relativeReturnPct <= -8) {
      out.push(
        `Theme underperforms ${relative.benchmarkTicker} by ${Math.abs(relative.relativeReturnPct).toFixed(1)}% over the sampled window.`,
      );
    }
    if (
      typeof relative.informationRatio === "number" &&
      typeof relative.trackingErrorPct === "number" &&
      relative.informationRatio < -0.25 &&
      relative.trackingErrorPct >= 20
    ) {
      out.push(
        "Active risk is high while information ratio is negative; benchmark-relative thesis quality is weak.",
      );
    }
    if (
      typeof relative.beta === "number" &&
      relative.beta >= 1.3 &&
      typeof relative.downsideCapturePct === "number" &&
      relative.downsideCapturePct > 110
    ) {
      out.push(
        "Theme has high beta and elevated downside capture versus benchmark; downside convexity is unfavorable.",
      );
    }
  }
  return out;
};

const buildInsightSummary = (params: {
  label: string;
  metrics: CrossSectionMetrics;
  leaders: CrossSectionConstituentInternal[];
  laggards: CrossSectionConstituentInternal[];
  riskFlags: string[];
  factorDecomposition?: FactorDecomposition;
  catalystCalendar?: CatalystCalendarMetrics;
  factorAttribution?: CrossSectionFactorAttribution;
  benchmarkRelative?: ThemeBenchmarkRelativeMetrics;
}): string[] => {
  const out: string[] = [];
  out.push(
    `${params.label} regime=${params.metrics.regime}, breadth63d=${(params.metrics.breadthPositive63dPct * 100).toFixed(1)}%, readiness=${params.metrics.institutionalReadinessScore.toFixed(2)}.`,
  );
  if (params.leaders.length) {
    out.push(
      `Leadership: ${params.leaders
        .slice(0, 3)
        .map(
          (row) =>
            `${row.ticker}(${row.compositeScore.toFixed(2)}, ${typeof row.return63dPct === "number" ? row.return63dPct.toFixed(1) : "n/a"}%)`,
        )
        .join(", ")}.`,
    );
  }
  if (params.laggards.length) {
    out.push(
      `Lagging cohort: ${params.laggards
        .slice(0, 3)
        .map(
          (row) =>
            `${row.ticker}(${row.compositeScore.toFixed(2)}, ${typeof row.return63dPct === "number" ? row.return63dPct.toFixed(1) : "n/a"}%)`,
        )
        .join(", ")}.`,
    );
  }
  if (params.factorDecomposition) {
    const line = params.factorDecomposition.exposures
      .map((row) => `${row.factor}=${row.exposureZ >= 0 ? "+" : ""}${row.exposureZ.toFixed(2)}`)
      .join(", ");
    out.push(
      `Style decomposition: ${line}; dominant=${params.factorDecomposition.dominantFactor}, concentration=${params.factorDecomposition.concentration.toFixed(2)}.`,
    );
  }
  if (params.catalystCalendar && params.catalystCalendar.totalOpenEvents > 0) {
    out.push(
      `Catalyst calendar ${params.catalystCalendar.horizonDays}d: events=${params.catalystCalendar.eventsInHorizon}/${params.catalystCalendar.totalOpenEvents}, near_term=${params.catalystCalendar.nearTermEvents}, net_expected_impact=${params.catalystCalendar.weightedExpectedImpactBps.toFixed(1)}bps, downside_share=${params.catalystCalendar.weightedDownsideSharePct.toFixed(1)}%, crowding=${params.catalystCalendar.crowdingScore.toFixed(2)}.`,
    );
  }
  if (params.factorAttribution) {
    const attr = params.factorAttribution;
    out.push(
      `Attribution vs ${attr.benchmarkTicker}: alpha=${attr.annualizedAlphaPct.toFixed(1)}% ann, R2=${attr.rSquared.toFixed(2)}, dominant_driver=${attr.dominantContributionFactor}, beta_mkt=${attr.factorBetas.benchmark.toFixed(2)}.`,
    );
  }
  if (params.benchmarkRelative) {
    const bench = params.benchmarkRelative;
    out.push(
      `Benchmark ${bench.benchmarkTicker}: theme=${bench.themeReturnPct.toFixed(1)}%, benchmark=${bench.benchmarkReturnPct.toFixed(1)}%, relative=${bench.relativeReturnPct >= 0 ? "+" : ""}${bench.relativeReturnPct.toFixed(1)}%, beta=${typeof bench.beta === "number" ? bench.beta.toFixed(2) : "n/a"}, IR=${typeof bench.informationRatio === "number" ? bench.informationRatio.toFixed(2) : "n/a"}.`,
    );
  }
  if (params.riskFlags.length) out.push(`Primary risk: ${params.riskFlags[0]}`);
  return out;
};

const deriveConstituentSet = (params: {
  sector?: string;
  tickers?: string[];
  lookbackDays: number;
  dbPath?: string;
}): CrossSectionConstituentInternal[] => {
  const universe = loadUniverse({
    sector: params.sector,
    tickers: params.tickers,
    dbPath: params.dbPath,
  });
  if (!universe.length) throw new Error("No constituents found for requested universe.");
  return universe.map((row) =>
    buildConstituentSnapshot({
      ticker: row.ticker,
      companyName: row.name,
      sector: row.sector,
      industry: row.industry,
      lookbackDays: params.lookbackDays,
      dbPath: params.dbPath,
    }),
  );
};

const resolveThemeDefinitionForReport = (params: {
  theme: string;
  preferredVersion?: number;
  fallbackVersion?: number;
  dbPath?: string;
}) => {
  const definitions = listThemeDefinitions({
    theme: params.theme,
    includeInactive: true,
    dbPath: params.dbPath,
  });
  if (!definitions.length) return undefined;
  const versionsToTry = [params.preferredVersion, params.fallbackVersion].filter(
    (value): value is number => typeof value === "number" && Number.isFinite(value),
  );
  for (const version of versionsToTry) {
    const found = definitions.find((row) => row.version === Math.round(version));
    if (found) return found;
  }
  const active = definitions.find((row) => row.status === "active");
  return active ?? definitions[0];
};

const computeThemeBenchmarkRelative = (params: {
  rows: CrossSectionConstituentInternal[];
  benchmarkTicker: string;
  lookbackDays: number;
  dbPath?: string;
}): ThemeBenchmarkRelativeMetrics | undefined => {
  const benchmarkTicker = normalizeTicker(params.benchmarkTicker);
  if (!benchmarkTicker) return undefined;
  const themeByDate = new Map<string, number[]>();
  for (const row of params.rows) {
    for (const [date, ret] of row.returnsByDateLookback) {
      const bucket = themeByDate.get(date) ?? [];
      bucket.push(ret);
      themeByDate.set(date, bucket);
    }
  }
  if (!themeByDate.size) return undefined;
  const themeReturnByDate = new Map<string, number>();
  for (const [date, values] of themeByDate) {
    if (!values.length) continue;
    themeReturnByDate.set(date, mean(values));
  }
  const benchmarkReturns = derivePriceSignals(
    loadPrices({
      ticker: benchmarkTicker,
      lookbackDays: params.lookbackDays,
      dbPath: params.dbPath,
    }),
  ).returnsByDateLookback;
  if (!benchmarkReturns.size) return undefined;
  const overlapDates = Array.from(themeReturnByDate.keys())
    .filter((date) => benchmarkReturns.has(date))
    .sort((left, right) => left.localeCompare(right));
  if (overlapDates.length < 30) return undefined;

  const themeSeries: number[] = [];
  const benchmarkSeries: number[] = [];
  for (const date of overlapDates) {
    const themeRet = themeReturnByDate.get(date);
    const benchmarkRet = benchmarkReturns.get(date);
    if (typeof themeRet !== "number" || typeof benchmarkRet !== "number") continue;
    themeSeries.push(themeRet);
    benchmarkSeries.push(benchmarkRet);
  }
  if (themeSeries.length < 30 || benchmarkSeries.length < 30) return undefined;

  const activeSeries = themeSeries.map((ret, index) => ret - (benchmarkSeries[index] ?? 0));
  const themeReturnPct = compoundedReturnPct(themeSeries);
  const benchmarkReturnPct = compoundedReturnPct(benchmarkSeries);
  if (typeof themeReturnPct !== "number" || typeof benchmarkReturnPct !== "number")
    return undefined;

  const covThemeBenchmark = covariance(themeSeries, benchmarkSeries);
  const benchmarkVariance = covariance(benchmarkSeries, benchmarkSeries);
  const beta =
    typeof covThemeBenchmark === "number" &&
    typeof benchmarkVariance === "number" &&
    benchmarkVariance > 1e-9
      ? covThemeBenchmark / benchmarkVariance
      : undefined;
  const themeVol = stddev(themeSeries);
  const benchmarkVol = stddev(benchmarkSeries);
  const correlation =
    typeof covThemeBenchmark === "number" &&
    typeof themeVol === "number" &&
    themeVol > 1e-9 &&
    typeof benchmarkVol === "number" &&
    benchmarkVol > 1e-9
      ? clamp(covThemeBenchmark / (themeVol * benchmarkVol), -1, 1)
      : undefined;
  const activeStd = stddev(activeSeries);
  const trackingErrorPct =
    typeof activeStd === "number" && activeStd > 1e-9
      ? activeStd * Math.sqrt(252) * 100
      : undefined;
  const informationRatio =
    typeof activeStd === "number" && activeStd > 1e-9
      ? (mean(activeSeries) / activeStd) * Math.sqrt(252)
      : undefined;
  const annualizedActiveReturnPct = mean(activeSeries) * 252 * 100;
  const annualizedAlphaPct =
    typeof beta === "number"
      ? (mean(themeSeries) - beta * mean(benchmarkSeries)) * 252 * 100
      : undefined;

  const upTheme: number[] = [];
  const upBenchmark: number[] = [];
  const downTheme: number[] = [];
  const downBenchmark: number[] = [];
  for (let index = 0; index < benchmarkSeries.length; index += 1) {
    const benchmarkRet = benchmarkSeries[index] ?? 0;
    const themeRet = themeSeries[index] ?? 0;
    if (benchmarkRet > 0) {
      upBenchmark.push(benchmarkRet);
      upTheme.push(themeRet);
    } else if (benchmarkRet < 0) {
      downBenchmark.push(benchmarkRet);
      downTheme.push(themeRet);
    }
  }
  const upsideBenchmarkMean = upBenchmark.length ? mean(upBenchmark) : undefined;
  const downsideBenchmarkMean = downBenchmark.length ? mean(downBenchmark) : undefined;
  const upsideCapturePct =
    typeof upsideBenchmarkMean === "number" && Math.abs(upsideBenchmarkMean) > 1e-9
      ? (mean(upTheme) / upsideBenchmarkMean) * 100
      : undefined;
  const downsideCapturePct =
    typeof downsideBenchmarkMean === "number" && Math.abs(downsideBenchmarkMean) > 1e-9
      ? (mean(downTheme) / downsideBenchmarkMean) * 100
      : undefined;
  const activeHitRatePct =
    activeSeries.length > 0
      ? (activeSeries.filter((value) => value > 0).length / activeSeries.length) * 100
      : 0;

  return {
    benchmarkTicker,
    sampleSize: activeSeries.length,
    lookbackDays: params.lookbackDays,
    themeReturnPct,
    benchmarkReturnPct,
    relativeReturnPct: themeReturnPct - benchmarkReturnPct,
    activeHitRatePct,
    annualizedActiveReturnPct,
    annualizedAlphaPct,
    beta,
    correlation,
    trackingErrorPct,
    informationRatio,
    upsideCapturePct,
    downsideCapturePct,
  };
};

export const computeSectorResearch = (params: {
  sector: string;
  tickers?: string[];
  benchmarkTicker?: string;
  lookbackDays?: number;
  topN?: number;
  dbPath?: string;
}): SectorResearchResult => {
  const sector = params.sector.trim();
  if (!sector) throw new Error("sector is required");
  const lookbackDays = Math.max(90, Math.round(params.lookbackDays ?? 365));
  const topN = Math.max(1, Math.round(params.topN ?? 5));
  const benchmarkTicker = params.benchmarkTicker ? normalizeTicker(params.benchmarkTicker) : "";
  const rows = deriveConstituentSet({
    sector,
    tickers: params.tickers,
    lookbackDays,
    dbPath: params.dbPath,
  });
  const metrics = computeCrossSectionMetrics(rows);
  const factorDecomposition = computeFactorDecomposition(rows);
  const catalystCalendar = computeCatalystCalendar({
    rows,
    dbPath: params.dbPath,
    horizonDays: 90,
  });
  const factorAttribution =
    benchmarkTicker && benchmarkTicker.length
      ? computeCrossSectionFactorAttribution({
          rows,
          benchmarkTicker,
          lookbackDays,
          dbPath: params.dbPath,
        })
      : undefined;
  const sorted = [...rows].sort((left, right) => right.compositeScore - left.compositeScore);
  const leaders = sorted.slice(0, Math.min(topN, sorted.length));
  const laggards = [...sorted].reverse().slice(0, Math.min(topN, sorted.length));
  const riskFlags = buildRiskFlags({
    metrics,
    rows,
    factorDecomposition,
    catalystCalendar,
    factorAttribution,
  });
  const insightSummary = buildInsightSummary({
    label: `Sector ${sector}`,
    metrics,
    leaders,
    laggards,
    riskFlags,
    factorDecomposition,
    catalystCalendar,
    factorAttribution,
  });
  try {
    appendProvenanceEvent({
      eventType: "sector_research_report",
      entityType: "sector",
      entityId: sector.toLowerCase(),
      payload: {
        sector,
        constituent_count: rows.length,
        regime: metrics.regime,
        institutional_readiness: metrics.institutionalReadinessScore,
        risk_flags: riskFlags,
        factor_dominant: factorDecomposition.dominantFactor,
        factor_concentration: factorDecomposition.concentration,
        catalyst_events_horizon: catalystCalendar.eventsInHorizon,
        catalyst_downside_share_pct: catalystCalendar.weightedDownsideSharePct,
        benchmark_ticker: benchmarkTicker || null,
        factor_attribution_alpha_pct: factorAttribution?.annualizedAlphaPct ?? null,
        factor_attribution_r2: factorAttribution?.rSquared ?? null,
      },
      metadata: {
        lookback_days: lookbackDays,
      },
      dbPath: params.dbPath,
    });
  } catch {
    // Preserve research generation path even when provenance write fails.
  }
  return {
    generatedAt: new Date().toISOString(),
    sector,
    tickers: rows.map((row) => row.ticker),
    lookbackDays,
    metrics,
    factorDecomposition,
    catalystCalendar,
    factorAttribution,
    leaders: leaders.map(toPublicSnapshot),
    laggards: laggards.map(toPublicSnapshot),
    constituents: sorted.map(toPublicSnapshot),
    riskFlags,
    insightSummary,
  };
};

export const computeThemeResearch = (params: {
  theme: string;
  tickers?: string[];
  themeVersion?: number;
  minMembershipScore?: number;
  maxConstituents?: number;
  lookbackDays?: number;
  topN?: number;
  dbPath?: string;
}): ThemeResearchResult => {
  const theme = params.theme.trim();
  if (!theme) throw new Error("theme is required");
  let normalizedTickers = Array.from(
    new Set((params.tickers ?? []).map(normalizeTicker).filter(Boolean)),
  );
  let usedThemeRegistry = false;
  let resolvedThemeVersion: number | undefined;
  if (!normalizedTickers.length) {
    const membershipRows = getThemeConstituents({
      theme,
      version: params.themeVersion,
      status: "active",
      minMembershipScore: params.minMembershipScore,
      limit: Math.max(5, Math.round(params.maxConstituents ?? 400)),
      dbPath: params.dbPath,
    });
    if (!membershipRows.length) {
      throw new Error(
        "No active theme membership found; pass --tickers or refresh theme membership first.",
      );
    }
    normalizedTickers = membershipRows.map((row) => normalizeTicker(row.ticker));
    usedThemeRegistry = true;
    resolvedThemeVersion = membershipRows[0]?.themeVersion;
  }
  const resolvedThemeDefinition = resolveThemeDefinitionForReport({
    theme,
    preferredVersion: params.themeVersion,
    fallbackVersion: resolvedThemeVersion,
    dbPath: params.dbPath,
  });
  if (typeof resolvedThemeVersion !== "number" && resolvedThemeDefinition) {
    resolvedThemeVersion = resolvedThemeDefinition.version;
  }
  const benchmarkTicker = resolvedThemeDefinition?.benchmark
    ? normalizeTicker(resolvedThemeDefinition.benchmark)
    : "";
  const lookbackDays = Math.max(90, Math.round(params.lookbackDays ?? 365));
  const topN = Math.max(1, Math.round(params.topN ?? 5));
  const rows = deriveConstituentSet({
    tickers: normalizedTickers,
    lookbackDays,
    dbPath: params.dbPath,
  });
  const benchmarkRelative =
    benchmarkTicker && benchmarkTicker.length
      ? computeThemeBenchmarkRelative({
          rows,
          benchmarkTicker,
          lookbackDays,
          dbPath: params.dbPath,
        })
      : undefined;
  const factorAttribution =
    benchmarkTicker && benchmarkTicker.length
      ? computeCrossSectionFactorAttribution({
          rows,
          benchmarkTicker,
          lookbackDays,
          dbPath: params.dbPath,
        })
      : undefined;
  const metrics = computeCrossSectionMetrics(rows);
  const factorDecomposition = computeFactorDecomposition(rows);
  const catalystCalendar = computeCatalystCalendar({
    rows,
    dbPath: params.dbPath,
    horizonDays: 90,
  });
  const sorted = [...rows].sort((left, right) => right.compositeScore - left.compositeScore);
  const leaders = sorted.slice(0, Math.min(topN, sorted.length));
  const laggards = [...sorted].reverse().slice(0, Math.min(topN, sorted.length));
  const bySector = new Map<string, CrossSectionConstituentInternal[]>();
  for (const row of rows) {
    const group = bySector.get(row.sector) ?? [];
    group.push(row);
    bySector.set(row.sector, group);
  }
  const sectorExposure = Array.from(bySector.entries())
    .map(([sector, group]) => ({
      sector,
      count: group.length,
      sharePct: group.length / rows.length,
      avgCompositeScore: mean(group.map((row) => row.compositeScore)),
    }))
    .sort((left, right) => right.sharePct - left.sharePct);
  const riskFlags = buildRiskFlags({
    metrics,
    rows,
    factorDecomposition,
    catalystCalendar,
    factorAttribution,
    benchmarkRelative,
  });
  const insightSummary = buildInsightSummary({
    label: `Theme ${theme}`,
    metrics,
    leaders,
    laggards,
    riskFlags,
    factorDecomposition,
    catalystCalendar,
    factorAttribution,
    benchmarkRelative,
  });
  try {
    appendProvenanceEvent({
      eventType: "theme_research_report",
      entityType: "theme",
      entityId: theme.toLowerCase(),
      payload: {
        theme,
        constituent_count: rows.length,
        regime: metrics.regime,
        sector_exposure: sectorExposure,
        institutional_readiness: metrics.institutionalReadinessScore,
        risk_flags: riskFlags,
        benchmark_ticker: benchmarkTicker || null,
        benchmark_relative_return_pct: benchmarkRelative?.relativeReturnPct ?? null,
        benchmark_information_ratio: benchmarkRelative?.informationRatio ?? null,
        factor_dominant: factorDecomposition.dominantFactor,
        factor_concentration: factorDecomposition.concentration,
        catalyst_events_horizon: catalystCalendar.eventsInHorizon,
        catalyst_downside_share_pct: catalystCalendar.weightedDownsideSharePct,
        factor_attribution_alpha_pct: factorAttribution?.annualizedAlphaPct ?? null,
        factor_attribution_r2: factorAttribution?.rSquared ?? null,
      },
      metadata: {
        lookback_days: lookbackDays,
        used_theme_registry: usedThemeRegistry,
        theme_version: resolvedThemeVersion ?? null,
        membership_min_score:
          typeof params.minMembershipScore === "number" ? params.minMembershipScore : null,
      },
      dbPath: params.dbPath,
    });
  } catch {
    // Preserve research generation path even when provenance write fails.
  }
  return {
    generatedAt: new Date().toISOString(),
    theme,
    themeVersion: resolvedThemeVersion,
    usedThemeRegistry,
    membershipMinScore: params.minMembershipScore,
    benchmarkRelative,
    factorAttribution,
    tickers: rows.map((row) => row.ticker),
    lookbackDays,
    metrics,
    factorDecomposition,
    catalystCalendar,
    sectorExposure,
    leaders: leaders.map(toPublicSnapshot),
    laggards: laggards.map(toPublicSnapshot),
    constituents: sorted.map(toPublicSnapshot),
    riskFlags,
    insightSummary,
  };
};
