import { getCatalystSummary } from "./catalyst.js";
import { openResearchDb } from "./db.js";
import { computePortfolioPlan } from "./portfolio.js";
import { appendProvenanceEvent } from "./provenance.js";
import { computeValuation } from "./valuation.js";
import { computeVariantPerception } from "./variant.js";

type PriceRow = {
  date: string;
  close: number;
  volume?: number;
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
};

export type CrossSectionConstituentSnapshot = Omit<
  CrossSectionConstituentInternal,
  "returnsByDate63d"
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
  leaders: CrossSectionConstituentSnapshot[];
  laggards: CrossSectionConstituentSnapshot[];
  constituents: CrossSectionConstituentSnapshot[];
  riskFlags: string[];
  insightSummary: string[];
};

export type ThemeResearchResult = {
  generatedAt: string;
  theme: string;
  tickers: string[];
  lookbackDays: number;
  metrics: CrossSectionMetrics;
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
  const returnValues: number[] = [];
  const startIndex = Math.max(1, prices.length - 63);
  for (let index = startIndex; index < prices.length; index += 1) {
    const current = prices[index];
    const prior = prices[index - 1];
    if (!current || !prior || Math.abs(prior.close) <= 1e-9) continue;
    const ret = current.close / prior.close - 1;
    if (!Number.isFinite(ret)) continue;
    returnsByDate63d.set(current.date, ret);
    returnValues.push(ret);
  }
  const volatility63dPct =
    returnValues.length >= 15 ? (stddev(returnValues) ?? 0) * Math.sqrt(252) * 100 : undefined;

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

const buildRiskFlags = (params: {
  metrics: CrossSectionMetrics;
  rows: CrossSectionConstituentInternal[];
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
  return out;
};

const buildInsightSummary = (params: {
  label: string;
  metrics: CrossSectionMetrics;
  leaders: CrossSectionConstituentInternal[];
  laggards: CrossSectionConstituentInternal[];
  riskFlags: string[];
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

export const computeSectorResearch = (params: {
  sector: string;
  tickers?: string[];
  lookbackDays?: number;
  topN?: number;
  dbPath?: string;
}): SectorResearchResult => {
  const sector = params.sector.trim();
  if (!sector) throw new Error("sector is required");
  const lookbackDays = Math.max(90, Math.round(params.lookbackDays ?? 365));
  const topN = Math.max(1, Math.round(params.topN ?? 5));
  const rows = deriveConstituentSet({
    sector,
    tickers: params.tickers,
    lookbackDays,
    dbPath: params.dbPath,
  });
  const metrics = computeCrossSectionMetrics(rows);
  const sorted = [...rows].sort((left, right) => right.compositeScore - left.compositeScore);
  const leaders = sorted.slice(0, Math.min(topN, sorted.length));
  const laggards = [...sorted].reverse().slice(0, Math.min(topN, sorted.length));
  const riskFlags = buildRiskFlags({ metrics, rows });
  const insightSummary = buildInsightSummary({
    label: `Sector ${sector}`,
    metrics,
    leaders,
    laggards,
    riskFlags,
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
    leaders: leaders.map(toPublicSnapshot),
    laggards: laggards.map(toPublicSnapshot),
    constituents: sorted.map(toPublicSnapshot),
    riskFlags,
    insightSummary,
  };
};

export const computeThemeResearch = (params: {
  theme: string;
  tickers: string[];
  lookbackDays?: number;
  topN?: number;
  dbPath?: string;
}): ThemeResearchResult => {
  const theme = params.theme.trim();
  if (!theme) throw new Error("theme is required");
  const normalizedTickers = Array.from(
    new Set(params.tickers.map(normalizeTicker).filter(Boolean)),
  );
  if (!normalizedTickers.length) throw new Error("tickers are required");
  const lookbackDays = Math.max(90, Math.round(params.lookbackDays ?? 365));
  const topN = Math.max(1, Math.round(params.topN ?? 5));
  const rows = deriveConstituentSet({
    tickers: normalizedTickers,
    lookbackDays,
    dbPath: params.dbPath,
  });
  const metrics = computeCrossSectionMetrics(rows);
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
  const riskFlags = buildRiskFlags({ metrics, rows });
  const insightSummary = buildInsightSummary({
    label: `Theme ${theme}`,
    metrics,
    leaders,
    laggards,
    riskFlags,
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
    theme,
    tickers: rows.map((row) => row.ticker),
    lookbackDays,
    metrics,
    sectorExposure,
    leaders: leaders.map(toPublicSnapshot),
    laggards: laggards.map(toPublicSnapshot),
    constituents: sorted.map(toPublicSnapshot),
    riskFlags,
    insightSummary,
  };
};
