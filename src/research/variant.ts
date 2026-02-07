import { openResearchDb } from "./db.js";

type QuarterlyExpectation = {
  fiscalDateEnding: string;
  reportedDate: string;
  reportedEps?: number;
  estimatedEps?: number;
  surprisePct?: number;
};

type FundamentalPoint = {
  periodEnd: string;
  revenue?: number;
  operatingIncome?: number;
};

const REVENUE_CONCEPTS = [
  "Revenues",
  "RevenueFromContractWithCustomerExcludingAssessedTax",
  "SalesRevenueNet",
  "Revenue",
];
const OPERATING_INCOME_CONCEPTS = ["OperatingIncomeLoss", "ProfitLossFromOperatingActivities"];

export type VariantPerceptionResult = {
  ticker: string;
  computedAt: string;
  expectationScore: number;
  fundamentalScore: number;
  variantGapScore: number;
  confidence: number;
  stance: "positive-variant" | "negative-variant" | "in-line" | "insufficient-evidence";
  expectationObservations: number;
  fundamentalObservations: number;
  metrics: {
    avgSurprisePct?: number;
    estimateTrend?: number;
    revenueGrowth?: number;
    marginDelta?: number;
  };
  notes: string[];
};

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const toFiniteOrUndefined = (value: unknown): number | undefined => {
  if (typeof value !== "number") return undefined;
  return Number.isFinite(value) ? value : undefined;
};

const normalizeSignal = (value: number, scale: number): number =>
  clamp01(0.5 + Math.max(-1, Math.min(1, value / scale)) * 0.5);

const slope = (series: number[]): number => {
  if (series.length < 2) return 0;
  const newest = series[0]!;
  const oldest = series[series.length - 1]!;
  const denom = Math.max(1e-9, Math.abs(oldest));
  return (newest - oldest) / denom;
};

export const deriveVariantScores = (inputs: {
  surprisesPct: number[];
  estimatedEps: number[];
  revenueSeries: number[];
  marginSeries: number[];
}): {
  expectationScore: number;
  fundamentalScore: number;
  variantGapScore: number;
  avgSurprisePct?: number;
  estimateTrend?: number;
  revenueGrowth?: number;
  marginDelta?: number;
} => {
  const avgSurprisePct =
    inputs.surprisesPct.length > 0
      ? inputs.surprisesPct.reduce((sum, value) => sum + value, 0) / inputs.surprisesPct.length
      : undefined;
  const estimateTrend = inputs.estimatedEps.length >= 2 ? slope(inputs.estimatedEps) : undefined;
  const revenueGrowth = inputs.revenueSeries.length >= 2 ? slope(inputs.revenueSeries) : undefined;
  const marginDelta =
    inputs.marginSeries.length >= 2
      ? inputs.marginSeries[0]! - inputs.marginSeries[inputs.marginSeries.length - 1]!
      : undefined;

  const surpriseNorm =
    typeof avgSurprisePct === "number" ? normalizeSignal(avgSurprisePct, 20) : 0.5;
  const estimateNorm =
    typeof estimateTrend === "number" ? normalizeSignal(estimateTrend, 0.8) : 0.5;
  const revenueNorm = typeof revenueGrowth === "number" ? normalizeSignal(revenueGrowth, 0.6) : 0.5;
  const marginNorm = typeof marginDelta === "number" ? normalizeSignal(marginDelta, 0.15) : 0.5;

  const expectationScore = clamp01(0.6 * surpriseNorm + 0.4 * estimateNorm);
  const fundamentalScore = clamp01(0.7 * revenueNorm + 0.3 * marginNorm);
  const variantGapScore = clamp01(0.5 + (fundamentalScore - expectationScore));

  return {
    expectationScore,
    fundamentalScore,
    variantGapScore,
    avgSurprisePct,
    estimateTrend,
    revenueGrowth,
    marginDelta,
  };
};

const loadQuarterlyExpectations = (ticker: string, dbPath?: string): QuarterlyExpectation[] => {
  const db = openResearchDb(dbPath);
  const rows = db
    .prepare(
      `SELECT
         e.fiscal_date_ending,
         e.reported_date,
         e.reported_eps,
         e.estimated_eps,
         e.surprise_pct
       FROM earnings_expectations e
       JOIN instruments i ON i.id=e.instrument_id
       WHERE i.ticker=? AND e.period_type='quarterly'
       ORDER BY CASE
         WHEN e.reported_date <> '' THEN e.reported_date
         ELSE e.fiscal_date_ending
       END DESC
       LIMIT 12`,
    )
    .all(ticker) as Array<{
    fiscal_date_ending: string;
    reported_date: string;
    reported_eps?: number | null;
    estimated_eps?: number | null;
    surprise_pct?: number | null;
  }>;
  return rows.map((row) => ({
    fiscalDateEnding: row.fiscal_date_ending,
    reportedDate: row.reported_date,
    reportedEps: toFiniteOrUndefined(row.reported_eps ?? undefined),
    estimatedEps: toFiniteOrUndefined(row.estimated_eps ?? undefined),
    surprisePct: toFiniteOrUndefined(row.surprise_pct ?? undefined),
  }));
};

const loadFundamentalSeries = (ticker: string, dbPath?: string): FundamentalPoint[] => {
  const db = openResearchDb(dbPath);
  const rows = db
    .prepare(
      `SELECT ff.period_end, ff.concept, ff.value
       FROM fundamental_facts ff
       JOIN instruments i ON i.id=ff.instrument_id
       WHERE i.ticker=?
         AND ff.is_latest=1
         AND (
           (ff.taxonomy='us-gaap' AND ff.concept IN ('Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'OperatingIncomeLoss'))
           OR (ff.taxonomy='ifrs-full' AND ff.concept IN ('Revenue', 'ProfitLossFromOperatingActivities'))
         )
       ORDER BY ff.period_end DESC
       LIMIT 48`,
    )
    .all(ticker) as Array<{
    period_end: string;
    concept: string;
    value: number;
  }>;

  const byPeriod = new Map<string, FundamentalPoint>();
  for (const row of rows) {
    const current = byPeriod.get(row.period_end) ?? { periodEnd: row.period_end };
    if (REVENUE_CONCEPTS.includes(row.concept)) current.revenue = toFiniteOrUndefined(row.value);
    if (OPERATING_INCOME_CONCEPTS.includes(row.concept))
      current.operatingIncome = toFiniteOrUndefined(row.value);
    byPeriod.set(row.period_end, current);
  }

  return Array.from(byPeriod.values())
    .filter((point) => typeof point.revenue === "number")
    .toSorted((a, b) => b.periodEnd.localeCompare(a.periodEnd))
    .slice(0, 8);
};

export const computeVariantPerception = (params: {
  ticker: string;
  dbPath?: string;
}): VariantPerceptionResult => {
  const ticker = params.ticker.trim().toUpperCase();
  const expectations = loadQuarterlyExpectations(ticker, params.dbPath);
  const fundamentals = loadFundamentalSeries(ticker, params.dbPath);

  const surprisesPct = expectations
    .map((row) => row.surprisePct)
    .filter((value): value is number => typeof value === "number")
    .slice(0, 6);
  const estimatedEps = expectations
    .map((row) => row.estimatedEps)
    .filter((value): value is number => typeof value === "number")
    .slice(0, 6);
  const revenueSeries = fundamentals
    .map((row) => row.revenue)
    .filter((value): value is number => typeof value === "number");
  const marginSeries = fundamentals
    .map((row) => {
      if (typeof row.revenue !== "number" || typeof row.operatingIncome !== "number")
        return undefined;
      if (Math.abs(row.revenue) <= 1e-9) return undefined;
      return row.operatingIncome / row.revenue;
    })
    .filter((value): value is number => typeof value === "number");

  const derived = deriveVariantScores({
    surprisesPct,
    estimatedEps,
    revenueSeries,
    marginSeries,
  });

  const notes: string[] = [];
  const expectationCoverage = clamp01(expectations.length / 8);
  const fundamentalCoverage = clamp01(fundamentals.length / 6);
  const signalStrength = clamp01(Math.abs(derived.fundamentalScore - derived.expectationScore) * 2);
  const confidence = clamp01(
    0.4 * expectationCoverage + 0.4 * fundamentalCoverage + 0.2 * signalStrength,
  );

  if (expectations.length < 4) notes.push("Limited expectations history (<4 quarters)");
  if (fundamentals.length < 4) notes.push("Limited fundamental history (<4 periods)");

  let stance: VariantPerceptionResult["stance"] = "in-line";
  if (expectations.length < 2 || fundamentals.length < 2) {
    stance = "insufficient-evidence";
  } else if (derived.variantGapScore >= 0.62) {
    stance = "positive-variant";
  } else if (derived.variantGapScore <= 0.38) {
    stance = "negative-variant";
  }

  return {
    ticker,
    computedAt: new Date().toISOString(),
    expectationScore: derived.expectationScore,
    fundamentalScore: derived.fundamentalScore,
    variantGapScore: derived.variantGapScore,
    confidence,
    stance,
    expectationObservations: expectations.length,
    fundamentalObservations: fundamentals.length,
    metrics: {
      avgSurprisePct: derived.avgSurprisePct,
      estimateTrend: derived.estimateTrend,
      revenueGrowth: derived.revenueGrowth,
      marginDelta: derived.marginDelta,
    },
    notes,
  };
};
