import { getCatalystSummary, type CatalystSummary } from "./catalyst.js";
import { openResearchDb } from "./db.js";

type ScenarioName = "bear" | "base" | "bull";

type FundamentalPeriod = {
  periodEnd: string;
  fiscalPeriod: string;
  revenue?: number;
  operatingIncome?: number;
};

type ScenarioDriver = {
  name: ScenarioName;
  probability: number;
  revenueGrowth: number;
  operatingMargin: number;
  wacc: number;
  terminalGrowth: number;
  taxRate: number;
  fcfConversion: number;
};

export type ScenarioValuation = ScenarioDriver & {
  nextRevenue: number;
  nextEbit: number;
  nextFcf: number;
  enterpriseValue: number;
  impliedSharePrice?: number;
  upsidePct?: number;
};

export type ImpliedExpectations = {
  impliedRevenueGrowth: number;
  impliedOperatingMargin: number;
  impliedNextFcf: number;
  modelRevenueGrowth: number;
  modelOperatingMargin: number;
  growthGap: number;
  marginGap: number;
  stance: "market-too-bearish" | "market-too-bullish" | "aligned" | "insufficient-evidence";
};

export type ValuationResult = {
  ticker: string;
  computedAt: string;
  currentPrice?: number;
  currentPriceDate?: string;
  sharesOutstanding?: number;
  revenueTtm?: number;
  operatingMarginTtm?: number;
  expectationObservationCount: number;
  scenarios: ScenarioValuation[];
  catalystSummary?: CatalystSummary;
  expectedSharePrice?: number;
  expectedUpsidePct?: number;
  expectedUpsideWithCatalystsPct?: number;
  impliedExpectations?: ImpliedExpectations;
  confidence: number;
  notes: string[];
};

export type ValuationInputs = {
  revenueTtm: number;
  operatingMarginTtm: number;
  sharesOutstanding?: number;
  currentPrice?: number;
  recentRevenueGrowth: number;
  expectationTrend: number;
  expectationObservationCount: number;
  wacc?: number;
  terminalGrowth?: number;
  taxRate?: number;
  fcfConversion?: number;
};

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const mean = (values: number[]): number =>
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const sum = (values: number[]): number => values.reduce((acc, value) => acc + value, 0);

const slope = (values: number[]): number => {
  if (values.length < 2) return 0;
  const newest = values[0]!;
  const oldest = values[values.length - 1]!;
  const denom = Math.max(1e-9, Math.abs(oldest));
  return (newest - oldest) / denom;
};

const toFiniteOrUndefined = (value: unknown): number | undefined => {
  if (typeof value !== "number") return undefined;
  return Number.isFinite(value) ? value : undefined;
};

const weightedAverage = (values: Array<{ value?: number; weight: number }>): number | undefined => {
  const usable = values.filter((entry) => typeof entry.value === "number");
  if (!usable.length) return undefined;
  const weightSum = sum(usable.map((entry) => entry.weight));
  if (!Number.isFinite(weightSum) || weightSum <= 1e-9) return undefined;
  const weighted = sum(usable.map((entry) => (entry.value as number) * entry.weight));
  return weighted / weightSum;
};

const annualizedRevenueGrowth = (periods: FundamentalPeriod[]): number => {
  const quarterRevenue = periods
    .filter((period) => /^Q[1-4]/i.test(period.fiscalPeriod) && typeof period.revenue === "number")
    .map((period) => period.revenue as number);
  if (quarterRevenue.length >= 8) {
    const recent = sum(quarterRevenue.slice(0, 4));
    const prior = sum(quarterRevenue.slice(4, 8));
    if (Math.abs(prior) > 1e-9) {
      return clamp((recent - prior) / Math.abs(prior), -0.5, 0.8);
    }
  }
  const annualRevenue = periods
    .filter((period) => /^FY$/i.test(period.fiscalPeriod) && typeof period.revenue === "number")
    .map((period) => period.revenue as number);
  if (annualRevenue.length >= 2 && Math.abs(annualRevenue[1]!) > 1e-9) {
    return clamp((annualRevenue[0]! - annualRevenue[1]!) / Math.abs(annualRevenue[1]!), -0.5, 0.8);
  }
  return 0;
};

const ttmFromQuartersOrAnnual = (
  periods: FundamentalPeriod[],
): { revenueTtm?: number; operatingMarginTtm?: number } => {
  const quarterPeriods = periods.filter((period) => /^Q[1-4]/i.test(period.fiscalPeriod));
  const quarterRevenue = quarterPeriods
    .map((period) => period.revenue)
    .filter((value): value is number => typeof value === "number")
    .slice(0, 4);
  const quarterOperatingIncome = quarterPeriods
    .map((period) => period.operatingIncome)
    .filter((value): value is number => typeof value === "number")
    .slice(0, 4);
  if (quarterRevenue.length >= 4) {
    const revenueTtm = sum(quarterRevenue);
    const opIncomeTtm =
      quarterOperatingIncome.length >= 4 ? sum(quarterOperatingIncome) : undefined;
    const operatingMarginTtm =
      typeof opIncomeTtm === "number" && Math.abs(revenueTtm) > 1e-9
        ? clamp(opIncomeTtm / revenueTtm, -0.2, 0.7)
        : undefined;
    return { revenueTtm, operatingMarginTtm };
  }

  const annual = periods.find(
    (period) => /^FY$/i.test(period.fiscalPeriod) && typeof period.revenue === "number",
  );
  if (annual && typeof annual.revenue === "number") {
    const margin =
      typeof annual.operatingIncome === "number" && Math.abs(annual.revenue) > 1e-9
        ? clamp(annual.operatingIncome / annual.revenue, -0.2, 0.7)
        : undefined;
    return { revenueTtm: annual.revenue, operatingMarginTtm: margin };
  }
  return {};
};

export const buildScenarioDrivers = (inputs: ValuationInputs): ScenarioDriver[] => {
  const terminalGrowth = clamp(inputs.terminalGrowth ?? 0.03, 0.005, 0.045);
  const baseWacc = clamp(inputs.wacc ?? 0.1, 0.06, 0.16);
  const taxRate = clamp(inputs.taxRate ?? 0.21, 0.05, 0.35);
  const fcfConversion = clamp(inputs.fcfConversion ?? 0.78, 0.4, 1.1);

  const trendBlend = 0.65 * inputs.recentRevenueGrowth + 0.35 * inputs.expectationTrend;
  const baseGrowth = clamp(trendBlend, -0.2, 0.45);
  const baseMargin = clamp(
    inputs.operatingMarginTtm + 0.02 * Math.sign(inputs.expectationTrend),
    0.03,
    0.6,
  );

  return [
    {
      name: "bear",
      probability: 0.25,
      revenueGrowth: clamp(baseGrowth - 0.05, -0.3, 0.3),
      operatingMargin: clamp(baseMargin - 0.03, 0.02, 0.55),
      wacc: clamp(baseWacc + 0.01, terminalGrowth + 0.01, 0.2),
      terminalGrowth,
      taxRate,
      fcfConversion,
    },
    {
      name: "base",
      probability: 0.5,
      revenueGrowth: baseGrowth,
      operatingMargin: baseMargin,
      wacc: clamp(baseWacc, terminalGrowth + 0.01, 0.2),
      terminalGrowth,
      taxRate,
      fcfConversion,
    },
    {
      name: "bull",
      probability: 0.25,
      revenueGrowth: clamp(baseGrowth + 0.05, -0.15, 0.55),
      operatingMargin: clamp(baseMargin + 0.03, 0.04, 0.7),
      wacc: clamp(baseWacc - 0.0075, terminalGrowth + 0.01, 0.2),
      terminalGrowth,
      taxRate,
      fcfConversion,
    },
  ];
};

const valueScenario = (params: {
  revenueTtm: number;
  sharesOutstanding?: number;
  currentPrice?: number;
  driver: ScenarioDriver;
}): ScenarioValuation => {
  const nextRevenue = params.revenueTtm * (1 + params.driver.revenueGrowth);
  const nextEbit = nextRevenue * params.driver.operatingMargin;
  const nextFcf = nextEbit * (1 - params.driver.taxRate) * params.driver.fcfConversion;
  const denom = Math.max(0.01, params.driver.wacc - params.driver.terminalGrowth);
  const enterpriseValue = nextFcf / denom;
  const impliedSharePrice =
    typeof params.sharesOutstanding === "number" && params.sharesOutstanding > 0
      ? enterpriseValue / params.sharesOutstanding
      : undefined;
  const upsidePct =
    typeof impliedSharePrice === "number" &&
    typeof params.currentPrice === "number" &&
    params.currentPrice > 0
      ? impliedSharePrice / params.currentPrice - 1
      : undefined;
  return {
    ...params.driver,
    nextRevenue,
    nextEbit,
    nextFcf,
    enterpriseValue,
    impliedSharePrice,
    upsidePct,
  };
};

export const deriveImpliedExpectations = (params: {
  marketCap: number;
  revenueTtm: number;
  modelRevenueGrowth: number;
  modelOperatingMargin: number;
  wacc: number;
  terminalGrowth: number;
  taxRate: number;
  fcfConversion: number;
}): ImpliedExpectations => {
  const denom = Math.max(0.01, params.wacc - params.terminalGrowth);
  const impliedNextFcf = params.marketCap * denom;
  const fcfDenom = Math.max(1e-9, (1 - params.taxRate) * params.fcfConversion);
  const impliedNextEbit = impliedNextFcf / fcfDenom;

  const impliedRevenueGrowth =
    params.modelOperatingMargin > 1e-9
      ? impliedNextEbit / (params.revenueTtm * params.modelOperatingMargin) - 1
      : 0;
  const impliedOperatingMargin =
    params.revenueTtm * (1 + params.modelRevenueGrowth) > 1e-9
      ? impliedNextEbit / (params.revenueTtm * (1 + params.modelRevenueGrowth))
      : params.modelOperatingMargin;

  const growthGap = params.modelRevenueGrowth - impliedRevenueGrowth;
  const marginGap = params.modelOperatingMargin - impliedOperatingMargin;

  let stance: ImpliedExpectations["stance"] = "aligned";
  if (!Number.isFinite(impliedRevenueGrowth) || !Number.isFinite(impliedOperatingMargin)) {
    stance = "insufficient-evidence";
  } else if (growthGap > 0.03 && marginGap > 0.01) {
    stance = "market-too-bearish";
  } else if (growthGap < -0.03 && marginGap < -0.01) {
    stance = "market-too-bullish";
  }

  return {
    impliedRevenueGrowth,
    impliedOperatingMargin,
    impliedNextFcf,
    modelRevenueGrowth: params.modelRevenueGrowth,
    modelOperatingMargin: params.modelOperatingMargin,
    growthGap,
    marginGap,
    stance,
  };
};

const loadLatestPrice = (ticker: string, dbPath?: string): { close?: number; date?: string } => {
  const db = openResearchDb(dbPath);
  const row = db
    .prepare(
      `SELECT p.close, p.date
       FROM prices p
       JOIN instruments i ON i.id=p.instrument_id
       WHERE i.ticker=?
       ORDER BY p.date DESC
       LIMIT 1`,
    )
    .get(ticker) as { close?: number; date?: string } | undefined;
  return {
    close: toFiniteOrUndefined(row?.close),
    date: row?.date?.trim() || undefined,
  };
};

const loadFundamentals = (ticker: string, dbPath?: string): FundamentalPeriod[] => {
  const db = openResearchDb(dbPath);
  const rows = db
    .prepare(
      `SELECT ff.period_end, ff.fiscal_period, ff.concept, ff.value
       FROM fundamental_facts ff
       JOIN instruments i ON i.id=ff.instrument_id
       WHERE i.ticker=?
         AND ff.is_latest=1
         AND ff.taxonomy='us-gaap'
         AND ff.concept IN ('Revenues', 'OperatingIncomeLoss')
       ORDER BY ff.period_end DESC
       LIMIT 80`,
    )
    .all(ticker) as Array<{
    period_end: string;
    fiscal_period: string;
    concept: string;
    value: number;
  }>;
  const byPeriod = new Map<string, FundamentalPeriod>();
  for (const row of rows) {
    const key = `${row.period_end}|${row.fiscal_period}`;
    const current = byPeriod.get(key) ?? {
      periodEnd: row.period_end,
      fiscalPeriod: row.fiscal_period,
    };
    if (row.concept === "Revenues") current.revenue = toFiniteOrUndefined(row.value);
    if (row.concept === "OperatingIncomeLoss")
      current.operatingIncome = toFiniteOrUndefined(row.value);
    byPeriod.set(key, current);
  }
  return Array.from(byPeriod.values()).toSorted((a, b) => b.periodEnd.localeCompare(a.periodEnd));
};

const loadSharesOutstanding = (ticker: string, dbPath?: string): number | undefined => {
  const db = openResearchDb(dbPath);
  const row = db
    .prepare(
      `SELECT ff.value
       FROM fundamental_facts ff
       JOIN instruments i ON i.id=ff.instrument_id
       WHERE i.ticker=?
         AND ff.is_latest=1
         AND (
           (ff.taxonomy='dei' AND ff.concept='EntityCommonStockSharesOutstanding')
           OR (ff.taxonomy='us-gaap' AND ff.concept='CommonStockSharesOutstanding')
         )
       ORDER BY ff.period_end DESC, ff.filing_date DESC
       LIMIT 1`,
    )
    .get(ticker) as { value?: number } | undefined;
  return toFiniteOrUndefined(row?.value);
};

const loadExpectationTrend = (
  ticker: string,
  dbPath?: string,
): { trend: number; observationCount: number } => {
  const db = openResearchDb(dbPath);
  const rows = db
    .prepare(
      `SELECT estimated_eps, surprise_pct
       FROM earnings_expectations e
       JOIN instruments i ON i.id=e.instrument_id
       WHERE i.ticker=?
         AND e.period_type='quarterly'
       ORDER BY CASE
         WHEN e.reported_date <> '' THEN e.reported_date
         ELSE e.fiscal_date_ending
       END DESC
       LIMIT 12`,
    )
    .all(ticker) as Array<{ estimated_eps?: number | null; surprise_pct?: number | null }>;
  const estimated = rows
    .map((row) => toFiniteOrUndefined(row.estimated_eps ?? undefined))
    .filter((value): value is number => typeof value === "number")
    .slice(0, 6);
  const surprises = rows
    .map((row) => toFiniteOrUndefined(row.surprise_pct ?? undefined))
    .filter((value): value is number => typeof value === "number")
    .slice(0, 6);
  const trendFromEstimate = estimated.length >= 2 ? slope(estimated) : 0;
  const surpriseSignal = surprises.length ? clamp(mean(surprises) / 40, -0.5, 0.5) : 0;
  return {
    trend: clamp(0.75 * trendFromEstimate + 0.25 * surpriseSignal, -0.6, 0.6),
    observationCount: rows.length,
  };
};

export const computeValuation = (params: { ticker: string; dbPath?: string }): ValuationResult => {
  const ticker = params.ticker.trim().toUpperCase();
  const notes: string[] = [];
  const price = loadLatestPrice(ticker, params.dbPath);
  const currentPrice = price.close;
  const currentPriceDate = price.date;
  const sharesOutstanding = loadSharesOutstanding(ticker, params.dbPath);
  const fundamentals = loadFundamentals(ticker, params.dbPath);
  const ttm = ttmFromQuartersOrAnnual(fundamentals);
  const recentRevenueGrowth = annualizedRevenueGrowth(fundamentals);
  const expectationSignal = loadExpectationTrend(ticker, params.dbPath);
  const catalystSummary = getCatalystSummary({ ticker, dbPath: params.dbPath });

  if (typeof currentPrice !== "number") notes.push("Missing latest price");
  if (typeof sharesOutstanding !== "number") notes.push("Missing shares outstanding");
  if (typeof ttm.revenueTtm !== "number") notes.push("Missing revenue history");
  if (typeof ttm.operatingMarginTtm !== "number") notes.push("Missing operating margin history");
  if (expectationSignal.observationCount < 4) notes.push("Limited expectations observations (<4)");

  if (typeof ttm.revenueTtm !== "number" || typeof ttm.operatingMarginTtm !== "number") {
    return {
      ticker,
      computedAt: new Date().toISOString(),
      currentPrice,
      currentPriceDate,
      sharesOutstanding,
      revenueTtm: ttm.revenueTtm,
      operatingMarginTtm: ttm.operatingMarginTtm,
      expectationObservationCount: expectationSignal.observationCount,
      scenarios: [],
      catalystSummary,
      confidence: 0,
      notes,
    };
  }

  const scenarioDrivers = buildScenarioDrivers({
    revenueTtm: ttm.revenueTtm,
    operatingMarginTtm: ttm.operatingMarginTtm,
    sharesOutstanding,
    currentPrice,
    recentRevenueGrowth,
    expectationTrend: expectationSignal.trend,
    expectationObservationCount: expectationSignal.observationCount,
  });
  const scenarios = scenarioDrivers.map((driver) =>
    valueScenario({
      revenueTtm: ttm.revenueTtm as number,
      sharesOutstanding,
      currentPrice,
      driver,
    }),
  );

  const expectedSharePrice = weightedAverage(
    scenarios.map((scenario) => ({
      value: scenario.impliedSharePrice,
      weight: scenario.probability,
    })),
  );
  const expectedUpsidePct =
    typeof expectedSharePrice === "number" && typeof currentPrice === "number" && currentPrice > 0
      ? expectedSharePrice / currentPrice - 1
      : undefined;
  const expectedUpsideWithCatalystsPct =
    typeof expectedUpsidePct === "number"
      ? expectedUpsidePct + catalystSummary.expectedImpactPct
      : undefined;

  const base = scenarios.find((scenario) => scenario.name === "base");
  const impliedExpectations =
    typeof currentPrice === "number" &&
    typeof sharesOutstanding === "number" &&
    typeof base === "object"
      ? deriveImpliedExpectations({
          marketCap: currentPrice * sharesOutstanding,
          revenueTtm: ttm.revenueTtm,
          modelRevenueGrowth: base.revenueGrowth,
          modelOperatingMargin: base.operatingMargin,
          wacc: base.wacc,
          terminalGrowth: base.terminalGrowth,
          taxRate: base.taxRate,
          fcfConversion: base.fcfConversion,
        })
      : undefined;

  const confidence = clamp(
    0.2 * (typeof currentPrice === "number" ? 1 : 0) +
      0.2 * (typeof sharesOutstanding === "number" ? 1 : 0) +
      0.25 * (typeof ttm.revenueTtm === "number" ? 1 : 0) +
      0.2 * (typeof ttm.operatingMarginTtm === "number" ? 1 : 0) +
      0.15 * clamp(expectationSignal.observationCount / 8, 0, 1),
    0,
    1,
  );

  return {
    ticker,
    computedAt: new Date().toISOString(),
    currentPrice,
    currentPriceDate,
    sharesOutstanding,
    revenueTtm: ttm.revenueTtm,
    operatingMarginTtm: ttm.operatingMarginTtm,
    expectationObservationCount: expectationSignal.observationCount,
    scenarios,
    catalystSummary,
    expectedSharePrice,
    expectedUpsidePct,
    expectedUpsideWithCatalystsPct,
    impliedExpectations,
    confidence,
    notes,
  };
};

export const recordValuationForecast = (params: {
  ticker: string;
  predictedReturn: number;
  startPrice: number;
  basePriceDate: string;
  horizonDays?: number;
  source?: string;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  const ticker = params.ticker.trim().toUpperCase();
  const horizonDays = Math.max(7, params.horizonDays ?? 90);
  const source = params.source?.trim() || "memo";
  const existing = db
    .prepare(
      `SELECT id
       FROM thesis_forecasts
       WHERE ticker=?
         AND forecast_type='valuation_upside'
         AND horizon_days=?
         AND base_price_date=?
         AND source=?
         AND resolved=0
       ORDER BY created_at DESC
       LIMIT 1`,
    )
    .get(ticker, horizonDays, params.basePriceDate, source) as { id?: number } | undefined;
  if (typeof existing?.id === "number") {
    return existing.id;
  }
  const instrument = db.prepare(`SELECT id FROM instruments WHERE ticker=?`).get(ticker) as
    | { id?: number }
    | undefined;
  const row = db
    .prepare(
      `INSERT INTO thesis_forecasts (
         instrument_id, ticker, forecast_type, horizon_days, predicted_return,
         start_price, base_price_date, source, created_at, resolved
       )
       VALUES (?, ?, 'valuation_upside', ?, ?, ?, ?, ?, ?, 0)
       RETURNING id`,
    )
    .get(
      instrument?.id ?? null,
      ticker,
      horizonDays,
      params.predictedReturn,
      params.startPrice,
      params.basePriceDate,
      source,
      Date.now(),
    ) as { id: number };
  return row.id;
};

const resolveTargetPrice = (params: {
  ticker: string;
  targetDate: string;
  dbPath?: string;
}): { price?: number; date?: string } => {
  const db = openResearchDb(params.dbPath);
  const future = db
    .prepare(
      `SELECT p.close, p.date
       FROM prices p
       JOIN instruments i ON i.id=p.instrument_id
       WHERE i.ticker=? AND p.date >= ?
       ORDER BY p.date ASC
       LIMIT 1`,
    )
    .get(params.ticker, params.targetDate) as { close?: number; date?: string } | undefined;
  if (typeof future?.close === "number") {
    return { price: future.close, date: future.date };
  }
  const latest = db
    .prepare(
      `SELECT p.close, p.date
       FROM prices p
       JOIN instruments i ON i.id=p.instrument_id
       WHERE i.ticker=? AND p.date <= ?
       ORDER BY p.date DESC
       LIMIT 1`,
    )
    .get(params.ticker, params.targetDate) as { close?: number; date?: string } | undefined;
  return { price: toFiniteOrUndefined(latest?.close), date: latest?.date };
};

export const resolveMatureForecasts = (params: { dbPath?: string } = {}) => {
  const db = openResearchDb(params.dbPath);
  const unresolved = db
    .prepare(
      `SELECT id, ticker, horizon_days, start_price, created_at
       FROM thesis_forecasts
       WHERE resolved=0
       ORDER BY created_at ASC
       LIMIT 200`,
    )
    .all() as Array<{
    id: number;
    ticker: string;
    horizon_days: number;
    start_price: number;
    created_at: number;
  }>;
  let resolvedNow = 0;
  for (const row of unresolved) {
    const targetMs = row.created_at + row.horizon_days * 86_400_000;
    if (Date.now() < targetMs) continue;
    const targetIso = new Date(targetMs).toISOString().slice(0, 10);
    const end = resolveTargetPrice({
      ticker: row.ticker,
      targetDate: targetIso,
      dbPath: params.dbPath,
    });
    const endPrice = toFiniteOrUndefined(end.price);
    const realizedReturn =
      typeof endPrice === "number" && Math.abs(row.start_price) > 1e-9
        ? endPrice / row.start_price - 1
        : null;
    db.prepare(
      `UPDATE thesis_forecasts
       SET resolved=1,
           resolved_at=?,
           end_price=?,
           realized_return=?,
           resolution_note=?
       WHERE id=?`,
    ).run(
      Date.now(),
      endPrice ?? null,
      realizedReturn,
      end.date ? `resolved_with_price_date=${end.date}` : "no_price_available",
      row.id,
    );
    resolvedNow += 1;
  }
  return { unresolvedCount: unresolved.length, resolvedNow };
};

export const forecastDecisionMetrics = (params: { dbPath?: string } = {}) => {
  const db = openResearchDb(params.dbPath);
  const rows = db
    .prepare(
      `SELECT predicted_return, realized_return
       FROM thesis_forecasts
       WHERE resolved=1
         AND realized_return IS NOT NULL
       ORDER BY resolved_at DESC
       LIMIT 200`,
    )
    .all() as Array<{ predicted_return: number; realized_return: number }>;

  const count = rows.length;
  if (!count) {
    return {
      count: 0,
      mae: undefined as number | undefined,
      directionalAccuracy: undefined as number | undefined,
      meanPredicted: undefined as number | undefined,
      meanRealized: undefined as number | undefined,
    };
  }
  const mae =
    rows.reduce(
      (sumValue, row) => sumValue + Math.abs(row.predicted_return - row.realized_return),
      0,
    ) / count;
  const directionalCorrect = rows.filter(
    (row) => Math.sign(row.predicted_return) === Math.sign(row.realized_return),
  ).length;
  const directionalAccuracy = directionalCorrect / count;
  const meanPredicted = rows.reduce((sumValue, row) => sumValue + row.predicted_return, 0) / count;
  const meanRealized = rows.reduce((sumValue, row) => sumValue + row.realized_return, 0) / count;
  return {
    count,
    mae,
    directionalAccuracy,
    meanPredicted,
    meanRealized,
  };
};
