import { openResearchDb } from "./db.js";
import {
  type CrossSectionConstituentSnapshot,
  type SectorResearchResult,
  type ThemeResearchResult,
} from "./theme-sector.js";
import { computeValuation } from "./valuation.js";

type SourceObservation = {
  sourceTable: string;
  date?: string;
  url?: string;
  host?: string;
};

type SourceTableSummary = {
  sourceTable: string;
  count: number;
  datedCount: number;
  freshCount: number;
  freshRatio180d: number;
  uniqueHosts: number;
  latestDate?: string;
};

type SourceDiagnostics = {
  totalCount: number;
  datedCount: number;
  freshCount180d: number;
  freshRatio180d: number;
  uniqueHosts: number;
  uniqueSourceTables: number;
  byTable: SourceTableSummary[];
  observations: SourceObservation[];
};

type SynthesisPass = {
  structuralShift: string[];
  whyNow: string[];
  winners: string[];
  losers: string[];
  historicalSetup: string;
  inflectionPoint: string;
  consensusView: string;
  differentiatedView: string;
};

type MarketBeliefPass = {
  marketBelief: string[];
  underappreciated: string[];
  isMarketEarly: boolean;
};

type CapitalImplicationsPass = {
  exposureSizingLogic: string;
  equitiesCore: string[];
  equitiesSatellite: string[];
  privateMarket: string[];
  infrastructure: string[];
  optionality: string[];
  avoid: string[];
};

type ReportExhibit = {
  title: string;
  body: string[];
  takeaway: string;
};

export type InstitutionalCrossSectionReportQuality = {
  narrativeClarityScore: number;
  exhibitCount: number;
  actionabilityScore: number;
  freshness180dRatio: number;
  requiredFailures: string[];
};

export type InstitutionalCrossSectionReport = {
  markdown: string;
  quality: InstitutionalCrossSectionReportQuality;
  sourceDiagnostics: SourceDiagnostics;
  exhibits: ReportExhibit[];
};

const REVENUE_CONCEPTS = new Set([
  "Revenues",
  "RevenueFromContractWithCustomerExcludingAssessedTax",
  "SalesRevenueNet",
  "Revenue",
]);

const OPERATING_INCOME_CONCEPTS = new Set([
  "OperatingIncomeLoss",
  "ProfitLossFromOperatingActivities",
  "ProfitLoss",
]);

const CAPEX_CONCEPTS = new Set([
  "PaymentsToAcquirePropertyPlantAndEquipment",
  "CapitalExpendituresIncurredButNotYetPaid",
  "PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities",
]);

const RND_CONCEPTS = new Set(["ResearchAndDevelopmentExpense"]);

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const mean = (values: number[]): number =>
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const median = (values: number[]): number | undefined => {
  if (!values.length) return undefined;
  const sorted = [...values].sort((left, right) => left - right);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
};

const parseDateMs = (value?: string): number | undefined => {
  if (!value || !value.trim()) return undefined;
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(value.trim())
    ? `${value.trim()}T00:00:00.000Z`
    : value.trim();
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? parsed : undefined;
};

const toDateOnly = (value?: string): string | undefined => {
  if (!value?.trim()) return undefined;
  return value.trim().slice(0, 10);
};

const extractHost = (url?: string): string | undefined => {
  if (!url?.trim()) return undefined;
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return undefined;
  }
};

const fmtPct = (value?: number, digits = 1): string =>
  typeof value === "number" ? `${value.toFixed(digits)}%` : "n/a";

const fmtNum = (value?: number, digits = 2): string =>
  typeof value === "number" ? value.toFixed(digits) : "n/a";

const fmtSignedPct = (value?: number, digits = 1): string => {
  if (typeof value !== "number") return "n/a";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}%`;
};

const toUpperTickers = (tickers: string[]): string[] =>
  Array.from(new Set(tickers.map((ticker) => ticker.trim().toUpperCase()).filter(Boolean)));

const pickTop = <T>(rows: T[], count: number): T[] => rows.slice(0, Math.max(0, count));

const joinedPlaceholders = (count: number): string =>
  Array.from({ length: count }, () => "?").join(",");

const loadSourceDiagnostics = (params: {
  tickers: string[];
  dbPath?: string;
  freshDays?: number;
}): SourceDiagnostics => {
  const tickers = toUpperTickers(params.tickers);
  const db = openResearchDb(params.dbPath);
  if (!tickers.length) {
    return {
      totalCount: 0,
      datedCount: 0,
      freshCount180d: 0,
      freshRatio180d: 0,
      uniqueHosts: 0,
      uniqueSourceTables: 0,
      byTable: [],
      observations: [],
    };
  }

  const placeholders = joinedPlaceholders(tickers.length);
  const observations: SourceObservation[] = [];

  const filingRows = db
    .prepare(
      `SELECT 'filings' AS source_table,
              COALESCE(NULLIF(TRIM(f.filed), ''), NULLIF(TRIM(f.period_end), ''), NULLIF(TRIM(f.as_of_date), '')) AS event_date,
              COALESCE(NULLIF(TRIM(f.url), ''), NULLIF(TRIM(f.source_url), '')) AS source_url
       FROM filings f
       JOIN instruments i ON i.id=f.instrument_id
       WHERE UPPER(i.ticker) IN (${placeholders})
       ORDER BY f.filed DESC
       LIMIT 800`,
    )
    .all(...tickers) as Array<{ source_table: string; event_date?: string; source_url?: string }>;
  observations.push(
    ...filingRows.map((row) => ({
      sourceTable: row.source_table,
      date: toDateOnly(row.event_date),
      url: row.source_url,
      host: extractHost(row.source_url),
    })),
  );

  const transcriptRows = db
    .prepare(
      `SELECT 'transcripts' AS source_table,
              NULLIF(TRIM(t.event_date), '') AS event_date,
              NULLIF(TRIM(t.url), '') AS source_url
       FROM transcripts t
       JOIN instruments i ON i.id=t.instrument_id
       WHERE UPPER(i.ticker) IN (${placeholders})
       ORDER BY t.event_date DESC
       LIMIT 800`,
    )
    .all(...tickers) as Array<{ source_table: string; event_date?: string; source_url?: string }>;
  observations.push(
    ...transcriptRows.map((row) => ({
      sourceTable: row.source_table,
      date: toDateOnly(row.event_date),
      url: row.source_url,
      host: extractHost(row.source_url),
    })),
  );

  const expectationRows = db
    .prepare(
      `SELECT 'earnings_expectations' AS source_table,
              COALESCE(NULLIF(TRIM(e.reported_date), ''), NULLIF(TRIM(e.fiscal_date_ending), '')) AS event_date,
              NULLIF(TRIM(e.source_url), '') AS source_url
       FROM earnings_expectations e
       JOIN instruments i ON i.id=e.instrument_id
       WHERE UPPER(i.ticker) IN (${placeholders})
       ORDER BY e.reported_date DESC, e.fiscal_date_ending DESC
       LIMIT 1200`,
    )
    .all(...tickers) as Array<{ source_table: string; event_date?: string; source_url?: string }>;
  observations.push(
    ...expectationRows.map((row) => ({
      sourceTable: row.source_table,
      date: toDateOnly(row.event_date),
      url: row.source_url,
      host: extractHost(row.source_url),
    })),
  );

  const fundamentalRows = db
    .prepare(
      `SELECT 'fundamental_facts' AS source_table,
              COALESCE(NULLIF(TRIM(ff.filing_date), ''), NULLIF(TRIM(ff.as_of_date), ''), NULLIF(TRIM(ff.period_end), '')) AS event_date,
              NULLIF(TRIM(ff.source_url), '') AS source_url
       FROM fundamental_facts ff
       JOIN instruments i ON i.id=ff.instrument_id
       WHERE UPPER(i.ticker) IN (${placeholders})
         AND ff.is_latest=1
       ORDER BY ff.filing_date DESC, ff.as_of_date DESC
       LIMIT 1800`,
    )
    .all(...tickers) as Array<{ source_table: string; event_date?: string; source_url?: string }>;
  observations.push(
    ...fundamentalRows.map((row) => ({
      sourceTable: row.source_table,
      date: toDateOnly(row.event_date),
      url: row.source_url,
      host: extractHost(row.source_url),
    })),
  );

  const macroRows = db
    .prepare(
      `SELECT 'macro_factor_observations' AS source_table,
              NULLIF(TRIM(m.date), '') AS event_date,
              NULLIF(TRIM(m.source_url), '') AS source_url
       FROM macro_factor_observations m
       ORDER BY m.date DESC
       LIMIT 240`,
    )
    .all() as Array<{ source_table: string; event_date?: string; source_url?: string }>;
  observations.push(
    ...macroRows.map((row) => ({
      sourceTable: row.source_table,
      date: toDateOnly(row.event_date),
      url: row.source_url,
      host: extractHost(row.source_url),
    })),
  );

  const cutoffMs = Date.now() - Math.max(1, params.freshDays ?? 180) * 86_400_000;
  const dated = observations.filter((row) => typeof parseDateMs(row.date) === "number");
  const fresh = dated.filter((row) => (parseDateMs(row.date) ?? 0) >= cutoffMs);
  const uniqueHosts = new Set(
    observations
      .map((row) => row.host)
      .filter(Boolean as unknown as (value: string | undefined) => value is string),
  ).size;
  const uniqueTables = new Set(observations.map((row) => row.sourceTable)).size;

  const byTableMap = new Map<string, SourceObservation[]>();
  for (const row of observations) {
    const bucket = byTableMap.get(row.sourceTable) ?? [];
    bucket.push(row);
    byTableMap.set(row.sourceTable, bucket);
  }

  const byTable: SourceTableSummary[] = Array.from(byTableMap.entries())
    .map(([sourceTable, rows]) => {
      const datedRows = rows.filter((row) => typeof parseDateMs(row.date) === "number");
      const freshRows = datedRows.filter((row) => (parseDateMs(row.date) ?? 0) >= cutoffMs);
      const latestDate = rows
        .map((row) => row.date)
        .filter((value): value is string => typeof value === "string")
        .sort((left, right) => right.localeCompare(left))[0];
      return {
        sourceTable,
        count: rows.length,
        datedCount: datedRows.length,
        freshCount: freshRows.length,
        freshRatio180d: datedRows.length ? freshRows.length / datedRows.length : 0,
        uniqueHosts: new Set(
          rows
            .map((row) => row.host)
            .filter(Boolean as unknown as (value: string | undefined) => value is string),
        ).size,
        latestDate,
      };
    })
    .sort((left, right) => right.count - left.count);

  return {
    totalCount: observations.length,
    datedCount: dated.length,
    freshCount180d: fresh.length,
    freshRatio180d: dated.length ? fresh.length / dated.length : 0,
    uniqueHosts,
    uniqueSourceTables: uniqueTables,
    byTable,
    observations: observations
      .slice()
      .sort((left, right) => (right.date ?? "").localeCompare(left.date ?? "")),
  };
};

type MarginRow = {
  ticker: string;
  industry: string;
  revenue?: number;
  operatingIncome?: number;
  operatingMargin?: number;
};

const loadMarginStructure = (params: {
  constituents: CrossSectionConstituentSnapshot[];
  dbPath?: string;
}): MarginRow[] => {
  const tickers = toUpperTickers(params.constituents.map((row) => row.ticker));
  if (!tickers.length) return [];
  const db = openResearchDb(params.dbPath);
  const placeholders = joinedPlaceholders(tickers.length);
  const rows = db
    .prepare(
      `SELECT UPPER(i.ticker) AS ticker,
              COALESCE(NULLIF(TRIM(i.industry), ''), 'Unknown') AS industry,
              ff.concept,
              ff.value,
              ff.filing_date,
              ff.as_of_date
       FROM fundamental_facts ff
       JOIN instruments i ON i.id=ff.instrument_id
       WHERE UPPER(i.ticker) IN (${placeholders})
         AND ff.is_latest=1
         AND (
           (ff.taxonomy='us-gaap' AND ff.concept IN ('Revenues','RevenueFromContractWithCustomerExcludingAssessedTax','SalesRevenueNet','OperatingIncomeLoss'))
           OR (ff.taxonomy='ifrs-full' AND ff.concept IN ('Revenue','ProfitLossFromOperatingActivities','ProfitLoss'))
         )
       ORDER BY ff.filing_date DESC, ff.as_of_date DESC`,
    )
    .all(...tickers) as Array<{
    ticker: string;
    industry: string;
    concept: string;
    value: number;
    filing_date?: string;
    as_of_date?: string;
  }>;

  const bucket = new Map<string, MarginRow>();
  for (const row of rows) {
    const key = row.ticker;
    const existing = bucket.get(key) ?? { ticker: row.ticker, industry: row.industry };
    if (REVENUE_CONCEPTS.has(row.concept) && typeof existing.revenue !== "number") {
      existing.revenue = row.value;
    }
    if (
      OPERATING_INCOME_CONCEPTS.has(row.concept) &&
      typeof existing.operatingIncome !== "number"
    ) {
      existing.operatingIncome = row.value;
    }
    if (typeof existing.revenue === "number" && typeof existing.operatingIncome === "number") {
      existing.operatingMargin =
        Math.abs(existing.revenue) > 1e-9 ? existing.operatingIncome / existing.revenue : undefined;
    }
    bucket.set(key, existing);
  }
  return Array.from(bucket.values());
};

type CapitalIntensityRow = {
  ticker: string;
  capexToRevenuePct?: number;
  rndToRevenuePct?: number;
};

const loadCapitalIntensity = (params: {
  constituents: CrossSectionConstituentSnapshot[];
  dbPath?: string;
}): CapitalIntensityRow[] => {
  const tickers = toUpperTickers(params.constituents.map((row) => row.ticker));
  if (!tickers.length) return [];
  const db = openResearchDb(params.dbPath);
  const placeholders = joinedPlaceholders(tickers.length);
  const rows = db
    .prepare(
      `SELECT UPPER(i.ticker) AS ticker,
              ff.concept,
              ff.value
       FROM fundamental_facts ff
       JOIN instruments i ON i.id=ff.instrument_id
       WHERE UPPER(i.ticker) IN (${placeholders})
         AND ff.is_latest=1
         AND (
           ff.concept IN (
             'Revenues',
             'RevenueFromContractWithCustomerExcludingAssessedTax',
             'SalesRevenueNet',
             'Revenue',
             'PaymentsToAcquirePropertyPlantAndEquipment',
             'CapitalExpendituresIncurredButNotYetPaid',
             'PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities',
             'ResearchAndDevelopmentExpense'
           )
         )
       ORDER BY ff.filing_date DESC, ff.as_of_date DESC`,
    )
    .all(...tickers) as Array<{ ticker: string; concept: string; value: number }>;

  const bucket = new Map<string, { revenue?: number; capex?: number; rnd?: number }>();
  for (const row of rows) {
    const current = bucket.get(row.ticker) ?? {};
    if (REVENUE_CONCEPTS.has(row.concept) && typeof current.revenue !== "number") {
      current.revenue = row.value;
    }
    if (CAPEX_CONCEPTS.has(row.concept) && typeof current.capex !== "number") {
      current.capex = Math.abs(row.value);
    }
    if (RND_CONCEPTS.has(row.concept) && typeof current.rnd !== "number") {
      current.rnd = Math.abs(row.value);
    }
    bucket.set(row.ticker, current);
  }

  return Array.from(bucket.entries()).map(([ticker, value]) => ({
    ticker,
    capexToRevenuePct:
      typeof value.revenue === "number" &&
      Math.abs(value.revenue) > 1e-9 &&
      typeof value.capex === "number"
        ? (value.capex / Math.abs(value.revenue)) * 100
        : undefined,
    rndToRevenuePct:
      typeof value.revenue === "number" &&
      Math.abs(value.revenue) > 1e-9 &&
      typeof value.rnd === "number"
        ? (value.rnd / Math.abs(value.revenue)) * 100
        : undefined,
  }));
};

type ScenarioSummaryRow = {
  ticker: string;
  currentPrice?: number;
  bearUpsidePct?: number;
  baseUpsidePct?: number;
  bullUpsidePct?: number;
};

const loadScenarioSummary = (params: {
  constituents: CrossSectionConstituentSnapshot[];
  dbPath?: string;
  limit?: number;
}): ScenarioSummaryRow[] => {
  const topTickers = pickTop(
    params.constituents.map((row) => row.ticker).filter(Boolean),
    Math.max(1, params.limit ?? 5),
  );
  const out: ScenarioSummaryRow[] = [];
  for (const ticker of topTickers) {
    try {
      const valuation = computeValuation({ ticker, dbPath: params.dbPath });
      const bear = valuation.scenarios.find((row) => row.name === "bear");
      const base = valuation.scenarios.find((row) => row.name === "base");
      const bull = valuation.scenarios.find((row) => row.name === "bull");
      out.push({
        ticker,
        currentPrice: valuation.currentPrice,
        bearUpsidePct: typeof bear?.upsidePct === "number" ? bear.upsidePct * 100 : undefined,
        baseUpsidePct: typeof base?.upsidePct === "number" ? base.upsidePct * 100 : undefined,
        bullUpsidePct: typeof bull?.upsidePct === "number" ? bull.upsidePct * 100 : undefined,
      });
    } catch {
      out.push({ ticker });
    }
  }
  return out;
};

const inferInvestmentCall = (params: {
  breadthPositive63dPct: number;
  medianExpectedUpsidePct?: number;
  benchmarkRelativeReturnPct?: number;
  regime: string;
}): string => {
  const upside = params.medianExpectedUpsidePct ?? 0;
  const relative = params.benchmarkRelativeReturnPct ?? 0;
  if (upside >= 8 && params.breadthPositive63dPct >= 0.55 && relative >= -2) {
    return "Early-cycle earnings breadth with underpriced operating leverage and improving scenario asymmetry.";
  }
  if (upside <= -5 && params.breadthPositive63dPct <= 0.45) {
    return "Late-cycle pressure with deteriorating unit economics and unfavorable risk-reward skew.";
  }
  if (params.regime === "rotation") {
    return "Rotation regime: dispersion is creating selective alpha despite noisy top-down signals.";
  }
  return "Cross-currents regime: stay selective and underwrite to scenario math instead of narrative momentum.";
};

const buildSynthesisPass = (params: {
  label: string;
  metrics: {
    breadthPositive63dPct: number;
    medianReturn63dPct?: number;
    medianExpectedUpsidePct?: number;
    avgVariantConfidence: number;
    avgValuationConfidence: number;
    regime: string;
  };
  leaders: CrossSectionConstituentSnapshot[];
  laggards: CrossSectionConstituentSnapshot[];
  benchmarkRelativeReturnPct?: number;
}): SynthesisPass => {
  const median63 = params.metrics.medianReturn63dPct ?? 0;
  const medianUpside = params.metrics.medianExpectedUpsidePct ?? 0;
  const relative = params.benchmarkRelativeReturnPct ?? 0;
  const leaders = params.leaders.slice(0, 3).map((row) => row.ticker);
  const laggards = params.laggards.slice(0, 3).map((row) => row.ticker);
  const structuralShift = [
    `${params.label} breadth is ${(params.metrics.breadthPositive63dPct * 100).toFixed(1)}% with median 63-day return ${fmtSignedPct(median63, 1)}.`,
    `Variant-vs-valuation confidence stack is ${params.metrics.avgVariantConfidence.toFixed(2)} / ${params.metrics.avgValuationConfidence.toFixed(2)}, indicating ${params.metrics.avgVariantConfidence >= params.metrics.avgValuationConfidence ? "information edge in fundamentals" : "valuation uncertainty still dominates"}.`,
  ];
  const whyNow = [
    `Regime is ${params.metrics.regime}; this matters because dispersion and correlation structure changed vs the prior cycle and is now creating asymmetric entry points.`,
    `Median scenario upside is ${fmtSignedPct(medianUpside, 1)} and benchmark-relative return is ${fmtSignedPct(relative, 1)}, giving a real-time read of whether consensus is leaning too optimistic or too defensive.`,
  ];

  return {
    structuralShift,
    whyNow,
    winners: leaders.length
      ? leaders.map(
          (ticker) => `${ticker}: positive factor alignment and superior scenario asymmetry.`,
        )
      : ["No clear winners yet; breadth and scenario data are inconclusive."],
    losers: laggards.length
      ? laggards.map((ticker) => `${ticker}: weak scenario skew and fragile confidence stack.`)
      : ["No clear laggards yet; the drawdown map is still diffuse."],
    historicalSetup:
      "Two to three years ago this complex was mostly traded as a macro beta basket. Today, the return distribution is more idiosyncratic: the spread between top and bottom deciles is widening, and factor concentration rather than headline growth is driving outcomes.",
    inflectionPoint:
      "The inflection is the transition from narrative-led multiple expansion to cash-flow and margin accountability. As financing conditions normalized, the market moved from paying for optionality to paying for demonstrable operating conversion.",
    consensusView:
      "Consensus still anchors on a top-down growth narrative and assumes broad participation. That view underweights dispersion risk and the fact that incremental returns are now concentrated in names with durable margin pathways.",
    differentiatedView:
      "Our view is that capital should follow value-capture physics, not headline momentum. The right exposure is barbelled: own cash-generative enablers with improving operating leverage, and avoid capital-hungry business models where implied expectations already embed flawless execution.",
  };
};

const buildMarketBeliefPass = (params: {
  constituents: CrossSectionConstituentSnapshot[];
  benchmarkRelativeReturnPct?: number;
}): MarketBeliefPass => {
  const stanceCounts = new Map<string, number>();
  for (const row of params.constituents) {
    const key = row.impliedValuationStance || "insufficient-evidence";
    stanceCounts.set(key, (stanceCounts.get(key) ?? 0) + 1);
  }
  const total = Math.max(1, params.constituents.length);
  const dominant = Array.from(stanceCounts.entries()).sort((a, b) => b[1] - a[1])[0];
  const dominantShare = dominant ? (dominant[1] / total) * 100 : 0;
  const relative = params.benchmarkRelativeReturnPct ?? 0;
  const marketBelief = [
    dominant
      ? `Implied valuation stance is dominated by '${dominant[0]}' across ${dominantShare.toFixed(1)}% of the universe.`
      : "Implied valuation stance is fragmented; consensus conviction is weak.",
    `Benchmark-relative return is ${fmtSignedPct(relative, 1)}, suggesting the market is ${relative >= 0 ? "rewarding" : "discounting"} this basket vs broad risk assets.`,
  ];
  const underappreciated = [
    "The market is still underpricing second-order margin transfer inside the value chain (enablers capture more economics than end-point distributors).",
    "Consensus is extrapolating top-line vectors but underweighting capital intensity dispersion and balance-sheet survivability.",
  ];
  return {
    marketBelief,
    underappreciated,
    isMarketEarly: relative < 0 || (dominant?.[0] ?? "") === "market-too-bearish",
  };
};

const buildCapitalImplicationsPass = (params: {
  leaders: CrossSectionConstituentSnapshot[];
  laggards: CrossSectionConstituentSnapshot[];
  themeOrSector: string;
  regime: string;
}): CapitalImplicationsPass => {
  const core = params.leaders.slice(0, 3).map((row) => row.ticker);
  const lag = params.laggards.slice(0, 3).map((row) => row.ticker);
  return {
    exposureSizingLogic:
      "Size by confidence-adjusted scenario asymmetry: core risk in highest-conviction operators, satellite risk in convex catalysts, and explicit underweights where downside capture is structurally high.",
    equitiesCore: core.length
      ? core.map(
          (ticker) =>
            `${ticker}: core long/overweight candidate when spread between base and bear scenarios remains supportive.`,
        )
      : ["No core equity exposure until scenario spread quality improves."],
    equitiesSatellite: [
      "Tactical satellites around catalyst windows where probability-weighted impact is positive and correlation is manageable.",
      "Pairs and relative-value overlays to isolate idiosyncratic alpha from regime beta.",
    ],
    privateMarket: [
      `${params.themeOrSector}: prioritize companies with proven unit economics and short payback on growth spend; avoid narrative-only rounds with weak margin conversion.`,
      "Use private exposure as optionality, not as a substitute for underwriting discipline.",
    ],
    infrastructure: [
      "Own picks-and-shovels with pricing power and recurring demand sensitivity to deployment intensity.",
      `In ${params.regime} regimes, infrastructure exposures with contracted cash flows provide drawdown ballast.`,
    ],
    optionality: [
      "Use options to express event-risk asymmetry where catalysts cluster and realized vol can gap.",
      "Favor structures where downside is pre-defined and upside participates in scenario skew re-rating.",
    ],
    avoid: lag.length
      ? lag.map(
          (ticker) =>
            `${ticker}: avoid/underweight until confidence stack and scenario asymmetry improve.`,
        )
      : ["Avoid high-duration, capital-intensive names with weak free-cash conversion."],
  };
};

const buildWinnersLosersSecondOrder = (params: {
  leaders: CrossSectionConstituentSnapshot[];
  laggards: CrossSectionConstituentSnapshot[];
  sectorExposure?: Array<{ sector: string; count: number; sharePct: number }>;
}) => {
  const directWinners = params.leaders.slice(0, 3).map((row) => row.ticker);
  const disrupted = params.laggards.slice(0, 3).map((row) => row.ticker);
  const sectorLine = params.sectorExposure?.[0]
    ? `${params.sectorExposure[0].sector} now represents ${(params.sectorExposure[0].sharePct * 100).toFixed(1)}% of thematic exposure, amplifying cross-sector transmission effects.`
    : "Cross-sector spillovers are visible through changing supplier/customer economics.";
  return {
    directWinners: directWinners.length
      ? directWinners
      : ["No direct winners with sufficient confidence yet."],
    indirectWinners: [
      "Infrastructure suppliers tied to deployment intensity and throughput growth.",
      "Service layers that monetize complexity and compliance.",
    ],
    disruptedIncumbents: disrupted.length
      ? disrupted
      : ["Incumbents with rigid cost structures and declining marginal returns on capital."],
    secondOrder: [
      sectorLine,
      "Balance-sheet strength becomes a strategic moat as the cycle shifts from growth-at-any-cost to return-on-incremental-capital.",
    ],
  };
};

const buildTimelineCheckpoints = () => [
  {
    horizon: "30d",
    mustBeTrue:
      "Catalyst pipeline remains intact and no high-severity contradiction appears in filings/transcripts.",
    confirmOrDeny: "Catalyst windows, transcript tone, and estimate revision direction.",
  },
  {
    horizon: "90d",
    mustBeTrue:
      "Breadth and scenario skew remain consistent with the thesis (no sharp collapse in expected-value dispersion).",
    confirmOrDeny: "Quarterly prints, guidance revisions, and scenario re-pricing vs benchmark.",
  },
  {
    horizon: "180d",
    mustBeTrue:
      "Margin conversion and capital efficiency improve in leaders while laggards fail to close the gap.",
    confirmOrDeny:
      "Operating margin bridges, capex-to-revenue trend, and downside capture statistics.",
  },
  {
    horizon: "365d",
    mustBeTrue: "The thesis compounds through sustained alpha, not just beta drift.",
    confirmOrDeny:
      "Benchmark-relative alpha, rolling attribution stability, and realized forecast calibration.",
  },
];

const renderExhibit = (index: number, exhibit: ReportExhibit): string =>
  [
    `### Exhibit ${index}: ${exhibit.title}`,
    ...exhibit.body,
    `Takeaway: ${exhibit.takeaway}`,
    "",
  ].join("\n");

const scoreNarrativeClarity = (params: {
  synthesis: SynthesisPass;
  marketBelief: MarketBeliefPass;
  storyParagraphs: string[];
}): number => {
  const wordCount = params.storyParagraphs.join(" ").split(/\s+/).filter(Boolean).length;
  const paragraphScore = clamp01(params.storyParagraphs.length / 6);
  const depthScore = clamp01(wordCount / 900);
  const consensusDiff =
    params.synthesis.consensusView.trim().length > 40 &&
    params.synthesis.differentiatedView.trim().length > 40
      ? 1
      : 0;
  const marketBeliefCoverage =
    params.marketBelief.marketBelief.length >= 2 && params.marketBelief.underappreciated.length >= 2
      ? 1
      : 0;
  return clamp01(
    0.32 * paragraphScore + 0.28 * depthScore + 0.2 * consensusDiff + 0.2 * marketBeliefCoverage,
  );
};

const scoreActionability = (params: {
  capital: CapitalImplicationsPass;
  checkpointCount: number;
  falsifierCount: number;
}): number => {
  const exposureCoverage = [
    params.capital.equitiesCore.length,
    params.capital.equitiesSatellite.length,
    params.capital.privateMarket.length,
    params.capital.infrastructure.length,
    params.capital.optionality.length,
    params.capital.avoid.length,
  ].filter((count) => count > 0).length;
  return clamp01(
    0.42 * clamp01(exposureCoverage / 6) +
      0.28 * clamp01(params.checkpointCount / 4) +
      0.2 * clamp01(params.falsifierCount / 5) +
      0.1 * (params.capital.exposureSizingLogic.trim().length > 60 ? 1 : 0),
  );
};

const buildFalsifiers = (
  riskFlags: string[],
): Array<{ thesisRisk: string; invalidateWith: string; horizon: string }> => {
  const defaults: Array<{ thesisRisk: string; invalidateWith: string; horizon: string }> = [
    {
      thesisRisk: "Margin capture fails to materialize in expected winners.",
      invalidateWith: "Two consecutive periods of margin compression vs sector median.",
      horizon: "90-180d",
    },
    {
      thesisRisk: "Consensus was already right on valuation re-rating path.",
      invalidateWith: "No alpha after controlling for benchmark and style exposures.",
      horizon: "180-365d",
    },
    {
      thesisRisk: "Capital intensity rises faster than revenue conversion.",
      invalidateWith: "Capex/R&D intensity up while operating leverage deteriorates.",
      horizon: "90-180d",
    },
    {
      thesisRisk: "Regulatory/policy drag overwhelms operating progress.",
      invalidateWith: "Policy events create recurring downside shocks and guidance cuts.",
      horizon: "30-180d",
    },
    {
      thesisRisk: "Catalyst path slips and timeline shifts right.",
      invalidateWith: "Key catalyst windows miss with no replacement pipeline.",
      horizon: "30-90d",
    },
  ];
  if (!riskFlags.length) return defaults;
  return defaults.map((row, index) => ({
    ...row,
    thesisRisk: riskFlags[index] ?? row.thesisRisk,
  }));
};

const buildExhibits = (params: {
  label: string;
  lookbackDays: number;
  metrics: {
    breadthPositive20dPct: number;
    breadthPositive63dPct: number;
    medianReturn20dPct?: number;
    medianReturn63dPct?: number;
    medianExpectedUpsidePct?: number;
  };
  marginRows: MarginRow[];
  capitalRows: CapitalIntensityRow[];
  sourceDiagnostics: SourceDiagnostics;
  scenarioRows: ScenarioSummaryRow[];
  catalystWindows: Array<{ date: string; eventCount: number; weightedExpectedImpactBps: number }>;
  benchmarkLine?: string;
}): ReportExhibit[] => {
  const byIndustry = new Map<string, MarginRow[]>();
  for (const row of params.marginRows) {
    const bucket = byIndustry.get(row.industry) ?? [];
    bucket.push(row);
    byIndustry.set(row.industry, bucket);
  }
  const marginTableRows = Array.from(byIndustry.entries())
    .map(([industry, rows]) => ({
      industry,
      medianMarginPct: median(
        rows
          .map((row) =>
            typeof row.operatingMargin === "number" ? row.operatingMargin * 100 : undefined,
          )
          .filter((value): value is number => typeof value === "number"),
      ),
      count: rows.length,
    }))
    .sort((left, right) => (right.medianMarginPct ?? -999) - (left.medianMarginPct ?? -999))
    .slice(0, 8);

  const capexMedian = median(
    params.capitalRows
      .map((row) => row.capexToRevenuePct)
      .filter((value): value is number => typeof value === "number"),
  );
  const rndMedian = median(
    params.capitalRows
      .map((row) => row.rndToRevenuePct)
      .filter((value): value is number => typeof value === "number"),
  );

  const exhibits: ReportExhibit[] = [];

  exhibits.push({
    title: "Long-term Structural Trend (max available history)",
    body: [
      `- Coverage window: ${(params.lookbackDays / 365).toFixed(1)} years of price history across the selected universe.`,
      `- Median 63-day return: ${fmtSignedPct(params.metrics.medianReturn63dPct, 1)}.`,
      `- ${params.benchmarkLine ?? "Benchmark-relative context unavailable in this run."}`,
    ],
    takeaway:
      "The long-cycle setup is no longer uniform beta; structural dispersion is high enough to reward selective underwriting.",
  });

  exhibits.push({
    title: "Recent Acceleration vs Deceleration",
    body: [
      `| Metric | Value |`,
      `|---|---:|`,
      `| Breadth positive (20d) | ${(params.metrics.breadthPositive20dPct * 100).toFixed(1)}% |`,
      `| Breadth positive (63d) | ${(params.metrics.breadthPositive63dPct * 100).toFixed(1)}% |`,
      `| Median return (20d) | ${fmtSignedPct(params.metrics.medianReturn20dPct, 1)} |`,
      `| Median return (63d) | ${fmtSignedPct(params.metrics.medianReturn63dPct, 1)} |`,
    ],
    takeaway:
      "Short-term tape direction and medium-term participation are diverging; this is a stock-selection regime, not a blanket-beta regime.",
  });

  exhibits.push({
    title: "Margin Structure by Subsector",
    body: [
      `| Subsector | Names | Median Operating Margin |`,
      `|---|---:|---:|`,
      ...marginTableRows.map(
        (row) => `| ${row.industry} | ${row.count} | ${fmtPct(row.medianMarginPct, 1)} |`,
      ),
    ],
    takeaway:
      "Value capture is uneven across the chain; gross exposure should follow margin durability, not narrative popularity.",
  });

  exhibits.push({
    title: "Capital Flows / Capex / R&D Intensity",
    body: [
      `| Indicator | Median |`,
      `|---|---:|`,
      `| Capex / Revenue | ${fmtPct(capexMedian, 1)} |`,
      `| R&D / Revenue | ${fmtPct(rndMedian, 1)} |`,
      `| Source coverage (dated rows) | ${params.sourceDiagnostics.datedCount} |`,
    ],
    takeaway:
      "Capital intensity is the key filter for durability; prioritize businesses with disciplined reinvestment and visible payback.",
  });

  exhibits.push({
    title: "Market-Implied Expectations vs Underwriting",
    body: [
      `| Metric | Value |`,
      `|---|---:|`,
      `| Median expected upside | ${fmtSignedPct(params.metrics.medianExpectedUpsidePct, 1)} |`,
      `| Unique source hosts | ${params.sourceDiagnostics.uniqueHosts} |`,
      `| Unique source tables | ${params.sourceDiagnostics.uniqueSourceTables} |`,
    ],
    takeaway:
      "Consensus is often anchored to a single narrative source; differentiated returns require multi-source expectation mapping.",
  });

  exhibits.push({
    title: "Regulatory / Policy Backdrop Proxies",
    body: [
      `| Source Table | Rows | Fresh ≤180d | Latest Date |`,
      `|---|---:|---:|---|`,
      ...params.sourceDiagnostics.byTable
        .slice(0, 6)
        .map(
          (row) =>
            `| ${row.sourceTable} | ${row.count} | ${(row.freshRatio180d * 100).toFixed(1)}% | ${row.latestDate ?? "n/a"} |`,
        ),
    ],
    takeaway:
      "Policy and disclosure cadence is itself a signal: when fresh primary data slows, confidence should be de-levered.",
  });

  exhibits.push({
    title: "Bull vs Bear Scenario Economics",
    body: [
      `| Ticker | Bear Upside | Base Upside | Bull Upside |`,
      `|---|---:|---:|---:|`,
      ...params.scenarioRows.map(
        (row) =>
          `| ${row.ticker} | ${fmtSignedPct(row.bearUpsidePct, 1)} | ${fmtSignedPct(row.baseUpsidePct, 1)} | ${fmtSignedPct(row.bullUpsidePct, 1)} |`,
      ),
    ],
    takeaway:
      "Capital should concentrate where base-case support is strong and bull-bear spread is genuinely asymmetric.",
  });

  exhibits.push({
    title: "Catalyst Timeline and Event Density",
    body: [
      `| Window Date | Event Count | Weighted Expected Impact (bps) |`,
      `|---|---:|---:|`,
      ...params.catalystWindows
        .slice(0, 6)
        .map(
          (row) =>
            `| ${row.date} | ${row.eventCount} | ${row.weightedExpectedImpactBps.toFixed(1)} |`,
        ),
    ],
    takeaway:
      "The catalyst path is tradable only if event density and expected impact are translated into explicit timing and sizing rules.",
  });

  return exhibits;
};

const renderAppendixSources = (sourceDiagnostics: SourceDiagnostics): string[] => {
  const rows = sourceDiagnostics.observations
    .filter((row) => row.date || row.url)
    .slice(0, 24)
    .map(
      (row) =>
        `- ${row.sourceTable} | date=${row.date ?? "n/a"} | host=${row.host ?? "n/a"}${row.url ? ` | ${row.url}` : ""}`,
    );
  if (!rows.length)
    return ["- No timestamped sources found in the local research DB for this universe."];
  return rows;
};

const buildStoryParagraphs = (params: {
  synthesis: SynthesisPass;
  marketBelief: MarketBeliefPass;
  attempt?: number;
}): string[] => {
  const paragraphs = [
    `${params.synthesis.historicalSetup} ${params.synthesis.inflectionPoint}`,
    `${params.synthesis.consensusView}`,
    `${params.synthesis.differentiatedView}`,
    `${params.marketBelief.marketBelief.join(" ")}`,
    `${params.marketBelief.underappreciated.join(" ")}`,
    `What changed versus two to three years ago is not just top-line growth but the distribution of economic rents. The current setup rewards disciplined capital allocation, fast learning loops, and balance-sheet resilience more than narrative beta.`,
  ];
  if ((params.attempt ?? 1) >= 2) {
    paragraphs.push(
      "In practical terms, this is an underwriting problem first and a stock-picking problem second: rank opportunities by incremental return on incremental capital, then by valuation support under conservative assumptions.",
    );
  }
  if ((params.attempt ?? 1) >= 3) {
    paragraphs.push(
      "The key contradiction to resolve each quarter is whether guidance quality and margin conversion remain synchronized. If guidance optimism rises while cash conversion weakens, the thesis must de-risk quickly.",
    );
  }
  return paragraphs;
};

const buildCommonReport = (params: {
  reportType: "theme" | "sector";
  title: string;
  subtitleLabel: string;
  generatedAt: string;
  lookbackDays: number;
  metrics: {
    breadthPositive20dPct: number;
    breadthPositive63dPct: number;
    medianReturn20dPct?: number;
    medianReturn63dPct?: number;
    medianExpectedUpsidePct?: number;
    avgVariantConfidence: number;
    avgValuationConfidence: number;
    regime: string;
  };
  constituents: CrossSectionConstituentSnapshot[];
  leaders: CrossSectionConstituentSnapshot[];
  laggards: CrossSectionConstituentSnapshot[];
  riskFlags: string[];
  sourceDiagnostics: SourceDiagnostics;
  benchmarkLine?: string;
  benchmarkRelativeReturnPct?: number;
  sectorExposure?: Array<{ sector: string; count: number; sharePct: number }>;
  catalystWindows: Array<{ date: string; eventCount: number; weightedExpectedImpactBps: number }>;
  dbPath?: string;
  attempt?: number;
}): InstitutionalCrossSectionReport => {
  const synthesis = buildSynthesisPass({
    label: params.subtitleLabel,
    metrics: {
      breadthPositive63dPct: params.metrics.breadthPositive63dPct,
      medianReturn63dPct: params.metrics.medianReturn63dPct,
      medianExpectedUpsidePct: params.metrics.medianExpectedUpsidePct,
      avgVariantConfidence: params.metrics.avgVariantConfidence,
      avgValuationConfidence: params.metrics.avgValuationConfidence,
      regime: params.metrics.regime,
    },
    leaders: params.leaders,
    laggards: params.laggards,
    benchmarkRelativeReturnPct: params.benchmarkRelativeReturnPct,
  });

  const marketBelief = buildMarketBeliefPass({
    constituents: params.constituents,
    benchmarkRelativeReturnPct: params.benchmarkRelativeReturnPct,
  });

  const capital = buildCapitalImplicationsPass({
    leaders: params.leaders,
    laggards: params.laggards,
    themeOrSector: params.subtitleLabel,
    regime: params.metrics.regime,
  });

  const winnersLosers = buildWinnersLosersSecondOrder({
    leaders: params.leaders,
    laggards: params.laggards,
    sectorExposure: params.sectorExposure,
  });

  const marginRows = loadMarginStructure({
    constituents: params.constituents,
    dbPath: params.dbPath,
  });
  const capitalRows = loadCapitalIntensity({
    constituents: params.constituents,
    dbPath: params.dbPath,
  });
  const scenarioRows = loadScenarioSummary({
    constituents: params.constituents,
    dbPath: params.dbPath,
    limit: 6,
  });

  const exhibits = buildExhibits({
    label: params.subtitleLabel,
    lookbackDays: params.lookbackDays,
    metrics: {
      breadthPositive20dPct: params.metrics.breadthPositive20dPct,
      breadthPositive63dPct: params.metrics.breadthPositive63dPct,
      medianReturn20dPct: params.metrics.medianReturn20dPct,
      medianReturn63dPct: params.metrics.medianReturn63dPct,
      medianExpectedUpsidePct: params.metrics.medianExpectedUpsidePct,
    },
    marginRows,
    capitalRows,
    sourceDiagnostics: params.sourceDiagnostics,
    scenarioRows,
    catalystWindows: params.catalystWindows,
    benchmarkLine: params.benchmarkLine,
  });

  const storyParagraphs = buildStoryParagraphs({
    synthesis,
    marketBelief,
    attempt: params.attempt,
  });

  const falsifiers = buildFalsifiers(params.riskFlags);
  const checkpoints = buildTimelineCheckpoints();

  const investmentCall = inferInvestmentCall({
    breadthPositive63dPct: params.metrics.breadthPositive63dPct,
    medianExpectedUpsidePct: params.metrics.medianExpectedUpsidePct,
    benchmarkRelativeReturnPct: params.benchmarkRelativeReturnPct,
    regime: params.metrics.regime,
  });

  const narrativeClarityScore = scoreNarrativeClarity({
    synthesis,
    marketBelief,
    storyParagraphs,
  });
  const actionabilityScore = scoreActionability({
    capital,
    checkpointCount: checkpoints.length,
    falsifierCount: falsifiers.length,
  });
  const freshness180dRatio = params.sourceDiagnostics.freshRatio180d;

  const quality: InstitutionalCrossSectionReportQuality = {
    narrativeClarityScore,
    exhibitCount: exhibits.length,
    actionabilityScore,
    freshness180dRatio,
    requiredFailures: [
      ...(narrativeClarityScore >= 0.7 ? [] : ["narrative_clarity"]),
      ...(exhibits.length >= 8 ? [] : ["exhibit_minimums"]),
      ...(actionabilityScore >= 0.7 ? [] : ["capital_actionability"]),
      ...(freshness180dRatio >= 0.6 ? [] : ["evidence_freshness"]),
    ],
  };

  const markdown = [
    `# ${params.title}`,
    "",
    "## A. Cover",
    `- ${params.reportType === "theme" ? "Theme" : "Sector"}: ${params.subtitleLabel}`,
    `- Date: ${params.generatedAt.slice(0, 10)}`,
    `- Investment call: ${investmentCall}`,
    "",
    "## B. Executive Summary",
    "- Structural shift:",
    ...synthesis.structuralShift.map((line) => `  - ${line}`),
    "- Why now:",
    ...synthesis.whyNow.map((line) => `  - ${line}`),
    "- Winners:",
    ...synthesis.winners.map((line) => `  - ${line}`),
    "- Losers:",
    ...synthesis.losers.map((line) => `  - ${line}`),
    `- Valuation and market expectations: ${marketBelief.marketBelief[0] ?? "n/a"}`,
    `- Recommended exposure framework: ${capital.exposureSizingLogic}`,
    "",
    "## C. The Story",
    ...storyParagraphs,
    "",
    "## D. Mechanics of the Theme",
    `- Value chain margin capture: ${marginRows.length ? "Leaders demonstrate higher operating margin durability than the tail cohort." : "Insufficient margin disclosure coverage to rank value-chain capture."}`,
    `- Operating leverage vs capital intensity: median capex/revenue=${fmtPct(median(capitalRows.map((row) => row.capexToRevenuePct).filter((v): v is number => typeof v === "number")), 1)}, median R&D/revenue=${fmtPct(median(capitalRows.map((row) => row.rndToRevenuePct).filter((v): v is number => typeof v === "number")), 1)}.`,
    `- Macro sensitivity: regime=${params.metrics.regime}; fresh-source ratio=${(params.sourceDiagnostics.freshRatio180d * 100).toFixed(1)}%; benchmark context=${params.benchmarkLine ?? "n/a"}.`,
    "",
    "## E. Exhibits",
    ...exhibits.map((exhibit, index) => renderExhibit(index + 1, exhibit)),
    "## F. Winners, Losers, and Second-Order Effects",
    "- Direct beneficiaries:",
    ...winnersLosers.directWinners.map((line) => `  - ${line}`),
    "- Indirect beneficiaries:",
    ...winnersLosers.indirectWinners.map((line) => `  - ${line}`),
    "- Disrupted incumbents:",
    ...winnersLosers.disruptedIncumbents.map((line) => `  - ${line}`),
    "- Second-order impacts:",
    ...winnersLosers.secondOrder.map((line) => `  - ${line}`),
    "",
    "## G. Capital Allocation Playbook",
    "- Public equities (core):",
    ...capital.equitiesCore.map((line) => `  - ${line}`),
    "- Public equities (satellite):",
    ...capital.equitiesSatellite.map((line) => `  - ${line}`),
    "- Private markets implications:",
    ...capital.privateMarket.map((line) => `  - ${line}`),
    "- Infrastructure / picks-and-shovels:",
    ...capital.infrastructure.map((line) => `  - ${line}`),
    "- Optionality trades:",
    ...capital.optionality.map((line) => `  - ${line}`),
    "- What not to own:",
    ...capital.avoid.map((line) => `  - ${line}`),
    "",
    "## H. Risks and Falsifiers",
    ...falsifiers.map(
      (row, index) =>
        `${index + 1}. Risk: ${row.thesisRisk}\n   Invalidate with: ${row.invalidateWith}\n   Horizon: ${row.horizon}`,
    ),
    "",
    "## I. Timeline and Checkpoints",
    ...checkpoints.map(
      (row) =>
        `- ${row.horizon}: must_be_true=${row.mustBeTrue}\n  confirm_or_deny=${row.confirmOrDeny}`,
    ),
    "",
    "## J. Appendix",
    "### Source List (timestamped)",
    ...renderAppendixSources(params.sourceDiagnostics),
    "",
    "### Data Snapshot",
    `- Constituents: ${params.constituents.length}`,
    `- Leaders used in narrative: ${
      params.leaders
        .slice(0, 3)
        .map((row) => row.ticker)
        .join(", ") || "n/a"
    }`,
    `- Laggards used in narrative: ${
      params.laggards
        .slice(0, 3)
        .map((row) => row.ticker)
        .join(", ") || "n/a"
    }`,
    `- Source freshness (<=180d): ${(params.sourceDiagnostics.freshRatio180d * 100).toFixed(1)}%`,
    `- Unique hosts: ${params.sourceDiagnostics.uniqueHosts}`,
    "",
    "### Methodology Notes",
    "- Data hierarchy: primary filings/transcripts/fundamentals/expectations first, then macro/policy context.",
    "- Scenario economics sourced from valuation engine (bear/base/bull) and translated into exposure sizing logic.",
    "- Confidence is downgraded when source freshness or source diversity is weak.",
    "",
  ].join("\n");

  return {
    markdown,
    quality,
    sourceDiagnostics: params.sourceDiagnostics,
    exhibits,
  };
};

export const buildSectorInstitutionalReport = (params: {
  result: SectorResearchResult;
  dbPath?: string;
  attempt?: number;
}): InstitutionalCrossSectionReport => {
  const sourceDiagnostics = loadSourceDiagnostics({
    tickers: params.result.tickers,
    dbPath: params.dbPath,
    freshDays: 180,
  });
  const benchmarkLine = params.result.factorAttribution
    ? `Attribution alpha=${params.result.factorAttribution.annualizedAlphaPct.toFixed(1)}% ann, R2=${params.result.factorAttribution.rSquared.toFixed(2)} vs ${params.result.factorAttribution.benchmarkTicker}.`
    : undefined;

  return buildCommonReport({
    reportType: "sector",
    title: `Institutional Sector Research: ${params.result.sector}`,
    subtitleLabel: params.result.sector,
    generatedAt: params.result.generatedAt,
    lookbackDays: params.result.lookbackDays,
    metrics: {
      breadthPositive20dPct: params.result.metrics.breadthPositive20dPct,
      breadthPositive63dPct: params.result.metrics.breadthPositive63dPct,
      medianReturn20dPct: params.result.metrics.medianReturn20dPct,
      medianReturn63dPct: params.result.metrics.medianReturn63dPct,
      medianExpectedUpsidePct: params.result.metrics.medianExpectedUpsidePct,
      avgVariantConfidence: params.result.metrics.avgVariantConfidence,
      avgValuationConfidence: params.result.metrics.avgValuationConfidence,
      regime: params.result.metrics.regime,
    },
    constituents: params.result.constituents,
    leaders: params.result.leaders,
    laggards: params.result.laggards,
    riskFlags: params.result.riskFlags,
    sourceDiagnostics,
    benchmarkLine,
    catalystWindows: params.result.catalystCalendar.topEventWindows,
    dbPath: params.dbPath,
    attempt: params.attempt,
  });
};

export const buildThemeInstitutionalReport = (params: {
  result: ThemeResearchResult;
  dbPath?: string;
  attempt?: number;
}): InstitutionalCrossSectionReport => {
  const sourceDiagnostics = loadSourceDiagnostics({
    tickers: params.result.tickers,
    dbPath: params.dbPath,
    freshDays: 180,
  });

  const benchmarkLine = params.result.benchmarkRelative
    ? `Theme ${fmtSignedPct(params.result.benchmarkRelative.themeReturnPct, 1)} vs ${params.result.benchmarkRelative.benchmarkTicker} ${fmtSignedPct(params.result.benchmarkRelative.benchmarkReturnPct, 1)} (relative ${fmtSignedPct(params.result.benchmarkRelative.relativeReturnPct, 1)}).`
    : params.result.factorAttribution
      ? `Attribution alpha=${params.result.factorAttribution.annualizedAlphaPct.toFixed(1)}% ann, R2=${params.result.factorAttribution.rSquared.toFixed(2)} vs ${params.result.factorAttribution.benchmarkTicker}.`
      : undefined;

  return buildCommonReport({
    reportType: "theme",
    title: `Institutional Thematic Research: ${params.result.theme}`,
    subtitleLabel: params.result.theme,
    generatedAt: params.result.generatedAt,
    lookbackDays: params.result.lookbackDays,
    metrics: {
      breadthPositive20dPct: params.result.metrics.breadthPositive20dPct,
      breadthPositive63dPct: params.result.metrics.breadthPositive63dPct,
      medianReturn20dPct: params.result.metrics.medianReturn20dPct,
      medianReturn63dPct: params.result.metrics.medianReturn63dPct,
      medianExpectedUpsidePct: params.result.metrics.medianExpectedUpsidePct,
      avgVariantConfidence: params.result.metrics.avgVariantConfidence,
      avgValuationConfidence: params.result.metrics.avgValuationConfidence,
      regime: params.result.metrics.regime,
    },
    constituents: params.result.constituents,
    leaders: params.result.leaders,
    laggards: params.result.laggards,
    riskFlags: params.result.riskFlags,
    sourceDiagnostics,
    benchmarkLine,
    benchmarkRelativeReturnPct: params.result.benchmarkRelative?.relativeReturnPct,
    sectorExposure: params.result.sectorExposure,
    catalystWindows: params.result.catalystCalendar.topEventWindows,
    dbPath: params.dbPath,
    attempt: params.attempt,
  });
};
