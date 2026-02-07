import { createHash } from "node:crypto";
import { openResearchDb } from "./db.js";
import {
  type AdversarialDebateAssessment,
  gradeInstitutionalMemo,
  type MemoCitation,
  type MemoContradiction,
  type MemoDiagnostics,
  type MemoEvidenceClaim,
} from "./grade.js";
import { ingestExpectations, ingestFilings, ingestFundamentals, ingestPrices } from "./ingest.js";
import { enforceInstitutionalOutputGateV3 } from "./institutional-output-gate.js";
import { buildTickerPointInTimeGraph, getTickerPointInTimeSnapshot } from "./knowledge-graph.js";
import { addClaimEvidence, createResearchClaim, upsertResearchEntity } from "./memory-graph.js";
import { composePortfolioDecision } from "./portfolio-decision.js";
import { computePortfolioPlan, type PortfolioPlan } from "./portfolio.js";
import { appendProvenanceEvent } from "./provenance.js";
import { recordQualityGateRun } from "./quality-gate.js";
import { runAdversarialResearchCell } from "./research-cell.js";
import { computeValuation, recordValuationForecast, type ValuationResult } from "./valuation.js";
import { computeVariantPerception, type VariantPerceptionResult } from "./variant.js";
import { searchResearch, syncEmbeddings, type SearchHit } from "./vector-search.js";

type MemoLine = MemoEvidenceClaim;
type CitationRow = MemoCitation;
type PriceActionHorizon = {
  label: string;
  horizonDays: number;
  baseDate?: string;
  baseClose?: number;
  returnPct?: number;
};
type PriceActionSnapshot = {
  latestDate?: string;
  latestClose?: number;
  annualizedVolatility30d?: number;
  horizons: PriceActionHorizon[];
};
type FundamentalExhibitRow = {
  periodEnd: string;
  fiscalPeriod: string;
  revenue?: number;
  operatingIncome?: number;
  operatingMargin?: number;
};
type ExpectationExhibitRow = {
  fiscalDateEnding: string;
  reportedDate?: string;
  estimatedEps?: number;
  reportedEps?: number;
  surprisePct?: number;
};

const REVENUE_CONCEPTS = [
  "Revenues",
  "RevenueFromContractWithCustomerExcludingAssessedTax",
  "SalesRevenueNet",
  "Revenue",
] as const;
const OPERATING_INCOME_CONCEPTS = [
  "OperatingIncomeLoss",
  "ProfitLossFromOperatingActivities",
  "ProfitLoss",
] as const;

const toFinite = (value: unknown): number | undefined => {
  if (typeof value !== "number") return undefined;
  return Number.isFinite(value) ? value : undefined;
};

const mean = (values: number[]): number =>
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const stdev = (values: number[]): number => {
  if (values.length < 2) return 0;
  const avg = mean(values);
  const variance =
    values.reduce((sum, value) => sum + Math.pow(value - avg, 2), 0) / (values.length - 1);
  return Math.sqrt(Math.max(0, variance));
};

const toYmd = (ms: number) => new Date(ms).toISOString().slice(0, 10);

const formatPct = (value?: number, scale = 100, digits = 1): string =>
  typeof value === "number" ? `${(value * scale).toFixed(digits)}%` : "n/a";

const formatNum = (value?: number, digits = 2): string =>
  typeof value === "number" ? value.toFixed(digits) : "n/a";

const parseMetadataObject = (raw?: string): Record<string, unknown> => {
  if (!raw?.trim()) return {};
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
};

const citationDate = (citation: CitationRow): string | undefined => {
  const meta = parseMetadataObject(citation.metadata);
  const keys = [
    "reportedDate",
    "filingDate",
    "filed",
    "periodEnd",
    "asOfDate",
    "eventDate",
    "event_date",
  ];
  for (const key of keys) {
    const value = meta[key];
    if (typeof value === "string" && value.trim()) return value.trim().slice(0, 10);
  }
  return undefined;
};

const loadPriceActionSnapshot = (params: {
  ticker: string;
  dbPath?: string;
}): PriceActionSnapshot => {
  const db = openResearchDb(params.dbPath);
  const ticker = params.ticker.trim().toUpperCase();
  const rows = db
    .prepare(
      `SELECT p.date, p.close
       FROM prices p
       JOIN instruments i ON i.id=p.instrument_id
       WHERE i.ticker=?
       ORDER BY p.date DESC
       LIMIT 1800`,
    )
    .all(ticker) as Array<{ date: string; close: number }>;
  const latest = rows[0];
  const latestClose = toFinite(latest?.close);
  const horizons: PriceActionHorizon[] = [
    { label: "1Y", horizonDays: 365 },
    { label: "3Y", horizonDays: 365 * 3 },
    { label: "5Y", horizonDays: 365 * 5 },
  ];
  if (typeof latestClose === "number") {
    for (const horizon of horizons) {
      const cutoff = toYmd(Date.now() - horizon.horizonDays * 86_400_000);
      const base = rows.find((row) => row.date <= cutoff);
      const baseClose = toFinite(base?.close);
      horizon.baseDate = base?.date;
      horizon.baseClose = baseClose;
      horizon.returnPct =
        typeof baseClose === "number" && baseClose > 0 ? latestClose / baseClose - 1 : undefined;
    }
  }
  const dailyReturns = rows
    .slice(0, 31)
    .map((row, idx, arr) => {
      const next = arr[idx + 1];
      const currentClose = toFinite(row?.close);
      const priorClose = toFinite(next?.close);
      if (typeof currentClose !== "number" || typeof priorClose !== "number" || priorClose <= 0) {
        return undefined;
      }
      return currentClose / priorClose - 1;
    })
    .filter((value): value is number => typeof value === "number");
  const annualizedVolatility30d =
    dailyReturns.length >= 10 ? stdev(dailyReturns) * Math.sqrt(252) : undefined;
  return {
    latestDate: latest?.date,
    latestClose,
    annualizedVolatility30d,
    horizons,
  };
};

const loadFundamentalExhibitRows = (params: {
  ticker: string;
  dbPath?: string;
  limit?: number;
}): FundamentalExhibitRow[] => {
  const db = openResearchDb(params.dbPath);
  const ticker = params.ticker.trim().toUpperCase();
  const rows = db
    .prepare(
      `SELECT ff.period_end, ff.fiscal_period, ff.concept, ff.value
       FROM fundamental_facts ff
       JOIN instruments i ON i.id=ff.instrument_id
       WHERE i.ticker=?
         AND ff.is_latest=1
         AND (
           (ff.taxonomy='us-gaap' AND ff.concept IN ('Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet', 'OperatingIncomeLoss'))
           OR (ff.taxonomy='ifrs-full' AND ff.concept IN ('Revenue', 'ProfitLossFromOperatingActivities', 'ProfitLoss'))
         )
       ORDER BY ff.period_end DESC, ff.fiscal_period DESC, ff.filing_date DESC
       LIMIT 240`,
    )
    .all(ticker) as Array<{
    period_end: string;
    fiscal_period: string;
    concept: string;
    value: number;
  }>;
  const byPeriod = new Map<string, FundamentalExhibitRow>();
  for (const row of rows) {
    const key = `${row.period_end}|${row.fiscal_period}`;
    const current = byPeriod.get(key) ?? {
      periodEnd: row.period_end,
      fiscalPeriod: row.fiscal_period,
    };
    if (REVENUE_CONCEPTS.includes(row.concept as (typeof REVENUE_CONCEPTS)[number])) {
      current.revenue = toFinite(row.value);
    }
    if (
      OPERATING_INCOME_CONCEPTS.includes(row.concept as (typeof OPERATING_INCOME_CONCEPTS)[number])
    ) {
      current.operatingIncome = toFinite(row.value);
    }
    if (typeof current.revenue === "number" && typeof current.operatingIncome === "number") {
      current.operatingMargin =
        Math.abs(current.revenue) > 1e-9 ? current.operatingIncome / current.revenue : undefined;
    }
    byPeriod.set(key, current);
  }
  return Array.from(byPeriod.values())
    .filter((row) => typeof row.revenue === "number")
    .toSorted((a, b) =>
      `${b.periodEnd}|${b.fiscalPeriod}`.localeCompare(`${a.periodEnd}|${a.fiscalPeriod}`),
    )
    .slice(0, Math.max(2, params.limit ?? 6));
};

const loadExpectationExhibitRows = (params: {
  ticker: string;
  dbPath?: string;
  limit?: number;
}): ExpectationExhibitRow[] => {
  const db = openResearchDb(params.dbPath);
  const ticker = params.ticker.trim().toUpperCase();
  const rows = db
    .prepare(
      `SELECT fiscal_date_ending, reported_date, estimated_eps, reported_eps, surprise_pct
       FROM earnings_expectations e
       JOIN instruments i ON i.id=e.instrument_id
       WHERE i.ticker=? AND e.period_type='quarterly'
       ORDER BY CASE
         WHEN reported_date <> '' THEN reported_date
         ELSE fiscal_date_ending
       END DESC
       LIMIT ?`,
    )
    .all(ticker, Math.max(2, params.limit ?? 6)) as Array<{
    fiscal_date_ending: string;
    reported_date?: string;
    estimated_eps?: number;
    reported_eps?: number;
    surprise_pct?: number;
  }>;
  return rows.map((row) => ({
    fiscalDateEnding: row.fiscal_date_ending,
    reportedDate: row.reported_date?.trim() ? row.reported_date : undefined,
    estimatedEps: toFinite(row.estimated_eps),
    reportedEps: toFinite(row.reported_eps),
    surprisePct: toFinite(row.surprise_pct),
  }));
};

const buildActionabilityPlan = (params: {
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
  portfolioDecision: ReturnType<typeof composePortfolioDecision>;
  diagnostics: MemoDiagnostics;
}) => {
  const expectedUpside =
    params.valuation.expectedUpsideWithCatalystsPct ?? params.valuation.expectedUpsidePct;
  const entryTrigger =
    typeof expectedUpside === "number"
      ? expectedUpside >= 0
        ? `Enter only when expected upside remains >= ${(expectedUpside * 100).toFixed(1)}% and no high-severity contradiction is active.`
        : `Enter only as a tactical short when expected downside remains <= ${(expectedUpside * 100).toFixed(1)}% and contradiction count stays <= 1.`
      : "Enter only after valuation refresh yields quantified expected upside/downside and scenario pricing.";
  const sizingBands = [
    `Pilot: ${(params.portfolio.recommendedWeightPct * 0.4).toFixed(2)}% weight, risk budget ${(params.portfolio.maxRiskBudgetPct * 0.4).toFixed(2)}%.`,
    `Core: ${(params.portfolio.recommendedWeightPct * 0.7).toFixed(2)}% weight, risk budget ${(params.portfolio.maxRiskBudgetPct * 0.7).toFixed(2)}%.`,
    `Full: ${params.portfolio.recommendedWeightPct.toFixed(2)}% weight, risk budget ${params.portfolio.maxRiskBudgetPct.toFixed(2)}% (only if debate passed and no unresolved high risks).`,
  ];
  const falsificationTop3 = Array.from(new Set(params.diagnostics.falsificationTriggers))
    .slice(0, 3)
    .map((trigger, idx) => `${idx + 1}. ${trigger}`);
  while (falsificationTop3.length < 3) {
    falsificationTop3.push(
      `${falsificationTop3.length + 1}. Re-underwrite if decision score falls below ${Math.max(0.35, params.portfolioDecision.decisionScore - 0.2).toFixed(2)}.`,
    );
  }
  return {
    entryTrigger,
    sizingBands,
    riskLine: `Risk budget ${params.portfolio.maxRiskBudgetPct.toFixed(2)}% with hard stop-loss ${(params.portfolio.stopLossPct * 100).toFixed(1)}%.`,
    falsificationTop3,
  };
};

const buildDisagreementResolutions = (
  researchCell: ReturnType<typeof runAdversarialResearchCell>,
) => {
  if (!researchCell.debate.majorDisagreements.length) {
    return ["1. None material."];
  }
  const unresolved = researchCell.debate.unresolvedRisks;
  return researchCell.debate.majorDisagreements.slice(0, 4).map((item, idx) => {
    const linkedRisk = unresolved[idx] ?? unresolved[0];
    const followUp = researchCell.debate.requiredFollowUps[idx] ?? researchCell.allocator.summary;
    return `${idx + 1}. Disagreement: ${item} | Resolution path: ${followUp}${linkedRisk ? ` | Residual risk: ${linkedRisk}` : ""}`;
  });
};

const extractHost = (url?: string): string | undefined => {
  if (!url?.trim()) return undefined;
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return undefined;
  }
};

const fallbackScoreForSourceTable = (sourceTable?: string): number => {
  if (sourceTable === "fundamental_facts") return 0.95;
  if (sourceTable === "filings") return 0.92;
  if (sourceTable === "earnings_expectations") return 0.86;
  if (sourceTable === "transcripts") return 0.8;
  return 0.6;
};

const MEMO_BOOTSTRAP_ATTEMPTS = new Set<string>();

type TickerEvidenceCoverage = {
  filingsChunks: number;
  transcriptChunks: number;
  expectationChunks: number;
  fundamentalChunks: number;
  priceRows: number;
};

type MemoAutoBootstrapResult = {
  attempted: boolean;
  notes: string[];
  before: TickerEvidenceCoverage;
  after: TickerEvidenceCoverage;
};

const bootstrapKeyForTicker = (ticker: string, dbPath?: string): string =>
  `${ticker.trim().toUpperCase()}::${dbPath ?? "default"}`;

const loadTickerEvidenceCoverage = (params: {
  ticker: string;
  dbPath?: string;
}): TickerEvidenceCoverage => {
  const db = openResearchDb(params.dbPath);
  const ticker = params.ticker.trim().toUpperCase();
  const count = (sql: string): number => {
    const row = db.prepare(sql).get(ticker) as { count?: number } | undefined;
    return Number(row?.count ?? 0);
  };
  const filingsChunks = count(
    `SELECT COUNT(*) AS count
     FROM chunks c
     WHERE c.source_table='filings'
       AND c.ref_id IN (
         SELECT f.id
         FROM filings f
         JOIN instruments i ON i.id=f.instrument_id
         WHERE i.ticker=?
       )`,
  );
  const transcriptChunks = count(
    `SELECT COUNT(*) AS count
     FROM chunks c
     WHERE c.source_table='transcripts'
       AND c.ref_id IN (
         SELECT t.id
         FROM transcripts t
         JOIN instruments i ON i.id=t.instrument_id
         WHERE i.ticker=?
       )`,
  );
  const expectationChunks = count(
    `SELECT COUNT(*) AS count
     FROM chunks c
     WHERE c.source_table='earnings_expectations'
       AND c.ref_id IN (
         SELECT e.id
         FROM earnings_expectations e
         JOIN instruments i ON i.id=e.instrument_id
         WHERE i.ticker=?
       )`,
  );
  const fundamentalChunks = count(
    `SELECT COUNT(*) AS count
     FROM chunks c
     WHERE c.source_table='fundamental_facts'
       AND c.ref_id IN (
         SELECT ff.id
         FROM fundamental_facts ff
         JOIN instruments i ON i.id=ff.instrument_id
         WHERE i.ticker=?
       )`,
  );
  const priceRows = count(
    `SELECT COUNT(*) AS count
     FROM prices p
     JOIN instruments i ON i.id=p.instrument_id
     WHERE i.ticker=?`,
  );
  return {
    filingsChunks,
    transcriptChunks,
    expectationChunks,
    fundamentalChunks,
    priceRows,
  };
};

const maybeAutoBootstrapCompanyEvidence = async (params: {
  ticker: string;
  dbPath?: string;
}): Promise<MemoAutoBootstrapResult> => {
  const key = bootstrapKeyForTicker(params.ticker, params.dbPath);
  const before = loadTickerEvidenceCoverage(params);
  if (MEMO_BOOTSTRAP_ATTEMPTS.has(key)) {
    return {
      attempted: false,
      notes: ["auto-bootstrap already attempted in current process"],
      before,
      after: before,
    };
  }
  MEMO_BOOTSTRAP_ATTEMPTS.add(key);
  const notes: string[] = [];
  const userAgent =
    process.env.SEC_USER_AGENT?.trim() || process.env.SEC_EDGAR_USER_AGENT?.trim() || undefined;

  const runStep = async (label: string, fn: () => Promise<unknown>) => {
    try {
      await fn();
      notes.push(`${label}=ok`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      notes.push(`${label}=skip(${message})`);
    }
  };

  if (before.fundamentalChunks < 2) {
    await runStep("fundamentals", async () => {
      await ingestFundamentals(params.ticker, {
        userAgent,
        dbPath: params.dbPath,
      });
    });
  }
  if (before.filingsChunks < 2) {
    await runStep("filings", async () => {
      await ingestFilings(params.ticker, {
        limit: 20,
        userAgent,
        dbPath: params.dbPath,
      });
    });
  }
  if (before.expectationChunks < 1) {
    await runStep("expectations", async () => {
      await ingestExpectations(params.ticker, { dbPath: params.dbPath });
    });
  }
  if (before.priceRows < 120) {
    await runStep("prices", async () => {
      await ingestPrices(params.ticker, { dbPath: params.dbPath });
    });
  }
  await runStep("embed", async () => {
    await syncEmbeddings(params.dbPath);
  });

  const after = loadTickerEvidenceCoverage(params);
  if (!notes.length) notes.push("no bootstrap steps required");
  return {
    attempted: true,
    notes,
    before,
    after,
  };
};

const loadTickerFallbackHits = (params: {
  ticker: string;
  dbPath?: string;
  limitPerTable?: number;
  excludeIds: Set<number>;
}): SearchHit[] => {
  const db = openResearchDb(params.dbPath);
  const ticker = params.ticker.trim().toUpperCase();
  const limitPerTable = Math.max(1, params.limitPerTable ?? 2);
  const tables = ["filings", "transcripts", "earnings_expectations", "fundamental_facts"] as const;
  const out: SearchHit[] = [];

  const loaders: Record<
    (typeof tables)[number],
    () => Array<{ id: number; text: string; metadata?: string; citation_url?: string }>
  > = {
    filings: () =>
      db
        .prepare(
          `SELECT c.id, c.text, c.metadata,
                  (SELECT f.url FROM filings f WHERE f.id=c.ref_id) AS citation_url
           FROM chunks c
           WHERE c.source_table='filings'
             AND c.ref_id IN (
               SELECT f.id
               FROM filings f
               JOIN instruments i ON i.id=f.instrument_id
               WHERE i.ticker=?
             )
           ORDER BY c.id DESC
           LIMIT ?`,
        )
        .all(ticker, limitPerTable) as Array<{
        id: number;
        text: string;
        metadata?: string;
        citation_url?: string;
      }>,
    transcripts: () =>
      db
        .prepare(
          `SELECT c.id, c.text, c.metadata,
                  (SELECT t.url FROM transcripts t WHERE t.id=c.ref_id) AS citation_url
           FROM chunks c
           WHERE c.source_table='transcripts'
             AND c.ref_id IN (
               SELECT t.id
               FROM transcripts t
               JOIN instruments i ON i.id=t.instrument_id
               WHERE i.ticker=?
             )
           ORDER BY c.id DESC
           LIMIT ?`,
        )
        .all(ticker, limitPerTable) as Array<{
        id: number;
        text: string;
        metadata?: string;
        citation_url?: string;
      }>,
    earnings_expectations: () =>
      db
        .prepare(
          `SELECT c.id, c.text, c.metadata,
                  (SELECT e.source_url FROM earnings_expectations e WHERE e.id=c.ref_id) AS citation_url
           FROM chunks c
           WHERE c.source_table='earnings_expectations'
             AND c.ref_id IN (
               SELECT e.id
               FROM earnings_expectations e
               JOIN instruments i ON i.id=e.instrument_id
               WHERE i.ticker=?
             )
           ORDER BY c.id DESC
           LIMIT ?`,
        )
        .all(ticker, limitPerTable) as Array<{
        id: number;
        text: string;
        metadata?: string;
        citation_url?: string;
      }>,
    fundamental_facts: () =>
      db
        .prepare(
          `SELECT c.id, c.text, c.metadata,
                  (SELECT ff.source_url FROM fundamental_facts ff WHERE ff.id=c.ref_id) AS citation_url
           FROM chunks c
           WHERE c.source_table='fundamental_facts'
             AND c.ref_id IN (
               SELECT ff.id
               FROM fundamental_facts ff
               JOIN instruments i ON i.id=ff.instrument_id
               WHERE i.ticker=?
             )
           ORDER BY c.id DESC
           LIMIT ?`,
        )
        .all(ticker, limitPerTable) as Array<{
        id: number;
        text: string;
        metadata?: string;
        citation_url?: string;
      }>,
  };

  for (const table of tables) {
    const rows = loaders[table]();
    for (const row of rows) {
      if (params.excludeIds.has(row.id)) continue;
      params.excludeIds.add(row.id);
      out.push({
        table: "chunks",
        id: row.id,
        score: 0.2,
        vectorScore: 0,
        lexicalScore: 0,
        sourceQualityScore: fallbackScoreForSourceTable(table),
        freshnessScore: 0.5,
        text: row.text,
        meta: row.metadata,
        sourceTable: table,
        citationUrl: row.citation_url,
      });
    }
  }
  return out;
};

const selectDiverseEvidenceHits = (hits: SearchHit[], limit: number): SearchHit[] => {
  const target = Math.max(3, limit);
  const selected: SearchHit[] = [];
  const used = new Set<number>();

  const tryPick = (predicate: (hit: SearchHit) => boolean): boolean => {
    for (const hit of hits) {
      if (used.has(hit.id)) continue;
      if (!predicate(hit)) continue;
      used.add(hit.id);
      selected.push(hit);
      return true;
    }
    return false;
  };

  for (const table of ["filings", "transcripts", "fundamental_facts", "earnings_expectations"]) {
    if (selected.length >= target) break;
    tryPick((hit) => hit.sourceTable === table);
  }

  const seenHosts = new Set(
    selected.map((hit) => extractHost(hit.citationUrl)).filter((value): value is string => !!value),
  );
  while (selected.length < target) {
    const added = tryPick((hit) => {
      const host = extractHost(hit.citationUrl);
      if (!host) return false;
      if (seenHosts.has(host)) return false;
      seenHosts.add(host);
      return true;
    });
    if (!added) break;
  }

  for (const hit of hits) {
    if (selected.length >= target) break;
    if (used.has(hit.id)) continue;
    used.add(hit.id);
    selected.push(hit);
  }
  return selected;
};

const buildCitationPairs = (hits: SearchHit[]): Array<[SearchHit, SearchHit]> => {
  const pairs: Array<[SearchHit, SearchHit]> = [];
  const remaining = [...hits];

  while (remaining.length >= 2) {
    const first = remaining.shift();
    if (!first) break;
    let pairIndex = remaining.findIndex((candidate) => {
      const tableDiff = candidate.sourceTable !== first.sourceTable;
      const hostDiff =
        extractHost(candidate.citationUrl) && extractHost(first.citationUrl)
          ? extractHost(candidate.citationUrl) !== extractHost(first.citationUrl)
          : false;
      return tableDiff || hostDiff;
    });
    if (pairIndex < 0) pairIndex = 0;
    const second = remaining.splice(pairIndex, 1)[0];
    if (!second) break;
    pairs.push([first, second]);
  }
  return pairs;
};

const buildMemoLines = (params: {
  hits: SearchHit[];
  requestedEvidence: number;
  minimumClaims: number;
  enforceInstitutionalGrade: boolean;
}): MemoLine[] => {
  const lines: MemoLine[] = [];
  const selectedHits = selectDiverseEvidenceHits(
    params.hits,
    Math.min(28, Math.max(params.requestedEvidence, params.minimumClaims * 2)),
  );
  const pairs = buildCitationPairs(selectedHits);
  const usedPairKeys = new Set<string>();

  for (const [a, b] of pairs) {
    const text = [a?.text ?? "", b?.text ?? ""].join(" ").trim();
    if (!text) continue;
    const ids = [a?.id, b?.id].filter((n): n is number => typeof n === "number");
    const key = ids.toSorted((x, y) => x - y).join(":");
    if (ids.length < 2 || usedPairKeys.has(key)) continue;
    usedPairKeys.add(key);
    lines.push({ claim: summarizeText(text, 220), citationIds: ids });
  }

  if (lines.length < params.minimumClaims) {
    const fallbackHits = params.hits.slice(
      0,
      Math.min(params.hits.length, params.minimumClaims * 6),
    );
    for (let i = 0; i < fallbackHits.length && lines.length < params.minimumClaims; i += 1) {
      const a = fallbackHits[i];
      const b = fallbackHits[(i + 1) % fallbackHits.length];
      if (!a || !b || a.id === b.id) continue;
      const ids = [a.id, b.id].toSorted((x, y) => x - y);
      const key = ids.join(":");
      if (usedPairKeys.has(key)) continue;
      const text = `${a.text} ${b.text}`.trim();
      if (!text) continue;
      usedPairKeys.add(key);
      lines.push({
        claim: summarizeText(text, 220),
        citationIds: ids,
      });
    }
  }

  if (params.enforceInstitutionalGrade && lines.length < params.minimumClaims) {
    throw new Error(
      `Institutional claim floor not met: generated ${lines.length} claims, required ${params.minimumClaims}.`,
    );
  }
  if (lines.length < 2 || lines.some((line) => line.citationIds.length === 0)) {
    throw new Error("Citation enforcement failed: every claim must map to evidence");
  }
  return lines;
};

const loadCitationsForLines = (params: {
  db: ReturnType<typeof openResearchDb>;
  lines: MemoLine[];
}): CitationRow[] => {
  const citationIds = params.lines.flatMap((line) => line.citationIds);
  if (!citationIds.length) return [];
  return params.db
    .prepare(
      `SELECT c.id, c.source_table, c.ref_id, c.metadata,
              CASE
                WHEN c.source_table='filings' THEN (SELECT url FROM filings WHERE id=c.ref_id)
                WHEN c.source_table='transcripts' THEN (SELECT url FROM transcripts WHERE id=c.ref_id)
                WHEN c.source_table='fundamental_facts' THEN (SELECT source_url FROM fundamental_facts WHERE id=c.ref_id)
                WHEN c.source_table='earnings_expectations' THEN (SELECT source_url FROM earnings_expectations WHERE id=c.ref_id)
                ELSE NULL
              END AS url
       FROM chunks c
       WHERE c.id IN (${citationIds.map(() => "?").join(",")})`,
    )
    .all(...citationIds) as CitationRow[];
};

const buildDiagnostics = (params: {
  variant: VariantPerceptionResult;
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
}): MemoDiagnostics => {
  const contradictions: MemoContradiction[] = [];
  const impliedStance = params.valuation.impliedExpectations?.stance;
  const expectedUpside =
    params.valuation.expectedUpsideWithCatalystsPct ?? params.valuation.expectedUpsidePct;

  if (params.variant.stance === "positive-variant" && impliedStance === "market-too-bullish") {
    contradictions.push({
      severity: "medium",
      detail: "Variant signal is positive while valuation implies market is already too bullish.",
    });
  }
  if (params.variant.stance === "negative-variant" && impliedStance === "market-too-bearish") {
    contradictions.push({
      severity: "medium",
      detail: "Variant signal is negative while valuation implies market is already too bearish.",
    });
  }
  if (typeof expectedUpside === "number") {
    if (expectedUpside >= 0.15 && params.portfolio.stance === "short") {
      contradictions.push({
        severity: "high",
        detail: "Portfolio stance is short despite strongly positive expected upside.",
      });
    }
    if (expectedUpside <= -0.15 && params.portfolio.stance === "long") {
      contradictions.push({
        severity: "high",
        detail: "Portfolio stance is long despite strongly negative expected upside.",
      });
    }
  }
  if (params.valuation.confidence < 0.5 && params.portfolio.recommendedWeightPct >= 5) {
    contradictions.push({
      severity: "medium",
      detail: "Position size appears too large for current valuation confidence.",
    });
  }

  const falsificationTriggers: string[] = [];
  const avgSurprise = params.variant.metrics.avgSurprisePct;
  const estimateTrend = params.variant.metrics.estimateTrend;
  const revenueGrowth = params.variant.metrics.revenueGrowth;
  const marginDelta = params.variant.metrics.marginDelta;

  if (typeof avgSurprise === "number") {
    falsificationTriggers.push(
      `Two consecutive quarterly EPS surprises below ${Math.max(-12, avgSurprise - 6).toFixed(1)}%.`,
    );
  }
  if (typeof estimateTrend === "number") {
    falsificationTriggers.push(
      `Consensus estimate trend drops below ${Math.min(-0.05, estimateTrend - 0.05).toFixed(3)}.`,
    );
  }
  if (typeof revenueGrowth === "number") {
    falsificationTriggers.push(
      `Revenue growth signal falls below ${Math.min(-0.05, revenueGrowth - 0.05).toFixed(3)}.`,
    );
  }
  if (typeof marginDelta === "number") {
    falsificationTriggers.push(
      `Operating margin delta drops below ${Math.min(-0.02, marginDelta - 0.02).toFixed(3)}.`,
    );
  }
  if (typeof expectedUpside === "number") {
    if (expectedUpside >= 0) {
      falsificationTriggers.push("Expected upside turns negative on refreshed valuation.");
    } else {
      falsificationTriggers.push(
        "Expected downside closes to within +/-2% on refreshed valuation.",
      );
    }
  }
  falsificationTriggers.push(
    `Stop-loss breach at ${(params.portfolio.stopLossPct * 100).toFixed(1)}% from entry.`,
  );

  return {
    contradictions,
    falsificationTriggers: Array.from(new Set(falsificationTriggers)),
  };
};

export const generateMemoAsync = async (params: {
  ticker: string;
  question: string;
  dbPath?: string;
  maxEvidence?: number;
  enforceInstitutionalGrade?: boolean;
  minQualityScore?: number;
}) => {
  const enforceInstitutionalGrade = params.enforceInstitutionalGrade ?? true;
  const minQualityScore = params.minQualityScore ?? 0.8;
  const requestedEvidence = params.maxEvidence ?? 12;
  const minimumClaims = enforceInstitutionalGrade ? 7 : 4;
  const loadEvidenceHits = async () => {
    const primaryHits = await searchResearch({
      query: `${params.ticker} ${params.question}`,
      ticker: params.ticker,
      limit: Math.max(16, requestedEvidence * 3),
      dbPath: params.dbPath,
      source: "research",
    });
    const excludeIds = new Set(primaryHits.map((hit) => hit.id));
    const fallbackHits = loadTickerFallbackHits({
      ticker: params.ticker,
      dbPath: params.dbPath,
      limitPerTable: 3,
      excludeIds,
    });
    return {
      primaryHits,
      fallbackHits,
      hits: [...primaryHits, ...fallbackHits],
    };
  };
  const db = openResearchDb(params.dbPath);
  let { primaryHits, fallbackHits, hits } = await loadEvidenceHits();
  let bootstrapResult: MemoAutoBootstrapResult | undefined;
  if (hits.length < 3) {
    bootstrapResult = await maybeAutoBootstrapCompanyEvidence({
      ticker: params.ticker,
      dbPath: params.dbPath,
    });
    const refreshed = await loadEvidenceHits();
    primaryHits = refreshed.primaryHits;
    fallbackHits = refreshed.fallbackHits;
    hits = refreshed.hits;
  }
  if (hits.length < 3) {
    const bootstrapDetail = bootstrapResult
      ? ` bootstrap_steps=${bootstrapResult.notes.join(", ")} before=${JSON.stringify(bootstrapResult.before)} after=${JSON.stringify(bootstrapResult.after)}`
      : "";
    throw new Error(
      `Insufficient evidence: fewer than 3 retrieved citations (search_hits=${primaryHits.length}, fallback_hits=${fallbackHits.length}).${bootstrapDetail}`,
    );
  }
  const lines = buildMemoLines({
    hits,
    requestedEvidence,
    minimumClaims,
    enforceInstitutionalGrade,
  });
  const citations = loadCitationsForLines({ db, lines });
  const byId = new Map(citations.map((c) => [c.id, c]));
  const variant = computeVariantPerception({
    ticker: params.ticker,
    dbPath: params.dbPath,
  });
  const valuation = computeValuation({
    ticker: params.ticker,
    dbPath: params.dbPath,
  });
  const portfolio = computePortfolioPlan({
    ticker: params.ticker,
    dbPath: params.dbPath,
  });
  const diagnostics = buildDiagnostics({
    variant,
    valuation,
    portfolio,
  });
  const graphBuild = buildTickerPointInTimeGraph({
    ticker: params.ticker,
    dbPath: params.dbPath,
    maxFundamentalFacts: 400,
    maxExpectations: 140,
    maxFilings: 120,
    maxTranscripts: 100,
    maxCatalysts: 100,
  });
  const graphSnapshot = getTickerPointInTimeSnapshot({
    ticker: params.ticker,
    dbPath: params.dbPath,
    lookbackDays: 730,
    eventLimit: 24,
    factLimit: 240,
    metricLimit: 16,
  });
  const researchCell = runAdversarialResearchCell({
    ticker: params.ticker,
    question: params.question,
    claims: lines,
    citations,
    variant,
    valuation,
    portfolio,
    diagnostics,
    graphSnapshot,
  });
  const researchCellAssessment: AdversarialDebateAssessment = {
    coverageScore: researchCell.debate.adversarialCoverageScore,
    dissentCount: researchCell.debate.majorDisagreements.length,
    disconfirmingEvidenceCount: researchCell.debate.disconfirmingEvidence.length,
    riskControlCount: researchCell.debate.riskControls.length,
    unresolvedRiskCount: researchCell.debate.unresolvedRisks.length,
    finalStance: researchCell.allocator.finalStance,
    finalConfidence: researchCell.allocator.confidence,
    passed: researchCell.debate.passed,
  };
  const portfolioDecision = composePortfolioDecision({
    ticker: params.ticker,
    question: params.question,
    portfolio,
    valuation,
    variant,
    researchCell,
  });
  const priceAction = loadPriceActionSnapshot({
    ticker: params.ticker,
    dbPath: params.dbPath,
  });
  const fundamentalExhibit = loadFundamentalExhibitRows({
    ticker: params.ticker,
    dbPath: params.dbPath,
    limit: 6,
  });
  const expectationExhibit = loadExpectationExhibitRows({
    ticker: params.ticker,
    dbPath: params.dbPath,
    limit: 6,
  });
  const actionabilityPlan = buildActionabilityPlan({
    valuation,
    portfolio,
    portfolioDecision,
    diagnostics,
  });
  const disagreementResolutions = buildDisagreementResolutions(researchCell);
  const expectedUpsidePct = valuation.expectedUpsideWithCatalystsPct ?? valuation.expectedUpsidePct;
  const baseScenario = valuation.scenarios.find((scenario) => scenario.name === "base");
  const primaryDisconfirmers = researchCell.debate.disconfirmingEvidence.slice(0, 3);
  const primaryDisagreements = disagreementResolutions.slice(0, 3);
  const topFalsification = diagnostics.falsificationTriggers.slice(0, 3);

  const generatedOn = new Date().toISOString().slice(0, 10);
  const storyMarketLine = valuation.impliedExpectations
    ? `Consensus pricing implies revenue growth ${(valuation.impliedExpectations.impliedRevenueGrowth * 100).toFixed(1)}% and operating margin ${(valuation.impliedExpectations.impliedOperatingMargin * 100).toFixed(1)}%, while our base underwriting uses ${(valuation.impliedExpectations.modelRevenueGrowth * 100).toFixed(1)}% growth and ${(valuation.impliedExpectations.modelOperatingMargin * 100).toFixed(1)}% margin.`
    : "Consensus-implied expectations are incomplete in the current dataset; confidence is reduced until that gap is closed.";
  const catalystDate = valuation.catalystSummary?.nearestCatalystDate ?? "n/a";
  const catalystImpact = valuation.catalystSummary?.expectedImpactPct;

  const memo = [
    `# Institutional Single-Name Research: ${params.ticker.toUpperCase()}`,
    ``,
    `## A. Cover`,
    `- Ticker: ${params.ticker.toUpperCase()}`,
    `- Date: ${generatedOn}`,
    `- Investment question: ${params.question}`,
    `- One-line call: ${portfolioDecision.finalStance.toUpperCase()} bias with ${portfolioDecision.recommendation.toUpperCase()} deployment until catalyst path and confidence improve.`,
    ``,
    `## B. Exec Summary`,
    `- Structural shift: Variant gap is ${variant.variantGapScore.toFixed(2)} with fundamental score ${variant.fundamentalScore.toFixed(2)} versus expectation score ${variant.expectationScore.toFixed(2)}.`,
    `- Why now: Base scenario implies ${formatNum(baseScenario?.impliedSharePrice)} versus spot ${formatNum(valuation.currentPrice)}, forcing immediate expectation reset work.`,
    `- Market belief vs our view: ${storyMarketLine}`,
    `- Decision and confidence: ${portfolioDecision.recommendation} | score ${portfolioDecision.decisionScore.toFixed(2)} | confidence ${portfolioDecision.confidence.toFixed(2)}.`,
    `- Deployment stance today: ${portfolio.stance} with max risk budget ${portfolio.maxRiskBudgetPct.toFixed(2)}% and stop ${formatPct(portfolio.stopLossPct)}.`,
    ``,
    `## C. The Story`,
    `Two to three years ago, this name was primarily a growth narrative. The current setup is different: valuation is now being set by the market's implied terminal assumptions versus observed margin conversion and risk-adjusted scenario economics.`,
    `Our variant is not based on a single data point. It comes from the joint read of filing-grounded fundamentals, expectations trend quality, and scenario pricing dispersion. Where that stack is thin, we explicitly de-rate confidence and sizing.`,
    `Why now is timing plus asymmetry: the next catalyst window (${catalystDate}) arrives while expected return under base assumptions remains ${formatPct(expectedUpsidePct)}. That combination requires disciplined capital deployment, not narrative conviction.`,
    ``,
    `## D. Business Model & Value Drivers`,
    `- Revenue and operating leverage trajectory:`,
    `| Period | Fiscal | Revenue | Operating Income | Operating Margin |`,
    `|---|---|---:|---:|---:|`,
    ...(fundamentalExhibit.length
      ? fundamentalExhibit.map(
          (row) =>
            `| ${row.periodEnd} | ${row.fiscalPeriod} | ${formatNum(row.revenue, 0)} | ${formatNum(row.operatingIncome, 0)} | ${formatPct(row.operatingMargin)} |`,
        )
      : ["| n/a | n/a | n/a | n/a | n/a |"]),
    `- Core value drivers: customer growth quality, margin conversion, and loss/credit-cost discipline in stressed scenarios.`,
    ``,
    `## E. Variant View / Debate`,
    `- Consensus framing: ${valuation.impliedExpectations?.stance ?? "insufficient-evidence"}`,
    `- Our variant stance: ${variant.stance}`,
    `- Debate outcome: passed=${researchCell.debate.passed ? 1 : 0}, coverage=${researchCell.debate.adversarialCoverageScore.toFixed(2)}, consensus=${researchCell.debate.consensusScore.toFixed(2)}.`,
    ...primaryDisagreements.map((line) => `- ${line}`),
    ``,
    `## F. Valuation`,
    `- Driver model cross-check:`,
    `  - Expected upside (with catalysts): ${formatPct(expectedUpsidePct)}`,
    `  - Catalyst expected impact: ${formatPct(catalystImpact)}`,
    `  - Implied-vs-model gap: ${valuation.impliedExpectations ? `growth gap ${(valuation.impliedExpectations.growthGap * 100).toFixed(1)}%, margin gap ${(valuation.impliedExpectations.marginGap * 100).toFixed(1)}%` : "insufficient evidence"}`,
    `- Scenario pricing table:`,
    `| Scenario | Probability | Revenue Growth | Operating Margin | WACC | Implied Price | Upside |`,
    `|---|---:|---:|---:|---:|---:|---:|`,
    ...valuation.scenarios.map(
      (scenario) =>
        `| ${scenario.name.toUpperCase()} | ${formatPct(scenario.probability)} | ${formatPct(scenario.revenueGrowth)} | ${formatPct(scenario.operatingMargin)} | ${formatPct(scenario.wacc)} | ${formatNum(scenario.impliedSharePrice)} | ${formatPct(scenario.upsidePct)} |`,
    ),
    ``,
    `## G. Exhibits`,
    `### Exhibit 1: Long-Term Operating Trend`,
    `| Period | Revenue | Operating Margin |`,
    `|---|---:|---:|`,
    ...(fundamentalExhibit.length
      ? fundamentalExhibit.map(
          (row) =>
            `| ${row.periodEnd} | ${formatNum(row.revenue, 0)} | ${formatPct(row.operatingMargin)} |`,
        )
      : ["| n/a | n/a | n/a |"]),
    `Takeaway: Operating trend quality, not top-line alone, drives sustainable compounding.`,
    ``,
    `### Exhibit 2: Scenario Economics`,
    `| Scenario | Implied Price | Upside |`,
    `|---|---:|---:|`,
    ...valuation.scenarios.map(
      (scenario) =>
        `| ${scenario.name.toUpperCase()} | ${formatNum(scenario.impliedSharePrice)} | ${formatPct(scenario.upsidePct)} |`,
    ),
    `Takeaway: Current pricing requires assumptions that are richer than base underwriting.`,
    ``,
    `### Exhibit 3: Market Tape`,
    `- Latest close: ${formatNum(priceAction.latestClose)} (${priceAction.latestDate ?? "n/a"})`,
    `- 30D annualized volatility: ${formatPct(priceAction.annualizedVolatility30d)}`,
    `| Horizon | Base Date | Base Price | Return |`,
    `|---|---|---:|---:|`,
    ...priceAction.horizons.map(
      (horizon) =>
        `| ${horizon.label} | ${horizon.baseDate ?? "n/a"} | ${formatNum(horizon.baseClose)} | ${formatPct(horizon.returnPct)} |`,
    ),
    `Takeaway: Tape performance is strong, but volatility and valuation spread require tighter sizing discipline.`,
    ``,
    `### Exhibit 4: Expectations and Surprise Path`,
    `| Fiscal Date | Reported Date | Est EPS | Reported EPS | Surprise % |`,
    `|---|---|---:|---:|---:|`,
    ...(expectationExhibit.length
      ? expectationExhibit.map(
          (row) =>
            `| ${row.fiscalDateEnding} | ${row.reportedDate ?? "n/a"} | ${formatNum(row.estimatedEps, 3)} | ${formatNum(row.reportedEps, 3)} | ${formatPct(typeof row.surprisePct === "number" ? row.surprisePct / 100 : undefined)} |`,
        )
      : ["| n/a | n/a | n/a | n/a | n/a |"]),
    `Takeaway: Expectation quality is still thin; confidence should stay discounted until quarterly depth improves.`,
    ``,
    `### Exhibit 5: Disconfirming Evidence Stack`,
    ...(primaryDisconfirmers.length
      ? primaryDisconfirmers.map((item, index) => `${index + 1}. ${item}`)
      : ["1. None captured."]),
    `Takeaway: The thesis must survive active disconfirmation, not only confirming signals.`,
    ``,
    `### Exhibit 6: Catalyst Timeline`,
    `- Open catalysts: ${valuation.catalystSummary?.openCount ?? 0}`,
    `- Weighted expected impact: ${formatPct(catalystImpact)}`,
    `- Nearest catalyst date: ${catalystDate}`,
    `Takeaway: Timing and impact mapping turn research into executable risk-adjusted deployment.`,
    ``,
    `## H. Risks / Kill Shots + Monitoring`,
    `- High/medium contradictions: ${diagnostics.contradictions.length} total (${diagnostics.contradictions.filter((item) => item.severity === "high").length} high).`,
    ...topFalsification.map((trigger, index) => `${index + 1}. ${trigger}`),
    `- Monitoring dashboard: revisions trend, margin delta, scenario repricing, and contradiction count.`,
    ``,
    `## I. Catalysts & Timeline`,
    `- 30d: confirm guidance quality and contradiction count <= 1.`,
    `- 90d: validate scenario skew versus realized prints and revisions.`,
    `- 180d: confirm margin conversion and loss/credit-cost discipline.`,
    `- 365d: require sustained alpha versus risk-adjusted benchmark expectations.`,
    ``,
    `## J. Positioning & Risk Controls`,
    `- Recommendation: ${portfolioDecision.recommendation}`,
    `- Entry trigger: ${actionabilityPlan.entryTrigger}`,
    ...actionabilityPlan.sizingBands.map((line, index) => `${index + 1}. ${line}`),
    `- Risk controls: ${actionabilityPlan.riskLine}`,
    `- Risk breaches: ${portfolioDecision.riskBreaches.length}`,
    ...(portfolioDecision.riskBreaches.length
      ? portfolioDecision.riskBreaches.map((issue, index) => `  ${index + 1}. ${issue}`)
      : ["  1. None detected."]),
    ``,
    `## K. Appendix`,
    `### Source List (timestamped)`,
    ...citations.map(
      (c) =>
        `- C${c.id}: source=${c.source_table} ref=${c.ref_id}${citationDate(c) ? ` date=${citationDate(c)}` : ""}${extractHost(c.url) ? ` host=${extractHost(c.url)}` : ""}${c.url ? ` | ${c.url}` : ""}`,
    ),
    ``,
    `### Data Snapshot`,
    `- Graph snapshot as-of: ${graphSnapshot.asOfDate}; events=${graphSnapshot.events.length}; facts=${graphSnapshot.facts.length}; scanned_rows=${graphBuild.rowsScanned}`,
    `- Variant confidence: ${variant.confidence.toFixed(2)} | Valuation confidence: ${valuation.confidence.toFixed(2)} | Portfolio confidence: ${portfolio.confidence.toFixed(2)}`,
    ...(variant.notes.length ? variant.notes.map((note) => `- Note: ${note}`) : []),
    ...(valuation.notes.length ? valuation.notes.map((note) => `- Note: ${note}`) : []),
    ``,
    `### Methodology Notes`,
    `- Evidence hierarchy: SEC filings > transcripts > expectations > fundamentals > market tape.`,
    `- Scenario economics are generated from bear/base/bull drivers and cross-checked with implied expectations.`,
    `- Confidence is reduced when expectations depth or source freshness is limited.`,
  ].join("\n");

  const quality = gradeInstitutionalMemo({
    hitsCount: hits.length,
    claims: lines,
    citations,
    variant,
    valuation,
    portfolio,
    diagnostics,
    researchCell: researchCellAssessment,
    minScore: minQualityScore,
    dbPath: params.dbPath,
  });

  const memoGateArtifactId = `${params.ticker.toUpperCase()}:${createHash("sha256")
    .update(
      JSON.stringify({
        ticker: params.ticker.toUpperCase(),
        question: params.question,
        claims: lines.map((line) => line.claim),
        citationIds: lines.flatMap((line) => line.citationIds),
      }),
    )
    .digest("hex")
    .slice(0, 16)}`;
  const outputGate = enforceInstitutionalOutputGateV3({
    kind: "memo",
    artifactId: memoGateArtifactId,
    markdown: memo,
    minScore: minQualityScore,
    maxRepairPasses: 5,
    sources: citations.map((citation) => ({
      sourceTable: citation.source_table,
      date: citationDate(citation),
      url: citation.url,
      host: extractHost(citation.url),
    })),
  });
  const qualityGate = outputGate.evaluation;
  const finalMemo = outputGate.markdown;
  const qualityGateRun = recordQualityGateRun({
    evaluation: qualityGate,
    metadata: {
      ticker: params.ticker.toUpperCase(),
      question: params.question,
      enforce_institutional_grade: enforceInstitutionalGrade,
      output_gate_repairs: outputGate.repairs,
      output_gate_hard_fails: qualityGate.hardFails,
    },
    dbPath: params.dbPath,
  });

  if (enforceInstitutionalGrade && !qualityGate.passed) {
    const failed = qualityGate.checks.filter((c) => !c.passed);
    const details = [
      ...failed.map((f) => `${f.name}: ${f.detail}`),
      ...(qualityGate.hardFails.length ? [`hard_fails=${qualityGate.hardFails.join(",")}`] : []),
    ].join("; ");
    const requiredFailures =
      qualityGate.requiredFailures.length > 0
        ? ` required_failures=${qualityGate.requiredFailures.join(",")};`
        : "";
    const comparator = qualityGate.score < minQualityScore ? "<" : ">=";
    throw new Error(
      `Institutional-grade quality gate failed (score=${qualityGate.score.toFixed(2)} ${comparator} ${minQualityScore.toFixed(2)};${requiredFailures} run_id=${qualityGateRun.id}): ${details}`,
    );
  }

  const entity = upsertResearchEntity({
    kind: "company",
    canonicalName: params.ticker.toUpperCase(),
    ticker: params.ticker,
    metadata: {
      source: "memo",
      question: params.question,
    },
    dbPath: params.dbPath,
  });
  const claimIds: number[] = [];
  for (const line of lines) {
    const claim = createResearchClaim({
      entityId: entity.id,
      claimText: line.claim,
      claimType: "memo_claim",
      confidence: quality.score,
      validFrom: new Date().toISOString().slice(0, 10),
      status: quality.passed ? "active" : "draft",
      metadata: {
        question: params.question,
        quality_score: quality.score,
        citation_ids: line.citationIds,
      },
      dbPath: params.dbPath,
    });
    claimIds.push(claim.id);
    for (const citationId of line.citationIds) {
      const citation = byId.get(citationId);
      if (!citation) continue;
      addClaimEvidence({
        claimId: claim.id,
        sourceTable: citation.source_table,
        refId: citation.ref_id,
        citationUrl: citation.url,
        excerptText: line.claim,
        metadata: {
          chunk_id: citation.id,
          chunk_metadata: citation.metadata ?? "",
        },
        dbPath: params.dbPath,
      });
    }
  }

  let forecastId: number | undefined;
  if (
    typeof valuation.currentPrice === "number" &&
    typeof valuation.currentPriceDate === "string" &&
    typeof valuation.expectedUpsideWithCatalystsPct === "number"
  ) {
    forecastId = recordValuationForecast({
      ticker: params.ticker,
      predictedReturn: valuation.expectedUpsideWithCatalystsPct,
      startPrice: valuation.currentPrice,
      basePriceDate: valuation.currentPriceDate,
      source: "memo",
      dbPath: params.dbPath,
    });
  }

  const memoHash = createHash("sha256").update(finalMemo).digest("hex");
  try {
    appendProvenanceEvent({
      eventType: "memo_deliverable",
      entityType: "research_memo",
      entityId: `${params.ticker.toUpperCase()}:${forecastId ?? "na"}:${memoHash.slice(0, 12)}`,
      payload: {
        ticker: params.ticker.toUpperCase(),
        question: params.question,
        memo_hash: memoHash,
        claims: lines.length,
        citations: citations.length,
        quality_score: quality.score,
        quality_passed: quality.passed,
        required_failures: quality.requiredFailures,
        institutional_gate: {
          run_id: qualityGateRun.id,
          gate_name: qualityGate.gateName,
          artifact_type: qualityGate.artifactType,
          artifact_id: qualityGate.artifactId,
          score: qualityGate.score,
          min_score: qualityGate.minScore,
          passed: qualityGate.passed,
          required_failures: qualityGate.requiredFailures,
        },
        actionability_score: quality.actionabilityScore,
        adversarial_coverage_score: quality.adversarialCoverageScore,
        research_cell: {
          coverage_score: researchCell.debate.adversarialCoverageScore,
          consensus_score: researchCell.debate.consensusScore,
          passed: researchCell.debate.passed,
          final_stance: researchCell.allocator.finalStance,
          final_confidence: researchCell.allocator.confidence,
          recommended_weight_pct: researchCell.allocator.recommendedWeightPct,
          max_risk_budget_pct: researchCell.allocator.maxRiskBudgetPct,
          disconfirming_evidence_count: researchCell.debate.disconfirmingEvidence.length,
          major_disagreements_count: researchCell.debate.majorDisagreements.length,
          unresolved_risks_count: researchCell.debate.unresolvedRisks.length,
        },
        portfolio_decision: {
          recommendation: portfolioDecision.recommendation,
          final_stance: portfolioDecision.finalStance,
          decision_score: portfolioDecision.decisionScore,
          confidence: portfolioDecision.confidence,
          expected_return_pct: portfolioDecision.expectedReturnPct,
          downside_risk_pct: portfolioDecision.downsideRiskPct,
          risk_breaches: portfolioDecision.riskBreaches,
          size_candidates: portfolioDecision.sizeCandidates.map((candidate) => ({
            label: candidate.label,
            recommendation: candidate.recommendation,
            weight_pct: candidate.weightPct,
            risk_budget_pct: candidate.riskBudgetPct,
            expected_pnl_pct: candidate.expectedPnlPct,
            downside_pnl_pct: candidate.downsidePnlPct,
            score: candidate.score,
          })),
        },
        point_in_time_graph: {
          snapshot_as_of: graphSnapshot.asOfDate,
          rows_scanned: graphBuild.rowsScanned,
          events: graphSnapshot.events.length,
          facts: graphSnapshot.facts.length,
          metrics: graphSnapshot.metrics.length,
        },
        calibration: {
          mode: quality.calibration.mode,
          score: quality.calibration.score,
          sample_count: quality.calibration.sampleCount,
          mae: quality.calibration.mae ?? null,
          directional_accuracy: quality.calibration.directionalAccuracy ?? null,
          confidence_outcome_mae: quality.calibration.confidenceOutcomeMae ?? null,
        },
        forecast_id: forecastId ?? null,
        claim_ids: claimIds,
      },
      metadata: {
        min_quality_score: minQualityScore,
        enforce_institutional_grade: enforceInstitutionalGrade,
      },
      dbPath: params.dbPath,
    });
  } catch {
    // Preserve memo path even if provenance persistence fails.
  }

  return {
    memo: finalMemo,
    citations: citations.length,
    claims: lines.length,
    quality,
    qualityGate,
    qualityGateRunId: qualityGateRun.id,
    variant,
    valuation,
    portfolio,
    diagnostics,
    researchCell,
    portfolioDecision,
    forecastId,
  };
};

const summarizeText = (text: string, maxChars: number): string => {
  const cleaned = text.replace(/\s+/g, " ").trim();
  if (cleaned.length <= maxChars) return cleaned;
  const clipped = cleaned.slice(0, maxChars);
  const lastSpace = clipped.lastIndexOf(" ");
  return `${clipped.slice(0, Math.max(0, lastSpace))}...`;
};
