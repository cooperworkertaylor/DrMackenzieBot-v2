import { openResearchDb } from "./db.js";
import { computeValuation } from "./valuation.js";
import { computeVariantPerception } from "./variant.js";

type AlertSeverity = "high" | "medium" | "low";

export type ThesisAlert = {
  ticker: string;
  severity: AlertSeverity;
  alertType: string;
  message: string;
  details: string;
};

export type ThesisMonitorResult = {
  ticker: string;
  checkedAt: string;
  alerts: ThesisAlert[];
  persisted: number;
};

const normalizeTicker = (ticker: string) => ticker.trim().toUpperCase();

const toFinite = (value: unknown): number | undefined => {
  if (typeof value !== "number") return undefined;
  return Number.isFinite(value) ? value : undefined;
};

const mean = (values: number[]): number =>
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const riskDensity = (text: string): number => {
  const cleaned = text.toLowerCase();
  const words = cleaned.split(/\s+/).filter(Boolean);
  if (!words.length) return 0;
  const riskWords = [
    "risk",
    "adverse",
    "uncertain",
    "uncertainty",
    "volatile",
    "litigation",
    "regulation",
    "headwind",
    "impairment",
  ];
  let hits = 0;
  for (const word of riskWords) {
    const regex = new RegExp(`\\b${word}\\b`, "g");
    const matches = cleaned.match(regex);
    hits += matches?.length ?? 0;
  }
  return hits / words.length;
};

const evaluateEstimateRevision = (ticker: string, dbPath?: string): ThesisAlert | null => {
  const db = openResearchDb(dbPath);
  const rows = db
    .prepare(
      `SELECT estimated_eps
       FROM earnings_expectations e
       JOIN instruments i ON i.id=e.instrument_id
       WHERE i.ticker=?
         AND e.period_type='quarterly'
         AND estimated_eps IS NOT NULL
       ORDER BY CASE
         WHEN e.reported_date <> '' THEN e.reported_date
         ELSE e.fiscal_date_ending
       END DESC
       LIMIT 6`,
    )
    .all(ticker) as Array<{ estimated_eps?: number }>;
  const values = rows
    .map((row) => toFinite(row.estimated_eps))
    .filter((v): v is number => typeof v === "number");
  if (values.length < 6) return null;
  const recent = mean(values.slice(0, 3));
  const prior = mean(values.slice(3, 6));
  if (Math.abs(prior) <= 1e-9) return null;
  const revisionPct = recent / prior - 1;
  if (revisionPct <= -0.08) {
    return {
      ticker,
      severity: "high",
      alertType: "estimate-revision",
      message: "Consensus EPS revisions deteriorated materially.",
      details: `recent_avg=${recent.toFixed(3)} prior_avg=${prior.toFixed(3)} revision_pct=${(revisionPct * 100).toFixed(1)}%`,
    };
  }
  if (revisionPct <= -0.04) {
    return {
      ticker,
      severity: "medium",
      alertType: "estimate-revision",
      message: "Consensus EPS revisions turned negative.",
      details: `recent_avg=${recent.toFixed(3)} prior_avg=${prior.toFixed(3)} revision_pct=${(revisionPct * 100).toFixed(1)}%`,
    };
  }
  return null;
};

const evaluateLatestSurprise = (ticker: string, dbPath?: string): ThesisAlert | null => {
  const db = openResearchDb(dbPath);
  const row = db
    .prepare(
      `SELECT surprise_pct, fiscal_date_ending
       FROM earnings_expectations e
       JOIN instruments i ON i.id=e.instrument_id
       WHERE i.ticker=?
         AND e.period_type='quarterly'
         AND surprise_pct IS NOT NULL
       ORDER BY CASE
         WHEN e.reported_date <> '' THEN e.reported_date
         ELSE e.fiscal_date_ending
       END DESC
       LIMIT 1`,
    )
    .get(ticker) as { surprise_pct?: number; fiscal_date_ending?: string } | undefined;
  const surprisePct = toFinite(row?.surprise_pct);
  if (typeof surprisePct !== "number") return null;
  if (surprisePct <= -10) {
    return {
      ticker,
      severity: "high",
      alertType: "earnings-surprise",
      message: "Latest earnings surprise is strongly negative.",
      details: `surprise_pct=${surprisePct.toFixed(2)} fiscal_date=${row?.fiscal_date_ending ?? ""}`,
    };
  }
  if (surprisePct <= -5) {
    return {
      ticker,
      severity: "medium",
      alertType: "earnings-surprise",
      message: "Latest earnings surprise is negative.",
      details: `surprise_pct=${surprisePct.toFixed(2)} fiscal_date=${row?.fiscal_date_ending ?? ""}`,
    };
  }
  return null;
};

const evaluatePriceRegimeBreak = (ticker: string, dbPath?: string): ThesisAlert | null => {
  const db = openResearchDb(dbPath);
  const rows = db
    .prepare(
      `SELECT p.close, p.date
       FROM prices p
       JOIN instruments i ON i.id=p.instrument_id
       WHERE i.ticker=?
       ORDER BY p.date DESC
       LIMIT 80`,
    )
    .all(ticker) as Array<{ close?: number; date: string }>;
  const closes = rows
    .map((row) => toFinite(row.close))
    .filter((value): value is number => typeof value === "number");
  if (closes.length < 30) return null;
  const latest = closes[0]!;
  const ma20 = mean(closes.slice(0, 20));
  const ma60 = closes.length >= 60 ? mean(closes.slice(0, 60)) : mean(closes);
  const below20 = latest / ma20 - 1;
  const below60 = latest / ma60 - 1;
  if (below20 <= -0.1 && below60 <= -0.15) {
    return {
      ticker,
      severity: "high",
      alertType: "price-regime",
      message: "Price broke below both short and medium trend regimes.",
      details: `latest=${latest.toFixed(2)} ma20=${ma20.toFixed(2)} ma60=${ma60.toFixed(2)} delta20=${(below20 * 100).toFixed(1)}% delta60=${(below60 * 100).toFixed(1)}%`,
    };
  }
  if (below20 <= -0.07) {
    return {
      ticker,
      severity: "medium",
      alertType: "price-regime",
      message: "Price moved materially below short-term trend.",
      details: `latest=${latest.toFixed(2)} ma20=${ma20.toFixed(2)} delta20=${(below20 * 100).toFixed(1)}%`,
    };
  }
  return null;
};

const evaluateFilingRiskLanguage = (ticker: string, dbPath?: string): ThesisAlert | null => {
  const db = openResearchDb(dbPath);
  const rows = db
    .prepare(
      `SELECT f.text, f.form, f.filed
       FROM filings f
       JOIN instruments i ON i.id=f.instrument_id
       WHERE i.ticker=?
       ORDER BY f.filed DESC
       LIMIT 2`,
    )
    .all(ticker) as Array<{ text?: string; form?: string; filed?: string }>;
  if (rows.length < 2) return null;
  const latest = riskDensity(rows[0]?.text ?? "");
  const prior = riskDensity(rows[1]?.text ?? "");
  if (!Number.isFinite(latest) || !Number.isFinite(prior) || prior <= 1e-9) return null;
  const change = latest / prior - 1;
  if (change >= 0.35) {
    return {
      ticker,
      severity: "medium",
      alertType: "filing-risk-language",
      message: "Risk-language density increased materially in latest filing.",
      details: `latest_density=${latest.toFixed(4)} prior_density=${prior.toFixed(4)} change=${(change * 100).toFixed(1)}%`,
    };
  }
  return null;
};

const evaluateSignalConflict = (ticker: string, dbPath?: string): ThesisAlert | null => {
  const variant = computeVariantPerception({ ticker, dbPath });
  const valuation = computeValuation({ ticker, dbPath });
  const implied = valuation.impliedExpectations?.stance;
  if (
    variant.stance === "positive-variant" &&
    implied === "market-too-bullish" &&
    variant.confidence >= 0.6
  ) {
    return {
      ticker,
      severity: "medium",
      alertType: "signal-conflict",
      message: "Variant signal conflicts with market-implied valuation stance.",
      details: `variant=${variant.stance} implied=${implied} variant_confidence=${variant.confidence.toFixed(2)}`,
    };
  }
  if (
    variant.stance === "negative-variant" &&
    implied === "market-too-bearish" &&
    variant.confidence >= 0.6
  ) {
    return {
      ticker,
      severity: "medium",
      alertType: "signal-conflict",
      message: "Variant signal conflicts with market-implied valuation stance.",
      details: `variant=${variant.stance} implied=${implied} variant_confidence=${variant.confidence.toFixed(2)}`,
    };
  }
  return null;
};

const persistAlerts = (alerts: ThesisAlert[], dbPath?: string): number => {
  if (!alerts.length) return 0;
  const db = openResearchDb(dbPath);
  const insert = db.prepare(
    `INSERT INTO thesis_alerts (
       instrument_id, ticker, severity, alert_type, message, details, created_at, resolved
     )
     VALUES (
       (SELECT id FROM instruments WHERE ticker=?),
       ?, ?, ?, ?, ?, ?, 0
     )`,
  );
  db.exec("BEGIN");
  try {
    for (const alert of alerts) {
      insert.run(
        alert.ticker,
        alert.ticker,
        alert.severity,
        alert.alertType,
        alert.message,
        alert.details,
        Date.now(),
      );
    }
    db.exec("COMMIT");
    return alerts.length;
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
};

export const monitorTicker = (params: { ticker: string; dbPath?: string }): ThesisMonitorResult => {
  const ticker = normalizeTicker(params.ticker);
  const alerts = [
    evaluateEstimateRevision(ticker, params.dbPath),
    evaluateLatestSurprise(ticker, params.dbPath),
    evaluatePriceRegimeBreak(ticker, params.dbPath),
    evaluateFilingRiskLanguage(ticker, params.dbPath),
    evaluateSignalConflict(ticker, params.dbPath),
  ].filter((alert): alert is ThesisAlert => Boolean(alert));

  const persisted = persistAlerts(alerts, params.dbPath);
  return {
    ticker,
    checkedAt: new Date().toISOString(),
    alerts,
    persisted,
  };
};

export const monitorTickers = (params: { tickers: string[]; dbPath?: string }) =>
  params.tickers.map((ticker) => monitorTicker({ ticker, dbPath: params.dbPath }));
