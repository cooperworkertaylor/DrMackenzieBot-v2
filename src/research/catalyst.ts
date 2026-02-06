import { openResearchDb } from "./db.js";

type CatalystDirection = "up" | "down" | "both";
type CatalystStatus = "open" | "resolved" | "cancelled";

export type CatalystRecord = {
  id: number;
  ticker: string;
  category: string;
  name: string;
  dateWindowStart: string;
  dateWindowEnd: string;
  probability: number;
  impactBps: number;
  confidence: number;
  direction: CatalystDirection;
  source: string;
  status: CatalystStatus;
  notes: string;
  createdAt: number;
  updatedAt: number;
};

export type CatalystSummary = {
  ticker: string;
  openCount: number;
  expectedImpactBps: number;
  expectedImpactPct: number;
  weightedConfidence: number;
  highImpactCount: number;
  nearestCatalystDate?: string;
};

const normalizeTicker = (ticker: string) => ticker.trim().toUpperCase();

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const normalizeDate = (value?: string): string => {
  if (!value) return "";
  const trimmed = value.trim();
  if (!trimmed) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toISOString().slice(0, 10);
};

const resolveInstrumentId = (ticker: string, dbPath?: string): number => {
  const db = openResearchDb(dbPath);
  const normalized = normalizeTicker(ticker);
  const existing = db.prepare(`SELECT id FROM instruments WHERE ticker=?`).get(normalized) as
    | { id: number }
    | undefined;
  if (existing?.id) return existing.id;
  const inserted = db
    .prepare(
      `INSERT INTO instruments (ticker, updated_at)
       VALUES (?, ?)
       ON CONFLICT(ticker) DO UPDATE SET updated_at=excluded.updated_at
       RETURNING id`,
    )
    .get(normalized, Date.now()) as { id: number };
  return inserted.id;
};

export const addCatalyst = (params: {
  ticker: string;
  name: string;
  category?: string;
  dateWindowStart?: string;
  dateWindowEnd?: string;
  probability: number;
  impactBps: number;
  confidence: number;
  direction?: CatalystDirection;
  source?: string;
  notes?: string;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
  const instrumentId = resolveInstrumentId(ticker, params.dbPath);
  const row = db
    .prepare(
      `INSERT INTO catalysts (
         instrument_id, ticker, category, name, date_window_start, date_window_end,
         probability, impact_bps, confidence, direction, source, status, notes, created_at, updated_at
       )
       VALUES (
         @instrument_id, @ticker, @category, @name, @date_window_start, @date_window_end,
         @probability, @impact_bps, @confidence, @direction, @source, 'open', @notes, @created_at, @updated_at
       )
       RETURNING id`,
    )
    .get({
      instrument_id: instrumentId,
      ticker,
      category: params.category?.trim() || "company",
      name: params.name.trim(),
      date_window_start: normalizeDate(params.dateWindowStart),
      date_window_end: normalizeDate(params.dateWindowEnd),
      probability: clamp(params.probability, 0, 1),
      impact_bps: params.impactBps,
      confidence: clamp(params.confidence, 0, 1),
      direction: (params.direction ?? "both") as CatalystDirection,
      source: params.source?.trim() || "manual",
      notes: params.notes?.trim() || "",
      created_at: Date.now(),
      updated_at: Date.now(),
    }) as { id: number };
  return row.id;
};

export const listCatalysts = (params: {
  ticker: string;
  status?: CatalystStatus | "all";
  dbPath?: string;
}): CatalystRecord[] => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
  const status = params.status ?? "open";
  const rows = db
    .prepare(
      `SELECT
         id, ticker, category, name, date_window_start, date_window_end, probability,
         impact_bps, confidence, direction, source, status, notes, created_at, updated_at
       FROM catalysts
       WHERE ticker = ?
         ${status === "all" ? "" : "AND status = ?"}
       ORDER BY
         CASE WHEN date_window_start <> '' THEN date_window_start ELSE '9999-12-31' END ASC,
         updated_at DESC`,
    )
    .all(...(status === "all" ? [ticker] : [ticker, status])) as Array<{
    id: number;
    ticker: string;
    category: string;
    name: string;
    date_window_start: string;
    date_window_end: string;
    probability: number;
    impact_bps: number;
    confidence: number;
    direction: CatalystDirection;
    source: string;
    status: CatalystStatus;
    notes: string;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map((row) => ({
    id: row.id,
    ticker: row.ticker,
    category: row.category,
    name: row.name,
    dateWindowStart: row.date_window_start,
    dateWindowEnd: row.date_window_end,
    probability: row.probability,
    impactBps: row.impact_bps,
    confidence: row.confidence,
    direction: row.direction,
    source: row.source,
    status: row.status,
    notes: row.notes,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }));
};

export const resolveCatalyst = (params: {
  catalystId: number;
  occurred: boolean;
  realizedImpactBps?: number;
  notes?: string;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  const existing = db
    .prepare(`SELECT id, status FROM catalysts WHERE id=?`)
    .get(params.catalystId) as { id: number; status: CatalystStatus } | undefined;
  if (!existing) throw new Error(`Catalyst not found: ${params.catalystId}`);

  db.exec("BEGIN");
  try {
    db.prepare(
      `INSERT INTO catalyst_outcomes (catalyst_id, occurred, realized_impact_bps, resolved_at, notes)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(catalyst_id) DO UPDATE SET
         occurred=excluded.occurred,
         realized_impact_bps=excluded.realized_impact_bps,
         resolved_at=excluded.resolved_at,
         notes=excluded.notes`,
    ).run(
      params.catalystId,
      params.occurred ? 1 : 0,
      typeof params.realizedImpactBps === "number" ? params.realizedImpactBps : null,
      Date.now(),
      params.notes?.trim() || "",
    );
    db.prepare(`UPDATE catalysts SET status='resolved', updated_at=? WHERE id=?`).run(
      Date.now(),
      params.catalystId,
    );
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
};

export const cancelCatalyst = (params: {
  catalystId: number;
  reason?: string;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  db.prepare(
    `UPDATE catalysts
     SET status='cancelled',
         notes=CASE WHEN ? = '' THEN notes ELSE TRIM(notes || ' | cancelled:' || ?) END,
         updated_at=?
     WHERE id=?`,
  ).run(params.reason?.trim() || "", params.reason?.trim() || "", Date.now(), params.catalystId);
};

export const getCatalystSummary = (params: {
  ticker: string;
  dbPath?: string;
}): CatalystSummary => {
  const rows = listCatalysts({ ticker: params.ticker, status: "open", dbPath: params.dbPath });
  if (!rows.length) {
    return {
      ticker: normalizeTicker(params.ticker),
      openCount: 0,
      expectedImpactBps: 0,
      expectedImpactPct: 0,
      weightedConfidence: 0,
      highImpactCount: 0,
    };
  }
  const expectedImpactBps = rows.reduce(
    (sum, row) => sum + row.probability * row.impactBps * row.confidence,
    0,
  );
  const weight = rows.reduce((sum, row) => sum + Math.abs(row.impactBps), 0);
  const weightedConfidence =
    weight > 1e-9
      ? rows.reduce((sum, row) => sum + row.confidence * Math.abs(row.impactBps), 0) / weight
      : 0;
  const highImpactCount = rows.filter((row) => Math.abs(row.impactBps) >= 300).length;
  const nearestCatalystDate = rows
    .map((row) => row.dateWindowStart)
    .filter((value) => value)
    .sort((a, b) => a.localeCompare(b))[0];
  return {
    ticker: normalizeTicker(params.ticker),
    openCount: rows.length,
    expectedImpactBps,
    expectedImpactPct: expectedImpactBps / 10_000,
    weightedConfidence: clamp(weightedConfidence, 0, 1),
    highImpactCount,
    nearestCatalystDate,
  };
};

export const __testOnly = {
  normalizeDate,
};
