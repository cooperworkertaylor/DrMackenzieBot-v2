import { createHash } from "node:crypto";
import { openResearchDb } from "./db.js";
import { appendProvenanceEvent } from "./provenance.js";

export type PointInTimeEvent = {
  id: number;
  entityId: number;
  eventType: string;
  eventTime: number;
  periodStart: string;
  periodEnd: string;
  sourceTable: string;
  sourceRefId: number;
  sourceUrl: string;
  title: string;
  payload: Record<string, unknown>;
  eventHash: string;
  createdAt: number;
  updatedAt: number;
};

export type PointInTimeFact = {
  id: number;
  entityId: number;
  eventId?: number;
  metricKey: string;
  metricKind: string;
  valueNum?: number;
  valueText: string;
  unit: string;
  direction: string;
  confidence: number;
  asOfDate: string;
  validFrom: string;
  validTo: string;
  sourceTable: string;
  sourceRefId: number;
  sourceUrl: string;
  metadata: Record<string, unknown>;
  factHash: string;
  createdAt: number;
  updatedAt: number;
};

export type PointInTimeMetric = {
  metricKey: string;
  metricKind: string;
  unit: string;
  samples: number;
  latestAsOfDate: string;
  latestValueNum?: number;
  latestValueText?: string;
  previousValueNum?: number;
  deltaValueNum?: number;
};

export type PointInTimeSnapshot = {
  ticker: string;
  entityId?: number;
  entityName?: string;
  asOfDate: string;
  windowStartDate: string;
  events: PointInTimeEvent[];
  facts: PointInTimeFact[];
  metrics: PointInTimeMetric[];
};

export type GraphBuildSourceStats = {
  source: string;
  rowsScanned: number;
  eventsInserted: number;
  eventsUpdated: number;
  factsInserted: number;
  factsUpdated: number;
};

export type GraphBuildSummary = {
  ticker: string;
  entityId: number;
  entityName: string;
  generatedAt: string;
  rowsScanned: number;
  eventsInserted: number;
  eventsUpdated: number;
  factsInserted: number;
  factsUpdated: number;
  sourceStats: GraphBuildSourceStats[];
};

const DAY_MS = 86_400_000;

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

const normalizeMetricKey = (value: string): string =>
  value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "_")
    .replace(/_{2,}/g, "_")
    .replace(/^_+|_+$/g, "");

const parseDateMs = (value: unknown): number | undefined => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const text = value.trim();
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(text) ? `${text}T00:00:00.000Z` : text;
  const ts = Date.parse(normalized);
  return Number.isFinite(ts) ? ts : undefined;
};

const toIsoDate = (value: number): string => new Date(value).toISOString().slice(0, 10);

const normalizeDate = (value: unknown): string => {
  const ts = parseDateMs(value);
  return typeof ts === "number" ? toIsoDate(ts) : "";
};

const toFiniteOrUndefined = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isFinite(value) ? value : undefined;

const normalizeObject = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
};

const parseJsonObject = <T extends object>(value: unknown, fallback: T): T => {
  if (typeof value !== "string" || !value.trim()) return fallback;
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return fallback;
    return parsed as T;
  } catch {
    return fallback;
  }
};

const toCanonicalJson = (value: unknown): string => {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => toCanonicalJson(item)).join(",")}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([key, child]) => `${JSON.stringify(key)}:${toCanonicalJson(child)}`);
  return `{${entries.join(",")}}`;
};

const hashMaterial = (parts: Array<string | number>): string =>
  createHash("sha256")
    .update(parts.map((part) => String(part)).join("|"))
    .digest("hex");

const resolveEventTime = (params: { dateCandidates: unknown[]; fallbackMs?: number }): number => {
  for (const candidate of params.dateCandidates) {
    const parsed = parseDateMs(candidate);
    if (typeof parsed === "number") return parsed;
  }
  if (typeof params.fallbackMs === "number" && Number.isFinite(params.fallbackMs)) {
    return params.fallbackMs;
  }
  return Date.now();
};

const parseEventRow = (row: {
  id: number;
  entity_id: number;
  event_type: string;
  event_time: number;
  period_start: string;
  period_end: string;
  source_table: string;
  source_ref_id: number;
  source_url: string;
  title: string;
  payload?: string;
  event_hash: string;
  created_at: number;
  updated_at: number;
}): PointInTimeEvent => ({
  id: row.id,
  entityId: row.entity_id,
  eventType: row.event_type,
  eventTime: row.event_time,
  periodStart: row.period_start,
  periodEnd: row.period_end,
  sourceTable: row.source_table,
  sourceRefId: row.source_ref_id,
  sourceUrl: row.source_url,
  title: row.title,
  payload: parseJsonObject<Record<string, unknown>>(row.payload, {}),
  eventHash: row.event_hash,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
});

const parseFactRow = (row: {
  id: number;
  entity_id: number;
  event_id?: number | null;
  metric_key: string;
  metric_kind: string;
  value_num?: number | null;
  value_text: string;
  unit: string;
  direction: string;
  confidence: number;
  as_of_date: string;
  valid_from: string;
  valid_to: string;
  source_table: string;
  source_ref_id: number;
  source_url: string;
  metadata?: string;
  fact_hash: string;
  created_at: number;
  updated_at: number;
}): PointInTimeFact => ({
  id: row.id,
  entityId: row.entity_id,
  eventId: typeof row.event_id === "number" ? row.event_id : undefined,
  metricKey: row.metric_key,
  metricKind: row.metric_kind,
  valueNum: typeof row.value_num === "number" ? row.value_num : undefined,
  valueText: row.value_text,
  unit: row.unit,
  direction: row.direction,
  confidence: clamp(row.confidence, 0, 1),
  asOfDate: row.as_of_date,
  validFrom: row.valid_from,
  validTo: row.valid_to,
  sourceTable: row.source_table,
  sourceRefId: Math.max(0, Math.round(row.source_ref_id)),
  sourceUrl: row.source_url,
  metadata: parseJsonObject<Record<string, unknown>>(row.metadata, {}),
  factHash: row.fact_hash,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
});

const ensureCompanyEntity = (params: {
  db: ReturnType<typeof openResearchDb>;
  ticker: string;
  name?: string;
}): { id: number; canonicalName: string } => {
  const now = Date.now();
  const canonicalName = (params.name ?? params.ticker).trim() || params.ticker;
  params.db
    .prepare(
      `INSERT INTO research_entities (
         kind, canonical_name, ticker, metadata, created_at, updated_at
       ) VALUES ('company', ?, ?, ?, ?, ?)
       ON CONFLICT(kind, canonical_name, ticker) DO UPDATE SET
         metadata=excluded.metadata,
         updated_at=excluded.updated_at`,
    )
    .run(
      canonicalName,
      params.ticker,
      JSON.stringify({
        source: "point_in_time_graph",
        updated_at: new Date(now).toISOString(),
      }),
      now,
      now,
    );
  const row = params.db
    .prepare(
      `SELECT id, canonical_name
       FROM research_entities
       WHERE kind='company' AND canonical_name=? AND ticker=?
       ORDER BY updated_at DESC
       LIMIT 1`,
    )
    .get(canonicalName, params.ticker) as { id: number; canonical_name: string } | undefined;
  if (!row) throw new Error(`Unable to upsert company entity for ticker=${params.ticker}`);
  return {
    id: row.id,
    canonicalName: row.canonical_name,
  };
};

const upsertGraphEvent = (params: {
  db: ReturnType<typeof openResearchDb>;
  entityId: number;
  eventType: string;
  eventTime: number;
  periodStart?: string;
  periodEnd?: string;
  sourceTable: string;
  sourceRefId: number;
  sourceUrl?: string;
  title?: string;
  payload?: Record<string, unknown>;
}): { id: number; inserted: boolean } => {
  const now = Date.now();
  const payload = normalizeObject(params.payload);
  const eventHash = hashMaterial([
    "event_v1",
    params.entityId,
    params.eventType,
    params.eventTime,
    params.sourceTable,
    params.sourceRefId,
    params.title ?? "",
    toCanonicalJson(payload),
  ]);
  const existing = params.db
    .prepare(`SELECT id FROM research_events WHERE entity_id=? AND event_hash=?`)
    .get(params.entityId, eventHash) as { id: number } | undefined;
  if (existing) {
    params.db
      .prepare(
        `UPDATE research_events
         SET event_type=?,
             event_time=?,
             period_start=?,
             period_end=?,
             source_table=?,
             source_ref_id=?,
             source_url=?,
             title=?,
             payload=?,
             updated_at=?
         WHERE id=?`,
      )
      .run(
        params.eventType,
        params.eventTime,
        (params.periodStart ?? "").trim(),
        (params.periodEnd ?? "").trim(),
        params.sourceTable,
        Math.max(0, Math.round(params.sourceRefId)),
        (params.sourceUrl ?? "").trim(),
        (params.title ?? "").trim(),
        JSON.stringify(payload),
        now,
        existing.id,
      );
    return { id: existing.id, inserted: false };
  }
  const row = params.db
    .prepare(
      `INSERT INTO research_events (
         entity_id, event_type, event_time, period_start, period_end, source_table,
         source_ref_id, source_url, title, payload, event_hash, created_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      params.entityId,
      params.eventType,
      params.eventTime,
      (params.periodStart ?? "").trim(),
      (params.periodEnd ?? "").trim(),
      params.sourceTable,
      Math.max(0, Math.round(params.sourceRefId)),
      (params.sourceUrl ?? "").trim(),
      (params.title ?? "").trim(),
      JSON.stringify(payload),
      eventHash,
      now,
      now,
    ) as { id: number };
  return { id: row.id, inserted: true };
};

const upsertGraphFact = (params: {
  db: ReturnType<typeof openResearchDb>;
  entityId: number;
  eventId?: number;
  metricKey: string;
  metricKind: string;
  valueNum?: number;
  valueText?: string;
  unit?: string;
  direction?: string;
  confidence?: number;
  asOfDate?: string;
  validFrom?: string;
  validTo?: string;
  sourceTable: string;
  sourceRefId: number;
  sourceUrl?: string;
  metadata?: Record<string, unknown>;
}): { id: number; inserted: boolean } => {
  const now = Date.now();
  const metricKey = normalizeMetricKey(params.metricKey);
  if (!metricKey) {
    throw new Error("metricKey is required");
  }
  const metricKind = (params.metricKind || "numeric").trim().toLowerCase();
  const valueNum = toFiniteOrUndefined(params.valueNum);
  const valueText = (params.valueText ?? "").trim();
  const asOfDate = normalizeDate(params.asOfDate);
  const validFrom = normalizeDate(params.validFrom);
  const validTo = normalizeDate(params.validTo);
  const metadata = normalizeObject(params.metadata);
  const factHash = hashMaterial([
    "fact_v1",
    params.entityId,
    params.eventId ?? 0,
    metricKey,
    metricKind,
    valueNum ?? "",
    valueText,
    params.unit ?? "",
    asOfDate,
    validFrom,
    validTo,
    params.sourceTable,
    params.sourceRefId,
    toCanonicalJson(metadata),
  ]);
  const existing = params.db
    .prepare(`SELECT id FROM research_facts WHERE entity_id=? AND fact_hash=?`)
    .get(params.entityId, factHash) as { id: number } | undefined;
  if (existing) {
    params.db
      .prepare(
        `UPDATE research_facts
         SET event_id=?,
             metric_key=?,
             metric_kind=?,
             value_num=?,
             value_text=?,
             unit=?,
             direction=?,
             confidence=?,
             as_of_date=?,
             valid_from=?,
             valid_to=?,
             source_table=?,
             source_ref_id=?,
             source_url=?,
             metadata=?,
             updated_at=?
         WHERE id=?`,
      )
      .run(
        typeof params.eventId === "number" ? Math.max(1, Math.round(params.eventId)) : null,
        metricKey,
        metricKind,
        valueNum ?? null,
        valueText,
        (params.unit ?? "").trim(),
        (params.direction ?? "").trim(),
        clamp(params.confidence ?? 0.6, 0, 1),
        asOfDate,
        validFrom,
        validTo,
        params.sourceTable,
        Math.max(0, Math.round(params.sourceRefId)),
        (params.sourceUrl ?? "").trim(),
        JSON.stringify(metadata),
        now,
        existing.id,
      );
    return { id: existing.id, inserted: false };
  }
  const row = params.db
    .prepare(
      `INSERT INTO research_facts (
         entity_id, event_id, metric_key, metric_kind, value_num, value_text, unit, direction,
         confidence, as_of_date, valid_from, valid_to, source_table, source_ref_id, source_url,
         metadata, fact_hash, created_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      params.entityId,
      typeof params.eventId === "number" ? Math.max(1, Math.round(params.eventId)) : null,
      metricKey,
      metricKind,
      valueNum ?? null,
      valueText,
      (params.unit ?? "").trim(),
      (params.direction ?? "").trim(),
      clamp(params.confidence ?? 0.6, 0, 1),
      asOfDate,
      validFrom,
      validTo,
      params.sourceTable,
      Math.max(0, Math.round(params.sourceRefId)),
      (params.sourceUrl ?? "").trim(),
      JSON.stringify(metadata),
      factHash,
      now,
      now,
    ) as { id: number };
  return { id: row.id, inserted: true };
};

const addSourceRow = (
  stats: Map<string, GraphBuildSourceStats>,
  source: string,
): GraphBuildSourceStats => {
  const existing = stats.get(source);
  if (existing) {
    existing.rowsScanned += 1;
    return existing;
  }
  const created: GraphBuildSourceStats = {
    source,
    rowsScanned: 1,
    eventsInserted: 0,
    eventsUpdated: 0,
    factsInserted: 0,
    factsUpdated: 0,
  };
  stats.set(source, created);
  return created;
};

const eventAction = (
  source: GraphBuildSourceStats,
  result: { inserted: boolean },
): { inserted: number; updated: number } => {
  if (result.inserted) {
    source.eventsInserted += 1;
    return { inserted: 1, updated: 0 };
  }
  source.eventsUpdated += 1;
  return { inserted: 0, updated: 1 };
};

const factAction = (
  source: GraphBuildSourceStats,
  result: { inserted: boolean },
): { inserted: number; updated: number } => {
  if (result.inserted) {
    source.factsInserted += 1;
    return { inserted: 1, updated: 0 };
  }
  source.factsUpdated += 1;
  return { inserted: 0, updated: 1 };
};

export const buildTickerPointInTimeGraph = (params: {
  ticker: string;
  dbPath?: string;
  maxFundamentalFacts?: number;
  maxExpectations?: number;
  maxFilings?: number;
  maxTranscripts?: number;
  maxCatalysts?: number;
}): GraphBuildSummary => {
  const ticker = normalizeTicker(params.ticker);
  if (!ticker) throw new Error("ticker is required");
  const maxFundamentalFacts = Math.max(10, Math.round(params.maxFundamentalFacts ?? 500));
  const maxExpectations = Math.max(10, Math.round(params.maxExpectations ?? 160));
  const maxFilings = Math.max(10, Math.round(params.maxFilings ?? 160));
  const maxTranscripts = Math.max(10, Math.round(params.maxTranscripts ?? 120));
  const maxCatalysts = Math.max(10, Math.round(params.maxCatalysts ?? 120));
  const db = openResearchDb(params.dbPath);
  const sourceStats = new Map<string, GraphBuildSourceStats>();
  let rowsScanned = 0;
  let eventsInserted = 0;
  let eventsUpdated = 0;
  let factsInserted = 0;
  let factsUpdated = 0;

  const instrument = db
    .prepare(`SELECT id, name FROM instruments WHERE ticker=? LIMIT 1`)
    .get(ticker) as { id?: number; name?: string } | undefined;
  const entity = ensureCompanyEntity({
    db,
    ticker,
    name: instrument?.name || ticker,
  });

  db.exec("BEGIN");
  try {
    const fundamentalRows = db
      .prepare(
        `SELECT
           ff.id, ff.taxonomy, ff.concept, ff.label, ff.unit, ff.value, ff.as_of_date,
           ff.period_start, ff.period_end, ff.filing_date, ff.accepted_at, ff.source_url,
           ff.source, ff.fetched_at
         FROM fundamental_facts ff
         WHERE ff.ticker=? OR (? > 0 AND ff.instrument_id=?)
         ORDER BY ff.as_of_date DESC, ff.period_end DESC, ff.id DESC
         LIMIT ?`,
      )
      .all(
        ticker,
        typeof instrument?.id === "number" ? instrument.id : 0,
        typeof instrument?.id === "number" ? instrument.id : 0,
        maxFundamentalFacts,
      ) as Array<{
      id: number;
      taxonomy: string;
      concept: string;
      label: string;
      unit: string;
      value: number;
      as_of_date: string;
      period_start: string;
      period_end: string;
      filing_date: string;
      accepted_at: string;
      source_url: string;
      source: string;
      fetched_at: number;
    }>;
    for (const row of fundamentalRows) {
      rowsScanned += 1;
      const source = addSourceRow(sourceStats, "fundamental_facts");
      const event = upsertGraphEvent({
        db,
        entityId: entity.id,
        eventType: "fundamental_fact",
        eventTime: resolveEventTime({
          dateCandidates: [row.as_of_date, row.filing_date, row.period_end, row.accepted_at],
          fallbackMs: row.fetched_at,
        }),
        periodStart: row.period_start,
        periodEnd: row.period_end,
        sourceTable: "fundamental_facts",
        sourceRefId: row.id,
        sourceUrl: row.source_url,
        title: `${row.taxonomy}.${row.concept}`,
        payload: {
          taxonomy: row.taxonomy,
          concept: row.concept,
          label: row.label,
          unit: row.unit,
          source: row.source,
          as_of_date: row.as_of_date,
          period_end: row.period_end,
          filing_date: row.filing_date,
        },
      });
      const eventCounts = eventAction(source, event);
      eventsInserted += eventCounts.inserted;
      eventsUpdated += eventCounts.updated;

      const fact = upsertGraphFact({
        db,
        entityId: entity.id,
        eventId: event.id,
        metricKey: `${row.taxonomy}.${row.concept}`,
        metricKind: "numeric",
        valueNum: row.value,
        unit: row.unit,
        confidence: 0.76,
        asOfDate: row.as_of_date || row.filing_date || row.period_end,
        validFrom: row.period_start || row.period_end,
        validTo: "",
        sourceTable: "fundamental_facts",
        sourceRefId: row.id,
        sourceUrl: row.source_url,
        metadata: {
          label: row.label,
          taxonomy: row.taxonomy,
          concept: row.concept,
          filing_date: row.filing_date,
          period_end: row.period_end,
          accepted_at: row.accepted_at,
        },
      });
      const factCounts = factAction(source, fact);
      factsInserted += factCounts.inserted;
      factsUpdated += factCounts.updated;
    }

    const expectationRows = db
      .prepare(
        `SELECT
           ee.id, ee.period_type, ee.fiscal_date_ending, ee.reported_date, ee.reported_eps,
           ee.estimated_eps, ee.surprise, ee.surprise_pct, ee.report_time, ee.source, ee.source_url,
           ee.fetched_at
         FROM earnings_expectations ee
         WHERE ee.ticker=? OR (? > 0 AND ee.instrument_id=?)
         ORDER BY
           CASE WHEN ee.reported_date <> '' THEN ee.reported_date ELSE ee.fiscal_date_ending END DESC,
           ee.id DESC
         LIMIT ?`,
      )
      .all(
        ticker,
        typeof instrument?.id === "number" ? instrument.id : 0,
        typeof instrument?.id === "number" ? instrument.id : 0,
        maxExpectations,
      ) as Array<{
      id: number;
      period_type: string;
      fiscal_date_ending: string;
      reported_date: string;
      reported_eps?: number;
      estimated_eps?: number;
      surprise?: number;
      surprise_pct?: number;
      report_time?: string;
      source: string;
      source_url: string;
      fetched_at: number;
    }>;
    for (const row of expectationRows) {
      rowsScanned += 1;
      const source = addSourceRow(sourceStats, "earnings_expectations");
      const event = upsertGraphEvent({
        db,
        entityId: entity.id,
        eventType: `earnings_${(row.period_type || "period").toLowerCase()}`,
        eventTime: resolveEventTime({
          dateCandidates: [row.reported_date, row.fiscal_date_ending],
          fallbackMs: row.fetched_at,
        }),
        periodEnd: row.fiscal_date_ending,
        sourceTable: "earnings_expectations",
        sourceRefId: row.id,
        sourceUrl: row.source_url,
        title: `${row.period_type || "period"} ${row.fiscal_date_ending || "n/a"}`,
        payload: {
          period_type: row.period_type,
          fiscal_date_ending: row.fiscal_date_ending,
          reported_date: row.reported_date,
          report_time: row.report_time ?? "",
          source: row.source,
        },
      });
      const eventCounts = eventAction(source, event);
      eventsInserted += eventCounts.inserted;
      eventsUpdated += eventCounts.updated;

      const metricRows: Array<{ key: string; value?: number; unit: string }> = [
        { key: "earnings.reported_eps", value: row.reported_eps, unit: "usd_per_share" },
        { key: "earnings.estimated_eps", value: row.estimated_eps, unit: "usd_per_share" },
        { key: "earnings.surprise", value: row.surprise, unit: "usd_per_share" },
        { key: "earnings.surprise_pct", value: row.surprise_pct, unit: "pct" },
      ];
      for (const metric of metricRows) {
        if (typeof metric.value !== "number" || !Number.isFinite(metric.value)) continue;
        const fact = upsertGraphFact({
          db,
          entityId: entity.id,
          eventId: event.id,
          metricKey: metric.key,
          metricKind: "numeric",
          valueNum: metric.value,
          unit: metric.unit,
          confidence: 0.78,
          asOfDate: row.reported_date || row.fiscal_date_ending,
          validFrom: row.fiscal_date_ending,
          sourceTable: "earnings_expectations",
          sourceRefId: row.id,
          sourceUrl: row.source_url,
          metadata: {
            period_type: row.period_type,
            report_time: row.report_time ?? "",
            source: row.source,
          },
        });
        const factCounts = factAction(source, fact);
        factsInserted += factCounts.inserted;
        factsUpdated += factCounts.updated;
      }
    }

    const filingRows = db
      .prepare(
        `SELECT
           f.id, f.form, f.is_amendment, f.filed, f.accepted_at, f.period_end, f.as_of_date,
           f.title, f.url, f.source_url, f.source, f.fetched_at, f.accession
         FROM filings f
         JOIN instruments i ON i.id=f.instrument_id
         WHERE i.ticker=?
         ORDER BY f.filed DESC, f.id DESC
         LIMIT ?`,
      )
      .all(ticker, maxFilings) as Array<{
      id: number;
      form: string;
      is_amendment: number;
      filed: string;
      accepted_at: string;
      period_end: string;
      as_of_date: string;
      title: string;
      url: string;
      source_url: string;
      source: string;
      fetched_at: number;
      accession: string;
    }>;
    for (const row of filingRows) {
      rowsScanned += 1;
      const source = addSourceRow(sourceStats, "filings");
      const formKey = (row.form || "filing")
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_");
      const event = upsertGraphEvent({
        db,
        entityId: entity.id,
        eventType: `filing_${formKey || "filing"}`,
        eventTime: resolveEventTime({
          dateCandidates: [row.filed, row.accepted_at, row.as_of_date, row.period_end],
          fallbackMs: row.fetched_at,
        }),
        periodEnd: row.period_end,
        sourceTable: "filings",
        sourceRefId: row.id,
        sourceUrl: row.source_url || row.url,
        title: row.title || row.form || "filing",
        payload: {
          accession: row.accession,
          form: row.form,
          is_amendment: row.is_amendment,
          filed: row.filed,
          accepted_at: row.accepted_at,
          period_end: row.period_end,
          source: row.source,
        },
      });
      const eventCounts = eventAction(source, event);
      eventsInserted += eventCounts.inserted;
      eventsUpdated += eventCounts.updated;

      const formFact = upsertGraphFact({
        db,
        entityId: entity.id,
        eventId: event.id,
        metricKey: "filing.form",
        metricKind: "categorical",
        valueText: row.form || "unknown",
        confidence: 0.92,
        asOfDate: row.filed || row.as_of_date || row.period_end,
        validFrom: row.period_end || row.filed || row.as_of_date,
        sourceTable: "filings",
        sourceRefId: row.id,
        sourceUrl: row.source_url || row.url,
        metadata: {
          accession: row.accession,
          is_amendment: row.is_amendment,
          source: row.source,
        },
      });
      const formFactCounts = factAction(source, formFact);
      factsInserted += formFactCounts.inserted;
      factsUpdated += formFactCounts.updated;

      const amendFact = upsertGraphFact({
        db,
        entityId: entity.id,
        eventId: event.id,
        metricKey: "filing.is_amendment",
        metricKind: "numeric",
        valueNum: row.is_amendment ? 1 : 0,
        unit: "flag",
        confidence: 0.92,
        asOfDate: row.filed || row.as_of_date || row.period_end,
        sourceTable: "filings",
        sourceRefId: row.id,
        sourceUrl: row.source_url || row.url,
      });
      const amendFactCounts = factAction(source, amendFact);
      factsInserted += amendFactCounts.inserted;
      factsUpdated += amendFactCounts.updated;
    }

    const transcriptRows = db
      .prepare(
        `SELECT
           t.id, t.event_date, t.event_type, t.source, t.url, t.title, t.fetched_at
         FROM transcripts t
         JOIN instruments i ON i.id=t.instrument_id
         WHERE i.ticker=?
         ORDER BY t.event_date DESC, t.id DESC
         LIMIT ?`,
      )
      .all(ticker, maxTranscripts) as Array<{
      id: number;
      event_date: string;
      event_type: string;
      source: string;
      url: string;
      title: string;
      fetched_at: number;
    }>;
    for (const row of transcriptRows) {
      rowsScanned += 1;
      const source = addSourceRow(sourceStats, "transcripts");
      const eventType = (row.event_type || "transcript").trim().toLowerCase().replace(/\s+/g, "_");
      const event = upsertGraphEvent({
        db,
        entityId: entity.id,
        eventType: `transcript_${eventType || "event"}`,
        eventTime: resolveEventTime({
          dateCandidates: [row.event_date],
          fallbackMs: row.fetched_at,
        }),
        sourceTable: "transcripts",
        sourceRefId: row.id,
        sourceUrl: row.url,
        title: row.title || row.event_type || "transcript",
        payload: {
          event_date: row.event_date,
          event_type: row.event_type,
          source: row.source,
        },
      });
      const eventCounts = eventAction(source, event);
      eventsInserted += eventCounts.inserted;
      eventsUpdated += eventCounts.updated;

      const typeFact = upsertGraphFact({
        db,
        entityId: entity.id,
        eventId: event.id,
        metricKey: "transcript.event_type",
        metricKind: "categorical",
        valueText: row.event_type || "transcript",
        confidence: 0.8,
        asOfDate: row.event_date,
        sourceTable: "transcripts",
        sourceRefId: row.id,
        sourceUrl: row.url,
        metadata: { source: row.source },
      });
      const factCounts = factAction(source, typeFact);
      factsInserted += factCounts.inserted;
      factsUpdated += factCounts.updated;
    }

    const catalystRows = db
      .prepare(
        `SELECT
           c.id, c.name, c.category, c.date_window_start, c.date_window_end, c.probability,
           c.impact_bps, c.confidence, c.direction, c.status, c.source, c.notes, c.created_at,
           c.updated_at, o.id AS outcome_id, o.occurred, o.realized_impact_bps, o.resolved_at,
           o.notes AS outcome_notes
         FROM catalysts c
         LEFT JOIN catalyst_outcomes o ON o.catalyst_id=c.id
         WHERE c.ticker=?
         ORDER BY c.created_at DESC, c.id DESC
         LIMIT ?`,
      )
      .all(ticker, maxCatalysts) as Array<{
      id: number;
      name: string;
      category: string;
      date_window_start: string;
      date_window_end: string;
      probability: number;
      impact_bps: number;
      confidence: number;
      direction: string;
      status: string;
      source: string;
      notes: string;
      created_at: number;
      updated_at: number;
      outcome_id?: number;
      occurred?: number;
      realized_impact_bps?: number;
      resolved_at?: number;
      outcome_notes?: string;
    }>;
    for (const row of catalystRows) {
      rowsScanned += 1;
      const source = addSourceRow(sourceStats, "catalysts");
      const catalystEvent = upsertGraphEvent({
        db,
        entityId: entity.id,
        eventType: "catalyst",
        eventTime: resolveEventTime({
          dateCandidates: [row.date_window_start, row.date_window_end],
          fallbackMs: row.created_at,
        }),
        periodStart: row.date_window_start,
        periodEnd: row.date_window_end,
        sourceTable: "catalysts",
        sourceRefId: row.id,
        title: row.name,
        payload: {
          category: row.category,
          direction: row.direction,
          status: row.status,
          source: row.source,
          notes: row.notes,
        },
      });
      const eventCounts = eventAction(source, catalystEvent);
      eventsInserted += eventCounts.inserted;
      eventsUpdated += eventCounts.updated;

      for (const metric of [
        { key: "catalyst.probability", value: row.probability, unit: "probability" },
        { key: "catalyst.impact_bps", value: row.impact_bps, unit: "bps" },
        { key: "catalyst.confidence", value: row.confidence, unit: "confidence" },
      ]) {
        const fact = upsertGraphFact({
          db,
          entityId: entity.id,
          eventId: catalystEvent.id,
          metricKey: metric.key,
          metricKind: "numeric",
          valueNum: metric.value,
          unit: metric.unit,
          confidence: 0.7,
          asOfDate: row.date_window_start || toIsoDate(row.created_at),
          validFrom: row.date_window_start || toIsoDate(row.created_at),
          validTo: row.date_window_end,
          sourceTable: "catalysts",
          sourceRefId: row.id,
        });
        const factCounts = factAction(source, fact);
        factsInserted += factCounts.inserted;
        factsUpdated += factCounts.updated;
      }

      const statusFact = upsertGraphFact({
        db,
        entityId: entity.id,
        eventId: catalystEvent.id,
        metricKey: "catalyst.status",
        metricKind: "categorical",
        valueText: row.status,
        confidence: 0.8,
        asOfDate: row.date_window_start || toIsoDate(row.created_at),
        validFrom: row.date_window_start || toIsoDate(row.created_at),
        validTo: row.date_window_end,
        sourceTable: "catalysts",
        sourceRefId: row.id,
      });
      const statusFactCounts = factAction(source, statusFact);
      factsInserted += statusFactCounts.inserted;
      factsUpdated += statusFactCounts.updated;

      if (typeof row.outcome_id === "number") {
        const outcomeEvent = upsertGraphEvent({
          db,
          entityId: entity.id,
          eventType: "catalyst_outcome",
          eventTime: typeof row.resolved_at === "number" ? row.resolved_at : row.updated_at,
          sourceTable: "catalyst_outcomes",
          sourceRefId: row.outcome_id,
          title: `${row.name} outcome`,
          payload: {
            occurred: row.occurred ?? 0,
            realized_impact_bps: row.realized_impact_bps ?? null,
            notes: row.outcome_notes ?? "",
          },
        });
        const outcomeEventCounts = eventAction(source, outcomeEvent);
        eventsInserted += outcomeEventCounts.inserted;
        eventsUpdated += outcomeEventCounts.updated;

        const occurredFact = upsertGraphFact({
          db,
          entityId: entity.id,
          eventId: outcomeEvent.id,
          metricKey: "catalyst.outcome_occurred",
          metricKind: "numeric",
          valueNum: row.occurred ? 1 : 0,
          unit: "flag",
          confidence: 0.9,
          asOfDate: typeof row.resolved_at === "number" ? toIsoDate(row.resolved_at) : "",
          sourceTable: "catalyst_outcomes",
          sourceRefId: row.outcome_id,
        });
        const occurredCounts = factAction(source, occurredFact);
        factsInserted += occurredCounts.inserted;
        factsUpdated += occurredCounts.updated;

        if (
          typeof row.realized_impact_bps === "number" &&
          Number.isFinite(row.realized_impact_bps)
        ) {
          const impactFact = upsertGraphFact({
            db,
            entityId: entity.id,
            eventId: outcomeEvent.id,
            metricKey: "catalyst.realized_impact_bps",
            metricKind: "numeric",
            valueNum: row.realized_impact_bps,
            unit: "bps",
            confidence: 0.9,
            asOfDate: typeof row.resolved_at === "number" ? toIsoDate(row.resolved_at) : "",
            sourceTable: "catalyst_outcomes",
            sourceRefId: row.outcome_id,
          });
          const impactCounts = factAction(source, impactFact);
          factsInserted += impactCounts.inserted;
          factsUpdated += impactCounts.updated;
        }
      }
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }

  const summary: GraphBuildSummary = {
    ticker,
    entityId: entity.id,
    entityName: entity.canonicalName,
    generatedAt: new Date().toISOString(),
    rowsScanned,
    eventsInserted,
    eventsUpdated,
    factsInserted,
    factsUpdated,
    sourceStats: Array.from(sourceStats.values()).sort((a, b) => a.source.localeCompare(b.source)),
  };

  try {
    appendProvenanceEvent({
      eventType: "graph_build",
      entityType: "company",
      entityId: ticker,
      payload: summary as unknown as Record<string, unknown>,
      metadata: {
        module: "knowledge_graph",
      },
      dbPath: params.dbPath,
    });
  } catch {
    // Graph build should remain usable even if provenance append fails.
  }

  return summary;
};

const factSortMs = (fact: PointInTimeFact): number => {
  const asOfMs = parseDateMs(fact.asOfDate);
  if (typeof asOfMs === "number") return asOfMs;
  return fact.updatedAt;
};

const buildMetricSeries = (facts: PointInTimeFact[], limit: number): PointInTimeMetric[] => {
  const byMetric = new Map<string, PointInTimeFact[]>();
  for (const fact of facts) {
    const list = byMetric.get(fact.metricKey);
    if (list) list.push(fact);
    else byMetric.set(fact.metricKey, [fact]);
  }
  const metrics: PointInTimeMetric[] = [];
  for (const [metricKey, rows] of byMetric.entries()) {
    const sorted = rows.toSorted((a, b) => factSortMs(b) - factSortMs(a));
    const latest = sorted[0];
    if (!latest) continue;
    const numericSeries = sorted
      .map((row) => row.valueNum)
      .filter((value): value is number => typeof value === "number");
    const latestValueNum = typeof latest.valueNum === "number" ? latest.valueNum : undefined;
    const previousValueNum = numericSeries.length >= 2 ? numericSeries[1] : undefined;
    const deltaValueNum =
      typeof latestValueNum === "number" && typeof previousValueNum === "number"
        ? latestValueNum - previousValueNum
        : undefined;
    metrics.push({
      metricKey,
      metricKind: latest.metricKind,
      unit: latest.unit,
      samples: sorted.length,
      latestAsOfDate: latest.asOfDate || toIsoDate(latest.updatedAt),
      latestValueNum,
      latestValueText: latest.valueText || undefined,
      previousValueNum,
      deltaValueNum,
    });
  }
  return metrics
    .toSorted(
      (a, b) =>
        b.latestAsOfDate.localeCompare(a.latestAsOfDate) || a.metricKey.localeCompare(b.metricKey),
    )
    .slice(0, Math.max(1, limit));
};

export const getTickerPointInTimeSnapshot = (params: {
  ticker: string;
  asOfDate?: string;
  lookbackDays?: number;
  eventLimit?: number;
  factLimit?: number;
  metricLimit?: number;
  dbPath?: string;
}): PointInTimeSnapshot => {
  const ticker = normalizeTicker(params.ticker);
  if (!ticker) throw new Error("ticker is required");
  const db = openResearchDb(params.dbPath);
  const asOfDate = normalizeDate(params.asOfDate) || toIsoDate(Date.now());
  const asOfMs = resolveEventTime({
    dateCandidates: [`${asOfDate}T23:59:59.999Z`],
    fallbackMs: Date.now(),
  });
  const lookbackDays = Math.max(30, Math.round(params.lookbackDays ?? 730));
  const windowStartDate = toIsoDate(asOfMs - lookbackDays * DAY_MS);
  const windowStartMs = asOfMs - lookbackDays * DAY_MS;
  const eventLimit = Math.max(1, Math.round(params.eventLimit ?? 40));
  const factLimit = Math.max(1, Math.round(params.factLimit ?? 300));
  const metricLimit = Math.max(1, Math.round(params.metricLimit ?? 40));
  const entity = db
    .prepare(
      `SELECT id, canonical_name
       FROM research_entities
       WHERE kind='company' AND ticker=?
       ORDER BY updated_at DESC
       LIMIT 1`,
    )
    .get(ticker) as { id?: number; canonical_name?: string } | undefined;
  if (!entity?.id) {
    return {
      ticker,
      asOfDate,
      windowStartDate,
      events: [],
      facts: [],
      metrics: [],
    };
  }

  const eventRows = db
    .prepare(
      `SELECT
         id, entity_id, event_type, event_time, period_start, period_end, source_table, source_ref_id,
         source_url, title, payload, event_hash, created_at, updated_at
       FROM research_events
       WHERE entity_id=?
         AND event_time >= ?
         AND event_time <= ?
       ORDER BY event_time DESC, id DESC
       LIMIT ?`,
    )
    .all(entity.id, windowStartMs, asOfMs, eventLimit) as Array<{
    id: number;
    entity_id: number;
    event_type: string;
    event_time: number;
    period_start: string;
    period_end: string;
    source_table: string;
    source_ref_id: number;
    source_url: string;
    title: string;
    payload?: string;
    event_hash: string;
    created_at: number;
    updated_at: number;
  }>;
  const factRows = db
    .prepare(
      `SELECT
         rf.id, rf.entity_id, rf.event_id, rf.metric_key, rf.metric_kind, rf.value_num, rf.value_text,
         rf.unit, rf.direction, rf.confidence, rf.as_of_date, rf.valid_from, rf.valid_to,
         rf.source_table, rf.source_ref_id, rf.source_url, rf.metadata, rf.fact_hash, rf.created_at,
         rf.updated_at
       FROM research_facts rf
       LEFT JOIN research_events re ON re.id = rf.event_id
       WHERE rf.entity_id=?
         AND (? = '' OR rf.as_of_date = '' OR rf.as_of_date <= ?)
         AND (? = '' OR rf.valid_from = '' OR rf.valid_from <= ?)
         AND (? = '' OR rf.valid_to = '' OR rf.valid_to >= ?)
         AND (rf.as_of_date <> '' OR re.event_time IS NULL OR re.event_time <= ?)
       ORDER BY
         CASE WHEN rf.as_of_date = '' THEN '0000-00-00' ELSE rf.as_of_date END DESC,
         rf.updated_at DESC,
         rf.id DESC
       LIMIT ?`,
    )
    .all(
      entity.id,
      asOfDate,
      asOfDate,
      asOfDate,
      asOfDate,
      asOfDate,
      asOfDate,
      asOfMs,
      factLimit,
    ) as Array<{
    id: number;
    entity_id: number;
    event_id?: number | null;
    metric_key: string;
    metric_kind: string;
    value_num?: number | null;
    value_text: string;
    unit: string;
    direction: string;
    confidence: number;
    as_of_date: string;
    valid_from: string;
    valid_to: string;
    source_table: string;
    source_ref_id: number;
    source_url: string;
    metadata?: string;
    fact_hash: string;
    created_at: number;
    updated_at: number;
  }>;
  const events = eventRows.map(parseEventRow);
  const facts = factRows.map(parseFactRow);
  const metrics = buildMetricSeries(facts, metricLimit);
  return {
    ticker,
    entityId: entity.id,
    entityName: entity.canonical_name || ticker,
    asOfDate,
    windowStartDate,
    events,
    facts,
    metrics,
  };
};
