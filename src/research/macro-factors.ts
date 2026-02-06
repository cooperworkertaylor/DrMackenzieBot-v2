import { setTimeout as delay } from "node:timers/promises";
import { ensureApiKey, fetchAlphaVantageDaily, type PriceRow } from "./alpha-vantage.js";
import { openResearchDb } from "./db.js";

export type MacroFactorKey = "rates" | "credit_spread" | "dollar" | "oil" | "vix";

export type MacroFactorObservation = {
  factorKey: MacroFactorKey;
  date: string;
  value: number;
  source: string;
  sourceUrl: string;
  metadata: Record<string, unknown>;
  fetchedAt: number;
};

export type MacroIngestFactorResult = {
  factorKey: MacroFactorKey;
  observations: number;
  startDate?: string;
  endDate?: string;
};

export type MacroIngestResult = {
  fetchedAt: string;
  factors: MacroIngestFactorResult[];
};

const DEFAULT_SOURCE = "alphavantage_macro_proxy";
const DEFAULT_SOURCE_URL = "https://www.alphavantage.co/documentation/";

export const DEFAULT_MACRO_FACTOR_KEYS: MacroFactorKey[] = [
  "rates",
  "credit_spread",
  "dollar",
  "oil",
  "vix",
];

const normalizeDate = (value: string): string => {
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
  const parsed = new Date(trimmed);
  if (!Number.isFinite(parsed.getTime())) {
    throw new Error(`Invalid date: ${value}`);
  }
  return parsed.toISOString().slice(0, 10);
};

const toFactorKey = (value: string): MacroFactorKey => {
  const normalized = value.trim().toLowerCase();
  if (normalized === "rates" || normalized === "rate" || normalized === "yield") return "rates";
  if (normalized === "credit" || normalized === "credit_spread" || normalized === "credit-spread") {
    return "credit_spread";
  }
  if (normalized === "dollar" || normalized === "usd") return "dollar";
  if (normalized === "oil" || normalized === "wti") return "oil";
  if (normalized === "vix" || normalized === "vol" || normalized === "volatility") return "vix";
  throw new Error(`Unsupported macro factor key: ${value}`);
};

const sortPriceRows = (rows: PriceRow[]): PriceRow[] =>
  [...rows].sort((left, right) => left.date.localeCompare(right.date));

const toReturnSeries = (rows: PriceRow[]): Map<string, number> => {
  const sorted = sortPriceRows(rows);
  const out = new Map<string, number>();
  for (let index = 1; index < sorted.length; index += 1) {
    const prev = sorted[index - 1]?.close;
    const next = sorted[index]?.close;
    const date = sorted[index]?.date;
    if (
      typeof prev !== "number" ||
      typeof next !== "number" ||
      !Number.isFinite(prev) ||
      !Number.isFinite(next) ||
      Math.abs(prev) <= 1e-9 ||
      !date
    ) {
      continue;
    }
    const ret = next / prev - 1;
    if (!Number.isFinite(ret)) continue;
    out.set(date, ret);
  }
  return out;
};

const upsertRows = (params: {
  factorKey: MacroFactorKey;
  rows: Array<{ date: string; value: number; source: string; sourceUrl: string; metadata: string }>;
  dbPath?: string;
}): number => {
  if (!params.rows.length) return 0;
  const db = openResearchDb(params.dbPath);
  const stmt = db.prepare(
    `INSERT INTO macro_factor_observations (
       factor_key, date, value, source, source_url, metadata, fetched_at
     )
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(factor_key, date, source) DO UPDATE SET
       value=excluded.value,
       source_url=excluded.source_url,
       metadata=excluded.metadata,
       fetched_at=excluded.fetched_at`,
  );
  const now = Date.now();
  db.exec("BEGIN");
  try {
    for (const row of params.rows) {
      stmt.run(params.factorKey, row.date, row.value, row.source, row.sourceUrl, row.metadata, now);
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return params.rows.length;
};

export const upsertMacroFactorObservations = (params: {
  factorKey: MacroFactorKey | string;
  observations: Array<{
    date: string;
    value: number;
    source?: string;
    sourceUrl?: string;
    metadata?: Record<string, unknown>;
  }>;
  dbPath?: string;
}): number => {
  const factorKey = toFactorKey(params.factorKey);
  const rows = params.observations
    .filter((row) => Number.isFinite(row.value))
    .map((row) => ({
      date: normalizeDate(row.date),
      value: Number(row.value),
      source: row.source?.trim() || DEFAULT_SOURCE,
      sourceUrl: row.sourceUrl?.trim() || DEFAULT_SOURCE_URL,
      metadata: JSON.stringify(row.metadata ?? {}),
    }));
  return upsertRows({
    factorKey,
    rows,
    dbPath: params.dbPath,
  });
};

export const listMacroFactorObservations = (
  params: {
    factorKey?: MacroFactorKey | string;
    startDate?: string;
    endDate?: string;
    limit?: number;
    dbPath?: string;
  } = {},
): MacroFactorObservation[] => {
  const db = openResearchDb(params.dbPath);
  const factorKey = params.factorKey ? toFactorKey(params.factorKey) : "";
  const startDate = params.startDate ? normalizeDate(params.startDate) : "";
  const endDate = params.endDate ? normalizeDate(params.endDate) : "";
  const limit = Math.max(1, Math.round(params.limit ?? 500));
  const rows = db
    .prepare(
      `SELECT
         factor_key,
         date,
         value,
         source,
         source_url,
         metadata,
         fetched_at
       FROM macro_factor_observations
       WHERE (? = '' OR factor_key = ?)
         AND (? = '' OR date >= ?)
         AND (? = '' OR date <= ?)
       ORDER BY date DESC, factor_key ASC
       LIMIT ?`,
    )
    .all(factorKey, factorKey, startDate, startDate, endDate, endDate, limit) as Array<{
    factor_key: string;
    date: string;
    value: number;
    source: string;
    source_url: string;
    metadata: string;
    fetched_at: number;
  }>;
  return rows.map((row) => ({
    factorKey: toFactorKey(row.factor_key),
    date: row.date,
    value: row.value,
    source: row.source,
    sourceUrl: row.source_url,
    metadata: (() => {
      try {
        const parsed = JSON.parse(row.metadata) as unknown;
        return parsed && typeof parsed === "object" && !Array.isArray(parsed)
          ? (parsed as Record<string, unknown>)
          : {};
      } catch {
        return {};
      }
    })(),
    fetchedAt: row.fetched_at,
  }));
};

export const loadMacroFactorSeries = (
  params: {
    factorKeys?: Array<MacroFactorKey | string>;
    startDate?: string;
    endDate?: string;
    dbPath?: string;
  } = {},
): Partial<Record<MacroFactorKey, Map<string, number>>> => {
  const keysRaw = params.factorKeys?.length
    ? params.factorKeys.map((value) => toFactorKey(value))
    : DEFAULT_MACRO_FACTOR_KEYS;
  const factorKeys = Array.from(new Set(keysRaw));
  if (!factorKeys.length) return {};
  const db = openResearchDb(params.dbPath);
  const startDate = params.startDate ? normalizeDate(params.startDate) : "";
  const endDate = params.endDate ? normalizeDate(params.endDate) : "";
  const placeholders = factorKeys.map(() => "?").join(",");
  const rows = db
    .prepare(
      `SELECT factor_key, date, value
       FROM macro_factor_observations
       WHERE factor_key IN (${placeholders})
         AND (? = '' OR date >= ?)
         AND (? = '' OR date <= ?)
       ORDER BY date ASC`,
    )
    .all(...factorKeys, startDate, startDate, endDate, endDate) as Array<{
    factor_key: string;
    date: string;
    value: number;
  }>;
  const out: Partial<Record<MacroFactorKey, Map<string, number>>> = {};
  for (const key of factorKeys) out[key] = new Map<string, number>();
  for (const row of rows) {
    const key = toFactorKey(row.factor_key);
    out[key]?.set(row.date, row.value);
  }
  return out;
};

const buildCreditSpreadSeries = (
  hyg: Map<string, number>,
  lqd: Map<string, number>,
): Map<string, number> => {
  const out = new Map<string, number>();
  for (const [date, hygValue] of hyg) {
    const lqdValue = lqd.get(date);
    if (typeof lqdValue !== "number") continue;
    out.set(date, hygValue - lqdValue);
  }
  return out;
};

const toObservationRows = (params: {
  series: Map<string, number>;
  source: string;
  sourceUrl: string;
  metadata: Record<string, unknown>;
}): Array<{ date: string; value: number; source: string; sourceUrl: string; metadata: string }> =>
  Array.from(params.series.entries())
    .map(([date, value]) => ({ date, value }))
    .filter((row) => Number.isFinite(row.value))
    .sort((left, right) => left.date.localeCompare(right.date))
    .map((row) => ({
      date: row.date,
      value: row.value,
      source: params.source,
      sourceUrl: params.sourceUrl,
      metadata: JSON.stringify(params.metadata),
    }));

export const ingestDefaultMacroFactors = async (
  params: {
    apiKey?: string;
    retries?: number;
    pauseMs?: number;
    dbPath?: string;
  } = {},
): Promise<MacroIngestResult> => {
  const apiKey = params.apiKey?.trim() || ensureApiKey();
  const pauseMs = Math.max(500, Math.round(params.pauseMs ?? 12500));
  const retries = Math.max(1, Math.round(params.retries ?? 3));
  const proxySymbols = ["IEF", "HYG", "LQD", "UUP", "USO", "VIXY"] as const;
  const priceSeries: Record<string, PriceRow[]> = {};
  for (let index = 0; index < proxySymbols.length; index += 1) {
    const symbol = proxySymbols[index]!;
    priceSeries[symbol] = await fetchAlphaVantageDaily(symbol, apiKey, {
      retries,
      pauseMs,
    });
    if (index < proxySymbols.length - 1) {
      await delay(pauseMs);
    }
  }
  const returnsIEF = toReturnSeries(priceSeries.IEF ?? []);
  const returnsHYG = toReturnSeries(priceSeries.HYG ?? []);
  const returnsLQD = toReturnSeries(priceSeries.LQD ?? []);
  const returnsUUP = toReturnSeries(priceSeries.UUP ?? []);
  const returnsUSO = toReturnSeries(priceSeries.USO ?? []);
  const returnsVIXY = toReturnSeries(priceSeries.VIXY ?? []);

  const rates = new Map<string, number>();
  for (const [date, ret] of returnsIEF) rates.set(date, -ret);

  const factorSeries: Record<MacroFactorKey, Map<string, number>> = {
    rates,
    credit_spread: buildCreditSpreadSeries(returnsHYG, returnsLQD),
    dollar: returnsUUP,
    oil: returnsUSO,
    vix: returnsVIXY,
  };

  const factorMetadata: Record<MacroFactorKey, Record<string, unknown>> = {
    rates: { proxy: "IEF", transform: "negative_daily_return" },
    credit_spread: { proxy_long: "HYG", proxy_short: "LQD", transform: "return_spread" },
    dollar: { proxy: "UUP", transform: "daily_return" },
    oil: { proxy: "USO", transform: "daily_return" },
    vix: { proxy: "VIXY", transform: "daily_return" },
  };

  const factors: MacroIngestFactorResult[] = [];
  for (const key of DEFAULT_MACRO_FACTOR_KEYS) {
    const rows = toObservationRows({
      series: factorSeries[key],
      source: DEFAULT_SOURCE,
      sourceUrl: DEFAULT_SOURCE_URL,
      metadata: factorMetadata[key],
    });
    const observations = upsertRows({
      factorKey: key,
      rows,
      dbPath: params.dbPath,
    });
    factors.push({
      factorKey: key,
      observations,
      startDate: rows[0]?.date,
      endDate: rows[rows.length - 1]?.date,
    });
  }

  return {
    fetchedAt: new Date().toISOString(),
    factors,
  };
};

export const __testOnly = {
  toFactorKey,
  toReturnSeries,
  buildCreditSpreadSeries,
};
