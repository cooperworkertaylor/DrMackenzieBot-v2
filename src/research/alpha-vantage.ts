import { setTimeout as delay } from "node:timers/promises";
import { defaultRuntime } from "../runtime.js";

const BASE = "https://api.polygon.io";

type AggregateResponse = {
  status?: string;
  error?: string;
  message?: string;
  results?: Array<{
    t?: number;
    o?: number;
    h?: number;
    l?: number;
    c?: number;
    v?: number;
  }>;
};

type FinancialStatementValue = {
  value?: number | string;
};

type FinancialsResult = {
  timeframe?: string;
  filing_date?: string;
  acceptance_datetime?: string;
  end_date?: string;
  source_filing_url?: string;
  source_filing_file_url?: string;
  financials?: {
    income_statement?: Record<string, FinancialStatementValue | undefined>;
  };
};

type FinancialsResponse = {
  status?: string;
  error?: string;
  message?: string;
  next_url?: string;
  results?: FinancialsResult[];
};

export type PriceRow = {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  source: string;
};

export type EarningsRow = {
  periodType: "quarterly" | "annual";
  fiscalDateEnding: string;
  reportedDate?: string;
  reportedEps?: number;
  estimatedEps?: number;
  surprise?: number;
  surprisePct?: number;
  reportTime?: string;
  source: string;
  sourceUrl: string;
};

const toNumber = (raw?: string): number | undefined => {
  if (!raw) return undefined;
  const trimmed = raw.trim();
  if (!trimmed || /^none$/i.test(trimmed)) return undefined;
  const value = Number(trimmed);
  return Number.isFinite(value) ? value : undefined;
};

const toYmd = (epochMs?: number): string | undefined => {
  if (typeof epochMs !== "number" || !Number.isFinite(epochMs)) return undefined;
  return new Date(epochMs).toISOString().slice(0, 10);
};

const parseValue = (value: unknown): number | undefined => {
  if (typeof value === "number") return Number.isFinite(value) ? value : undefined;
  if (typeof value === "string") return toNumber(value);
  return undefined;
};

const parseEps = (row: FinancialsResult): number | undefined => {
  const income = row.financials?.income_statement;
  if (!income) return undefined;
  const candidates = [
    income.diluted_earnings_per_share?.value,
    income.basic_earnings_per_share?.value,
    income.earnings_per_share_diluted?.value,
    income.earnings_per_share_basic?.value,
    income.net_income_loss_available_to_common_stockholders_basic?.value,
  ];
  for (const candidate of candidates) {
    const parsed = parseValue(candidate);
    if (typeof parsed === "number") return parsed;
  }
  return undefined;
};

const resolveMassiveApiKey = (provided?: string): string | undefined => {
  const key =
    provided?.trim() || process.env.MASSIVE_API_KEY?.trim() || process.env.POLYGON_API_KEY?.trim();
  return key ? key : undefined;
};

const appendApiKey = (url: string, apiKey: string): string => {
  const parsed = new URL(url);
  if (!parsed.searchParams.has("apiKey")) {
    parsed.searchParams.set("apiKey", apiKey);
  }
  return parsed.toString();
};

const fetchJsonWithRetries = async <T>(
  url: string,
  opts: { retries: number; pauseMs: number },
): Promise<T> => {
  let lastError: Error | undefined;
  for (let attempt = 0; attempt <= opts.retries; attempt += 1) {
    try {
      const res = await fetch(url);
      if (res.status === 429 || res.status === 503) {
        if (attempt >= opts.retries) {
          throw new Error(`Massive throttled (HTTP ${res.status})`);
        }
        await delay(opts.pauseMs);
        continue;
      }
      if (!res.ok) {
        throw new Error(`Massive HTTP ${res.status}`);
      }
      const json = (await res.json()) as T;
      return json;
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      if (attempt >= opts.retries) break;
      await delay(opts.pauseMs);
    }
  }
  throw lastError ?? new Error("Massive request failed");
};

export const fetchAlphaVantageDaily = async (
  ticker: string,
  apiKey: string,
  { retries = 3, pauseMs = 15000 }: { retries?: number; pauseMs?: number } = {},
): Promise<PriceRow[]> => {
  const resolved = resolveMassiveApiKey(apiKey);
  if (!resolved) throw new Error("Missing MASSIVE_API_KEY");
  const endDate = new Date().toISOString().slice(0, 10);
  const startDate = "2000-01-01";
  const url = `${BASE}/v2/aggs/ticker/${encodeURIComponent(
    ticker.trim().toUpperCase(),
  )}/range/1/day/${startDate}/${endDate}?adjusted=true&sort=asc&limit=50000&apiKey=${encodeURIComponent(resolved)}`;
  const body = await fetchJsonWithRetries<AggregateResponse>(url, { retries, pauseMs });
  if (body.error) throw new Error(`Massive error: ${body.error}`);
  if (body.message && /error|invalid|not found|permission/i.test(body.message)) {
    throw new Error(`Massive error: ${body.message}`);
  }
  return (body.results ?? [])
    .map((row) => {
      const date = toYmd(row.t);
      const open = parseValue(row.o);
      const high = parseValue(row.h);
      const low = parseValue(row.l);
      const close = parseValue(row.c);
      const volume = parseValue(row.v) ?? 0;
      if (
        !date ||
        typeof open !== "number" ||
        typeof high !== "number" ||
        typeof low !== "number" ||
        typeof close !== "number"
      ) {
        return undefined;
      }
      return {
        date,
        open,
        high,
        low,
        close,
        volume,
        source: "massive",
      } satisfies PriceRow;
    })
    .filter((row): row is PriceRow => Boolean(row));
};

export const fetchAlphaVantageEarnings = async (
  ticker: string,
  apiKey: string,
  { retries = 3, pauseMs = 15000 }: { retries?: number; pauseMs?: number } = {},
): Promise<EarningsRow[]> => {
  const resolved = resolveMassiveApiKey(apiKey);
  if (!resolved) throw new Error("Missing MASSIVE_API_KEY");
  const tickerNorm = ticker.trim().toUpperCase();

  const fetchFinancials = async (
    timeframe: "quarterly" | "annual",
    maxPages = 8,
  ): Promise<FinancialsResult[]> => {
    let url = `${BASE}/vX/reference/financials?ticker=${encodeURIComponent(
      tickerNorm,
    )}&timeframe=${timeframe}&order=desc&sort=filing_date&limit=100&apiKey=${encodeURIComponent(resolved)}`;
    const out: FinancialsResult[] = [];
    let page = 0;
    while (url && page < maxPages) {
      const body = await fetchJsonWithRetries<FinancialsResponse>(url, { retries, pauseMs });
      if (body.error) throw new Error(`Massive error: ${body.error}`);
      out.push(...(body.results ?? []));
      const next = body.next_url?.trim();
      url = next ? appendApiKey(next, resolved) : "";
      page += 1;
    }
    return out;
  };

  const sourceUrl = "https://massive.com/docs/rest/stocks/fundamentals/financials";
  const mapRows = (periodType: "quarterly" | "annual", rows: FinancialsResult[]): EarningsRow[] => {
    const out: EarningsRow[] = [];
    for (const row of rows) {
      const fiscalDateEnding = row.end_date?.trim() ?? "";
      if (!fiscalDateEnding) continue;
      const reportedDate = row.filing_date?.trim() || row.acceptance_datetime?.slice(0, 10);
      const reportedEps = parseEps(row);
      out.push({
        periodType,
        fiscalDateEnding,
        ...(reportedDate ? { reportedDate } : {}),
        ...(typeof reportedEps === "number" ? { reportedEps } : {}),
        source: "massive",
        sourceUrl: row.source_filing_url?.trim() || row.source_filing_file_url?.trim() || sourceUrl,
      });
    }
    return out;
  };

  const quarterly = mapRows("quarterly", await fetchFinancials("quarterly"));
  const annual = mapRows("annual", await fetchFinancials("annual"));
  const deduped = new Map<string, EarningsRow>();
  for (const row of [...quarterly, ...annual]) {
    const key = `${row.periodType}|${row.fiscalDateEnding}|${row.reportedDate ?? ""}`;
    if (!deduped.has(key)) deduped.set(key, row);
  }
  return Array.from(deduped.values());
};

export const ensureApiKey = (): string => {
  const key = resolveMassiveApiKey();
  if (!key) {
    defaultRuntime.error("Set MASSIVE_API_KEY (or POLYGON_API_KEY) to fetch prices via Massive.");
    throw new Error("Missing MASSIVE_API_KEY");
  }
  return key.trim();
};
