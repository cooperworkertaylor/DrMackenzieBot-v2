import { setTimeout as delay } from "node:timers/promises";
import { defaultRuntime } from "../runtime.js";

const BASE = "https://www.alphavantage.co/query";

type DailyResponse = {
  "Time Series (Daily)"?: Record<
    string,
    {
      "1. open": string;
      "2. high": string;
      "3. low": string;
      "4. close": string;
      "6. volume"?: string;
      "5. adjusted close"?: string;
      "7. dividend amount"?: string;
      "8. split coefficient"?: string;
    }
  >;
  Note?: string;
  Information?: string;
};

type EarningsResponse = {
  annualEarnings?: Array<{
    fiscalDateEnding?: string;
    reportedEPS?: string;
  }>;
  quarterlyEarnings?: Array<{
    fiscalDateEnding?: string;
    reportedDate?: string;
    reportedEPS?: string;
    estimatedEPS?: string;
    surprise?: string;
    surprisePercentage?: string;
    reportTime?: string;
  }>;
  Note?: string;
  Information?: string;
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

export const fetchAlphaVantageDaily = async (
  ticker: string,
  apiKey: string,
  { retries = 3, pauseMs = 15000 }: { retries?: number; pauseMs?: number } = {},
): Promise<PriceRow[]> => {
  const params = new URLSearchParams({
    function: "TIME_SERIES_DAILY_ADJUSTED",
    symbol: ticker,
    outputsize: "full",
    apikey: apiKey,
  });
  const url = `${BASE}?${params.toString()}`;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`AlphaVantage HTTP ${res.status}`);
    const body = (await res.json()) as DailyResponse;
    if (body.Information || body.Note) {
      if (attempt >= retries) {
        throw new Error(`AlphaVantage throttled: ${body.Information ?? body.Note}`);
      }
      await delay(pauseMs);
      continue;
    }
    const series = body["Time Series (Daily)"] ?? {};
    return Object.entries(series).map(([date, row]) => ({
      date,
      open: Number(row["1. open"]),
      high: Number(row["2. high"]),
      low: Number(row["3. low"]),
      close: Number(row["5. adjusted close"] ?? row["4. close"]),
      volume: Number(row["6. volume"] ?? 0),
      source: "alphavantage",
    }));
  }
  throw new Error("AlphaVantage: unexpected retry exhaustion");
};

export const fetchAlphaVantageEarnings = async (
  ticker: string,
  apiKey: string,
  { retries = 3, pauseMs = 15000 }: { retries?: number; pauseMs?: number } = {},
): Promise<EarningsRow[]> => {
  const params = new URLSearchParams({
    function: "EARNINGS",
    symbol: ticker,
    apikey: apiKey,
  });
  const url = `${BASE}?${params.toString()}`;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`AlphaVantage HTTP ${res.status}`);
    const body = (await res.json()) as EarningsResponse;
    if (body.Information || body.Note) {
      if (attempt >= retries) {
        throw new Error(`AlphaVantage throttled: ${body.Information ?? body.Note}`);
      }
      await delay(pauseMs);
      continue;
    }
    const sourceUrl = "https://www.alphavantage.co/documentation/#earnings";
    const annual: EarningsRow[] = (body.annualEarnings ?? [])
      .map((row) => ({
        periodType: "annual" as const,
        fiscalDateEnding: row.fiscalDateEnding?.trim() ?? "",
        reportedEps: toNumber(row.reportedEPS),
        source: "alphavantage",
        sourceUrl,
      }))
      .filter((row) => Boolean(row.fiscalDateEnding));
    const quarterly: EarningsRow[] = (body.quarterlyEarnings ?? [])
      .map((row) => ({
        periodType: "quarterly" as const,
        fiscalDateEnding: row.fiscalDateEnding?.trim() ?? "",
        reportedDate: row.reportedDate?.trim() || undefined,
        reportedEps: toNumber(row.reportedEPS),
        estimatedEps: toNumber(row.estimatedEPS),
        surprise: toNumber(row.surprise),
        surprisePct: toNumber(row.surprisePercentage),
        reportTime: row.reportTime?.trim() || undefined,
        source: "alphavantage",
        sourceUrl,
      }))
      .filter((row) => Boolean(row.fiscalDateEnding));
    return [...annual, ...quarterly];
  }
  throw new Error("AlphaVantage: unexpected retry exhaustion");
};

export const ensureApiKey = (): string => {
  const key = process.env.ALPHA_VANTAGE_API_KEY;
  if (!key) {
    defaultRuntime.error("Set ALPHA_VANTAGE_API_KEY to fetch prices.");
    throw new Error("Missing ALPHA_VANTAGE_API_KEY");
  }
  return key.trim();
};
