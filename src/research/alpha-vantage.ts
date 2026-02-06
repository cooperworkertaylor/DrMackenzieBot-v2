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

export type PriceRow = {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  source: string;
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

export const ensureApiKey = (): string => {
  const key = process.env.ALPHA_VANTAGE_API_KEY;
  if (!key) {
    defaultRuntime.error("Set ALPHA_VANTAGE_API_KEY to fetch prices.");
    throw new Error("Missing ALPHA_VANTAGE_API_KEY");
  }
  return key.trim();
};
