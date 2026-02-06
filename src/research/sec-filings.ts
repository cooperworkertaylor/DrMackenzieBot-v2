import { setTimeout as delay } from "node:timers/promises";
import { createSecHeaders, padCik } from "../agents/sec-xbrl-timeseries.js";
import { chunkText } from "./chunker.js";

export type FilingMeta = {
  accession: string;
  form: string;
  filed: string;
  periodEnd?: string;
  primaryDoc?: string;
  url: string;
  title?: string;
};

export type FilingText = FilingMeta & { text: string };

const SUBMISSIONS_BASE = "https://data.sec.gov/submissions";

export const fetchRecentFilings = async (
  cik: string,
  userAgent?: string,
  limit = 50,
): Promise<FilingMeta[]> => {
  const normalized = padCik(cik);
  const res = await fetch(`${SUBMISSIONS_BASE}/CIK${normalized}.json`, {
    headers: createSecHeaders(userAgent),
  });
  if (!res.ok) throw new Error(`SEC submissions HTTP ${res.status}`);
  const body = (await res.json()) as any;
  const recent = body.filings?.recent;
  if (!recent) return [];
  const out: FilingMeta[] = [];
  const n = Math.min(recent.accessionNumber.length, limit);
  for (let i = 0; i < n; i += 1) {
    const accessionRaw = recent.accessionNumber[i] as string;
    const accession = accessionRaw.replace(/-/g, "");
    const form = recent.form[i] as string;
    const filed = recent.filingDate[i] as string;
    const periodEnd = recent.reportDate?.[i] as string | undefined;
    const primaryDoc = recent.primaryDocument?.[i] as string | undefined;
    const url = `https://www.sec.gov/Archives/edgar/data/${Number(normalized)}/${accession}/${primaryDoc ?? ""}`;
    out.push({ accession, form, filed, periodEnd, primaryDoc, url });
  }
  return out;
};

export const downloadFilingText = async (
  meta: FilingMeta,
  userAgent?: string,
): Promise<FilingText> => {
  const res = await fetch(meta.url, { headers: createSecHeaders(userAgent) });
  if (!res.ok) throw new Error(`SEC filing fetch failed: HTTP ${res.status}`);
  const contentType = res.headers.get("content-type") ?? "";
  const raw = await res.text();
  const text =
    contentType.includes("html") || raw.includes("<html") ? extractTextFromHtml(raw) : raw;
  return { ...meta, text: text.trim() };
};

const extractTextFromHtml = (html: string): string => {
  const withoutScripts = html.replace(/<script[\s\S]*?<\/script>/gi, "");
  const withoutStyles = withoutScripts.replace(/<style[\s\S]*?<\/style>/gi, "");
  const stripped = withoutStyles.replace(/<[^>]+>/g, " ");
  return stripped.replace(/\s+/g, " ").trim();
};

export const chunkFiling = (filing: FilingText) => chunkText(filing.text, 256);

export const politeThrottle = async (ms = 200) => {
  await delay(ms);
};
