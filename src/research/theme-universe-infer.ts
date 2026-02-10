import { openResearchDb } from "./db.js";

const normalizeSpaces = (value: string): string => value.replaceAll(/\s+/g, " ").trim();

const normalizeTicker = (value: string): string =>
  value
    .trim()
    .toUpperCase()
    .replaceAll(/[^A-Z0-9.]/g, "")
    .slice(0, 10);

const STOP_TOKENS = new Set(
  [
    "A",
    "AN",
    "AND",
    "ARE",
    "AS",
    "AT",
    "BE",
    "BY",
    "FOR",
    "FROM",
    "HAS",
    "IF",
    "IN",
    "IS",
    "IT",
    "ITS",
    "OF",
    "ON",
    "OR",
    "THAT",
    "THE",
    "THESE",
    "THIS",
    "TO",
    "WAS",
    "WE",
    "WILL",
    "WITH",
    // Domain / research noise
    "AI",
    "API",
    "APIS",
    "SDK",
    "SLA",
    "SOC",
    "KPI",
    "KPIS",
    "GAAP",
    "NON",
    "USD",
    "EUR",
    "ET",
    "FY",
    "Q",
    "YOY",
    "QOQ",
    "N",
    "PDF",
    "SEC",
    "FORM",
    "CEO",
    "CFO",
  ].map((t) => t.toUpperCase()),
);

const extractTickers = (text: string): string[] => {
  const out: string[] = [];
  // Bias toward equities/crypto tickers; keep it simple and fast.
  const tokens = text.match(/\b[A-Z][A-Z0-9.]{1,7}\b/g) ?? [];
  for (const raw of tokens) {
    const t = normalizeTicker(raw);
    if (!t) continue;
    if (STOP_TOKENS.has(t)) continue;
    // Avoid obvious non-tickers.
    if (/^\d+$/.test(t)) continue;
    out.push(t);
  }
  return out;
};

const extractDomains = (text: string): string[] => {
  const urls = text.match(/https?:\/\/\S+/gi) ?? [];
  const domains: string[] = [];
  for (const u of urls) {
    try {
      const url = new URL(u.replace(/[),.;]+$/g, ""));
      const host = url.hostname.toLowerCase();
      if (!host) continue;
      // Strip common prefixes.
      const normalized = host.replace(/^www\./, "");
      domains.push(normalized);
    } catch {
      // ignore
    }
  }
  return domains;
};

export type InferredThemeUniverse = {
  theme: string;
  scanned_docs: number;
  inferred_tickers: string[];
  inferred_domains: string[];
  note: string;
};

export function inferThemeUniverseFromDb(params: {
  theme: string;
  dbPath?: string;
  maxDocs?: number;
  maxTickers?: number;
  maxDomains?: number;
}): InferredThemeUniverse {
  const theme = normalizeSpaces(params.theme);
  const maxDocs = Math.max(10, Math.min(500, Math.round(params.maxDocs ?? 120)));
  const maxTickers = Math.max(5, Math.min(60, Math.round(params.maxTickers ?? 20)));
  const maxDomains = Math.max(5, Math.min(60, Math.round(params.maxDomains ?? 20)));

  const db = openResearchDb(params.dbPath);

  // Pull recent docs; bias toward newsletters/research emails, but allow all.
  const rows = db
    .prepare(
      `SELECT id, source_type, provider, title, subject, url, content, fetched_at, received_at, published_at
       FROM external_documents
       WHERE (
         lower(coalesce(title,'')) LIKE '%' || lower(?) || '%'
         OR lower(coalesce(subject,'')) LIKE '%' || lower(?) || '%'
         OR lower(coalesce(content,'')) LIKE '%' || lower(?) || '%'
       )
       ORDER BY received_at DESC, fetched_at DESC
       LIMIT ?`,
    )
    .all(theme, theme, theme, maxDocs) as Array<{
    id: number;
    source_type?: string;
    provider?: string;
    title?: string;
    subject?: string;
    url?: string;
    content?: string;
    fetched_at?: number;
    received_at?: string;
    published_at?: string;
  }>;

  const tickerCounts = new Map<string, number>();
  const domainCounts = new Map<string, number>();

  for (const row of rows) {
    const combined = normalizeSpaces(
      [row.title ?? "", row.subject ?? "", row.url ?? "", row.content ?? ""].join("\n"),
    );
    for (const t of extractTickers(combined)) {
      tickerCounts.set(t, (tickerCounts.get(t) ?? 0) + 1);
    }
    for (const d of extractDomains(combined)) {
      domainCounts.set(d, (domainCounts.get(d) ?? 0) + 1);
    }
  }

  const inferred_tickers = Array.from(tickerCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([t]) => t)
    .slice(0, maxTickers);
  const inferred_domains = Array.from(domainCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([d]) => d)
    .slice(0, maxDomains);

  return {
    theme,
    scanned_docs: rows.length,
    inferred_tickers,
    inferred_domains,
    note: "Universe inferred from external_documents by keyword match; confirm/override with 'tickers: ...' for better coverage.",
  };
}
