import type { QueryPlan, RetrievalFilters } from "./types.js";

const STOP_WORDS = new Set([
  "the",
  "a",
  "an",
  "of",
  "for",
  "and",
  "to",
  "in",
  "on",
  "with",
  "vs",
  "is",
  "are",
  "was",
  "were",
  "from",
  "by",
  "at",
  "as",
]);

const KPI_SYNONYMS: Record<string, string[]> = {
  revenue: ["sales", "top line", "turnover"],
  margin: ["gross margin", "operating margin", "profitability"],
  growth: ["yoy", "year over year", "cagr"],
  capex: ["capital expenditure", "capital spending"],
  rpo: ["remaining performance obligations", "backlog"],
  freecashflow: ["fcf", "free cash flow"],
  ecl: ["expected credit loss", "credit loss rate"],
};

const UPPERCASE_NOT_TICKER = new Set([
  "AI",
  "LLM",
  "RAG",
  "KPI",
  "RPO",
  "EPS",
  "AWS",
  "CEO",
  "CFO",
  "USD",
  "YOY",
  "FCF",
  "SEC",
]);

const COMPANY_TO_TICKER: Record<string, string> = {
  nvidia: "NVDA",
  microsoft: "MSFT",
  amazon: "AMZN",
  alphabet: "GOOGL",
  google: "GOOGL",
  meta: "META",
  asml: "ASML",
  tsmc: "TSM",
  palantir: "PLTR",
  nu: "NU",
};

const DOC_TYPE_HINTS: Array<{ pattern: RegExp; docType: string }> = [
  { pattern: /\b(10-k|10q|20-f|8-k|filing|sec)\b/i, docType: "filing" },
  { pattern: /\b(transcript|earnings call|q[1-4])\b/i, docType: "transcript" },
  { pattern: /\b(news|reportedly|press release)\b/i, docType: "news" },
  { pattern: /\b(memo|letter)\b/i, docType: "memo" },
  { pattern: /\b(research|analysis)\b/i, docType: "research" },
];

function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .split(/\s+/)
    .filter(Boolean);
}

function buildQueryRewrites(question: string, limit: number): string[] {
  const clean = question.trim();
  if (!clean) {
    return [];
  }

  const variants = new Set<string>([clean]);
  const tokens = tokenize(clean).filter((token) => !STOP_WORDS.has(token));
  if (tokens.length > 0) {
    variants.add(tokens.join(" "));
  }

  const upperTicker = clean.match(/\b[A-Z]{1,5}\b/g) ?? [];
  if (upperTicker.length > 0) {
    variants.add(`${upperTicker.join(" ")} ${tokens.join(" ")}`.trim());
  }

  for (const token of tokens) {
    const synonyms = KPI_SYNONYMS[token.replace(/\s+/g, "")];
    if (!synonyms) {
      continue;
    }
    variants.add(`${clean} ${synonyms.join(" ")}`.trim());
  }

  const years = clean.match(/\b20\d{2}\b/g) ?? [];
  if (years.length > 0) {
    variants.add(`${tokens.join(" ")} ${years.join(" ")}`.trim());
  }

  return Array.from(variants).filter(Boolean).slice(0, Math.max(3, Math.min(5, limit)));
}

function extractMetricHints(question: string): string[] {
  const out = new Set<string>();
  const lower = question.toLowerCase();
  for (const [metric, synonyms] of Object.entries(KPI_SYNONYMS)) {
    if (lower.includes(metric)) {
      out.add(metric);
      continue;
    }
    if (synonyms.some((synonym) => lower.includes(synonym))) {
      out.add(metric);
    }
  }
  return Array.from(out);
}

function inferDocTypeHints(question: string): string[] {
  return DOC_TYPE_HINTS.filter((hint) => hint.pattern.test(question)).map((hint) => hint.docType);
}

function inferTimeframe(question: string): QueryPlan["timeframe"] {
  const years = Array.from(new Set(question.match(/\b20\d{2}\b/g) ?? [])).map((value) => Number(value));
  if (years.length === 0) {
    return { label: "unspecified" };
  }
  const sorted = years.sort((a, b) => a - b);
  const from = Date.parse(`${sorted[0]}-01-01T00:00:00Z`);
  const to = Date.parse(`${sorted[sorted.length - 1]}-12-31T23:59:59Z`);
  return { from, to, label: sorted.join(",") };
}

function extractTicker(question: string): { ticker: string; explicit: boolean } {
  const explicit = question.match(/\$([A-Z]{1,5})\b/);
  if (explicit?.[1]) {
    return { ticker: explicit[1], explicit: true };
  }

  const upperTicker = (question.match(/\b[A-Z]{1,5}\b/g) ?? [])
    .map((value) => value.toUpperCase())
    .filter((value) => !UPPERCASE_NOT_TICKER.has(value));
  if (upperTicker.length > 0) {
    const lower = question.toLowerCase();
    for (const candidate of upperTicker) {
      const companyMatch = Object.entries(COMPANY_TO_TICKER).some(
        ([company, ticker]) => ticker === candidate && lower.includes(company),
      );
      if (companyMatch) {
        return { ticker: candidate, explicit: true };
      }
    }
    return { ticker: upperTicker[0] ?? "", explicit: false };
  }

  const lower = question.toLowerCase();
  for (const [company, ticker] of Object.entries(COMPANY_TO_TICKER)) {
    if (lower.includes(company)) {
      return { ticker, explicit: false };
    }
  }

  return { ticker: "", explicit: false };
}

export function planQuery(question: string, baseFilters: RetrievalFilters = {}): QueryPlan {
  const { ticker, explicit } = extractTicker(question);
  const timeframe = inferTimeframe(question);
  const docTypes = inferDocTypeHints(question);

  const strictFilters: RetrievalFilters = {
    ...baseFilters,
  };
  if (ticker && explicit && !strictFilters.tickers?.length) {
    strictFilters.tickers = [ticker];
  }
  if (docTypes.length > 0 && baseFilters.docTypes == null) {
    strictFilters.docTypes = docTypes;
  }
  if (typeof timeframe.from === "number" && baseFilters.publishedAtMin == null) {
    strictFilters.publishedAtMin = timeframe.from;
  }
  if (typeof timeframe.to === "number" && baseFilters.publishedAtMax == null) {
    strictFilters.publishedAtMax = timeframe.to;
  }

  const looseFilters: RetrievalFilters = {
    tickers: strictFilters.tickers ?? (ticker ? [ticker] : undefined),
    docTypes: strictFilters.docTypes,
    sourceUrls: strictFilters.sourceUrls,
    publishedAtMin: strictFilters.publishedAtMin,
    publishedAtMax: strictFilters.publishedAtMax,
  };

  const rewrites = buildQueryRewrites(question, 5);
  if (ticker && !rewrites.some((variant) => variant.includes(ticker))) {
    rewrites.push(`${ticker} ${question}`.trim());
  }

  return {
    entity: ticker || "",
    ticker,
    metrics: extractMetricHints(question),
    timeframe,
    docTypes,
    rewrites: rewrites.slice(0, 5),
    strictFilters,
    looseFilters,
  };
}
