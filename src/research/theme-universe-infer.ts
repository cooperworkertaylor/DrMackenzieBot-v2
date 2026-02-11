import { openResearchDb } from "./db.js";

const normalizeSpaces = (value: string): string => value.replaceAll(/\s+/g, " ").trim();

const normalizeTicker = (value: string): string =>
  value
    .trim()
    .toUpperCase()
    .replaceAll(/[^A-Z0-9.]/g, "")
    .slice(0, 10);

const TICKER_ALIAS_MAP: Record<string, string> = {
  TSMC: "TSM",
  TAIWANSEMICONDUCTOR: "TSM",
  FACEBOOK: "META",
};

const NON_TICKER_NOISE = new Set(
  ["CMOS", "SILICONPHOTONICS", "FOUNDRY", "OPTICALNETWORKING", "PHOTONICS", "DATACENTER"].map((t) =>
    t.toUpperCase(),
  ),
);

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

const canonicalizeTickerCandidate = (value: string): string => {
  const normalized = normalizeTicker(value);
  if (!normalized) return "";
  const mapped = TICKER_ALIAS_MAP[normalized] ?? normalized;
  if (!mapped) return "";
  if (STOP_TOKENS.has(mapped)) return "";
  if (NON_TICKER_NOISE.has(mapped)) return "";
  if (/^\d+$/.test(mapped)) return "";
  return mapped;
};

const listKnownInstrumentTickers = (
  db: ReturnType<typeof openResearchDb>,
  tickers: string[],
): Set<string> => {
  if (!tickers.length) return new Set();
  const placeholders = tickers.map(() => "?").join(",");
  const rows = db
    .prepare(
      `SELECT UPPER(TRIM(ticker)) AS ticker
       FROM instruments
       WHERE UPPER(TRIM(ticker)) IN (${placeholders})`,
    )
    .all(...tickers) as Array<{ ticker?: string }>;
  return new Set(rows.map((row) => normalizeTicker(String(row.ticker ?? ""))).filter(Boolean));
};

const normalizeThemeTickerUniverseWithDb = (params: {
  db: ReturnType<typeof openResearchDb>;
  tickers: string[];
  maxTickers?: number;
}): { tickers: string[]; dropped: string[] } => {
  const maxTickers = Math.max(1, Math.min(120, Math.round(params.maxTickers ?? 60)));
  const normalizedCandidates: string[] = [];
  const seen = new Set<string>();
  for (const raw of params.tickers ?? []) {
    const canonical = canonicalizeTickerCandidate(raw);
    if (!canonical || seen.has(canonical)) continue;
    seen.add(canonical);
    normalizedCandidates.push(canonical);
  }
  if (!normalizedCandidates.length) {
    return { tickers: [], dropped: [] };
  }

  const known = listKnownInstrumentTickers(params.db, normalizedCandidates);
  const tickers = normalizedCandidates.filter((ticker) => known.has(ticker)).slice(0, maxTickers);
  const dropped = normalizedCandidates.filter((ticker) => !known.has(ticker));
  return { tickers, dropped };
};

export function normalizeThemeTickerUniverse(params: {
  tickers: string[];
  dbPath?: string;
  maxTickers?: number;
}): { tickers: string[]; dropped: string[] } {
  const db = openResearchDb(params.dbPath);
  return normalizeThemeTickerUniverseWithDb({
    db,
    tickers: params.tickers,
    maxTickers: params.maxTickers,
  });
}

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
  inferred_entities: Array<{
    id: string;
    type: "equity" | "crypto_asset" | "protocol" | "private_company" | "index" | "other";
    label: string;
    symbol?: string;
    urls?: string[];
    notes?: string[];
  }>;
  note: string;
};

const slugify = (value: string): string =>
  value
    .trim()
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, "-")
    .replaceAll(/(^-+|-+$)/g, "")
    .slice(0, 64) || "entity";

const CRYPTO_ASSET_ALIASES: Array<{ symbol: string; label: string; aliases: string[] }> = [
  { symbol: "BTC", label: "Bitcoin", aliases: ["BTC", "Bitcoin"] },
  { symbol: "ETH", label: "Ethereum", aliases: ["ETH", "Ethereum"] },
  { symbol: "SOL", label: "Solana", aliases: ["SOL", "Solana"] },
  { symbol: "USDC", label: "USD Coin", aliases: ["USDC", "USD Coin"] },
  { symbol: "USDT", label: "Tether", aliases: ["USDT", "Tether"] },
  { symbol: "DAI", label: "DAI", aliases: ["DAI"] },
];

const PROTOCOL_DOMAIN_HINTS: Array<{ re: RegExp; label: string; urls?: string[] }> = [
  { re: /\buniswap\b/i, label: "Uniswap", urls: ["https://uniswap.org"] },
  { re: /\baave\b/i, label: "Aave", urls: ["https://aave.com"] },
  { re: /\bjup\.ag\b/i, label: "Jupiter", urls: ["https://jup.ag"] },
  { re: /\blido\b/i, label: "Lido", urls: ["https://lido.fi"] },
  { re: /\bcoinbase\b/i, label: "Coinbase", urls: ["https://www.coinbase.com"] },
  { re: /\bstripe\b/i, label: "Stripe", urls: ["https://stripe.com"] },
];

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
  const cryptoAssetCounts = new Map<string, number>();
  const protocolCounts = new Map<string, number>();

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
    const lowered = combined.toLowerCase();
    for (const asset of CRYPTO_ASSET_ALIASES) {
      if (asset.aliases.some((a) => lowered.includes(a.toLowerCase()))) {
        cryptoAssetCounts.set(asset.symbol, (cryptoAssetCounts.get(asset.symbol) ?? 0) + 1);
      }
    }
    for (const hint of PROTOCOL_DOMAIN_HINTS) {
      if (hint.re.test(combined)) {
        protocolCounts.set(hint.label, (protocolCounts.get(hint.label) ?? 0) + 1);
      }
    }
  }

  const rawInferredTickers = Array.from(tickerCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([t]) => t)
    .slice(0, maxTickers);
  const { tickers: inferred_tickers } = normalizeThemeTickerUniverseWithDb({
    db,
    tickers: rawInferredTickers,
    maxTickers,
  });
  const inferred_domains = Array.from(domainCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([d]) => d)
    .slice(0, maxDomains);

  const inferred_entities: InferredThemeUniverse["inferred_entities"] = [];
  for (const t of inferred_tickers) {
    inferred_entities.push({
      id: `equity:${t}`,
      type: "equity",
      label: t,
      symbol: t,
      notes: ["inferred from evidence DB keyword match"],
    });
  }
  for (const symbol of Array.from(cryptoAssetCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([s]) => s)
    .slice(0, 10)) {
    const meta = CRYPTO_ASSET_ALIASES.find((a) => a.symbol === symbol);
    inferred_entities.push({
      id: `crypto_asset:${symbol}`,
      type: "crypto_asset",
      label: meta?.label ?? symbol,
      symbol,
      urls:
        symbol === "BTC"
          ? ["https://bitcoin.org"]
          : symbol === "ETH"
            ? ["https://ethereum.org"]
            : undefined,
      notes: ["inferred from evidence DB keyword match"],
    });
  }
  for (const label of Array.from(protocolCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([l]) => l)
    .slice(0, 15)) {
    const hint = PROTOCOL_DOMAIN_HINTS.find((h) => h.label === label);
    inferred_entities.push({
      id: `protocol:${slugify(label)}`,
      type: "protocol",
      label,
      urls: hint?.urls,
      notes: ["inferred from evidence DB keyword/domain match"],
    });
  }

  return {
    theme,
    scanned_docs: rows.length,
    inferred_tickers,
    inferred_domains,
    inferred_entities,
    note: "Universe inferred from external_documents by keyword match; confirm/override with 'tickers: ...' for better coverage.",
  };
}

export function inferThemeUniverseFromInstruments(params: {
  theme: string;
  dbPath?: string;
  maxTickers?: number;
}): { theme: string; inferred_tickers: string[] } {
  const theme = normalizeSpaces(params.theme);
  const maxTickers = Math.max(5, Math.min(80, Math.round(params.maxTickers ?? 25)));
  const db = openResearchDb(params.dbPath);

  // Tokenize the theme and use as keyword probes over the instruments table.
  const tokens = Array.from(
    new Set(
      theme
        .toLowerCase()
        .split(/[^a-z0-9]+/g)
        .map((t) => t.trim())
        .filter((t) => t.length >= 3)
        .filter((t) => !STOP_TOKENS.has(t.toUpperCase())),
    ),
  ).slice(0, 8);

  if (!tokens.length) {
    return { theme, inferred_tickers: [] };
  }

  // Pull a small candidate set then score in JS (SQLite FTS isn't guaranteed here).
  const ors = tokens.map(() => "lower(coalesce(name,'')) LIKE '%' || ? || '%'").join(" OR ");
  const rows = db
    .prepare(
      `SELECT ticker, name, sector, industry
       FROM instruments
       WHERE trim(coalesce(ticker,'')) <> ''
         AND (${ors}
              OR ${tokens.map(() => "lower(coalesce(sector,'')) LIKE '%' || ? || '%'").join(" OR ")}
              OR ${tokens
                .map(() => "lower(coalesce(industry,'')) LIKE '%' || ? || '%'")
                .join(" OR ")})
       LIMIT 600`,
    )
    .all(...tokens, ...tokens, ...tokens) as Array<{
    ticker: string;
    name?: string;
    sector?: string;
    industry?: string;
  }>;

  const scored = rows
    .map((row) => {
      const blob = normalizeSpaces(
        [row.ticker ?? "", row.name ?? "", row.sector ?? "", row.industry ?? ""].join(" "),
      ).toLowerCase();
      let hits = 0;
      for (const t of tokens) {
        if (blob.includes(t)) hits += 1;
      }
      // Prefer more specific matches (more hits) and shorter tickers (avoid weird tokens).
      const ticker = normalizeTicker(row.ticker ?? "");
      const score = hits + (ticker.length <= 5 ? 0.2 : 0);
      return { ticker, score };
    })
    .filter((r) => r.ticker && !STOP_TOKENS.has(r.ticker))
    .sort((a, b) => b.score - a.score)
    .map((r) => r.ticker);

  const { tickers: inferred_tickers } = normalizeThemeTickerUniverseWithDb({
    db,
    tickers: scored,
    maxTickers,
  });
  return { theme, inferred_tickers };
}
