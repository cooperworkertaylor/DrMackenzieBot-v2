import { Readability } from "@mozilla/readability";
import { parseHTML } from "linkedom";
import { createHash } from "node:crypto";
import { chunkText } from "./chunker.js";
import { openResearchDb } from "./db.js";

export type ExternalResearchSourceType = "email_research" | "newsletter" | "manual";
export type NewsletterProvider = "substack" | "stratechery" | "diff" | "other";

export type IngestExternalResearchParams = {
  sourceType: ExternalResearchSourceType;
  provider?: NewsletterProvider | string;
  externalId?: string;
  sender?: string;
  title: string;
  subject?: string;
  content: string;
  url?: string;
  ticker?: string;
  publishedAt?: string;
  receivedAt?: string;
  tags?: string[];
  metadata?: Record<string, unknown>;
  dbPath?: string;
};

export type IngestExternalResearchResult = {
  id: number;
  chunks: number;
  sourceType: ExternalResearchSourceType;
  provider: string;
  ticker?: string;
  title: string;
};

export type ParsedHookEmail = {
  senderRaw: string;
  senderEmail: string;
  subject: string;
  content: string;
  url?: string;
};

export type ExternalResearchHookCandidate = {
  sourceType: ExternalResearchSourceType;
  provider: NewsletterProvider | string;
  externalId?: string;
  sender: string;
  title: string;
  subject: string;
  content: string;
  url?: string;
  ticker?: string;
  tags: string[];
};

export type ExternalResearchDigestDoc = {
  id: number;
  provider: string;
  sender: string;
  title: string;
  subject: string;
  url: string;
  content: string;
  ticker: string;
  fetchedAt: number;
  score: number;
  readInFull: boolean;
  reasons: string[];
};

export type WeeklyNewsletterDigest = {
  generatedAt: string;
  lookbackDays: number;
  totalDocs: number;
  providers: Array<{ provider: string; count: number }>;
  readInFull: ExternalResearchDigestDoc[];
  quickScan: ExternalResearchDigestDoc[];
};

export type NewsletterSyncSource = {
  provider: NewsletterProvider | string;
  url: string;
  sender?: string;
  ticker?: string;
  tags?: string[];
};

export type NewsletterSyncResultDoc = {
  provider: string;
  sourceUrl: string;
  url: string;
  title: string;
  ingested: boolean;
  reason?: string;
  documentId?: number;
  chunks?: number;
};

export type NewsletterSyncResult = {
  sources: number;
  attempted: number;
  ingested: number;
  skipped: number;
  failures: number;
  docs: NewsletterSyncResultDoc[];
};

const RESEARCH_SUBJECT_DEFAULT = "RESEARCH";
const DIGEST_MAX_CONTENT_CHARS = 12_000;
const NEWSLETTER_SYNC_DEFAULT_UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36";
const NEWSLETTER_SOURCE_DEFAULT_MAX_LINKS = 10;
const NEWSLETTER_SOURCE_DEFAULT_MAX_DOCS = 50;

const sha256 = (value: string): string => createHash("sha256").update(value).digest("hex");

const normalizeEmail = (value?: string): string => {
  if (!value) return "";
  const match = value.match(/<([^>]+)>/);
  const candidate = (match?.[1] ?? value).trim().toLowerCase();
  return candidate;
};

const normalizeTicker = (value?: string): string => {
  if (!value) return "";
  return value.trim().toUpperCase();
};

const normalizeProvider = (value?: string): string => {
  if (!value) return "other";
  const normalized = value.trim().toLowerCase();
  if (normalized === "substack" || normalized === "stratechery" || normalized === "diff") {
    return normalized;
  }
  return normalized || "other";
};

const normalizeDate = (value?: string): string => {
  if (!value) return "";
  const trimmed = value.trim();
  if (!trimmed) return "";
  const ms = Date.parse(trimmed);
  if (!Number.isFinite(ms)) return "";
  return new Date(ms).toISOString();
};

const parseSubjectPrefix = (value?: string): string => {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : RESEARCH_SUBJECT_DEFAULT;
};

export const parseCsvLower = (value?: string): string[] =>
  (value ?? "")
    .split(",")
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);

const parseCsv = (value?: string): string[] =>
  (value ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);

const extractFirstUrl = (value: string): string | undefined => {
  const match = value.match(/https?:\/\/[^\s)\]]+/i);
  return match?.[0];
};

const parseArchiveList = (value?: string): string[] =>
  (value ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);

const parseUrlSafe = (value: string): string | null => {
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return null;
  }
};

const toAbsoluteUrl = (href: string, baseUrl: string): string | null => {
  try {
    const parsed = new URL(href, baseUrl);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return null;
  }
};

const defaultSenderForProvider = (provider: string): string => {
  if (provider === "substack") return "updates@substack.com";
  if (provider === "stratechery") return "updates@stratechery.com";
  if (provider === "diff") return "team@thediff.co";
  return "research-feed@openclaw.local";
};

const providerCookieEnv = (provider: string, env: NodeJS.ProcessEnv): string => {
  if (provider === "substack") return (env.OPENCLAW_RESEARCH_SUBSTACK_COOKIE ?? "").trim();
  if (provider === "stratechery") return (env.OPENCLAW_RESEARCH_STRATECHERY_COOKIE ?? "").trim();
  if (provider === "diff") return (env.OPENCLAW_RESEARCH_DIFF_COOKIE ?? "").trim();
  return "";
};

const parseSourceSpecLine = (line: string): NewsletterSyncSource | null => {
  const trimmed = line.trim();
  if (!trimmed) return null;
  if (trimmed.startsWith("#")) return null;

  const parts = trimmed
    .split("|")
    .map((part) => part.trim())
    .filter(Boolean);
  if (!parts.length) return null;
  if (parts.length === 1) {
    const url = parseUrlSafe(parts[0]);
    if (!url) return null;
    return { provider: "other", url };
  }
  const provider = normalizeProvider(parts[0]);
  const url = parseUrlSafe(parts[1]);
  if (!url) return null;
  const ticker = parts[2] ? normalizeTicker(parts[2]) : "";
  return {
    provider,
    url,
    ticker: ticker || undefined,
  };
};

export const parseNewsletterSourceSpecs = (value: string): NewsletterSyncSource[] =>
  value
    .split(/\r?\n|;/)
    .map((line) => parseSourceSpecLine(line))
    .filter((entry): entry is NewsletterSyncSource => Boolean(entry));

export const resolveNewsletterSourcesFromEnv = (env: NodeJS.ProcessEnv = process.env) => {
  const configured: NewsletterSyncSource[] = [];
  const explicit = parseNewsletterSourceSpecs(env.OPENCLAW_RESEARCH_NEWSLETTER_SOURCES ?? "");
  configured.push(...explicit);
  parseArchiveList(env.OPENCLAW_RESEARCH_SUBSTACK_ARCHIVES).forEach((url) =>
    configured.push({ provider: "substack", url }),
  );
  parseArchiveList(env.OPENCLAW_RESEARCH_STRATECHERY_ARCHIVES).forEach((url) =>
    configured.push({ provider: "stratechery", url }),
  );
  parseArchiveList(env.OPENCLAW_RESEARCH_DIFF_ARCHIVES).forEach((url) =>
    configured.push({ provider: "diff", url }),
  );
  const dedup = new Map<string, NewsletterSyncSource>();
  configured.forEach((source) => {
    const provider = normalizeProvider(source.provider);
    const url = parseUrlSafe(source.url);
    if (!url) return;
    const key = `${provider}|${url}`;
    if (!dedup.has(key)) {
      dedup.set(key, {
        provider,
        url,
        sender: source.sender,
        ticker: normalizeTicker(source.ticker),
        tags: source.tags,
      });
    }
  });
  return Array.from(dedup.values());
};

const extractCandidateLinks = (html: string, sourceUrl: string, maxLinks: number): string[] => {
  const { document } = parseHTML(html);
  const source = new URL(sourceUrl);
  const links = new Set<string>();
  const anchors = Array.from(document.querySelectorAll("a[href]"));
  for (const anchor of anchors) {
    const href = anchor.getAttribute("href");
    if (!href) continue;
    const resolved = toAbsoluteUrl(href, sourceUrl);
    if (!resolved) continue;
    const candidate = new URL(resolved);
    if (candidate.hostname !== source.hostname) {
      continue;
    }
    if (candidate.pathname === "/" || candidate.pathname.length < 2) {
      continue;
    }
    if (candidate.pathname.includes("/archive") || candidate.pathname.includes("/tag/")) {
      continue;
    }
    links.add(candidate.toString());
    if (links.size >= maxLinks) break;
  }
  return Array.from(links.values());
};

const extractPublishedTimeFromHtml = (html: string): string => {
  const { document } = parseHTML(html);
  const selectors = [
    "meta[property='article:published_time']",
    "meta[name='article:published_time']",
    "meta[name='parsely-pub-date']",
    "meta[name='date']",
  ];
  for (const selector of selectors) {
    const value = document.querySelector(selector)?.getAttribute("content") ?? "";
    const normalized = normalizeDate(value);
    if (normalized) return normalized;
  }
  return "";
};

const extractArticleFromHtml = (
  html: string,
): { title: string; content: string; publishedAt?: string } | null => {
  const { document } = parseHTML(html);
  const reader = new Readability(document, { charThreshold: 200 });
  const article = reader.parse();
  const title = (article?.title ?? document.querySelector("title")?.textContent ?? "").trim();
  const content = (article?.textContent ?? "").replace(/\s+/g, " ").trim();
  if (!title || content.length < 350) return null;
  const publishedAt = extractPublishedTimeFromHtml(html);
  return {
    title,
    content,
    publishedAt: publishedAt || undefined,
  };
};

const buildAuthHeaders = (params: {
  provider: string;
  userAgent?: string;
  cookie?: string;
  env?: NodeJS.ProcessEnv;
}): HeadersInit => {
  const env = params.env ?? process.env;
  const provider = normalizeProvider(params.provider);
  const providerCookie = params.cookie?.trim() || providerCookieEnv(provider, env);
  const genericCookie = (env.OPENCLAW_RESEARCH_NEWSLETTER_COOKIE ?? "").trim();
  const cookie = providerCookie || genericCookie;
  const headers: Record<string, string> = {
    "user-agent": (params.userAgent ?? "").trim() || NEWSLETTER_SYNC_DEFAULT_UA,
    accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  };
  if (cookie) headers.cookie = cookie;
  return headers;
};

const fetchHtmlWithAuth = async (params: {
  url: string;
  provider: string;
  userAgent?: string;
  cookie?: string;
  fetchFn?: typeof fetch;
  env?: NodeJS.ProcessEnv;
}): Promise<{ html: string; finalUrl: string }> => {
  const fetchFn = params.fetchFn ?? fetch;
  const response = await fetchFn(params.url, {
    method: "GET",
    redirect: "follow",
    headers: buildAuthHeaders({
      provider: params.provider,
      userAgent: params.userAgent,
      cookie: params.cookie,
      env: params.env,
    }),
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const html = await response.text();
  return {
    html,
    finalUrl: response.url || params.url,
  };
};

export const extractTickerFromSubject = (subject: string, prefix = RESEARCH_SUBJECT_DEFAULT) => {
  const normalizedPrefix = parseSubjectPrefix(prefix).toUpperCase();
  const upper = subject.trim().toUpperCase();
  if (!upper.startsWith(normalizedPrefix)) return "";
  const remainder = subject
    .trim()
    .slice(normalizedPrefix.length)
    .replace(/^[:\-\s]+/, "");
  const token = remainder.split(/\s+/, 1)[0] ?? "";
  if (!/^[A-Z]{1,5}$/.test(token)) return "";
  return token;
};

export const detectNewsletterProvider = (params: {
  sender: string;
  subject?: string;
  content?: string;
  url?: string;
}): NewsletterProvider => {
  const sender = params.sender.toLowerCase();
  const subject = (params.subject ?? "").toLowerCase();
  const content = (params.content ?? "").toLowerCase();
  const url = (params.url ?? "").toLowerCase();
  const haystack = `${sender}\n${subject}\n${content}\n${url}`;

  if (haystack.includes("stratechery")) return "stratechery";
  if (haystack.includes("thediff.co") || haystack.includes(" the diff")) return "diff";
  if (haystack.includes("substack")) return "substack";
  return "other";
};

export const parseGmailHookMessage = (message: string): ParsedHookEmail | null => {
  const lines = message
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line, index, arr) => !(line === "" && arr[index - 1] === ""));
  if (!lines.length) return null;

  const senderLine = lines.find((line) => /^new email from\s+/i.test(line));
  const subjectLine = lines.find((line) => /^subject:\s*/i.test(line));
  if (!senderLine || !subjectLine) return null;

  const senderRaw = senderLine.replace(/^new email from\s+/i, "").trim();
  const senderEmail = normalizeEmail(senderRaw);
  const subject = subjectLine.replace(/^subject:\s*/i, "").trim();
  const subjectIndex = lines.indexOf(subjectLine);
  const content = lines
    .slice(subjectIndex + 1)
    .join("\n")
    .trim();
  const url = extractFirstUrl(content);

  if (!senderEmail || !subject || !content) return null;
  return {
    senderRaw,
    senderEmail,
    subject,
    content,
    url,
  };
};

const buildExternalIdFromSessionKey = (sessionKey: string): string | undefined => {
  if (!sessionKey.startsWith("hook:gmail:")) return undefined;
  const id = sessionKey.slice("hook:gmail:".length).trim();
  return id || undefined;
};

export const buildResearchCandidateFromGmailHook = (params: {
  sessionKey: string;
  message: string;
  subjectPrefix?: string;
  allowedSenders?: string[];
}): ExternalResearchHookCandidate | null => {
  const parsed = parseGmailHookMessage(params.message);
  if (!parsed) return null;

  const normalizedAllowedSenders = (params.allowedSenders ?? [])
    .map((sender) => normalizeEmail(sender))
    .filter(Boolean);
  const senderAllowed =
    normalizedAllowedSenders.length === 0 ||
    normalizedAllowedSenders.includes(parsed.senderEmail.toLowerCase());

  const provider = detectNewsletterProvider({
    sender: parsed.senderEmail,
    subject: parsed.subject,
    content: parsed.content,
    url: parsed.url,
  });

  const prefix = parseSubjectPrefix(params.subjectPrefix);
  const subjectUpper = parsed.subject.toUpperCase();
  const isResearchSubject = subjectUpper.startsWith(prefix.toUpperCase());

  if (!isResearchSubject && provider === "other") {
    return null;
  }
  if (isResearchSubject && !senderAllowed) {
    return null;
  }

  const sourceType: ExternalResearchSourceType = isResearchSubject
    ? "email_research"
    : "newsletter";
  const ticker = isResearchSubject ? extractTickerFromSubject(parsed.subject, prefix) : "";
  const title = isResearchSubject
    ? parsed.subject
        .replace(new RegExp(`^${prefix}`, "i"), "")
        .replace(/^[:\-\s]+/, "")
        .trim() || parsed.subject
    : parsed.subject;

  const tags = new Set<string>(["email", sourceType]);
  if (ticker) tags.add(`ticker:${ticker}`);
  if (provider !== "other") tags.add(`provider:${provider}`);

  return {
    sourceType,
    provider,
    externalId: buildExternalIdFromSessionKey(params.sessionKey),
    sender: parsed.senderRaw || parsed.senderEmail,
    title,
    subject: parsed.subject,
    content: parsed.content,
    url: parsed.url,
    ticker: ticker || undefined,
    tags: Array.from(tags),
  };
};

export const ingestExternalResearchDocument = (
  params: IngestExternalResearchParams,
): IngestExternalResearchResult => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const sender = normalizeEmail(params.sender);
  const title = params.title.trim();
  const content = params.content.trim();
  const sourceType = params.sourceType;
  if (!title) throw new Error("title is required");
  if (!content) throw new Error("content is required");

  const providerRaw = (params.provider ?? "other").toString().trim().toLowerCase();
  const provider = providerRaw || "other";
  const subject = (params.subject ?? title).trim();
  const url = (params.url ?? "").trim();
  const ticker = normalizeTicker(params.ticker);
  const publishedAt = normalizeDate(params.publishedAt);
  const receivedAt = normalizeDate(params.receivedAt) || new Date(now).toISOString();
  const tags = parseCsv((params.tags ?? []).join(","));
  const metadata = {
    ...(params.metadata ?? {}),
    tags,
  };

  const contentHash = sha256(
    JSON.stringify({
      sourceType,
      provider,
      sender,
      title,
      subject,
      url,
      content,
      ticker,
    }),
  );

  const externalId = (params.externalId ?? "").trim();

  const insertDocument = db.prepare(`
    INSERT INTO external_documents (
      source_type,
      provider,
      external_id,
      sender,
      title,
      subject,
      url,
      ticker,
      published_at,
      received_at,
      content,
      content_hash,
      metadata,
      fetched_at
    )
    VALUES (
      @source_type,
      @provider,
      @external_id,
      @sender,
      @title,
      @subject,
      @url,
      @ticker,
      @published_at,
      @received_at,
      @content,
      @content_hash,
      @metadata,
      @fetched_at
    )
    ON CONFLICT(content_hash) DO UPDATE SET
      provider=excluded.provider,
      external_id=CASE WHEN excluded.external_id<>'' THEN excluded.external_id ELSE external_documents.external_id END,
      sender=excluded.sender,
      title=excluded.title,
      subject=excluded.subject,
      url=excluded.url,
      ticker=excluded.ticker,
      published_at=excluded.published_at,
      received_at=excluded.received_at,
      content=excluded.content,
      metadata=excluded.metadata,
      fetched_at=excluded.fetched_at
    RETURNING id
  `);

  const insertChunk = db.prepare(`
    INSERT INTO chunks (source_table, ref_id, seq, text, metadata)
    VALUES ('external_documents', @ref_id, @seq, @text, @metadata)
    ON CONFLICT(source_table, ref_id, seq) DO UPDATE SET
      text=excluded.text,
      metadata=excluded.metadata,
      pending_embedding=1
  `);
  const pruneChunks = db.prepare(
    `DELETE FROM chunks WHERE source_table='external_documents' AND ref_id=? AND seq>=?`,
  );

  const row = insertDocument.get({
    source_type: sourceType,
    provider,
    external_id: externalId,
    sender,
    title,
    subject,
    url,
    ticker,
    published_at: publishedAt,
    received_at: receivedAt,
    content,
    content_hash: contentHash,
    metadata: JSON.stringify(metadata),
    fetched_at: now,
  }) as { id: number };

  const chunkMeta = JSON.stringify({
    sourceType,
    provider,
    sender,
    title,
    subject,
    url,
    ticker,
    publishedAt: publishedAt || undefined,
    receivedAt,
    tags,
  });
  const chunks = chunkText(content, 220);
  db.exec("BEGIN");
  try {
    for (const chunk of chunks) {
      insertChunk.run({
        ref_id: row.id,
        seq: chunk.seq,
        text: chunk.text,
        metadata: chunkMeta,
      });
    }
    pruneChunks.run(row.id, chunks.length);
    db.exec("COMMIT");
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }

  return {
    id: row.id,
    chunks: chunks.length,
    sourceType,
    provider,
    ticker: ticker || undefined,
    title,
  };
};

const providerPriorityScore = (provider: string): number => {
  const normalized = provider.toLowerCase();
  if (normalized === "stratechery") return 1;
  if (normalized === "diff") return 0.92;
  if (normalized === "substack") return 0.82;
  return 0.65;
};

const keywordWeights: Array<{ key: string; weight: number; reason: string }> = [
  { key: "valuation", weight: 0.16, reason: "valuation setup" },
  { key: "multiple", weight: 0.1, reason: "multiple framework" },
  { key: "margin", weight: 0.14, reason: "margin structure" },
  { key: "capex", weight: 0.12, reason: "capital cycle" },
  { key: "market share", weight: 0.12, reason: "competitive position" },
  { key: "pricing", weight: 0.1, reason: "pricing power" },
  { key: "regulation", weight: 0.1, reason: "policy risk" },
  { key: "cash flow", weight: 0.1, reason: "cash conversion" },
  { key: "guidance", weight: 0.1, reason: "near-term catalyst" },
  { key: "consensus", weight: 0.1, reason: "market belief gap" },
];

const scoreDigestDoc = (params: {
  provider: string;
  title: string;
  subject: string;
  content: string;
  fetchedAt: number;
  lookbackDays: number;
}) => {
  const combined = `${params.title}\n${params.subject}\n${params.content.slice(0, DIGEST_MAX_CONTENT_CHARS)}`;
  const lower = combined.toLowerCase();
  const matched = keywordWeights.filter((keyword) => lower.includes(keyword.key));
  const signalScore = Math.min(
    1,
    matched.reduce((sum, keyword) => sum + keyword.weight, 0),
  );
  const lengthScore = Math.min(1, params.content.length / 5000);
  const ageDays = Math.max(0, (Date.now() - params.fetchedAt) / 86_400_000);
  const freshnessScore = Math.max(0, Math.min(1, 1 - ageDays / Math.max(1, params.lookbackDays)));
  const providerScore = providerPriorityScore(params.provider);
  const score =
    0.34 * signalScore + 0.24 * freshnessScore + 0.22 * lengthScore + 0.2 * providerScore;
  return {
    score: Math.max(0, Math.min(1, score)),
    reasons: matched.slice(0, 3).map((item) => item.reason),
  };
};

export const computeWeeklyNewsletterDigest = (params: {
  dbPath?: string;
  lookbackDays?: number;
  limit?: number;
  providers?: string[];
}): WeeklyNewsletterDigest => {
  const db = openResearchDb(params.dbPath);
  const lookbackDays = Math.max(1, Math.round(params.lookbackDays ?? 7));
  const limit = Math.max(5, Math.round(params.limit ?? 80));
  const providersFilter = (params.providers ?? [])
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);
  const cutoff = Date.now() - lookbackDays * 86_400_000;

  const sql = `
    SELECT id, provider, sender, title, subject, url, ticker, content, fetched_at
    FROM external_documents
    WHERE source_type='newsletter'
      AND fetched_at >= @cutoff
      ${providersFilter.length ? `AND lower(provider) IN (${providersFilter.map((_, index) => `@provider_${index}`).join(", ")})` : ""}
    ORDER BY fetched_at DESC
    LIMIT @limit
  `;
  const stmt = db.prepare(sql);
  const bind: Record<string, string | number | null> = { cutoff, limit };
  providersFilter.forEach((provider, index) => {
    bind[`provider_${index}`] = provider;
  });

  const rows = stmt.all(bind) as Array<{
    id: number;
    provider: string;
    sender: string;
    title: string;
    subject: string;
    url: string;
    ticker: string;
    content: string;
    fetched_at: number;
  }>;

  const docs: ExternalResearchDigestDoc[] = rows.map((row) => {
    const scored = scoreDigestDoc({
      provider: row.provider,
      title: row.title,
      subject: row.subject,
      content: row.content,
      fetchedAt: row.fetched_at,
      lookbackDays,
    });
    return {
      id: row.id,
      provider: row.provider || "other",
      sender: row.sender,
      title: row.title,
      subject: row.subject,
      url: row.url,
      content: row.content,
      ticker: row.ticker,
      fetchedAt: row.fetched_at,
      score: scored.score,
      readInFull: scored.score >= 0.62,
      reasons: scored.reasons,
    };
  });

  const readInFull = docs
    .filter((doc) => doc.readInFull)
    .sort((left, right) => right.score - left.score)
    .slice(0, 20);
  const quickScan = docs
    .filter((doc) => !doc.readInFull)
    .sort((left, right) => right.score - left.score)
    .slice(0, 30);

  const counts = new Map<string, number>();
  docs.forEach((doc) => {
    const key = doc.provider || "other";
    counts.set(key, (counts.get(key) ?? 0) + 1);
  });
  const providers = Array.from(counts.entries())
    .map(([provider, count]) => ({ provider, count }))
    .sort((left, right) => right.count - left.count);

  return {
    generatedAt: new Date().toISOString(),
    lookbackDays,
    totalDocs: docs.length,
    providers,
    readInFull,
    quickScan,
  };
};

const formatDocLine = (doc: ExternalResearchDigestDoc): string => {
  const receivedAt = new Date(doc.fetchedAt).toISOString().slice(0, 10);
  const headline = doc.url ? `[${doc.title}](${doc.url})` : doc.title;
  const reasons = doc.reasons.length ? doc.reasons.join(", ") : "high-signal framing";
  const ticker = doc.ticker ? ` | ticker=${doc.ticker}` : "";
  return `- ${headline} | provider=${doc.provider} | sender=${doc.sender} | date=${receivedAt}${ticker} | score=${doc.score.toFixed(2)} | why=${reasons}`;
};

export const renderWeeklyNewsletterDigestMarkdown = (digest: WeeklyNewsletterDigest): string => {
  const lines: string[] = [];
  lines.push(`# Weekly Research Newsletter Digest`);
  lines.push(``);
  lines.push(`Generated: ${digest.generatedAt}`);
  lines.push(`Window: last ${digest.lookbackDays} days`);
  lines.push(`Documents reviewed: ${digest.totalDocs}`);
  lines.push(``);
  lines.push(`## Coverage`);
  if (!digest.providers.length) {
    lines.push(`- No newsletter documents ingested in this window.`);
  } else {
    digest.providers.forEach((row) => {
      lines.push(`- ${row.provider}: ${row.count}`);
    });
  }
  lines.push(``);
  lines.push(`## Read In Full`);
  if (!digest.readInFull.length) {
    lines.push(`- No items crossed the full-read threshold this week.`);
  } else {
    digest.readInFull.forEach((doc) => lines.push(formatDocLine(doc)));
  }
  lines.push(``);
  lines.push(`## Quick Scan`);
  if (!digest.quickScan.length) {
    lines.push(`- No additional quick-scan items this week.`);
  } else {
    digest.quickScan.slice(0, 15).forEach((doc) => lines.push(formatDocLine(doc)));
  }
  lines.push(``);
  lines.push(`## Notes`);
  lines.push(
    `- This digest uses ingestion metadata + deterministic scoring; use full reads for final investment judgement.`,
  );
  lines.push(`- To improve ranking quality, ingest rich body content (not snippet-only emails).`);
  return lines.join("\n");
};

export const syncNewsletterSources = async (params: {
  dbPath?: string;
  sources?: NewsletterSyncSource[];
  providers?: string[];
  maxLinksPerSource?: number;
  maxDocs?: number;
  userAgent?: string;
  cookies?: Record<string, string>;
  fetchFn?: typeof fetch;
  env?: NodeJS.ProcessEnv;
}): Promise<NewsletterSyncResult> => {
  const env = params.env ?? process.env;
  const configuredSources =
    params.sources && params.sources.length ? params.sources : resolveNewsletterSourcesFromEnv(env);
  const providerFilter = new Set(
    (params.providers ?? []).map((provider) => normalizeProvider(provider)).filter(Boolean),
  );
  const sources = configuredSources.filter((source) => {
    if (!providerFilter.size) return true;
    return providerFilter.has(normalizeProvider(source.provider));
  });
  if (!sources.length) {
    throw new Error(
      "No newsletter sources configured. Set OPENCLAW_RESEARCH_NEWSLETTER_SOURCES or pass --source.",
    );
  }

  const maxLinksPerSource = Math.max(
    1,
    Math.round(params.maxLinksPerSource ?? NEWSLETTER_SOURCE_DEFAULT_MAX_LINKS),
  );
  const maxDocs = Math.max(1, Math.round(params.maxDocs ?? NEWSLETTER_SOURCE_DEFAULT_MAX_DOCS));
  const docs: NewsletterSyncResultDoc[] = [];
  let attempted = 0;
  let ingested = 0;
  let skipped = 0;
  let failures = 0;

  for (const source of sources) {
    if (attempted >= maxDocs) break;
    const provider = normalizeProvider(source.provider);
    const sourceUrl = parseUrlSafe(source.url);
    if (!sourceUrl) continue;
    const cookie = params.cookies?.[provider] ?? "";
    let archiveHtml = "";
    let archiveUrl = sourceUrl;
    try {
      const archive = await fetchHtmlWithAuth({
        url: sourceUrl,
        provider,
        userAgent: params.userAgent,
        cookie,
        fetchFn: params.fetchFn,
        env,
      });
      archiveHtml = archive.html;
      archiveUrl = archive.finalUrl;
    } catch (error) {
      docs.push({
        provider,
        sourceUrl,
        url: sourceUrl,
        title: sourceUrl,
        ingested: false,
        reason: `source fetch failed: ${error instanceof Error ? error.message : String(error)}`,
      });
      failures += 1;
      continue;
    }

    const candidates = extractCandidateLinks(archiveHtml, archiveUrl, maxLinksPerSource);
    for (const candidateUrl of candidates) {
      if (attempted >= maxDocs) break;
      attempted += 1;
      try {
        const page = await fetchHtmlWithAuth({
          url: candidateUrl,
          provider,
          userAgent: params.userAgent,
          cookie,
          fetchFn: params.fetchFn,
          env,
        });
        const extracted = extractArticleFromHtml(page.html);
        if (!extracted) {
          docs.push({
            provider,
            sourceUrl,
            url: page.finalUrl,
            title: candidateUrl,
            ingested: false,
            reason: "article extraction returned insufficient content",
          });
          skipped += 1;
          continue;
        }
        const ingestion = ingestExternalResearchDocument({
          dbPath: params.dbPath,
          sourceType: "newsletter",
          provider,
          sender: source.sender || defaultSenderForProvider(provider),
          externalId: sha256(`${provider}|${page.finalUrl}`),
          title: extracted.title,
          subject: extracted.title,
          content: extracted.content,
          url: page.finalUrl,
          ticker: source.ticker,
          publishedAt: extracted.publishedAt,
          receivedAt: new Date().toISOString(),
          tags: ["newsletter", "newsletter-sync", `provider:${provider}`, ...(source.tags ?? [])],
        });
        docs.push({
          provider,
          sourceUrl,
          url: page.finalUrl,
          title: extracted.title,
          ingested: true,
          documentId: ingestion.id,
          chunks: ingestion.chunks,
        });
        ingested += 1;
      } catch (error) {
        docs.push({
          provider,
          sourceUrl,
          url: candidateUrl,
          title: candidateUrl,
          ingested: false,
          reason: error instanceof Error ? error.message : String(error),
        });
        failures += 1;
      }
    }
  }

  return {
    sources: sources.length,
    attempted,
    ingested,
    skipped,
    failures,
    docs,
  };
};
