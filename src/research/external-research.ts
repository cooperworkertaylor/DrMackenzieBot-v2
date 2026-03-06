import { Readability } from "@mozilla/readability";
import { parseHTML } from "linkedom";
import { createHash } from "node:crypto";
import { chunkText } from "./chunker.js";
import { openResearchDb } from "./db.js";
import {
  buildSourceKey,
  fingerprintExternalDocument,
  inferDocumentTrustTier,
  normalizeDocumentText,
  safeCanonicalizeUrl,
  scoreExternalDocumentMateriality,
  writeRawExternalDocumentArtifactSync,
} from "./ingestion-utils.js";
import { upsertResearchSource } from "./source-registry.js";
import { extractStructuredResearchFromExternalDocument } from "./structured-extraction.js";
import {
  buildExternalResearchStructuredReport,
  storeExternalResearchStructuredReport,
} from "./external-research-report.js";
import {
  buildExternalResearchThesisFromReport,
  getLatestExternalResearchThesis,
  persistExternalResearchThesisBreakAlert,
  storeExternalResearchThesis,
  storeExternalResearchThesisDiff,
} from "./external-research-thesis.js";
import { enqueueWatchlistRefresh } from "./external-research-watchlists.js";

const sha256 = (value: string): string => createHash("sha256").update(value).digest("hex");

export type ExternalResearchSourceType = "email_research" | "newsletter" | "manual";
export type NewsletterProvider = "substack" | "stratechery" | "diff" | "semianalysis" | "other";

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
  reportId?: number;
  thesisId?: number;
  thesisDiffId?: number;
  thesisAlertId?: number;
  watchlistRefreshId?: number;
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
const NEWSLETTER_SITEMAP_MAX_URLS = 5000;
const NEWSLETTER_SITEMAP_MAX_FILES = 64;

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
  if (
    normalized === "substack" ||
    normalized === "stratechery" ||
    normalized === "diff" ||
    normalized === "semianalysis"
  ) {
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

const normalizeLowerBoundDate = (value?: string): string => {
  if (!value) return "";
  const trimmed = value.trim();
  if (!trimmed) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return `${trimmed}T00:00:00.000Z`;
  }
  return normalizeDate(trimmed);
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

type SitemapUrlEntry = {
  url: string;
  lastmod?: string;
};

const extractXmlTagValue = (block: string, tag: string): string => {
  const pattern = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const match = block.match(pattern);
  return (match?.[1] ?? "").trim();
};

const parseSitemapUrlEntries = (xml: string): SitemapUrlEntry[] => {
  const entries: SitemapUrlEntry[] = [];
  for (const match of xml.matchAll(/<url\b[^>]*>([\s\S]*?)<\/url>/gi)) {
    const block = match[1] ?? "";
    const loc = parseUrlSafe(extractXmlTagValue(block, "loc"));
    if (!loc) continue;
    const lastmod = normalizeDate(extractXmlTagValue(block, "lastmod"));
    entries.push({ url: loc, lastmod: lastmod || undefined });
  }
  return entries;
};

const parseSitemapChildUrls = (xml: string): string[] => {
  const urls: string[] = [];
  for (const match of xml.matchAll(/<sitemap\b[^>]*>([\s\S]*?)<\/sitemap>/gi)) {
    const block = match[1] ?? "";
    const loc = parseUrlSafe(extractXmlTagValue(block, "loc"));
    if (loc) urls.push(loc);
  }
  return urls;
};

const parseRobotsSitemapUrls = (robotsTxt: string, baseUrl: string): string[] => {
  const urls: string[] = [];
  const lines = robotsTxt.split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const match = trimmed.match(/^sitemap:\s*(.+)$/i);
    if (!match) continue;
    const absolute = (() => {
      try {
        const parsed = new URL(match[1] ?? "", baseUrl);
        if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
        parsed.hash = "";
        return parsed.toString();
      } catch {
        return null;
      }
    })();
    if (absolute) urls.push(absolute);
  }
  return urls;
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

const isLikelyLoginUrl = (url: string): boolean => {
  const lower = url.toLowerCase();
  return (
    lower.includes("passport.online/member/login") ||
    lower.includes("/member/login") ||
    lower.includes("/login") ||
    lower.includes("request_token=") ||
    lower.includes("mode=password")
  );
};

const isStaticPath = (pathname: string): boolean => {
  const lower = pathname.toLowerCase();
  const blockedPrefixes = [
    "/about",
    "/privacy",
    "/jobs",
    "/people",
    "/manifesto",
    "/glossary",
    "/topics",
    "/topic",
    "/companies",
    "/concepts",
    "/tags",
    "/tag",
    "/category",
    "/categories",
    "/archive",
    "/account",
    "/login",
    "/subscribe",
    "/verify",
    "/member",
    "/wp-json",
  ];
  return blockedPrefixes.some((prefix) => lower === prefix || lower.startsWith(`${prefix}/`));
};

const isAllowedCandidateHost = (provider: string, sourceHost: string, candidateHost: string) => {
  const normalizedProvider = normalizeProvider(provider);
  const source = sourceHost.toLowerCase();
  const candidate = candidateHost.toLowerCase();
  if (candidate === source) return true;
  if (normalizedProvider === "substack") {
    if (candidate === "substack.com" || candidate.endsWith(".substack.com")) return true;
    if (source === "substack.com" || source.endsWith(".substack.com")) return true;
  }
  if (normalizedProvider === "semianalysis") {
    if (candidate === "semianalysis.com" || candidate.endsWith(".semianalysis.com")) return true;
    if (source === "semianalysis.com" || source.endsWith(".semianalysis.com")) return true;
  }
  return false;
};

const scoreArticlePath = (provider: string, pathname: string): number => {
  const normalizedProvider = normalizeProvider(provider);
  const lower = pathname.toLowerCase();
  if (!lower || lower === "/" || lower.length < 2) return -10;
  if (isStaticPath(lower)) return -10;
  let score = 0;
  if (/\/20\d{2}\//.test(lower)) score += 4;
  if (/\/20\d{2}\/\d{2}\//.test(lower)) score += 4;
  if (/\/p\/[a-z0-9-]+/.test(lower)) score += 5;
  if (lower.split("/").filter(Boolean).length >= 2) score += 1;
  if (normalizedProvider === "substack") {
    if (/\/p\/[a-z0-9-]+/.test(lower)) score += 5;
    if (lower.startsWith("/archive") || lower.startsWith("/publish")) score -= 4;
  }
  if (normalizedProvider === "stratechery" && lower.startsWith("/category/")) score -= 4;
  return score;
};

const defaultSenderForProvider = (provider: string): string => {
  if (provider === "substack") return "updates@substack.com";
  if (provider === "stratechery") return "updates@stratechery.com";
  if (provider === "diff") return "team@thediff.co";
  if (provider === "semianalysis") return "updates@semianalysis.com";
  return "research-feed@openclaw.local";
};

const providerCookieEnv = (provider: string, env: NodeJS.ProcessEnv): string => {
  if (provider === "substack") return (env.OPENCLAW_RESEARCH_SUBSTACK_COOKIE ?? "").trim();
  if (provider === "stratechery") return (env.OPENCLAW_RESEARCH_STRATECHERY_COOKIE ?? "").trim();
  if (provider === "diff") return (env.OPENCLAW_RESEARCH_DIFF_COOKIE ?? "").trim();
  if (provider === "semianalysis") return (env.OPENCLAW_RESEARCH_SEMIANALYSIS_COOKIE ?? "").trim();
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
  parseArchiveList(env.OPENCLAW_RESEARCH_SEMIANALYSIS_ARCHIVES).forEach((url) =>
    configured.push({ provider: "semianalysis", url }),
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

const extractCandidateLinks = (
  provider: string,
  html: string,
  sourceUrl: string,
  maxLinks: number,
): string[] => {
  const { document } = parseHTML(html);
  const source = new URL(sourceUrl);
  const links = new Map<string, number>();
  const anchors = Array.from(document.querySelectorAll("a[href]"));
  for (const anchor of anchors) {
    const href = anchor.getAttribute("href");
    if (!href) continue;
    const resolved = toAbsoluteUrl(href, sourceUrl);
    if (!resolved) continue;
    if (isLikelyLoginUrl(resolved)) continue;
    const candidate = new URL(resolved);
    if (!isAllowedCandidateHost(provider, source.hostname, candidate.hostname)) {
      continue;
    }
    const pathScore = scoreArticlePath(provider, candidate.pathname);
    if (pathScore <= 0) {
      continue;
    }
    const anchorText = (anchor.textContent ?? "").trim();
    const textScore = Math.min(3, Math.floor(anchorText.length / 24));
    const finalScore = pathScore + textScore;
    const key = candidate.toString();
    const existing = links.get(key);
    if (existing === undefined || finalScore > existing) {
      links.set(key, finalScore);
    }
  }
  return Array.from(links.entries())
    .sort((left, right) => right[1] - left[1])
    .slice(0, maxLinks)
    .map(([url]) => url);
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

const collectSitemapCandidates = async (params: {
  provider: string;
  sourceUrl: string;
  userAgent?: string;
  cookie?: string;
  fetchFn?: typeof fetch;
  env?: NodeJS.ProcessEnv;
  maxUrls: number;
  sinceEpochMs?: number;
}): Promise<Map<string, number>> => {
  const source = new URL(params.sourceUrl);
  const root = `${source.protocol}//${source.host}/`;
  const toVisit: string[] = [];
  const visited = new Set<string>();
  const discovered = new Map<string, number>();

  try {
    const robots = await fetchHtmlWithAuth({
      url: new URL("/robots.txt", root).toString(),
      provider: params.provider,
      userAgent: params.userAgent,
      cookie: params.cookie,
      fetchFn: params.fetchFn,
      env: params.env,
    });
    parseRobotsSitemapUrls(robots.html, root).forEach((url) => toVisit.push(url));
  } catch {
    // Robots can fail on some hosts; fallback sitemap below still works.
  }

  const fallbackSitemap = new URL("/sitemap.xml", root).toString();
  toVisit.push(fallbackSitemap);

  while (toVisit.length > 0 && visited.size < NEWSLETTER_SITEMAP_MAX_FILES) {
    if (discovered.size >= params.maxUrls) break;
    const sitemapUrl = toVisit.shift() ?? "";
    if (!sitemapUrl) continue;
    if (visited.has(sitemapUrl)) continue;
    visited.add(sitemapUrl);

    let xml = "";
    let finalUrl = sitemapUrl;
    try {
      const response = await fetchHtmlWithAuth({
        url: sitemapUrl,
        provider: params.provider,
        userAgent: params.userAgent,
        cookie: params.cookie,
        fetchFn: params.fetchFn,
        env: params.env,
      });
      xml = response.html;
      finalUrl = response.finalUrl;
    } catch {
      continue;
    }

    const childSitemaps = parseSitemapChildUrls(xml);
    childSitemaps.forEach((child) => {
      const normalized = toAbsoluteUrl(child, finalUrl);
      if (normalized && !visited.has(normalized)) toVisit.push(normalized);
    });

    const entries = parseSitemapUrlEntries(xml);
    for (const entry of entries) {
      if (discovered.size >= params.maxUrls) break;
      const candidate = new URL(entry.url);
      if (
        !isAllowedCandidateHost(params.provider, source.hostname, candidate.hostname) ||
        isLikelyLoginUrl(candidate.toString())
      ) {
        continue;
      }
      const score = scoreArticlePath(params.provider, candidate.pathname);
      if (score <= 0) continue;
      const lastmodMs = entry.lastmod ? Date.parse(entry.lastmod) : Number.NaN;
      if (
        Number.isFinite(params.sinceEpochMs) &&
        Number.isFinite(lastmodMs) &&
        lastmodMs < (params.sinceEpochMs as number)
      ) {
        continue;
      }
      const existing = discovered.get(candidate.toString()) ?? 0;
      if (Number.isFinite(lastmodMs)) {
        discovered.set(candidate.toString(), Math.max(existing, Math.floor(lastmodMs)));
      } else if (!discovered.has(candidate.toString())) {
        discovered.set(candidate.toString(), 0);
      }
    }
  }

  return discovered;
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
  if (haystack.includes("semianalysis")) return "semianalysis";
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
  forceSenders?: string[];
}): ExternalResearchHookCandidate | null => {
  const parsed = parseGmailHookMessage(params.message);
  if (!parsed) return null;

  const normalizedAllowedSenders = (params.allowedSenders ?? [])
    .map((sender) => normalizeEmail(sender))
    .filter(Boolean);
  const normalizedForceSenders = (params.forceSenders ?? [])
    .map((sender) => normalizeEmail(sender))
    .filter(Boolean);
  const senderForced = normalizedForceSenders.includes(parsed.senderEmail.toLowerCase());
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

  if (!senderForced && !isResearchSubject && provider === "other") {
    return null;
  }
  if (!senderForced && isResearchSubject && !senderAllowed) {
    return null;
  }

  const sourceType: ExternalResearchSourceType = senderForced
    ? "email_research"
    : isResearchSubject
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
  if (senderForced) tags.add("sender-force-ingest");

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
  const canonicalUrl = safeCanonicalizeUrl(url);
  const ticker = normalizeTicker(params.ticker);
  const publishedAt = normalizeDate(params.publishedAt);
  const receivedAt = normalizeDate(params.receivedAt) || new Date(now).toISOString();
  const tags = parseCsv((params.tags ?? []).join(","));
  const normalizedContent = normalizeDocumentText(content);
  const sourceKey = buildSourceKey({
    sourceType,
    provider,
    sender,
    canonicalUrl,
  });
  const trustTier = inferDocumentTrustTier({
    canonicalUrl,
    sourceType,
    provider,
  });
  const materialityScore = scoreExternalDocumentMateriality({
    sourceType,
    ticker,
    title,
    content: normalizedContent,
    tags,
    canonicalUrl,
  });
  const metadata = {
    ...(params.metadata ?? {}),
    tags,
    sourceKey,
    canonicalUrl,
    trustTier,
    materialityScore,
  };

  const contentHash = fingerprintExternalDocument({
    sourceType,
    provider,
    sender,
    title,
    subject,
    url: canonicalUrl || url,
    content: normalizedContent,
    ticker,
  });

  const externalId = (params.externalId ?? "").trim();
  const baseUrl = (() => {
    try {
      const target = canonicalUrl || url;
      if (!target) return "";
      const parsed = new URL(target);
      return `${parsed.protocol}//${parsed.hostname}`;
    } catch {
      return "";
    }
  })();

  upsertResearchSource({
    dbPath: params.dbPath,
    sourceKey,
    sourceType,
    provider,
    sender,
    baseUrl,
    trustTier,
    metadata: {
      tags,
      lastSeenAt: new Date(now).toISOString(),
    },
  });

  const rawArtifactPath = writeRawExternalDocumentArtifactSync({
    sourceKey,
    contentHash,
    payload: {
      sourceType,
      provider,
      externalId,
      sender,
      title,
      subject,
      url,
      canonicalUrl,
      ticker,
      publishedAt,
      receivedAt,
      content,
      normalizedContent,
      metadata,
    },
  });

  const insertDocument = db.prepare(`
    INSERT INTO external_documents (
      source_type,
      provider,
      source_key,
      external_id,
      sender,
      title,
      subject,
      url,
      canonical_url,
      ticker,
      published_at,
      received_at,
      content,
      normalized_content,
      content_hash,
      trust_tier,
      materiality_score,
      raw_artifact_path,
      metadata,
      fetched_at
    )
    VALUES (
      @source_type,
      @provider,
      @source_key,
      @external_id,
      @sender,
      @title,
      @subject,
      @url,
      @canonical_url,
      @ticker,
      @published_at,
      @received_at,
      @content,
      @normalized_content,
      @content_hash,
      @trust_tier,
      @materiality_score,
      @raw_artifact_path,
      @metadata,
      @fetched_at
    )
    ON CONFLICT(content_hash) DO UPDATE SET
      provider=excluded.provider,
      source_key=excluded.source_key,
      external_id=CASE WHEN excluded.external_id<>'' THEN excluded.external_id ELSE external_documents.external_id END,
      sender=excluded.sender,
      title=excluded.title,
      subject=excluded.subject,
      url=excluded.url,
      canonical_url=excluded.canonical_url,
      ticker=excluded.ticker,
      published_at=excluded.published_at,
      received_at=excluded.received_at,
      content=excluded.content,
      normalized_content=excluded.normalized_content,
      trust_tier=excluded.trust_tier,
      materiality_score=excluded.materiality_score,
      raw_artifact_path=excluded.raw_artifact_path,
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
    source_key: sourceKey,
    external_id: externalId,
    sender,
    title,
    subject,
    url,
    canonical_url: canonicalUrl,
    ticker,
    published_at: publishedAt,
    received_at: receivedAt,
    content,
    normalized_content: normalizedContent,
    content_hash: contentHash,
    trust_tier: trustTier,
    materiality_score: materialityScore,
    raw_artifact_path: rawArtifactPath,
    metadata: JSON.stringify(metadata),
    fetched_at: now,
  }) as { id: number };

  const chunkMeta = JSON.stringify({
    sourceType,
    provider,
    sourceKey,
    sender,
    title,
    subject,
    url,
    canonicalUrl,
    ticker,
    publishedAt: publishedAt || undefined,
    receivedAt,
    tags,
    trustTier,
    materialityScore,
  });
  const chunks = chunkText(normalizedContent, 220);
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

  extractStructuredResearchFromExternalDocument({
    documentId: row.id,
    dbPath: params.dbPath,
  });

  let reportId: number | undefined;
  let thesisId: number | undefined;
  let thesisDiffId: number | undefined;
  let thesisAlertId: number | undefined;
  let watchlistRefreshId: number | undefined;
  if (ticker) {
    const report = buildExternalResearchStructuredReport({
      ticker,
      dbPath: params.dbPath,
    });
    const storedReport = storeExternalResearchStructuredReport({
      report,
      dbPath: params.dbPath,
    });
    reportId = storedReport.id;

    const previousThesis = getLatestExternalResearchThesis({
      ticker,
      dbPath: params.dbPath,
    });
    const storedThesis = storeExternalResearchThesis({
      thesis: buildExternalResearchThesisFromReport({
        report,
        reportId,
      }),
      dbPath: params.dbPath,
    });
    thesisId = storedThesis.id;
    const storedDiff = storeExternalResearchThesisDiff({
      previous: previousThesis,
      current: storedThesis,
      dbPath: params.dbPath,
    });
    thesisDiffId = storedDiff.id;
    thesisAlertId =
      persistExternalResearchThesisBreakAlert({
        thesis: storedThesis,
        diff: storedDiff,
        dbPath: params.dbPath,
      }) ?? undefined;

    watchlistRefreshId =
      enqueueWatchlistRefresh({
        ticker,
        sourceDocumentId: row.id,
        materialityScore,
        reason: `${sourceType}:${title}`,
        dbPath: params.dbPath,
      })?.id ?? undefined;
  }

  return {
    id: row.id,
    chunks: chunks.length,
    sourceType,
    provider,
    ticker: ticker || undefined,
    title,
    reportId,
    thesisId,
    thesisDiffId,
    thesisAlertId,
    watchlistRefreshId,
  };
};

const providerPriorityScore = (provider: string): number => {
  const normalized = provider.toLowerCase();
  if (normalized === "stratechery") return 1;
  if (normalized === "semianalysis") return 0.96;
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
  sinceDate?: string;
  useSitemaps?: boolean;
  sitemapMaxUrls?: number;
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
  const useSitemaps = params.useSitemaps !== false;
  const sitemapMaxUrls = Math.max(
    1,
    Math.min(
      NEWSLETTER_SITEMAP_MAX_URLS,
      Math.round(params.sitemapMaxUrls ?? NEWSLETTER_SITEMAP_MAX_URLS),
    ),
  );
  const sinceDate = normalizeLowerBoundDate(params.sinceDate);
  const sinceEpochMs = sinceDate ? Date.parse(sinceDate) : Number.NaN;
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

    const candidateLastmodMs = new Map<string, number>();
    extractCandidateLinks(provider, archiveHtml, archiveUrl, maxLinksPerSource).forEach((url) => {
      if (!candidateLastmodMs.has(url)) candidateLastmodMs.set(url, 0);
    });

    if (useSitemaps) {
      const sitemapCandidates = await collectSitemapCandidates({
        provider,
        sourceUrl,
        userAgent: params.userAgent,
        cookie,
        fetchFn: params.fetchFn,
        env,
        maxUrls: sitemapMaxUrls,
        sinceEpochMs: Number.isFinite(sinceEpochMs) ? sinceEpochMs : undefined,
      });
      sitemapCandidates.forEach((lastmodMs, url) => {
        const existing = candidateLastmodMs.get(url) ?? 0;
        if (!candidateLastmodMs.has(url) || lastmodMs > existing) {
          candidateLastmodMs.set(url, lastmodMs);
        }
      });
    }

    const candidates = Array.from(candidateLastmodMs.entries())
      .sort((left, right) => right[1] - left[1])
      .map(([url]) => url);

    for (const candidateUrl of candidates) {
      if (attempted >= maxDocs) break;
      const candidateLastmod = candidateLastmodMs.get(candidateUrl) ?? 0;
      if (
        Number.isFinite(sinceEpochMs) &&
        candidateLastmod > 0 &&
        candidateLastmod < sinceEpochMs
      ) {
        continue;
      }
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
        const publishedEpochMs = extracted.publishedAt
          ? Date.parse(extracted.publishedAt)
          : Number.NaN;
        if (
          Number.isFinite(sinceEpochMs) &&
          ((Number.isFinite(publishedEpochMs) && publishedEpochMs < sinceEpochMs) ||
            (!Number.isFinite(publishedEpochMs) &&
              candidateLastmod > 0 &&
              candidateLastmod < sinceEpochMs))
        ) {
          docs.push({
            provider,
            sourceUrl,
            url: page.finalUrl,
            title: extracted.title,
            ingested: false,
            reason: `published before since-date ${sinceDate.slice(0, 10)}`,
          });
          skipped += 1;
          continue;
        }
        if (isLikelyLoginUrl(page.finalUrl)) {
          docs.push({
            provider,
            sourceUrl,
            url: page.finalUrl,
            title: extracted.title,
            ingested: false,
            reason: "final URL redirected to login",
          });
          skipped += 1;
          continue;
        }
        const finalPathScore = scoreArticlePath(provider, new URL(page.finalUrl).pathname);
        if (finalPathScore <= 0) {
          docs.push({
            provider,
            sourceUrl,
            url: page.finalUrl,
            title: extracted.title,
            ingested: false,
            reason: "final URL is not an article path",
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
