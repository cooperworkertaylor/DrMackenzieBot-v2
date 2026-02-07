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

const RESEARCH_SUBJECT_DEFAULT = "RESEARCH";
const DIGEST_MAX_CONTENT_CHARS = 12_000;

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
