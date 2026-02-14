import { createHash } from "node:crypto";
import type { MemoryDocType, RawChunkRow } from "./types.js";

const BOILERPLATE_LINE_PATTERNS: RegExp[] = [
  /(^|\s)(cookie|cookies|accept all|privacy policy|terms of service)(\s|$)/i,
  /(^|\s)(subscribe|sign in|log in|create account|newsletter)(\s|$)/i,
  /(^|\s)(menu|navigation|breadcrumb|advertisement|sponsored)(\s|$)/i,
  /(^|\s)(back to top|share this|all rights reserved)(\s|$)/i,
  /^\s*(home|about|contact|careers|jobs|help)\s*$/i,
];

export type ChunkingOptions = {
  targetTokens: number;
  minTokens: number;
  maxTokens: number;
  overlapRatio: number;
};

export type ChunkSeed = {
  doc_id: string;
  text: string;
  importance: number;
  category: string;
  company: string;
  ticker: string;
  doc_type: MemoryDocType;
  published_at: number;
  retrieved_at: number;
  source_url: string;
  source_title: string;
  section: string;
  page: number;
  chunk_index: number;
  char_start: number;
  tags: string[];
};

export type ParsedDocMetadata = {
  source_url: string;
  source_title: string;
  company: string;
  ticker: string;
  doc_type: MemoryDocType;
  published_at: number | null;
  retrieved_at: number;
  section: string;
  tags: string[];
};

export const DEFAULT_CHUNKING: ChunkingOptions = {
  targetTokens: 700,
  minTokens: 500,
  maxTokens: 900,
  overlapRatio: 0.12,
};

export function normalizeText(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function hashNormalizedText(text: string): string {
  return createHash("sha256").update(normalizeText(text)).digest("hex");
}

export function removeBoilerplate(raw: string): string {
  const seen = new Set<string>();
  const kept = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .filter((line) => !BOILERPLATE_LINE_PATTERNS.some((pattern) => pattern.test(line)))
    .filter((line) => {
      const norm = normalizeText(line);
      if (!norm) {
        return false;
      }
      if (seen.has(norm)) {
        return false;
      }
      seen.add(norm);
      return true;
    });
  return kept.join("\n");
}

function splitByHeadings(cleanText: string): Array<{ section: string; text: string }> {
  const lines = cleanText.split(/\r?\n/);
  const sections: Array<{ section: string; text: string }> = [];
  let currentSection = "body";
  let currentLines: string[] = [];

  const flush = () => {
    const text = currentLines.join(" ").trim();
    if (text) {
      sections.push({ section: currentSection, text });
    }
    currentLines = [];
  };

  for (const line of lines) {
    const heading = /^#{1,6}\s+(.+)/.exec(line)?.[1]?.trim();
    const upperHeading = /^[A-Z][A-Z0-9\s:&/-]{6,}$/.test(line) ? line.trim() : "";
    const numberedHeading = /^\d+(\.\d+)*\.\s+(.+)/.exec(line)?.[2]?.trim() ?? "";
    const inferredHeading = heading || upperHeading || numberedHeading;
    if (inferredHeading) {
      if (currentLines.length > 0) {
        flush();
      }
      currentSection = inferredHeading.toLowerCase();
      continue;
    }
    currentLines.push(line.trim());
  }
  flush();
  return sections.length > 0 ? sections : [{ section: "body", text: cleanText }];
}

function tokenizeForChunking(text: string): string[] {
  return text
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean);
}

function untokenize(tokens: string[]): string {
  return tokens.join(" ").trim();
}

function buildCitationKey(params: {
  sourceUrl: string;
  chunkHash: string;
  page: number;
  charStart: number;
  charEnd: number;
  section: string;
}): string {
  const source = params.sourceUrl || "local://memory";
  const section = params.section ? `:${params.section.slice(0, 24).replace(/\s+/g, "_")}` : "";
  return `${source}#${params.chunkHash.slice(0, 12)}:p${params.page}:c${params.charStart}-${params.charEnd}${section}`;
}

export function extractPublishedAt(raw: string): number | null {
  const match =
    raw.match(/\b(20\d{2})-(\d{2})-(\d{2})\b/) ??
    raw.match(/\b(20\d{2})\/(\d{2})\/(\d{2})\b/) ??
    raw.match(/\b(20\d{2})\.(\d{2})\.(\d{2})\b/);
  if (!match) {
    return null;
  }
  const iso = `${match[1]}-${match[2]}-${match[3]}`;
  const ts = Date.parse(`${iso}T00:00:00Z`);
  return Number.isFinite(ts) ? ts : null;
}

function inferDocType(text: string): MemoryDocType {
  const lower = text.toLowerCase();
  if (/\bform 10-k\b|\b20-f\b|\b8-k\b|\b10-q\b/.test(lower)) {
    return "filing";
  }
  if (/\bearnings call\b|\btranscript\b|\bq\d\b/.test(lower)) {
    return "transcript";
  }
  if (/\bpress release\b|\breported by\b|\bnews\b/.test(lower)) {
    return "news";
  }
  if (/\bmemo\b|\binvestor letter\b/.test(lower)) {
    return "memo";
  }
  if (/\bresearch\b|\banalysis\b/.test(lower)) {
    return "research";
  }
  return "other";
}

function inferTicker(text: string): string {
  const direct = text.match(/\b[A-Z]{1,5}\b/g) ?? [];
  const ticker = direct.find((value) => !["AI", "CEO", "CFO", "USD", "EPS"].includes(value));
  return ticker ?? "";
}

export function inferMetadata(input: {
  text: string;
  sourceUrl: string;
  sourceTitle?: string;
  ticker?: string;
  company?: string;
  docType?: MemoryDocType;
  section?: string;
  publishedAt?: number;
  retrievedAt?: number;
  tags?: string[];
}): ParsedDocMetadata {
  const cleaned = removeBoilerplate(input.text);
  const published = Number.isFinite(input.publishedAt) ? input.publishedAt : extractPublishedAt(cleaned);
  const ticker = input.ticker?.trim().toUpperCase() || inferTicker(cleaned);
  const company = input.company?.trim() || "";
  const docType = input.docType ?? inferDocType(cleaned);
  let sourceTitle = input.sourceTitle?.trim() || "";
  if (!sourceTitle) {
    try {
      sourceTitle = new URL(input.sourceUrl).hostname;
    } catch {
      sourceTitle = input.sourceUrl;
    }
  }
  return {
    source_url: input.sourceUrl,
    source_title: sourceTitle,
    company,
    ticker,
    doc_type: docType,
    published_at: published ?? null,
    retrieved_at:
      typeof input.retrievedAt === "number" && Number.isFinite(input.retrievedAt)
        ? Math.floor(input.retrievedAt)
        : Date.now(),
    section: input.section?.trim() || "",
    tags: input.tags?.map((tag) => tag.trim().toLowerCase()).filter(Boolean) ?? [],
  };
}

export function chunkDocument(
  input: ChunkSeed,
  options: Partial<ChunkingOptions> = {},
): RawChunkRow[] {
  const cfg: ChunkingOptions = { ...DEFAULT_CHUNKING, ...options };
  const clean = removeBoilerplate(input.text);
  const sections = splitByHeadings(clean);
  const out: RawChunkRow[] = [];
  const overlap = Math.max(1, Math.floor(cfg.targetTokens * cfg.overlapRatio));

  let globalChunkIndex = input.chunk_index;
  let charCursor = input.char_start;

  for (const sectionBlock of sections) {
    const tokens = tokenizeForChunking(sectionBlock.text);
    if (tokens.length === 0) {
      continue;
    }

    let start = 0;
    while (start < tokens.length) {
      const remaining = tokens.length - start;
      let size = Math.min(cfg.targetTokens, remaining);
      if (remaining > cfg.minTokens && size < cfg.minTokens) {
        size = Math.min(cfg.maxTokens, remaining);
      }
      const window = tokens.slice(start, start + size);
      const text = untokenize(window);
      if (!text) {
        break;
      }

      const textNormHash = hashNormalizedText(text);
      const charStart = charCursor;
      const charEnd = charStart + text.length;
      const page = input.page > 0 ? input.page : 1;
      const section = input.section || sectionBlock.section || "body";
      const citationKey = buildCitationKey({
        sourceUrl: input.source_url,
        chunkHash: textNormHash,
        page,
        charStart,
        charEnd,
        section,
      });

      out.push({
        chunk_id: "",
        doc_id: input.doc_id,
        text,
        text_norm_hash: textNormHash,
        source_url: input.source_url,
        source_title: input.source_title,
        doc_type: input.doc_type,
        company: input.company,
        ticker: input.ticker,
        published_at: input.published_at,
        retrieved_at: input.retrieved_at,
        section,
        page,
        char_start: charStart,
        char_end: charEnd,
        embedding: [],
        tags: input.tags,
        // compatibility fields
        chunk_hash: textNormHash,
        text_norm: normalizeText(text),
        citation_key: citationKey,
        search_text: text,
        chunk_index: globalChunkIndex++,
        token_count: window.length,
        ingested_at: 0,
        category: input.category,
        importance: input.importance,
        vector: [],
      });

      charCursor = charEnd + 1;
      if (start + size >= tokens.length) {
        break;
      }
      start += Math.max(1, size - overlap);
    }
  }

  return out;
}
