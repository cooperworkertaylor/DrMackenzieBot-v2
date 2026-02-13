import { createHash } from "node:crypto";
import type { MemoryChunkRow } from "./types.js";

const BOILERPLATE_LINE_PATTERNS: RegExp[] = [
  /(^|\s)(cookie|cookies|accept all|privacy policy|terms of service)(\s|$)/i,
  /(^|\s)(subscribe|sign in|log in|create account|newsletter)(\s|$)/i,
  /(^|\s)(menu|navigation|breadcrumb|advertisement|sponsored)(\s|$)/i,
  /(^|\s)(back to top|share this|all rights reserved)(\s|$)/i,
];

export type ChunkingOptions = {
  targetTokens: number;
  minTokens: number;
  maxTokens: number;
  overlapRatio: number;
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

function splitByHeadings(cleanText: string): string[] {
  const lines = cleanText.split(/\r?\n/);
  const sections: string[] = [];
  let current: string[] = [];

  const flush = () => {
    const text = current.join(" ").trim();
    if (text) {
      sections.push(text);
    }
    current = [];
  };

  for (const line of lines) {
    const heading =
      /^#{1,6}\s+/.test(line) ||
      /^[A-Z][A-Z0-9\s:&-]{6,}$/.test(line) ||
      /^\d+\.\s+[A-Z]/.test(line);
    if (heading && current.length > 0) {
      flush();
    }
    current.push(line.trim());
  }
  flush();
  return sections;
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
}): string {
  const source = params.sourceUrl || "local://memory";
  return `${source}#${params.chunkHash.slice(0, 12)}:p${params.page}:c${params.charStart}-${params.charEnd}`;
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

export type ChunkSeed = {
  doc_id: string;
  text: string;
  importance: number;
  category: string;
  company: string;
  ticker: string;
  doc_type: MemoryChunkRow["doc_type"];
  published_at: number;
  source_url: string;
  section: string;
  page: number;
  chunk_index: number;
  char_start: number;
};

export function chunkDocument(
  input: ChunkSeed,
  options: Partial<ChunkingOptions> = {},
): MemoryChunkRow[] {
  const cfg: ChunkingOptions = { ...DEFAULT_CHUNKING, ...options };
  const clean = removeBoilerplate(input.text);
  const sections = splitByHeadings(clean);
  const out: MemoryChunkRow[] = [];
  const overlap = Math.max(1, Math.floor(cfg.targetTokens * cfg.overlapRatio));

  let globalChunkIndex = input.chunk_index;
  let charCursor = input.char_start;

  for (const sectionText of sections) {
    const tokens = tokenizeForChunking(sectionText);
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

      const chunkHash = hashNormalizedText(text);
      const charStart = charCursor;
      const charEnd = charStart + text.length;
      const page = input.page > 0 ? input.page : 1;

      out.push({
        chunk_id: "",
        doc_id: input.doc_id,
        text,
        text_norm: normalizeText(text),
        vector: [],
        importance: input.importance,
        category: input.category,
        company: input.company,
        ticker: input.ticker,
        doc_type: input.doc_type,
        published_at: input.published_at,
        ingested_at: 0,
        source_url: input.source_url,
        section: input.section,
        page,
        chunk_index: globalChunkIndex++,
        chunk_hash: chunkHash,
        char_start: charStart,
        char_end: charEnd,
        token_count: window.length,
        citation_key: buildCitationKey({
          sourceUrl: input.source_url,
          chunkHash,
          page,
          charStart,
          charEnd,
        }),
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
