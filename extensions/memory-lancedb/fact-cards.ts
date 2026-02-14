import { createHash } from "node:crypto";
import { hashNormalizedText, normalizeText } from "./ingestion.js";
import type { CitationRecord, FactCardConfidence, FactCardRow, MetricValue, RawChunkRow } from "./types.js";

const ENTITY_BLACKLIST = new Set(["THE", "AND", "FOR", "WITH", "FROM", "THIS", "THAT", "Q1", "Q2", "Q3", "Q4"]);

function splitSentences(text: string): string[] {
  return text
    .split(/(?<=[.!?])\s+(?=[A-Z0-9])/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
}

function normalizeClaim(sentence: string): string {
  const cleaned = sentence.replace(/\s+/g, " ").trim();
  return cleaned.length > 260 ? `${cleaned.slice(0, 257)}...` : cleaned;
}

function extractEntities(text: string): string[] {
  const tickers = text.match(/\b[A-Z]{1,5}\b/g) ?? [];
  const products = text.match(/\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\b/g) ?? [];
  const entities = [...tickers, ...products]
    .map((value) => value.trim())
    .filter((value) => value.length > 1)
    .filter((value) => !ENTITY_BLACKLIST.has(value.toUpperCase()));
  return Array.from(new Set(entities)).slice(0, 12);
}

function extractMetrics(text: string, asOf: number | null): MetricValue[] {
  const metrics: MetricValue[] = [];
  const percentRegex = /\b([A-Za-z][A-Za-z\s/-]{2,40})\s(?:was|were|is|at|to|of|=)?\s*(-?\d+(?:\.\d+)?)\s*%/g;
  const moneyRegex = /\b([A-Za-z][A-Za-z\s/-]{2,40})\s(?:was|were|is|at|to|of|=)?\s*\$?(\d+(?:\.\d+)?)\s*(bn|billion|m|million|k|thousand)?/gi;

  let match: RegExpExecArray | null;
  while ((match = percentRegex.exec(text)) !== null) {
    metrics.push({
      name: normalizeText(match[1]).slice(0, 40),
      value: match[2],
      unit: "%",
      period: "",
      as_of: asOf,
    });
  }
  while ((match = moneyRegex.exec(text)) !== null) {
    const name = normalizeText(match[1]).slice(0, 40);
    const value = match[2];
    if (!name || Number.isNaN(Number(value))) {
      continue;
    }
    metrics.push({
      name,
      value,
      unit: match[3] ? match[3].toLowerCase() : "count",
      period: "",
      as_of: asOf,
    });
  }
  return metrics.slice(0, 8);
}

function scoreConfidence(claim: string, evidence: string, metrics: MetricValue[]): FactCardConfidence {
  const hasNumber = /\d/.test(claim) || /\d/.test(evidence);
  if (metrics.length >= 2 || (hasNumber && evidence.length > 30)) {
    return "high";
  }
  if (metrics.length > 0 || hasNumber) {
    return "med";
  }
  return "low";
}

function buildCitation(chunk: RawChunkRow): CitationRecord {
  return {
    doc_id: chunk.doc_id,
    chunk_id: chunk.chunk_id,
    page: chunk.page,
    section: chunk.section,
    url: chunk.source_url,
    published_at: chunk.published_at,
  };
}

function stableCardId(chunkId: string, claim: string): string {
  const digest = createHash("sha1").update(`${chunkId}:${normalizeText(claim)}`).digest("hex");
  return `${chunkId}:card:${digest.slice(0, 12)}`;
}

export function generateFactCardsFromChunk(chunk: RawChunkRow): FactCardRow[] {
  const sentences = splitSentences(chunk.text);
  if (sentences.length === 0) {
    return [];
  }

  const cards: FactCardRow[] = [];
  for (let i = 0; i < sentences.length; i++) {
    const claimSentence = sentences[i];
    if (!claimSentence || claimSentence.length < 16) {
      continue;
    }
    if (!/[A-Za-z]/.test(claimSentence)) {
      continue;
    }
    const claim = normalizeClaim(claimSentence);
    const evidenceCandidates = [sentences[i], sentences[i + 1]].filter(
      (line): line is string => Boolean(line?.trim()),
    );
    const evidenceBullets = evidenceCandidates
      .slice(0, 2)
      .map((line) => `- ${line.trim()}`)
      .join("\n");

    const metrics = extractMetrics(`${claim}\n${evidenceBullets}`, chunk.published_at);
    const confidence = scoreConfidence(claim, evidenceBullets, metrics);
    const citation = buildCitation(chunk);
    const entities = extractEntities(`${claim} ${evidenceBullets}`);
    const tags = Array.from(new Set([chunk.doc_type, chunk.ticker, ...chunk.tags].filter(Boolean))).map((tag) =>
      String(tag).toLowerCase(),
    );

    const cardId = stableCardId(chunk.chunk_id, claim);
    const textNormHash = hashNormalizedText(`${claim}\n${evidenceBullets}`);
    cards.push({
      card_id: cardId,
      doc_id: chunk.doc_id,
      chunk_id: chunk.chunk_id,
      claim,
      evidence_text: evidenceBullets,
      entities,
      metrics,
      as_of: chunk.published_at,
      doc_date: chunk.published_at,
      confidence,
      tags,
      source_url: chunk.source_url,
      source_title: chunk.source_title,
      doc_type: chunk.doc_type,
      company: chunk.company,
      ticker: chunk.ticker,
      published_at: chunk.published_at,
      retrieved_at: chunk.retrieved_at,
      citations: [citation],
      embedding: [],
      text_norm_hash: textNormHash,
      search_text: `${claim}\n${evidenceBullets}`,
    });
    if (cards.length >= 4) {
      break;
    }
  }

  return cards;
}
