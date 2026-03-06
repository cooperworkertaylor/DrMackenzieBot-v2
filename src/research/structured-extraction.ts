import { addClaimEvidence, createResearchClaim, upsertResearchEntity } from "./memory-graph.js";
import { openResearchDb } from "./db.js";
import type { ResearchDb } from "./db.js";
import { upsertPointInTimeEvent, upsertPointInTimeFact } from "./knowledge-graph.js";

const SENTENCE_SPLIT_RE = /(?<=[.!?])\s+|\n{2,}/g;

const CLAIM_KEYWORDS = [
  "revenue",
  "margin",
  "guidance",
  "valuation",
  "growth",
  "demand",
  "pricing",
  "competition",
  "capex",
  "earnings",
  "catalyst",
  "risk",
  "backlog",
  "bookings",
  "estimate",
  "supply",
];

const EVENT_PATTERNS: Array<{ eventType: string; pattern: RegExp }> = [
  { eventType: "earnings", pattern: /\bearnings|results|quarter\b/i },
  { eventType: "guidance", pattern: /\bguidance|outlook|forecast\b/i },
  { eventType: "m&a", pattern: /\bacquisition|merger|buyout|takeover\b/i },
  { eventType: "financing", pattern: /\bfinancing|offering|raised|funding|debt\b/i },
  { eventType: "product", pattern: /\blaunch|product|release|rollout\b/i },
  { eventType: "partnership", pattern: /\bpartnership|partnered|agreement|contract\b/i },
  { eventType: "regulation", pattern: /\bregulation|regulatory|probe|antitrust|litigation\b/i },
];

const METRIC_PATTERNS: Array<{
  metricKey: string;
  metricKind: string;
  unit: string;
  pattern: RegExp;
}> = [
  {
    metricKey: "revenue_growth_pct",
    metricKind: "percentage",
    unit: "percent",
    pattern: /\brevenue\b[\s\S]{0,32}?(\d+(?:\.\d+)?)%/i,
  },
  {
    metricKey: "gross_margin_pct",
    metricKind: "percentage",
    unit: "percent",
    pattern: /\bgross margin\b[\s\S]{0,24}?(\d+(?:\.\d+)?)%/i,
  },
  {
    metricKey: "operating_margin_pct",
    metricKind: "percentage",
    unit: "percent",
    pattern: /\boperating margin\b[\s\S]{0,24}?(\d+(?:\.\d+)?)%/i,
  },
  {
    metricKey: "capex_amount",
    metricKind: "currency",
    unit: "usd",
    pattern: /\bcapex\b[\s\S]{0,24}?\$?(\d+(?:\.\d+)?)\s*(billion|million|bn|mm|m)\b/i,
  },
  {
    metricKey: "bookings_growth_pct",
    metricKind: "percentage",
    unit: "percent",
    pattern: /\bbookings\b[\s\S]{0,24}?(\d+(?:\.\d+)?)%/i,
  },
];

export type StructuredExtractionSummary = {
  entityId?: number;
  claimsCreated: number;
  eventsCreated: number;
  factsCreated: number;
  extractedAt: number;
};

type ExternalDocumentRow = {
  id: number;
  source_type: string;
  provider: string;
  source_key: string;
  title: string;
  subject: string;
  canonical_url: string;
  url: string;
  ticker: string;
  published_at: string;
  received_at: string;
  content: string;
  normalized_content: string;
  trust_tier: number;
  materiality_score: number;
  metadata?: string;
};

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

const parseJsonObject = (value?: string): Record<string, unknown> => {
  if (!value?.trim()) return {};
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
};

const normalizeDate = (value?: string): string => {
  if (!value?.trim()) return "";
  const ts = Date.parse(value);
  if (!Number.isFinite(ts)) return "";
  return new Date(ts).toISOString().slice(0, 10);
};

const sentenceList = (text: string): string[] =>
  text
    .split(SENTENCE_SPLIT_RE)
    .map((sentence) => sentence.replaceAll(/\s+/g, " ").trim())
    .filter((sentence) => sentence.length >= 40 && sentence.length <= 320);

const claimTypeForSentence = (sentence: string): string => {
  const lowered = sentence.toLowerCase();
  if (/\b(bull|upside|opportunity)\b/.test(lowered)) return "bull_case";
  if (/\b(bear|downside|risk|threat)\b/.test(lowered)) return "risk";
  if (/\b(valuation|multiple|priced|estimate)\b/.test(lowered)) return "valuation";
  return "external_research";
};

const selectClaimSentences = (text: string): string[] => {
  const selected: string[] = [];
  for (const sentence of sentenceList(text)) {
    const lowered = sentence.toLowerCase();
    if (!CLAIM_KEYWORDS.some((keyword) => lowered.includes(keyword))) continue;
    selected.push(sentence);
    if (selected.length >= 6) break;
  }
  return selected;
};

const eventTypeForText = (title: string, text: string): string | null => {
  const haystack = `${title}\n${text}`;
  for (const rule of EVENT_PATTERNS) {
    if (rule.pattern.test(haystack)) return rule.eventType;
  }
  return null;
};

const normalizeMetricValue = (value: number, magnitude?: string): number => {
  const mag = (magnitude ?? "").toLowerCase();
  if (mag === "billion" || mag === "bn") return value * 1_000_000_000;
  if (mag === "million" || mag === "mm" || mag === "m") return value * 1_000_000;
  return value;
};

const extractMetricCandidates = (text: string): Array<{
  metricKey: string;
  metricKind: string;
  unit: string;
  valueNum: number;
  sourceSentence: string;
}> => {
  const out: Array<{
    metricKey: string;
    metricKind: string;
    unit: string;
    valueNum: number;
    sourceSentence: string;
  }> = [];
  for (const sentence of sentenceList(text)) {
    for (const metric of METRIC_PATTERNS) {
      const match = sentence.match(metric.pattern);
      if (!match?.[1]) continue;
      const raw = Number.parseFloat(match[1]);
      if (!Number.isFinite(raw)) continue;
      out.push({
        metricKey: metric.metricKey,
        metricKind: metric.metricKind,
        unit: metric.unit,
        valueNum: normalizeMetricValue(raw, match[2]),
        sourceSentence: sentence,
      });
    }
  }
  return out;
};

const updateExternalDocumentExtractionMetadata = (params: {
  db: ResearchDb;
  documentId: number;
  summary: StructuredExtractionSummary;
}) => {
  const existing = params.db
    .prepare(`SELECT metadata FROM external_documents WHERE id=?`)
    .get(params.documentId) as { metadata?: string } | undefined;
  const metadata = parseJsonObject(existing?.metadata);
  metadata.extraction = {
    status: "completed",
    extractedAt: new Date(params.summary.extractedAt).toISOString(),
    claimsCreated: params.summary.claimsCreated,
    eventsCreated: params.summary.eventsCreated,
    factsCreated: params.summary.factsCreated,
    entityId: params.summary.entityId,
  };
  params.db
    .prepare(`UPDATE external_documents SET metadata=? WHERE id=?`)
    .run(JSON.stringify(metadata), params.documentId);
};

const findExistingClaimId = (params: {
  db: ResearchDb;
  entityId: number;
  claimText: string;
  documentId: number;
}): number | undefined => {
  const row = params.db
    .prepare(
      `SELECT c.id
       FROM research_claims c
       JOIN research_claim_evidence e ON e.claim_id = c.id
       WHERE c.entity_id = ?
         AND c.claim_text = ?
         AND e.source_table = 'external_documents'
         AND e.ref_id = ?
       ORDER BY c.id DESC
       LIMIT 1`,
    )
    .get(params.entityId, params.claimText, params.documentId) as { id?: number } | undefined;
  return typeof row?.id === "number" ? row.id : undefined;
};

export const extractStructuredResearchFromExternalDocument = (params: {
  documentId: number;
  dbPath?: string;
}): StructuredExtractionSummary => {
  const db = openResearchDb(params.dbPath);
  const row = db
    .prepare(
      `SELECT
         id, source_type, provider, source_key, title, subject, canonical_url, url, ticker,
         published_at, received_at, content, normalized_content, trust_tier, materiality_score, metadata
       FROM external_documents
       WHERE id=?`,
    )
    .get(params.documentId) as ExternalDocumentRow | undefined;
  if (!row) throw new Error(`external document not found: id=${params.documentId}`);

  const ticker = normalizeTicker(row.ticker);
  if (!ticker) {
    const summary: StructuredExtractionSummary = {
      claimsCreated: 0,
      eventsCreated: 0,
      factsCreated: 0,
      extractedAt: Date.now(),
    };
    updateExternalDocumentExtractionMetadata({ db, documentId: row.id, summary });
    return summary;
  }

  const entity = upsertResearchEntity({
    canonicalName: ticker,
    ticker,
    metadata: {
      source: "external_document_extraction",
      latestSourceKey: row.source_key,
    },
    dbPath: params.dbPath,
  });

  const text = row.normalized_content?.trim() || row.content.trim();
  const citationUrl = row.canonical_url || row.url;
  const asOfDate = normalizeDate(row.published_at || row.received_at);
  let claimsCreated = 0;
  let eventsCreated = 0;
  let factsCreated = 0;

  for (const sentence of selectClaimSentences(text)) {
    const existingClaimId = findExistingClaimId({
      db,
      entityId: entity.id,
      claimText: sentence,
      documentId: row.id,
    });
    if (existingClaimId) continue;

    const claim = createResearchClaim({
      entityId: entity.id,
      claimText: sentence,
      claimType: claimTypeForSentence(sentence),
      confidence: Math.min(0.95, Math.max(0.45, row.materiality_score || 0.6)),
      validFrom: asOfDate,
      metadata: {
        sourceType: row.source_type,
        provider: row.provider,
        sourceKey: row.source_key,
        trustTier: row.trust_tier,
        externalDocumentId: row.id,
      },
      dbPath: params.dbPath,
    });
    addClaimEvidence({
      claimId: claim.id,
      sourceTable: "external_documents",
      refId: row.id,
      citationUrl,
      excerptText: sentence,
      metadata: {
        sourceKey: row.source_key,
        title: row.title,
      },
      dbPath: params.dbPath,
    });
    claimsCreated += 1;
  }

  const eventType = eventTypeForText(row.title, text);

  const metadata = parseJsonObject(row.metadata);
  const extractedDateMentions = Array.from(
    new Set(
      [row.published_at, row.received_at]
        .map((value) => normalizeDate(value))
        .filter(Boolean),
    ),
  );

  let eventId: number | undefined;
  if (eventType) {
    const result = upsertPointInTimeEvent({
      dbPath: params.dbPath,
      ticker,
      title: row.title || row.subject,
      eventType,
      sourceRefId: row.id,
      sourceUrl: citationUrl,
      eventDate: extractedDateMentions[0],
      payload: {
        sourceType: row.source_type,
        provider: row.provider,
        sourceKey: row.source_key,
        materialityScore: row.materiality_score,
        metadata,
      },
    });
    eventId = result.id;
    if (result.inserted) eventsCreated += 1;
  }

  for (const metric of extractMetricCandidates(text)) {
    const result = upsertPointInTimeFact({
      dbPath: params.dbPath,
      ticker,
      eventId,
      metricKey: metric.metricKey,
      metricKind: metric.metricKind,
      valueNum: metric.valueNum,
      unit: metric.unit,
      asOfDate,
      sourceRefId: row.id,
      sourceUrl: citationUrl,
      metadata: {
        sourceSentence: metric.sourceSentence,
        sourceKey: row.source_key,
      },
    });
    if (result.inserted) factsCreated += 1;
  }

  const summary: StructuredExtractionSummary = {
    entityId: entity.id,
    claimsCreated,
    eventsCreated,
    factsCreated,
    extractedAt: Date.now(),
  };
  updateExternalDocumentExtractionMetadata({ db, documentId: row.id, summary });
  return summary;
};
