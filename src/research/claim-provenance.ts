import { openResearchDb } from "./db.js";

export type ClaimProvenanceEvidence = {
  documentId: number;
  title: string;
  url: string;
  provider: string;
  sourceType: string;
  publishedAt?: string;
  trustTier: number;
};

export type ClaimProvenanceMatch = {
  claimId: number;
  claimText: string;
  claimType: string;
  confidence: number;
  validFrom?: string;
  matchScore: number;
  matchedTerms: string[];
  evidence: ClaimProvenanceEvidence[];
};

export type ClaimProvenanceReport = {
  ticker: string;
  topic: string;
  matches: ClaimProvenanceMatch[];
};

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

const normalizeDate = (value?: string): string | undefined => {
  if (!value?.trim()) return undefined;
  const ts = Date.parse(value);
  if (!Number.isFinite(ts)) return undefined;
  return new Date(ts).toISOString().slice(0, 10);
};

const tokenize = (value: string): string[] =>
  Array.from(
    new Set(
      value
        .toLowerCase()
        .split(/[^a-z0-9]+/g)
        .map((token) => token.trim())
        .filter((token) => token.length >= 3),
    ),
  );

const scoreClaim = (claimText: string, title: string, terms: string[]) => {
  const claimLower = claimText.toLowerCase();
  const titleLower = title.toLowerCase();
  let score = 0;
  const matchedTerms: string[] = [];
  for (const term of terms) {
    let matched = false;
    if (claimLower.includes(term)) {
      score += 3;
      matched = true;
    }
    if (titleLower.includes(term)) {
      score += 1;
      matched = true;
    }
    if (matched) matchedTerms.push(term);
  }
  return { score, matchedTerms };
};

export const explainResearchClaim = (params: {
  ticker: string;
  topic: string;
  dbPath?: string;
  maxClaims?: number;
  maxEvidencePerClaim?: number;
}): ClaimProvenanceReport => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
  const topic = params.topic.trim();
  if (!topic) throw new Error("topic is required");

  const entity = db
    .prepare(
      `SELECT id
       FROM research_entities
       WHERE ticker=?
       ORDER BY updated_at DESC, id DESC
       LIMIT 1`,
    )
    .get(ticker) as { id: number } | undefined;
  if (!entity) {
    throw new Error(`research entity not found for ticker=${ticker}`);
  }

  const rows = db
    .prepare(
      `SELECT
         c.id AS claim_id,
         c.claim_text,
         c.claim_type,
         c.confidence,
         c.valid_from,
         d.id AS document_id,
         d.title,
         COALESCE(NULLIF(d.canonical_url, ''), d.url) AS url,
         d.provider,
         d.source_type,
         COALESCE(NULLIF(d.published_at, ''), NULLIF(d.received_at, ''), '') AS published_at,
         d.trust_tier
       FROM research_claims c
       JOIN research_claim_evidence e
         ON e.claim_id = c.id
        AND e.source_table = 'external_documents'
       JOIN external_documents d
         ON d.id = e.ref_id
       WHERE c.entity_id = ?
         AND c.status = 'active'
       ORDER BY c.confidence DESC, c.updated_at DESC, d.trust_tier ASC, d.id DESC`,
    )
    .all(entity.id) as Array<{
    claim_id: number;
    claim_text: string;
    claim_type: string;
    confidence: number;
    valid_from?: string;
    document_id: number;
    title: string;
    url: string;
    provider: string;
    source_type: string;
    published_at?: string;
    trust_tier: number;
  }>;

  const terms = tokenize(topic);
  const grouped = new Map<number, ClaimProvenanceMatch>();
  for (const row of rows) {
    const scored = scoreClaim(row.claim_text, row.title, terms);
    const existing = grouped.get(row.claim_id);
    if (!existing) {
      grouped.set(row.claim_id, {
        claimId: row.claim_id,
        claimText: row.claim_text,
        claimType: row.claim_type,
        confidence: Math.max(0, Math.min(1, row.confidence)),
        validFrom: normalizeDate(row.valid_from),
        matchScore: scored.score,
        matchedTerms: scored.matchedTerms,
        evidence: [
          {
            documentId: row.document_id,
            title: row.title,
            url: row.url,
            provider: row.provider,
            sourceType: row.source_type,
            publishedAt: normalizeDate(row.published_at),
            trustTier: row.trust_tier,
          },
        ],
      });
      continue;
    }
    existing.matchScore = Math.max(existing.matchScore, scored.score);
    existing.matchedTerms = Array.from(new Set([...existing.matchedTerms, ...scored.matchedTerms]));
    if (!existing.evidence.some((item) => item.documentId === row.document_id)) {
      existing.evidence.push({
        documentId: row.document_id,
        title: row.title,
        url: row.url,
        provider: row.provider,
        sourceType: row.source_type,
        publishedAt: normalizeDate(row.published_at),
        trustTier: row.trust_tier,
      });
    }
  }

  const matches = Array.from(grouped.values())
    .filter((item) => (terms.length ? item.matchScore > 0 : true))
    .map((item) => ({
      ...item,
      evidence: item.evidence
        .toSorted((a, b) => {
          if (a.trustTier !== b.trustTier) return a.trustTier - b.trustTier;
          return (b.publishedAt ?? "").localeCompare(a.publishedAt ?? "");
        })
        .slice(0, Math.max(1, Math.round(params.maxEvidencePerClaim ?? 3))),
    }))
    .toSorted((a, b) => {
      if (b.matchScore !== a.matchScore) return b.matchScore - a.matchScore;
      if (b.confidence !== a.confidence) return b.confidence - a.confidence;
      return (b.validFrom ?? "").localeCompare(a.validFrom ?? "");
    })
    .slice(0, Math.max(1, Math.round(params.maxClaims ?? 3)));

  return {
    ticker,
    topic,
    matches,
  };
};
