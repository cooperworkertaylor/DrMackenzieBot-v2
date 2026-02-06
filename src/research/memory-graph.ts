import { createHash } from "node:crypto";
import { openResearchDb } from "./db.js";

export type ResearchEntity = {
  id: number;
  kind: string;
  canonicalName: string;
  ticker: string;
  metadata: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type ResearchClaim = {
  id: number;
  entityId: number;
  claimText: string;
  claimType: string;
  confidence: number;
  validFrom: string;
  validTo: string;
  status: string;
  sourceTaskOutcomeId?: number;
  metadata: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type ResearchClaimEvidence = {
  id: number;
  claimId: number;
  sourceTable: string;
  refId: number;
  citationUrl?: string;
  excerptHash: string;
  metadata: Record<string, unknown>;
  createdAt: number;
};

const clamp = (value: number, min: number, max: number): number =>
  Math.max(min, Math.min(max, value));

const parseJsonObject = <T extends object>(value: unknown, fallback: T): T => {
  if (typeof value !== "string" || !value.trim()) return fallback;
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return fallback;
    return parsed as T;
  } catch {
    return fallback;
  }
};

const hashExcerpt = (value: string): string =>
  createHash("sha256").update(value.trim()).digest("hex").slice(0, 32);

export const upsertResearchEntity = (params: {
  kind?: string;
  canonicalName: string;
  ticker?: string;
  metadata?: Record<string, unknown>;
  dbPath?: string;
}): ResearchEntity => {
  const now = Date.now();
  const kind = (params.kind ?? "company").trim().toLowerCase() || "company";
  const canonicalName = params.canonicalName.trim();
  if (!canonicalName) throw new Error("canonicalName is required");
  const ticker = (params.ticker ?? "").trim().toUpperCase();
  const db = openResearchDb(params.dbPath);
  db.prepare(
    `INSERT INTO research_entities (
       kind, canonical_name, ticker, metadata, created_at, updated_at
     ) VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(kind, canonical_name, ticker) DO UPDATE SET
       metadata=excluded.metadata,
       updated_at=excluded.updated_at`,
  ).run(kind, canonicalName, ticker, JSON.stringify(params.metadata ?? {}), now, now);

  const row = db
    .prepare(
      `SELECT id, kind, canonical_name, ticker, metadata, created_at, updated_at
       FROM research_entities
       WHERE kind=? AND canonical_name=? AND ticker=?`,
    )
    .get(kind, canonicalName, ticker) as {
    id: number;
    kind: string;
    canonical_name: string;
    ticker: string;
    metadata?: string;
    created_at: number;
    updated_at: number;
  };
  return {
    id: row.id,
    kind: row.kind,
    canonicalName: row.canonical_name,
    ticker: row.ticker,
    metadata: parseJsonObject<Record<string, unknown>>(row.metadata, {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};

export const createResearchClaim = (params: {
  entityId: number;
  claimText: string;
  claimType?: string;
  confidence?: number;
  validFrom?: string;
  validTo?: string;
  status?: string;
  sourceTaskOutcomeId?: number;
  metadata?: Record<string, unknown>;
  dbPath?: string;
}): ResearchClaim => {
  const now = Date.now();
  const claimText = params.claimText.trim();
  if (!claimText) throw new Error("claimText is required");
  const db = openResearchDb(params.dbPath);
  const row = db
    .prepare(
      `INSERT INTO research_claims (
         entity_id, claim_text, claim_type, confidence, valid_from, valid_to, status,
         source_task_outcome_id, metadata, created_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      Math.max(1, Math.round(params.entityId)),
      claimText,
      (params.claimType ?? "thesis").trim().toLowerCase() || "thesis",
      clamp(params.confidence ?? 0.5, 0, 1),
      (params.validFrom ?? "").trim(),
      (params.validTo ?? "").trim(),
      (params.status ?? "active").trim().toLowerCase() || "active",
      typeof params.sourceTaskOutcomeId === "number"
        ? Math.round(params.sourceTaskOutcomeId)
        : null,
      JSON.stringify(params.metadata ?? {}),
      now,
      now,
    ) as { id: number };

  return getResearchClaim({
    id: row.id,
    dbPath: params.dbPath,
  });
};

export const getResearchClaim = (params: { id: number; dbPath?: string }): ResearchClaim => {
  const db = openResearchDb(params.dbPath);
  const row = db
    .prepare(
      `SELECT
         id, entity_id, claim_text, claim_type, confidence, valid_from, valid_to, status,
         source_task_outcome_id, metadata, created_at, updated_at
       FROM research_claims
       WHERE id=?`,
    )
    .get(Math.round(params.id)) as
    | {
        id: number;
        entity_id: number;
        claim_text: string;
        claim_type: string;
        confidence: number;
        valid_from: string;
        valid_to: string;
        status: string;
        source_task_outcome_id?: number;
        metadata?: string;
        created_at: number;
        updated_at: number;
      }
    | undefined;
  if (!row) throw new Error(`Research claim not found: id=${params.id}`);
  return {
    id: row.id,
    entityId: row.entity_id,
    claimText: row.claim_text,
    claimType: row.claim_type,
    confidence: clamp(row.confidence, 0, 1),
    validFrom: row.valid_from,
    validTo: row.valid_to,
    status: row.status,
    sourceTaskOutcomeId: row.source_task_outcome_id,
    metadata: parseJsonObject<Record<string, unknown>>(row.metadata, {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};

export const addClaimEvidence = (params: {
  claimId: number;
  sourceTable: string;
  refId?: number;
  citationUrl?: string;
  excerptText?: string;
  metadata?: Record<string, unknown>;
  dbPath?: string;
}): ResearchClaimEvidence => {
  const now = Date.now();
  const sourceTable = params.sourceTable.trim();
  if (!sourceTable) throw new Error("sourceTable is required");
  const excerptHash = hashExcerpt(params.excerptText ?? "");
  const db = openResearchDb(params.dbPath);
  const row = db
    .prepare(
      `INSERT INTO research_claim_evidence (
         claim_id, source_table, ref_id, citation_url, excerpt_hash, metadata, created_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      Math.max(1, Math.round(params.claimId)),
      sourceTable,
      Math.max(0, Math.round(params.refId ?? 0)),
      (params.citationUrl ?? "").trim(),
      excerptHash,
      JSON.stringify(params.metadata ?? {}),
      now,
    ) as { id: number };
  return {
    id: row.id,
    claimId: Math.max(1, Math.round(params.claimId)),
    sourceTable,
    refId: Math.max(0, Math.round(params.refId ?? 0)),
    citationUrl: (params.citationUrl ?? "").trim() || undefined,
    excerptHash,
    metadata: params.metadata ?? {},
    createdAt: now,
  };
};

export const updateClaimStatus = (params: {
  claimId: number;
  status: string;
  reason?: string;
  confidence?: number;
  metadata?: Record<string, unknown>;
  dbPath?: string;
}): ResearchClaim => {
  const now = Date.now();
  const status = params.status.trim().toLowerCase();
  if (!status) throw new Error("status is required");
  const db = openResearchDb(params.dbPath);
  db.exec("BEGIN");
  try {
    db.prepare(
      `UPDATE research_claims
       SET status=?, confidence=COALESCE(?, confidence), updated_at=?
       WHERE id=?`,
    ).run(
      status,
      typeof params.confidence === "number" ? clamp(params.confidence, 0, 1) : null,
      now,
      Math.max(1, Math.round(params.claimId)),
    );
    db.prepare(
      `INSERT INTO research_claim_status_history (
         claim_id, status, reason, confidence, changed_at, metadata
       ) VALUES (?, ?, ?, ?, ?, ?)`,
    ).run(
      Math.max(1, Math.round(params.claimId)),
      status,
      (params.reason ?? "").trim(),
      typeof params.confidence === "number" ? clamp(params.confidence, 0, 1) : null,
      now,
      JSON.stringify(params.metadata ?? {}),
    );
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return getResearchClaim({
    id: params.claimId,
    dbPath: params.dbPath,
  });
};

export const listEntityClaims = (params: {
  ticker?: string;
  entityName?: string;
  status?: string;
  limit?: number;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
  const ticker = (params.ticker ?? "").trim().toUpperCase();
  const entityName = (params.entityName ?? "").trim();
  const status = (params.status ?? "").trim().toLowerCase();
  const limit = Math.max(1, Math.round(params.limit ?? 100));
  const rows = db
    .prepare(
      `SELECT
         c.id, c.entity_id, c.claim_text, c.claim_type, c.confidence, c.valid_from, c.valid_to,
         c.status, c.source_task_outcome_id, c.metadata, c.created_at, c.updated_at,
         e.kind AS entity_kind, e.canonical_name AS entity_name, e.ticker AS entity_ticker,
         (SELECT COUNT(1) FROM research_claim_evidence ev WHERE ev.claim_id = c.id) AS evidence_count
       FROM research_claims c
       JOIN research_entities e ON e.id = c.entity_id
       WHERE (? = '' OR e.ticker = ?)
         AND (? = '' OR e.canonical_name = ?)
         AND (? = '' OR c.status = ?)
       ORDER BY c.updated_at DESC
       LIMIT ?`,
    )
    .all(ticker, ticker, entityName, entityName, status, status, limit) as Array<{
    id: number;
    entity_id: number;
    claim_text: string;
    claim_type: string;
    confidence: number;
    valid_from: string;
    valid_to: string;
    status: string;
    source_task_outcome_id?: number;
    metadata?: string;
    created_at: number;
    updated_at: number;
    entity_kind: string;
    entity_name: string;
    entity_ticker: string;
    evidence_count: number;
  }>;
  return rows.map((row) => ({
    claim: {
      id: row.id,
      entityId: row.entity_id,
      claimText: row.claim_text,
      claimType: row.claim_type,
      confidence: clamp(row.confidence, 0, 1),
      validFrom: row.valid_from,
      validTo: row.valid_to,
      status: row.status,
      sourceTaskOutcomeId: row.source_task_outcome_id,
      metadata: parseJsonObject<Record<string, unknown>>(row.metadata, {}),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    } as ResearchClaim,
    entity: {
      kind: row.entity_kind,
      canonicalName: row.entity_name,
      ticker: row.entity_ticker,
    },
    evidenceCount: Math.max(0, Math.round(row.evidence_count)),
  }));
};
