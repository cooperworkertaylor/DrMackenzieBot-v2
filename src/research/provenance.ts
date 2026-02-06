import { createHash, createHmac } from "node:crypto";
import { openResearchDb } from "./db.js";

type JsonLike = null | boolean | number | string | JsonLike[] | { [key: string]: JsonLike };

export type ProvenanceEvent = {
  id: number;
  eventType: string;
  entityType: string;
  entityId: string;
  payloadHash: string;
  prevHash: string;
  eventHash: string;
  signature: string;
  keyId: string;
  metadata: Record<string, unknown>;
  createdAt: number;
};

export type AppendProvenanceEventParams = {
  eventType: string;
  entityType?: string;
  entityId?: string | number;
  payload: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  keyId?: string;
  dbPath?: string;
};

export type ProvenanceReport = {
  generatedAt: string;
  totalEvents: number;
  chainValid: boolean;
  signatureCoverage: number;
  signatureValidRate?: number;
  firstEventAt?: number;
  lastEventAt?: number;
  issues: string[];
  events: ProvenanceEvent[];
};

const toCanonicalJson = (value: JsonLike): string => {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => toCanonicalJson(item)).join(",")}]`;
  }
  const entries = Object.entries(value)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([key, child]) => `${JSON.stringify(key)}:${toCanonicalJson(child)}`);
  return `{${entries.join(",")}}`;
};

const sha256 = (value: string): string => createHash("sha256").update(value).digest("hex");

const signPayload = (params: { key?: string; payload: string }): string => {
  if (!params.key?.trim()) return "";
  return createHmac("sha256", params.key).update(params.payload).digest("hex");
};

const normalizeObject = (value: unknown): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
};

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

const parseRow = (row: {
  id: number;
  event_type: string;
  entity_type: string;
  entity_id: string;
  payload_hash: string;
  prev_hash: string;
  event_hash: string;
  signature: string;
  key_id: string;
  metadata?: string;
  created_at: number;
}): ProvenanceEvent => ({
  id: row.id,
  eventType: row.event_type,
  entityType: row.entity_type,
  entityId: row.entity_id,
  payloadHash: row.payload_hash,
  prevHash: row.prev_hash,
  eventHash: row.event_hash,
  signature: row.signature,
  keyId: row.key_id,
  metadata: parseJsonObject<Record<string, unknown>>(row.metadata, {}),
  createdAt: row.created_at,
});

export const appendProvenanceEvent = (params: AppendProvenanceEventParams): ProvenanceEvent => {
  const eventType = params.eventType.trim().toLowerCase();
  if (!eventType) throw new Error("eventType is required");
  const entityType = (params.entityType ?? "").trim().toLowerCase();
  const entityId = String(params.entityId ?? "").trim();
  const payload = normalizeObject(params.payload);
  const metadata = normalizeObject(params.metadata);
  const db = openResearchDb(params.dbPath);
  const now = Date.now();

  const previous = db
    .prepare(
      `SELECT event_hash
       FROM provenance_events
       ORDER BY id DESC
       LIMIT 1`,
    )
    .get() as { event_hash?: string } | undefined;
  const prevHash = previous?.event_hash?.trim() ?? "";
  const payloadHash = sha256(toCanonicalJson(payload as JsonLike));
  const eventMaterial = `${prevHash}|${eventType}|${entityType}|${entityId}|${payloadHash}|${now}`;
  const eventHash = sha256(eventMaterial);
  const signingKey = process.env.RESEARCH_PROVENANCE_SECRET?.trim();
  const keyId =
    (params.keyId ?? process.env.RESEARCH_PROVENANCE_KEY_ID ?? "default").trim() || "default";
  const signature = signPayload({
    key: signingKey,
    payload: `${eventHash}|${keyId}`,
  });

  const row = db
    .prepare(
      `INSERT INTO provenance_events (
         event_type, entity_type, entity_id, payload_hash, prev_hash, event_hash,
         signature, key_id, metadata, created_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      eventType,
      entityType,
      entityId,
      payloadHash,
      prevHash,
      eventHash,
      signature,
      keyId,
      JSON.stringify(metadata),
      now,
    ) as { id: number };

  return {
    id: row.id,
    eventType,
    entityType,
    entityId,
    payloadHash,
    prevHash,
    eventHash,
    signature,
    keyId,
    metadata,
    createdAt: now,
  };
};

export const provenanceReport = (
  params: {
    eventType?: string;
    entityType?: string;
    entityId?: string;
    limit?: number;
    dbPath?: string;
  } = {},
): ProvenanceReport => {
  const db = openResearchDb(params.dbPath);
  const eventType = (params.eventType ?? "").trim().toLowerCase();
  const entityType = (params.entityType ?? "").trim().toLowerCase();
  const entityId = (params.entityId ?? "").trim();
  const limit = Math.max(1, Math.round(params.limit ?? 200));
  const rows = db
    .prepare(
      `SELECT
         id, event_type, entity_type, entity_id, payload_hash, prev_hash, event_hash,
         signature, key_id, metadata, created_at
       FROM provenance_events
       WHERE (? = '' OR event_type = ?)
         AND (? = '' OR entity_type = ?)
         AND (? = '' OR entity_id = ?)
       ORDER BY id ASC
       LIMIT ?`,
    )
    .all(eventType, eventType, entityType, entityType, entityId, entityId, limit) as Array<{
    id: number;
    event_type: string;
    entity_type: string;
    entity_id: string;
    payload_hash: string;
    prev_hash: string;
    event_hash: string;
    signature: string;
    key_id: string;
    metadata?: string;
    created_at: number;
  }>;
  const events = rows.map(parseRow);
  const issues: string[] = [];

  for (let i = 0; i < events.length; i += 1) {
    const current = events[i];
    const prev = events[i - 1];
    const expectedPrev = prev?.eventHash ?? "";
    if (current.prevHash !== expectedPrev) {
      issues.push(
        `chain_break id=${current.id} prev_hash=${current.prevHash || "empty"} expected=${expectedPrev || "empty"}`,
      );
    }
  }

  const signingKey = process.env.RESEARCH_PROVENANCE_SECRET?.trim();
  const signed = events.filter((event) => Boolean(event.signature));
  let validSignatures = 0;
  if (signed.length && signingKey) {
    for (const event of signed) {
      const expected = signPayload({
        key: signingKey,
        payload: `${event.eventHash}|${event.keyId}`,
      });
      if (expected === event.signature) validSignatures += 1;
      else issues.push(`signature_mismatch id=${event.id}`);
    }
  }

  return {
    generatedAt: new Date().toISOString(),
    totalEvents: events.length,
    chainValid: !issues.some((issue) => issue.startsWith("chain_break")),
    signatureCoverage: events.length > 0 ? signed.length / events.length : 0,
    signatureValidRate:
      signed.length > 0 && signingKey ? validSignatures / signed.length : undefined,
    firstEventAt: events[0]?.createdAt,
    lastEventAt: events[events.length - 1]?.createdAt,
    issues,
    events,
  };
};
