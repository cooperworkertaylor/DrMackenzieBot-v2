import { openResearchDb } from "./db.js";

export type ResearchSourceRecord = {
  id: number;
  sourceKey: string;
  sourceType: string;
  provider: string;
  sender: string;
  baseUrl: string;
  trustTier: number;
  active: boolean;
  metadata: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

const parseJsonObject = (value: string): Record<string, unknown> => {
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed as Record<string, unknown>;
  } catch {
    return {};
  }
};

export const upsertResearchSource = (params: {
  sourceKey: string;
  sourceType: string;
  provider: string;
  sender?: string;
  baseUrl?: string;
  trustTier: number;
  metadata?: Record<string, unknown>;
  dbPath?: string;
}): ResearchSourceRecord => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const row = db
    .prepare(
      `INSERT INTO research_sources (
         source_key, source_type, provider, sender, base_url, trust_tier, active, metadata, created_at, updated_at
       )
       VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
       ON CONFLICT(source_key) DO UPDATE SET
         source_type=excluded.source_type,
         provider=excluded.provider,
         sender=excluded.sender,
         base_url=excluded.base_url,
         trust_tier=excluded.trust_tier,
         metadata=excluded.metadata,
         updated_at=excluded.updated_at
       RETURNING id, source_key, source_type, provider, sender, base_url, trust_tier, active, metadata, created_at, updated_at`,
    )
    .get(
      params.sourceKey,
      params.sourceType,
      params.provider,
      (params.sender ?? "").trim().toLowerCase(),
      (params.baseUrl ?? "").trim(),
      params.trustTier,
      JSON.stringify(params.metadata ?? {}),
      now,
      now,
    ) as {
    id: number;
    source_key: string;
    source_type: string;
    provider: string;
    sender: string;
    base_url: string;
    trust_tier: number;
    active: number;
    metadata: string;
    created_at: number;
    updated_at: number;
  };

  return {
    id: row.id,
    sourceKey: row.source_key,
    sourceType: row.source_type,
    provider: row.provider,
    sender: row.sender,
    baseUrl: row.base_url,
    trustTier: row.trust_tier,
    active: Boolean(row.active),
    metadata: parseJsonObject(row.metadata),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};
