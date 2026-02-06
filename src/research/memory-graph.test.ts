import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  addClaimEvidence,
  createResearchClaim,
  listEntityClaims,
  updateClaimStatus,
  upsertResearchEntity,
} from "./memory-graph.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-memory-graph-${name}-${Date.now()}-${Math.random()}.db`);

describe("research memory graph", () => {
  it("creates entities, claims, evidence, and status transitions", () => {
    const dbPath = testDbPath("flow");
    const entity = upsertResearchEntity({
      kind: "company",
      canonicalName: "Apple Inc",
      ticker: "AAPL",
      metadata: { sector: "technology" },
      dbPath,
    });
    const claim = createResearchClaim({
      entityId: entity.id,
      claimText: "Gross margin is resilient under base-case demand assumptions.",
      claimType: "fundamental",
      confidence: 0.76,
      validFrom: "2026-01-01",
      dbPath,
    });
    addClaimEvidence({
      claimId: claim.id,
      sourceTable: "filings",
      refId: 101,
      citationUrl: "https://www.sec.gov/ixviewer/ix.html",
      excerptText: "Gross margin remains strong...",
      dbPath,
    });
    addClaimEvidence({
      claimId: claim.id,
      sourceTable: "transcripts",
      refId: 55,
      citationUrl: "https://example.com/transcript",
      excerptText: "Management discussed margin durability.",
      dbPath,
    });

    const updated = updateClaimStatus({
      claimId: claim.id,
      status: "contested",
      reason: "New evidence indicates higher input-cost pressure.",
      confidence: 0.58,
      dbPath,
    });
    expect(updated.status).toBe("contested");
    expect(updated.confidence).toBeCloseTo(0.58, 5);

    const listed = listEntityClaims({
      ticker: "AAPL",
      dbPath,
    });
    expect(listed).toHaveLength(1);
    expect(listed[0]?.claim.id).toBe(claim.id);
    expect(listed[0]?.evidenceCount).toBe(2);
    expect(listed[0]?.claim.status).toBe("contested");
  });
});
