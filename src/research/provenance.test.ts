import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import { appendProvenanceEvent, provenanceReport } from "./provenance.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-provenance-${name}-${Date.now()}-${Math.random()}.db`);

describe("research provenance", () => {
  const previousSecret = process.env.RESEARCH_PROVENANCE_SECRET;

  afterEach(() => {
    process.env.RESEARCH_PROVENANCE_SECRET = previousSecret;
  });

  it("builds a valid hash chain and verifies signatures", () => {
    const dbPath = testDbPath("chain");
    process.env.RESEARCH_PROVENANCE_SECRET = "test-secret";
    appendProvenanceEvent({
      eventType: "task_outcome",
      entityType: "task_outcomes",
      entityId: 1,
      payload: { id: 1, score: 0.8 },
      dbPath,
    });
    appendProvenanceEvent({
      eventType: "execution_trace",
      entityType: "execution_traces",
      entityId: 2,
      payload: { id: 2, success: 1 },
      dbPath,
    });

    const report = provenanceReport({ dbPath, limit: 20 });
    expect(report.totalEvents).toBe(2);
    expect(report.chainValid).toBe(true);
    expect(report.signatureCoverage).toBe(1);
    expect(report.signatureValidRate).toBe(1);
  });

  it("detects chain break conditions", () => {
    const dbPath = testDbPath("tamper");
    appendProvenanceEvent({
      eventType: "memo_deliverable",
      entityType: "research_memo",
      entityId: "AAPL:1",
      payload: { memo_hash: "abc" },
      dbPath,
    });
    appendProvenanceEvent({
      eventType: "memo_deliverable",
      entityType: "research_memo",
      entityId: "AAPL:2",
      payload: { memo_hash: "def" },
      dbPath,
    });
    const db = openResearchDb(dbPath);
    db.prepare(`UPDATE provenance_events SET prev_hash='tampered' WHERE id=2`).run();

    const report = provenanceReport({ dbPath, limit: 20 });
    expect(report.chainValid).toBe(false);
    expect(report.issues.some((issue) => issue.includes("chain_break"))).toBe(true);
  });
});
