import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { openResearchDb } from "./db.js";
import {
  addTickerToResearchWatchlist,
  upsertResearchWatchlist,
} from "./external-research-watchlists.js";
import {
  createResearchBackup,
  getResearchHealthSnapshot,
  replayFailedQuickrunJobs,
  runResearchSchedulerPass,
} from "./ops.js";
import { QuickrunJobStore } from "./quickrun/job-store.js";

const tempDirs: string[] = [];

const makeTempDir = (prefix: string) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
};

const makeDbPath = (prefix: string) => path.join(makeTempDir(prefix), "research.db");

afterEach(() => {
  vi.unstubAllEnvs();
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  }
});

describe("research ops", () => {
  it("requeues failed quickrun jobs and reports them in health", () => {
    const dbPath = makeDbPath("research-ops-health-");
    const store = QuickrunJobStore.open(dbPath);
    store.enqueue({
      id: "job-1",
      jobType: "quick_research_pdf_v2",
      payload: { ok: true },
      runAfterMs: 0,
      maxAttempts: 1,
    });
    store.claimNext({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
      nowMs: 10,
    });
    store.markFailed({
      id: "job-1",
      workerId: "worker-a",
      error: "boom",
      nowMs: 20,
    });

    const before = getResearchHealthSnapshot({ dbPath });
    expect(before.quickrun.failed).toBe(1);
    expect(before.warnings.some((warning) => warning.includes("failed"))).toBe(true);

    const replayed = replayFailedQuickrunJobs({ dbPath });
    expect(replayed).toHaveLength(1);
    expect(replayed[0]?.status).toBe("queued");

    const after = getResearchHealthSnapshot({ dbPath });
    expect(after.quickrun.failed).toBe(0);
    expect(after.quickrun.queued).toBe(1);
  });

  it("creates a filesystem backup with db and research artifact directories", () => {
    const dbPath = makeDbPath("research-ops-backup-");
    const stateRoot = makeTempDir("research-state-");
    vi.stubEnv("OPENCLAW_STATE_DIR", stateRoot);
    openResearchDb(dbPath);

    const rawDir = path.join(stateRoot, "research", "raw-artifacts", "newsletter");
    fs.mkdirSync(rawDir, { recursive: true });
    fs.writeFileSync(path.join(rawDir, "doc.json"), '{"ok":true}\n', "utf8");

    const backupDest = makeTempDir("research-backups-");
    const backup = createResearchBackup({
      dbPath,
      destDir: backupDest,
    });

    expect(fs.existsSync(backup.dbPath)).toBe(true);
    expect(fs.existsSync(backup.manifestPath)).toBe(true);
    expect(fs.existsSync(path.join(backup.backupDir, "raw-artifacts", "newsletter", "doc.json"))).toBe(
      true,
    );
  });

  it("processes queued refreshes and generates default watchlist briefs", () => {
    const dbPath = makeDbPath("research-ops-scheduler-");
    const watchlist = upsertResearchWatchlist({
      name: "Core",
      description: "core names",
      isDefault: true,
      dbPath,
    });
    addTickerToResearchWatchlist({
      watchlistId: watchlist.id,
      ticker: "NVDA",
      priority: 1,
      dbPath,
    });

    const db = openResearchDb(dbPath);
    const now = Date.now();
    db.prepare(
      `INSERT INTO research_reports (
         entity_id, ticker, report_type, title, summary, markdown, report_json,
         confidence, source_count, lookback_days, generated_at, created_at, updated_at
       ) VALUES (?, ?, 'external_structured', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).run(
      1,
      "NVDA",
      "NVDA External Research Memo",
      "summary",
      "# Memo",
      JSON.stringify({
        entityId: 1,
        ticker: "NVDA",
        title: "NVDA External Research Memo",
        generatedAt: new Date(now).toISOString(),
        lookbackDays: 45,
        summary: "summary",
        whatChanged: ["new evidence"],
        evidence: [],
        bullCase: [],
        bearCase: [],
        unknowns: [],
        nextActions: [],
        sources: [{ documentId: 99, title: "source", url: "https://example.com" }],
        confidence: 0.7,
        confidenceRationale: [],
        evidenceCoverage: {
          sourceCount: 1,
          providerCount: 1,
          avgTrustScore: 0.8,
          avgMaterialityScore: 0.8,
          claimCount: 0,
          eventCount: 0,
          factCount: 0,
        },
        markdown: "# Memo",
      }),
      0.7,
      1,
      45,
      now,
      now,
      now,
    );
    db.prepare(
      `INSERT INTO research_refresh_queue (
         watchlist_id, ticker, source_document_id, priority, reason, status, created_at, updated_at
       ) VALUES (?, ?, ?, 'high', ?, 'queued', ?, ?)`,
    ).run(watchlist.id, "NVDA", 99, "new doc", now, now);

    const result = runResearchSchedulerPass({ dbPath, briefDate: "2026-03-09" });
    expect(result.generatedBriefs).toBe(1);
    expect(result.processedRefreshes).toBe(1);

    const refresh = db.prepare(
      `SELECT status FROM research_refresh_queue WHERE watchlist_id=? AND ticker='NVDA'`,
    ).get(watchlist.id) as { status: string };
    expect(refresh.status).toBe("completed");

    const brief = db.prepare(
      `SELECT COUNT(*) AS count FROM research_briefs WHERE watchlist_id=? AND brief_date='2026-03-09'`,
    ).get(watchlist.id) as { count: number };
    expect(brief.count).toBe(1);
  });
});
