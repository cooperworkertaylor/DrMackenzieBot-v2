import fs from "node:fs";
import path from "node:path";
import { loadConfig } from "../config/config.js";
import { resolveStateDir } from "../config/paths.js";
import { defaultRuntime, type RuntimeEnv } from "../runtime.js";
import { resolveResearchDbPath, openResearchDb } from "./db.js";
import { getLatestExternalResearchStructuredReport } from "./external-research-report.js";
import {
  buildDailyWatchlistBrief,
  listResearchWatchlists,
  storeDailyWatchlistBrief,
  type ResearchRefreshQueueItem,
} from "./external-research-watchlists.js";
import {
  QuickrunJobStore,
  type QuickrunJobRecord,
  type QuickrunJobStatus,
} from "./quickrun/job-store.js";
import {
  stopQuickResearchWorker,
  startQuickResearchWorker,
} from "./quickrun/quick-research-jobs.js";

const DEFAULT_WORKER_POLL_MS = 60_000;
const DEFAULT_SCHEDULER_INTERVAL_MS = 300_000;

type TimestampSummary = {
  iso: string | null;
  ageHours: number | null;
};

export type ResearchHealthSnapshot = {
  checkedAt: string;
  dbPath: string;
  stateDir: string;
  quickrun: {
    queued: number;
    running: number;
    failed: number;
    completed: number;
    staleRunning: number;
    oldestQueuedAgeHours: number | null;
  };
  refreshQueue: {
    queued: number;
    completed: number;
    skipped: number;
    highPriorityQueued: number;
    oldestQueuedAgeHours: number | null;
  };
  externalResearch: {
    latestDocument: TimestampSummary;
    latestReport: TimestampSummary;
    latestBrief: TimestampSummary;
    unresolvedThesisAlerts: number;
  };
  backups: {
    latestBackupPath: string | null;
    latestBackupAt: string | null;
  };
  warnings: string[];
};

export type ResearchBackupResult = {
  backupDir: string;
  dbPath: string;
  manifestPath: string;
  copiedPaths: string[];
};

export type ResearchRestoreResult = {
  backupDir: string;
  restoredDbPath: string;
  restoredStateDir: string;
  restoredPaths: string[];
};

export type ResearchSchedulerPassResult = {
  processedRefreshes: number;
  generatedBriefs: number;
  queuedRefreshesRemaining: number;
};

const toIso = (value?: number | null): string | null => {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return null;
  return new Date(value).toISOString();
};

const toAgeHours = (value?: number | null, nowMs = Date.now()): number | null => {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return null;
  return Number(((nowMs - value) / 36e5).toFixed(2));
};

const summarizeTimestamp = (value?: number | null, nowMs = Date.now()): TimestampSummary => ({
  iso: toIso(value),
  ageHours: toAgeHours(value, nowMs),
});

const wait = async (ms: number) => {
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
};

const resolveResearchStateDir = (): string => {
  const stateDir = resolveStateDir(process.env);
  return path.join(stateDir, "research");
};

export const resolveResearchBackupDir = (): string =>
  path.join(resolveResearchStateDir(), "backups");

const listBackups = (dir: string): Array<{ path: string; mtimeMs: number }> => {
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name.startsWith("research-backup-"))
    .map((entry) => {
      const fullPath = path.join(dir, entry.name);
      const stat = fs.statSync(fullPath);
      return { path: fullPath, mtimeMs: stat.mtimeMs };
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs);
};

const copyIfExists = (srcPath: string, destPath: string, copiedPaths: string[]) => {
  if (!fs.existsSync(srcPath)) return;
  fs.mkdirSync(path.dirname(destPath), { recursive: true });
  fs.cpSync(srcPath, destPath, { recursive: true });
  copiedPaths.push(destPath);
};

const countQuickrunStatus = (store: QuickrunJobStore, status: QuickrunJobStatus): number =>
  store.list({ status, limit: 10_000 }).length;

const oldestQuickrunQueuedAgeHours = (store: QuickrunJobStore): number | null => {
  const jobs = store.list({ status: "queued", limit: 10_000 });
  if (!jobs.length) return null;
  const oldest = jobs.reduce((min, job) => Math.min(min, job.createdAtMs), jobs[0]!.createdAtMs);
  return toAgeHours(oldest);
};

const staleRunningCount = (store: QuickrunJobStore, staleAfterMs = 90_000): number => {
  const now = Date.now();
  return store
    .list({ status: "running", limit: 10_000 })
    .filter((job) => now - (job.heartbeatAtMs ?? job.lockedAtMs ?? job.updatedAtMs) > staleAfterMs)
    .length;
};

const selectRefreshQueueRows = (dbPath?: string) => {
  const db = openResearchDb(dbPath);
  return db
    .prepare(
      `SELECT id, watchlist_id, ticker, source_document_id, priority, reason, status, created_at, updated_at
       FROM research_refresh_queue
       ORDER BY
         CASE priority
           WHEN 'high' THEN 0
           WHEN 'medium' THEN 1
           ELSE 2
         END,
         created_at ASC`,
    )
    .all() as Array<{
    id: number;
    watchlist_id: number;
    ticker: string;
    source_document_id: number;
    priority: ResearchRefreshQueueItem["priority"];
    reason: string;
    status: ResearchRefreshQueueItem["status"];
    created_at: number;
    updated_at: number;
  }>;
};

export const getResearchHealthSnapshot = (params?: {
  dbPath?: string;
  backupDir?: string;
}): ResearchHealthSnapshot => {
  const dbPath = resolveResearchDbPath(params?.dbPath);
  const db = openResearchDb(dbPath);
  const store = QuickrunJobStore.open(dbPath);
  const now = Date.now();
  const refreshRows = selectRefreshQueueRows(dbPath);
  const backupDir = path.resolve(params?.backupDir ?? resolveResearchBackupDir());
  const latestBackup = listBackups(backupDir)[0];

  const latestDocument = db
    .prepare(`SELECT MAX(fetched_at) AS ts FROM external_documents`)
    .get() as {
    ts?: number;
  };
  const latestReport = db.prepare(`SELECT MAX(generated_at) AS ts FROM research_reports`).get() as {
    ts?: number;
  };
  const latestBrief = db.prepare(`SELECT MAX(updated_at) AS ts FROM research_briefs`).get() as {
    ts?: number;
  };
  const unresolvedThesisAlerts = db
    .prepare(`SELECT COUNT(*) AS count FROM thesis_alerts WHERE resolved=0`)
    .get() as { count: number };

  const queuedRefreshes = refreshRows.filter((row) => row.status === "queued");
  const oldestQueuedRefresh = queuedRefreshes.reduce<number | null>(
    (min, row) => (min == null ? row.created_at : Math.min(min, row.created_at)),
    null,
  );

  const snapshot: ResearchHealthSnapshot = {
    checkedAt: new Date(now).toISOString(),
    dbPath,
    stateDir: resolveResearchStateDir(),
    quickrun: {
      queued: countQuickrunStatus(store, "queued"),
      running: countQuickrunStatus(store, "running"),
      failed: countQuickrunStatus(store, "failed"),
      completed: countQuickrunStatus(store, "completed"),
      staleRunning: staleRunningCount(store),
      oldestQueuedAgeHours: oldestQuickrunQueuedAgeHours(store),
    },
    refreshQueue: {
      queued: queuedRefreshes.length,
      completed: refreshRows.filter((row) => row.status === "completed").length,
      skipped: refreshRows.filter((row) => row.status === "skipped").length,
      highPriorityQueued: queuedRefreshes.filter((row) => row.priority === "high").length,
      oldestQueuedAgeHours: toAgeHours(oldestQueuedRefresh, now),
    },
    externalResearch: {
      latestDocument: summarizeTimestamp(latestDocument.ts, now),
      latestReport: summarizeTimestamp(latestReport.ts, now),
      latestBrief: summarizeTimestamp(latestBrief.ts, now),
      unresolvedThesisAlerts: Number(unresolvedThesisAlerts.count ?? 0),
    },
    backups: {
      latestBackupPath: latestBackup?.path ?? null,
      latestBackupAt: latestBackup ? new Date(latestBackup.mtimeMs).toISOString() : null,
    },
    warnings: [],
  };

  if ((snapshot.quickrun.failed ?? 0) > 0) {
    snapshot.warnings.push(`${snapshot.quickrun.failed} quickrun jobs are failed and need replay.`);
  }
  if ((snapshot.quickrun.staleRunning ?? 0) > 0) {
    snapshot.warnings.push(`${snapshot.quickrun.staleRunning} quickrun jobs look stale.`);
  }
  if ((snapshot.refreshQueue.highPriorityQueued ?? 0) > 0) {
    snapshot.warnings.push(
      `${snapshot.refreshQueue.highPriorityQueued} high-priority refreshes are still queued.`,
    );
  }
  if ((snapshot.externalResearch.latestBrief.ageHours ?? Infinity) > 36) {
    snapshot.warnings.push("Daily watchlist brief is stale or missing.");
  }
  if ((snapshot.externalResearch.latestDocument.ageHours ?? Infinity) > 72) {
    snapshot.warnings.push("External research ingestion is stale.");
  }
  if (!snapshot.backups.latestBackupAt) {
    snapshot.warnings.push("No research backup has been created yet.");
  }

  return snapshot;
};

export const createResearchBackup = (params: {
  dbPath?: string;
  destDir: string;
  stateDir?: string;
}): ResearchBackupResult => {
  const dbPath = resolveResearchDbPath(params.dbPath);
  const stateDir = path.resolve(params.stateDir ?? resolveResearchStateDir());
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const backupDir = path.resolve(params.destDir, `research-backup-${ts}`);
  const copiedPaths: string[] = [];
  fs.mkdirSync(backupDir, { recursive: true });

  const dbOut = path.join(backupDir, "research.db");
  fs.copyFileSync(dbPath, dbOut);
  copiedPaths.push(dbOut);

  copyIfExists(
    path.join(stateDir, "raw-artifacts"),
    path.join(backupDir, "raw-artifacts"),
    copiedPaths,
  );
  copyIfExists(path.join(stateDir, "quickrun"), path.join(backupDir, "quickrun"), copiedPaths);

  const manifestPath = path.join(backupDir, "manifest.json");
  const manifest = {
    createdAt: new Date().toISOString(),
    dbPath,
    stateDir,
    copiedPaths,
  };
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");

  return {
    backupDir,
    dbPath: dbOut,
    manifestPath,
    copiedPaths,
  };
};

export const restoreResearchBackup = (params: {
  backupDir: string;
  dbPath?: string;
  stateDir?: string;
  force?: boolean;
}): ResearchRestoreResult => {
  const backupDir = path.resolve(params.backupDir);
  if (!fs.existsSync(backupDir)) {
    throw new Error(`Backup directory not found: ${backupDir}`);
  }
  const sourceDbPath = path.join(backupDir, "research.db");
  if (!fs.existsSync(sourceDbPath)) {
    throw new Error(`Backup database missing: ${sourceDbPath}`);
  }
  const restoredDbPath = resolveResearchDbPath(params.dbPath);
  const restoredStateDir = path.resolve(params.stateDir ?? resolveResearchStateDir());
  const restoredPaths: string[] = [];

  if (!params.force && fs.existsSync(restoredDbPath)) {
    throw new Error(`Destination DB already exists: ${restoredDbPath} (use --force to overwrite)`);
  }
  fs.mkdirSync(path.dirname(restoredDbPath), { recursive: true });
  fs.copyFileSync(sourceDbPath, restoredDbPath);
  restoredPaths.push(restoredDbPath);

  const rawArtifactsSrc = path.join(backupDir, "raw-artifacts");
  if (fs.existsSync(rawArtifactsSrc)) {
    const rawArtifactsDest = path.join(restoredStateDir, "raw-artifacts");
    if (fs.existsSync(rawArtifactsDest) && !params.force) {
      throw new Error(
        `Destination raw-artifacts already exists: ${rawArtifactsDest} (use --force to overwrite)`,
      );
    }
    fs.mkdirSync(restoredStateDir, { recursive: true });
    fs.cpSync(rawArtifactsSrc, rawArtifactsDest, {
      recursive: true,
      force: Boolean(params.force),
    });
    restoredPaths.push(rawArtifactsDest);
  }

  const quickrunSrc = path.join(backupDir, "quickrun");
  if (fs.existsSync(quickrunSrc)) {
    const quickrunDest = path.join(restoredStateDir, "quickrun");
    if (fs.existsSync(quickrunDest) && !params.force) {
      throw new Error(
        `Destination quickrun already exists: ${quickrunDest} (use --force to overwrite)`,
      );
    }
    fs.mkdirSync(restoredStateDir, { recursive: true });
    fs.cpSync(quickrunSrc, quickrunDest, {
      recursive: true,
      force: Boolean(params.force),
    });
    restoredPaths.push(quickrunDest);
  }

  return {
    backupDir,
    restoredDbPath,
    restoredStateDir,
    restoredPaths,
  };
};

export const replayFailedQuickrunJobs = (params: {
  dbPath?: string;
  limit?: number;
}): QuickrunJobRecord[] => {
  const store = QuickrunJobStore.open(params.dbPath);
  const failed = store.list({
    status: "failed",
    limit: Math.max(1, Math.floor(params.limit ?? 25)),
  });
  return failed
    .map((job) => store.requeueFailed({ id: job.id }))
    .filter((job): job is NonNullable<typeof job> => Boolean(job));
};

export const runResearchSchedulerPass = (params?: {
  dbPath?: string;
  briefDate?: string;
  lookbackDays?: number;
  limit?: number;
}): ResearchSchedulerPassResult => {
  const db = openResearchDb(params?.dbPath);
  const watchlists = listResearchWatchlists({ dbPath: params?.dbPath });
  let generatedBriefs = 0;

  for (const watchlist of watchlists.filter((item) => item.isDefault)) {
    const briefDate = params?.briefDate ?? new Date().toISOString().slice(0, 10);
    const existing = db
      .prepare(
        `SELECT id FROM research_briefs WHERE watchlist_id=? AND brief_type='daily_watchlist' AND brief_date=?`,
      )
      .get(watchlist.id, briefDate) as { id: number } | undefined;
    if (!existing) {
      const brief = buildDailyWatchlistBrief({
        watchlistId: watchlist.id,
        briefDate,
        lookbackDays: params?.lookbackDays,
        dbPath: params?.dbPath,
      });
      storeDailyWatchlistBrief({ brief, dbPath: params?.dbPath });
      generatedBriefs += 1;
    }
  }

  const limit = Math.max(1, Math.floor(params?.limit ?? 50));
  const queuedRows = selectRefreshQueueRows(params?.dbPath)
    .filter((row) => row.status === "queued")
    .slice(0, limit);
  let processedRefreshes = 0;
  const now = Date.now();
  for (const row of queuedRows) {
    const report = getLatestExternalResearchStructuredReport({
      ticker: row.ticker,
      dbPath: params?.dbPath,
    });
    const matched = report?.sources.some((source) => source.documentId === row.source_document_id);
    if (!matched) continue;
    db.prepare(
      `UPDATE research_refresh_queue
       SET status='completed', updated_at=?
       WHERE id=?`,
    ).run(now, row.id);
    processedRefreshes += 1;
  }

  const remaining = db
    .prepare(`SELECT COUNT(*) AS count FROM research_refresh_queue WHERE status='queued'`)
    .get() as { count: number };

  return {
    processedRefreshes,
    generatedBriefs,
    queuedRefreshesRemaining: Number(remaining.count ?? 0),
  };
};

const registerShutdownHandlers = (onStop: () => Promise<void> | void) => {
  let stopping = false;
  const handler = () => {
    if (stopping) return;
    stopping = true;
    void Promise.resolve(onStop()).finally(() => {
      process.exit(0);
    });
  };
  process.once("SIGTERM", handler);
  process.once("SIGINT", handler);
};

export const runResearchWorkerLoop = async (params?: { dbPath?: string; runtime?: RuntimeEnv }) => {
  const cfg = loadConfig();
  startQuickResearchWorker({ cfg, dbPath: params?.dbPath });
  registerShutdownHandlers(() => {
    stopQuickResearchWorker();
  });
  params?.runtime?.log?.("research worker started");
  // eslint-disable-next-line no-constant-condition
  while (true) {
    await wait(DEFAULT_WORKER_POLL_MS);
  }
};

export const runResearchSchedulerLoop = async (params?: {
  dbPath?: string;
  intervalMs?: number;
  runtime?: RuntimeEnv;
}) => {
  let stopped = false;
  registerShutdownHandlers(() => {
    stopped = true;
  });
  const runtime = params?.runtime ?? defaultRuntime;
  const intervalMs = Math.max(
    10_000,
    Math.floor(params?.intervalMs ?? DEFAULT_SCHEDULER_INTERVAL_MS),
  );
  runtime.log("research scheduler started");
  while (!stopped) {
    const result = runResearchSchedulerPass({ dbPath: params?.dbPath });
    runtime.log(
      `research scheduler tick processed_refreshes=${result.processedRefreshes} generated_briefs=${result.generatedBriefs} queued_remaining=${result.queuedRefreshesRemaining}`,
    );
    await wait(intervalMs);
  }
};
