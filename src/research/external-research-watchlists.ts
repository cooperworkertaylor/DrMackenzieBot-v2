import type { CronJobCreate } from "../cron/types.js";
import { openResearchDb } from "./db.js";
import { getLatestExternalResearchStructuredReport } from "./external-research-report.js";

export type ResearchWatchlist = {
  id: number;
  name: string;
  description: string;
  isDefault: boolean;
  createdAt: number;
  updatedAt: number;
};

export type ResearchWatchlistMembership = {
  id: number;
  watchlistId: number;
  ticker: string;
  priority: number;
  tags: string[];
  createdAt: number;
  updatedAt: number;
};

export type ResearchRefreshQueueItem = {
  id: number;
  watchlistId: number;
  ticker: string;
  sourceDocumentId: number;
  priority: "high" | "medium" | "low";
  reason: string;
  status: "queued" | "completed" | "skipped";
  createdAt: number;
  updatedAt: number;
};

export type DailyWatchlistBrief = {
  watchlistId: number;
  briefDate: string;
  generatedAt: string;
  title: string;
  summary: string;
  materialChanges: string[];
  thesisBreaks: string[];
  pendingRefreshes: string[];
  nextActions: string[];
  markdown: string;
};

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

const parseJsonArray = (value: unknown): string[] => {
  if (typeof value !== "string" || !value.trim()) return [];
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed.map((item) => (typeof item === "string" ? item.trim() : "")).filter(Boolean);
  } catch {
    return [];
  }
};

const toBriefDate = (value = new Date()): string => value.toISOString().slice(0, 10);

const priorityFromMateriality = (materialityScore: number): ResearchRefreshQueueItem["priority"] => {
  if (materialityScore >= 0.85) return "high";
  if (materialityScore >= 0.65) return "medium";
  return "low";
};

const priorityRank = (priority: string): number => {
  if (priority === "high") return 0;
  if (priority === "medium") return 1;
  return 2;
};

export const upsertResearchWatchlist = (params: {
  name: string;
  description?: string;
  isDefault?: boolean;
  dbPath?: string;
}): ResearchWatchlist => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const name = params.name.trim();
  if (!name) throw new Error("watchlist name is required");
  db.prepare(
    `INSERT INTO research_watchlists (name, description, is_default, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(name) DO UPDATE SET
       description=excluded.description,
       is_default=excluded.is_default,
       updated_at=excluded.updated_at`,
  ).run(name, (params.description ?? "").trim(), params.isDefault ? 1 : 0, now, now);
  const row = db
    .prepare(
      `SELECT id, name, description, is_default, created_at, updated_at
       FROM research_watchlists
       WHERE name=?`,
    )
    .get(name) as {
    id: number;
    name: string;
    description: string;
    is_default: number;
    created_at: number;
    updated_at: number;
  };
  return {
    id: row.id,
    name: row.name,
    description: row.description,
    isDefault: row.is_default === 1,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};

export const addTickerToResearchWatchlist = (params: {
  watchlistId: number;
  ticker: string;
  priority?: number;
  tags?: string[];
  dbPath?: string;
}): ResearchWatchlistMembership => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const ticker = normalizeTicker(params.ticker);
  db.prepare(
    `INSERT INTO research_watchlist_memberships (
       watchlist_id, ticker, priority, tags, created_at, updated_at
     ) VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(watchlist_id, ticker) DO UPDATE SET
       priority=excluded.priority,
       tags=excluded.tags,
       updated_at=excluded.updated_at`,
  ).run(
    params.watchlistId,
    ticker,
    Math.max(1, Math.min(5, Math.round(params.priority ?? 3))),
    JSON.stringify(params.tags ?? []),
    now,
    now,
  );
  const row = db
    .prepare(
      `SELECT id, watchlist_id, ticker, priority, tags, created_at, updated_at
       FROM research_watchlist_memberships
       WHERE watchlist_id=? AND ticker=?`,
    )
    .get(params.watchlistId, ticker) as {
    id: number;
    watchlist_id: number;
    ticker: string;
    priority: number;
    tags?: string;
    created_at: number;
    updated_at: number;
  };
  return {
    id: row.id,
    watchlistId: row.watchlist_id,
    ticker: row.ticker,
    priority: row.priority,
    tags: parseJsonArray(row.tags),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};

export const listResearchWatchlistMembers = (params: {
  watchlistId: number;
  dbPath?: string;
}): ResearchWatchlistMembership[] => {
  const db = openResearchDb(params.dbPath);
  const rows = db
    .prepare(
      `SELECT id, watchlist_id, ticker, priority, tags, created_at, updated_at
       FROM research_watchlist_memberships
       WHERE watchlist_id=?
       ORDER BY priority ASC, ticker ASC`,
    )
    .all(params.watchlistId) as Array<{
    id: number;
    watchlist_id: number;
    ticker: string;
    priority: number;
    tags?: string;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map((row) => ({
    id: row.id,
    watchlistId: row.watchlist_id,
    ticker: row.ticker,
    priority: row.priority,
    tags: parseJsonArray(row.tags),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }));
};

export const enqueueWatchlistRefresh = (params: {
  ticker: string;
  sourceDocumentId: number;
  materialityScore: number;
  reason: string;
  dbPath?: string;
}): ResearchRefreshQueueItem | null => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
  if (!ticker || params.materialityScore < 0.55) return null;
  const watchlist = db
    .prepare(
      `SELECT m.watchlist_id
       FROM research_watchlist_memberships m
       WHERE m.ticker=?
       ORDER BY m.priority ASC, m.id ASC
       LIMIT 1`,
    )
    .get(ticker) as { watchlist_id?: number } | undefined;
  if (typeof watchlist?.watchlist_id !== "number") return null;
  const now = Date.now();
  const priority = priorityFromMateriality(params.materialityScore);
  db.prepare(
    `INSERT INTO research_refresh_queue (
       watchlist_id, ticker, source_document_id, priority, reason, status, created_at, updated_at
     ) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)
     ON CONFLICT(watchlist_id, ticker, source_document_id) DO UPDATE SET
       priority=excluded.priority,
       reason=excluded.reason,
       status='queued',
       updated_at=excluded.updated_at`,
  ).run(watchlist.watchlist_id, ticker, params.sourceDocumentId, priority, params.reason, now, now);
  const row = db
    .prepare(
      `SELECT id, watchlist_id, ticker, source_document_id, priority, reason, status, created_at, updated_at
       FROM research_refresh_queue
       WHERE watchlist_id=? AND ticker=? AND source_document_id=?`,
    )
    .get(watchlist.watchlist_id, ticker, params.sourceDocumentId) as {
    id: number;
    watchlist_id: number;
    ticker: string;
    source_document_id: number;
    priority: "high" | "medium" | "low";
    reason: string;
    status: "queued" | "completed" | "skipped";
    created_at: number;
    updated_at: number;
  };
  return {
    id: row.id,
    watchlistId: row.watchlist_id,
    ticker: row.ticker,
    sourceDocumentId: row.source_document_id,
    priority: row.priority,
    reason: row.reason,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};

export const buildDailyWatchlistBrief = (params: {
  watchlistId: number;
  briefDate?: string;
  lookbackDays?: number;
  dbPath?: string;
}): DailyWatchlistBrief => {
  const db = openResearchDb(params.dbPath);
  const lookbackDays = Math.max(1, Math.round(params.lookbackDays ?? 1));
  const cutoffMs = Date.now() - lookbackDays * 86_400_000;
  const watchlist = db
    .prepare(`SELECT id, name FROM research_watchlists WHERE id=?`)
    .get(params.watchlistId) as { id: number; name: string } | undefined;
  if (!watchlist) {
    throw new Error(`watchlist not found: id=${params.watchlistId}`);
  }
  const members = listResearchWatchlistMembers({
    watchlistId: params.watchlistId,
    dbPath: params.dbPath,
  });

  const materialChanges: string[] = [];
  const thesisBreaks: string[] = [];
  const pendingRefreshes: string[] = [];
  const nextActions = new Set<string>();

  for (const member of members) {
    const report = getLatestExternalResearchStructuredReport({
      ticker: member.ticker,
      dbPath: params.dbPath,
    });
    if (report?.whatChanged.length) {
      materialChanges.push(`${member.ticker}: ${report.whatChanged[0]}`);
    }

    const alerts = db
      .prepare(
        `SELECT severity, message
         FROM thesis_alerts
         WHERE ticker=?
           AND resolved=0
           AND created_at >= ?
         ORDER BY
           CASE severity
             WHEN 'high' THEN 0
             WHEN 'medium' THEN 1
             ELSE 2
           END,
           created_at DESC`,
      )
      .all(member.ticker, cutoffMs) as Array<{ severity: string; message: string }>;
    if (alerts.length > 0) {
      thesisBreaks.push(`${member.ticker}: [${alerts[0]!.severity}] ${alerts[0]!.message}`);
      nextActions.add(`Review ${member.ticker} thesis alerts before the next refresh cycle.`);
    }

    const refreshes = db
      .prepare(
        `SELECT priority, reason
         FROM research_refresh_queue
         WHERE watchlist_id=?
           AND ticker=?
           AND status='queued'
         ORDER BY
           CASE priority
             WHEN 'high' THEN 0
             WHEN 'medium' THEN 1
             ELSE 2
           END,
           created_at DESC`,
      )
      .all(params.watchlistId, member.ticker) as Array<{ priority: string; reason: string }>;
    if (refreshes.length > 0) {
      pendingRefreshes.push(`${member.ticker}: [${refreshes[0]!.priority}] ${refreshes[0]!.reason}`);
      nextActions.add(`Run a watchlist refresh for ${member.ticker}.`);
    }
  }

  const summary = [
    `${watchlist.name} has ${members.length} tracked names.`,
    `${materialChanges.length} names show fresh material changes in the current window.`,
    thesisBreaks.length > 0
      ? `${thesisBreaks.length} names have unresolved thesis-break alerts.`
      : "No unresolved thesis-break alerts in the current window.",
  ].join(" ");

  const title = `${watchlist.name} Daily Brief`;
  const brief: DailyWatchlistBrief = {
    watchlistId: watchlist.id,
    briefDate: params.briefDate ?? toBriefDate(),
    generatedAt: new Date().toISOString(),
    title,
    summary,
    materialChanges: materialChanges.slice(0, 12),
    thesisBreaks: thesisBreaks.slice(0, 12),
    pendingRefreshes: pendingRefreshes
      .toSorted((a, b) => priorityRank(a.split("[")[1]?.split("]")[0] ?? "low") - priorityRank(b.split("[")[1]?.split("]")[0] ?? "low"))
      .slice(0, 12),
    nextActions: Array.from(nextActions).slice(0, 8),
    markdown: "",
  };

  const lines = [
    `# ${brief.title}`,
    "",
    `- Brief date: ${brief.briefDate}`,
    `- Generated at: ${brief.generatedAt}`,
    "",
    "## Summary",
    "",
    brief.summary,
    "",
    "## Material Changes",
    "",
    ...(brief.materialChanges.length ? brief.materialChanges.map((line) => `- ${line}`) : ["- None."]),
    "",
    "## Thesis Breaks",
    "",
    ...(brief.thesisBreaks.length ? brief.thesisBreaks.map((line) => `- ${line}`) : ["- None."]),
    "",
    "## Pending Refreshes",
    "",
    ...(brief.pendingRefreshes.length ? brief.pendingRefreshes.map((line) => `- ${line}`) : ["- None."]),
    "",
    "## Next Actions",
    "",
    ...(brief.nextActions.length ? brief.nextActions.map((line) => `- ${line}`) : ["- None."]),
    "",
  ];
  brief.markdown = lines.join("\n");
  return brief;
};

export const storeDailyWatchlistBrief = (params: {
  brief: DailyWatchlistBrief;
  dbPath?: string;
}): number => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const row = db
    .prepare(
      `INSERT INTO research_briefs (
         watchlist_id, brief_type, brief_date, title, markdown, brief_json, created_at, updated_at
       ) VALUES (?, 'daily_watchlist', ?, ?, ?, ?, ?, ?)
       ON CONFLICT(watchlist_id, brief_type, brief_date) DO UPDATE SET
         title=excluded.title,
         markdown=excluded.markdown,
         brief_json=excluded.brief_json,
         updated_at=excluded.updated_at
       RETURNING id`,
    )
    .get(
      params.brief.watchlistId,
      params.brief.briefDate,
      params.brief.title,
      params.brief.markdown,
      JSON.stringify(params.brief),
      now,
      now,
    ) as { id: number };
  return row.id;
};

export const buildDailyWatchlistBriefCronJob = (params: {
  watchlistId: number;
  watchlistName: string;
  hourEt?: number;
  minuteEt?: number;
  agentId?: string;
}): CronJobCreate => {
  const hourEt = Math.max(0, Math.min(23, Math.round(params.hourEt ?? 7)));
  const minuteEt = Math.max(0, Math.min(59, Math.round(params.minuteEt ?? 30)));
  return {
    agentId: params.agentId,
    name: `Daily brief: ${params.watchlistName}`,
    description: `Generate the daily research brief for watchlist ${params.watchlistName}.`,
    enabled: true,
    schedule: { kind: "cron", expr: `${minuteEt} ${hourEt} * * *`, tz: "America/New_York" },
    sessionTarget: "isolated",
    wakeMode: "now",
    payload: {
      kind: "agentTurn",
      message: `Generate the daily watchlist brief for watchlist_id=${params.watchlistId}. Summarize material changes, thesis breaks, pending refreshes, and next actions.`,
      deliver: false,
      bestEffortDeliver: true,
    },
  };
};
