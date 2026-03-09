import { openResearchDb } from "./db.js";
import {
  ingestExternalResearchDocument,
  type IngestExternalResearchResult,
} from "./external-research.js";
import { getLatestExternalResearchStructuredReport } from "./external-research-report.js";
import { getLatestExternalResearchThesis } from "./external-research-thesis.js";

export type ResearchUserPreference = {
  id: number;
  key: string;
  valueText: string;
  valueJson: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
};

export type ResearchNotebookEntry = {
  id: number;
  ticker?: string;
  title: string;
  content: string;
  tags: string[];
  source: string;
  externalDocumentId?: number;
  createdAt: number;
  updatedAt: number;
};

export type IngestResearchNotebookResult = {
  entry: ResearchNotebookEntry;
  ingest: IngestExternalResearchResult;
};

export type PersonalizedResearchSnapshot = {
  ticker: string;
  generatedAt: string;
  preferences: ResearchUserPreference[];
  notebookEntries: ResearchNotebookEntry[];
  summary: string;
  thesisSummary: string;
  nextActions: string[];
  markdown: string;
};

const normalizeTicker = (value?: string): string => value?.trim().toUpperCase() ?? "";

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

const parseJsonArray = (value?: string): string[] => {
  if (!value?.trim()) return [];
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed.map((item) => (typeof item === "string" ? item.trim() : "")).filter(Boolean);
  } catch {
    return [];
  }
};

const renderPersonalizedResearchSnapshotMarkdown = (
  snapshot: PersonalizedResearchSnapshot,
): string => {
  const lines: string[] = [];
  lines.push(`# ${snapshot.ticker} Personalized Snapshot`);
  lines.push("");
  lines.push(`- Generated at: ${snapshot.generatedAt}`);
  lines.push(`- Preferences: ${snapshot.preferences.length}`);
  lines.push(`- Notebook entries: ${snapshot.notebookEntries.length}`);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(snapshot.summary);
  lines.push("");
  lines.push("## Thesis");
  lines.push("");
  lines.push(snapshot.thesisSummary);
  lines.push("");
  lines.push("## Preferences");
  lines.push("");
  if (snapshot.preferences.length) {
    snapshot.preferences.forEach((preference) => {
      lines.push(`- ${preference.key}: ${preference.valueText || JSON.stringify(preference.valueJson)}`);
    });
  } else {
    lines.push("- No stored preferences.");
  }
  lines.push("");
  lines.push("## Notebook");
  lines.push("");
  if (snapshot.notebookEntries.length) {
    snapshot.notebookEntries.forEach((entry) => {
      lines.push(
        `- ${new Date(entry.createdAt).toISOString().slice(0, 10)} | ${entry.title}${entry.tags.length ? ` | tags=${entry.tags.join(",")}` : ""}`,
      );
    });
  } else {
    lines.push("- No notebook entries for this ticker.");
  }
  lines.push("");
  lines.push("## Next Actions");
  lines.push("");
  snapshot.nextActions.forEach((action) => lines.push(`- ${action}`));
  lines.push("");
  return lines.join("\n");
};

export const upsertResearchUserPreference = (params: {
  key: string;
  valueText?: string;
  valueJson?: Record<string, unknown>;
  dbPath?: string;
}): ResearchUserPreference => {
  const db = openResearchDb(params.dbPath);
  const key = params.key.trim().toLowerCase();
  if (!key) throw new Error("preference key is required");
  const valueText = params.valueText?.trim() ?? "";
  const valueJson = params.valueJson ?? {};
  const now = Date.now();
  db.prepare(
    `INSERT INTO research_user_preferences (
       preference_key, value_text, value_json, created_at, updated_at
     ) VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(preference_key) DO UPDATE SET
       value_text=excluded.value_text,
       value_json=excluded.value_json,
       updated_at=excluded.updated_at`,
  ).run(key, valueText, JSON.stringify(valueJson), now, now);
  const row = db
    .prepare(
      `SELECT id, preference_key, value_text, value_json, created_at, updated_at
       FROM research_user_preferences
       WHERE preference_key=?`,
    )
    .get(key) as {
    id: number;
    preference_key: string;
    value_text: string;
    value_json?: string;
    created_at: number;
    updated_at: number;
  };
  return {
    id: row.id,
    key: row.preference_key,
    valueText: row.value_text,
    valueJson: parseJsonObject(row.value_json),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};

export const listResearchUserPreferences = (params: { dbPath?: string } = {}): ResearchUserPreference[] => {
  const db = openResearchDb(params.dbPath);
  const rows = db
    .prepare(
      `SELECT id, preference_key, value_text, value_json, created_at, updated_at
       FROM research_user_preferences
       ORDER BY preference_key ASC`,
    )
    .all() as Array<{
    id: number;
    preference_key: string;
    value_text: string;
    value_json?: string;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map((row) => ({
    id: row.id,
    key: row.preference_key,
    valueText: row.value_text,
    valueJson: parseJsonObject(row.value_json),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }));
};

export const listResearchNotebookEntries = (params: {
  ticker?: string;
  limit?: number;
  dbPath?: string;
} = {}): ResearchNotebookEntry[] => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
  const rows = (
    ticker
      ? db
          .prepare(
            `SELECT id, ticker, title, content, tags, source, external_document_id, created_at, updated_at
             FROM research_notebook_entries
             WHERE ticker=?
             ORDER BY created_at DESC, id DESC
             LIMIT ?`,
          )
          .all(ticker, Math.max(1, Math.round(params.limit ?? 20)))
      : db
          .prepare(
            `SELECT id, ticker, title, content, tags, source, external_document_id, created_at, updated_at
             FROM research_notebook_entries
             ORDER BY created_at DESC, id DESC
             LIMIT ?`,
          )
          .all(Math.max(1, Math.round(params.limit ?? 20)))) as Array<{
    id: number;
    ticker: string;
    title: string;
    content: string;
    tags?: string;
    source: string;
    external_document_id?: number;
    created_at: number;
    updated_at: number;
  }>;
  return rows.map((row) => ({
    id: row.id,
    ticker: row.ticker || undefined,
    title: row.title,
    content: row.content,
    tags: parseJsonArray(row.tags),
    source: row.source,
    externalDocumentId:
      typeof row.external_document_id === "number" ? row.external_document_id : undefined,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }));
};

export const ingestResearchNotebookEntry = (params: {
  title: string;
  content: string;
  ticker?: string;
  tags?: string[];
  source?: string;
  dbPath?: string;
}): IngestResearchNotebookResult => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const ticker = normalizeTicker(params.ticker);
  const tags = Array.from(new Set([...(params.tags ?? []), "notebook"])).map((tag) => tag.trim()).filter(Boolean);
  const source = params.source?.trim() || "manual";
  const row = db
    .prepare(
      `INSERT INTO research_notebook_entries (
         ticker, title, content, tags, source, created_at, updated_at
       ) VALUES (?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(ticker, params.title.trim(), params.content.trim(), JSON.stringify(tags), source, now, now) as {
    id: number;
  };

  const ingest = ingestExternalResearchDocument({
    sourceType: "manual",
    provider: "other",
    sender: "research-notebook",
    title: params.title,
    subject: params.title,
    content: params.content,
    ticker,
    tags,
    metadata: {
      source: "research_notebook",
      notebookEntryId: row.id,
      notebookSource: source,
    },
    dbPath: params.dbPath,
  });
  db.prepare(
    `UPDATE research_notebook_entries
     SET external_document_id=?, updated_at=?
     WHERE id=?`,
  ).run(ingest.id, Date.now(), row.id);

  return {
    entry: {
      id: row.id,
      ticker: ticker || undefined,
      title: params.title.trim(),
      content: params.content.trim(),
      tags,
      source,
      externalDocumentId: ingest.id,
      createdAt: now,
      updatedAt: now,
    },
    ingest,
  };
};

export const buildPersonalizedResearchSnapshot = (params: {
  ticker: string;
  dbPath?: string;
}): PersonalizedResearchSnapshot => {
  const ticker = normalizeTicker(params.ticker);
  const report = getLatestExternalResearchStructuredReport({
    ticker,
    dbPath: params.dbPath,
  });
  const thesis = getLatestExternalResearchThesis({
    ticker,
    dbPath: params.dbPath,
  });
  if (!report || !thesis) {
    throw new Error(`missing personalized research context for ticker=${ticker}`);
  }
  const preferences = listResearchUserPreferences({ dbPath: params.dbPath });
  const notebookEntries = listResearchNotebookEntries({
    ticker,
    limit: 5,
    dbPath: params.dbPath,
  });
  const preferenceLens = preferences
    .map((preference) => `${preference.key}=${preference.valueText || JSON.stringify(preference.valueJson)}`)
    .slice(0, 3);
  const summaryParts = [
    `${ticker} is currently ${thesis.stance} with ${(report.confidence * 100).toFixed(0)}% report confidence.`,
    report.whatChanged[0] ? `Latest change: ${report.whatChanged[0]}` : "",
    preferenceLens.length ? `Preference lens: ${preferenceLens.join(" | ")}` : "",
    notebookEntries[0] ? `Latest notebook entry: ${notebookEntries[0].title}` : "",
  ].filter(Boolean);
  const nextActions = [
    ...report.nextActions.slice(0, 2),
    ...(thesis.openQuestions.length
      ? [`Close the highest-priority open question: ${thesis.openQuestions[0]}`]
      : []),
    ...(notebookEntries.length === 0
      ? [`Add one notebook entry for ${ticker} to compound your own research edge.`]
      : []),
  ].slice(0, 4);
  const snapshot: PersonalizedResearchSnapshot = {
    ticker,
    generatedAt: new Date().toISOString(),
    preferences,
    notebookEntries,
    summary: summaryParts.join(" "),
    thesisSummary: thesis.summary,
    nextActions,
    markdown: "",
  };
  snapshot.markdown = renderPersonalizedResearchSnapshotMarkdown(snapshot);
  return snapshot;
};
