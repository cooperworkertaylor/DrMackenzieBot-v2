import { openResearchDb } from "./db.js";
import type { ExternalResearchStructuredReport } from "./external-research-report.js";

export type ExternalResearchThesis = {
  id: number;
  entityId: number;
  ticker: string;
  thesisType: "external_structured";
  versionNumber: number;
  stance: "bullish" | "balanced" | "cautious";
  summary: string;
  confidence: number;
  bullCase: string[];
  bearCase: string[];
  openQuestions: string[];
  supportingEvidence: string[];
  reportId?: number;
  createdAt: number;
  updatedAt: number;
};

export type ExternalResearchThesisDraft = Omit<
  ExternalResearchThesis,
  "id" | "versionNumber" | "createdAt" | "updatedAt"
> & {
  versionNumber?: number;
};

export type ExternalResearchThesisDiff = {
  id: number;
  entityId: number;
  ticker: string;
  thesisType: "external_structured";
  previousThesisId?: number;
  currentThesisId: number;
  reportId?: number;
  thesisBreak: boolean;
  confidenceDelta: number;
  summary: string;
  newBullCase: string[];
  newBearCase: string[];
  newOpenQuestions: string[];
  resolvedOpenQuestions: string[];
  thesisBreakReasons: string[];
  createdAt: number;
};

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

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

const normalizedKey = (value: string): string =>
  value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const diffStrings = (current: string[], previous: string[]): string[] => {
  const previousKeys = new Set(previous.map(normalizedKey));
  return current.filter((value) => !previousKeys.has(normalizedKey(value)));
};

const overlapScore = (left: string, right: string): number => {
  const leftTokens = new Set(
    normalizedKey(left)
      .split(" ")
      .filter((token) => token.length >= 5),
  );
  const rightTokens = new Set(
    normalizedKey(right)
      .split(" ")
      .filter((token) => token.length >= 5),
  );
  if (leftTokens.size === 0 || rightTokens.size === 0) return 0;
  let overlap = 0;
  for (const token of leftTokens) {
    if (rightTokens.has(token)) overlap += 1;
  }
  return overlap / Math.max(1, Math.min(leftTokens.size, rightTokens.size));
};

const stanceFromReport = (
  report: ExternalResearchStructuredReport,
): ExternalResearchThesisDraft["stance"] => {
  const bullStrength = report.bullCase.length + report.confidence;
  const bearStrength = report.bearCase.length + report.unknowns.length * 0.5;
  if (bullStrength - bearStrength >= 1) return "bullish";
  if (bearStrength - bullStrength >= 1) return "cautious";
  return "balanced";
};

export const buildExternalResearchThesisFromReport = (params: {
  report: ExternalResearchStructuredReport;
  reportId?: number;
}): ExternalResearchThesisDraft => ({
  entityId: params.report.entityId,
  ticker: normalizeTicker(params.report.ticker),
  thesisType: "external_structured",
  stance: stanceFromReport(params.report),
  summary: params.report.summary,
  confidence: clamp01(params.report.confidence),
  bullCase: [...params.report.bullCase],
  bearCase: [...params.report.bearCase],
  openQuestions: [...params.report.unknowns],
  supportingEvidence: [...params.report.evidence],
  reportId: params.reportId,
});

export const getLatestExternalResearchThesis = (params: {
  ticker: string;
  dbPath?: string;
}): ExternalResearchThesis | null => {
  const db = openResearchDb(params.dbPath);
  const row = db
    .prepare(
      `SELECT
         id, entity_id, ticker, thesis_type, version_number, stance, summary, confidence,
         bull_case, bear_case, open_questions, supporting_evidence, report_id, created_at, updated_at
       FROM research_theses
       WHERE ticker=? AND thesis_type='external_structured'
       ORDER BY version_number DESC, id DESC
       LIMIT 1`,
    )
    .get(normalizeTicker(params.ticker)) as
    | {
        id: number;
        entity_id: number;
        ticker: string;
        thesis_type: "external_structured";
        version_number: number;
        stance: "bullish" | "balanced" | "cautious";
        summary: string;
        confidence: number;
        bull_case?: string;
        bear_case?: string;
        open_questions?: string;
        supporting_evidence?: string;
        report_id?: number;
        created_at: number;
        updated_at: number;
      }
    | undefined;
  if (!row) return null;
  return {
    id: row.id,
    entityId: row.entity_id,
    ticker: row.ticker,
    thesisType: row.thesis_type,
    versionNumber: row.version_number,
    stance: row.stance,
    summary: row.summary,
    confidence: clamp01(row.confidence),
    bullCase: parseJsonArray(row.bull_case),
    bearCase: parseJsonArray(row.bear_case),
    openQuestions: parseJsonArray(row.open_questions),
    supportingEvidence: parseJsonArray(row.supporting_evidence),
    reportId: typeof row.report_id === "number" ? row.report_id : undefined,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
};

export const storeExternalResearchThesis = (params: {
  thesis: ExternalResearchThesisDraft;
  dbPath?: string;
}): ExternalResearchThesis => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const latest = getLatestExternalResearchThesis({
    ticker: params.thesis.ticker,
    dbPath: params.dbPath,
  });
  const versionNumber = Math.max(1, params.thesis.versionNumber ?? (latest?.versionNumber ?? 0) + 1);
  const row = db
    .prepare(
      `INSERT INTO research_theses (
         entity_id, ticker, thesis_type, version_number, stance, summary, confidence,
         bull_case, bear_case, open_questions, supporting_evidence, report_id, created_at, updated_at
       ) VALUES (?, ?, 'external_structured', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      params.thesis.entityId,
      normalizeTicker(params.thesis.ticker),
      versionNumber,
      params.thesis.stance,
      params.thesis.summary,
      clamp01(params.thesis.confidence),
      JSON.stringify(params.thesis.bullCase),
      JSON.stringify(params.thesis.bearCase),
      JSON.stringify(params.thesis.openQuestions),
      JSON.stringify(params.thesis.supportingEvidence),
      typeof params.thesis.reportId === "number" ? params.thesis.reportId : null,
      now,
      now,
    ) as { id: number };
  return {
    id: row.id,
    versionNumber,
    createdAt: now,
    updatedAt: now,
    ...params.thesis,
    ticker: normalizeTicker(params.thesis.ticker),
  };
};

export const diffExternalResearchTheses = (params: {
  previous: ExternalResearchThesis | null;
  current: ExternalResearchThesis;
}): Omit<ExternalResearchThesisDiff, "id" | "createdAt" | "entityId" | "ticker" | "thesisType" | "currentThesisId"> => {
  const previous = params.previous;
  const current = params.current;
  const newBullCase = diffStrings(current.bullCase, previous?.bullCase ?? []);
  const newBearCase = diffStrings(current.bearCase, previous?.bearCase ?? []);
  const newOpenQuestions = diffStrings(current.openQuestions, previous?.openQuestions ?? []);
  const resolvedOpenQuestions = diffStrings(previous?.openQuestions ?? [], current.openQuestions);
  const confidenceDelta = current.confidence - (previous?.confidence ?? current.confidence);
  const thesisBreakReasons: string[] = [];
  const explicitBreakText = newBearCase.some((item) =>
    /\b(contradict|contradiction|no longer hold|breaks?|weakening)\b/i.test(item),
  );

  if (previous) {
    if (confidenceDelta <= -0.2) {
      thesisBreakReasons.push(
        `confidence dropped ${(Math.abs(confidenceDelta) * 100).toFixed(0)} points from the prior thesis`,
      );
    }
    if (previous.stance === "bullish" && current.stance === "cautious") {
      thesisBreakReasons.push("stance flipped from bullish to cautious");
    }
    if (newBearCase.length >= 2 && current.bearCase.length >= current.bullCase.length) {
      thesisBreakReasons.push("new disconfirming evidence now outweighs bullish support");
    }
    const contradictingBull = previous.bullCase.filter((priorBull) =>
      current.bearCase.some((bear) => overlapScore(priorBull, bear) >= 0.35),
    );
    if (contradictingBull.length > 0) {
      thesisBreakReasons.push(
        `prior bullish points are now contradicted by current risks (${contradictingBull.slice(0, 2).join("; ")})`,
      );
    }
    if (newBearCase.length >= 1 && contradictingBull.length >= 1) {
      thesisBreakReasons.push("new risk language directly conflicts with the prior core thesis");
    }
    if (explicitBreakText && previous.bullCase.length > 0) {
      thesisBreakReasons.push("current report includes explicit contradiction language against the prior thesis");
    }
  }

  const thesisBreak = thesisBreakReasons.length > 0 || (previous !== null && explicitBreakText);
  const summaryParts = [
    newBullCase.length > 0 ? `${newBullCase.length} new bullish points` : "",
    newBearCase.length > 0 ? `${newBearCase.length} new risks` : "",
    newOpenQuestions.length > 0 ? `${newOpenQuestions.length} new open questions` : "",
    resolvedOpenQuestions.length > 0 ? `${resolvedOpenQuestions.length} resolved questions` : "",
    previous ? `confidence delta ${(confidenceDelta * 100).toFixed(0)} pts` : "initial thesis version",
  ].filter(Boolean);

  return {
    previousThesisId: previous?.id,
    reportId: current.reportId,
    thesisBreak,
    confidenceDelta,
    summary: summaryParts.join(" | "),
    newBullCase,
    newBearCase,
    newOpenQuestions,
    resolvedOpenQuestions,
    thesisBreakReasons,
  };
};

export const storeExternalResearchThesisDiff = (params: {
  current: ExternalResearchThesis;
  previous: ExternalResearchThesis | null;
  dbPath?: string;
}): ExternalResearchThesisDiff => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const diff = diffExternalResearchTheses({
    previous: params.previous,
    current: params.current,
  });
  const row = db
    .prepare(
      `INSERT INTO research_thesis_diffs (
         entity_id, ticker, thesis_type, previous_thesis_id, current_thesis_id, report_id,
         thesis_break, confidence_delta, summary, delta_json, created_at
       ) VALUES (?, ?, 'external_structured', ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      params.current.entityId,
      params.current.ticker,
      typeof diff.previousThesisId === "number" ? diff.previousThesisId : null,
      params.current.id,
      typeof diff.reportId === "number" ? diff.reportId : null,
      diff.thesisBreak ? 1 : 0,
      diff.confidenceDelta,
      diff.summary,
      JSON.stringify({
        newBullCase: diff.newBullCase,
        newBearCase: diff.newBearCase,
        newOpenQuestions: diff.newOpenQuestions,
        resolvedOpenQuestions: diff.resolvedOpenQuestions,
        thesisBreakReasons: diff.thesisBreakReasons,
      }),
      now,
    ) as { id: number };
  return {
    id: row.id,
    entityId: params.current.entityId,
    ticker: params.current.ticker,
    thesisType: "external_structured",
    currentThesisId: params.current.id,
    createdAt: now,
    ...diff,
  };
};

export const persistExternalResearchThesisBreakAlert = (params: {
  thesis: ExternalResearchThesis;
  diff: ExternalResearchThesisDiff;
  dbPath?: string;
}): number | null => {
  if (!params.diff.thesisBreak) return null;
  const db = openResearchDb(params.dbPath);
  const existing = db
    .prepare(
      `SELECT id
       FROM thesis_alerts
       WHERE ticker=?
         AND alert_type='external-thesis-break'
         AND resolved=0
         AND details=?
       ORDER BY id DESC
       LIMIT 1`,
    )
    .get(
      params.thesis.ticker,
      JSON.stringify({
        thesisId: params.thesis.id,
        reasons: params.diff.thesisBreakReasons,
      }),
    ) as { id?: number } | undefined;
  if (typeof existing?.id === "number") return existing.id;

  const severity =
    params.diff.confidenceDelta <= -0.25 || params.diff.thesisBreakReasons.length >= 2
      ? "high"
      : "medium";
  const details = JSON.stringify({
    thesisId: params.thesis.id,
    reportId: params.thesis.reportId,
    reasons: params.diff.thesisBreakReasons,
    newBearCase: params.diff.newBearCase,
    newOpenQuestions: params.diff.newOpenQuestions,
  });
  const row = db
    .prepare(
      `INSERT INTO thesis_alerts (
         instrument_id, ticker, severity, alert_type, message, details, created_at, resolved
       ) VALUES (
         (SELECT id FROM instruments WHERE ticker=?),
         ?, ?, 'external-thesis-break', ?, ?, ?, 0
       )
       RETURNING id`,
    )
    .get(
      params.thesis.ticker,
      params.thesis.ticker,
      severity,
      `External research thesis break detected for ${params.thesis.ticker}.`,
      details,
      Date.now(),
    ) as { id: number };
  return row.id;
};
