import { openResearchDb } from "./db.js";
import {
  getLatestExternalResearchStructuredReport,
  type StoredExternalResearchReport,
} from "./external-research-report.js";
import {
  getLatestExternalResearchThesis,
  type ExternalResearchThesis,
} from "./external-research-thesis.js";

type ConflictSeverity = "low" | "medium" | "high";
type ClaimPolarity = "positive" | "negative" | "neutral";

export type ExternalResearchConflictEntry = {
  documentId: number;
  provider: string;
  title: string;
  url: string;
  publishedAt?: string;
  valueText: string;
};

export type ExternalResearchSourceConflict = {
  ticker: string;
  kind: "fact" | "claim";
  severity: ConflictSeverity;
  score: number;
  topic: string;
  asOfDate?: string;
  summary: string;
  entries: ExternalResearchConflictEntry[];
};

export type ExternalResearchSourceConflictReport = {
  ticker: string;
  generatedAt: string;
  lookbackDays: number;
  conflicts: ExternalResearchSourceConflict[];
  markdown: string;
};

export type ExternalResearchPeerSnapshot = {
  ticker: string;
  title: string;
  generatedAt: string;
  confidence: number;
  stance: ExternalResearchThesis["stance"];
  sourceCount: number;
  providerCount: number;
  freshSourceRatio: number;
  bullCount: number;
  bearCount: number;
  unknownCount: number;
  unresolvedThesisAlerts: number;
  pendingRefreshes: number;
  whatChanged: string[];
  evidence: string[];
  openQuestions: string[];
};

export type ExternalResearchPeerComparison = {
  generatedAt: string;
  left: ExternalResearchPeerSnapshot;
  right: ExternalResearchPeerSnapshot;
  summary: string;
  evidenceEdge: string;
  riskEdge: string;
  notableDeltas: string[];
  nextActions: string[];
  markdown: string;
};

const DAY_MS = 86_400_000;

const STOPWORDS = new Set([
  "about",
  "above",
  "after",
  "along",
  "around",
  "because",
  "below",
  "could",
  "demand",
  "guidance",
  "investors",
  "management",
  "margin",
  "pricing",
  "remains",
  "should",
  "supply",
  "their",
  "there",
  "these",
  "those",
  "valuation",
  "while",
]);

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

const normalizeText = (value: string): string =>
  value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeDate = (value?: string): string | undefined => {
  if (!value?.trim()) return undefined;
  const ts = Date.parse(value);
  if (!Number.isFinite(ts)) return undefined;
  return new Date(ts).toISOString().slice(0, 10);
};

const toDateMs = (value?: string): number | undefined => {
  const normalized = normalizeDate(value);
  if (!normalized) return undefined;
  const ts = Date.parse(`${normalized}T00:00:00.000Z`);
  return Number.isFinite(ts) ? ts : undefined;
};

const titleCaseMetric = (value: string): string =>
  value
    .split("_")
    .map((part) => part.toUpperCase() === "PCT" ? "%" : part)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");

const extractTopicTokens = (value: string): string[] =>
  normalizeText(value)
    .split(" ")
    .filter((token) => token.length >= 5 && !STOPWORDS.has(token));

const tokenOverlapScore = (left: string, right: string): { score: number; shared: string[] } => {
  const leftTokens = Array.from(new Set(extractTopicTokens(left)));
  const rightTokens = new Set(extractTopicTokens(right));
  const shared = leftTokens.filter((token) => rightTokens.has(token));
  if (!leftTokens.length || !rightTokens.size) return { score: 0, shared: [] };
  return {
    score: shared.length / Math.max(1, Math.min(leftTokens.length, rightTokens.size)),
    shared,
  };
};

const classifyClaimPolarity = (text: string): ClaimPolarity => {
  if (
    /\b(risk|downside|pressure|weak|weakening|slip|uncertain|uncertainty|contradict|break|competition)\b/i.test(
      text,
    )
  ) {
    return "negative";
  }
  if (
    /\b(upside|favorable|strength|improve|discipline|pricing power|demand remains strong|constructive)\b/i.test(
      text,
    )
  ) {
    return "positive";
  }
  return "neutral";
};

const severityFromScore = (score: number): ConflictSeverity => {
  if (score >= 0.7) return "high";
  if (score >= 0.4) return "medium";
  return "low";
};

const formatMetricValue = (valueNum: number | null, valueText: string, unit: string): string => {
  if (typeof valueNum === "number" && Number.isFinite(valueNum)) {
    if (unit === "percent") return `${valueNum.toFixed(1)}%`;
    if (unit === "usd") {
      if (Math.abs(valueNum) >= 1_000_000_000) return `$${(valueNum / 1_000_000_000).toFixed(2)}B`;
      if (Math.abs(valueNum) >= 1_000_000) return `$${(valueNum / 1_000_000).toFixed(2)}M`;
      return `$${valueNum.toFixed(0)}`;
    }
    return `${valueNum.toFixed(2)}${unit ? ` ${unit}` : ""}`.trim();
  }
  return valueText || "n/a";
};

const compareByScore = <T extends { score: number }>(left: T, right: T): number =>
  right.score - left.score;

const renderSourceConflictMarkdown = (report: ExternalResearchSourceConflictReport): string => {
  const lines: string[] = [];
  lines.push(`# ${report.ticker} Source Conflicts`);
  lines.push("");
  lines.push(`- Generated at: ${report.generatedAt}`);
  lines.push(`- Lookback days: ${report.lookbackDays}`);
  lines.push(`- Conflicts: ${report.conflicts.length}`);
  lines.push("");
  if (!report.conflicts.length) {
    lines.push("No material cross-source conflicts detected.");
    lines.push("");
    return lines.join("\n");
  }
  report.conflicts.forEach((conflict, index) => {
    lines.push(`## ${index + 1}. ${conflict.topic}`);
    lines.push("");
    lines.push(
      `- Kind: ${conflict.kind} | Severity: ${conflict.severity} | Score: ${conflict.score.toFixed(2)}${conflict.asOfDate ? ` | As of: ${conflict.asOfDate}` : ""}`,
    );
    lines.push(`- Summary: ${conflict.summary}`);
    conflict.entries.forEach((entry) => {
      lines.push(
        `- ${entry.provider} | ${entry.title} | ${entry.publishedAt ?? "undated"} | ${entry.valueText}${entry.url ? ` | ${entry.url}` : ""}`,
      );
    });
    lines.push("");
  });
  return lines.join("\n");
};

const renderPeerComparisonMarkdown = (comparison: ExternalResearchPeerComparison): string => {
  const lines: string[] = [];
  lines.push(`# Peer Comparison: ${comparison.left.ticker} vs ${comparison.right.ticker}`);
  lines.push("");
  lines.push(`- Generated at: ${comparison.generatedAt}`);
  lines.push(`- Summary: ${comparison.summary}`);
  lines.push(`- Evidence edge: ${comparison.evidenceEdge}`);
  lines.push(`- Risk edge: ${comparison.riskEdge}`);
  lines.push("");
  lines.push("## Scorecard");
  lines.push("");
  lines.push(
    `- ${comparison.left.ticker}: stance=${comparison.left.stance} confidence=${(comparison.left.confidence * 100).toFixed(0)}% sources=${comparison.left.sourceCount} providers=${comparison.left.providerCount} fresh=${(comparison.left.freshSourceRatio * 100).toFixed(0)}% bull=${comparison.left.bullCount} bear=${comparison.left.bearCount} unknowns=${comparison.left.unknownCount} alerts=${comparison.left.unresolvedThesisAlerts}`,
  );
  lines.push(
    `- ${comparison.right.ticker}: stance=${comparison.right.stance} confidence=${(comparison.right.confidence * 100).toFixed(0)}% sources=${comparison.right.sourceCount} providers=${comparison.right.providerCount} fresh=${(comparison.right.freshSourceRatio * 100).toFixed(0)}% bull=${comparison.right.bullCount} bear=${comparison.right.bearCount} unknowns=${comparison.right.unknownCount} alerts=${comparison.right.unresolvedThesisAlerts}`,
  );
  lines.push("");
  lines.push("## Notable Deltas");
  lines.push("");
  if (comparison.notableDeltas.length) {
    comparison.notableDeltas.forEach((delta) => lines.push(`- ${delta}`));
  } else {
    lines.push("- No material deltas detected.");
  }
  lines.push("");
  lines.push("## Next Actions");
  lines.push("");
  comparison.nextActions.forEach((action) => lines.push(`- ${action}`));
  lines.push("");
  return lines.join("\n");
};

export const detectExternalResearchSourceConflicts = (params: {
  ticker: string;
  dbPath?: string;
  lookbackDays?: number;
  maxConflicts?: number;
}): ExternalResearchSourceConflictReport => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
  const lookbackDays = Math.max(7, Math.round(params.lookbackDays ?? 90));
  const maxConflicts = Math.max(1, Math.round(params.maxConflicts ?? 8));
  const cutoffMs = Date.now() - lookbackDays * DAY_MS;
  const entity = db
    .prepare(
      `SELECT id
       FROM research_entities
       WHERE ticker=?
       ORDER BY updated_at DESC, id DESC
       LIMIT 1`,
    )
    .get(ticker) as { id: number } | undefined;
  if (!entity) {
    throw new Error(`research entity not found for ticker=${ticker}`);
  }

  const factRows = db
    .prepare(
      `SELECT
         rf.metric_key,
         rf.unit,
         rf.as_of_date,
         rf.value_num,
         rf.value_text,
         d.id AS document_id,
         d.provider,
         d.title,
         COALESCE(NULLIF(d.canonical_url, ''), d.url) AS url,
         COALESCE(NULLIF(d.published_at, ''), NULLIF(d.received_at, ''), '') AS published_at
       FROM research_facts rf
       JOIN external_documents d
         ON d.id=rf.source_ref_id
        AND rf.source_table='external_documents'
       WHERE rf.entity_id=?
         AND d.ticker=?
         AND d.fetched_at >= ?
       ORDER BY rf.metric_key, rf.as_of_date DESC, d.trust_tier ASC, d.fetched_at DESC, d.id DESC`,
    )
    .all(entity.id, ticker, cutoffMs) as Array<{
    metric_key: string;
    unit: string;
    as_of_date?: string;
    value_num: number | null;
    value_text: string;
    document_id: number;
    provider: string;
    title: string;
    url: string;
    published_at?: string;
  }>;

  const factConflicts: ExternalResearchSourceConflict[] = [];
  const factGroups = new Map<string, typeof factRows>();
  factRows.forEach((row) => {
    const key = `${row.metric_key}::${row.unit}`;
    const bucket = factGroups.get(key) ?? [];
    bucket.push(row);
    factGroups.set(key, bucket);
  });
  for (const [key, rows] of factGroups) {
    const numericRows = rows.filter(
      (row) => typeof row.value_num === "number" && Number.isFinite(row.value_num),
    );
    const uniqueDocs = new Set(rows.map((row) => row.document_id));
    if (numericRows.length < 2 || uniqueDocs.size < 2) continue;
    const values = numericRows.map((row) => row.value_num as number);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const absDiff = Math.abs(max - min);
    const denominator = Math.max(1, Math.abs(max), Math.abs(min));
    const relativeSpread = absDiff / denominator;
    const [metricKey, unit] = key.split("::");
    const datedRows = rows
      .map((row) => toDateMs(row.as_of_date) ?? toDateMs(row.published_at))
      .filter((value): value is number => typeof value === "number")
      .sort((left, right) => left - right);
    if (
      datedRows.length >= 2 &&
      datedRows[datedRows.length - 1]! - datedRows[0]! > 21 * DAY_MS
    ) {
      continue;
    }
    const bestAsOfDate = normalizeDate(rows[0]?.as_of_date) ?? normalizeDate(rows[0]?.published_at);
    const percentThreshold = unit === "percent" ? 2 : 0;
    if (relativeSpread < 0.12 && absDiff < percentThreshold) continue;
    const score = clamp01(Math.max(relativeSpread, unit === "percent" ? absDiff / 12 : absDiff / denominator));
    const topRows = numericRows
      .toSorted((left, right) => {
        if ((right.value_num ?? 0) !== (left.value_num ?? 0)) {
          return (right.value_num ?? 0) - (left.value_num ?? 0);
        }
        return left.document_id - right.document_id;
      })
      .slice(0, 3);
    factConflicts.push({
      ticker,
      kind: "fact",
      severity: severityFromScore(score),
      score,
      topic: titleCaseMetric(metricKey),
      asOfDate: bestAsOfDate,
      summary: `${titleCaseMetric(metricKey)} differs across sources by ${unit === "percent" ? `${absDiff.toFixed(1)} pts` : `${(relativeSpread * 100).toFixed(0)}%`} for ${ticker}.`,
      entries: topRows.map((row) => ({
        documentId: row.document_id,
        provider: row.provider,
        title: row.title,
        url: row.url,
        publishedAt: normalizeDate(row.published_at),
        valueText: formatMetricValue(row.value_num, row.value_text, row.unit),
      })),
    });
  }

  const claimRows = db
    .prepare(
      `SELECT
         c.id,
         c.claim_text,
         c.claim_type,
         c.confidence,
         d.id AS document_id,
         d.provider,
         d.title,
         COALESCE(NULLIF(d.canonical_url, ''), d.url) AS url,
         COALESCE(NULLIF(d.published_at, ''), NULLIF(d.received_at, ''), '') AS published_at
       FROM research_claims c
       JOIN research_claim_evidence e
         ON e.claim_id=c.id
        AND e.source_table='external_documents'
       JOIN external_documents d
         ON d.id=e.ref_id
       WHERE c.entity_id=?
         AND d.ticker=?
         AND d.fetched_at >= ?
       ORDER BY c.confidence DESC, d.trust_tier ASC, d.fetched_at DESC, c.id DESC
       LIMIT 24`,
    )
    .all(entity.id, ticker, cutoffMs) as Array<{
    id: number;
    claim_text: string;
    claim_type: string;
    confidence: number;
    document_id: number;
    provider: string;
    title: string;
    url: string;
    published_at?: string;
  }>;

  const claimConflicts: ExternalResearchSourceConflict[] = [];
  for (let i = 0; i < claimRows.length; i += 1) {
    const left = claimRows[i]!;
    const leftPolarity = classifyClaimPolarity(left.claim_text);
    if (leftPolarity === "neutral") continue;
    for (let j = i + 1; j < claimRows.length; j += 1) {
      const right = claimRows[j]!;
      if (left.document_id === right.document_id) continue;
      const rightPolarity = classifyClaimPolarity(right.claim_text);
      if (rightPolarity === "neutral" || leftPolarity === rightPolarity) continue;
      const overlap = tokenOverlapScore(left.claim_text, right.claim_text);
      if (overlap.score < 0.34 || overlap.shared.length < 2) continue;
      const score = clamp01(((left.confidence + right.confidence) / 2) * 0.7 + overlap.score * 0.3);
      claimConflicts.push({
        ticker,
        kind: "claim",
        severity: severityFromScore(score),
        score,
        topic: overlap.shared.slice(0, 3).join(", "),
        summary: `${left.provider} and ${right.provider} disagree on ${overlap.shared.slice(0, 3).join(", ")} for ${ticker}.`,
        entries: [
          {
            documentId: left.document_id,
            provider: left.provider,
            title: left.title,
            url: left.url,
            publishedAt: normalizeDate(left.published_at),
            valueText: left.claim_text,
          },
          {
            documentId: right.document_id,
            provider: right.provider,
            title: right.title,
            url: right.url,
            publishedAt: normalizeDate(right.published_at),
            valueText: right.claim_text,
          },
        ],
      });
    }
  }

  const conflicts = [...factConflicts, ...claimConflicts]
    .toSorted(compareByScore)
    .slice(0, maxConflicts);
  const report: ExternalResearchSourceConflictReport = {
    ticker,
    generatedAt: new Date().toISOString(),
    lookbackDays,
    conflicts,
    markdown: "",
  };
  report.markdown = renderSourceConflictMarkdown(report);
  return report;
};

const loadPeerSnapshot = (params: { ticker: string; dbPath?: string }): ExternalResearchPeerSnapshot => {
  const report = getLatestExternalResearchStructuredReport({
    ticker: params.ticker,
    dbPath: params.dbPath,
  });
  if (!report) {
    throw new Error(`no external structured report found for ticker=${normalizeTicker(params.ticker)}`);
  }
  const thesis = getLatestExternalResearchThesis({
    ticker: params.ticker,
    dbPath: params.dbPath,
  });
  if (!thesis) {
    throw new Error(`no external thesis found for ticker=${normalizeTicker(params.ticker)}`);
  }
  const db = openResearchDb(params.dbPath);
  const unresolvedThesisAlerts =
    (
      db.prepare(
        `SELECT COUNT(*) AS count
         FROM thesis_alerts
         WHERE ticker=? AND resolved=0`,
      ).get(normalizeTicker(params.ticker)) as { count?: number } | undefined
    )?.count ?? 0;
  const pendingRefreshes =
    (
      db.prepare(
        `SELECT COUNT(*) AS count
         FROM research_refresh_queue
         WHERE ticker=? AND status='queued'`,
      ).get(normalizeTicker(params.ticker)) as { count?: number } | undefined
    )?.count ?? 0;
  return buildPeerSnapshot({ report, thesis, unresolvedThesisAlerts, pendingRefreshes });
};

const buildPeerSnapshot = (params: {
  report: StoredExternalResearchReport;
  thesis: ExternalResearchThesis;
  unresolvedThesisAlerts: number;
  pendingRefreshes: number;
}): ExternalResearchPeerSnapshot => ({
  ticker: params.report.ticker,
  title: params.report.title,
  generatedAt: params.report.generatedAt,
  confidence: params.report.confidence,
  stance: params.thesis.stance,
  sourceCount: params.report.evidenceCoverage.sourceCount,
  providerCount: params.report.evidenceCoverage.providerCount,
  freshSourceRatio: params.report.evidenceCoverage.freshSourceRatio,
  bullCount: params.report.bullCase.length,
  bearCount: params.report.bearCase.length,
  unknownCount: params.report.unknowns.length,
  unresolvedThesisAlerts: params.unresolvedThesisAlerts,
  pendingRefreshes: params.pendingRefreshes,
  whatChanged: params.report.whatChanged.slice(0, 5),
  evidence: params.report.evidence.slice(0, 4),
  openQuestions: params.thesis.openQuestions.slice(0, 5),
});

export const compareExternalResearchPeers = (params: {
  leftTicker: string;
  rightTicker: string;
  dbPath?: string;
}): ExternalResearchPeerComparison => {
  const left = loadPeerSnapshot({ ticker: params.leftTicker, dbPath: params.dbPath });
  const right = loadPeerSnapshot({ ticker: params.rightTicker, dbPath: params.dbPath });
  const confidenceLeader = left.confidence >= right.confidence ? left : right;
  const evidenceLeader =
    left.providerCount > right.providerCount ||
    (left.providerCount === right.providerCount && left.sourceCount >= right.sourceCount)
      ? left
      : right;
  const riskHeavier =
    left.unresolvedThesisAlerts > right.unresolvedThesisAlerts ||
    (left.unresolvedThesisAlerts === right.unresolvedThesisAlerts &&
      left.bearCount + left.unknownCount >= right.bearCount + right.unknownCount)
      ? left
      : right;

  const notableDeltas: string[] = [
    `${confidenceLeader.ticker} has the stronger current research confidence (${(confidenceLeader.confidence * 100).toFixed(0)}%).`,
    `${evidenceLeader.ticker} has the stronger evidence base (${evidenceLeader.providerCount} providers across ${evidenceLeader.sourceCount} sources).`,
    `${riskHeavier.ticker} carries the heavier monitoring burden (${riskHeavier.unresolvedThesisAlerts} unresolved thesis alerts, ${riskHeavier.pendingRefreshes} queued refreshes).`,
  ];
  if (left.whatChanged[0]) {
    notableDeltas.push(`${left.ticker} latest change: ${left.whatChanged[0]}`);
  }
  if (right.whatChanged[0]) {
    notableDeltas.push(`${right.ticker} latest change: ${right.whatChanged[0]}`);
  }

  const nextActions = [
    `${riskHeavier.ticker}: review the newest disconfirming evidence and close queued refreshes before increasing exposure.`,
    `${evidenceLeader.ticker}: reuse the current evidence base as the benchmark when refreshing the weaker-covered peer.`,
    `${confidenceLeader.ticker === left.ticker ? right.ticker : left.ticker}: add at least one differentiated source to narrow the evidence gap.`,
  ];

  const comparison: ExternalResearchPeerComparison = {
    generatedAt: new Date().toISOString(),
    left,
    right,
    summary: `${confidenceLeader.ticker} currently has the stronger supported setup, while ${riskHeavier.ticker} needs tighter monitoring for thesis drift.`,
    evidenceEdge: `${evidenceLeader.ticker} leads on source diversity and report coverage.`,
    riskEdge: `${riskHeavier.ticker} has the higher thesis-break and refresh burden.`,
    notableDeltas,
    nextActions,
    markdown: "",
  };
  comparison.markdown = renderPeerComparisonMarkdown(comparison);
  return comparison;
};
