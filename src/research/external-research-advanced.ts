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

export type ExternalResearchGuidanceDriftItem = {
  ticker: string;
  metricKey: string;
  metricLabel: string;
  score: number;
  direction: "up" | "down" | "flat";
  summary: string;
  latest: ExternalResearchConflictEntry & {
    asOfDate?: string;
    valueNum?: number;
  };
  previous: ExternalResearchConflictEntry & {
    asOfDate?: string;
    valueNum?: number;
  };
};

export type ExternalResearchGuidanceDriftReport = {
  ticker: string;
  generatedAt: string;
  items: ExternalResearchGuidanceDriftItem[];
  markdown: string;
};

export type ExternalResearchCredibilityAlert = {
  topic: string;
  summary: string;
  earlier: ExternalResearchConflictEntry;
  later: ExternalResearchConflictEntry;
  score: number;
};

export type ExternalResearchManagementCredibilityReport = {
  ticker: string;
  generatedAt: string;
  trackedClaims: number;
  reaffirmedClaims: number;
  contradictedClaims: number;
  openClaims: number;
  credibilityScore: number;
  alerts: ExternalResearchCredibilityAlert[];
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

const renderGuidanceDriftMarkdown = (report: ExternalResearchGuidanceDriftReport): string => {
  const lines: string[] = [];
  lines.push(`# ${report.ticker} Guidance Drift`);
  lines.push("");
  lines.push(`- Generated at: ${report.generatedAt}`);
  lines.push(`- Metrics with material drift: ${report.items.length}`);
  lines.push("");
  if (!report.items.length) {
    lines.push("No material guidance drift detected.");
    lines.push("");
    return lines.join("\n");
  }
  report.items.forEach((item) => {
    lines.push(`## ${item.metricLabel}`);
    lines.push("");
    lines.push(`- Direction: ${item.direction} | Score: ${item.score.toFixed(2)}`);
    lines.push(`- Summary: ${item.summary}`);
    lines.push(
      `- Latest: ${item.latest.provider} | ${item.latest.valueText} | ${item.latest.asOfDate ?? item.latest.publishedAt ?? "undated"} | ${item.latest.title}`,
    );
    lines.push(
      `- Previous: ${item.previous.provider} | ${item.previous.valueText} | ${item.previous.asOfDate ?? item.previous.publishedAt ?? "undated"} | ${item.previous.title}`,
    );
    lines.push("");
  });
  return lines.join("\n");
};

const renderManagementCredibilityMarkdown = (
  report: ExternalResearchManagementCredibilityReport,
): string => {
  const lines: string[] = [];
  lines.push(`# ${report.ticker} Management Credibility`);
  lines.push("");
  lines.push(`- Generated at: ${report.generatedAt}`);
  lines.push(`- Credibility score: ${(report.credibilityScore * 100).toFixed(0)}%`);
  lines.push(
    `- Tracked claims: ${report.trackedClaims} | Reaffirmed: ${report.reaffirmedClaims} | Contradicted: ${report.contradictedClaims} | Open: ${report.openClaims}`,
  );
  lines.push("");
  if (!report.alerts.length) {
    lines.push("No management credibility breaks detected in the tracked evidence set.");
    lines.push("");
    return lines.join("\n");
  }
  lines.push("## Contradictions");
  lines.push("");
  report.alerts.forEach((alert) => {
    lines.push(`- ${alert.summary} (score=${alert.score.toFixed(2)})`);
    lines.push(
      `  earlier: ${alert.earlier.provider} | ${alert.earlier.publishedAt ?? "undated"} | ${alert.earlier.valueText}`,
    );
    lines.push(
      `  later: ${alert.later.provider} | ${alert.later.publishedAt ?? "undated"} | ${alert.later.valueText}`,
    );
  });
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

const GUIDANCE_DRIFT_METRICS = new Set([
  "revenue_growth_pct",
  "gross_margin_pct",
  "operating_margin_pct",
  "capex_amount",
]);

export const analyzeExternalResearchGuidanceDrift = (params: {
  ticker: string;
  dbPath?: string;
  limit?: number;
}): ExternalResearchGuidanceDriftReport => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
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
  const rows = db
    .prepare(
      `SELECT
         rf.metric_key,
         rf.value_num,
         rf.value_text,
         rf.unit,
         rf.as_of_date,
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
       ORDER BY COALESCE(NULLIF(rf.as_of_date, ''), NULLIF(d.published_at, ''), NULLIF(d.received_at, ''), '') DESC,
                d.id DESC`,
    )
    .all(entity.id, ticker) as Array<{
    metric_key: string;
    value_num: number | null;
    value_text: string;
    unit: string;
    as_of_date?: string;
    document_id: number;
    provider: string;
    title: string;
    url: string;
    published_at?: string;
  }>;
  const grouped = new Map<string, typeof rows>();
  rows.forEach((row) => {
    if (!GUIDANCE_DRIFT_METRICS.has(row.metric_key)) return;
    const bucket = grouped.get(row.metric_key) ?? [];
    if (bucket.some((existing) => existing.document_id === row.document_id)) return;
    bucket.push(row);
    grouped.set(row.metric_key, bucket);
  });
  const items: ExternalResearchGuidanceDriftItem[] = [];
  for (const [metricKey, metricRows] of grouped) {
    if (metricRows.length < 2) continue;
    const latest = metricRows[0]!;
    const previous = metricRows[1]!;
    const latestValue =
      typeof latest.value_num === "number" && Number.isFinite(latest.value_num)
        ? latest.value_num
        : undefined;
    const previousValue =
      typeof previous.value_num === "number" && Number.isFinite(previous.value_num)
        ? previous.value_num
        : undefined;
    if (typeof latestValue !== "number" || typeof previousValue !== "number") continue;
    const delta = latestValue - previousValue;
    const denominator = Math.max(1, Math.abs(previousValue));
    const normalizedDelta = Math.abs(delta) / denominator;
    if (normalizedDelta < 0.08 && Math.abs(delta) < (latest.unit === "percent" ? 1.5 : 0.15)) {
      continue;
    }
    const direction = delta > 0 ? "up" : delta < 0 ? "down" : "flat";
    const score = clamp01(
      Math.max(normalizedDelta, latest.unit === "percent" ? Math.abs(delta) / 10 : normalizedDelta),
    );
    items.push({
      ticker,
      metricKey,
      metricLabel: titleCaseMetric(metricKey),
      score,
      direction,
      summary: `${titleCaseMetric(metricKey)} moved ${direction} from ${formatMetricValue(previousValue, previous.value_text, previous.unit)} to ${formatMetricValue(latestValue, latest.value_text, latest.unit)}.`,
      latest: {
        documentId: latest.document_id,
        provider: latest.provider,
        title: latest.title,
        url: latest.url,
        publishedAt: normalizeDate(latest.published_at),
        asOfDate: normalizeDate(latest.as_of_date),
        valueNum: latestValue,
        valueText: formatMetricValue(latestValue, latest.value_text, latest.unit),
      },
      previous: {
        documentId: previous.document_id,
        provider: previous.provider,
        title: previous.title,
        url: previous.url,
        publishedAt: normalizeDate(previous.published_at),
        asOfDate: normalizeDate(previous.as_of_date),
        valueNum: previousValue,
        valueText: formatMetricValue(previousValue, previous.value_text, previous.unit),
      },
    });
  }
  const report: ExternalResearchGuidanceDriftReport = {
    ticker,
    generatedAt: new Date().toISOString(),
    items: items
      .toSorted(compareByScore)
      .slice(0, Math.max(1, Math.round(params.limit ?? 6))),
    markdown: "",
  };
  report.markdown = renderGuidanceDriftMarkdown(report);
  return report;
};

export const analyzeExternalResearchManagementCredibility = (params: {
  ticker: string;
  dbPath?: string;
  maxAlerts?: number;
}): ExternalResearchManagementCredibilityReport => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
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
  const rows = db
    .prepare(
      `SELECT
         c.id,
         c.claim_text,
         c.confidence,
         c.valid_from,
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
       ORDER BY COALESCE(NULLIF(c.valid_from, ''), NULLIF(d.published_at, ''), NULLIF(d.received_at, ''), '') ASC,
                c.id ASC`,
    )
    .all(entity.id, ticker) as Array<{
    id: number;
    claim_text: string;
    confidence: number;
    valid_from?: string;
    document_id: number;
    provider: string;
    title: string;
    url: string;
    published_at?: string;
  }>;
  const tracked = rows.filter((row) =>
    /\b(management|guidance|outlook|forecast)\b/i.test(row.claim_text),
  );
  let reaffirmedClaims = 0;
  let contradictedClaims = 0;
  const alerts: ExternalResearchCredibilityAlert[] = [];
  tracked.forEach((earlier, index) => {
    const earlierPolarity = classifyClaimPolarity(earlier.claim_text);
    if (earlierPolarity === "neutral") return;
    let bestReaffirmationScore = 0;
    let bestContradiction: ExternalResearchCredibilityAlert | null = null;
    for (let i = index + 1; i < tracked.length; i += 1) {
      const later = tracked[i]!;
      if (later.document_id === earlier.document_id) continue;
      const laterPolarity = classifyClaimPolarity(later.claim_text);
      if (laterPolarity === "neutral") continue;
      const overlap = tokenOverlapScore(earlier.claim_text, later.claim_text);
      if (overlap.score < 0.34 || overlap.shared.length < 2) continue;
      const score = clamp01(((earlier.confidence + later.confidence) / 2) * 0.6 + overlap.score * 0.4);
      if (laterPolarity === earlierPolarity) {
        bestReaffirmationScore = Math.max(bestReaffirmationScore, score);
        continue;
      }
      if (!bestContradiction || score > bestContradiction.score) {
        bestContradiction = {
          topic: overlap.shared.slice(0, 3).join(", "),
          summary: `${later.provider} later contradicted earlier management/guidance language on ${overlap.shared.slice(0, 3).join(", ")}.`,
          earlier: {
            documentId: earlier.document_id,
            provider: earlier.provider,
            title: earlier.title,
            url: earlier.url,
            publishedAt: normalizeDate(earlier.valid_from) ?? normalizeDate(earlier.published_at),
            valueText: earlier.claim_text,
          },
          later: {
            documentId: later.document_id,
            provider: later.provider,
            title: later.title,
            url: later.url,
            publishedAt: normalizeDate(later.valid_from) ?? normalizeDate(later.published_at),
            valueText: later.claim_text,
          },
          score,
        };
      }
    }
    if (bestContradiction) {
      contradictedClaims += 1;
      alerts.push(bestContradiction);
      return;
    }
    if (bestReaffirmationScore > 0) {
      reaffirmedClaims += 1;
    }
  });
  const trackedClaims = tracked.length;
  const openClaims = Math.max(0, trackedClaims - reaffirmedClaims - contradictedClaims);
  const credibilityScore =
    trackedClaims > 0 ? clamp01((reaffirmedClaims + openClaims * 0.5) / trackedClaims) : 0;
  const report: ExternalResearchManagementCredibilityReport = {
    ticker,
    generatedAt: new Date().toISOString(),
    trackedClaims,
    reaffirmedClaims,
    contradictedClaims,
    openClaims,
    credibilityScore,
    alerts: alerts
      .toSorted(compareByScore)
      .slice(0, Math.max(1, Math.round(params.maxAlerts ?? 6))),
    markdown: "",
  };
  report.markdown = renderManagementCredibilityMarkdown(report);
  return report;
};
