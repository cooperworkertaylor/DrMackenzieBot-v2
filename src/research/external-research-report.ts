import { openResearchDb } from "./db.js";

export type ExternalResearchReportSource = {
  documentId: number;
  title: string;
  url: string;
  provider: string;
  sourceType: string;
  publishedAt?: string;
  receivedAt?: string;
  trustTier: number;
  materialityScore: number;
  claimCount: number;
  eventCount: number;
  factCount: number;
};

export type ExternalResearchReportClaim = {
  id: number;
  claimText: string;
  claimType: string;
  confidence: number;
  validFrom?: string;
  documentId?: number;
  title?: string;
  url?: string;
  provider?: string;
  trustTier?: number;
};

export type ExternalResearchReportEvent = {
  id: number;
  eventType: string;
  title: string;
  eventDate?: string;
  documentId: number;
  url?: string;
  provider?: string;
};

export type ExternalResearchReportFact = {
  id: number;
  metricKey: string;
  metricKind: string;
  valueNum?: number;
  valueText: string;
  unit: string;
  asOfDate?: string;
  documentId: number;
  url?: string;
  provider?: string;
};

export type ExternalResearchEvidenceCoverage = {
  sourceCount: number;
  providerCount: number;
  avgTrustScore: number;
  freshSourceRatio: number;
  claims: number;
  events: number;
  facts: number;
};

export type ExternalResearchStructuredReport = {
  entityId: number;
  ticker: string;
  title: string;
  generatedAt: string;
  lookbackDays: number;
  summary: string;
  whatChanged: string[];
  evidence: string[];
  bullCase: string[];
  bearCase: string[];
  unknowns: string[];
  nextActions: string[];
  sources: ExternalResearchReportSource[];
  confidence: number;
  confidenceRationale: string;
  evidenceCoverage: ExternalResearchEvidenceCoverage;
  diffFromPrevious?: {
    previousReportId: number;
    previousGeneratedAt: string;
    confidenceDelta: number;
    newBullCase: string[];
    newBearCase: string[];
    newUnknowns: string[];
    resolvedUnknowns: string[];
  };
  markdown: string;
};

export type StoredExternalResearchReport = ExternalResearchStructuredReport & {
  id: number;
  createdAt: number;
  updatedAt: number;
};

const DAY_MS = 86_400_000;

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const normalizeTicker = (value: string): string => value.trim().toUpperCase();

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

const unique = <T>(values: T[]): T[] => Array.from(new Set(values));

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

const formatMetricValue = (fact: ExternalResearchReportFact): string => {
  if (typeof fact.valueNum === "number" && Number.isFinite(fact.valueNum)) {
    if (fact.unit === "percent") return `${fact.valueNum.toFixed(1)}%`;
    if (fact.unit === "usd") {
      if (Math.abs(fact.valueNum) >= 1_000_000_000) return `$${(fact.valueNum / 1_000_000_000).toFixed(2)}B`;
      if (Math.abs(fact.valueNum) >= 1_000_000) return `$${(fact.valueNum / 1_000_000).toFixed(2)}M`;
      return `$${fact.valueNum.toFixed(0)}`;
    }
    return `${fact.valueNum}${fact.unit ? ` ${fact.unit}` : ""}`.trim();
  }
  return fact.valueText || "n/a";
};

const computeConfidence = (params: {
  sources: ExternalResearchReportSource[];
  claims: ExternalResearchReportClaim[];
  events: ExternalResearchReportEvent[];
  facts: ExternalResearchReportFact[];
  lookbackDays: number;
}): { score: number; rationale: string; coverage: ExternalResearchEvidenceCoverage } => {
  const providers = unique(
    params.sources.map((source) => source.provider.trim().toLowerCase()).filter(Boolean),
  );
  const trustScores = params.sources.map((source) => clamp01((5 - source.trustTier) / 4));
  const avgTrustScore =
    trustScores.length > 0 ? trustScores.reduce((sum, score) => sum + score, 0) / trustScores.length : 0;
  const recencyCutoffMs = Date.now() - Math.min(30, Math.max(7, params.lookbackDays)) * DAY_MS;
  const freshSourceRatio =
    params.sources.length > 0
      ? params.sources.filter((source) => {
          const datedMs = toDateMs(source.publishedAt ?? source.receivedAt);
          return typeof datedMs === "number" ? datedMs >= recencyCutoffMs : false;
        }).length / params.sources.length
      : 0;
  const diversityScore = clamp01(providers.length / 3);
  const structureScore = clamp01(
    (Math.min(params.claims.length, 4) / 4 +
      Math.min(params.events.length, 2) / 2 +
      Math.min(params.facts.length, 2) / 2) /
      3,
  );
  const score =
    0.4 * avgTrustScore + 0.25 * freshSourceRatio + 0.2 * diversityScore + 0.15 * structureScore;
  const rationaleParts = [
    `trust ${(avgTrustScore * 100).toFixed(0)}%`,
    `freshness ${(freshSourceRatio * 100).toFixed(0)}%`,
    `providers=${providers.length}`,
    `claims=${params.claims.length}`,
    `events=${params.events.length}`,
    `facts=${params.facts.length}`,
  ];
  return {
    score: clamp01(score),
    rationale: rationaleParts.join(" | "),
    coverage: {
      sourceCount: params.sources.length,
      providerCount: providers.length,
      avgTrustScore,
      freshSourceRatio,
      claims: params.claims.length,
      events: params.events.length,
      facts: params.facts.length,
    },
  };
};

const renderMarkdown = (report: ExternalResearchStructuredReport): string => {
  const lines: string[] = [];
  lines.push(`# ${report.title}`);
  lines.push("");
  lines.push(`- Generated at: ${report.generatedAt}`);
  lines.push(`- Confidence: ${(report.confidence * 100).toFixed(0)}%`);
  lines.push(`- Coverage: ${report.evidenceCoverage.sourceCount} sources / ${report.evidenceCoverage.claims} claims / ${report.evidenceCoverage.events} events / ${report.evidenceCoverage.facts} facts`);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push(report.summary);
  lines.push("");

  const sections: Array<{ title: string; items: string[] }> = [
    { title: "What Changed", items: report.whatChanged },
    { title: "Evidence", items: report.evidence },
    { title: "Bull Case", items: report.bullCase },
    { title: "Bear Case", items: report.bearCase },
    { title: "Unknowns", items: report.unknowns },
    { title: "Next Actions", items: report.nextActions },
  ];
  for (const section of sections) {
    lines.push(`## ${section.title}`);
    lines.push("");
    for (const item of section.items) {
      lines.push(`- ${item}`);
    }
    if (section.items.length === 0) lines.push("- None.");
    lines.push("");
  }

  lines.push("## Sources");
  lines.push("");
  for (const source of report.sources) {
    const dateLabel = source.publishedAt ?? source.receivedAt ?? "undated";
    lines.push(
      `- D${source.documentId} | ${dateLabel} | tier=${source.trustTier} | ${source.provider}/${source.sourceType} | ${source.title} | ${source.url}`,
    );
  }
  lines.push("");
  lines.push(`Confidence rationale: ${report.confidenceRationale}`);
  if (report.diffFromPrevious) {
    lines.push(`Previous report: ${report.diffFromPrevious.previousGeneratedAt}`);
    lines.push(
      `Delta: confidence ${(report.diffFromPrevious.confidenceDelta * 100).toFixed(0)} pts | +bull ${report.diffFromPrevious.newBullCase.length} | +bear ${report.diffFromPrevious.newBearCase.length} | +unknowns ${report.diffFromPrevious.newUnknowns.length} | resolved ${report.diffFromPrevious.resolvedUnknowns.length}`,
    );
  }
  lines.push("");
  return lines.join("\n");
};

const chooseEvidenceClaims = (
  claims: ExternalResearchReportClaim[],
  limit: number,
): ExternalResearchReportClaim[] =>
  claims
    .toSorted((a, b) => {
      if (b.confidence !== a.confidence) return b.confidence - a.confidence;
      return (b.validFrom ?? "").localeCompare(a.validFrom ?? "");
    })
    .slice(0, limit);

const sentenceLooksNegative = (value: string): boolean =>
  /\b(risk|downside|threat|pressure|stretch|weak|weakening|bottleneck|uncertain|uncertainty|contradict|no longer hold)\b/i.test(
    value,
  );

const sentenceLooksPositive = (value: string): boolean =>
  !sentenceLooksNegative(value) &&
  /\b(upside|improve|strength|favorable|discipline|accelerat|pricing power|demand remains strong)\b/i.test(
    value,
  );

export const buildExternalResearchStructuredReport = (params: {
  ticker: string;
  dbPath?: string;
  lookbackDays?: number;
  maxSources?: number;
  maxClaims?: number;
  maxEvents?: number;
  maxFacts?: number;
}): ExternalResearchStructuredReport => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
  const lookbackDays = Math.max(7, Math.round(params.lookbackDays ?? 45));
  const maxSources = Math.max(3, Math.round(params.maxSources ?? 8));
  const maxClaims = Math.max(4, Math.round(params.maxClaims ?? 8));
  const maxEvents = Math.max(2, Math.round(params.maxEvents ?? 6));
  const maxFacts = Math.max(2, Math.round(params.maxFacts ?? 6));
  const cutoffMs = Date.now() - lookbackDays * DAY_MS;

  const entity = db
    .prepare(
      `SELECT id, canonical_name
       FROM research_entities
       WHERE ticker=?
       ORDER BY updated_at DESC, id DESC
       LIMIT 1`,
    )
    .get(ticker) as { id: number; canonical_name: string } | undefined;
  if (!entity) {
    throw new Error(`research entity not found for ticker=${ticker}`);
  }

  const previousReportRow = db
    .prepare(
      `SELECT id, report_json, generated_at
       FROM research_reports
       WHERE ticker=? AND report_type='external_structured'
       ORDER BY generated_at DESC, id DESC
       LIMIT 1`,
    )
    .get(ticker) as
    | {
        id: number;
        report_json?: string;
        generated_at: number;
      }
    | undefined;
  const previousReport =
    typeof previousReportRow?.report_json === "string" && previousReportRow.report_json.trim()
      ? (JSON.parse(previousReportRow.report_json) as ExternalResearchStructuredReport)
      : null;

  const sources = db
    .prepare(
      `SELECT
         d.id,
         d.title,
         COALESCE(NULLIF(d.canonical_url, ''), d.url) AS url,
         d.provider,
         d.source_type,
         d.published_at,
         d.received_at,
         d.trust_tier,
         d.materiality_score,
         (SELECT COUNT(*) FROM research_claim_evidence e WHERE e.source_table='external_documents' AND e.ref_id=d.id) AS claim_count,
         (SELECT COUNT(*) FROM research_events re WHERE re.source_table='external_documents' AND re.source_ref_id=d.id) AS event_count,
         (SELECT COUNT(*) FROM research_facts rf WHERE rf.source_table='external_documents' AND rf.source_ref_id=d.id) AS fact_count
       FROM external_documents d
       WHERE d.ticker=?
         AND d.fetched_at >= ?
       ORDER BY
         d.materiality_score DESC,
         COALESCE(NULLIF(d.published_at, ''), NULLIF(d.received_at, ''), '') DESC,
         d.id DESC
       LIMIT ?`,
    )
    .all(ticker, cutoffMs, maxSources) as Array<{
    id: number;
    title: string;
    url: string;
    provider: string;
    source_type: string;
    published_at?: string;
    received_at?: string;
    trust_tier: number;
    materiality_score: number;
    claim_count: number;
    event_count: number;
    fact_count: number;
  }>;

  const claims = db
    .prepare(
      `SELECT
         c.id,
         c.claim_text,
         c.claim_type,
         c.confidence,
         c.valid_from,
         d.id AS document_id,
         d.title,
         COALESCE(NULLIF(d.canonical_url, ''), d.url) AS url,
         d.provider,
         d.trust_tier
       FROM research_claims c
       JOIN research_claim_evidence e ON e.claim_id=c.id AND e.source_table='external_documents'
       JOIN external_documents d ON d.id=e.ref_id
       WHERE c.entity_id=?
         AND d.ticker=?
         AND d.fetched_at >= ?
       ORDER BY c.confidence DESC, c.updated_at DESC
       LIMIT ?`,
    )
    .all(entity.id, ticker, cutoffMs, maxClaims) as Array<{
    id: number;
    claim_text: string;
    claim_type: string;
    confidence: number;
    valid_from?: string;
    document_id?: number;
    title?: string;
    url?: string;
    provider?: string;
    trust_tier?: number;
  }>;

  const events = db
    .prepare(
      `SELECT
         re.id,
         re.event_type,
         re.title,
         re.period_end,
         re.event_time,
         re.source_ref_id,
         COALESCE(NULLIF(d.canonical_url, ''), d.url) AS url,
         d.provider
       FROM research_events re
       LEFT JOIN external_documents d ON d.id=re.source_ref_id
       WHERE re.entity_id=?
         AND re.source_table='external_documents'
         AND re.event_time >= ?
       ORDER BY re.event_time DESC, re.id DESC
       LIMIT ?`,
    )
    .all(entity.id, cutoffMs, maxEvents) as Array<{
    id: number;
    event_type: string;
    title: string;
    period_end?: string;
    event_time: number;
    source_ref_id: number;
    url?: string;
    provider?: string;
  }>;

  const facts = db
    .prepare(
      `SELECT
         rf.id,
         rf.metric_key,
         rf.metric_kind,
         rf.value_num,
         rf.value_text,
         rf.unit,
         rf.as_of_date,
         rf.source_ref_id,
         COALESCE(NULLIF(d.canonical_url, ''), d.url) AS url,
         d.provider
       FROM research_facts rf
       LEFT JOIN external_documents d ON d.id=rf.source_ref_id
       WHERE rf.entity_id=?
         AND rf.source_table='external_documents'
         AND (
           rf.as_of_date='' OR
           rf.as_of_date >= ?
         )
       ORDER BY rf.as_of_date DESC, rf.updated_at DESC, rf.id DESC
       LIMIT ?`,
    )
    .all(entity.id, new Date(cutoffMs).toISOString().slice(0, 10), maxFacts) as Array<{
    id: number;
    metric_key: string;
    metric_kind: string;
    value_num?: number;
    value_text: string;
    unit: string;
    as_of_date?: string;
    source_ref_id: number;
    url?: string;
    provider?: string;
  }>;

  const mappedSources: ExternalResearchReportSource[] = sources.map((source) => ({
    documentId: source.id,
    title: source.title,
    url: source.url,
    provider: source.provider,
    sourceType: source.source_type,
    publishedAt: normalizeDate(source.published_at),
    receivedAt: normalizeDate(source.received_at),
    trustTier: source.trust_tier,
    materialityScore: source.materiality_score,
    claimCount: source.claim_count,
    eventCount: source.event_count,
    factCount: source.fact_count,
  }));
  const mappedClaims: ExternalResearchReportClaim[] = claims.map((claim) => ({
    id: claim.id,
    claimText: claim.claim_text,
    claimType: claim.claim_type,
    confidence: clamp01(claim.confidence),
    validFrom: normalizeDate(claim.valid_from),
    documentId: claim.document_id,
    title: claim.title,
    url: claim.url,
    provider: claim.provider,
    trustTier: claim.trust_tier,
  }));
  const mappedEvents: ExternalResearchReportEvent[] = events.map((event) => ({
    id: event.id,
    eventType: event.event_type,
    title: event.title,
    eventDate: normalizeDate(event.period_end) ?? new Date(event.event_time).toISOString().slice(0, 10),
    documentId: event.source_ref_id,
    url: event.url,
    provider: event.provider,
  }));
  const mappedFacts: ExternalResearchReportFact[] = facts.map((fact) => ({
    id: fact.id,
    metricKey: fact.metric_key,
    metricKind: fact.metric_kind,
    valueNum: typeof fact.value_num === "number" ? fact.value_num : undefined,
    valueText: fact.value_text,
    unit: fact.unit,
    asOfDate: normalizeDate(fact.as_of_date),
    documentId: fact.source_ref_id,
    url: fact.url,
    provider: fact.provider,
  }));

  const { score, rationale, coverage } = computeConfidence({
    sources: mappedSources,
    claims: mappedClaims,
    events: mappedEvents,
    facts: mappedFacts,
    lookbackDays,
  });

  const keyClaims = chooseEvidenceClaims(mappedClaims, 4);
  const bullCase = mappedClaims
    .filter((claim) => claim.claimType === "bull_case" || sentenceLooksPositive(claim.claimText))
    .slice(0, 3)
    .map((claim) => `${claim.claimText} (${claim.validFrom ?? "undated"}${claim.provider ? `; ${claim.provider}` : ""})`);
  const bearCase = mappedClaims
    .filter((claim) => claim.claimType === "risk" || sentenceLooksNegative(claim.claimText))
    .slice(0, 3)
    .map((claim) => `${claim.claimText} (${claim.validFrom ?? "undated"}${claim.provider ? `; ${claim.provider}` : ""})`);
  const evidence = [
    ...keyClaims.map(
      (claim) =>
        `${claim.claimText} (${claim.validFrom ?? "undated"}${claim.provider ? `; ${claim.provider}` : ""}${typeof claim.trustTier === "number" ? `; tier ${claim.trustTier}` : ""})`,
    ),
    ...mappedFacts.slice(0, 2).map(
      (fact) =>
        `${fact.metricKey} = ${formatMetricValue(fact)} (${fact.asOfDate ?? "undated"}${fact.provider ? `; ${fact.provider}` : ""})`,
    ),
  ].slice(0, 6);

  const unknowns: string[] = [];
  if (mappedSources.length < 2) {
    unknowns.push("Coverage depends on fewer than two external research documents.");
  }
  if (coverage.providerCount < 2) {
    unknowns.push("Provider diversity is thin; findings may reflect a single lens.");
  }
  if (mappedFacts.length === 0) {
    unknowns.push("No structured numeric facts were extracted from recent external research.");
  }
  if (bearCase.length === 0) {
    unknowns.push("Current external research set does not contain strong disconfirming evidence.");
  }
  if (coverage.avgTrustScore < 0.45) {
    unknowns.push("Source trust is still skewed toward secondary commentary rather than primary documents.");
  }

  const diffFromPrevious =
    previousReportRow && previousReport
      ? {
          previousReportId: previousReportRow.id,
          previousGeneratedAt: previousReport.generatedAt,
          confidenceDelta: score - previousReport.confidence,
          newBullCase: diffStrings(bullCase, previousReport.bullCase),
          newBearCase: diffStrings(bearCase, previousReport.bearCase),
          newUnknowns: diffStrings(unknowns, previousReport.unknowns),
          resolvedUnknowns: diffStrings(previousReport.unknowns, unknowns),
        }
      : undefined;

  const whatChanged = diffFromPrevious
    ? [
        ...diffFromPrevious.newBullCase
          .slice(0, 2)
          .map((item) => `New bullish support: ${item}`),
        ...diffFromPrevious.newBearCase
          .slice(0, 2)
          .map((item) => `New risk: ${item}`),
        ...diffFromPrevious.newUnknowns
          .slice(0, 2)
          .map((item) => `New unknown: ${item}`),
        ...diffFromPrevious.resolvedUnknowns
          .slice(0, 2)
          .map((item) => `Resolved unknown: ${item}`),
        `Confidence delta ${(diffFromPrevious.confidenceDelta * 100).toFixed(0)} pts vs prior report.`,
      ]
        .filter(Boolean)
        .slice(0, 5)
    : mappedEvents.length
      ? mappedEvents
          .slice(0, 4)
          .map((event) => `${event.eventDate ?? "undated"}: ${event.title} [${event.eventType}]`)
      : keyClaims
          .slice(0, 3)
          .map((claim) => `${claim.validFrom ?? "undated"}: ${claim.claimText}`);

  const nextActions = unique(
    [
      coverage.avgTrustScore < 0.7
        ? `Refresh ${ticker} with a primary filing or transcript to raise trust coverage.`
        : "",
      mappedEvents.length === 0
        ? `Ingest a dated catalyst, earnings, or guidance update for ${ticker}.`
        : "",
      mappedFacts.length < 2
        ? `Capture explicit numeric metrics for ${ticker} so the thesis can be stress-tested quantitatively.`
        : "",
      unknowns.length > 0 ? `Review open unknowns before treating this memo as investment-ready.` : "",
    ].filter(Boolean),
  ).slice(0, 4);

  const summarySource = mappedSources[0];
  const summary = [
    `${ticker} external research coverage currently includes ${coverage.sourceCount} dated sources across ${coverage.providerCount} providers over the last ${lookbackDays} days.`,
    summarySource
      ? `The highest-signal recent input is "${summarySource.title}" from ${summarySource.provider} (tier ${summarySource.trustTier}).`
      : "There is no recent ticker-linked external research in the current lookback window.",
    evidence[0] ? `Top supported evidence: ${evidence[0]}` : "No supported evidence has been extracted yet.",
  ].join(" ");

  const report: ExternalResearchStructuredReport = {
    entityId: entity.id,
    ticker,
    title: `${ticker} External Research Memo`,
    generatedAt: new Date().toISOString(),
    lookbackDays,
    summary,
    whatChanged,
    evidence,
    bullCase,
    bearCase,
    unknowns,
    nextActions,
    sources: mappedSources,
    confidence: score,
    confidenceRationale: rationale,
    evidenceCoverage: coverage,
    diffFromPrevious,
    markdown: "",
  };
  report.markdown = renderMarkdown(report);
  return report;
};

export const storeExternalResearchStructuredReport = (params: {
  report: ExternalResearchStructuredReport;
  dbPath?: string;
}): StoredExternalResearchReport => {
  const db = openResearchDb(params.dbPath);
  const now = Date.now();
  const row = db
    .prepare(
      `INSERT INTO research_reports (
         entity_id, ticker, report_type, title, summary, markdown, report_json,
         confidence, source_count, lookback_days, generated_at, created_at, updated_at
       ) VALUES (?, ?, 'external_structured', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       RETURNING id`,
    )
    .get(
      params.report.entityId,
      params.report.ticker,
      params.report.title,
      params.report.summary,
      params.report.markdown,
      JSON.stringify(params.report),
      params.report.confidence,
      params.report.sources.length,
      params.report.lookbackDays,
      Date.parse(params.report.generatedAt),
      now,
      now,
    ) as { id: number };
  return {
    id: row.id,
    createdAt: now,
    updatedAt: now,
    ...params.report,
  };
};

export const getLatestExternalResearchStructuredReport = (params: {
  ticker: string;
  dbPath?: string;
}): StoredExternalResearchReport | null => {
  const db = openResearchDb(params.dbPath);
  const ticker = normalizeTicker(params.ticker);
  const row = db
    .prepare(
      `SELECT id, report_json, created_at, updated_at
       FROM research_reports
       WHERE ticker=? AND report_type='external_structured'
       ORDER BY generated_at DESC, id DESC
       LIMIT 1`,
    )
    .get(ticker) as
    | {
        id: number;
        report_json?: string;
        created_at: number;
        updated_at: number;
      }
    | undefined;
  if (!row?.report_json) return null;
  const parsed = JSON.parse(row.report_json) as ExternalResearchStructuredReport;
  return {
    id: row.id,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    ...parsed,
  };
};
