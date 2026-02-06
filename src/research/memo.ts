import { createHash } from "node:crypto";
import { openResearchDb } from "./db.js";
import { addClaimEvidence, createResearchClaim, upsertResearchEntity } from "./memory-graph.js";
import { computePortfolioPlan, type PortfolioPlan } from "./portfolio.js";
import { appendProvenanceEvent } from "./provenance.js";
import { computeValuation, recordValuationForecast, type ValuationResult } from "./valuation.js";
import { computeVariantPerception, type VariantPerceptionResult } from "./variant.js";
import { searchResearch } from "./vector-search.js";

type MemoLine = { claim: string; citationIds: number[] };

type QualityGateCheck = {
  name: string;
  passed: boolean;
  detail: string;
  weight: number;
};

type QualityGateResult = {
  score: number;
  checks: QualityGateCheck[];
  passed: boolean;
};

type CitationRow = {
  id: number;
  source_table: string;
  ref_id: number;
  metadata?: string;
  url?: string;
};

type Contradiction = {
  severity: "high" | "medium";
  detail: string;
};

type MemoDiagnostics = {
  contradictions: Contradiction[];
  falsificationTriggers: string[];
};

const buildDiagnostics = (params: {
  variant: VariantPerceptionResult;
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
}): MemoDiagnostics => {
  const contradictions: Contradiction[] = [];
  const impliedStance = params.valuation.impliedExpectations?.stance;
  const expectedUpside =
    params.valuation.expectedUpsideWithCatalystsPct ?? params.valuation.expectedUpsidePct;

  if (params.variant.stance === "positive-variant" && impliedStance === "market-too-bullish") {
    contradictions.push({
      severity: "high",
      detail: "Variant signal is positive while valuation implies market is already too bullish.",
    });
  }
  if (params.variant.stance === "negative-variant" && impliedStance === "market-too-bearish") {
    contradictions.push({
      severity: "high",
      detail: "Variant signal is negative while valuation implies market is already too bearish.",
    });
  }
  if (typeof expectedUpside === "number") {
    if (expectedUpside >= 0.15 && params.portfolio.stance === "short") {
      contradictions.push({
        severity: "high",
        detail: "Portfolio stance is short despite strongly positive expected upside.",
      });
    }
    if (expectedUpside <= -0.15 && params.portfolio.stance === "long") {
      contradictions.push({
        severity: "high",
        detail: "Portfolio stance is long despite strongly negative expected upside.",
      });
    }
  }
  if (params.valuation.confidence < 0.5 && params.portfolio.recommendedWeightPct >= 5) {
    contradictions.push({
      severity: "medium",
      detail: "Position size appears too large for current valuation confidence.",
    });
  }

  const falsificationTriggers: string[] = [];
  const avgSurprise = params.variant.metrics.avgSurprisePct;
  const estimateTrend = params.variant.metrics.estimateTrend;
  const revenueGrowth = params.variant.metrics.revenueGrowth;
  const marginDelta = params.variant.metrics.marginDelta;

  if (typeof avgSurprise === "number") {
    falsificationTriggers.push(
      `Two consecutive quarterly EPS surprises below ${Math.max(-12, avgSurprise - 6).toFixed(1)}%.`,
    );
  }
  if (typeof estimateTrend === "number") {
    falsificationTriggers.push(
      `Consensus estimate trend drops below ${Math.min(-0.05, estimateTrend - 0.05).toFixed(3)}.`,
    );
  }
  if (typeof revenueGrowth === "number") {
    falsificationTriggers.push(
      `Revenue growth signal falls below ${Math.min(-0.05, revenueGrowth - 0.05).toFixed(3)}.`,
    );
  }
  if (typeof marginDelta === "number") {
    falsificationTriggers.push(
      `Operating margin delta drops below ${Math.min(-0.02, marginDelta - 0.02).toFixed(3)}.`,
    );
  }
  if (typeof expectedUpside === "number") {
    if (expectedUpside >= 0) {
      falsificationTriggers.push("Expected upside turns negative on refreshed valuation.");
    } else {
      falsificationTriggers.push(
        "Expected downside closes to within +/-2% on refreshed valuation.",
      );
    }
  }
  falsificationTriggers.push(
    `Stop-loss breach at ${(params.portfolio.stopLossPct * 100).toFixed(1)}% from entry.`,
  );

  return {
    contradictions,
    falsificationTriggers: Array.from(new Set(falsificationTriggers)),
  };
};

export const generateMemoAsync = async (params: {
  ticker: string;
  question: string;
  dbPath?: string;
  maxEvidence?: number;
  enforceInstitutionalGrade?: boolean;
  minQualityScore?: number;
}) => {
  const enforceInstitutionalGrade = params.enforceInstitutionalGrade ?? true;
  const minQualityScore = params.minQualityScore ?? 0.8;
  const hits = await searchResearch({
    query: `${params.ticker} ${params.question}`,
    ticker: params.ticker,
    limit: params.maxEvidence ?? 12,
    dbPath: params.dbPath,
    source: "research",
  });
  if (hits.length < 3) {
    throw new Error("Insufficient evidence: fewer than 3 retrieved citations");
  }

  const lines: MemoLine[] = [];
  const top = hits.slice(0, Math.min(hits.length, 8));
  for (let i = 0; i < top.length; i += 2) {
    const a = top[i];
    const b = top[i + 1];
    const text = [a?.text ?? "", b?.text ?? ""].join(" ").trim();
    if (!text) continue;
    const sentence = summarizeText(text, 220);
    const ids = [a?.id, b?.id].filter((n): n is number => typeof n === "number");
    lines.push({ claim: sentence, citationIds: ids });
  }
  if (lines.length < 2 || lines.some((l) => l.citationIds.length === 0)) {
    throw new Error("Citation enforcement failed: every claim must map to evidence");
  }

  const db = openResearchDb(params.dbPath);
  const citations = db
    .prepare(
      `SELECT c.id, c.source_table, c.ref_id, c.metadata,
              CASE
                WHEN c.source_table='filings' THEN (SELECT url FROM filings WHERE id=c.ref_id)
                WHEN c.source_table='transcripts' THEN (SELECT url FROM transcripts WHERE id=c.ref_id)
                WHEN c.source_table='fundamental_facts' THEN (SELECT source_url FROM fundamental_facts WHERE id=c.ref_id)
                WHEN c.source_table='earnings_expectations' THEN (SELECT source_url FROM earnings_expectations WHERE id=c.ref_id)
                ELSE NULL
              END AS url
       FROM chunks c
       WHERE c.id IN (${lines
         .flatMap((l) => l.citationIds)
         .map(() => "?")
         .join(",")})`,
    )
    .all(...lines.flatMap((l) => l.citationIds)) as CitationRow[];
  const byId = new Map(citations.map((c) => [c.id, c]));
  const variant = computeVariantPerception({
    ticker: params.ticker,
    dbPath: params.dbPath,
  });
  const valuation = computeValuation({
    ticker: params.ticker,
    dbPath: params.dbPath,
  });
  const portfolio = computePortfolioPlan({
    ticker: params.ticker,
    dbPath: params.dbPath,
  });
  const diagnostics = buildDiagnostics({
    variant,
    valuation,
    portfolio,
  });

  const memo = [
    `# Research Memo: ${params.ticker.toUpperCase()}`,
    ``,
    `## Question`,
    params.question,
    ``,
    `## Evidence-Based Claims`,
    ...lines.map((line, idx) => {
      const refs = line.citationIds
        .map((id) => {
          const c = byId.get(id);
          const suffix = c?.url ? ` (${c.url})` : "";
          return `[C${id}]${suffix}`;
        })
        .join(", ");
      return `${idx + 1}. ${line.claim}\n   Citations: ${refs}`;
    }),
    ``,
    `## Variant Perception`,
    `- Stance: ${variant.stance}`,
    `- Variant gap score: ${variant.variantGapScore.toFixed(2)}`,
    `- Expectation score: ${variant.expectationScore.toFixed(2)}`,
    `- Fundamental score: ${variant.fundamentalScore.toFixed(2)}`,
    `- Confidence: ${variant.confidence.toFixed(2)}`,
    `- Expectation observations: ${variant.expectationObservations}`,
    `- Fundamental observations: ${variant.fundamentalObservations}`,
    `- Avg surprise %: ${typeof variant.metrics.avgSurprisePct === "number" ? variant.metrics.avgSurprisePct.toFixed(2) : "n/a"}`,
    `- Estimate trend: ${typeof variant.metrics.estimateTrend === "number" ? variant.metrics.estimateTrend.toFixed(3) : "n/a"}`,
    `- Revenue growth signal: ${typeof variant.metrics.revenueGrowth === "number" ? variant.metrics.revenueGrowth.toFixed(3) : "n/a"}`,
    `- Margin delta signal: ${typeof variant.metrics.marginDelta === "number" ? variant.metrics.marginDelta.toFixed(3) : "n/a"}`,
    ...(variant.notes.length ? variant.notes.map((note) => `- Note: ${note}`) : []),
    ``,
    `## Valuation Scenarios`,
    `- Confidence: ${valuation.confidence.toFixed(2)}`,
    `- Current price: ${typeof valuation.currentPrice === "number" ? valuation.currentPrice.toFixed(2) : "n/a"}`,
    `- Expected value/share: ${typeof valuation.expectedSharePrice === "number" ? valuation.expectedSharePrice.toFixed(2) : "n/a"}`,
    `- Expected upside: ${typeof valuation.expectedUpsidePct === "number" ? `${(valuation.expectedUpsidePct * 100).toFixed(1)}%` : "n/a"}`,
    `- Expected upside (with catalysts): ${typeof valuation.expectedUpsideWithCatalystsPct === "number" ? `${(valuation.expectedUpsideWithCatalystsPct * 100).toFixed(1)}%` : "n/a"}`,
    `- Catalyst expected impact: ${typeof valuation.catalystSummary === "object" ? `${(valuation.catalystSummary.expectedImpactPct * 100).toFixed(2)}% (open=${valuation.catalystSummary.openCount})` : "n/a"}`,
    ...valuation.scenarios.map(
      (scenario) =>
        `- ${scenario.name.toUpperCase()}: growth=${(scenario.revenueGrowth * 100).toFixed(1)}% margin=${(scenario.operatingMargin * 100).toFixed(1)}% wacc=${(scenario.wacc * 100).toFixed(1)}% implied_price=${typeof scenario.impliedSharePrice === "number" ? scenario.impliedSharePrice.toFixed(2) : "n/a"}`,
    ),
    ...(valuation.impliedExpectations
      ? [
          `- Market implied stance: ${valuation.impliedExpectations.stance}`,
          `- Implied growth vs model: ${(valuation.impliedExpectations.impliedRevenueGrowth * 100).toFixed(1)}% vs ${(valuation.impliedExpectations.modelRevenueGrowth * 100).toFixed(1)}%`,
          `- Implied margin vs model: ${(valuation.impliedExpectations.impliedOperatingMargin * 100).toFixed(1)}% vs ${(valuation.impliedExpectations.modelOperatingMargin * 100).toFixed(1)}%`,
        ]
      : ["- Market implied stance: insufficient-evidence"]),
    ...(valuation.notes.length ? valuation.notes.map((note) => `- Note: ${note}`) : []),
    ``,
    `## Contradictions`,
    ...(diagnostics.contradictions.length
      ? diagnostics.contradictions.map(
          (item, index) => `${index + 1}. [${item.severity.toUpperCase()}] ${item.detail}`,
        )
      : ["1. None detected."]),
    ``,
    `## Falsification Triggers`,
    ...diagnostics.falsificationTriggers.map((trigger, index) => `${index + 1}. ${trigger}`),
    ``,
    `## Portfolio Plan`,
    `- Stance: ${portfolio.stance}`,
    `- Confidence: ${portfolio.confidence.toFixed(2)}`,
    `- Recommended weight: ${portfolio.recommendedWeightPct.toFixed(2)}%`,
    `- Max risk budget: ${portfolio.maxRiskBudgetPct.toFixed(2)}%`,
    `- Stop loss: ${(portfolio.stopLossPct * 100).toFixed(1)}%`,
    `- Horizon: ${portfolio.timeHorizonDays} days`,
    ...portfolio.rationale.map((line) => `- Rationale: ${line}`),
    ...portfolio.reviewTriggers.map((line) => `- Review trigger: ${line}`),
    ``,
    `## Citation Index`,
    ...citations.map(
      (c) =>
        `- C${c.id}: source=${c.source_table} ref=${c.ref_id}${c.url ? ` url=${c.url}` : ""}${c.metadata ? ` meta=${c.metadata}` : ""}`,
    ),
  ].join("\n");

  const quality = assessInstitutionalQuality({
    hitsCount: hits.length,
    lines,
    citations,
    variant,
    valuation,
    portfolio,
    diagnostics,
    minQualityScore,
  });

  if (enforceInstitutionalGrade && !quality.passed) {
    const failed = quality.checks.filter((c) => !c.passed);
    const details = failed.map((f) => `${f.name}: ${f.detail}`).join("; ");
    throw new Error(
      `Institutional-grade quality gate failed (score=${quality.score.toFixed(2)} < ${minQualityScore.toFixed(2)}): ${details}`,
    );
  }

  const entity = upsertResearchEntity({
    kind: "company",
    canonicalName: params.ticker.toUpperCase(),
    ticker: params.ticker,
    metadata: {
      source: "memo",
      question: params.question,
    },
    dbPath: params.dbPath,
  });
  const claimIds: number[] = [];
  for (const line of lines) {
    const claim = createResearchClaim({
      entityId: entity.id,
      claimText: line.claim,
      claimType: "memo_claim",
      confidence: quality.score,
      validFrom: new Date().toISOString().slice(0, 10),
      status: quality.passed ? "active" : "draft",
      metadata: {
        question: params.question,
        quality_score: quality.score,
        citation_ids: line.citationIds,
      },
      dbPath: params.dbPath,
    });
    claimIds.push(claim.id);
    for (const citationId of line.citationIds) {
      const citation = byId.get(citationId);
      if (!citation) continue;
      addClaimEvidence({
        claimId: claim.id,
        sourceTable: citation.source_table,
        refId: citation.ref_id,
        citationUrl: citation.url,
        excerptText: line.claim,
        metadata: {
          chunk_id: citation.id,
          chunk_metadata: citation.metadata ?? "",
        },
        dbPath: params.dbPath,
      });
    }
  }

  let forecastId: number | undefined;
  if (
    typeof valuation.currentPrice === "number" &&
    typeof valuation.currentPriceDate === "string" &&
    typeof valuation.expectedUpsideWithCatalystsPct === "number"
  ) {
    forecastId = recordValuationForecast({
      ticker: params.ticker,
      predictedReturn: valuation.expectedUpsideWithCatalystsPct,
      startPrice: valuation.currentPrice,
      basePriceDate: valuation.currentPriceDate,
      source: "memo",
      dbPath: params.dbPath,
    });
  }

  const memoWithQuality = [
    memo,
    "",
    "## Quality Gate",
    `- Score: ${quality.score.toFixed(2)} (threshold ${minQualityScore.toFixed(2)})`,
    ...quality.checks.map(
      (c) => `- ${c.passed ? "PASS" : "FAIL"} ${c.name}: ${c.detail} (weight ${c.weight})`,
    ),
    `- Contradictions: ${diagnostics.contradictions.length}`,
    `- Falsification triggers: ${diagnostics.falsificationTriggers.length}`,
    `- Portfolio stance: ${portfolio.stance}`,
    typeof forecastId === "number" ? `- Forecast id: ${forecastId}` : "- Forecast id: n/a",
  ].join("\n");
  const memoHash = createHash("sha256").update(memoWithQuality).digest("hex");
  try {
    appendProvenanceEvent({
      eventType: "memo_deliverable",
      entityType: "research_memo",
      entityId: `${params.ticker.toUpperCase()}:${forecastId ?? "na"}:${memoHash.slice(0, 12)}`,
      payload: {
        ticker: params.ticker.toUpperCase(),
        question: params.question,
        memo_hash: memoHash,
        claims: lines.length,
        citations: citations.length,
        quality_score: quality.score,
        quality_passed: quality.passed,
        forecast_id: forecastId ?? null,
        claim_ids: claimIds,
      },
      metadata: {
        min_quality_score: minQualityScore,
        enforce_institutional_grade: enforceInstitutionalGrade,
      },
      dbPath: params.dbPath,
    });
  } catch {
    // Preserve memo path even if provenance persistence fails.
  }

  return {
    memo: memoWithQuality,
    citations: citations.length,
    claims: lines.length,
    quality,
    variant,
    valuation,
    portfolio,
    diagnostics,
    forecastId,
  };
};

const summarizeText = (text: string, maxChars: number): string => {
  const cleaned = text.replace(/\s+/g, " ").trim();
  if (cleaned.length <= maxChars) return cleaned;
  const clipped = cleaned.slice(0, maxChars);
  const lastSpace = clipped.lastIndexOf(" ");
  return `${clipped.slice(0, Math.max(0, lastSpace))}...`;
};

const parseJsonObject = <T extends object>(value: unknown, fallback: T): T => {
  if (typeof value !== "string" || !value.trim()) return fallback;
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return fallback;
    return parsed as T;
  } catch {
    return fallback;
  }
};

const toDateMs = (value: unknown): number | undefined => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T00:00:00.000Z` : value;
  const ts = Date.parse(normalized);
  return Number.isFinite(ts) ? ts : undefined;
};

const citationRecencyMs = (citation: CitationRow): number | undefined => {
  const metadata = parseJsonObject<Record<string, unknown>>(citation.metadata, {});
  for (const key of [
    "asOfDate",
    "periodEnd",
    "filingDate",
    "filed",
    "event_date",
    "eventDate",
    "acceptedAt",
  ]) {
    const ts = toDateMs(metadata[key]);
    if (typeof ts === "number") return ts;
  }
  const fromUrl = citation.url?.match(/\b\d{4}-\d{2}-\d{2}\b/)?.[0];
  return typeof fromUrl === "string" ? toDateMs(fromUrl) : undefined;
};

const extractCitationHost = (url?: string): string | undefined => {
  if (!url?.trim()) return undefined;
  try {
    return new URL(url).hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return undefined;
  }
};

const assessInstitutionalQuality = (params: {
  hitsCount: number;
  lines: MemoLine[];
  citations: CitationRow[];
  variant: VariantPerceptionResult;
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
  diagnostics: MemoDiagnostics;
  minQualityScore: number;
}): QualityGateResult => {
  const checks: QualityGateCheck[] = [];
  const claimCount = params.lines.length;
  const citationsPerClaim =
    claimCount > 0 ? params.lines.reduce((s, l) => s + l.citationIds.length, 0) / claimCount : 0;
  const uniqueUrls = new Set(
    params.citations.map((c) => c.url?.trim()).filter((u): u is string => Boolean(u)),
  ).size;
  const uniqueHosts = new Set(
    params.citations
      .map((citation) => extractCitationHost(citation.url))
      .filter((value): value is string => Boolean(value)),
  ).size;
  const uniqueSourceTables = new Set(params.citations.map((c) => c.source_table)).size;
  const datedCitations = params.citations
    .map((citation) => citationRecencyMs(citation))
    .filter((value): value is number => typeof value === "number");
  const freshCutoff = Date.now() - 730 * 86_400_000;
  const freshRatio =
    datedCitations.length > 0
      ? datedCitations.filter((timestamp) => timestamp >= freshCutoff).length /
        datedCitations.length
      : 0;
  const mediumContradictions = params.diagnostics.contradictions.filter(
    (item) => item.severity === "medium",
  ).length;

  checks.push({
    name: "claim_count",
    passed: claimCount >= 4,
    detail: `claims=${claimCount} (required >= 4)`,
    weight: 0.16,
  });
  checks.push({
    name: "citations_per_claim",
    passed: citationsPerClaim >= 2,
    detail: `avg=${citationsPerClaim.toFixed(2)} (required >= 2.00)`,
    weight: 0.18,
  });
  checks.push({
    name: "source_diversity",
    passed: uniqueUrls >= 3 || uniqueSourceTables >= 2,
    detail: `unique_urls=${uniqueUrls}, source_tables=${uniqueSourceTables} (required urls>=3 or sources>=2)`,
    weight: 0.1,
  });
  checks.push({
    name: "source_independence",
    passed: uniqueHosts >= 2 || uniqueSourceTables >= 3,
    detail: `unique_hosts=${uniqueHosts}, source_tables=${uniqueSourceTables} (required hosts>=2 or sources>=3)`,
    weight: 0.1,
  });
  checks.push({
    name: "evidence_freshness",
    passed: freshRatio >= 0.6 || datedCitations.length === 0,
    detail: `fresh_ratio=${(freshRatio * 100).toFixed(1)}% dated_citations=${datedCitations.length} (required >= 60%)`,
    weight: 0.08,
  });
  checks.push({
    name: "retrieval_depth",
    passed: params.hitsCount >= 8,
    detail: `hits=${params.hitsCount} (required >= 8)`,
    weight: 0.08,
  });
  checks.push({
    name: "expectation_coverage",
    passed: params.variant.expectationObservations >= 4,
    detail: `expectation_observations=${params.variant.expectationObservations} (required >= 4)`,
    weight: 0.08,
  });
  checks.push({
    name: "variant_confidence",
    passed: params.variant.confidence >= 0.55,
    detail: `variant_confidence=${params.variant.confidence.toFixed(2)} (required >= 0.55)`,
    weight: 0.08,
  });
  checks.push({
    name: "valuation_coverage",
    passed:
      params.valuation.confidence >= 0.6 &&
      params.valuation.scenarios.filter(
        (scenario) => typeof scenario.impliedSharePrice === "number",
      ).length >= 2,
    detail: `valuation_confidence=${params.valuation.confidence.toFixed(2)} priced_scenarios=${params.valuation.scenarios.filter((scenario) => typeof scenario.impliedSharePrice === "number").length} (required confidence>=0.60 and priced>=2)`,
    weight: 0.08,
  });
  checks.push({
    name: "contradiction_check",
    passed:
      params.diagnostics.contradictions.filter((item) => item.severity === "high").length === 0,
    detail: `high_severity_contradictions=${params.diagnostics.contradictions.filter((item) => item.severity === "high").length} (required = 0)`,
    weight: 0.07,
  });
  checks.push({
    name: "contradiction_resolution",
    passed: mediumContradictions <= 1,
    detail: `medium_severity_contradictions=${mediumContradictions} (required <= 1)`,
    weight: 0.07,
  });
  checks.push({
    name: "falsification_depth",
    passed: params.diagnostics.falsificationTriggers.length >= 4,
    detail: `triggers=${params.diagnostics.falsificationTriggers.length} (required >= 4)`,
    weight: 0.08,
  });
  checks.push({
    name: "portfolio_viability",
    passed:
      params.portfolio.stance !== "insufficient-evidence" &&
      params.portfolio.recommendedWeightPct > 0 &&
      params.portfolio.maxRiskBudgetPct > 0,
    detail: `stance=${params.portfolio.stance} weight=${params.portfolio.recommendedWeightPct.toFixed(2)} risk_budget=${params.portfolio.maxRiskBudgetPct.toFixed(2)} (required actionable stance and positive sizing)`,
    weight: 0.06,
  });

  const totalWeight = checks.reduce((sum, check) => sum + Math.max(0, check.weight), 0);
  const scoreRaw = checks.reduce((sum, check) => sum + (check.passed ? check.weight : 0), 0);
  const score = totalWeight > 1e-9 ? scoreRaw / totalWeight : 0;
  return {
    score,
    checks,
    passed: score >= params.minQualityScore,
  };
};
