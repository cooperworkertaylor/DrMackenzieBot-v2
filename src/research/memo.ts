import { createHash } from "node:crypto";
import { openResearchDb } from "./db.js";
import {
  type AdversarialDebateAssessment,
  gradeInstitutionalMemo,
  type MemoCitation,
  type MemoContradiction,
  type MemoDiagnostics,
  type MemoEvidenceClaim,
} from "./grade.js";
import { buildTickerPointInTimeGraph, getTickerPointInTimeSnapshot } from "./knowledge-graph.js";
import { addClaimEvidence, createResearchClaim, upsertResearchEntity } from "./memory-graph.js";
import { composePortfolioDecision } from "./portfolio-decision.js";
import { computePortfolioPlan, type PortfolioPlan } from "./portfolio.js";
import { appendProvenanceEvent } from "./provenance.js";
import { evaluateMemoQualityGate, recordQualityGateRun } from "./quality-gate.js";
import { runAdversarialResearchCell } from "./research-cell.js";
import { computeValuation, recordValuationForecast, type ValuationResult } from "./valuation.js";
import { computeVariantPerception, type VariantPerceptionResult } from "./variant.js";
import { searchResearch } from "./vector-search.js";

type MemoLine = MemoEvidenceClaim;
type CitationRow = MemoCitation;

const buildDiagnostics = (params: {
  variant: VariantPerceptionResult;
  valuation: ValuationResult;
  portfolio: PortfolioPlan;
}): MemoDiagnostics => {
  const contradictions: MemoContradiction[] = [];
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
  const graphBuild = buildTickerPointInTimeGraph({
    ticker: params.ticker,
    dbPath: params.dbPath,
    maxFundamentalFacts: 400,
    maxExpectations: 140,
    maxFilings: 120,
    maxTranscripts: 100,
    maxCatalysts: 100,
  });
  const graphSnapshot = getTickerPointInTimeSnapshot({
    ticker: params.ticker,
    dbPath: params.dbPath,
    lookbackDays: 730,
    eventLimit: 24,
    factLimit: 240,
    metricLimit: 16,
  });
  const researchCell = runAdversarialResearchCell({
    ticker: params.ticker,
    question: params.question,
    claims: lines,
    citations,
    variant,
    valuation,
    portfolio,
    diagnostics,
    graphSnapshot,
  });
  const researchCellAssessment: AdversarialDebateAssessment = {
    coverageScore: researchCell.debate.adversarialCoverageScore,
    dissentCount: researchCell.debate.majorDisagreements.length,
    disconfirmingEvidenceCount: researchCell.debate.disconfirmingEvidence.length,
    riskControlCount: researchCell.debate.riskControls.length,
    unresolvedRiskCount: researchCell.debate.unresolvedRisks.length,
    finalStance: researchCell.allocator.finalStance,
    finalConfidence: researchCell.allocator.confidence,
    passed: researchCell.debate.passed,
  };
  const portfolioDecision = composePortfolioDecision({
    ticker: params.ticker,
    question: params.question,
    portfolio,
    valuation,
    variant,
    researchCell,
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
    `## Point-in-Time Graph`,
    `- Snapshot as-of: ${graphSnapshot.asOfDate}`,
    `- Graph rows scanned: ${graphBuild.rowsScanned}`,
    `- Graph events: ${graphSnapshot.events.length} (inserted=${graphBuild.eventsInserted}, updated=${graphBuild.eventsUpdated})`,
    `- Graph facts: ${graphSnapshot.facts.length} (inserted=${graphBuild.factsInserted}, updated=${graphBuild.factsUpdated})`,
    `- Key metrics:`,
    ...graphSnapshot.metrics.slice(0, 8).map((metric) => {
      if (typeof metric.latestValueNum === "number") {
        return `  - ${metric.metricKey}: latest=${metric.latestValueNum.toFixed(4)}${typeof metric.previousValueNum === "number" ? ` prev=${metric.previousValueNum.toFixed(4)}` : ""}${typeof metric.deltaValueNum === "number" ? ` delta=${metric.deltaValueNum.toFixed(4)}` : ""} as_of=${metric.latestAsOfDate}`;
      }
      return `  - ${metric.metricKey}: latest_text=${metric.latestValueText ?? "n/a"} as_of=${metric.latestAsOfDate}`;
    }),
    `- Recent events:`,
    ...graphSnapshot.events.slice(0, 6).map((event) => {
      return `  - ${new Date(event.eventTime).toISOString().slice(0, 10)} ${event.eventType} (${event.sourceTable}:${event.sourceRefId}) ${event.title || ""}`.trim();
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
    `## Adversarial Research Cell`,
    `- Coverage score: ${researchCell.debate.adversarialCoverageScore.toFixed(2)}`,
    `- Consensus score: ${researchCell.debate.consensusScore.toFixed(2)}`,
    `- Debate passed: ${researchCell.debate.passed ? "yes" : "no"}`,
    `- Final stance: ${researchCell.allocator.finalStance}`,
    `- Final confidence: ${researchCell.allocator.confidence.toFixed(2)}`,
    `- Recommended weight: ${researchCell.allocator.recommendedWeightPct.toFixed(2)}%`,
    `- Max risk budget: ${researchCell.allocator.maxRiskBudgetPct.toFixed(2)}%`,
    `- Stop loss: ${(researchCell.allocator.stopLossPct * 100).toFixed(1)}%`,
    `- Thesis summary: ${researchCell.thesis.summary}`,
    `- Skeptic summary: ${researchCell.skeptic.summary}`,
    `- Risk summary: ${researchCell.riskManager.summary}`,
    `- Allocator summary: ${researchCell.allocator.summary}`,
    `- Disconfirming evidence:`,
    ...researchCell.debate.disconfirmingEvidence
      .slice(0, 6)
      .map((item, index) => `  ${index + 1}. ${item}`),
    `- Major disagreements:`,
    ...(researchCell.debate.majorDisagreements.length
      ? researchCell.debate.majorDisagreements
          .slice(0, 4)
          .map((item, index) => `  ${index + 1}. ${item}`)
      : ["  1. None material."]),
    `- Unresolved risks:`,
    ...(researchCell.debate.unresolvedRisks.length
      ? researchCell.debate.unresolvedRisks
          .slice(0, 4)
          .map((item, index) => `  ${index + 1}. ${item}`)
      : ["  1. None material."]),
    `- Required follow-ups:`,
    ...researchCell.debate.requiredFollowUps
      .slice(0, 4)
      .map((item, index) => `  ${index + 1}. ${item}`),
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
    `## Portfolio Decision Layer`,
    `- Recommendation: ${portfolioDecision.recommendation}`,
    `- Final stance: ${portfolioDecision.finalStance}`,
    `- Decision score: ${portfolioDecision.decisionScore.toFixed(2)}`,
    `- Confidence: ${portfolioDecision.confidence.toFixed(2)}`,
    `- Expected return: ${portfolioDecision.expectedReturnPct.toFixed(2)}%`,
    `- Downside risk: ${portfolioDecision.downsideRiskPct.toFixed(2)}%`,
    `- Risk breaches:`,
    ...(portfolioDecision.riskBreaches.length
      ? portfolioDecision.riskBreaches.map((issue, index) => `  ${index + 1}. ${issue}`)
      : ["  1. None detected."]),
    `- Size candidates:`,
    ...portfolioDecision.sizeCandidates.map(
      (candidate) =>
        `  - ${candidate.label}: recommendation=${candidate.recommendation} weight=${candidate.weightPct.toFixed(2)}% risk_budget=${candidate.riskBudgetPct.toFixed(2)}% expected_pnl=${candidate.expectedPnlPct.toFixed(2)}% downside_pnl=${candidate.downsidePnlPct.toFixed(2)}% score=${candidate.score.toFixed(2)}`,
    ),
    `- Scenario stress:`,
    ...portfolioDecision.stress.map(
      (scenario) =>
        `  - ${scenario.scenario}: probability=${scenario.probability.toFixed(2)} return=${scenario.returnPct.toFixed(2)}% pnl=${scenario.pnlPct.toFixed(2)}% weighted_return=${scenario.weightedReturnPct.toFixed(2)}% risk_breach=${scenario.breachesRiskBudget ? "yes" : "no"}`,
    ),
    ``,
    `## Citation Index`,
    ...citations.map(
      (c) =>
        `- C${c.id}: source=${c.source_table} ref=${c.ref_id}${c.url ? ` url=${c.url}` : ""}${c.metadata ? ` meta=${c.metadata}` : ""}`,
    ),
  ].join("\n");

  const quality = gradeInstitutionalMemo({
    hitsCount: hits.length,
    claims: lines,
    citations,
    variant,
    valuation,
    portfolio,
    diagnostics,
    researchCell: researchCellAssessment,
    minScore: minQualityScore,
    dbPath: params.dbPath,
  });

  const memoGateArtifactId = `${params.ticker.toUpperCase()}:${createHash("sha256")
    .update(
      JSON.stringify({
        ticker: params.ticker.toUpperCase(),
        question: params.question,
        claims: lines.map((line) => line.claim),
        citationIds: lines.flatMap((line) => line.citationIds),
      }),
    )
    .digest("hex")
    .slice(0, 16)}`;
  const qualityGate = evaluateMemoQualityGate({
    artifactId: memoGateArtifactId,
    claims: lines,
    citations,
    diagnostics,
    valuation,
    grade: quality,
    minScore: minQualityScore,
  });
  const qualityGateRun = recordQualityGateRun({
    evaluation: qualityGate,
    metadata: {
      ticker: params.ticker.toUpperCase(),
      question: params.question,
      enforce_institutional_grade: enforceInstitutionalGrade,
    },
    dbPath: params.dbPath,
  });

  if (enforceInstitutionalGrade && !qualityGate.passed) {
    const failed = qualityGate.checks.filter((c) => !c.passed);
    const details = failed.map((f) => `${f.name}: ${f.detail}`).join("; ");
    const requiredFailures =
      qualityGate.requiredFailures.length > 0
        ? ` required_failures=${qualityGate.requiredFailures.join(",")};`
        : "";
    throw new Error(
      `Institutional-grade quality gate failed (score=${qualityGate.score.toFixed(2)} < ${minQualityScore.toFixed(2)};${requiredFailures} run_id=${qualityGateRun.id}): ${details}`,
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
    `- Passed: ${quality.passed ? "yes" : "no"}`,
    `- Required failures: ${quality.requiredFailures.length ? quality.requiredFailures.join(", ") : "none"}`,
    `- Calibration: mode=${quality.calibration.mode} score=${quality.calibration.score.toFixed(2)} sample=${quality.calibration.sampleCount}`,
    `- Actionability score: ${quality.actionabilityScore.toFixed(2)}`,
    `- Adversarial coverage score: ${quality.adversarialCoverageScore.toFixed(2)}`,
    ...quality.checks.map(
      (c) =>
        `- ${c.passed ? "PASS" : "FAIL"} ${c.name}: ${c.detail} (weight ${c.weight}, score ${c.score.toFixed(2)}, required ${c.required ? "yes" : "no"})`,
    ),
    `- Contradictions: ${diagnostics.contradictions.length}`,
    `- Falsification triggers: ${diagnostics.falsificationTriggers.length}`,
    `- Portfolio stance: ${portfolio.stance}`,
    `- Decision recommendation: ${portfolioDecision.recommendation}`,
    `- Decision score: ${portfolioDecision.decisionScore.toFixed(2)}`,
    `- Decision risk breaches: ${portfolioDecision.riskBreaches.length}`,
    "",
    "## Institutional Deliverable Gate",
    `- Gate: ${qualityGate.gateName}`,
    `- Run id: ${qualityGateRun.id}`,
    `- Artifact id: ${qualityGate.artifactId}`,
    `- Score: ${qualityGate.score.toFixed(2)} (threshold ${qualityGate.minScore.toFixed(2)})`,
    `- Passed: ${qualityGate.passed ? "yes" : "no"}`,
    `- Required failures: ${qualityGate.requiredFailures.length ? qualityGate.requiredFailures.join(", ") : "none"}`,
    ...qualityGate.checks.map(
      (c) =>
        `- ${c.passed ? "PASS" : "FAIL"} ${c.name}: ${c.detail} (weight ${c.weight}, score ${c.score.toFixed(2)}, required ${c.required ? "yes" : "no"})`,
    ),
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
        required_failures: quality.requiredFailures,
        institutional_gate: {
          run_id: qualityGateRun.id,
          gate_name: qualityGate.gateName,
          artifact_type: qualityGate.artifactType,
          artifact_id: qualityGate.artifactId,
          score: qualityGate.score,
          min_score: qualityGate.minScore,
          passed: qualityGate.passed,
          required_failures: qualityGate.requiredFailures,
        },
        actionability_score: quality.actionabilityScore,
        adversarial_coverage_score: quality.adversarialCoverageScore,
        research_cell: {
          coverage_score: researchCell.debate.adversarialCoverageScore,
          consensus_score: researchCell.debate.consensusScore,
          passed: researchCell.debate.passed,
          final_stance: researchCell.allocator.finalStance,
          final_confidence: researchCell.allocator.confidence,
          recommended_weight_pct: researchCell.allocator.recommendedWeightPct,
          max_risk_budget_pct: researchCell.allocator.maxRiskBudgetPct,
          disconfirming_evidence_count: researchCell.debate.disconfirmingEvidence.length,
          major_disagreements_count: researchCell.debate.majorDisagreements.length,
          unresolved_risks_count: researchCell.debate.unresolvedRisks.length,
        },
        portfolio_decision: {
          recommendation: portfolioDecision.recommendation,
          final_stance: portfolioDecision.finalStance,
          decision_score: portfolioDecision.decisionScore,
          confidence: portfolioDecision.confidence,
          expected_return_pct: portfolioDecision.expectedReturnPct,
          downside_risk_pct: portfolioDecision.downsideRiskPct,
          risk_breaches: portfolioDecision.riskBreaches,
          size_candidates: portfolioDecision.sizeCandidates.map((candidate) => ({
            label: candidate.label,
            recommendation: candidate.recommendation,
            weight_pct: candidate.weightPct,
            risk_budget_pct: candidate.riskBudgetPct,
            expected_pnl_pct: candidate.expectedPnlPct,
            downside_pnl_pct: candidate.downsidePnlPct,
            score: candidate.score,
          })),
        },
        point_in_time_graph: {
          snapshot_as_of: graphSnapshot.asOfDate,
          rows_scanned: graphBuild.rowsScanned,
          events: graphSnapshot.events.length,
          facts: graphSnapshot.facts.length,
          metrics: graphSnapshot.metrics.length,
        },
        calibration: {
          mode: quality.calibration.mode,
          score: quality.calibration.score,
          sample_count: quality.calibration.sampleCount,
          mae: quality.calibration.mae ?? null,
          directional_accuracy: quality.calibration.directionalAccuracy ?? null,
          confidence_outcome_mae: quality.calibration.confidenceOutcomeMae ?? null,
        },
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
    qualityGate,
    qualityGateRunId: qualityGateRun.id,
    variant,
    valuation,
    portfolio,
    diagnostics,
    researchCell,
    portfolioDecision,
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
