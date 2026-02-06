import { openResearchDb } from "./db.js";
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
    .all(...lines.flatMap((l) => l.citationIds)) as Array<{
    id: number;
    source_table: string;
    ref_id: number;
    metadata?: string;
    url?: string;
  }>;
  const byId = new Map(citations.map((c) => [c.id, c]));
  const variant = computeVariantPerception({
    ticker: params.ticker,
    dbPath: params.dbPath,
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
    minQualityScore,
  });

  if (enforceInstitutionalGrade && !quality.passed) {
    const failed = quality.checks.filter((c) => !c.passed);
    const details = failed.map((f) => `${f.name}: ${f.detail}`).join("; ");
    throw new Error(
      `Institutional-grade quality gate failed (score=${quality.score.toFixed(2)} < ${minQualityScore.toFixed(2)}): ${details}`,
    );
  }

  const memoWithQuality = [
    memo,
    "",
    "## Quality Gate",
    `- Score: ${quality.score.toFixed(2)} (threshold ${minQualityScore.toFixed(2)})`,
    ...quality.checks.map(
      (c) => `- ${c.passed ? "PASS" : "FAIL"} ${c.name}: ${c.detail} (weight ${c.weight})`,
    ),
  ].join("\n");

  return {
    memo: memoWithQuality,
    citations: citations.length,
    claims: lines.length,
    quality,
    variant,
  };
};

const summarizeText = (text: string, maxChars: number): string => {
  const cleaned = text.replace(/\s+/g, " ").trim();
  if (cleaned.length <= maxChars) return cleaned;
  const clipped = cleaned.slice(0, maxChars);
  const lastSpace = clipped.lastIndexOf(" ");
  return `${clipped.slice(0, Math.max(0, lastSpace))}...`;
};

const assessInstitutionalQuality = (params: {
  hitsCount: number;
  lines: MemoLine[];
  citations: Array<{ id: number; source_table: string; url?: string }>;
  variant: VariantPerceptionResult;
  minQualityScore: number;
}): QualityGateResult => {
  const checks: QualityGateCheck[] = [];
  const claimCount = params.lines.length;
  const citationsPerClaim =
    claimCount > 0 ? params.lines.reduce((s, l) => s + l.citationIds.length, 0) / claimCount : 0;
  const uniqueUrls = new Set(
    params.citations.map((c) => c.url?.trim()).filter((u): u is string => Boolean(u)),
  ).size;
  const uniqueSourceTables = new Set(params.citations.map((c) => c.source_table)).size;

  checks.push({
    name: "claim_count",
    passed: claimCount >= 4,
    detail: `claims=${claimCount} (required >= 4)`,
    weight: 0.2,
  });
  checks.push({
    name: "citations_per_claim",
    passed: citationsPerClaim >= 2,
    detail: `avg=${citationsPerClaim.toFixed(2)} (required >= 2.00)`,
    weight: 0.25,
  });
  checks.push({
    name: "source_diversity",
    passed: uniqueUrls >= 3 || uniqueSourceTables >= 2,
    detail: `unique_urls=${uniqueUrls}, source_tables=${uniqueSourceTables} (required urls>=3 or sources>=2)`,
    weight: 0.2,
  });
  checks.push({
    name: "retrieval_depth",
    passed: params.hitsCount >= 8,
    detail: `hits=${params.hitsCount} (required >= 8)`,
    weight: 0.15,
  });
  checks.push({
    name: "expectation_coverage",
    passed: params.variant.expectationObservations >= 4,
    detail: `expectation_observations=${params.variant.expectationObservations} (required >= 4)`,
    weight: 0.1,
  });
  checks.push({
    name: "variant_confidence",
    passed: params.variant.confidence >= 0.55,
    detail: `variant_confidence=${params.variant.confidence.toFixed(2)} (required >= 0.55)`,
    weight: 0.1,
  });

  const score = checks.reduce((s, c) => s + (c.passed ? c.weight : 0), 0);
  return {
    score,
    checks,
    passed: score >= params.minQualityScore,
  };
};
