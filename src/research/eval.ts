import { openResearchDb } from "./db.js";
import { computeVariantPerception } from "./variant.js";
import { searchResearch, type SearchHit } from "./vector-search.js";

type EvalCheck = { name: string; passed: boolean; detail: string };

const tickersFromEnv = (): string[] =>
  (process.env.RESEARCH_TICKERS ?? "")
    .split(",")
    .map((t) => t.trim().toUpperCase())
    .filter(Boolean);

const average = (values: number[]): number =>
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const precisionAtK = (
  hits: SearchHit[],
  k: number,
  predicate: (hit: SearchHit) => boolean,
): number => {
  const top = hits.slice(0, k);
  if (!top.length) return 0;
  const relevant = top.filter(predicate).length;
  return relevant / top.length;
};

const sourcesAvailableForTicker = (ticker: string) => {
  const db = openResearchDb();
  const rows = db
    .prepare(
      `SELECT c.source_table AS source_table, COUNT(*) AS count
       FROM chunks c
       WHERE
         (c.source_table='filings' AND c.ref_id IN (
            SELECT f.id FROM filings f JOIN instruments i ON i.id=f.instrument_id WHERE i.ticker=?
         ))
         OR
         (c.source_table='transcripts' AND c.ref_id IN (
            SELECT t.id FROM transcripts t JOIN instruments i ON i.id=t.instrument_id WHERE i.ticker=?
         ))
         OR
         (c.source_table='fundamental_facts' AND c.ref_id IN (
            SELECT ff.id FROM fundamental_facts ff JOIN instruments i ON i.id=ff.instrument_id WHERE i.ticker=?
         ))
         OR
         (c.source_table='earnings_expectations' AND c.ref_id IN (
            SELECT ee.id FROM earnings_expectations ee JOIN instruments i ON i.id=ee.instrument_id WHERE i.ticker=?
         ))
       GROUP BY c.source_table`,
    )
    .all(ticker, ticker, ticker, ticker) as Array<{ source_table: string; count: number }>;

  const sourceSet = new Set(rows.filter((row) => row.count > 0).map((row) => row.source_table));
  return {
    filings: sourceSet.has("filings"),
    transcripts: sourceSet.has("transcripts"),
    fundamentals: sourceSet.has("fundamental_facts"),
    expectations: sourceSet.has("earnings_expectations"),
  };
};

export const runFinanceEval = async () => {
  const tickers = tickersFromEnv();
  if (!tickers.length) {
    return persistEval("finance", [
      { name: "tickers", passed: false, detail: "RESEARCH_TICKERS empty" },
    ]);
  }
  const checks: EvalCheck[] = [];
  for (const ticker of tickers) {
    const hits = await searchResearch({
      query: `${ticker} revenue net income free cash flow risk factors`,
      ticker,
      limit: 8,
      source: "research",
    });
    const avgScore = average(hits.map((hit) => hit.score));
    const citationRate = precisionAtK(hits, 6, (hit) => Boolean(hit.citationUrl));
    const uniqueSourceTables = new Set(
      hits.map((hit) => hit.sourceTable).filter((value): value is string => Boolean(value)),
    ).size;
    checks.push({
      name: `evidence_${ticker}`,
      passed: hits.length >= 4,
      detail: `hits=${hits.length}`,
    });
    checks.push({
      name: `score_${ticker}`,
      passed: avgScore >= 0.45,
      detail: `avg_score=${avgScore.toFixed(2)} (threshold=0.45)`,
    });
    checks.push({
      name: `citations_${ticker}`,
      passed: citationRate >= 0.6,
      detail: `citation_precision@6=${citationRate.toFixed(2)} (threshold=0.60)`,
    });
    checks.push({
      name: `source_diversity_${ticker}`,
      passed: uniqueSourceTables >= 2,
      detail: `unique_sources=${uniqueSourceTables} (threshold=2)`,
    });
    const variant = computeVariantPerception({ ticker });
    checks.push({
      name: `variant_confidence_${ticker}`,
      passed: variant.confidence >= 0.55 && variant.stance !== "insufficient-evidence",
      detail: `confidence=${variant.confidence.toFixed(2)} stance=${variant.stance}`,
    });
  }
  return persistEval("finance", checks);
};

export const runRetrievalEval = async () => {
  const tickers = tickersFromEnv();
  if (!tickers.length) {
    return persistEval("retrieval", [
      { name: "tickers", passed: false, detail: "RESEARCH_TICKERS empty" },
    ]);
  }

  const checks: EvalCheck[] = [];
  let topKHits = 0;
  let topKCited = 0;
  let testCases = 0;

  for (const ticker of tickers) {
    const available = sourcesAvailableForTicker(ticker);
    const cases: Array<{ id: string; query: string; expectedSource: string }> = [];
    if (available.fundamentals) {
      cases.push({
        id: "fundamentals",
        query: `${ticker} revenue trend operating margin fundamentals`,
        expectedSource: "fundamental_facts",
      });
    }
    if (available.filings) {
      cases.push({
        id: "filings",
        query: `${ticker} risk factors 10-k filing`,
        expectedSource: "filings",
      });
    }
    if (available.transcripts) {
      cases.push({
        id: "transcripts",
        query: `${ticker} management commentary earnings call transcript`,
        expectedSource: "transcripts",
      });
    }
    if (available.expectations) {
      cases.push({
        id: "expectations",
        query: `${ticker} estimated eps surprise percentage consensus expectations`,
        expectedSource: "earnings_expectations",
      });
    }

    if (!cases.length) {
      checks.push({
        name: `sources_${ticker}`,
        passed: false,
        detail: "No research sources available (run filings/fundamentals/transcript ingestion)",
      });
      continue;
    }

    for (const testCase of cases) {
      const hits = await searchResearch({
        query: testCase.query,
        ticker,
        limit: 8,
        source: "research",
      });
      const top = hits.slice(0, 5);
      const sourcePrecision = precisionAtK(
        top,
        5,
        (hit) => hit.sourceTable === testCase.expectedSource,
      );
      const citationPrecision = precisionAtK(top, 5, (hit) => Boolean(hit.citationUrl));
      const avgScore = average(top.map((hit) => hit.score));
      topKHits += top.length;
      topKCited += top.filter((hit) => Boolean(hit.citationUrl)).length;
      testCases += 1;

      checks.push({
        name: `${ticker}_${testCase.id}_source_precision`,
        passed: sourcePrecision >= 0.4 && top.length >= 3,
        detail: `source_precision@5=${sourcePrecision.toFixed(2)} hits=${top.length} expected_source=${testCase.expectedSource}`,
      });
      checks.push({
        name: `${ticker}_${testCase.id}_citation_precision`,
        passed: citationPrecision >= 0.6,
        detail: `citation_precision@5=${citationPrecision.toFixed(2)} (threshold=0.60)`,
      });
      checks.push({
        name: `${ticker}_${testCase.id}_score`,
        passed: avgScore >= 0.45,
        detail: `avg_top5_score=${avgScore.toFixed(2)} (threshold=0.45)`,
      });
    }
  }

  const globalCitationRate = topKHits > 0 ? topKCited / topKHits : 0;
  checks.push({
    name: "global_citation_coverage",
    passed: globalCitationRate >= 0.7,
    detail: `citation_coverage@5=${globalCitationRate.toFixed(2)} cases=${testCases}`,
  });

  return persistEval("retrieval", checks);
};

export const runCodingEval = async (repoRoot: string) => {
  const checks: EvalCheck[] = [];
  const hits = await searchResearch({
    query: "function error handling tests",
    limit: 8,
    source: "code",
  });
  const avgScore = average(hits.map((hit) => hit.score));
  checks.push({
    name: "repo_search_hits",
    passed: hits.length >= 4,
    detail: `hits=${hits.length} root=${repoRoot}`,
  });
  checks.push({
    name: "repo_search_score",
    passed: avgScore >= 0.35,
    detail: `avg_score=${avgScore.toFixed(2)} (threshold=0.35)`,
  });
  return persistEval("coding", checks);
};

export const latestEvalReport = () => {
  const db = openResearchDb();
  const rows = db
    .prepare(
      `SELECT id, run_type, score, passed, total, details, created_at
       FROM eval_runs
       ORDER BY created_at DESC
       LIMIT 20`,
    )
    .all() as Array<{
    id: number;
    run_type: string;
    score: number;
    passed: number;
    total: number;
    details: string;
    created_at: number;
  }>;
  return rows;
};

const persistEval = (runType: string, checks: EvalCheck[]) => {
  const passed = checks.filter((c) => c.passed).length;
  const total = checks.length;
  const score = total > 0 ? passed / total : 0;
  const db = openResearchDb();
  db.prepare(
    `INSERT INTO eval_runs (run_type, score, passed, total, details, created_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
  ).run(runType, score, passed, total, JSON.stringify(checks), Date.now());
  return { runType, score, passed, total, checks };
};
