import { openResearchDb } from "./db.js";
import { searchResearch } from "./vector-search.js";

type EvalCheck = { name: string; passed: boolean; detail: string };

const tickersFromEnv = (): string[] =>
  (process.env.RESEARCH_TICKERS ?? "")
    .split(",")
    .map((t) => t.trim().toUpperCase())
    .filter(Boolean);

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
      query: `${ticker} revenue net income risk factors`,
      ticker,
      limit: 6,
      source: "research",
    });
    checks.push({
      name: `evidence_${ticker}`,
      passed: hits.length >= 3,
      detail: `hits=${hits.length}`,
    });
  }
  return persistEval("finance", checks);
};

export const runCodingEval = async (repoRoot: string) => {
  const checks: EvalCheck[] = [];
  const hits = await searchResearch({
    query: "function error handling tests",
    limit: 8,
    source: "code",
  });
  checks.push({
    name: "repo_search_hits",
    passed: hits.length >= 4,
    detail: `hits=${hits.length} root=${repoRoot}`,
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
