import fs from "node:fs";
import path from "node:path";
import { latestEvalReport, persistEvalRun, type EvalCheck, type EvalRunResult } from "./eval.js";
import { buildExternalResearchStructuredReport } from "./external-research-report.js";
import { getLatestExternalResearchStructuredReport } from "./external-research-report.js";
import { buildDailyWatchlistBrief } from "./external-research-watchlists.js";
import { searchResearch } from "./vector-search.js";

export type RetrievalEvalTask = {
  id: string;
  kind: "retrieval";
  query: string;
  ticker?: string;
  source?: "research" | "code";
  limit?: number;
  minHits?: number;
  minCitationRate?: number;
  minAverageScore?: number;
  expectedSourceTables?: string[];
};

export type ReportEvalTask = {
  id: string;
  kind: "report";
  ticker: string;
  minSources?: number;
  minEvidence?: number;
  minBullCase?: number;
  minBearCase?: number;
  minWhatChanged?: number;
  minConfidence?: number;
  maxUnknowns?: number;
};

export type WatchlistBriefEvalTask = {
  id: string;
  kind: "watchlist_brief";
  watchlistId: number;
  lookbackDays?: number;
  minMaterialChanges?: number;
  minPendingRefreshes?: number;
  minNextActions?: number;
  requireThesisBreaks?: boolean;
};

export type ResearchEvalTask = RetrievalEvalTask | ReportEvalTask | WatchlistBriefEvalTask;

export type ResearchEvalTaskSet = {
  name: string;
  description?: string;
  thresholds?: {
    minScore?: number;
    maxFailedChecks?: number;
  };
  tasks: ResearchEvalTask[];
};

export type ResearchEvalImprovementProfile = {
  version: 1;
  report?: {
    lookbackDays?: number;
    maxSources?: number;
    maxClaims?: number;
    maxEvents?: number;
    maxFacts?: number;
  };
  watchlistBrief?: {
    lookbackDays?: number;
  };
};

export type ResearchEvalHarnessResult = EvalRunResult & {
  taskSetName: string;
  passedGate: boolean;
  failedChecks: number;
  thresholds: {
    minScore: number;
    maxFailedChecks: number;
  };
  reasons: string[];
  scorecard: {
    generatedAt: string;
    taskSetName: string;
    score: number;
    passed: number;
    total: number;
    failedChecks: number;
    passedGate: boolean;
    reasons: string[];
    checks: EvalCheck[];
  };
};

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const average = (values: number[]): number =>
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const precisionAtK = <T>(items: T[], k: number, predicate: (item: T) => boolean): number => {
  const top = items.slice(0, k);
  if (!top.length) return 0;
  return top.filter(predicate).length / top.length;
};

const normalizeTaskId = (value: string): string => value.trim().replace(/\s+/g, "_");

export const loadResearchEvalTaskSet = (taskSetPath: string): ResearchEvalTaskSet => {
  const resolvedPath = path.resolve(taskSetPath);
  const parsed = JSON.parse(fs.readFileSync(resolvedPath, "utf8")) as ResearchEvalTaskSet;
  if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.tasks) || !parsed.name?.trim()) {
    throw new Error(`Invalid research eval task set: ${resolvedPath}`);
  }
  return {
    name: parsed.name.trim(),
    description: parsed.description?.trim(),
    thresholds:
      parsed.thresholds && typeof parsed.thresholds === "object"
        ? {
            minScore:
              typeof parsed.thresholds.minScore === "number"
                ? clamp01(parsed.thresholds.minScore)
                : undefined,
            maxFailedChecks:
              typeof parsed.thresholds.maxFailedChecks === "number"
                ? Math.max(0, Math.floor(parsed.thresholds.maxFailedChecks))
                : undefined,
          }
        : undefined,
    tasks: parsed.tasks.map((task) => {
      if (!task || typeof task !== "object" || typeof task.id !== "string" || typeof task.kind !== "string") {
        throw new Error(`Invalid task in eval task set: ${resolvedPath}`);
      }
      return { ...task, id: normalizeTaskId(task.id) } as ResearchEvalTask;
    }),
  };
};

export const evaluateResearchEvalThresholds = (params: {
  taskSet: ResearchEvalTaskSet;
  result: EvalRunResult;
}) => {
  const thresholds = {
    minScore: clamp01(params.taskSet.thresholds?.minScore ?? 0.8),
    maxFailedChecks: Math.max(0, Math.floor(params.taskSet.thresholds?.maxFailedChecks ?? 0)),
  };
  const failedChecks = params.result.total - params.result.passed;
  const reasons: string[] = [];
  if (params.result.score < thresholds.minScore) {
    reasons.push(
      `score ${params.result.score.toFixed(2)} below threshold ${thresholds.minScore.toFixed(2)}`,
    );
  }
  if (failedChecks > thresholds.maxFailedChecks) {
    reasons.push(`failed_checks ${failedChecks} above threshold ${thresholds.maxFailedChecks}`);
  }
  return {
    thresholds,
    failedChecks,
    passedGate: reasons.length === 0,
    reasons,
  };
};

export const renderResearchEvalScorecard = (result: ResearchEvalHarnessResult): string => {
  const lines = [
    `# Research Eval Scorecard: ${result.taskSetName}`,
    "",
    `- Generated at: ${result.scorecard.generatedAt}`,
    `- Score: ${(result.score * 100).toFixed(1)}%`,
    `- Checks passed: ${result.passed}/${result.total}`,
    `- Failed checks: ${result.failedChecks}`,
    `- Gate: ${result.passedGate ? "PASS" : "FAIL"}`,
    "",
    "## Thresholds",
    "",
    `- Min score: ${(result.thresholds.minScore * 100).toFixed(1)}%`,
    `- Max failed checks: ${result.thresholds.maxFailedChecks}`,
    "",
    "## Reasons",
    "",
    ...(result.reasons.length ? result.reasons.map((reason) => `- ${reason}`) : ["- None."]),
    "",
    "## Checks",
    "",
    ...result.checks.map((check) => `- ${check.passed ? "PASS" : "FAIL"} ${check.name}: ${check.detail}`),
    "",
  ];
  return lines.join("\n");
};

const runRetrievalTask = async (
  task: RetrievalEvalTask,
  dbPath?: string,
): Promise<EvalCheck[]> => {
  const hits = await searchResearch({
    query: task.query,
    ticker: task.ticker,
    source: task.source ?? "research",
    limit: Math.max(1, Math.floor(task.limit ?? 8)),
    dbPath,
  });
  const citationRate = precisionAtK(hits, Math.max(1, Math.min(hits.length, 5)), (hit) => Boolean(hit.citationUrl));
  const avgScore = average(hits.map((hit) => hit.score));
  const checks: EvalCheck[] = [];
  if (typeof task.minHits === "number") {
    checks.push({
      name: `${task.id}:hits`,
      passed: hits.length >= task.minHits,
      detail: `hits=${hits.length} threshold=${task.minHits}`,
    });
  }
  if (typeof task.minCitationRate === "number") {
    checks.push({
      name: `${task.id}:citation_rate`,
      passed: citationRate >= clamp01(task.minCitationRate),
      detail: `citation_rate=${citationRate.toFixed(2)} threshold=${clamp01(task.minCitationRate).toFixed(2)}`,
    });
  }
  if (typeof task.minAverageScore === "number") {
    checks.push({
      name: `${task.id}:avg_score`,
      passed: avgScore >= clamp01(task.minAverageScore),
      detail: `avg_score=${avgScore.toFixed(2)} threshold=${clamp01(task.minAverageScore).toFixed(2)}`,
    });
  }
  if (task.expectedSourceTables?.length) {
    const topSources = new Set(hits.slice(0, 5).map((hit) => hit.sourceTable).filter(Boolean));
    const matched = task.expectedSourceTables.filter((source) => topSources.has(source)).length;
    checks.push({
      name: `${task.id}:expected_sources`,
      passed: matched === task.expectedSourceTables.length,
      detail: `matched=${matched}/${task.expectedSourceTables.length} top_sources=${Array.from(topSources).join(",")}`,
    });
  }
  return checks;
};

const runReportTask = (
  task: ReportEvalTask,
  dbPath?: string,
  profile?: ResearchEvalImprovementProfile,
): EvalCheck[] => {
  const report =
    profile?.report && Object.keys(profile.report).length > 0
      ? buildExternalResearchStructuredReport({
          ticker: task.ticker,
          dbPath,
          lookbackDays: profile.report.lookbackDays,
          maxSources: profile.report.maxSources,
          maxClaims: profile.report.maxClaims,
          maxEvents: profile.report.maxEvents,
          maxFacts: profile.report.maxFacts,
        })
      : getLatestExternalResearchStructuredReport({ ticker: task.ticker, dbPath });
  if (!report) {
    return [
      {
        name: `${task.id}:report_exists`,
        passed: false,
        detail: `missing report for ticker=${task.ticker}`,
      },
    ];
  }
  const checks: EvalCheck[] = [];
  if (typeof task.minSources === "number") {
    checks.push({
      name: `${task.id}:sources`,
      passed: report.sources.length >= task.minSources,
      detail: `sources=${report.sources.length} threshold=${task.minSources}`,
    });
  }
  if (typeof task.minEvidence === "number") {
    checks.push({
      name: `${task.id}:evidence`,
      passed: report.evidence.length >= task.minEvidence,
      detail: `evidence=${report.evidence.length} threshold=${task.minEvidence}`,
    });
  }
  if (typeof task.minBullCase === "number") {
    checks.push({
      name: `${task.id}:bull_case`,
      passed: report.bullCase.length >= task.minBullCase,
      detail: `bull_case=${report.bullCase.length} threshold=${task.minBullCase}`,
    });
  }
  if (typeof task.minBearCase === "number") {
    checks.push({
      name: `${task.id}:bear_case`,
      passed: report.bearCase.length >= task.minBearCase,
      detail: `bear_case=${report.bearCase.length} threshold=${task.minBearCase}`,
    });
  }
  if (typeof task.minWhatChanged === "number") {
    checks.push({
      name: `${task.id}:what_changed`,
      passed: report.whatChanged.length >= task.minWhatChanged,
      detail: `what_changed=${report.whatChanged.length} threshold=${task.minWhatChanged}`,
    });
  }
  if (typeof task.minConfidence === "number") {
    checks.push({
      name: `${task.id}:confidence`,
      passed: report.confidence >= clamp01(task.minConfidence),
      detail: `confidence=${report.confidence.toFixed(2)} threshold=${clamp01(task.minConfidence).toFixed(2)}`,
    });
  }
  if (typeof task.maxUnknowns === "number") {
    checks.push({
      name: `${task.id}:unknowns`,
      passed: report.unknowns.length <= task.maxUnknowns,
      detail: `unknowns=${report.unknowns.length} threshold<=${task.maxUnknowns}`,
    });
  }
  return checks;
};

const runWatchlistBriefTask = (
  task: WatchlistBriefEvalTask,
  dbPath?: string,
  profile?: ResearchEvalImprovementProfile,
): EvalCheck[] => {
  const brief = buildDailyWatchlistBrief({
    watchlistId: task.watchlistId,
    lookbackDays: task.lookbackDays ?? profile?.watchlistBrief?.lookbackDays,
    dbPath,
  });
  const checks: EvalCheck[] = [];
  if (typeof task.minMaterialChanges === "number") {
    checks.push({
      name: `${task.id}:material_changes`,
      passed: brief.materialChanges.length >= task.minMaterialChanges,
      detail: `material_changes=${brief.materialChanges.length} threshold=${task.minMaterialChanges}`,
    });
  }
  if (typeof task.minPendingRefreshes === "number") {
    checks.push({
      name: `${task.id}:pending_refreshes`,
      passed: brief.pendingRefreshes.length >= task.minPendingRefreshes,
      detail: `pending_refreshes=${brief.pendingRefreshes.length} threshold=${task.minPendingRefreshes}`,
    });
  }
  if (typeof task.minNextActions === "number") {
    checks.push({
      name: `${task.id}:next_actions`,
      passed: brief.nextActions.length >= task.minNextActions,
      detail: `next_actions=${brief.nextActions.length} threshold=${task.minNextActions}`,
    });
  }
  if (task.requireThesisBreaks) {
    checks.push({
      name: `${task.id}:thesis_breaks`,
      passed: brief.thesisBreaks.length > 0,
      detail: `thesis_breaks=${brief.thesisBreaks.length} threshold>0`,
    });
  }
  return checks;
};

export const runResearchEvalTaskSet = async (params: {
  taskSet: ResearchEvalTaskSet;
  dbPath?: string;
  profile?: ResearchEvalImprovementProfile;
}): Promise<ResearchEvalHarnessResult> => {
  const checks: EvalCheck[] = [];
  for (const task of params.taskSet.tasks) {
    if (task.kind === "retrieval") {
      checks.push(...(await runRetrievalTask(task, params.dbPath)));
      continue;
    }
    if (task.kind === "report") {
      checks.push(...runReportTask(task, params.dbPath, params.profile));
      continue;
    }
    checks.push(...runWatchlistBriefTask(task, params.dbPath, params.profile));
  }
  const result = persistEvalRun(`harness:${params.taskSet.name}`, checks);
  const gate = evaluateResearchEvalThresholds({
    taskSet: params.taskSet,
    result,
  });
  return {
    ...result,
    taskSetName: params.taskSet.name,
    ...gate,
    scorecard: {
      generatedAt: new Date().toISOString(),
      taskSetName: params.taskSet.name,
      score: result.score,
      passed: result.passed,
      total: result.total,
      failedChecks: gate.failedChecks,
      passedGate: gate.passedGate,
      reasons: gate.reasons,
      checks: result.checks,
    },
  };
};

export const latestResearchEvalHarnessRuns = (params?: {
  taskSetName?: string;
  limit?: number;
}) =>
  latestEvalReport({
    runType: params?.taskSetName ? `harness:${params.taskSetName}` : undefined,
    limit: params?.limit,
  }).filter((row) => row.run_type.startsWith("harness:"));
