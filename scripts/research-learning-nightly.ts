import { runAllBenchmarksWithGovernance } from "../src/research/benchmark.js";
import { runDecisionEval } from "../src/research/eval.js";
import { buildTickerPointInTimeGraph } from "../src/research/knowledge-graph.js";
import { runLearningCalibration } from "../src/research/learning.js";
import { runPolicyGovernance } from "../src/research/policy.js";
import { provenanceReport } from "../src/research/provenance.js";
import { runResearchSecurityAudit } from "../src/research/security.js";

const parseTickers = (value: string): string[] =>
  value
    .split(",")
    .map((ticker) => ticker.trim().toUpperCase())
    .filter(Boolean);

const run = async () => {
  const decision = await runDecisionEval();
  console.log(
    `decision score=${(decision.score * 100).toFixed(1)}% (${decision.passed}/${decision.total})`,
  );

  const learning = runLearningCalibration({ days: 90, minSamples: 3 });
  console.log(
    `forecast sync: unresolved=${learning.forecastResolution.unresolvedCount} resolved_now=${learning.forecastResolution.resolvedNow}`,
  );
  console.log(`regrade: scanned=${learning.refresh.scanned} updated=${learning.refresh.updated}`);
  console.log(
    `learning report: tasks=${learning.report.totalTasks} avg_score=${typeof learning.report.avgGraderScore === "number" ? learning.report.avgGraderScore.toFixed(3) : "n/a"} trusted_rate=${typeof learning.report.trustedRate === "number" ? `${(learning.report.trustedRate * 100).toFixed(1)}%` : "n/a"}`,
  );
  if (learning.report.routing.length) {
    learning.report.routing.forEach((item) => {
      console.log(
        `routing ${item.taskType}: archetype=${item.bestArchetype ?? "n/a"} win=${typeof item.archetypeWinRate === "number" ? `${(item.archetypeWinRate * 100).toFixed(1)}%` : "n/a"} sources=${item.topSources.map((source) => `${source.source}:${source.score.toFixed(3)}`).join(",")}`,
      );
    });
  }

  const benchmark = runAllBenchmarksWithGovernance({
    lookbackDays: 90,
    mode: "champion_vs_challenger",
  });
  console.log(
    `benchmark governance: suites=${benchmark.suiteCount} runs=${benchmark.runCount} failures=${benchmark.failures}`,
  );
  benchmark.runs.slice(0, 20).forEach((run) => {
    const champion = run.policySummaries.find((summary) => summary.status === "champion");
    if (!champion) return;
    console.log(
      `benchmark suite=${run.suite.name} champion=${champion.policyName} score=${champion.weightedScore.toFixed(3)} completion=${typeof champion.reliabilityCompletionRate === "number" ? `${(champion.reliabilityCompletionRate * 100).toFixed(1)}%` : "n/a"} timeout=${typeof champion.reliabilityTimeoutRate === "number" ? `${(champion.reliabilityTimeoutRate * 100).toFixed(1)}%` : "n/a"} repro=${typeof champion.reliabilityReproducibility === "number" ? `${(champion.reliabilityReproducibility * 100).toFixed(1)}%` : "n/a"} retries=${typeof champion.reliabilityAvgRetries === "number" ? champion.reliabilityAvgRetries.toFixed(2) : "n/a"} canary=${run.gate.canaryBreach ? 1 : 0}`,
    );
  });
  benchmark.decisions.slice(0, 20).forEach((decision) => {
    console.log(
      `benchmark run=${decision.runId} ${decision.decisionType} applied=${decision.applied ? 1 : 0} ${decision.championBefore || "none"} -> ${decision.championAfter || "none"} (${decision.reason})`,
    );
  });

  const governance = runPolicyGovernance({
    days: 60,
    recentDays: 14,
    minSamples: 25,
  });
  console.log(
    `policy governance: promoted=${governance.promoted} rolled_back=${governance.rolledBack} held=${governance.held}`,
  );
  governance.decisions.slice(0, 10).forEach((decision) => {
    console.log(
      `${new Date(decision.createdAt).toISOString()} ${decision.decisionType} ${decision.taskType}:${decision.taskArchetype || "default"} ${decision.championBefore || "none"} -> ${decision.championAfter || "none"} (${decision.reason})`,
    );
  });

  const provenance = provenanceReport({ limit: 400 });
  console.log(
    `provenance: events=${provenance.totalEvents} chain_valid=${provenance.chainValid ? 1 : 0} signature_coverage=${(provenance.signatureCoverage * 100).toFixed(1)}%`,
  );
  provenance.issues.slice(0, 20).forEach((issue) => console.log(`provenance_issue: ${issue}`));

  const security = runResearchSecurityAudit();
  console.log(
    `security: pass=${security.passCount} warn=${security.warnCount} fail=${security.failCount}`,
  );

  const tickers = parseTickers(process.env.RESEARCH_TICKERS ?? "");
  if (tickers.length) {
    let totalRows = 0;
    let totalEventsInserted = 0;
    let totalFactsInserted = 0;
    for (const ticker of tickers) {
      const summary = buildTickerPointInTimeGraph({ ticker });
      totalRows += summary.rowsScanned;
      totalEventsInserted += summary.eventsInserted;
      totalFactsInserted += summary.factsInserted;
      console.log(
        `graph ${ticker}: rows=${summary.rowsScanned} events_inserted=${summary.eventsInserted} facts_inserted=${summary.factsInserted}`,
      );
    }
    console.log(
      `graph totals: tickers=${tickers.length} rows=${totalRows} events_inserted=${totalEventsInserted} facts_inserted=${totalFactsInserted}`,
    );
  }
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
