import { runDecisionEval } from "../src/research/eval.js";
import { runAllBenchmarksWithGovernance } from "../src/research/benchmark.js";
import { runLearningCalibration } from "../src/research/learning.js";
import { runPolicyGovernance } from "../src/research/policy.js";

const run = async () => {
  const decision = await runDecisionEval();
  console.log(
    `decision score=${(decision.score * 100).toFixed(1)}% (${decision.passed}/${decision.total})`,
  );

  const learning = runLearningCalibration({ days: 90, minSamples: 3 });
  console.log(
    `forecast sync: unresolved=${learning.forecastResolution.unresolvedCount} resolved_now=${learning.forecastResolution.resolvedNow}`,
  );
  console.log(
    `regrade: scanned=${learning.refresh.scanned} updated=${learning.refresh.updated}`,
  );
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
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
