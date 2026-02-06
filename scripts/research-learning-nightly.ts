import { runDecisionEval } from "../src/research/eval.js";
import { runLearningCalibration } from "../src/research/learning.js";

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
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
