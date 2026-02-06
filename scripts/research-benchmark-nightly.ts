import { runAllBenchmarksWithGovernance } from "../src/research/benchmark.js";
import { runPolicyGovernance } from "../src/research/policy.js";

const run = async () => {
  const benchmark = runAllBenchmarksWithGovernance({
    lookbackDays: 90,
    mode: "champion_vs_challenger",
  });
  console.log(
    `benchmark suites=${benchmark.suiteCount} runs=${benchmark.runCount} failures=${benchmark.failures}`,
  );
  benchmark.decisions.forEach((decision) => {
    console.log(
      `benchmark decision run=${decision.runId} type=${decision.decisionType} applied=${decision.applied ? 1 : 0} ${decision.championBefore || "none"} -> ${decision.championAfter || "none"} (${decision.reason})`,
    );
  });

  const policy = runPolicyGovernance({
    days: 60,
    recentDays: 14,
    minSamples: 25,
  });
  console.log(
    `policy promoted=${policy.promoted} rolled_back=${policy.rolledBack} held=${policy.held}`,
  );
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
