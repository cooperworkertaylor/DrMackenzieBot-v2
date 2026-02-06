import { runAllBenchmarksWithGovernance } from "../src/research/benchmark.js";
import { runPolicyGovernance } from "../src/research/policy.js";
import { provenanceReport } from "../src/research/provenance.js";
import { runResearchSecurityAudit } from "../src/research/security.js";

const run = async () => {
  const benchmark = runAllBenchmarksWithGovernance({
    lookbackDays: 90,
    mode: "champion_vs_challenger",
  });
  console.log(
    `benchmark suites=${benchmark.suiteCount} runs=${benchmark.runCount} failures=${benchmark.failures}`,
  );
  benchmark.runs.forEach((run) => {
    const champion = run.policySummaries.find((summary) => summary.status === "champion");
    if (!champion) return;
    console.log(
      `benchmark run=${run.runId} suite=${run.suite.name} champion=${champion.policyName} score=${champion.weightedScore.toFixed(3)} completion=${typeof champion.reliabilityCompletionRate === "number" ? `${(champion.reliabilityCompletionRate * 100).toFixed(1)}%` : "n/a"} timeout=${typeof champion.reliabilityTimeoutRate === "number" ? `${(champion.reliabilityTimeoutRate * 100).toFixed(1)}%` : "n/a"} repro=${typeof champion.reliabilityReproducibility === "number" ? `${(champion.reliabilityReproducibility * 100).toFixed(1)}%` : "n/a"} retries=${typeof champion.reliabilityAvgRetries === "number" ? champion.reliabilityAvgRetries.toFixed(2) : "n/a"} canary=${run.gate.canaryBreach ? 1 : 0}`,
    );
  });
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

  const provenance = provenanceReport({ limit: 400 });
  console.log(
    `provenance events=${provenance.totalEvents} chain_valid=${provenance.chainValid ? 1 : 0} signature_coverage=${(provenance.signatureCoverage * 100).toFixed(1)}%`,
  );

  const security = runResearchSecurityAudit();
  console.log(
    `security pass=${security.passCount} warn=${security.warnCount} fail=${security.failCount}`,
  );
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
