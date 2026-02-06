import {
  evaluateQualityGateRegression,
  listQualityGateRuns,
  summarizeQualityGateRuns,
  type QualityGateArtifactType,
} from "../src/research/quality-gate.js";

const artifactTypes: QualityGateArtifactType[] = ["memo", "sector_report", "theme_report"];

const run = async () => {
  for (const artifactType of artifactTypes) {
    const runs = listQualityGateRuns({
      artifactType,
      days: 90,
      limit: 1000,
    });
    const summary = summarizeQualityGateRuns(runs);
    console.log(
      `quality_gate_summary artifact=${artifactType} total=${summary.total} passed=${summary.passed} pass_rate=${(summary.passRate * 100).toFixed(1)}% avg_score=${summary.avgScore.toFixed(3)}`,
    );

    const regression = evaluateQualityGateRegression({
      artifactType,
      lookbackDays: 90,
      recentDays: 14,
      minRecentSamples: 8,
      minRecentPassRate: artifactType === "memo" ? 0.82 : 0.75,
      minRecentAvgScore: artifactType === "memo" ? 0.84 : 0.8,
      maxPassRateDrop: 0.1,
      maxAvgScoreDrop: 0.06,
    });
    console.log(
      `quality_gate_regression artifact=${artifactType} passed=${regression.passed ? 1 : 0} recent_count=${regression.recentCount} baseline_count=${regression.baselineCount} recent_pass_rate=${(regression.recentPassRate * 100).toFixed(1)}% baseline_pass_rate=${(regression.baselinePassRate * 100).toFixed(1)}% recent_avg_score=${regression.recentAvgScore.toFixed(3)} baseline_avg_score=${regression.baselineAvgScore.toFixed(3)}`,
    );
    if (!regression.passed) {
      throw new Error(
        `quality_gate_regression_failed artifact=${artifactType} reasons=${regression.reasons.join("; ")}`,
      );
    }
  }
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
