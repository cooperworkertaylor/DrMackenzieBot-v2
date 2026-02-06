import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  applyBenchmarkGovernance,
  runAllBenchmarksWithGovernance,
  runBenchmarkReplay,
  upsertBenchmarkCase,
  upsertBenchmarkSuite,
} from "./benchmark.js";
import { openResearchDb } from "./db.js";
import { logTaskOutcome } from "./learning.js";
import { listPolicyVariants, registerPolicyVariant } from "./policy.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-benchmark-${name}-${Date.now()}-${Math.random()}.db`);

const ageByPolicy = (dbPath: string, policyName: string, daysAgo: number) => {
  const db = openResearchDb(dbPath);
  const ts = Date.now() - daysAgo * 86_400_000;
  db.prepare(`UPDATE task_outcomes SET created_at=?, updated_at=? WHERE policy_name=?`).run(
    ts,
    ts,
    policyName,
  );
};

describe("benchmark replay harness", () => {
  it("runs replay and promotes challenger when benchmark gate passes", () => {
    const dbPath = testDbPath("promote");
    upsertBenchmarkSuite({
      name: "investment-core",
      taskType: "investment",
      taskArchetype: "deep-dive",
      gatingMinSamples: 3,
      gatingMinLift: 0.03,
      gatingMaxRiskBreaches: 0,
      dbPath,
    });
    upsertBenchmarkCase({
      suiteName: "investment-core",
      taskType: "investment",
      taskArchetype: "deep-dive",
      caseName: "quality-case",
      expected: {
        min_samples: 3,
        min_score: 0.6,
        min_win_rate: 0.5,
        max_quarantine_rate: 0.3,
      },
      dbPath,
    });
    registerPolicyVariant({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "champion-v1",
      status: "champion",
      minSamples: 3,
      dbPath,
    });
    registerPolicyVariant({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "challenger-v2",
      status: "challenger",
      minSamples: 3,
      minLift: 0.03,
      dbPath,
    });

    for (let i = 0; i < 4; i += 1) {
      logTaskOutcome({
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "champion-v1",
        policyRole: "primary",
        outputText: `champion-${i}`,
        userScore: 0.4,
        realizedOutcomeScore: 0.42,
        citationCount: 4,
        gradingMetrics: {
          contradictions: 1,
          falsification_count: 2,
        },
        dbPath,
      });
      logTaskOutcome({
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "challenger-v2",
        policyRole: "primary",
        outputText: `challenger-${i}`,
        userScore: 0.9,
        realizedOutcomeScore: 0.87,
        citationCount: 10,
        gradingMetrics: {
          contradictions: 0,
          falsification_count: 5,
        },
        dbPath,
      });
    }

    const run = runBenchmarkReplay({
      suiteName: "investment-core",
      taskType: "investment",
      taskArchetype: "deep-dive",
      lookbackDays: 90,
      dbPath,
    });
    expect(run.gate.promoteAllowed).toBe(true);
    expect(run.gate.promoteCandidate).toBe("challenger-v2");

    const decision = applyBenchmarkGovernance({ runId: run.runId, dbPath });
    expect(decision.applied).toBe(true);
    expect(decision.decisionType).toBe("promote");
    const variants = listPolicyVariants({
      taskType: "investment",
      taskArchetype: "deep-dive",
      includeRetired: true,
      dbPath,
    });
    expect(variants.find((variant) => variant.status === "champion")?.policyName).toBe(
      "challenger-v2",
    );
  });

  it("triggers canary rollback when champion quality drops", () => {
    const dbPath = testDbPath("rollback");
    upsertBenchmarkSuite({
      name: "coding-core",
      taskType: "coding",
      taskArchetype: "bugfix",
      gatingMinSamples: 3,
      gatingMinLift: 0.03,
      canaryDropThreshold: 0.05,
      dbPath,
    });
    upsertBenchmarkCase({
      suiteName: "coding-core",
      taskType: "coding",
      taskArchetype: "bugfix",
      caseName: "bugfix-case",
      expected: {
        min_samples: 3,
        min_score: 0.6,
      },
      dbPath,
    });
    registerPolicyVariant({
      taskType: "coding",
      taskArchetype: "bugfix",
      policyName: "champion-v2",
      status: "champion",
      minSamples: 3,
      dbPath,
    });
    registerPolicyVariant({
      taskType: "coding",
      taskArchetype: "bugfix",
      policyName: "challenger-v3",
      status: "challenger",
      minSamples: 3,
      dbPath,
    });

    for (let i = 0; i < 4; i += 1) {
      logTaskOutcome({
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "champion-v2",
        policyRole: "primary",
        outputText: `initial-champion-${i}`,
        userScore: 0.92,
        realizedOutcomeScore: 0.9,
        gradingMetrics: {
          tests_pass_rate: 0.95,
          regressions: 0,
          review_findings: 1,
          rollback_rate: 0,
        },
        dbPath,
      });
    }
    for (let i = 0; i < 4; i += 1) {
      logTaskOutcome({
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "challenger-v3",
        policyRole: "primary",
        outputText: `initial-challenger-${i}`,
        userScore: 0.75,
        realizedOutcomeScore: 0.74,
        gradingMetrics: {
          tests_pass_rate: 0.85,
          regressions: 0,
          review_findings: 2,
          rollback_rate: 0,
        },
        dbPath,
      });
    }
    const baseline = runBenchmarkReplay({
      suiteName: "coding-core",
      taskType: "coding",
      taskArchetype: "bugfix",
      lookbackDays: 90,
      dbPath,
    });
    expect(baseline.gate.canaryBreach).toBe(false);

    ageByPolicy(dbPath, "champion-v2", 45);
    ageByPolicy(dbPath, "challenger-v3", 45);
    for (let i = 0; i < 4; i += 1) {
      logTaskOutcome({
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "champion-v2",
        policyRole: "primary",
        outputText: `degraded-champion-${i}`,
        userScore: 0.15,
        realizedOutcomeScore: 0.18,
        gradingMetrics: {
          tests_pass_rate: 0.35,
          regressions: 2,
          review_findings: 8,
          rollback_rate: 0.3,
        },
        dbPath,
      });
      logTaskOutcome({
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "challenger-v3",
        policyRole: "primary",
        outputText: `stable-challenger-${i}`,
        userScore: 0.72,
        realizedOutcomeScore: 0.74,
        gradingMetrics: {
          tests_pass_rate: 0.88,
          regressions: 0,
          review_findings: 2,
          rollback_rate: 0,
        },
        dbPath,
      });
    }

    const canaryRun = runBenchmarkReplay({
      suiteName: "coding-core",
      taskType: "coding",
      taskArchetype: "bugfix",
      lookbackDays: 14,
      dbPath,
    });
    expect(canaryRun.gate.canaryBreach).toBe(true);
    expect(canaryRun.gate.rollbackCandidate).toBe("challenger-v3");

    const decision = applyBenchmarkGovernance({
      runId: canaryRun.runId,
      dbPath,
    });
    expect(decision.decisionType).toBe("rollback");
    expect(decision.applied).toBe(true);
    const variants = listPolicyVariants({
      taskType: "coding",
      taskArchetype: "bugfix",
      includeRetired: true,
      dbPath,
    });
    expect(variants.find((variant) => variant.status === "champion")?.policyName).toBe(
      "challenger-v3",
    );
  });

  it("runs all active suites with governance", () => {
    const dbPath = testDbPath("nightly");
    upsertBenchmarkSuite({
      name: "investment-nightly",
      taskType: "investment",
      taskArchetype: "deep-dive",
      gatingMinSamples: 1,
      dbPath,
    });
    upsertBenchmarkCase({
      suiteName: "investment-nightly",
      taskType: "investment",
      taskArchetype: "deep-dive",
      caseName: "nightly-case",
      expected: { min_samples: 1, min_score: 0.5 },
      dbPath,
    });
    registerPolicyVariant({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "nightly-champion",
      status: "champion",
      minSamples: 1,
      dbPath,
    });
    logTaskOutcome({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "nightly-champion",
      policyRole: "primary",
      outputText: "nightly-run",
      userScore: 0.8,
      realizedOutcomeScore: 0.82,
      citationCount: 8,
      gradingMetrics: { contradictions: 0, falsification_count: 4 },
      dbPath,
    });

    const nightly = runAllBenchmarksWithGovernance({
      lookbackDays: 30,
      dbPath,
    });
    expect(nightly.suiteCount).toBe(1);
    expect(nightly.runCount).toBe(1);
    expect(nightly.failures).toBe(0);
    expect(nightly.decisions).toHaveLength(1);
  });
});
