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
import { logExecutionTrace } from "./execution-trace.js";
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

const logStableTrace = (params: {
  dbPath: string;
  taskType: "investment" | "coding";
  taskArchetype: string;
  policyName: string;
  seed: string;
  outputText: string;
  success?: boolean;
  retries?: number;
  timeouts?: number;
  errors?: number;
}) => {
  logExecutionTrace({
    taskType: params.taskType,
    taskArchetype: params.taskArchetype,
    policyName: params.policyName,
    policyRole: "primary",
    seed: params.seed,
    outputText: params.outputText,
    success: params.success,
    retryCount: params.retries,
    timeoutCount: params.timeouts,
    errorCount: params.errors,
    dbPath: params.dbPath,
  });
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
      const seed = `quality-seed-${Math.floor(i / 2)}`;
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
      logStableTrace({
        dbPath,
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "champion-v1",
        seed,
        outputText: `champion-output-${seed}`,
        success: true,
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
      logStableTrace({
        dbPath,
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "challenger-v2",
        seed,
        outputText: `challenger-output-${seed}`,
        success: true,
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
      const seed = `baseline-seed-${Math.floor(i / 2)}`;
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
      logStableTrace({
        dbPath,
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "champion-v2",
        seed,
        outputText: `champion-baseline-${seed}`,
        success: true,
      });
    }
    for (let i = 0; i < 4; i += 1) {
      const seed = `baseline-seed-${Math.floor(i / 2)}`;
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
      logStableTrace({
        dbPath,
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "challenger-v3",
        seed,
        outputText: `challenger-baseline-${seed}`,
        success: true,
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
      const seed = `canary-seed-${Math.floor(i / 2)}`;
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
      logStableTrace({
        dbPath,
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "champion-v2",
        seed,
        outputText: `champion-degraded-${seed}`,
        success: true,
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
      logStableTrace({
        dbPath,
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "challenger-v3",
        seed,
        outputText: `challenger-stable-${seed}`,
        success: true,
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
    logStableTrace({
      dbPath,
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "nightly-champion",
      seed: "nightly-seed-0",
      outputText: "nightly-output",
      success: true,
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

  it("blocks promotion when challenger reliability gates fail", () => {
    const dbPath = testDbPath("reliability-promotion-block");
    upsertBenchmarkSuite({
      name: "coding-reliability",
      taskType: "coding",
      taskArchetype: "bugfix",
      gatingMinSamples: 3,
      gatingMinLift: 0.01,
      gatingMaxRiskBreaches: 10,
      reliabilityMinCompletion: 0.9,
      reliabilityMaxTimeoutRate: 0.1,
      reliabilityMinReproducibility: 0.8,
      reliabilityMaxAvgRetries: 1.5,
      dbPath,
    });
    upsertBenchmarkCase({
      suiteName: "coding-reliability",
      taskType: "coding",
      taskArchetype: "bugfix",
      caseName: "reliability-case",
      expected: {
        min_samples: 3,
        min_score: 0.6,
      },
      dbPath,
    });
    registerPolicyVariant({
      taskType: "coding",
      taskArchetype: "bugfix",
      policyName: "champion-safe",
      status: "champion",
      minSamples: 3,
      dbPath,
    });
    registerPolicyVariant({
      taskType: "coding",
      taskArchetype: "bugfix",
      policyName: "challenger-risky",
      status: "challenger",
      minSamples: 3,
      minLift: 0.01,
      dbPath,
    });

    for (let i = 0; i < 4; i += 1) {
      const seed = `rel-seed-${Math.floor(i / 2)}`;
      logTaskOutcome({
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "champion-safe",
        policyRole: "primary",
        outputText: `champion-safe-${i}`,
        userScore: 0.35,
        realizedOutcomeScore: 0.36,
        gradingMetrics: {
          tests_pass_rate: 0.55,
          regressions: 1,
          review_findings: 4,
          rollback_rate: 0.1,
        },
        dbPath,
      });
      logStableTrace({
        dbPath,
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "champion-safe",
        seed,
        outputText: `safe-output-${seed}`,
        success: true,
      });

      logTaskOutcome({
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "challenger-risky",
        policyRole: "primary",
        outputText: `challenger-risky-${i}`,
        userScore: 0.85,
        realizedOutcomeScore: 0.84,
        gradingMetrics: {
          tests_pass_rate: 0.93,
          regressions: 0,
          review_findings: 1,
          rollback_rate: 0,
        },
        dbPath,
      });
      logStableTrace({
        dbPath,
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "challenger-risky",
        seed,
        outputText: `risky-output-${i % 2 === 0 ? "a" : "b"}-${seed}`,
        success: false,
        retries: 3,
        timeouts: 1,
        errors: 1,
      });
    }

    const run = runBenchmarkReplay({
      suiteName: "coding-reliability",
      taskType: "coding",
      taskArchetype: "bugfix",
      lookbackDays: 30,
      dbPath,
    });
    expect(run.gate.promoteCandidate).toBe("challenger-risky");
    expect(run.gate.promoteAllowed).toBe(false);
    expect(run.gate.promoteReliabilityPass).toBe(false);
    expect(run.gate.promoteReason).toContain("reliability gate failed");
  });

  it("triggers canary rollback when champion reliability collapses", () => {
    const dbPath = testDbPath("reliability-rollback");
    upsertBenchmarkSuite({
      name: "investment-reliability",
      taskType: "investment",
      taskArchetype: "deep-dive",
      gatingMinSamples: 3,
      gatingMinLift: 0.5,
      canaryDropThreshold: 0.3,
      reliabilityMinCompletion: 0.9,
      reliabilityMaxTimeoutRate: 0.1,
      reliabilityMinReproducibility: 0.8,
      reliabilityMaxAvgRetries: 1.5,
      dbPath,
    });
    upsertBenchmarkCase({
      suiteName: "investment-reliability",
      taskType: "investment",
      taskArchetype: "deep-dive",
      caseName: "investment-reliability-case",
      expected: {
        min_samples: 3,
        min_score: 0.6,
      },
      dbPath,
    });
    registerPolicyVariant({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "champion-rel-v1",
      status: "champion",
      minSamples: 3,
      dbPath,
    });
    registerPolicyVariant({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "challenger-rel-v2",
      status: "challenger",
      minSamples: 3,
      dbPath,
    });

    for (let i = 0; i < 4; i += 1) {
      const seed = `baseline-seed-${Math.floor(i / 2)}`;
      logTaskOutcome({
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "champion-rel-v1",
        policyRole: "primary",
        outputText: `champion-baseline-${i}`,
        userScore: 0.9,
        realizedOutcomeScore: 0.9,
        citationCount: 10,
        gradingMetrics: {
          contradictions: 0,
          falsification_count: 5,
        },
        dbPath,
      });
      logStableTrace({
        dbPath,
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "champion-rel-v1",
        seed,
        outputText: `champion-baseline-output-${seed}`,
        success: true,
      });

      logTaskOutcome({
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "challenger-rel-v2",
        policyRole: "primary",
        outputText: `challenger-baseline-${i}`,
        userScore: 0.86,
        realizedOutcomeScore: 0.86,
        citationCount: 9,
        gradingMetrics: {
          contradictions: 0,
          falsification_count: 4,
        },
        dbPath,
      });
      logStableTrace({
        dbPath,
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "challenger-rel-v2",
        seed,
        outputText: `challenger-baseline-output-${seed}`,
        success: true,
      });
    }

    const baseline = runBenchmarkReplay({
      suiteName: "investment-reliability",
      taskType: "investment",
      taskArchetype: "deep-dive",
      lookbackDays: 90,
      dbPath,
    });
    expect(baseline.gate.canaryBreach).toBe(false);

    for (let i = 0; i < 4; i += 1) {
      const seed = `degraded-seed-${Math.floor(i / 2)}`;
      logTaskOutcome({
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "champion-rel-v1",
        policyRole: "primary",
        outputText: `champion-degraded-${i}`,
        userScore: 0.89,
        realizedOutcomeScore: 0.9,
        citationCount: 10,
        gradingMetrics: {
          contradictions: 0,
          falsification_count: 5,
        },
        dbPath,
      });
      logStableTrace({
        dbPath,
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "champion-rel-v1",
        seed,
        outputText: `champion-unstable-${i % 2 === 0 ? "x" : "y"}-${seed}`,
        success: false,
        retries: 3,
        timeouts: 1,
        errors: 1,
      });

      logTaskOutcome({
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "challenger-rel-v2",
        policyRole: "primary",
        outputText: `challenger-stable-${i}`,
        userScore: 0.86,
        realizedOutcomeScore: 0.86,
        citationCount: 9,
        gradingMetrics: {
          contradictions: 0,
          falsification_count: 4,
        },
        dbPath,
      });
      logStableTrace({
        dbPath,
        taskType: "investment",
        taskArchetype: "deep-dive",
        policyName: "challenger-rel-v2",
        seed,
        outputText: `challenger-stable-output-${seed}`,
        success: true,
      });
    }

    const canaryRun = runBenchmarkReplay({
      suiteName: "investment-reliability",
      taskType: "investment",
      taskArchetype: "deep-dive",
      lookbackDays: 90,
      dbPath,
    });
    expect(canaryRun.gate.canaryBreach).toBe(true);
    expect(canaryRun.gate.canaryReliabilityBreach).toBe(true);
    expect(canaryRun.gate.rollbackCandidate).toBe("challenger-rel-v2");
  });
});
