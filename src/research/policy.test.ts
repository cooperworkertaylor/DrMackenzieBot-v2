import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import { logTaskOutcome } from "./learning.js";
import {
  listPolicyVariants,
  routePolicyAssignment,
  runPolicyGovernance,
  registerPolicyVariant,
} from "./policy.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-policy-${name}-${Date.now()}-${Math.random()}.db`);

const ageOutcomeDays = (dbPath: string, id: number, daysAgo: number) => {
  const db = openResearchDb(dbPath);
  const ts = Date.now() - daysAgo * 86_400_000;
  db.prepare(`UPDATE task_outcomes SET created_at=?, updated_at=? WHERE id=?`).run(ts, ts, id);
};

describe("policy governance", () => {
  it("routes primary and shadow variants from champion/challenger set", () => {
    const dbPath = testDbPath("route");
    registerPolicyVariant({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "policy-a",
      status: "champion",
      trafficWeight: 1,
      shadowWeight: 0,
      dbPath,
    });
    registerPolicyVariant({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "policy-b",
      status: "challenger",
      trafficWeight: 0.5,
      shadowWeight: 1,
      dbPath,
    });

    const baseRoute = routePolicyAssignment({
      taskType: "investment",
      taskArchetype: "deep-dive",
      explorationRate: 0,
      seed: "seed-1",
      dbPath,
    });
    expect(baseRoute.primary?.policyName).toBe("policy-a");
    expect(baseRoute.shadows.map((variant) => variant.policyName)).toContain("policy-b");

    const exploreRoute = routePolicyAssignment({
      taskType: "investment",
      taskArchetype: "deep-dive",
      explorationRate: 1,
      seed: "seed-2",
      dbPath,
    });
    expect(exploreRoute.primary?.policyName).toBe("policy-b");
  });

  it("promotes challenger when it materially outperforms champion", () => {
    const dbPath = testDbPath("promote");
    registerPolicyVariant({
      taskType: "investment",
      taskArchetype: "sector-scan",
      policyName: "champion-v1",
      status: "champion",
      minSamples: 3,
      dbPath,
    });
    registerPolicyVariant({
      taskType: "investment",
      taskArchetype: "sector-scan",
      policyName: "challenger-v2",
      status: "challenger",
      minSamples: 3,
      minLift: 0.03,
      dbPath,
    });

    for (let i = 0; i < 4; i += 1) {
      logTaskOutcome({
        taskType: "investment",
        taskArchetype: "sector-scan",
        policyName: "champion-v1",
        policyRole: "primary",
        outputText: `champion-${i}`,
        userScore: 0.45,
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
        taskArchetype: "sector-scan",
        policyName: "challenger-v2",
        policyRole: "primary",
        outputText: `challenger-${i}`,
        userScore: 0.9,
        realizedOutcomeScore: 0.88,
        citationCount: 10,
        gradingMetrics: {
          contradictions: 0,
          falsification_count: 5,
        },
        dbPath,
      });
    }

    const governance = runPolicyGovernance({
      taskType: "investment",
      taskArchetype: "sector-scan",
      days: 90,
      recentDays: 14,
      minSamples: 3,
      dbPath,
    });
    expect(governance.promoted).toBe(1);
    const variants = listPolicyVariants({
      taskType: "investment",
      taskArchetype: "sector-scan",
      includeRetired: true,
      dbPath,
    });
    expect(variants.find((variant) => variant.status === "champion")?.policyName).toBe(
      "challenger-v2",
    );
  });

  it("rolls back degraded champion when fallback outperformer exists", () => {
    const dbPath = testDbPath("rollback");
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
      minLift: 0.35,
      dbPath,
    });

    const oldChampionIds: number[] = [];
    for (let i = 0; i < 5; i += 1) {
      const row = logTaskOutcome({
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "champion-v2",
        policyRole: "primary",
        outputText: `old-champion-${i}`,
        userScore: 0.9,
        realizedOutcomeScore: 0.9,
        gradingMetrics: {
          tests_pass_rate: 0.95,
          regressions: 0,
          review_findings: 1,
          rollback_rate: 0,
        },
        dbPath,
      });
      oldChampionIds.push(row.id);
    }
    oldChampionIds.forEach((id) => ageOutcomeDays(dbPath, id, 40));

    for (let i = 0; i < 8; i += 1) {
      logTaskOutcome({
        taskType: "coding",
        taskArchetype: "bugfix",
        policyName: "champion-v2",
        policyRole: "primary",
        outputText: `recent-champion-${i}`,
        userScore: 0.22,
        realizedOutcomeScore: 0.2,
        gradingMetrics: {
          tests_pass_rate: 0.4,
          regressions: 2,
          review_findings: 6,
          rollback_rate: 0.2,
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
        outputText: `challenger-fallback-${i}`,
        userScore: 0.76,
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

    const governance = runPolicyGovernance({
      taskType: "coding",
      taskArchetype: "bugfix",
      days: 60,
      recentDays: 14,
      minSamples: 3,
      dbPath,
    });
    expect(governance.rolledBack).toBe(1);
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
});
