import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { learningReport, logTaskOutcome } from "./learning.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-learning-${name}-${Date.now()}-${Math.random()}.db`);

describe("learning loop", () => {
  it("promotes high-quality investment outcomes to trusted", () => {
    const dbPath = testDbPath("trusted");
    const result = logTaskOutcome({
      taskType: "investment",
      taskArchetype: "sector-deep-dive",
      ticker: "MSFT",
      inputSummary: "Deep thesis memo with multiple sources.",
      outputText: "Institutional-grade memo with contradiction handling.",
      confidence: 0.78,
      citationCount: 11,
      userScore: 0.9,
      realizedOutcomeScore: 0.85,
      gradingMetrics: {
        contradictions: 0,
        falsification_count: 5,
        calibration_error: 0.06,
      },
      sourceMix: { filings: 5, transcripts: 3, expectations: 2 },
      dbPath,
    });
    expect(result.status).toBe("trusted");
    expect(result.graderScore).toBeGreaterThan(0.82);
  });

  it("quarantines weak coding outcomes", () => {
    const dbPath = testDbPath("quarantine");
    const result = logTaskOutcome({
      taskType: "coding",
      taskArchetype: "bugfix-hotpath",
      repoRoot: "/tmp/repo",
      inputSummary: "Patch hot path bug.",
      outputText: "Patch with regressions.",
      confidence: 0.85,
      userScore: 0.3,
      gradingMetrics: {
        tests_pass_rate: 0.4,
        regressions: 2,
        review_findings: 9,
        rollback_rate: 0.5,
      },
      dbPath,
    });
    expect(result.status).toBe("quarantine");
    expect(result.graderScore).toBeLessThan(0.55);
  });

  it("updates an existing task outcome and regrades status", () => {
    const dbPath = testDbPath("update");
    const initial = logTaskOutcome({
      taskType: "coding",
      taskArchetype: "refactor",
      inputSummary: "Refactor command parsing.",
      confidence: 0.6,
      gradingMetrics: {
        tests_pass_rate: 0.9,
        regressions: 0,
        review_findings: 1,
        rollback_rate: 0,
      },
      userScore: 0.55,
      dbPath,
    });
    expect(["pending", "trusted"]).toContain(initial.status);
    const updated = logTaskOutcome({
      id: initial.id,
      taskType: "coding",
      userScore: 0.92,
      realizedOutcomeScore: 0.9,
      dbPath,
    });
    expect(updated.id).toBe(initial.id);
    expect(updated.status).toBe("trusted");
    expect(updated.graderScore).toBeGreaterThan(0.82);
  });

  it("builds routing and source recommendations from outcomes", () => {
    const dbPath = testDbPath("report");
    logTaskOutcome({
      taskType: "investment",
      taskArchetype: "deep-dive",
      inputSummary: "A",
      outputText: "A1",
      userScore: 0.9,
      realizedOutcomeScore: 0.85,
      citationCount: 10,
      gradingMetrics: {
        contradictions: 0,
        falsification_count: 5,
      },
      sourceMix: { filings: 5, transcripts: 2 },
      dbPath,
    });
    logTaskOutcome({
      taskType: "investment",
      taskArchetype: "deep-dive",
      inputSummary: "B",
      outputText: "B1",
      userScore: 0.88,
      realizedOutcomeScore: 0.82,
      citationCount: 9,
      gradingMetrics: {
        contradictions: 0,
        falsification_count: 4,
      },
      sourceMix: { filings: 4, expectations: 2 },
      dbPath,
    });
    logTaskOutcome({
      taskType: "investment",
      taskArchetype: "quick-scan",
      inputSummary: "C",
      outputText: "C1",
      userScore: 0.45,
      realizedOutcomeScore: 0.5,
      citationCount: 4,
      gradingMetrics: {
        contradictions: 2,
        falsification_count: 2,
      },
      sourceMix: { transcripts: 3, expectations: 2 },
      dbPath,
    });
    logTaskOutcome({
      taskType: "coding",
      taskArchetype: "bugfix-hotpath",
      inputSummary: "D",
      outputText: "D1",
      userScore: 0.8,
      gradingMetrics: {
        tests_pass_rate: 0.95,
        regressions: 0,
        review_findings: 1,
        rollback_rate: 0,
      },
      sourceMix: { code: 6 },
      dbPath,
    });

    const report = learningReport({ days: 365, minSamples: 2, dbPath });
    expect(report.totalTasks).toBe(4);
    expect(report.byTaskType.find((row) => row.taskType === "investment")?.count).toBe(3);
    const investmentRouting = report.routing.find((row) => row.taskType === "investment");
    expect(investmentRouting?.bestArchetype).toBe("deep-dive");
    expect(report.sourceEffectiveness.length).toBeGreaterThan(0);
    expect(report.sourceEffectiveness[0]?.source).toBe("filings");
  });
});
