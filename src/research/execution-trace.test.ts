import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  executionReliabilitySummary,
  executionTraceReport,
  logExecutionTrace,
} from "./execution-trace.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-execution-trace-${name}-${Date.now()}-${Math.random()}.db`);

describe("execution trace reliability", () => {
  it("computes completion, timeout, retries, and reproducibility metrics", () => {
    const dbPath = testDbPath("metrics");
    logExecutionTrace({
      taskType: "coding",
      taskArchetype: "bugfix",
      policyName: "policy-a",
      seed: "seed-1",
      outputText: "stable-output",
      success: true,
      retryCount: 0,
      timeoutCount: 0,
      dbPath,
    });
    logExecutionTrace({
      taskType: "coding",
      taskArchetype: "bugfix",
      policyName: "policy-a",
      seed: "seed-1",
      outputText: "stable-output",
      success: true,
      retryCount: 0,
      timeoutCount: 0,
      dbPath,
    });
    logExecutionTrace({
      taskType: "coding",
      taskArchetype: "bugfix",
      policyName: "policy-a",
      seed: "seed-2",
      outputText: "unstable-output",
      success: false,
      retryCount: 2,
      timeoutCount: 1,
      errorCount: 1,
      dbPath,
    });

    const summary = executionReliabilitySummary({
      taskType: "coding",
      taskArchetype: "bugfix",
      policyName: "policy-a",
      lookbackDays: 7,
      dbPath,
    });
    expect(summary.traceCount).toBe(3);
    expect(summary.completionRate).toBeCloseTo(2 / 3, 5);
    expect(summary.timeoutRate).toBeCloseTo(1 / 3, 5);
    expect(summary.avgRetries).toBeCloseTo(2 / 3, 5);
    expect(summary.reproducibilitySampleCount).toBe(2);
    expect(summary.reproducibilityScore).toBeCloseTo(1, 5);

    const report = executionTraceReport({
      taskType: "coding",
      taskArchetype: "bugfix",
      policyName: "policy-a",
      lookbackDays: 7,
      dbPath,
    });
    expect(report.summary.traceCount).toBe(3);
    expect(report.seedStats.map((item) => item.seed)).toContain("seed-1");
  });

  it("marks reproducibility down when same seed produces divergent outputs", () => {
    const dbPath = testDbPath("repro");
    logExecutionTrace({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "policy-b",
      seed: "seed-repro",
      outputText: "output-a",
      success: true,
      dbPath,
    });
    logExecutionTrace({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "policy-b",
      seed: "seed-repro",
      outputText: "output-b",
      success: true,
      dbPath,
    });

    const summary = executionReliabilitySummary({
      taskType: "investment",
      taskArchetype: "deep-dive",
      policyName: "policy-b",
      lookbackDays: 7,
      dbPath,
    });
    expect(summary.traceCount).toBe(2);
    expect(summary.reproducibilitySampleCount).toBe(2);
    expect(summary.reproducibilityScore).toBeCloseTo(0.5, 5);
  });
});
