import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ingestExternalResearchDocument } from "./external-research.js";
import {
  addTickerToResearchWatchlist,
  upsertResearchWatchlist,
} from "./external-research-watchlists.js";
import {
  evaluateResearchEvalThresholds,
  latestResearchEvalHarnessRuns,
  loadResearchEvalTaskSet,
  renderResearchEvalScorecard,
  runResearchEvalTaskSet,
} from "./eval-harness.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-eval-harness-${name}-${Date.now()}-${Math.random()}.db`);

describe("research eval harness", () => {
  it("loads a task set, evaluates retrieval/report/brief tasks, and persists the run", async () => {
    const dbPath = testDbPath("core");
    process.env.OPENCLAW_RESEARCH_DB_PATH = dbPath;

    const watchlist = upsertResearchWatchlist({
      name: "Core AI Eval",
      description: "eval watchlist",
      isDefault: true,
      dbPath,
    });
    addTickerToResearchWatchlist({
      watchlistId: watchlist.id,
      ticker: "NVDA",
      priority: 1,
      dbPath,
    });

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "analyst@example.com",
      title: "NVDA demand setup",
      subject: "RESEARCH NVDA demand setup",
      ticker: "NVDA",
      content: [
        "NVDA demand remains strong because accelerator demand and pricing discipline continue to support revenue growth.",
        "Gross margin could remain above 70% while guidance stays constructive.",
      ].join(" "),
      url: "https://example.com/research/nvda-demand",
      publishedAt: "2026-03-01T10:00:00Z",
    });
    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "semianalysis",
      sender: "digest@semianalysis.com",
      title: "NVDA competition update",
      subject: "NVDA competition update",
      ticker: "NVDA",
      content: [
        "Competition risk is increasing as custom silicon programs pressure valuation and guidance.",
        "Investors should watch whether margins slip below 70%.",
      ].join(" "),
      url: "https://example.com/research/nvda-competition",
      publishedAt: "2026-03-03T12:00:00Z",
    });

    const taskSetPath = path.join(
      os.tmpdir(),
      `openclaw-eval-taskset-${Date.now()}-${Math.random()}.json`,
    );
    fs.writeFileSync(
      taskSetPath,
      JSON.stringify(
        {
          name: "test-harness",
          thresholds: {
            minScore: 0.75,
            maxFailedChecks: 2,
          },
          tasks: [
            {
              id: "retrieval_nvda",
              kind: "retrieval",
              query: "NVDA pricing discipline guidance competition",
              ticker: "NVDA",
              minHits: 2,
              minCitationRate: 0.5,
            },
            {
              id: "report_nvda",
              kind: "report",
              ticker: "NVDA",
              minSources: 2,
              minEvidence: 1,
              minBullCase: 1,
              minBearCase: 1,
              minWhatChanged: 1,
              minConfidence: 0.2,
            },
            {
              id: "brief_core_ai",
              kind: "watchlist_brief",
              watchlistId: watchlist.id,
              lookbackDays: 7,
              minMaterialChanges: 1,
              minNextActions: 1,
            },
          ],
        },
        null,
        2,
      ),
      "utf8",
    );

    const taskSet = loadResearchEvalTaskSet(taskSetPath);
    const result = await runResearchEvalTaskSet({ taskSet, dbPath });
    expect(result.taskSetName).toBe("test-harness");
    expect(result.total).toBeGreaterThan(0);
    expect(result.score).toBeGreaterThanOrEqual(0.75);
    expect(result.passedGate).toBe(true);
    expect(result.thresholds.minScore).toBe(0.75);
    expect(renderResearchEvalScorecard(result)).toContain("Research Eval Scorecard");

    const runs = latestResearchEvalHarnessRuns({ taskSetName: "test-harness", limit: 5 });
    expect(runs[0]?.run_type).toBe("harness:test-harness");
  });

  it("fails the gate when score or failed-check thresholds are missed", () => {
    const gate = evaluateResearchEvalThresholds({
      taskSet: {
        name: "failing",
        thresholds: {
          minScore: 0.9,
          maxFailedChecks: 0,
        },
        tasks: [],
      },
      result: {
        runType: "harness:failing",
        score: 0.5,
        passed: 2,
        total: 4,
        checks: [
          { name: "a", passed: true, detail: "ok" },
          { name: "b", passed: true, detail: "ok" },
          { name: "c", passed: false, detail: "bad" },
          { name: "d", passed: false, detail: "bad" },
        ],
      },
    });
    expect(gate.passedGate).toBe(false);
    expect(gate.failedChecks).toBe(2);
    expect(gate.reasons.length).toBe(2);
  });
});
