import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ingestExternalResearchDocument } from "./external-research.js";
import {
  loadResearchEvalImprovementProfile,
  runResearchEvalSelfImproveLoop,
  saveResearchEvalImprovementProfile,
} from "./eval-self-improve.js";
import type { ResearchEvalTaskSet } from "./eval-harness.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-eval-self-improve-${name}-${Date.now()}-${Math.random()}.db`);

const makeProfilePath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-eval-profile-${name}-${Date.now()}-${Math.random()}.json`);

describe("research eval self-improve loop", () => {
  it("keeps a better candidate profile and writes it back to disk", async () => {
    const dbPath = testDbPath("improves");
    process.env.OPENCLAW_RESEARCH_DB_PATH = dbPath;

    for (let i = 0; i < 4; i += 1) {
      ingestExternalResearchDocument({
        dbPath,
        sourceType: i % 2 === 0 ? "email_research" : "newsletter",
        provider: i % 2 === 0 ? "other" : "semianalysis",
        sender: "analyst@example.com",
        title: `NVDA source ${i + 1}`,
        subject: `NVDA source ${i + 1}`,
        ticker: "NVDA",
        content: [
          `NVDA demand remains strong in source ${i + 1} because accelerator demand and pricing discipline support revenue growth.`,
          `Gross margin could remain above ${70 + i}% while guidance stays constructive in source ${i + 1}.`,
        ].join(" "),
        url: `https://example.com/research/nvda-source-${i + 1}`,
        publishedAt: `2026-03-0${i + 1}T10:00:00Z`,
      });
    }

    const taskSet: ResearchEvalTaskSet = {
      name: "self-improve-report",
      thresholds: {
        minScore: 1,
        maxFailedChecks: 0,
      },
      tasks: [
        {
          id: "report_nvda",
          kind: "report",
          ticker: "NVDA",
          minSources: 4,
        },
      ],
    };

    const profilePath = makeProfilePath("improves");
    saveResearchEvalImprovementProfile({
      profilePath,
      profile: {
        version: 1,
        report: {
          lookbackDays: 45,
          maxSources: 3,
          maxClaims: 8,
          maxEvents: 6,
          maxFacts: 6,
        },
        watchlistBrief: { lookbackDays: 1 },
      },
    });

    const run = await runResearchEvalSelfImproveLoop({
      taskSet,
      profilePath,
      attempts: 12,
      minImprovement: 0.001,
      seed: "increase-max-sources",
      dbPath,
    });

    expect(run.appliedImprovement).toBe(true);
    expect(run.best.score).toBeGreaterThan(run.baseline.score);
    const saved = loadResearchEvalImprovementProfile(profilePath);
    expect(saved.report?.maxSources).toBeGreaterThanOrEqual(4);
    expect(run.attempts.some((attempt) => attempt.decision === "keep")).toBe(true);
  });

  it("reverts to the original profile when no candidate improves on baseline", async () => {
    const dbPath = testDbPath("reverts");
    process.env.OPENCLAW_RESEARCH_DB_PATH = dbPath;

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "analyst@example.com",
      title: "AMD source",
      subject: "AMD source",
      ticker: "AMD",
      content:
        "AMD competition risk is increasing and investors should watch whether margins slip below 60%.",
      url: "https://example.com/research/amd-source",
      publishedAt: "2026-03-01T10:00:00Z",
    });

    const taskSet: ResearchEvalTaskSet = {
      name: "self-improve-noop",
      thresholds: {
        minScore: 1,
        maxFailedChecks: 0,
      },
      tasks: [
        {
          id: "report_amd",
          kind: "report",
          ticker: "AMD",
          minSources: 1,
        },
      ],
    };

    const profilePath = makeProfilePath("reverts");
    const initialProfile = {
      version: 1 as const,
      report: {
        lookbackDays: 45,
        maxSources: 8,
        maxClaims: 8,
        maxEvents: 6,
        maxFacts: 6,
      },
      watchlistBrief: { lookbackDays: 1 },
    };
    fs.writeFileSync(profilePath, `${JSON.stringify(initialProfile, null, 2)}\n`, "utf8");

    const run = await runResearchEvalSelfImproveLoop({
      taskSet,
      profilePath,
      attempts: 6,
      minImprovement: 0.01,
      seed: "no-improvement",
      dbPath,
    });

    expect(run.appliedImprovement).toBe(false);
    const saved = JSON.parse(fs.readFileSync(profilePath, "utf8")) as typeof initialProfile;
    expect(saved).toEqual(initialProfile);
    expect(run.attempts.every((attempt) => attempt.decision === "revert")).toBe(true);
  });
});
