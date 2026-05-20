import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  loadResearchAutoresearchProgram,
  RESEARCH_AUTORESEARCH_PROTOCOL,
  runResearchAutoresearchProgram,
} from "./autoresearch-protocol.js";
import { ingestExternalResearchDocument } from "./external-research.js";

const makeTempDir = (name: string) =>
  fs.mkdtempSync(path.join(os.tmpdir(), `openclaw-autoresearch-${name}-`));

describe("research autoresearch protocol", () => {
  it("loads a root-style program.md with relative taskset/profile paths", async () => {
    const dir = makeTempDir("load");
    const programPath = path.join(dir, "program.md");
    const tasksetPath = path.join(dir, "taskset.json");
    const profilePath = path.join(dir, "profile.json");
    fs.writeFileSync(tasksetPath, JSON.stringify({ name: "dummy", tasks: [] }, null, 2), "utf8");
    fs.writeFileSync(
      profilePath,
      JSON.stringify(
        { version: 1, report: { maxSources: 8 }, watchlistBrief: { lookbackDays: 1 } },
        null,
        2,
      ),
      "utf8",
    );
    fs.writeFileSync(
      programPath,
      [
        "---",
        `protocol: ${RESEARCH_AUTORESEARCH_PROTOCOL}`,
        "name: repo-autoresearch",
        "taskset: taskset.json",
        "profile: profile.json",
        "attempts: 9",
        "min-improvement: 0.01",
        "write-best: false",
        "require-pass: true",
        "out: runs/latest.md",
        "---",
        "",
        "# Program",
        "",
        "Only mutate bounded research knobs.",
      ].join("\n"),
      "utf8",
    );

    const program = await loadResearchAutoresearchProgram(programPath);
    expect(program.protocol).toBe(RESEARCH_AUTORESEARCH_PROTOCOL);
    expect(program.tasksetPath).toBe(tasksetPath);
    expect(program.profilePath).toBe(profilePath);
    expect(program.attempts).toBe(9);
    expect(program.minImprovement).toBe(0.01);
    expect(program.writeBest).toBe(false);
    expect(program.requirePass).toBe(true);
    expect(program.outPath).toBe(path.join(dir, "runs/latest.md"));
    expect(program.body).toContain("Only mutate bounded research knobs.");
  });

  it("runs the protocol and writes the rendered report", async () => {
    const dir = makeTempDir("run");
    const dbPath = path.join(dir, "research.db");
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
          `NVDA demand remains strong in source ${i + 1} because accelerator demand supports revenue growth.`,
          `Gross margin could remain above ${70 + i}% while guidance stays constructive in source ${i + 1}.`,
        ].join(" "),
        url: `https://example.com/research/nvda-${i + 1}`,
        publishedAt: `2026-03-0${i + 1}T10:00:00Z`,
      });
    }

    const tasksetPath = path.join(dir, "taskset.json");
    const profilePath = path.join(dir, "profile.json");
    const outPath = path.join(dir, "runs/latest.md");
    const programPath = path.join(dir, "program.md");

    fs.writeFileSync(
      tasksetPath,
      JSON.stringify(
        {
          name: "autoresearch-report",
          thresholds: { minScore: 0.65, maxFailedChecks: 1 },
          tasks: [{ id: "report_nvda", kind: "report", ticker: "NVDA", minSources: 4 }],
        },
        null,
        2,
      ),
      "utf8",
    );
    fs.writeFileSync(
      profilePath,
      JSON.stringify(
        {
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
        null,
        2,
      ),
      "utf8",
    );
    fs.writeFileSync(
      programPath,
      [
        "---",
        `protocol: ${RESEARCH_AUTORESEARCH_PROTOCOL}`,
        "name: repo-autoresearch",
        "description: bounded mutate-run-score-keep loop",
        "taskset: taskset.json",
        "profile: profile.json",
        "attempts: 12",
        "min-improvement: 0.001",
        "seed: increase-max-sources",
        "write-best: true",
        "require-pass: true",
        "out: runs/latest.md",
        "---",
        "",
        "# Program",
        "",
        "Mutate only bounded research knobs and keep winners only if they pass the eval gate.",
      ].join("\n"),
      "utf8",
    );

    const run = await runResearchAutoresearchProgram({
      programPath,
      dbPath,
    });

    expect(run.program.name).toBe("repo-autoresearch");
    expect(run.result.appliedImprovement).toBe(true);
    expect(run.result.best.score).toBeGreaterThan(run.result.baseline.score);
    expect(fs.existsSync(outPath)).toBe(true);
    expect(fs.readFileSync(outPath, "utf8")).toContain("# repo-autoresearch");
  });
});
