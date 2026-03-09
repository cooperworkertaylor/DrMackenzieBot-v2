import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { OpenClawConfig } from "../../config/config.js";
import type { MsgContext } from "../templating.js";
import {
  buildExternalResearchStructuredReport,
  storeExternalResearchStructuredReport,
} from "../../research/external-research-report.js";
import {
  buildExternalResearchThesisFromReport,
  storeExternalResearchThesis,
} from "../../research/external-research-thesis.js";
import { ingestExternalResearchDocument } from "../../research/external-research.js";
import { QuickrunJobStore } from "../../research/quickrun/job-store.js";
import { getStoredResearchExecutionProfile } from "../../research/research-model-profile.js";
import { buildCommandContext, handleCommands } from "./commands.js";
import { parseInlineDirectives } from "./directive-handling.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-research-live-${name}-${Date.now()}-${Math.random()}.db`);

function buildParams(commandBody: string, cfg: OpenClawConfig, ctxOverrides?: Partial<MsgContext>) {
  const ctx = {
    Body: commandBody,
    CommandBody: commandBody,
    CommandSource: "text",
    CommandAuthorized: true,
    Provider: "telegram",
    Surface: "telegram",
    ...ctxOverrides,
  } as MsgContext;

  const command = buildCommandContext({
    ctx,
    cfg,
    isGroup: false,
    triggerBodyNormalized: commandBody.trim().toLowerCase(),
    commandAuthorized: true,
  });

  return {
    ctx,
    cfg,
    command,
    directives: parseInlineDirectives(commandBody),
    elevated: { enabled: true, allowed: true, failures: [] },
    sessionKey: "agent:main:main",
    workspaceDir: os.tmpdir(),
    defaultGroupActivation: () => "mention",
    resolvedVerboseLevel: "off" as const,
    resolvedReasoningLevel: "off" as const,
    resolveDefaultThinkingLevel: async () => undefined,
    provider: "telegram",
    model: "test-model",
    contextTokens: 0,
    isGroup: false,
  };
}

const seedTicker = (params: {
  dbPath: string;
  ticker: string;
  bullishTitle: string;
  bullishContent: string;
  riskTitle: string;
  riskContent: string;
}) => {
  ingestExternalResearchDocument({
    dbPath: params.dbPath,
    sourceType: "email_research",
    provider: "other",
    sender: "analyst@example.com",
    title: params.bullishTitle,
    subject: params.bullishTitle,
    ticker: params.ticker,
    content: params.bullishContent,
    url: `https://example.com/${params.ticker.toLowerCase()}/bull`,
    publishedAt: "2026-03-01T10:00:00Z",
  });

  ingestExternalResearchDocument({
    dbPath: params.dbPath,
    sourceType: "newsletter",
    provider: "semianalysis",
    sender: "digest@semianalysis.com",
    title: params.riskTitle,
    subject: params.riskTitle,
    ticker: params.ticker,
    content: params.riskContent,
    url: `https://example.com/${params.ticker.toLowerCase()}/risk`,
    publishedAt: "2026-03-03T12:00:00Z",
  });

  const report = buildExternalResearchStructuredReport({
    ticker: params.ticker,
    dbPath: params.dbPath,
    lookbackDays: 90,
  });
  const stored = storeExternalResearchStructuredReport({ report, dbPath: params.dbPath });
  storeExternalResearchThesis({
    thesis: buildExternalResearchThesisFromReport({
      report: stored,
      reportId: stored.id,
    }),
    dbPath: params.dbPath,
  });
};

const originalDbPath = process.env.OPENCLAW_RESEARCH_DB_PATH;

afterEach(() => {
  if (typeof originalDbPath === "string") {
    process.env.OPENCLAW_RESEARCH_DB_PATH = originalDbPath;
  } else {
    delete process.env.OPENCLAW_RESEARCH_DB_PATH;
  }
});

describe("live research commands", () => {
  it("returns a thesis summary from stored research state", async () => {
    const dbPath = testDbPath("thesis");
    process.env.OPENCLAW_RESEARCH_DB_PATH = dbPath;
    seedTicker({
      dbPath,
      ticker: "NVDA",
      bullishTitle: "NVDA demand setup",
      bullishContent:
        "NVDA demand remains strong because enterprise AI budgets keep expanding and pricing discipline supports revenue growth near 24%.",
      riskTitle: "NVDA competitive risk",
      riskContent:
        "Competition risk is rising because custom silicon programs could pressure pricing and gross margin if supply normalizes.",
    });

    const cfg = {
      commands: { text: true },
      channels: { telegram: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const result = await handleCommands(buildParams("/thesis NVDA", cfg));

    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("NVDA thesis");
    expect(result.reply?.text).toContain("Stance:");
    expect(result.reply?.text).toContain("Bull case");
    expect(result.reply?.text).toContain("Bear case");
  });

  it("returns top sources and coverage stats", async () => {
    const dbPath = testDbPath("sources");
    process.env.OPENCLAW_RESEARCH_DB_PATH = dbPath;
    seedTicker({
      dbPath,
      ticker: "NVDA",
      bullishTitle: "NVDA earnings setup",
      bullishContent:
        "NVDA earnings setup looks favorable because gross margin could hold around 72% while demand remains strong.",
      riskTitle: "NVDA risk update",
      riskContent:
        "Investors should monitor whether gross margin slips below 70% as competition and supply normalization pressure pricing.",
    });

    const cfg = {
      commands: { text: true },
      channels: { telegram: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const result = await handleCommands(buildParams("/sources NVDA", cfg));

    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("NVDA sources");
    expect(result.reply?.text).toContain("Coverage:");
    expect(result.reply?.text).toContain("semianalysis");
    expect(result.reply?.text).toContain("tier");
  });

  it("returns claim-level provenance for a ticker topic query", async () => {
    const dbPath = testDbPath("why");
    process.env.OPENCLAW_RESEARCH_DB_PATH = dbPath;
    seedTicker({
      dbPath,
      ticker: "NVDA",
      bullishTitle: "NVDA demand setup",
      bullishContent:
        "NVDA demand remains strong because enterprise AI budgets keep expanding and pricing discipline supports revenue growth near 24%.",
      riskTitle: "NVDA competitive risk",
      riskContent:
        "Competition risk is rising because custom silicon programs could pressure pricing and gross margin if supply normalizes.",
    });

    const cfg = {
      commands: { text: true },
      channels: { telegram: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const result = await handleCommands(buildParams("/why NVDA demand", cfg));

    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("NVDA why: demand");
    expect(result.reply?.text).toContain("Claim:");
    expect(result.reply?.text).toContain("Evidence:");
    expect(result.reply?.text).toContain("NVDA demand setup");
  });

  it("compares two peers from stored research state", async () => {
    const dbPath = testDbPath("compare");
    process.env.OPENCLAW_RESEARCH_DB_PATH = dbPath;
    seedTicker({
      dbPath,
      ticker: "NVDA",
      bullishTitle: "NVDA setup",
      bullishContent:
        "NVDA remains constructive because pricing power and enterprise AI demand remain strong.",
      riskTitle: "NVDA risk",
      riskContent:
        "NVDA downside risk is custom silicon competition and supply normalization pressuring pricing.",
    });
    seedTicker({
      dbPath,
      ticker: "AMD",
      bullishTitle: "AMD setup",
      bullishContent:
        "AMD upside depends on share gains in accelerators and improving gross margin through mix.",
      riskTitle: "AMD risk",
      riskContent:
        "AMD remains more execution-sensitive because accelerator adoption must broaden before the thesis is fully supported.",
    });

    const cfg = {
      commands: { text: true },
      channels: { telegram: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const result = await handleCommands(buildParams("/compare NVDA AMD", cfg));

    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("NVDA vs AMD");
    expect(result.reply?.text).toContain("Evidence edge:");
    expect(result.reply?.text).toContain("Risk edge:");
  });

  it("shows usage when ticker arguments are missing", async () => {
    const cfg = {
      commands: { text: true },
      channels: { telegram: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const result = await handleCommands(buildParams("/thesis", cfg));

    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("Usage: /thesis <ticker>");
  });

  it("shows usage when /why is missing a topic", async () => {
    const cfg = {
      commands: { text: true },
      channels: { telegram: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const result = await handleCommands(buildParams("/why NVDA", cfg));

    expect(result.shouldContinue).toBe(false);
    expect(result.reply?.text).toContain("Usage: /why <ticker> <topic>");
  });

  it("persists and reports the research model profile", async () => {
    const dbPath = testDbPath("rprofile");
    process.env.OPENCLAW_RESEARCH_DB_PATH = dbPath;

    const cfg = {
      commands: { text: true },
      channels: { telegram: { allowFrom: ["*"] } },
    } as OpenClawConfig;

    const setResult = await handleCommands(buildParams("/rprofile set primary", cfg));
    expect(setResult.shouldContinue).toBe(false);
    expect(setResult.reply?.text).toContain("Research profile updated.");

    const stored = getStoredResearchExecutionProfile({ dbPath });
    expect(stored?.modelRef).toBe("openai/gpt-5.4");

    const statusResult = await handleCommands(buildParams("/rprofile", cfg));
    expect(statusResult.shouldContinue).toBe(false);
    expect(statusResult.reply?.text).toContain("Research model profile");
    expect(statusResult.reply?.text).toContain("openai/gpt-5.4");
  });

  it("exposes quick research operator commands for the current chat", async () => {
    const dbPath = testDbPath("quickrun-commands");
    process.env.OPENCLAW_RESEARCH_DB_PATH = dbPath;
    const store = QuickrunJobStore.open(dbPath);
    store.enqueue({
      id: "job-run-1",
      jobType: "quick_research_pdf_v2",
      runAfterMs: 0,
      payload: {
        jobId: "job-run-1",
        request: { kind: "company", ticker: "NVDA", minutes: 5 },
        createdAtMs: Date.UTC(2026, 2, 9, 20, 0),
        deliverAtMs: Date.UTC(2026, 2, 9, 20, 5),
        researchProfile: { key: "primary", label: "Primary", modelRef: "openai/gpt-5.4" },
        route: {
          channel: "telegram",
          to: "telegram:123",
          sessionKey: "agent:main:main",
        },
      },
    });
    store.claimNext({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
      nowMs: Date.UTC(2026, 2, 9, 20, 1),
    });
    store.setProgress({
      id: "job-run-1",
      workerId: "worker-a",
      note: "Draft passed quality gate. Rendering PDF.",
      nowMs: Date.UTC(2026, 2, 9, 20, 2),
    });
    store.enqueue({
      id: "job-fail-1",
      jobType: "quick_research_pdf_v2",
      runAfterMs: 0,
      maxAttempts: 1,
      payload: {
        jobId: "job-fail-1",
        request: { kind: "company", ticker: "AMD", minutes: 10 },
        createdAtMs: Date.UTC(2026, 2, 9, 19, 0),
        deliverAtMs: Date.UTC(2026, 2, 9, 19, 10),
        researchProfile: { key: "primary", label: "Primary", modelRef: "openai/gpt-5.4" },
        route: {
          channel: "telegram",
          to: "telegram:123",
          sessionKey: "agent:main:main",
        },
      },
    });
    store.claimNext({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
      nowMs: Date.UTC(2026, 2, 9, 19, 1),
    });
    store.markFailed({
      id: "job-fail-1",
      workerId: "worker-a",
      error: "boom",
      nowMs: Date.UTC(2026, 2, 9, 19, 2),
    });

    const cfg = {
      commands: { text: true },
      channels: { telegram: { allowFrom: ["*"] } },
    } as OpenClawConfig;
    const ctxOverrides = { To: "telegram:123", OriginatingTo: "telegram:123" };

    const statusResult = await handleCommands(buildParams("/qstatus", cfg, ctxOverrides));
    expect(statusResult.shouldContinue).toBe(false);
    expect(statusResult.reply?.text).toContain("Quick status:");
    expect(statusResult.reply?.text).toContain("Draft passed quality gate. Rendering PDF.");

    const lastResult = await handleCommands(buildParams("/qlast", cfg, ctxOverrides));
    expect(lastResult.shouldContinue).toBe(false);
    expect(lastResult.reply?.text).toContain("Recent quick research jobs:");
    expect(lastResult.reply?.text).toContain("job-run-1");
    expect(lastResult.reply?.text).toContain("job-fail-1");

    const retryResult = await handleCommands(buildParams("/qretry job-fail-1", cfg, ctxOverrides));
    expect(retryResult.shouldContinue).toBe(false);
    expect(retryResult.reply?.text).toContain("Requeued quick research job job-fail-1");
  });
});
