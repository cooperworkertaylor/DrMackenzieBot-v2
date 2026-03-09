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
});
