import fs from "node:fs/promises";
import path from "node:path";
import type { EvidenceItem } from "../evidence/evidence-store.js";
import {
  ingestExpectations,
  ingestFilings,
  ingestFundamentals,
  ingestPrices,
} from "../../research/ingest.js";
import { ingestDefaultMacroFactors } from "../../research/macro-factors.js";
import { syncEmbeddings } from "../../research/vector-search.js";
import { evaluateEvidenceCoverage } from "../evidence/evidence-coverage.js";
import { buildPlanCompanyV2, buildPlanThemeV2, type ResearchPlanV2 } from "./passes/pass0-plan.js";
import { pass1EvidenceCompanyV2, pass1EvidenceThemeV2 } from "./passes/pass1-evidence.js";
import { pass2CompanyAnalyzersV2 } from "./passes/pass2-analyzers-company.js";
import { pass2ThemeAnalyzersV2 } from "./passes/pass2-analyzers-theme.js";
import { pass3RiskOfficerV2 } from "./passes/pass3-risk-officer.js";
import { pass4CompileReportV2 } from "./passes/pass4-compile.js";
import { ensureRunDir, writeRunJson, writeRunText } from "./run-io.js";

const slugify = (value: string): string =>
  value
    .trim()
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, "-")
    .replaceAll(/(^-+|-+$)/g, "")
    .slice(0, 64) || "run";

const runIdDefault = (prefix: string): string => {
  const ts = new Date().toISOString().replaceAll(/[:.]/g, "").replace("T", "-").replace("Z", "Z");
  return `${prefix}-${ts}`.slice(0, 96);
};

const parseBoolean = (raw: string | undefined): boolean | undefined => {
  if (typeof raw !== "string") return undefined;
  const normalized = raw.trim().toLowerCase();
  if (!normalized) return undefined;
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return undefined;
};

const v2HydrateEnabled = (): boolean =>
  parseBoolean(process.env.OPENCLAW_RESEARCH_V2_HYDRATE) ??
  String(process.env.OPENCLAW_HOST_ROLE ?? "")
    .trim()
    .toLowerCase() === "macmini";

const hydrateTickersIfEnabled = async (params: {
  tickers: string[];
  dbPath?: string;
}): Promise<string[]> => {
  if (!v2HydrateEnabled()) return ["disabled"];
  const userAgent =
    process.env.SEC_USER_AGENT?.trim() || process.env.SEC_EDGAR_USER_AGENT?.trim() || undefined;
  const notes: string[] = [];
  const run = async (label: string, task: () => Promise<unknown>) => {
    try {
      await task();
      notes.push(`${label}=ok`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      notes.push(`${label}=skip(${message})`);
    }
  };

  const tickers = Array.from(new Set(params.tickers.map((t) => t.trim().toUpperCase()))).filter(
    Boolean,
  );
  for (const ticker of tickers) {
    await run(`${ticker}:prices`, async () => {
      await ingestPrices(ticker, { dbPath: params.dbPath });
    });
    await run(`${ticker}:fundamentals`, async () => {
      await ingestFundamentals(ticker, { userAgent, dbPath: params.dbPath });
    });
    await run(`${ticker}:expectations`, async () => {
      await ingestExpectations(ticker, { dbPath: params.dbPath });
    });
    await run(`${ticker}:filings`, async () => {
      await ingestFilings(ticker, { limit: 20, userAgent, dbPath: params.dbPath });
    });
  }
  await run("macro:default", async () => {
    await ingestDefaultMacroFactors({ dbPath: params.dbPath });
  });
  await run("embed", async () => {
    await syncEmbeddings(params.dbPath);
  });
  return notes;
};

export type PipelineRunResultV2 = {
  runId: string;
  runDir: string;
  plan: ResearchPlanV2;
  evidence: EvidenceItem[];
  reportJsonPath: string;
  reportMarkdownPath: string;
  passed: boolean;
  issues: Array<{ severity: string; code: string; message: string; path?: string }>;
};

export async function runCompanyPipelineV2(params: {
  ticker: string;
  question: string;
  fixtureDir?: string;
  runId?: string;
  timeboxMinutes?: number;
  dbPath?: string;
}): Promise<PipelineRunResultV2> {
  const ticker = params.ticker.trim().toUpperCase();
  const runId = (params.runId ?? "").trim() || runIdDefault(`v2-company-${slugify(ticker)}`);
  const runDir = await ensureRunDir(runId);

  const hydration = await hydrateTickersIfEnabled({ tickers: [ticker], dbPath: params.dbPath });
  await writeRunJson({
    runDir,
    filename: "Hydration.json",
    value: { enabled: v2HydrateEnabled(), notes: hydration },
  });

  const plan = buildPlanCompanyV2({
    runId,
    ticker,
    question: params.question,
    timeboxMinutes: params.timeboxMinutes,
  });
  await writeRunJson({ runDir, filename: "ResearchPlan.json", value: plan });

  const evidencePass = await pass1EvidenceCompanyV2({
    runDir,
    ticker,
    fixtureDir: params.fixtureDir,
    dbPath: params.dbPath,
  });
  await writeRunJson({ runDir, filename: "EvidenceLibrary.json", value: evidencePass.evidence });
  await writeRunJson({ runDir, filename: "ClaimBacklog.json", value: evidencePass.claim_backlog });
  await writeRunJson({ runDir, filename: "EvidenceCoverage.json", value: evidencePass.coverage });

  const evidenceIssues = evaluateEvidenceCoverage({
    kind: "company",
    sources: evidencePass.evidence,
    subject: { ticker },
  });
  if (evidenceIssues.some((i) => i.severity === "error")) {
    await writeRunJson({
      runDir,
      filename: "FAILED_EVIDENCE.json",
      value: { status: "FAILED_EVIDENCE", issues: evidenceIssues },
    });
    const reportJsonPath = await writeRunJson({
      runDir,
      filename: "FinalReport.json",
      value: { status: "FAILED_EVIDENCE", issues: evidenceIssues },
    });
    const reportMarkdownPath = await writeRunText({
      runDir,
      filename: "FinalReport.md",
      text: [
        "# FAILED EVIDENCE GATE",
        "",
        `Ticker: ${ticker}`,
        "",
        "This run failed before synthesis because primary evidence coverage is insufficient.",
        "",
        ...evidenceIssues.map((i) => `- ${i.code}${i.path ? `@${i.path}` : ""}: ${i.message}`),
      ].join("\n"),
    });
    await writeRunJson({
      runDir,
      filename: "QualityGate.json",
      value: { passed: false, issues: evidenceIssues },
    });
    return {
      runId,
      runDir,
      plan,
      evidence: evidencePass.evidence,
      reportJsonPath,
      reportMarkdownPath,
      passed: false,
      issues: evidenceIssues,
    };
  }

  const analyzers = await pass2CompanyAnalyzersV2({ ticker, evidence: evidencePass.evidence });
  await writeRunJson({ runDir, filename: "Analyzers.company.json", value: analyzers });

  const risk = pass3RiskOfficerV2({ kind: "company", subject: ticker });
  await writeRunJson({ runDir, filename: "RiskOfficer.json", value: risk });

  const compiled = await pass4CompileReportV2({
    kind: "company",
    runId,
    subject: { ticker },
    plan,
    evidence: evidencePass.evidence,
    analyzers,
    risk,
  });
  const reportJsonPath = await writeRunJson({
    runDir,
    filename: "FinalReport.json",
    value: compiled.reportJson,
  });
  const reportMarkdownPath = await writeRunText({
    runDir,
    filename: "FinalReport.md",
    text: compiled.reportMarkdown,
  });

  await writeRunJson({ runDir, filename: "QualityGate.json", value: compiled.gate });

  if (!compiled.gate.passed) {
    await writeRunJson({
      runDir,
      filename: "FAILED_QUALITY_GATE.json",
      value: { status: "FAILED_QUALITY_GATE", issues: compiled.gate.issues },
    });
  } else {
    await fs.rm(path.join(runDir, "FAILED_QUALITY_GATE.json"), { force: true });
  }

  return {
    runId,
    runDir,
    plan,
    evidence: evidencePass.evidence,
    reportJsonPath,
    reportMarkdownPath,
    passed: compiled.gate.passed,
    issues: compiled.gate.issues,
  };
}

export async function runThemePipelineV2(params: {
  themeName: string;
  universe: string[];
  universeEntities?: import("../quality/types.js").ThemeUniverseEntityV2[];
  fixtureDir?: string;
  runId?: string;
  timeboxMinutes?: number;
  dbPath?: string;
}): Promise<PipelineRunResultV2> {
  const themeName = params.themeName.trim();
  const universe = params.universe.map((t) => t.trim().toUpperCase()).filter(Boolean);
  const runId =
    (params.runId ?? "").trim() ||
    runIdDefault(`v2-theme-${slugify(themeName || universe.join("-") || "theme")}`);
  const runDir = await ensureRunDir(runId);

  const hydration = await hydrateTickersIfEnabled({ tickers: universe, dbPath: params.dbPath });
  await writeRunJson({
    runDir,
    filename: "Hydration.json",
    value: { enabled: v2HydrateEnabled(), notes: hydration },
  });

  const plan = buildPlanThemeV2({
    runId,
    themeName,
    universe,
    timeboxMinutes: params.timeboxMinutes,
    universeEntities: params.universeEntities,
  });
  await writeRunJson({ runDir, filename: "ResearchPlan.json", value: plan });

  const evidencePass = await pass1EvidenceThemeV2({
    runDir,
    themeName,
    universe,
    universeEntities: params.universeEntities,
    fixtureDir: params.fixtureDir,
    dbPath: params.dbPath,
  });
  await writeRunJson({ runDir, filename: "EvidenceLibrary.json", value: evidencePass.evidence });
  await writeRunJson({ runDir, filename: "ClaimBacklog.json", value: evidencePass.claim_backlog });
  await writeRunJson({ runDir, filename: "EvidenceCoverage.json", value: evidencePass.coverage });

  const evidenceIssues = evaluateEvidenceCoverage({
    kind: "theme",
    sources: evidencePass.evidence,
    subject: { universe },
  });
  if (evidenceIssues.some((i) => i.severity === "error")) {
    await writeRunJson({
      runDir,
      filename: "FAILED_EVIDENCE.json",
      value: { status: "FAILED_EVIDENCE", issues: evidenceIssues },
    });
    const reportJsonPath = await writeRunJson({
      runDir,
      filename: "FinalReport.json",
      value: { status: "FAILED_EVIDENCE", issues: evidenceIssues },
    });
    const reportMarkdownPath = await writeRunText({
      runDir,
      filename: "FinalReport.md",
      text: [
        "# FAILED EVIDENCE GATE",
        "",
        `Theme: ${themeName}`,
        universe.length ? `Universe: ${universe.join(", ")}` : "Universe: (empty)",
        "",
        "This run failed before synthesis because primary evidence coverage is insufficient.",
        "",
        ...evidenceIssues.map((i) => `- ${i.code}${i.path ? `@${i.path}` : ""}: ${i.message}`),
      ].join("\n"),
    });
    await writeRunJson({
      runDir,
      filename: "QualityGate.json",
      value: { passed: false, issues: evidenceIssues },
    });
    return {
      runId,
      runDir,
      plan,
      evidence: evidencePass.evidence,
      reportJsonPath,
      reportMarkdownPath,
      passed: false,
      issues: evidenceIssues,
    };
  }

  const analyzers = await pass2ThemeAnalyzersV2({
    themeName,
    universe,
    universeEntities: params.universeEntities,
    evidence: evidencePass.evidence,
  });
  await writeRunJson({ runDir, filename: "Analyzers.theme.json", value: analyzers });

  const risk = pass3RiskOfficerV2({ kind: "theme", subject: themeName });
  await writeRunJson({ runDir, filename: "RiskOfficer.json", value: risk });

  const compiled = await pass4CompileReportV2({
    kind: "theme",
    runId,
    subject: { themeName, universe, universeEntities: params.universeEntities },
    plan,
    evidence: evidencePass.evidence,
    analyzers,
    risk,
  });
  const reportJsonPath = await writeRunJson({
    runDir,
    filename: "FinalReport.json",
    value: compiled.reportJson,
  });
  const reportMarkdownPath = await writeRunText({
    runDir,
    filename: "FinalReport.md",
    text: compiled.reportMarkdown,
  });

  await writeRunJson({ runDir, filename: "QualityGate.json", value: compiled.gate });

  if (!compiled.gate.passed) {
    await writeRunJson({
      runDir,
      filename: "FAILED_QUALITY_GATE.json",
      value: { status: "FAILED_QUALITY_GATE", issues: compiled.gate.issues },
    });
  } else {
    await fs.rm(path.join(runDir, "FAILED_QUALITY_GATE.json"), { force: true });
  }

  return {
    runId,
    runDir,
    plan,
    evidence: evidencePass.evidence,
    reportJsonPath,
    reportMarkdownPath,
    passed: compiled.gate.passed,
    issues: compiled.gate.issues,
  };
}
