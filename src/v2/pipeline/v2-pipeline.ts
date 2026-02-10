import fs from "node:fs/promises";
import path from "node:path";
import type { EvidenceItem } from "../evidence/evidence-store.js";
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
  dbPath?: string;
}): Promise<PipelineRunResultV2> {
  const ticker = params.ticker.trim().toUpperCase();
  const runId = (params.runId ?? "").trim() || runIdDefault(`v2-company-${slugify(ticker)}`);
  const runDir = await ensureRunDir(runId);

  const plan = buildPlanCompanyV2({ runId, ticker, question: params.question });
  await writeRunJson({ runDir, filename: "ResearchPlan.json", value: plan });

  const evidencePass = await pass1EvidenceCompanyV2({
    runDir,
    ticker,
    fixtureDir: params.fixtureDir,
    dbPath: params.dbPath,
  });
  await writeRunJson({ runDir, filename: "EvidenceLibrary.json", value: evidencePass.evidence });
  await writeRunJson({ runDir, filename: "ClaimBacklog.json", value: evidencePass.claim_backlog });

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
  fixtureDir?: string;
  runId?: string;
  dbPath?: string;
}): Promise<PipelineRunResultV2> {
  const themeName = params.themeName.trim();
  const universe = params.universe.map((t) => t.trim().toUpperCase()).filter(Boolean);
  const runId =
    (params.runId ?? "").trim() ||
    runIdDefault(`v2-theme-${slugify(themeName || universe.join("-") || "theme")}`);
  const runDir = await ensureRunDir(runId);

  const plan = buildPlanThemeV2({ runId, themeName, universe });
  await writeRunJson({ runDir, filename: "ResearchPlan.json", value: plan });

  const evidencePass = await pass1EvidenceThemeV2({
    runDir,
    themeName,
    universe,
    fixtureDir: params.fixtureDir,
    dbPath: params.dbPath,
  });
  await writeRunJson({ runDir, filename: "EvidenceLibrary.json", value: evidencePass.evidence });
  await writeRunJson({ runDir, filename: "ClaimBacklog.json", value: evidencePass.claim_backlog });

  const analyzers = await pass2ThemeAnalyzersV2({
    themeName,
    universe,
    evidence: evidencePass.evidence,
  });
  await writeRunJson({ runDir, filename: "Analyzers.theme.json", value: analyzers });

  const risk = pass3RiskOfficerV2({ kind: "theme", subject: themeName });
  await writeRunJson({ runDir, filename: "RiskOfficer.json", value: risk });

  const compiled = await pass4CompileReportV2({
    kind: "theme",
    runId,
    subject: { themeName, universe },
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
