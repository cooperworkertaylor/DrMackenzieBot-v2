import fs from "node:fs/promises";
import path from "node:path";
import { parseFrontmatterBlock } from "../markdown/frontmatter.js";
import { loadResearchEvalTaskSet, type ResearchEvalTaskSet } from "./eval-harness.js";
import {
  runResearchEvalSelfImproveLoop,
  type ResearchSelfImproveRun,
} from "./eval-self-improve.js";

export const RESEARCH_AUTORESEARCH_PROTOCOL = "research-autoresearch-v1";

export type ResearchAutoresearchProgram = {
  protocol: string;
  name: string;
  description?: string;
  programPath: string;
  tasksetPath: string;
  profilePath: string;
  attempts: number;
  minImprovement: number;
  seed?: string;
  outPath?: string;
  writeBest: boolean;
  requirePass: boolean;
  body: string;
};

export type ResearchAutoresearchRun = {
  program: ResearchAutoresearchProgram;
  taskSet: ResearchEvalTaskSet;
  result: ResearchSelfImproveRun;
  markdown: string;
  outPath?: string;
};

const parseBoolean = (value: string | undefined, fallback: boolean): boolean => {
  if (!value) {
    return fallback;
  }
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
};

const parsePositiveInt = (value: string | undefined, fallback: number): number => {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(1, Math.trunc(parsed));
};

const parseNonNegativeNumber = (value: string | undefined, fallback: number): number => {
  const parsed = Number.parseFloat(value ?? "");
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(0, parsed);
};

const stripFrontmatter = (content: string): string => {
  const normalized = content.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (!normalized.startsWith("---")) {
    return normalized.trim();
  }
  const endIndex = normalized.indexOf("\n---", 3);
  if (endIndex === -1) {
    return normalized.trim();
  }
  return normalized.slice(endIndex + 4).trim();
};

const resolveRelative = (baseDir: string, value: string): string => path.resolve(baseDir, value);

export const loadResearchAutoresearchProgram = async (
  programPath: string,
): Promise<ResearchAutoresearchProgram> => {
  const resolvedProgramPath = path.resolve(programPath);
  const raw = await fs.readFile(resolvedProgramPath, "utf8");
  const frontmatter = parseFrontmatterBlock(raw);
  const baseDir = path.dirname(resolvedProgramPath);
  const protocol = frontmatter.protocol?.trim() || RESEARCH_AUTORESEARCH_PROTOCOL;
  if (protocol !== RESEARCH_AUTORESEARCH_PROTOCOL) {
    throw new Error(
      `Unsupported research autoresearch protocol '${protocol}' in ${resolvedProgramPath}`,
    );
  }
  const tasksetRaw = frontmatter.taskset?.trim();
  const profileRaw = frontmatter.profile?.trim();
  if (!tasksetRaw) {
    throw new Error(
      `program.md is missing required frontmatter key 'taskset': ${resolvedProgramPath}`,
    );
  }
  if (!profileRaw) {
    throw new Error(
      `program.md is missing required frontmatter key 'profile': ${resolvedProgramPath}`,
    );
  }
  return {
    protocol,
    name: frontmatter.name?.trim() || "research-autoresearch",
    description: frontmatter.description?.trim() || undefined,
    programPath: resolvedProgramPath,
    tasksetPath: resolveRelative(baseDir, tasksetRaw),
    profilePath: resolveRelative(baseDir, profileRaw),
    attempts: parsePositiveInt(frontmatter.attempts, 8),
    minImprovement: parseNonNegativeNumber(
      frontmatter["min-improvement"] ?? frontmatter.min_improvement,
      0.005,
    ),
    seed: frontmatter.seed?.trim() || undefined,
    outPath: frontmatter.out?.trim() ? resolveRelative(baseDir, frontmatter.out.trim()) : undefined,
    writeBest: parseBoolean(frontmatter["write-best"] ?? frontmatter.write_best, true),
    requirePass: parseBoolean(frontmatter["require-pass"] ?? frontmatter.require_pass, true),
    body: stripFrontmatter(raw),
  };
};

export const renderResearchAutoresearchRun = (run: ResearchAutoresearchRun): string => {
  const lines: string[] = [];
  lines.push(`# ${run.program.name}`);
  lines.push("");
  if (run.program.description) {
    lines.push(run.program.description);
    lines.push("");
  }
  lines.push("## Protocol");
  lines.push("");
  lines.push(`- Protocol: ${run.program.protocol}`);
  lines.push(`- Program: ${run.program.programPath}`);
  lines.push(`- Task set: ${run.program.tasksetPath}`);
  lines.push(`- Profile: ${run.program.profilePath}`);
  lines.push(`- Attempts: ${run.program.attempts}`);
  lines.push(`- Min improvement: ${run.program.minImprovement.toFixed(3)}`);
  lines.push(`- Write best: ${run.program.writeBest ? "yes" : "no"}`);
  lines.push(`- Require pass: ${run.program.requirePass ? "yes" : "no"}`);
  if (run.program.seed) {
    lines.push(`- Seed: ${run.program.seed}`);
  }
  if (run.program.body) {
    lines.push("");
    lines.push("## Program");
    lines.push("");
    lines.push(run.program.body);
  }
  lines.push("");
  lines.push("## Run");
  lines.push("");
  lines.push(run.result.markdown);
  lines.push("");
  return lines.join("\n");
};

export const runResearchAutoresearchProgram = async (params: {
  programPath: string;
  dbPath?: string;
  attempts?: number;
  minImprovement?: number;
  seed?: string;
  outPath?: string;
  writeBest?: boolean;
  requirePass?: boolean;
}): Promise<ResearchAutoresearchRun> => {
  const program = await loadResearchAutoresearchProgram(params.programPath);
  const taskSet = loadResearchEvalTaskSet(program.tasksetPath);
  const result = await runResearchEvalSelfImproveLoop({
    taskSet,
    profilePath: program.profilePath,
    attempts: params.attempts ?? program.attempts,
    minImprovement: params.minImprovement ?? program.minImprovement,
    seed: params.seed ?? program.seed,
    dbPath: params.dbPath,
    writeBest: params.writeBest ?? program.writeBest,
  });
  if ((params.requirePass ?? program.requirePass) && !result.best.passedGate) {
    throw new Error("research autoresearch run failed the eval gate");
  }
  const run: ResearchAutoresearchRun = {
    program: {
      ...program,
      attempts: params.attempts ?? program.attempts,
      minImprovement: params.minImprovement ?? program.minImprovement,
      seed: params.seed ?? program.seed,
      outPath: params.outPath ? path.resolve(params.outPath) : program.outPath,
      writeBest: params.writeBest ?? program.writeBest,
      requirePass: params.requirePass ?? program.requirePass,
    },
    taskSet,
    result,
    markdown: "",
    outPath: params.outPath ? path.resolve(params.outPath) : program.outPath,
  };
  run.markdown = renderResearchAutoresearchRun(run);
  if (run.outPath) {
    await fs.mkdir(path.dirname(run.outPath), { recursive: true });
    await fs.writeFile(run.outPath, `${run.markdown}\n`, "utf8");
  }
  return run;
};
