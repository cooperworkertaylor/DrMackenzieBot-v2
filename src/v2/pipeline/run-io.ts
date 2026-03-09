import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const resolveUserPath = (value: string): string => {
  if (!value.trim()) return value;
  if (value.startsWith("~")) {
    return path.resolve(value.replace(/^~(?=$|[\\/])/, os.homedir()));
  }
  return path.resolve(value);
};

export const runsRoot = (): string => {
  const explicit = (process.env.OPENCLAW_RESEARCH_RUNS_DIR ?? "").trim();
  if (explicit) {
    return resolveUserPath(explicit);
  }
  const stateDir =
    (process.env.OPENCLAW_STATE_DIR ?? process.env.CLAWDBOT_STATE_DIR ?? "").trim() ||
    path.join(os.homedir(), ".openclaw");
  return path.join(resolveUserPath(stateDir), "research", "runs");
};

export const runDirFor = (runId: string): string => path.join(runsRoot(), runId);

export async function ensureRunDir(runId: string): Promise<string> {
  const dir = runDirFor(runId);
  await fs.mkdir(dir, { recursive: true });
  return dir;
}

export async function writeRunJson(params: {
  runDir: string;
  filename: string;
  value: unknown;
}): Promise<string> {
  const outPath = path.join(params.runDir, params.filename);
  const json = `${JSON.stringify(params.value, null, 2)}\n`;
  await fs.writeFile(outPath, json, "utf8");
  return outPath;
}

export async function writeRunText(params: {
  runDir: string;
  filename: string;
  text: string;
}): Promise<string> {
  const outPath = path.join(params.runDir, params.filename);
  await fs.writeFile(outPath, `${params.text}\n`, "utf8");
  return outPath;
}
