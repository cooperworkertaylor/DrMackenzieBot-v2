import fs from "node:fs/promises";
import path from "node:path";

export const runsRoot = (): string => path.resolve(process.cwd(), "runs");

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
