#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const defaultVault = process.env.OPENCLAW_OP_VAULT?.trim() || "OpenClaw";
const examplePath = path.join(repoRoot, "config", "op-env.example");
const outputPath = path.join(repoRoot, ".env.1password");

const args = process.argv.slice(2);
const vaultIndex = args.indexOf("--vault");
const outputIndex = args.indexOf("--out");
const vaultName = vaultIndex >= 0 ? (args[vaultIndex + 1] || "").trim() || defaultVault : defaultVault;
const outFile = outputIndex >= 0 ? path.resolve(args[outputIndex + 1] || outputPath) : outputPath;

const runOpJson = (opArgs) =>
  JSON.parse(execFileSync("op", opArgs, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }));

const runOp = (opArgs) => execFileSync("op", opArgs, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });

const parseExampleKeys = () => {
  if (!fs.existsSync(examplePath)) return [];
  return fs
    .readFileSync(examplePath, "utf8")
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#") && line.includes("="))
    .map((line) => line.slice(0, line.indexOf("=")).trim())
    .filter(Boolean);
};

const normalizeEnvName = (value) => value.trim().replace(/[^A-Za-z0-9_]/gu, "_");

const chooseSecretField = (item) => {
  const fields = Array.isArray(item.fields) ? item.fields : [];
  const preferredNames = new Set([
    "password",
    "credential",
    "api_key",
    "api key",
    "token",
    "secret",
    "apikey",
  ]);

  const candidates = fields.filter((field) => {
    const value = typeof field?.value === "string" ? field.value.trim() : "";
    return Boolean(value);
  });

  if (candidates.length === 0) return null;

  const preferred = candidates.find((field) => {
    const names = [field.id, field.label]
      .filter((part) => typeof part === "string")
      .map((part) => part.trim().toLowerCase());
    return names.some((name) => preferredNames.has(name));
  });
  if (preferred) return preferred;

  const concealed = candidates.find((field) => `${field.type || ""}`.toUpperCase() === "CONCEALED");
  if (concealed) return concealed;

  return candidates[0];
};

const itemList = runOpJson(["item", "list", "--vault", vaultName, "--format", "json"]);
const itemsByTitle = new Map();
for (const item of itemList) {
  if (typeof item?.title === "string" && item.title.trim()) {
    itemsByTitle.set(item.title.trim(), item);
  }
}

const envNames = new Set(parseExampleKeys());
for (const title of itemsByTitle.keys()) {
  if (/^[A-Z0-9_]+$/u.test(title)) {
    envNames.add(title);
  }
}

const resolvedLines = [];
const missingItems = [];
const missingFields = [];

for (const envName of Array.from(envNames).sort()) {
  const item = itemsByTitle.get(envName);
  if (!item) {
    missingItems.push(envName);
    continue;
  }
  const itemDetail = runOpJson(["item", "get", item.id, "--vault", vaultName, "--format", "json"]);
  const field = chooseSecretField(itemDetail);
  if (!field) {
    missingFields.push(envName);
    continue;
  }
  const fieldRef = (typeof field.id === "string" && field.id.trim()) ||
    (typeof field.label === "string" && field.label.trim());
  if (!fieldRef) {
    missingFields.push(envName);
    continue;
  }
  resolvedLines.push(`${normalizeEnvName(envName)}=op://${vaultName}/${item.title.trim()}/${fieldRef}`);
}

const header = [
  "# Generated from 1Password vault items via scripts/sync-op-env.mjs",
  `# Vault: ${vaultName}`,
  "# Regenerate with: node scripts/sync-op-env.mjs",
  "",
];
fs.writeFileSync(outFile, `${header.join("\n")}${resolvedLines.join("\n")}\n`, "utf8");

console.log(`wrote=${outFile}`);
console.log(`entries=${resolvedLines.length}`);
if (missingItems.length > 0) {
  console.log(`missing_items=${missingItems.length}`);
  for (const key of missingItems) console.log(`MISSING_ITEM ${key}`);
}
if (missingFields.length > 0) {
  console.log(`missing_fields=${missingFields.length}`);
  for (const key of missingFields) console.log(`MISSING_FIELD ${key}`);
}

try {
  const checkOutput = runOp([
    "run",
    "--env-file",
    outFile,
    "--",
    "node",
    "-e",
    "console.log('op_env_sync_check=ok')",
  ]);
  process.stdout.write(checkOutput);
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`validation=failed ${message}`);
  process.exit(1);
}
