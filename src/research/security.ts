import { openResearchDb } from "./db.js";

export type SecurityControlStatus = "pass" | "warn" | "fail";

export type SecurityControl = {
  id: string;
  status: SecurityControlStatus;
  detail: string;
};

export type ResearchSecurityAudit = {
  generatedAt: string;
  dbPath: string;
  controls: SecurityControl[];
  passCount: number;
  warnCount: number;
  failCount: number;
};

const parseBoolEnv = (value: string | undefined): boolean | undefined => {
  if (typeof value !== "string") return undefined;
  const normalized = value.trim().toLowerCase();
  if (!normalized) return undefined;
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return undefined;
};

export const runResearchSecurityAudit = (
  params: { dbPath?: string } = {},
): ResearchSecurityAudit => {
  const controls: SecurityControl[] = [];
  const db = openResearchDb(params.dbPath);
  const dbPath = params.dbPath ?? `${process.cwd()}/data/research.db`;

  const allowExtensions = parseBoolEnv(process.env.RESEARCH_DB_ALLOW_EXTENSIONS) ?? false;
  controls.push({
    id: "db_extension_loading",
    status: allowExtensions ? "warn" : "pass",
    detail: allowExtensions
      ? "RESEARCH_DB_ALLOW_EXTENSIONS is enabled; verify extension allowlist and path policy."
      : "SQLite extension loading defaults to disabled.",
  });

  const encryptionRequired = parseBoolEnv(process.env.RESEARCH_DB_REQUIRE_ENCRYPTION) ?? false;
  const encryptionKeySet = Boolean(process.env.RESEARCH_DB_KEY?.trim());
  const cipherVersionRow = db.prepare("PRAGMA cipher_version;").get() as
    | { cipher_version?: string }
    | undefined;
  const cipherVersion = cipherVersionRow?.cipher_version?.trim();
  if (encryptionRequired && (!encryptionKeySet || !cipherVersion)) {
    controls.push({
      id: "db_encryption_at_rest",
      status: "fail",
      detail:
        "Encryption required but SQLCipher/key are not fully configured (set RESEARCH_DB_KEY and SQLCipher-enabled sqlite).",
    });
  } else if (encryptionKeySet && cipherVersion) {
    controls.push({
      id: "db_encryption_at_rest",
      status: "pass",
      detail: `SQLCipher enabled (${cipherVersion}).`,
    });
  } else if (encryptionKeySet && !cipherVersion) {
    controls.push({
      id: "db_encryption_at_rest",
      status: "warn",
      detail:
        "RESEARCH_DB_KEY is set but SQLCipher is unavailable; key pragma may be ineffective on this build.",
    });
  } else {
    controls.push({
      id: "db_encryption_at_rest",
      status: "warn",
      detail:
        "Database encryption key is not configured (set RESEARCH_DB_KEY and RESEARCH_DB_REQUIRE_ENCRYPTION for production).",
    });
  }

  controls.push({
    id: "provenance_signing",
    status: process.env.RESEARCH_PROVENANCE_SECRET?.trim() ? "pass" : "warn",
    detail: process.env.RESEARCH_PROVENANCE_SECRET?.trim()
      ? "Provenance HMAC signing key is configured."
      : "Provenance signing key is missing; events remain hash-chained but unsigned.",
  });

  const apiKeyControls: Array<{ name: string; value?: string }> = [
    { name: "ALPHA_VANTAGE_API_KEY", value: process.env.ALPHA_VANTAGE_API_KEY },
    { name: "OPENAI_API_KEY", value: process.env.OPENAI_API_KEY },
  ];
  apiKeyControls.forEach((entry) => {
    controls.push({
      id: `secret_presence_${entry.name.toLowerCase()}`,
      status: entry.value?.trim() ? "pass" : "warn",
      detail: entry.value?.trim()
        ? `${entry.name} is configured.`
        : `${entry.name} is not set (feature paths may degrade or skip).`,
    });
  });

  const passCount = controls.filter((control) => control.status === "pass").length;
  const warnCount = controls.filter((control) => control.status === "warn").length;
  const failCount = controls.filter((control) => control.status === "fail").length;
  return {
    generatedAt: new Date().toISOString(),
    dbPath,
    controls,
    passCount,
    warnCount,
    failCount,
  };
};
