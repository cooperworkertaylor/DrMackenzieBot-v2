import type { DatabaseSync } from "node:sqlite";
import fs from "node:fs";
import path from "node:path";

const parseBoolEnv = (value: string | undefined): boolean => {
  if (typeof value !== "string") return false;
  const normalized = value.trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
};

const normalizeRealPath = (value: string): string => {
  const resolved = path.resolve(value.trim());
  try {
    return fs.realpathSync(resolved);
  } catch {
    return resolved;
  }
};

const extensionAllowlist = (defaultPath: string): Set<string> => {
  const out = new Set<string>([normalizeRealPath(defaultPath)]);
  const allowlistRaw =
    process.env.RESEARCH_SQLITE_EXTENSION_ALLOWLIST ??
    process.env.OPENCLAW_SQLITE_EXTENSION_ALLOWLIST ??
    "";
  allowlistRaw
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .forEach((entry) => out.add(normalizeRealPath(entry)));
  return out;
};

export async function loadSqliteVecExtension(params: {
  db: DatabaseSync;
  extensionPath?: string;
}): Promise<{ ok: boolean; extensionPath?: string; error?: string }> {
  try {
    const sqliteVec = await import("sqlite-vec");
    const requestedPath = params.extensionPath?.trim() ? params.extensionPath.trim() : undefined;
    const defaultPath = sqliteVec.getLoadablePath();
    const allowUnsafe = parseBoolEnv(process.env.OPENCLAW_ALLOW_UNSAFE_SQLITE_EXTENSION_PATHS);
    const allowlist = extensionAllowlist(defaultPath);
    const extensionPath = requestedPath ?? defaultPath;
    const normalizedExtensionPath = normalizeRealPath(extensionPath);

    if (!allowUnsafe && !allowlist.has(normalizedExtensionPath)) {
      return {
        ok: false,
        error: `sqlite extension path rejected by allowlist: ${normalizedExtensionPath}`,
      };
    }

    params.db.enableLoadExtension(true);
    if (requestedPath) {
      params.db.loadExtension(normalizedExtensionPath);
    } else {
      sqliteVec.load(params.db);
    }
    params.db.enableLoadExtension(false);

    return { ok: true, extensionPath: normalizedExtensionPath };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    try {
      params.db.enableLoadExtension(false);
    } catch {}
    return { ok: false, error: message };
  }
}
