import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { resolveStateDir } from "../config/paths.js";

const STORE_VERSION = 1;

export type TelegramLastSentPdfEntry = {
  sha256: string;
  bytes: number;
  fileName?: string;
  sentAt: string;
};

type TelegramLastSentPdfState = {
  version: number;
  chats: Record<string, TelegramLastSentPdfEntry>;
};

function normalizeAccountId(accountId?: string) {
  const trimmed = accountId?.trim();
  if (!trimmed) {
    return "default";
  }
  return trimmed.replace(/[^a-z0-9._-]+/gi, "_");
}

function resolveTelegramLastSentPdfPath(
  accountId?: string,
  env: NodeJS.ProcessEnv = process.env,
): string {
  const stateDir = resolveStateDir(env, os.homedir);
  const normalized = normalizeAccountId(accountId);
  return path.join(stateDir, "telegram", `last-sent-pdfs-${normalized}.json`);
}

function safeParseState(raw: string): TelegramLastSentPdfState | null {
  try {
    const parsed = JSON.parse(raw) as TelegramLastSentPdfState;
    if (!parsed || typeof parsed !== "object") return null;
    if (parsed.version !== STORE_VERSION) return null;
    if (!parsed.chats || typeof parsed.chats !== "object") return null;
    // Best-effort validation (avoid throwing on partial corruption).
    for (const [chatId, entry] of Object.entries(parsed.chats)) {
      if (!chatId) return null;
      if (!entry || typeof entry !== "object") return null;
      const rec = entry as Partial<TelegramLastSentPdfEntry>;
      if (typeof rec.sha256 !== "string" || !rec.sha256.trim()) return null;
      if (typeof rec.bytes !== "number" || !Number.isFinite(rec.bytes) || rec.bytes <= 0)
        return null;
      if (typeof rec.sentAt !== "string" || !rec.sentAt.trim()) return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

export async function readTelegramLastSentPdf(params: {
  accountId?: string;
  chatId: string;
  env?: NodeJS.ProcessEnv;
}): Promise<TelegramLastSentPdfEntry | null> {
  const filePath = resolveTelegramLastSentPdfPath(params.accountId, params.env);
  try {
    const raw = await fs.readFile(filePath, "utf-8");
    const parsed = safeParseState(raw);
    const entry = parsed?.chats?.[String(params.chatId)];
    return entry ?? null;
  } catch (err) {
    const code = (err as { code?: string }).code;
    if (code === "ENOENT") {
      return null;
    }
    return null;
  }
}

export async function writeTelegramLastSentPdf(params: {
  accountId?: string;
  chatId: string;
  sha256: string;
  bytes: number;
  fileName?: string;
  env?: NodeJS.ProcessEnv;
}): Promise<void> {
  const filePath = resolveTelegramLastSentPdfPath(params.accountId, params.env);
  const dir = path.dirname(filePath);
  await fs.mkdir(dir, { recursive: true, mode: 0o700 });

  let nextState: TelegramLastSentPdfState = { version: STORE_VERSION, chats: {} };
  try {
    const raw = await fs.readFile(filePath, "utf-8");
    const parsed = safeParseState(raw);
    if (parsed) {
      nextState = parsed;
    }
  } catch {
    // ignore missing/invalid store
  }

  nextState.chats[String(params.chatId)] = {
    sha256: params.sha256,
    bytes: params.bytes,
    ...(params.fileName?.trim() ? { fileName: params.fileName.trim() } : {}),
    sentAt: new Date().toISOString(),
  };

  const tmp = path.join(dir, `${path.basename(filePath)}.${crypto.randomUUID()}.tmp`);
  await fs.writeFile(tmp, `${JSON.stringify(nextState, null, 2)}\n`, { encoding: "utf-8" });
  await fs.chmod(tmp, 0o600);
  await fs.rename(tmp, filePath);
}
