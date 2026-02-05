import fs from "node:fs";
import path from "node:path";

const DEFAULT_MEMU_URL = "http://127.0.0.1:8787";

type MemuRetrieveResult = {
  categories?: unknown[];
  items?: unknown[];
  resources?: unknown[];
};

function isTruthy(value: string | undefined | null): boolean {
  if (!value) {
    return false;
  }
  return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
}

function getMemuUrl(): string {
  return (process.env.OPENCLAW_MEMU_URL || DEFAULT_MEMU_URL).replace(/\/$/, "");
}

export function isGroupLike(ctx: {
  ChatType?: string;
  GroupSubject?: string;
  GroupChannel?: string;
  GroupMembers?: string;
}): boolean {
  const chatType = (ctx.ChatType || "").toLowerCase();
  if (
    chatType.includes("group") ||
    chatType.includes("channel") ||
    chatType.includes("supergroup")
  ) {
    return true;
  }
  return Boolean(ctx.GroupSubject || ctx.GroupChannel || ctx.GroupMembers);
}

export function isSensitiveTurn(params: { commandBody: string; workspaceDir: string }): boolean {
  const text = params.commandBody || "";
  if (/^\s*SENSITIVE\s*:/i.test(text)) {
    return true;
  }

  // Heuristic for Cooper's "B" rule: positions/sizing/PnL or proprietary notes.
  // This is intentionally conservative.
  if (
    /(\bp\s*&\s*l\b|\bpnl\b|\bposition\b|\bshares\b|\bsizing\b|\ballocation\b|\bi (?:bought|sold|trimmed|added)\b|\bmy position\b)/i.test(
      text,
    )
  ) {
    return true;
  }

  // Respect persistent Sensitive Mode toggle in workspace.
  try {
    const togglePath = path.join(params.workspaceDir, "memory", "sensitive-mode.json");
    if (fs.existsSync(togglePath)) {
      const raw = fs.readFileSync(togglePath, "utf-8");
      const parsed = JSON.parse(raw) as { enabled?: boolean };
      if (parsed?.enabled === true) {
        return true;
      }
    }
  } catch {
    // ignore
  }

  return false;
}

function withTimeout(ms: number): AbortController {
  const ctrl = new AbortController();
  setTimeout(() => ctrl.abort(), Math.max(250, ms)).unref?.();
  return ctrl;
}

async function postJson(url: string, body: unknown, timeoutMs: number): Promise<unknown> {
  const ctrl = withTimeout(timeoutMs);
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal: ctrl.signal,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`memu http ${res.status}: ${text.slice(0, 400)}`);
  }
  return await res.json();
}

function summarizeMemuResult(result: MemuRetrieveResult): string {
  const categories = Array.isArray(result.categories) ? result.categories : [];
  const items = Array.isArray(result.items) ? result.items : [];

  // We don't assume exact schema; we try to extract a couple useful strings.
  const pickText = (obj: unknown): string | null => {
    if (!obj || typeof obj !== "object") {
      return null;
    }
    const rec = obj as Record<string, unknown>;
    const candidates = [
      rec.text,
      rec.content,
      rec.summary,
      rec.value,
      rec.name,
      rec.title,
      rec.description,
    ];
    for (const c of candidates) {
      if (typeof c === "string" && c.trim()) {
        return c.trim();
      }
    }
    return null;
  };

  const lines: string[] = [];
  for (const c of categories.slice(0, 4)) {
    const t = pickText(c);
    if (t) {
      lines.push(`- ${t}`);
    }
  }
  for (const it of items.slice(0, 6)) {
    const t = pickText(it);
    if (t) {
      lines.push(`- ${t}`);
    }
  }

  const uniq = Array.from(new Set(lines)).slice(0, 8);
  return uniq.join("\n");
}

export async function memuRetrieveContext(params: {
  commandBody: string;
  scopeUserId: string;
  timeoutMs?: number;
}): Promise<string | null> {
  if (isTruthy(process.env.OPENCLAW_MEMU_DISABLE)) {
    return null;
  }

  const timeoutMs = params.timeoutMs ?? 1200;
  const url = getMemuUrl();

  const payload = {
    queries: [
      {
        role: "user",
        content: { text: params.commandBody.slice(0, 6000) },
      },
    ],
    where: { user_id: params.scopeUserId },
    method: "rag",
  };

  try {
    const raw = await postJson(`${url}/retrieve`, payload, timeoutMs);
    const result = raw as MemuRetrieveResult;
    const summary = summarizeMemuResult(result);
    return summary.trim() ? summary : null;
  } catch {
    return null;
  }
}

export async function memuMemorizeSummary(params: {
  text: string;
  scopeUserId: string;
  meta?: Record<string, unknown>;
  timeoutMs?: number;
}): Promise<void> {
  if (isTruthy(process.env.OPENCLAW_MEMU_DISABLE)) {
    return;
  }
  const timeoutMs = params.timeoutMs ?? 1800;
  const url = getMemuUrl();

  const payload = {
    text: params.text.slice(0, 12000),
    modality: "conversation",
    user: { user_id: params.scopeUserId },
    resource_name: undefined,
    meta: params.meta ?? {},
  };

  try {
    await postJson(`${url}/memorize_text`, payload, timeoutMs);
  } catch {
    // best-effort
  }
}
