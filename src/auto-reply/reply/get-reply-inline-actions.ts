import fsSync from "node:fs";
import os from "node:os";
import type { SkillCommandSpec } from "../../agents/skills.js";
import type { OpenClawConfig } from "../../config/config.js";
import type { SessionEntry } from "../../config/sessions.js";
import type { MsgContext, TemplateContext } from "../templating.js";
import type { ElevatedLevel, ReasoningLevel, ThinkLevel, VerboseLevel } from "../thinking.js";
import type { GetReplyOptions, ReplyPayload } from "../types.js";
import type { InlineDirectives } from "./directive-handling.js";
import type { createModelSelectionState } from "./model-selection.js";
import type { TypingController } from "./typing.js";
import { createOpenClawTools } from "../../agents/openclaw-tools.js";
import { getChannelDock } from "../../channels/dock.js";
import { logVerbose } from "../../globals.js";
import { getLogger } from "../../logging/logger.js";
import { parseQuickResearchRequest } from "../../research/quick-research-request.js";
import { createBackgroundQueue } from "../../research/quickrun/background-queue.js";
import { resolveGatewayMessageChannel } from "../../utils/message-channel.js";
import { listSkillCommandsForWorkspace, resolveSkillCommandInvocation } from "../skill-commands.js";
import { getAbortMemory } from "./abort.js";
import { buildStatusReply, handleCommands } from "./commands.js";
import { isDirectiveOnly } from "./directive-handling.js";
import { extractInlineSimpleCommand } from "./reply-inline.js";

export type InlineActionResult =
  | { kind: "reply"; reply: ReplyPayload | ReplyPayload[] | undefined }
  | {
      kind: "continue";
      directives: InlineDirectives;
      abortedLastRun: boolean;
    };

// oxlint-disable-next-line typescript/no-explicit-any
function extractTextFromToolResult(result: any): string | null {
  if (!result || typeof result !== "object") {
    return null;
  }
  const content = (result as { content?: unknown }).content;
  if (typeof content === "string") {
    const trimmed = content.trim();
    return trimmed ? trimmed : null;
  }
  if (!Array.isArray(content)) {
    return null;
  }

  const parts: string[] = [];
  for (const block of content) {
    if (!block || typeof block !== "object") {
      continue;
    }
    const rec = block as { type?: unknown; text?: unknown };
    if (rec.type === "text" && typeof rec.text === "string") {
      parts.push(rec.text);
    }
  }
  const out = parts.join("");
  const trimmed = out.trim();
  return trimmed ? trimmed : null;
}

const escapeHtml = (value: string): string =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

const slugify = (value: string): string =>
  value
    .trim()
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, "-")
    .replaceAll(/(^-+|-+$)/g, "")
    .slice(0, 64) || "report";

const formatBuiltAtEt = (dt: Date): string => {
  // Example: 2026-02-10 14:38 ET
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(dt);
  const pick = (type: string) => parts.find((p) => p.type === type)?.value ?? "";
  const y = pick("year");
  const m = pick("month");
  const d = pick("day");
  const hh = pick("hour");
  const mm = pick("minute");
  return `${y}-${m}-${d} ${hh}:${mm} ET`;
};

const sanitizeThemeLabel = (value: string): string => {
  const raw = value.trim();
  // Be defensive: sometimes the message body includes a transport prefix like:
  //   "[Telegram ... id:... ...] optical networking"
  // If it looks like that, strip the bracketed prefix.
  const m = raw.match(/^\s*\[([^\]]{1,800})\]\s*(.+)$/s);
  if (!m) return raw;
  const header = (m[1] ?? "").toLowerCase();
  if (
    header.includes("telegram") ||
    header.includes("whatsapp") ||
    header.includes("signal") ||
    header.includes("discord") ||
    header.includes("slack") ||
    header.includes(" id:") ||
    header.includes(" est") ||
    header.includes(" et") ||
    header.match(/\+\s*\d{1,3}\s*m\b/) ||
    header.match(/\b\d{4}-\d{2}-\d{2}\b/) ||
    header.match(/\b\d{2}:\d{2}\b/)
  ) {
    return (m[2] ?? "").trim();
  }
  return raw;
};

const resolveChromeExecutablePath = (): string | undefined => {
  const env = (process.env.OPENCLAW_CHROME_PATH ?? process.env.CHROME_PATH ?? "").trim();
  if (env && fsSync.existsSync(env)) return env;
  if (process.platform === "darwin") {
    const macChrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
    if (fsSync.existsSync(macChrome)) return macChrome;
    const macCanary = "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary";
    if (fsSync.existsSync(macCanary)) return macCanary;
  }
  return undefined;
};

const QUICK_RESEARCH_BG_QUEUE = createBackgroundQueue({ concurrency: 1 });

const looksLikeQuickResearch = (value: string): boolean => {
  const s = value.trim();
  if (!s) return false;
  if (/^\/research(?:[_-]?(?:fast|deep))?\b/i.test(s)) {
    return true;
  }
  const lowered = s.toLowerCase();
  const hasTimebox =
    /\bt\+\s*\d{1,3}\b/.test(lowered) ||
    /\b\d{1,3}\s*(?:[-\u2010-\u2015\u2212]\s*)?(min|mins|minute|minutes)\b/.test(lowered) ||
    /\b\d{1,3}\b\s*[:.\u2010-\u2015\u2212]*\s*$/.test(s);
  if (!hasTimebox) return false;
  const hasIntent = /\b(research|reserach|reasearch|snapshot|memo|report|run|deep\s*dive)\b/.test(
    lowered,
  );
  const mentionsPdfOrAttach = /\b(pdf|attach|attachment|send\s+the\s+pdf|post\s+the\s+pdf)\b/.test(
    lowered,
  );
  return hasIntent || mentionsPdfOrAttach;
};

async function maybeHandleQuickResearchPdfRequest(params: {
  ctx: MsgContext;
  cleanedBody: string;
  command: Parameters<typeof handleCommands>[0]["command"];
  isGroup: boolean;
  cfg: OpenClawConfig;
  opts?: GetReplyOptions;
  typing: TypingController;
  agentId: string;
}): Promise<InlineActionResult | null> {
  const channelResolved =
    resolveGatewayMessageChannel(params.ctx.OriginatingChannel) ??
    resolveGatewayMessageChannel(params.ctx.Surface) ??
    resolveGatewayMessageChannel(params.ctx.Provider) ??
    undefined;
  const channelRaw = String(
    params.ctx.OriginatingChannel ?? params.ctx.Surface ?? params.ctx.Provider ?? "",
  )
    .trim()
    .toLowerCase();

  // IMPORTANT: do not rely solely on strict channel normalization.
  // If this is a timeboxed “send PDF” request and we're on Telegram, we must intercept.
  // If normalization fails for any reason, we still treat anything containing "telegram" as Telegram.
  const isTelegram = channelResolved === "telegram" || channelRaw.includes("telegram");
  if (!isTelegram) return null;

  const req = parseQuickResearchRequest(params.cleanedBody);
  if (!req) {
    if (!looksLikeQuickResearch(params.cleanedBody)) {
      return null;
    }
    // Fail closed: if it looks like a timeboxed "send PDF" research request but we can't parse it,
    // do NOT fall through to the chat model (which tends to "confirm" without delivering).
    params.typing.cleanup();
    return {
      kind: "reply",
      reply: {
        text: [
          "❌ Could not parse your timeboxed research-to-PDF request (fail-closed).",
          "",
          "Use one of these formats:",
          '- "optical networking 5"',
          '- "agentic commerce T+5 pdf"',
          '- "Run a 30 min research memo on PLTR and send the pdf"',
        ].join("\n"),
        isError: true,
      },
    };
  }
  if (!params.command.isAuthorizedSender) {
    params.typing.cleanup();
    return {
      kind: "reply",
      reply: {
        text: "❌ Refusing: quick research PDF runs require an authorized sender.",
        isError: true,
      },
    };
  }

  const hostRoleRaw =
    process.env.OPENCLAW_HOST_ROLE ??
    process.env.OPENCLAW_AGENT_ROLE ??
    process.env.OPENCLAW_AGENT ??
    "";
  const hostRole = String(hostRoleRaw).trim().toLowerCase();
  const hostname = os.hostname().trim().toLowerCase();
  const hostnameLooksLikeMacmini = hostname.includes("coopers") && hostname.includes("mini");
  const isMacmini = hostRole === "macmini" || hostnameLooksLikeMacmini;
  if (!isMacmini) {
    params.typing.cleanup();
    return {
      kind: "reply",
      reply: {
        text: `❌ Refusing to run research+PDF outside macmini (OPENCLAW_HOST_ROLE=${hostRole || "unset"} hostname=${hostname || "unknown"}).`,
        isError: true,
      },
    };
  }

  const allowBrowser = (
    process.env.OPENCLAW_ALLOW_BROWSER ??
    process.env.OPENCLAW_ALLOW_CHROME ??
    process.env.OPENCLAW_RENDER_PDF_ALLOWED ??
    ""
  ).trim();
  if (allowBrowser !== "1") {
    params.typing.cleanup();
    return {
      kind: "reply",
      reply: {
        text: "❌ Refusing to run research+PDF: OPENCLAW_ALLOW_BROWSER is not enabled. Set OPENCLAW_ALLOW_BROWSER=1 on macmini and restart the gateway.",
        isError: true,
      },
    };
  }

  const chromePath = resolveChromeExecutablePath();
  if (!chromePath) {
    params.typing.cleanup();
    return {
      kind: "reply",
      reply: {
        text: "❌ Chrome executable not found. Set OPENCLAW_CHROME_PATH on macmini and restart the gateway.",
        isError: true,
      },
    };
  }

  const { resolveCommitHash } = await import("../../infra/git-commit.js");
  const commit = resolveCommitHash({ cwd: process.cwd(), env: process.env }) ?? "unknown";

  const originTo = params.ctx.OriginatingTo ?? params.ctx.To;
  const originChannel = params.ctx.OriginatingChannel ?? "telegram";
  const originAccountId = params.ctx.AccountId ?? undefined;
  const originThreadId = params.ctx.MessageThreadId ?? undefined;
  const originSessionKey = params.ctx.SessionKey ?? undefined;
  if (!originTo) {
    params.typing.cleanup();
    return {
      kind: "reply",
      reply: {
        text: "❌ Cannot queue quick research: missing OriginatingTo/To routing target.",
        isError: true,
      },
    };
  }

  const createdAtMs = Date.now();
  const deliverAtMs = createdAtMs + req.minutes * 60_000;
  const deliverAtEt = formatBuiltAtEt(new Date(deliverAtMs));

  const crypto = await import("node:crypto");
  const jobId = crypto.randomUUID?.() ?? `${createdAtMs}-${Math.random().toString(16).slice(2)}`;

  QUICK_RESEARCH_BG_QUEUE.enqueue({
    id: jobId,
    label: `${req.kind}:${req.kind === "company" ? req.ticker : req.theme}`,
    createdAtMs,
    run: async () => {
      const delay = async (ms: number) => {
        if (ms <= 0) return;
        await new Promise<void>((r) => setTimeout(r, ms));
      };

      const safeSend = async (payload: ReplyPayload) => {
        const { routeReply } = await import("./route-reply.js");
        await routeReply({
          payload,
          channel: originChannel,
          to: originTo,
          accountId: originAccountId,
          threadId: originThreadId,
          cfg: params.cfg,
          sessionKey: originSessionKey,
          mirror: false,
        });
      };

      try {
        const fs = await import("node:fs/promises");
        const path = await import("node:path");
        const { pathToFileURL } = await import("node:url");
        const { chromium } = await import("playwright-core");
        const { diagnosePdfBuffer } = await import("../../research/pdf-diagnostics.js");
        const { writeFileArtifactManifest } = await import("../../research/artifact-manifest.js");
        const { resolveStateDir } = await import("../../config/paths.js");
        const { computeThemeResearch } = await import("../../research/theme-sector.js");
        const {
          inferThemeUniverseFromDb,
          inferThemeUniverseFromInstruments,
          normalizeThemeTickerUniverse,
        } = await import("../../research/theme-universe-infer.js");
        const { runCompanyPipelineV2, runThemePipelineV2 } =
          await import("../../v2/pipeline/v2-pipeline.js");

        const builtAtEt = formatBuiltAtEt(new Date());
        // Avoid hyphen mojibake in PDF text extraction (e.g. "-" -> standalone "n").
        const builtAtEtFooter = builtAtEt.replaceAll("-", "/");

        const renderPdfFromMarkdown = async (p: {
          inPath: string;
          outPath: string;
          title: string;
        }) => {
          const markdown = await fs.readFile(p.inPath, "utf8");
          const unicodeDashCount = (markdown.match(/[\u2010-\u2015\u2212]/g) ?? []).length;
          if (unicodeDashCount > 0) {
            throw new Error(
              `Unicode dash characters detected in markdown (count=${unicodeDashCount}). Replace with ASCII hyphens before rendering.`,
            );
          }
          const { default: MarkdownIt } = await import("markdown-it");
          const md = new MarkdownIt({
            html: true,
            linkify: true,
            breaks: false,
            typographer: false,
          });
          const bodyHtml = md.render(markdown);
          const baseHref = pathToFileURL(path.dirname(p.inPath) + path.sep).href;

          const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <base href="${escapeHtml(baseHref)}" />
    <title>${escapeHtml(p.title)}</title>
    <style>
      :root {
        --font-sans: ui-sans-serif, -apple-system, BlinkMacSystemFont, "SF Pro Text", "Helvetica Neue", Arial, sans-serif;
        --font-serif: ui-serif, "Iowan Old Style", "Palatino", "Georgia", serif;
        --font-mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
        --text: #111827;
        --muted: #6b7280;
        --rule: #e5e7eb;
        --accent: #0b3d91;
        --bg: #ffffff;
      }
      html { -webkit-print-color-adjust: exact; print-color-adjust: exact; background: var(--bg); }
      body {
        margin: 0; padding: 0; color: var(--text); background: var(--bg);
        font-family: var(--font-serif); font-size: 11.5pt; line-height: 1.38;
      }
      .doc { max-width: 7.2in; margin: 0 auto; padding: 0.15in 0; }
      h1, h2, h3, h4 { font-family: var(--font-sans); letter-spacing: -0.01em; margin: 0; }
      h1 { font-size: 22pt; line-height: 1.15; margin-bottom: 0.18in; }
      h2 { font-size: 14pt; margin-top: 0.26in; padding-top: 0.16in; border-top: 1px solid var(--rule); margin-bottom: 0.1in; }
      h3 { font-size: 11.75pt; margin-top: 0.16in; margin-bottom: 0.06in; }
      p { margin: 0 0 0.12in 0; }
      ul, ol { margin: 0 0 0.12in 0.22in; padding: 0; }
      li { margin: 0.04in 0; }
      blockquote { margin: 0.12in 0; padding: 0.12in 0.16in; border-left: 3px solid var(--rule); background: #fafafa; }
      table { width: 100%; border-collapse: collapse; margin: 0.14in 0; font-family: var(--font-sans); font-size: 10pt; }
      th, td { border-bottom: 1px solid var(--rule); padding: 0.06in 0.08in; vertical-align: top; }
      th { text-align: left; font-weight: 600; }
      code { font-family: var(--font-mono); font-size: 0.95em; background: #f3f4f6; padding: 0.03em 0.2em; border-radius: 4px; }
      pre { background: #0b1020; color: #e5e7eb; padding: 0.14in; border-radius: 8px; overflow-x: auto; margin: 0.14in 0; }
      pre code { background: transparent; padding: 0; border-radius: 0; font-size: 9.5pt; }
      a { color: var(--accent); text-decoration: none; }
      a:hover { text-decoration: underline; }
      h2, h3, h4 { break-after: avoid-page; }
      table, blockquote, pre { break-inside: avoid; }
    </style>
  </head>
  <body>
    <main class="doc">${bodyHtml}</main>
  </body>
</html>`;

          const browser = await chromium.launch({ headless: true, executablePath: chromePath });
          try {
            const page = await browser.newPage();
            await page.setContent(html, { waitUntil: "load" });
            await page.emulateMedia({ media: "print" });
            const headerTemplate = `<div style="font-size:8px;padding:0 24px;width:100%;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;color:#6b7280;display:flex;align-items:center;justify-content:space-between;"><span>${escapeHtml(
              p.title,
            )}</span><span></span></div>`;
            const footerTemplate = `<div style="font-size:8px;padding:0 24px;width:100%;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;color:#6b7280;display:flex;align-items:center;justify-content:space-between;"><span>FOR DISCUSSION PURPOSES ONLY; NOT INVESTMENT ADVICE.</span><span>Built: ${escapeHtml(
              builtAtEtFooter,
            )}</span><span>Page <span class="pageNumber"></span> / <span class="totalPages"></span></span></div>`;
            await page.pdf({
              path: p.outPath,
              format: "letter",
              printBackground: true,
              displayHeaderFooter: true,
              headerTemplate,
              footerTemplate,
              margin: { top: "0.9in", bottom: "0.9in", left: "0.9in", right: "0.9in" },
            });
          } finally {
            await browser.close();
          }
        };

        if (req.kind === "company") {
          const result = await runCompanyPipelineV2({
            ticker: req.ticker,
            question: req.question,
            timeboxMinutes: req.minutes,
          });
          if (!result.passed) {
            await delay(Math.max(0, deliverAtMs - Date.now()));
            await safeSend({
              text: `FAILED QUALITY GATE (v2)\njob_id=${jobId}\nrun_id=${result.runId}\nTop issues:\n- ${result.issues
                .filter((i: { severity: string }) => i.severity === "error")
                .slice(0, 8)
                .map(
                  (i: { code: string; message: string; path?: string }) =>
                    `${i.code}${i.path ? `@${i.path}` : ""}: ${i.message}`,
                )
                .join("\n- ")}`,
              isError: true,
            });
            return;
          }
          const outDir = path.join(path.dirname(result.reportMarkdownPath), "artifacts");
          await fs.mkdir(outDir, { recursive: true });
          const pdfPath = path.join(
            outDir,
            `${slugify(req.ticker)}-${new Date().toISOString().replaceAll(/[:.]/g, "-")}.pdf`,
          );
          await renderPdfFromMarkdown({
            inPath: result.reportMarkdownPath,
            outPath: pdfPath,
            title: `${req.ticker} (v2)`,
          });
          const pdfBuffer = await fs.readFile(pdfPath);
          const diag = await diagnosePdfBuffer({
            buffer: new Uint8Array(pdfBuffer),
            maxPages: 50,
            strict: true,
          });
          if (diag.errors.length) {
            await delay(Math.max(0, deliverAtMs - Date.now()));
            await safeSend({
              text: `FAILED PDF DIAGNOSTICS (strict)\njob_id=${jobId}\nrun_id=${result.runId}\n- ${diag.errors.slice(0, 12).join("\n- ")}`,
              isError: true,
            });
            return;
          }

          const os = await import("node:os");
          const stateDir = resolveStateDir(process.env, os.homedir);
          const seriesKey = `quickrun_company_${slugify(req.ticker)}`;
          const seriesManifestPath = path.join(
            stateDir,
            "research",
            "quickrun",
            `${seriesKey}.artifact.json`,
          );
          await fs.mkdir(path.dirname(seriesManifestPath), { recursive: true, mode: 0o700 });
          const manifestRes = await writeFileArtifactManifest({
            kind: "memo",
            outPath: pdfPath,
            seriesKey,
            seriesManifestPath,
            markdownForMetrics: await fs.readFile(result.reportMarkdownPath, "utf8"),
            metrics: {
              job_id: jobId,
              run_id: result.runId,
              built_at_et: builtAtEt,
              kind: "company",
            },
          });
          if (manifestRes.manifest.unchangedFromPrevious) {
            await delay(Math.max(0, deliverAtMs - Date.now()));
            await safeSend({
              text: `❌ Refusing to send: unchanged PDF artifact detected\njob_id=${jobId}\nrun_id=${result.runId}\nsha256=${manifestRes.manifest.sha256}`,
              isError: true,
            });
            return;
          }

          const sha256 = manifestRes.manifest.sha256;
          await delay(Math.max(0, deliverAtMs - Date.now()));
          await safeSend({
            text: `v2 company memo\njob_id=${jobId}\nrun_id=${result.runId}\nBuilt: ${builtAtEt}\nsha256=${sha256}\nbytes=${pdfBuffer.length}`,
            mediaUrl: pdfPath,
          });
          return;
        }

        const themeLabel = sanitizeThemeLabel(req.theme);
        const explicitUniverse = Array.from(
          new Set((req.tickers ?? []).map((t) => t.trim().toUpperCase()).filter(Boolean)),
        ).slice(0, 60);
        let inferredUniverse: {
          scanned_docs: number;
          inferred_tickers: string[];
          inferred_domains: string[];
          inferred_entities?: Array<{
            id: string;
            type: "equity" | "crypto_asset" | "protocol" | "private_company" | "index" | "other";
            label: string;
            symbol?: string;
            urls?: string[];
          }>;
        } | null = null;
        let themeRes: { tickers: string[] } | undefined;
        if (explicitUniverse.length) {
          themeRes = computeThemeResearch({ theme: themeLabel, tickers: explicitUniverse });
        } else {
          try {
            themeRes = computeThemeResearch({ theme: themeLabel });
          } catch {
            const inferred = inferThemeUniverseFromDb({ theme: themeLabel });
            inferredUniverse = inferred;

            let tickers = inferred.inferred_tickers;
            if (!tickers.length) {
              const instrumentGuess = inferThemeUniverseFromInstruments({ theme: themeLabel });
              tickers = instrumentGuess.inferred_tickers;
            }

            if (tickers.length) {
              themeRes = computeThemeResearch({
                theme: themeLabel,
                tickers,
              });
            }
          }
        }

        const maxUniverse =
          req.minutes <= 10 ? 5 : req.minutes <= 30 ? 10 : req.minutes <= 60 ? 15 : 25;
        let universe = normalizeThemeTickerUniverse({
          tickers: themeRes?.tickers ?? [],
          maxTickers: maxUniverse,
        }).tickers;
        if (!universe.length) {
          const instrumentGuess = inferThemeUniverseFromInstruments({
            theme: themeLabel,
            maxTickers: Math.max(10, maxUniverse * 3),
          });
          universe = normalizeThemeTickerUniverse({
            tickers: instrumentGuess.inferred_tickers,
            maxTickers: maxUniverse,
          }).tickers;
        }
        if (!universe.length) {
          await delay(Math.max(0, deliverAtMs - Date.now()));
          await safeSend({
            text: [
              `❌ Could not resolve a ticker universe for theme="${themeLabel}".`,
              "",
              "Re-run with an explicit universe list, e.g.:",
              `- "${themeLabel} tickers: CIEN, LITE, COHR, INFN, ANET 5"`,
              `- "${themeLabel} universe: NVDA, AVGO, ANET 10"`,
            ].join("\n"),
            isError: true,
          });
          return;
        }

        const result = await runThemePipelineV2({
          themeName: themeLabel,
          universe,
          universeEntities:
            inferredUniverse?.inferred_entities && inferredUniverse.inferred_entities.length
              ? inferredUniverse.inferred_entities
              : undefined,
          timeboxMinutes: req.minutes,
        });
        if (!result.passed) {
          await delay(Math.max(0, deliverAtMs - Date.now()));
          await safeSend({
            text: `FAILED QUALITY GATE (v2)\njob_id=${jobId}\nrun_id=${result.runId}\nTop issues:\n- ${result.issues
              .filter((i: { severity: string }) => i.severity === "error")
              .slice(0, 8)
              .map(
                (i: { code: string; message: string; path?: string }) =>
                  `${i.code}${i.path ? `@${i.path}` : ""}: ${i.message}`,
              )
              .join("\n- ")}`,
            isError: true,
          });
          return;
        }

        const outDir = path.join(path.dirname(result.reportMarkdownPath), "artifacts");
        await fs.mkdir(outDir, { recursive: true });
        const pdfPath = path.join(
          outDir,
          `${slugify(themeLabel)}-${new Date().toISOString().replaceAll(/[:.]/g, "-")}.pdf`,
        );
        await renderPdfFromMarkdown({
          inPath: result.reportMarkdownPath,
          outPath: pdfPath,
          title: `${themeLabel} (v2)`,
        });
        const pdfBuffer = await fs.readFile(pdfPath);
        const diag = await diagnosePdfBuffer({
          buffer: new Uint8Array(pdfBuffer),
          maxPages: 50,
          strict: true,
        });
        if (diag.errors.length) {
          await delay(Math.max(0, deliverAtMs - Date.now()));
          await safeSend({
            text: `FAILED PDF DIAGNOSTICS (strict)\njob_id=${jobId}\nrun_id=${result.runId}\n- ${diag.errors.slice(0, 12).join("\n- ")}`,
            isError: true,
          });
          return;
        }

        const os = await import("node:os");
        const stateDir = resolveStateDir(process.env, os.homedir);
        const seriesKey = `quickrun_theme_${slugify(themeLabel)}`;
        const seriesManifestPath = path.join(
          stateDir,
          "research",
          "quickrun",
          `${seriesKey}.artifact.json`,
        );
        await fs.mkdir(path.dirname(seriesManifestPath), { recursive: true, mode: 0o700 });
        const manifestRes = await writeFileArtifactManifest({
          kind: "theme_report",
          outPath: pdfPath,
          seriesKey,
          seriesManifestPath,
          markdownForMetrics: await fs.readFile(result.reportMarkdownPath, "utf8"),
          metrics: { job_id: jobId, run_id: result.runId, built_at_et: builtAtEt, kind: "theme" },
        });
        if (manifestRes.manifest.unchangedFromPrevious) {
          await delay(Math.max(0, deliverAtMs - Date.now()));
          await safeSend({
            text: `❌ Refusing to send: unchanged PDF artifact detected\njob_id=${jobId}\nrun_id=${result.runId}\nsha256=${manifestRes.manifest.sha256}`,
            isError: true,
          });
          return;
        }

        const sha256 = manifestRes.manifest.sha256;
        await delay(Math.max(0, deliverAtMs - Date.now()));
        await safeSend({
          text: [
            "v2 theme memo",
            `job_id=${jobId}`,
            `run_id=${result.runId}`,
            `Universe: ${universe.join(", ")}`,
            inferredUniverse
              ? `Universe bootstrap: scanned_docs=${inferredUniverse.scanned_docs} inferred_domains=${inferredUniverse.inferred_domains.slice(0, 10).join(", ")}`
              : null,
            `Built: ${builtAtEt}`,
            `sha256=${sha256}`,
            `bytes=${pdfBuffer.length}`,
          ]
            .filter(Boolean)
            .join("\n"),
          mediaUrl: pdfPath,
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        await delay(Math.max(0, deliverAtMs - Date.now()));
        await safeSend({
          text: `❌ quick research failed\njob_id=${jobId}\nerror=${message}`,
          isError: true,
        });
      }
    },
  });

  params.typing.cleanup();

  // Always-on log: helps debug cases where the LLM is replying "Confirmed..." instead of us intercepting.
  try {
    getLogger().info(
      {
        kind: req.kind,
        minutes: req.minutes,
        subject: req.kind === "company" ? req.ticker : req.theme,
        channelResolved,
        channelRaw,
      },
      "quickrun: accepted",
    );
  } catch {
    // ignore logging failures
  }

  return {
    kind: "reply",
    reply: {
      text: `Run accepted: ${req.kind} v2 (${req.minutes} min). Will post PDF at/after ${deliverAtEt} if it passes strict quality + strict PDF diagnostics.\njob_id=${jobId}\nagent_commit=${commit}`,
    },
  };
}

export async function handleInlineActions(params: {
  ctx: MsgContext;
  sessionCtx: TemplateContext;
  cfg: OpenClawConfig;
  agentId: string;
  agentDir?: string;
  sessionEntry?: SessionEntry;
  previousSessionEntry?: SessionEntry;
  sessionStore?: Record<string, SessionEntry>;
  sessionKey: string;
  storePath?: string;
  sessionScope: Parameters<typeof buildStatusReply>[0]["sessionScope"];
  workspaceDir: string;
  isGroup: boolean;
  opts?: GetReplyOptions;
  typing: TypingController;
  allowTextCommands: boolean;
  inlineStatusRequested: boolean;
  command: Parameters<typeof handleCommands>[0]["command"];
  skillCommands?: SkillCommandSpec[];
  directives: InlineDirectives;
  cleanedBody: string;
  elevatedEnabled: boolean;
  elevatedAllowed: boolean;
  elevatedFailures: Array<{ gate: string; key: string }>;
  defaultActivation: Parameters<typeof buildStatusReply>[0]["defaultGroupActivation"];
  resolvedThinkLevel: ThinkLevel | undefined;
  resolvedVerboseLevel: VerboseLevel | undefined;
  resolvedReasoningLevel: ReasoningLevel;
  resolvedElevatedLevel: ElevatedLevel;
  resolveDefaultThinkingLevel: Awaited<
    ReturnType<typeof createModelSelectionState>
  >["resolveDefaultThinkingLevel"];
  provider: string;
  model: string;
  contextTokens: number;
  directiveAck?: ReplyPayload;
  abortedLastRun: boolean;
  skillFilter?: string[];
}): Promise<InlineActionResult> {
  const {
    ctx,
    sessionCtx,
    cfg,
    agentId,
    agentDir,
    sessionEntry,
    previousSessionEntry,
    sessionStore,
    sessionKey,
    storePath,
    sessionScope,
    workspaceDir,
    isGroup,
    opts,
    typing,
    allowTextCommands,
    inlineStatusRequested,
    command,
    directives: initialDirectives,
    cleanedBody: initialCleanedBody,
    elevatedEnabled,
    elevatedAllowed,
    elevatedFailures,
    defaultActivation,
    resolvedThinkLevel,
    resolvedVerboseLevel,
    resolvedReasoningLevel,
    resolvedElevatedLevel,
    resolveDefaultThinkingLevel,
    provider,
    model,
    contextTokens,
    directiveAck,
    abortedLastRun: initialAbortedLastRun,
    skillFilter,
  } = params;

  let directives = initialDirectives;
  let cleanedBody = initialCleanedBody;

  const shouldLoadSkillCommands = command.commandBodyNormalized.startsWith("/");
  const skillCommands =
    shouldLoadSkillCommands && params.skillCommands
      ? params.skillCommands
      : shouldLoadSkillCommands
        ? listSkillCommandsForWorkspace({
            workspaceDir,
            cfg,
            skillFilter,
          })
        : [];

  const skillInvocation =
    allowTextCommands && skillCommands.length > 0
      ? resolveSkillCommandInvocation({
          commandBodyNormalized: command.commandBodyNormalized,
          skillCommands,
        })
      : null;
  if (skillInvocation) {
    if (!command.isAuthorizedSender) {
      logVerbose(
        `Ignoring /${skillInvocation.command.name} from unauthorized sender: ${command.senderId || "<unknown>"}`,
      );
      typing.cleanup();
      return { kind: "reply", reply: undefined };
    }

    const dispatch = skillInvocation.command.dispatch;
    if (dispatch?.kind === "tool") {
      const rawArgs = (skillInvocation.args ?? "").trim();
      const channel =
        resolveGatewayMessageChannel(ctx.Surface) ??
        resolveGatewayMessageChannel(ctx.Provider) ??
        undefined;

      const tools = createOpenClawTools({
        agentSessionKey: sessionKey,
        agentChannel: channel,
        agentAccountId: (ctx as { AccountId?: string }).AccountId,
        agentTo: ctx.OriginatingTo ?? ctx.To,
        agentThreadId: ctx.MessageThreadId ?? undefined,
        agentDir,
        workspaceDir,
        config: cfg,
      });

      const tool = tools.find((candidate) => candidate.name === dispatch.toolName);
      if (!tool) {
        typing.cleanup();
        return { kind: "reply", reply: { text: `❌ Tool not available: ${dispatch.toolName}` } };
      }

      const toolCallId = `cmd_${Date.now()}_${Math.random().toString(16).slice(2)}`;
      try {
        const result = await tool.execute(toolCallId, {
          command: rawArgs,
          commandName: skillInvocation.command.name,
          skillName: skillInvocation.command.skillName,
          // oxlint-disable-next-line typescript/no-explicit-any
        } as any);
        const text = extractTextFromToolResult(result) ?? "✅ Done.";
        typing.cleanup();
        return { kind: "reply", reply: { text } };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        typing.cleanup();
        return { kind: "reply", reply: { text: `❌ ${message}` } };
      }
    }

    const promptParts = [
      `Use the "${skillInvocation.command.skillName}" skill for this request.`,
      skillInvocation.args ? `User input:\n${skillInvocation.args}` : null,
    ].filter((entry): entry is string => Boolean(entry));
    const rewrittenBody = promptParts.join("\n\n");
    ctx.Body = rewrittenBody;
    ctx.BodyForAgent = rewrittenBody;
    sessionCtx.Body = rewrittenBody;
    sessionCtx.BodyForAgent = rewrittenBody;
    sessionCtx.BodyStripped = rewrittenBody;
    cleanedBody = rewrittenBody;
  }

  const sendInlineReply = async (reply?: ReplyPayload) => {
    if (!reply) {
      return;
    }
    if (!opts?.onBlockReply) {
      return;
    }
    await opts.onBlockReply(reply);
  };

  const inlineCommand =
    allowTextCommands && command.isAuthorizedSender
      ? extractInlineSimpleCommand(cleanedBody)
      : null;
  if (inlineCommand) {
    cleanedBody = inlineCommand.cleaned;
    sessionCtx.Body = cleanedBody;
    sessionCtx.BodyForAgent = cleanedBody;
    sessionCtx.BodyStripped = cleanedBody;
  }

  const quickRun = await maybeHandleQuickResearchPdfRequest({
    ctx,
    cleanedBody,
    command,
    isGroup,
    cfg,
    opts,
    typing,
    agentId,
  });
  if (quickRun) {
    return quickRun;
  }

  const handleInlineStatus =
    !isDirectiveOnly({
      directives,
      cleanedBody: directives.cleaned,
      ctx,
      cfg,
      agentId,
      isGroup,
    }) && inlineStatusRequested;
  if (handleInlineStatus) {
    const inlineStatusReply = await buildStatusReply({
      cfg,
      command,
      sessionEntry,
      sessionKey,
      sessionScope,
      provider,
      model,
      contextTokens,
      resolvedThinkLevel,
      resolvedVerboseLevel: resolvedVerboseLevel ?? "off",
      resolvedReasoningLevel,
      resolvedElevatedLevel,
      resolveDefaultThinkingLevel,
      isGroup,
      defaultGroupActivation: defaultActivation,
      mediaDecisions: ctx.MediaUnderstandingDecisions,
    });
    await sendInlineReply(inlineStatusReply);
    directives = { ...directives, hasStatusDirective: false };
  }

  if (inlineCommand) {
    const inlineCommandContext = {
      ...command,
      rawBodyNormalized: inlineCommand.command,
      commandBodyNormalized: inlineCommand.command,
    };
    const inlineResult = await handleCommands({
      ctx,
      cfg,
      command: inlineCommandContext,
      agentId,
      directives,
      elevated: {
        enabled: elevatedEnabled,
        allowed: elevatedAllowed,
        failures: elevatedFailures,
      },
      sessionEntry,
      previousSessionEntry,
      sessionStore,
      sessionKey,
      storePath,
      sessionScope,
      workspaceDir,
      defaultGroupActivation: defaultActivation,
      resolvedThinkLevel,
      resolvedVerboseLevel: resolvedVerboseLevel ?? "off",
      resolvedReasoningLevel,
      resolvedElevatedLevel,
      resolveDefaultThinkingLevel,
      provider,
      model,
      contextTokens,
      isGroup,
      skillCommands,
    });
    if (inlineResult.reply) {
      if (!inlineCommand.cleaned) {
        typing.cleanup();
        return { kind: "reply", reply: inlineResult.reply };
      }
      await sendInlineReply(inlineResult.reply);
    }
  }

  if (directiveAck) {
    await sendInlineReply(directiveAck);
  }

  const isEmptyConfig = Object.keys(cfg).length === 0;
  const skipWhenConfigEmpty = command.channelId
    ? Boolean(getChannelDock(command.channelId)?.commands?.skipWhenConfigEmpty)
    : false;
  if (
    skipWhenConfigEmpty &&
    isEmptyConfig &&
    command.from &&
    command.to &&
    command.from !== command.to
  ) {
    typing.cleanup();
    return { kind: "reply", reply: undefined };
  }

  let abortedLastRun = initialAbortedLastRun;
  if (!sessionEntry && command.abortKey) {
    abortedLastRun = getAbortMemory(command.abortKey) ?? false;
  }

  const commandResult = await handleCommands({
    ctx,
    cfg,
    command,
    agentId,
    directives,
    elevated: {
      enabled: elevatedEnabled,
      allowed: elevatedAllowed,
      failures: elevatedFailures,
    },
    sessionEntry,
    previousSessionEntry,
    sessionStore,
    sessionKey,
    storePath,
    sessionScope,
    workspaceDir,
    defaultGroupActivation: defaultActivation,
    resolvedThinkLevel,
    resolvedVerboseLevel: resolvedVerboseLevel ?? "off",
    resolvedReasoningLevel,
    resolvedElevatedLevel,
    resolveDefaultThinkingLevel,
    provider,
    model,
    contextTokens,
    isGroup,
    skillCommands,
  });
  if (!commandResult.shouldContinue) {
    typing.cleanup();
    return { kind: "reply", reply: commandResult.reply };
  }

  return {
    kind: "continue",
    directives,
    abortedLastRun,
  };
}
