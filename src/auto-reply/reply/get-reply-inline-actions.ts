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
import {
  parseQuickResearchRequest,
  type QuickResearchRequest,
} from "../../research/quick-research-request.js";
import {
  buildQuickResearchStatusReply,
  enqueueQuickResearchJob,
  formatBuiltAtEt,
} from "../../research/quickrun/quick-research-jobs.js";
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

export const looksLikeQuickResearchStatusPrompt = (value: string): boolean => {
  const normalized = value.trim().toLowerCase();
  if (!normalized) return false;
  return /^(?:\/qstatus|update|status|quick status|quick update|progress|eta)(?:\?+)?$/.test(
    normalized,
  );
};

const resolveQuickResearchStatusPrompt = (params: {
  ctx: MsgContext;
  cleanedBody: string;
  command: Parameters<typeof handleCommands>[0]["command"];
}): string | null => {
  const candidates = [
    typeof params.ctx.BodyForCommands === "string" ? params.ctx.BodyForCommands : "",
    typeof params.ctx.CommandBody === "string" ? params.ctx.CommandBody : "",
    params.command.commandBodyNormalized ?? "",
    params.command.rawBodyNormalized ?? "",
    params.cleanedBody,
  ]
    .map((value) => value.trim())
    .filter(Boolean);
  for (const candidate of candidates) {
    if (looksLikeQuickResearchStatusPrompt(candidate)) {
      return candidate;
    }
  }
  return null;
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
  const quickResearchResolution = resolveQuickResearchRequest({
    ctx: params.ctx,
    cleanedBody: params.cleanedBody,
    command: params.command,
  });
  const explicitQuickResearchCommand = /^\/research(?:[_-]?(?:fast|deep))?\b/i.test(
    (params.command.commandBodyNormalized ?? "").trim(),
  );
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
  // Also hard-intercept explicit /research* commands and parseable quick-research prompts
  // so they never fall through to free-form LLM confirmations.
  const isTelegram = channelResolved === "telegram" || channelRaw.includes("telegram");
  const req = quickResearchResolution.request;
  if (!isTelegram && !explicitQuickResearchCommand && !req) return null;

  if (!req) {
    if (!quickResearchResolution.looksLikeQuickResearch) {
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
  const originChannel =
    channelResolved ??
    params.ctx.OriginatingChannel ??
    params.command.channel ??
    params.ctx.Surface ??
    "telegram";
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
  const { resolveActiveResearchDbPath, resolveResearchExecutionProfile } =
    await import("../../research/research-model-profile.js");
  const jobId = crypto.randomUUID?.() ?? `${createdAtMs}-${Math.random().toString(16).slice(2)}`;
  const activeResearchProfile = resolveResearchExecutionProfile({
    dbPath: resolveActiveResearchDbPath(),
  });

  enqueueQuickResearchJob({
    cfg: params.cfg,
    payload: {
      jobId,
      request: req,
      createdAtMs,
      deliverAtMs,
      researchProfile: {
        key: activeResearchProfile.key,
        label: activeResearchProfile.label,
        modelRef: activeResearchProfile.modelRef,
        ...(activeResearchProfile.profileId ? { profileId: activeResearchProfile.profileId } : {}),
      },
      route: {
        channel: originChannel,
        to: originTo,
        accountId: originAccountId != null ? String(originAccountId) : undefined,
        threadId: originThreadId != null ? String(originThreadId) : undefined,
        sessionKey: originSessionKey != null ? String(originSessionKey) : undefined,
      },
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
      text: `Run accepted: ${req.kind} v2 (${req.minutes} min). Will post PDF at/after ${deliverAtEt} if it passes strict quality + strict PDF diagnostics.\njob_id=${jobId}\nresearch_profile=${activeResearchProfile.key}\nresearch_model=${activeResearchProfile.modelRef}\nagent_commit=${commit}`,
    },
  };
}

export function resolveQuickResearchRequest(params: {
  ctx: MsgContext;
  cleanedBody: string;
  command: Parameters<typeof handleCommands>[0]["command"];
}): {
  request: QuickResearchRequest | null;
  source: string | null;
  looksLikeQuickResearch: boolean;
} {
  const candidates = [
    typeof params.ctx.BodyForCommands === "string" ? params.ctx.BodyForCommands : "",
    typeof params.ctx.CommandBody === "string" ? params.ctx.CommandBody : "",
    params.command.commandBodyNormalized ?? "",
    params.command.rawBodyNormalized ?? "",
    params.cleanedBody,
  ]
    .map((value) => value.trim())
    .filter(Boolean);

  const seen = new Set<string>();
  for (const candidate of candidates) {
    if (seen.has(candidate)) continue;
    seen.add(candidate);
    const request = parseQuickResearchRequest(candidate);
    if (request) {
      return {
        request,
        source: candidate,
        looksLikeQuickResearch: true,
      };
    }
  }

  return {
    request: null,
    source: null,
    looksLikeQuickResearch: Array.from(seen).some((candidate) => looksLikeQuickResearch(candidate)),
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

  const quickResearchStatusPrompt = resolveQuickResearchStatusPrompt({
    ctx,
    cleanedBody,
    command,
  });
  if (quickResearchStatusPrompt) {
    const { resolveActiveResearchDbPath } =
      await import("../../research/research-model-profile.js");
    const route = {
      channel:
        resolveGatewayMessageChannel(ctx.OriginatingChannel) ??
        resolveGatewayMessageChannel(ctx.Surface) ??
        resolveGatewayMessageChannel(ctx.Provider) ??
        "telegram",
      to: String(ctx.OriginatingTo ?? ctx.To ?? "").trim(),
      accountId: ctx.AccountId != null ? String(ctx.AccountId) : undefined,
      threadId: ctx.MessageThreadId != null ? String(ctx.MessageThreadId) : undefined,
      sessionKey: ctx.SessionKey != null ? String(ctx.SessionKey) : undefined,
    };
    if (route.to) {
      const statusReply = buildQuickResearchStatusReply({
        route,
        dbPath: resolveActiveResearchDbPath(),
      });
      if (statusReply) {
        typing.cleanup();
        return {
          kind: "reply",
          reply: {
            text: statusReply,
          },
        };
      }
    }
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
