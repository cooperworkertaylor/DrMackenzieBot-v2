import { randomUUID } from "node:crypto";
import type { CliDeps } from "../../cli/deps.js";
import type { CronJob } from "../../cron/types.js";
import type { createSubsystemLogger } from "../../logging/subsystem.js";
import type { HookMessageChannel, HooksConfigResolved } from "../hooks.js";
import { loadConfig } from "../../config/config.js";
import { resolveMainSessionKeyFromConfig } from "../../config/sessions.js";
import { runCronIsolatedAgentTurn } from "../../cron/isolated-agent.js";
import { requestHeartbeatNow } from "../../infra/heartbeat-wake.js";
import { enqueueSystemEvent } from "../../infra/system-events.js";
import {
  buildResearchCandidateFromGmailHook,
  ingestExternalResearchDocument,
  parseCsvLower,
} from "../../research/external-research.js";
import { createHooksRequestHandler } from "../server-http.js";

type SubsystemLogger = ReturnType<typeof createSubsystemLogger>;

export function createGatewayHooksRequestHandler(params: {
  deps: CliDeps;
  getHooksConfig: () => HooksConfigResolved | null;
  bindHost: string;
  port: number;
  logHooks: SubsystemLogger;
}) {
  const { deps, getHooksConfig, bindHost, port, logHooks } = params;

  const dispatchWakeHook = (value: { text: string; mode: "now" | "next-heartbeat" }) => {
    const sessionKey = resolveMainSessionKeyFromConfig();
    enqueueSystemEvent(value.text, { sessionKey });
    if (value.mode === "now") {
      requestHeartbeatNow({ reason: "hook:wake" });
    }
  };

  const dispatchAgentHook = (value: {
    message: string;
    name: string;
    wakeMode: "now" | "next-heartbeat";
    sessionKey: string;
    deliver: boolean;
    channel: HookMessageChannel;
    to?: string;
    model?: string;
    thinking?: string;
    timeoutSeconds?: number;
    allowUnsafeExternalContent?: boolean;
  }) => {
    const sessionKey = value.sessionKey.trim() ? value.sessionKey.trim() : `hook:${randomUUID()}`;
    const mainSessionKey = resolveMainSessionKeyFromConfig();
    const runId = randomUUID();
    const skipAgentForResearchIngest = (() => {
      const raw = process.env.OPENCLAW_RESEARCH_EMAIL_INGEST_SKIP_AGENT?.trim().toLowerCase();
      if (!raw) return true;
      if (["1", "true", "yes", "y"].includes(raw)) return true;
      if (["0", "false", "no", "n"].includes(raw)) return false;
      return true;
    })();
    const researchCandidate = buildResearchCandidateFromGmailHook({
      sessionKey,
      message: value.message,
      subjectPrefix: process.env.OPENCLAW_RESEARCH_EMAIL_SUBJECT_PREFIX,
      allowedSenders: parseCsvLower(process.env.OPENCLAW_RESEARCH_EMAIL_ALLOW_SENDERS),
    });
    if (researchCandidate) {
      void (async () => {
        try {
          const ingestResult = ingestExternalResearchDocument({
            sourceType: researchCandidate.sourceType,
            provider: researchCandidate.provider,
            externalId: researchCandidate.externalId,
            sender: researchCandidate.sender,
            title: researchCandidate.title,
            subject: researchCandidate.subject,
            content: researchCandidate.content,
            url: researchCandidate.url,
            ticker: researchCandidate.ticker,
            tags: researchCandidate.tags,
          });
          enqueueSystemEvent(
            `Research ingestion: ${ingestResult.sourceType} provider=${ingestResult.provider} title="${ingestResult.title}" chunks=${ingestResult.chunks}`,
            {
              sessionKey: mainSessionKey,
            },
          );
          if (value.wakeMode === "now") {
            requestHeartbeatNow({ reason: "hook:research-ingest" });
          }
        } catch (err) {
          logHooks.warn(`research hook ingest failed: ${String(err)}`);
          enqueueSystemEvent(`Research ingestion failed: ${String(err)}`, {
            sessionKey: mainSessionKey,
          });
          if (value.wakeMode === "now") {
            requestHeartbeatNow({ reason: "hook:research-ingest:error" });
          }
        }
      })();
      if (skipAgentForResearchIngest) {
        return runId;
      }
    }

    const jobId = randomUUID();
    const now = Date.now();
    const job: CronJob = {
      id: jobId,
      name: value.name,
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "at", atMs: now },
      sessionTarget: "isolated",
      wakeMode: value.wakeMode,
      payload: {
        kind: "agentTurn",
        message: value.message,
        model: value.model,
        thinking: value.thinking,
        timeoutSeconds: value.timeoutSeconds,
        deliver: value.deliver,
        channel: value.channel,
        to: value.to,
        allowUnsafeExternalContent: value.allowUnsafeExternalContent,
      },
      state: { nextRunAtMs: now },
    };

    void (async () => {
      try {
        const cfg = loadConfig();
        const result = await runCronIsolatedAgentTurn({
          cfg,
          deps,
          job,
          message: value.message,
          sessionKey,
          lane: "cron",
        });
        const summary = result.summary?.trim() || result.error?.trim() || result.status;
        const prefix =
          result.status === "ok" ? `Hook ${value.name}` : `Hook ${value.name} (${result.status})`;
        enqueueSystemEvent(`${prefix}: ${summary}`.trim(), {
          sessionKey: mainSessionKey,
        });
        if (value.wakeMode === "now") {
          requestHeartbeatNow({ reason: `hook:${jobId}` });
        }
      } catch (err) {
        logHooks.warn(`hook agent failed: ${String(err)}`);
        enqueueSystemEvent(`Hook ${value.name} (error): ${String(err)}`, {
          sessionKey: mainSessionKey,
        });
        if (value.wakeMode === "now") {
          requestHeartbeatNow({ reason: `hook:${jobId}:error` });
        }
      }
    })();

    return runId;
  };

  return createHooksRequestHandler({
    getHooksConfig,
    bindHost,
    port,
    logHooks,
    dispatchAgentHook,
    dispatchWakeHook,
  });
}
