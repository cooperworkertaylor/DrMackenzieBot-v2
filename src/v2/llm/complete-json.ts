import type { Api, Context, Model } from "@mariozechner/pi-ai";
import { complete } from "@mariozechner/pi-ai";
import { resolveOpenClawAgentDir } from "../../agents/agent-paths.js";
import { getApiKeyForModel, requireApiKey } from "../../agents/model-auth.js";
import { ensureOpenClawModelsJson } from "../../agents/models-config.js";
import { extractAssistantText } from "../../agents/pi-embedded-utils.js";
import { discoverAuthStorage, discoverModels } from "../../agents/pi-model-discovery.js";
import { loadConfig } from "../../config/config.js";

export type ModelRef = { provider: string; model: string };

const parseModelRef = (raw: string): ModelRef | null => {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const idx = trimmed.indexOf("/");
  if (idx <= 0 || idx === trimmed.length - 1) return null;
  const provider = trimmed.slice(0, idx).trim();
  const model = trimmed.slice(idx + 1).trim();
  if (!provider || !model) return null;
  return { provider, model };
};

export function supportsTemperatureForResearchV2Model(params: {
  provider: string;
  model: string;
}): boolean {
  const provider = params.provider.trim().toLowerCase();
  const model = params.model.trim().toLowerCase();
  if (!provider || !model) return true;
  // OpenAI GPT-5 family rejects temperature in this execution path.
  if (provider === "openai" && /^gpt-5(?:$|[.\-])/.test(model)) return false;
  return true;
}

const appendStrictJsonRetryInstruction = (prompt: string): string =>
  [
    prompt.trim(),
    "",
    "RETRY INSTRUCTION:",
    "Return exactly one JSON object and nothing else.",
    "Do not return markdown fences, explanation, or blank output.",
    "If uncertain, return the best schema-conforming JSON with nulls/empty arrays.",
  ].join("\n");

export const extractBestEffortAssistantText = (message: unknown): string => {
  const primary = extractAssistantText(
    message as Parameters<typeof extractAssistantText>[0],
  ).trim();
  if (primary) return primary;
  const blocks = (message as { content?: unknown })?.content;
  if (!Array.isArray(blocks)) return "";
  const parts = blocks
    .map((block) => {
      if (!block || typeof block !== "object") return "";
      const rec = block as Record<string, unknown>;
      if (typeof rec.text === "string") return rec.text.trim();
      if (typeof rec.content === "string") return rec.content.trim();
      if (typeof rec.thinking === "string") return rec.thinking.trim();
      return "";
    })
    .filter(Boolean);
  return parts.join("\n").trim();
};

const summarizeAssistantMessage = (message: unknown): string => {
  const stopReason = String((message as { stopReason?: unknown })?.stopReason ?? "");
  const content = (message as { content?: unknown })?.content;
  const types = Array.isArray(content)
    ? content
        .map((block) => {
          if (!block || typeof block !== "object") return typeof block;
          const rec = block as Record<string, unknown>;
          return typeof rec.type === "string" ? rec.type : "unknown";
        })
        .slice(0, 8)
    : [];
  const err = String((message as { errorMessage?: unknown })?.errorMessage ?? "").trim();
  return `stopReason=${stopReason || "unknown"} contentTypes=[${types.join(",")}]${err ? ` error=${err}` : ""}`;
};

function extractJsonCandidate(text: string): string {
  const trimmed = text.trim();
  if (!trimmed) {
    throw new Error("Model returned empty text; expected JSON.");
  }

  // Strip fenced code blocks if the model wrapped JSON.
  const fence = trimmed.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  const unfenced = fence?.[1]?.trim() ? fence[1].trim() : trimmed;

  // If it's already JSON-ish, try directly.
  if (unfenced.startsWith("{") || unfenced.startsWith("[")) {
    return unfenced;
  }

  // Otherwise, attempt to locate the first top-level JSON object/array.
  const firstObj = unfenced.indexOf("{");
  const firstArr = unfenced.indexOf("[");
  const start =
    firstObj === -1 ? firstArr : firstArr === -1 ? firstObj : Math.min(firstObj, firstArr);
  if (start === -1) {
    throw new Error("Model output did not contain a JSON object/array.");
  }
  // Best-effort: take from start to last closing brace/bracket.
  const endObj = unfenced.lastIndexOf("}");
  const endArr = unfenced.lastIndexOf("]");
  const end = Math.max(endObj, endArr);
  if (end <= start) {
    return unfenced.slice(start).trim();
  }
  return unfenced.slice(start, end + 1);
}

export function tryRepairTruncatedJson(candidate: string): string | null {
  const input = candidate.trim();
  if (!input) return null;
  if (!(input.startsWith("{") || input.startsWith("["))) return null;

  let out = "";
  const closeStack: string[] = [];
  let inString = false;
  let escape = false;

  for (let i = 0; i < input.length; i += 1) {
    const ch = input[i]!;
    out += ch;
    if (inString) {
      if (escape) {
        escape = false;
      } else if (ch === "\\") {
        escape = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }
    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === "{") {
      closeStack.push("}");
      continue;
    }
    if (ch === "[") {
      closeStack.push("]");
      continue;
    }
    if (ch === "}" || ch === "]") {
      const expected = closeStack.pop();
      if (expected !== ch) return null;
    }
  }

  if (inString) {
    if (escape) out += "\\";
    out += '"';
  }
  while (closeStack.length > 0) {
    out += closeStack.pop();
  }
  return out;
}

const parseJsonFromModelText = (text: string): unknown => {
  const jsonText = extractJsonCandidate(text);
  try {
    return JSON.parse(jsonText) as unknown;
  } catch (e) {
    const repaired = tryRepairTruncatedJson(jsonText);
    if (repaired) {
      return JSON.parse(repaired) as unknown;
    }
    throw e;
  }
};

const appendJsonRepairInstruction = (params: {
  prompt: string;
  previous: string;
  parseError: string;
}) =>
  [
    appendStrictJsonRetryInstruction(params.prompt),
    "",
    "The previous answer was invalid JSON. Return a corrected full JSON object only.",
    `Parse error: ${params.parseError}`,
    "Previous invalid response:",
    "```",
    params.previous.slice(0, 12000),
    "```",
  ].join("\n");

export function resolveResearchV2ModelRef(params: {
  purpose: "writer" | "analyzer" | "seed";
  env?: NodeJS.ProcessEnv;
  cfg?: ReturnType<typeof loadConfig>;
}): string {
  const env = params.env ?? process.env;
  const cfg = params.cfg ?? loadConfig();

  const explicit =
    (params.purpose === "writer" ? env.OPENCLAW_RESEARCH_V2_WRITER_MODEL : undefined) ??
    (params.purpose === "analyzer" ? env.OPENCLAW_RESEARCH_V2_ANALYZER_MODEL : undefined) ??
    (params.purpose === "seed" ? env.OPENCLAW_RESEARCH_V2_SEED_MODEL : undefined) ??
    env.OPENCLAW_RESEARCH_V2_MODEL ??
    "";

  const fromCfg =
    (cfg.agents?.defaults?.model?.primary ?? "").trim() ||
    (cfg.agents?.defaults?.model?.fallbacks?.[0] ?? "").trim() ||
    "";

  const picked = (explicit || fromCfg).trim();
  if (!picked) {
    throw new Error(
      "No model configured for v2 research LLM. Set OPENCLAW_RESEARCH_V2_MODEL (e.g. anthropic/claude-opus-4-5) or configure agents.defaults.model.primary in openclaw.json.",
    );
  }
  if (!parseModelRef(picked)) {
    throw new Error(
      `Invalid model ref ${JSON.stringify(picked)} (expected provider/model, e.g. anthropic/claude-opus-4-5).`,
    );
  }
  return picked;
}

export async function completeJsonWithResearchV2Model(params: {
  purpose: "writer" | "analyzer" | "seed";
  prompt: string;
  maxTokens?: number;
  temperature?: number;
  agentDir?: string;
  modelRefOverride?: string;
  profileId?: string;
}): Promise<unknown> {
  const cfg = loadConfig();
  const agentDir = params.agentDir ?? resolveOpenClawAgentDir();
  await ensureOpenClawModelsJson(cfg, agentDir);

  const modelRefRaw =
    (params.modelRefOverride ?? "").trim() ||
    resolveResearchV2ModelRef({ purpose: params.purpose, env: process.env, cfg });
  const ref = parseModelRef(modelRefRaw);
  if (!ref) {
    throw new Error(`Invalid model ref ${JSON.stringify(modelRefRaw)} (expected provider/model).`);
  }

  const authStorage = discoverAuthStorage(agentDir);
  const modelRegistry = discoverModels(authStorage, agentDir);
  const model = modelRegistry.find(ref.provider, ref.model) as Model<Api> | null;
  if (!model) {
    throw new Error(`Unknown model: ${ref.provider}/${ref.model}`);
  }

  const apiKeyInfo = await getApiKeyForModel({
    model,
    cfg,
    agentDir,
    profileId: params.profileId,
  });
  const apiKey = requireApiKey(apiKeyInfo, model.provider);
  authStorage.setRuntimeApiKey(model.provider, apiKey);

  const completeParams: Parameters<typeof complete>[2] = {
    apiKey,
    maxTokens: params.maxTokens ?? 2400,
  };
  if (
    supportsTemperatureForResearchV2Model({
      provider: model.provider,
      model: model.id,
    })
  ) {
    completeParams.temperature = params.temperature ?? 0.2;
  }
  const completeOnce = async (prompt: string) => {
    const context: Context = {
      messages: [
        {
          role: "user",
          content: [{ type: "text", text: prompt }],
          timestamp: Date.now(),
        },
      ],
    };
    const message = await complete(model, context, completeParams);
    const stop = message.stopReason;
    const err = message.errorMessage?.trim();
    if (stop === "error" || stop === "aborted") {
      throw new Error(err ? `LLM failed (${model.provider}/${model.id}): ${err}` : "LLM failed.");
    }
    if (err) {
      throw new Error(`LLM failed (${model.provider}/${model.id}): ${err}`);
    }
    return message;
  };

  const firstMessage = await completeOnce(params.prompt);
  let text = extractBestEffortAssistantText(firstMessage);
  if (!text) {
    const retryMessage = await completeOnce(appendStrictJsonRetryInstruction(params.prompt));
    text = extractBestEffortAssistantText(retryMessage);
    if (!text) {
      throw new Error(
        `Model returned empty text; expected JSON. (${summarizeAssistantMessage(retryMessage)})`,
      );
    }
  }

  let parseErrorMessage = "";
  try {
    return parseJsonFromModelText(text);
  } catch (e) {
    parseErrorMessage = e instanceof Error ? e.message : String(e);
  }

  const repairMessage = await completeOnce(
    appendJsonRepairInstruction({
      prompt: params.prompt,
      previous: text,
      parseError: parseErrorMessage,
    }),
  );
  const repairedText = extractBestEffortAssistantText(repairMessage);
  if (!repairedText) {
    throw new Error(
      `Model returned empty text during JSON repair; expected JSON. (${summarizeAssistantMessage(repairMessage)})`,
    );
  }
  try {
    return parseJsonFromModelText(repairedText);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`Failed to parse model JSON: ${msg}`);
  }
}
