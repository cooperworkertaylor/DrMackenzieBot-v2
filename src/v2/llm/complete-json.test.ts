import { describe, expect, it } from "vitest";
import {
  extractBestEffortAssistantText,
  resolveResearchV2FallbackModelRefs,
  resolveResearchV2ModelRef,
  supportsTemperatureForResearchV2Model,
  tryRepairTruncatedJson,
} from "./complete-json.js";

describe("resolveResearchV2ModelRef", () => {
  it("defaults research v2 to openai/gpt-5.4 when unset", () => {
    expect(
      resolveResearchV2ModelRef({
        purpose: "writer",
        env: {} as NodeJS.ProcessEnv,
        cfg: { agents: { defaults: { model: {} } } } as unknown as ReturnType<
          typeof import("../../config/config.js").loadConfig
        >,
      }),
    ).toBe("openai/gpt-5.4");
  });

  it("accepts provider/model from env", () => {
    const ref = resolveResearchV2ModelRef({
      purpose: "writer",
      env: { OPENCLAW_RESEARCH_V2_MODEL: "anthropic/claude-opus-4-5" } as NodeJS.ProcessEnv,
      cfg: { agents: { defaults: { model: {} } } } as unknown as ReturnType<
        typeof import("../../config/config.js").loadConfig
      >,
    });
    expect(ref).toBe("anthropic/claude-opus-4-5");
  });
});

describe("supportsTemperatureForResearchV2Model", () => {
  it("disables temperature for openai gpt-5 family", () => {
    expect(
      supportsTemperatureForResearchV2Model({
        provider: "openai",
        model: "gpt-5",
      }),
    ).toBe(false);
    expect(
      supportsTemperatureForResearchV2Model({
        provider: "openai",
        model: "gpt-5.2",
      }),
    ).toBe(false);
  });

  it("keeps temperature for non-gpt-5 models/providers", () => {
    expect(
      supportsTemperatureForResearchV2Model({
        provider: "openai",
        model: "gpt-4.1",
      }),
    ).toBe(true);
    expect(
      supportsTemperatureForResearchV2Model({
        provider: "anthropic",
        model: "claude-opus-4-5",
      }),
    ).toBe(true);
  });
});

describe("tryRepairTruncatedJson", () => {
  it("repairs truncated objects and arrays", () => {
    expect(tryRepairTruncatedJson('{"a":1,"b":[1,2')).toBe('{"a":1,"b":[1,2]}');
    expect(tryRepairTruncatedJson('[{"a":1},{"b":2}')).toBe('[{"a":1},{"b":2}]');
  });

  it("repairs unclosed JSON strings", () => {
    expect(tryRepairTruncatedJson('{"name":"optic')).toBe('{"name":"optic"}');
  });

  it("returns null for non-json or malformed close order", () => {
    expect(tryRepairTruncatedJson("not json")).toBeNull();
    expect(tryRepairTruncatedJson('{"a":[1,2}}')).toBeNull();
  });
});

describe("extractBestEffortAssistantText", () => {
  it("falls back to thinking content when text is missing", () => {
    const msg = {
      content: [{ type: "thinking", thinking: '{"ok":true}' }],
    };
    expect(extractBestEffortAssistantText(msg)).toBe('{"ok":true}');
  });
});

describe("resolveResearchV2FallbackModelRefs", () => {
  it("includes primary, env fallbacks, and cfg fallbacks in order without duplicates", () => {
    const refs = resolveResearchV2FallbackModelRefs({
      primary: "openai/gpt-5.4",
      purpose: "writer",
      env: {
        OPENCLAW_RESEARCH_V2_WRITER_FALLBACK_MODEL: "anthropic/claude-opus-4-5",
        OPENCLAW_RESEARCH_V2_FALLBACK_MODELS: "openai/gpt-5.4, google/gemini-3-pro-preview",
      } as NodeJS.ProcessEnv,
      cfg: {
        agents: {
          defaults: {
            model: {
              fallbacks: ["openai/gpt-5.4-mini", "google/gemini-3-pro-preview"],
            },
          },
        },
      } as unknown as ReturnType<typeof import("../../config/config.js").loadConfig>,
    });
    expect(refs).toEqual([
      "openai/gpt-5.4",
      "anthropic/claude-opus-4-5",
      "google/gemini-3-pro-preview",
      "openai/gpt-5.4-mini",
      "openai-codex/gpt-5.4-codex",
      "openai/gpt-5.2",
      "openai-codex/gpt-5.2-codex",
    ]);
  });

  it("adds automatic OpenAI fallback for gpt-5 codex primary", () => {
    const refs = resolveResearchV2FallbackModelRefs({
      primary: "openai-codex/gpt-5.4-codex",
      purpose: "analyzer",
      env: {} as NodeJS.ProcessEnv,
      cfg: { agents: { defaults: { model: {} } } } as unknown as ReturnType<
        typeof import("../../config/config.js").loadConfig
      >,
    });
    expect(refs).toEqual([
      "openai-codex/gpt-5.4-codex",
      "openai/gpt-5.4",
      "openai/gpt-5.2",
      "openai-codex/gpt-5.2-codex",
    ]);
  });
});
