import { describe, expect, it } from "vitest";
import {
  resolveResearchV2ModelRef,
  supportsTemperatureForResearchV2Model,
} from "./complete-json.js";

describe("resolveResearchV2ModelRef", () => {
  it("throws on missing model config", () => {
    expect(() =>
      resolveResearchV2ModelRef({
        purpose: "writer",
        env: {} as NodeJS.ProcessEnv,
        cfg: { agents: { defaults: { model: {} } } } as unknown as ReturnType<
          typeof import("../../config/config.js").loadConfig
        >,
      }),
    ).toThrow(/No model configured/i);
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
