import { describe, expect, it } from "vitest";
import { resolveResearchV2ModelRef } from "./complete-json.js";

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
