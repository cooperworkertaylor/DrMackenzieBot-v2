import { afterEach, describe, expect, it, vi } from "vitest";
import { __testOnly } from "./research-cli.js";

describe("research outbound fetch default", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("defaults to enabled when env is unset", () => {
    vi.stubEnv("OPENCLAW_RESEARCH_FORCE_OUTBOUND_FETCH", "");
    expect(__testOnly.resolveResearchOutboundFetchEnabled()).toBe(true);
  });

  it("supports explicit disable via env", () => {
    vi.stubEnv("OPENCLAW_RESEARCH_FORCE_OUTBOUND_FETCH", "false");
    expect(__testOnly.resolveResearchOutboundFetchEnabled()).toBe(false);
  });

  it("supports explicit enable via env", () => {
    vi.stubEnv("OPENCLAW_RESEARCH_FORCE_OUTBOUND_FETCH", "true");
    expect(__testOnly.resolveResearchOutboundFetchEnabled()).toBe(true);
  });

  it("fails open on invalid env values", () => {
    vi.stubEnv("OPENCLAW_RESEARCH_FORCE_OUTBOUND_FETCH", "definitely");
    expect(__testOnly.resolveResearchOutboundFetchEnabled()).toBe(true);
  });
});
