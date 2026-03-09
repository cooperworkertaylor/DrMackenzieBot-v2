import { mkdtempSync } from "node:fs";
import fs from "node:fs/promises";
import { tmpdir } from "node:os";
import path, { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { OpenClawConfig } from "../config/config.js";

describe("models-config openai compatibility", () => {
  let previousHome: string | undefined;
  let previousOpenAiKey: string | undefined;

  beforeEach(() => {
    previousHome = process.env.HOME;
    previousOpenAiKey = process.env.OPENAI_API_KEY;
  });

  afterEach(() => {
    process.env.HOME = previousHome;
    if (previousOpenAiKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousOpenAiKey;
    }
  });

  it("injects an explicit openai provider when gpt-5.4 is configured", async () => {
    vi.resetModules();
    const home = mkdtempSync(join(tmpdir(), "openclaw-openai-models-"));
    process.env.HOME = home;
    process.env.OPENAI_API_KEY = "sk-openai-test";

    const { ensureOpenClawModelsJson } = await import("./models-config.js");
    const { resolveOpenClawAgentDir } = await import("./agent-paths.js");

    const cfg: OpenClawConfig = {
      agents: {
        defaults: {
          model: {
            primary: "openai/gpt-5.4",
          },
        },
      },
      models: {
        providers: {},
      },
    };

    await ensureOpenClawModelsJson(cfg);

    const modelPath = path.join(resolveOpenClawAgentDir(), "models.json");
    const raw = await fs.readFile(modelPath, "utf8");
    const parsed = JSON.parse(raw) as {
      providers: Record<string, { apiKey?: string; models?: Array<{ id: string }> }>;
    };

    expect(parsed.providers.openai?.apiKey).toBe("OPENAI_API_KEY");
    const ids = parsed.providers.openai?.models?.map((model) => model.id) ?? [];
    expect(ids).toContain("gpt-5.4");
    expect(ids).toContain("gpt-5.4-mini");
  });
});
