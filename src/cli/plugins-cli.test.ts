import { Command } from "commander";
import { beforeEach, describe, expect, it, vi } from "vitest";

const loadConfig = vi.fn();
const writeConfigFile = vi.fn(async () => {});

vi.mock("../config/config.js", () => ({
  loadConfig,
  writeConfigFile,
}));

vi.mock("../plugins/status.js", () => ({
  buildPluginStatusReport: () => ({
    workspaceDir: "/tmp/openclaw",
    plugins: [],
    diagnostics: [],
  }),
}));

describe("plugins cli", () => {
  beforeEach(() => {
    loadConfig.mockReset();
    writeConfigFile.mockClear();
  });

  it("adds enabled plugin to allowlist when allowlist is set", async () => {
    loadConfig.mockReturnValueOnce({
      plugins: {
        allow: ["memory-lancedb"],
        entries: {},
      },
    });

    const { registerPluginsCli } = await import("./plugins-cli.js");
    const program = new Command();
    program.name("test");
    registerPluginsCli(program);

    await program.parseAsync(["plugins", "enable", "research-context"], { from: "user" });

    expect(writeConfigFile).toHaveBeenCalledTimes(1);
    expect(writeConfigFile).toHaveBeenCalledWith(
      expect.objectContaining({
        plugins: expect.objectContaining({
          allow: ["memory-lancedb", "research-context"],
          entries: expect.objectContaining({
            "research-context": expect.objectContaining({ enabled: true }),
          }),
        }),
      }),
    );
  });
});
