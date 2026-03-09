import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { runsRoot, runDirFor } from "./run-io.js";

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("run-io", () => {
  it("uses OPENCLAW_STATE_DIR for pipeline runs by default", () => {
    vi.stubEnv("OPENCLAW_STATE_DIR", "/tmp/openclaw-state");
    expect(runsRoot()).toBe(path.join("/tmp/openclaw-state", "research", "runs"));
    expect(runDirFor("job-1")).toBe(path.join("/tmp/openclaw-state", "research", "runs", "job-1"));
  });

  it("allows an explicit OPENCLAW_RESEARCH_RUNS_DIR override", () => {
    vi.stubEnv("OPENCLAW_RESEARCH_RUNS_DIR", "~/custom-runs");
    expect(runsRoot()).toBe(path.join(os.homedir(), "custom-runs"));
  });
});
