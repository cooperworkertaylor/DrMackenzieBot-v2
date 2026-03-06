import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createQuickrunJobRunner } from "./job-runner.js";
import { QuickrunJobStore } from "./job-store.js";

const tempDirs: string[] = [];

const makeStore = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "quickrun-job-runner-"));
  tempDirs.push(dir);
  return QuickrunJobStore.open(path.join(dir, "research.db"));
};

afterEach(() => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  }
});

describe("createQuickrunJobRunner", () => {
  it("runs enqueued jobs sequentially and marks them completed", async () => {
    const store = makeStore();
    const events: string[] = [];
    let releaseFirst: (() => void) | null = null;
    const firstBarrier = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });

    const runner = createQuickrunJobRunner<{ name: string }>({
      store,
      jobType: "quick_research_pdf_v2",
      pollIntervalMs: 10,
      concurrency: 1,
      handler: async (job) => {
        events.push(`start:${job.payload.name}`);
        if (job.payload.name === "one") {
          await firstBarrier;
        }
        events.push(`end:${job.payload.name}`);
      },
    });

    runner.start();
    runner.enqueue({
      id: "job-1",
      jobType: "quick_research_pdf_v2",
      payload: { name: "one" },
      runAfterMs: Date.now(),
    });
    runner.enqueue({
      id: "job-2",
      jobType: "quick_research_pdf_v2",
      payload: { name: "two" },
      runAfterMs: Date.now(),
    });

    await vi.waitFor(() => {
      expect(events).toContain("start:one");
    });
    expect(events).not.toContain("start:two");

    releaseFirst?.();

    await vi.waitFor(() => {
      expect(events).toEqual(["start:one", "end:one", "start:two", "end:two"]);
      expect(store.getById("job-1")?.status).toBe("completed");
      expect(store.getById("job-2")?.status).toBe("completed");
    });

    runner.stop();
  });
});
