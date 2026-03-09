import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { QuickrunJobStore } from "./job-store.js";

const tempDirs: string[] = [];

const makeDbPath = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "quickrun-job-store-"));
  tempDirs.push(dir);
  return path.join(dir, "research.db");
};

afterEach(() => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  }
});

describe("QuickrunJobStore", () => {
  it("enqueues and claims queued jobs", () => {
    const store = QuickrunJobStore.open(makeDbPath());
    store.enqueue({
      id: "job-1",
      jobType: "quick_research_pdf_v2",
      payload: { ok: true },
      runAfterMs: Date.now() - 1,
    });

    const claimed = store.claimNext<{ ok: boolean }>({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
    });

    expect(claimed?.id).toBe("job-1");
    expect(claimed?.status).toBe("running");
    expect(claimed?.attempts).toBe(1);
    expect(claimed?.payload.ok).toBe(true);
  });

  it("requeues failures until max attempts is reached", () => {
    const store = QuickrunJobStore.open(makeDbPath());
    store.enqueue({
      id: "job-2",
      jobType: "quick_research_pdf_v2",
      payload: { ok: true },
      runAfterMs: 0,
      maxAttempts: 2,
    });

    const first = store.claimNext({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
      nowMs: 10,
    });
    expect(first?.attempts).toBe(1);

    const requeued = store.markFailed({
      id: "job-2",
      workerId: "worker-a",
      error: "boom",
      nowMs: 20,
      retryDelayMs: 5,
    });
    expect(requeued?.status).toBe("queued");

    const second = store.claimNext({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
      nowMs: 1_100,
    });
    expect(second?.attempts).toBe(2);

    const failed = store.markFailed({
      id: "job-2",
      workerId: "worker-a",
      error: "still boom",
      nowMs: 40,
    });
    expect(failed?.status).toBe("failed");
    expect(failed?.lastError).toBe("still boom");
  });

  it("persists progress notes for running jobs", () => {
    const store = QuickrunJobStore.open(makeDbPath());
    store.enqueue({
      id: "job-3",
      jobType: "quick_research_pdf_v2",
      payload: { ok: true },
      runAfterMs: 0,
    });

    const claimed = store.claimNext({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
      nowMs: 10,
    });
    expect(claimed?.status).toBe("running");

    store.setProgress({
      id: "job-3",
      workerId: "worker-a",
      note: "Draft passed quality gate. Rendering PDF.",
      nowMs: 20,
    });

    const updated = store.getById<{ ok: boolean }>("job-3");
    expect(updated?.progressNote).toBe("Draft passed quality gate. Rendering PDF.");
    expect(updated?.progressUpdatedAtMs).toBe(20);
  });

  it("persists final delivery metadata for completed job re-sends", () => {
    const store = QuickrunJobStore.open(makeDbPath());
    store.enqueue({
      id: "job-4",
      jobType: "quick_research_pdf_v2",
      payload: { ok: true },
      runAfterMs: 0,
    });

    store.claimNext({
      jobType: "quick_research_pdf_v2",
      workerId: "worker-a",
      nowMs: 10,
    });

    store.setResult({
      id: "job-4",
      workerId: "worker-a",
      text: "Company memo ready: NVDA",
      mediaUrl: "/tmp/nvda.pdf",
      runId: "run-4",
      nowMs: 20,
    });

    const updated = store.getById<{ ok: boolean }>("job-4");
    expect(updated?.resultText).toBe("Company memo ready: NVDA");
    expect(updated?.resultMediaUrl).toBe("/tmp/nvda.pdf");
    expect(updated?.resultRunId).toBe("run-4");
  });
});
