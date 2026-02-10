import { describe, expect, it, vi } from "vitest";
import { createBackgroundQueue } from "./background-queue.js";

describe("createBackgroundQueue", () => {
  it("runs jobs sequentially with concurrency=1", async () => {
    const q = createBackgroundQueue({ concurrency: 1 });
    const events: string[] = [];
    let release1: (() => void) | null = null;
    const p1 = new Promise<void>((r) => (release1 = r));

    q.enqueue({
      id: "j1",
      label: "one",
      createdAtMs: Date.now(),
      run: async () => {
        events.push("start1");
        await p1;
        events.push("end1");
      },
    });
    q.enqueue({
      id: "j2",
      label: "two",
      createdAtMs: Date.now(),
      run: async () => {
        events.push("start2");
        events.push("end2");
      },
    });

    await vi.waitFor(() => {
      expect(events).toContain("start1");
    });
    expect(events).not.toContain("start2");

    release1?.();

    await vi.waitFor(() => {
      expect(events).toEqual(["start1", "end1", "start2", "end2"]);
    });
  });
});
