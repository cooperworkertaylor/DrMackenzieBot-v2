import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { readTelegramLastSentPdf, writeTelegramLastSentPdf } from "./last-sent-pdf-store.js";

async function withTempStateDir<T>(fn: (dir: string) => Promise<T>) {
  const previous = process.env.OPENCLAW_STATE_DIR;
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-telegram-"));
  process.env.OPENCLAW_STATE_DIR = dir;
  try {
    return await fn(dir);
  } finally {
    if (previous === undefined) {
      delete process.env.OPENCLAW_STATE_DIR;
    } else {
      process.env.OPENCLAW_STATE_DIR = previous;
    }
    await fs.rm(dir, { recursive: true, force: true });
  }
}

describe("telegram last sent pdf store", () => {
  it("persists and reloads a chat entry", async () => {
    await withTempStateDir(async () => {
      expect(await readTelegramLastSentPdf({ accountId: "primary", chatId: "123" })).toBeNull();

      await writeTelegramLastSentPdf({
        accountId: "primary",
        chatId: "123",
        sha256: "abc",
        bytes: 42,
        fileName: "report.pdf",
      });

      const entry = await readTelegramLastSentPdf({ accountId: "primary", chatId: "123" });
      expect(entry?.sha256).toBe("abc");
      expect(entry?.bytes).toBe(42);
      expect(entry?.fileName).toBe("report.pdf");
      expect(typeof entry?.sentAt).toBe("string");
    });
  });

  it("overwrites the existing chat entry", async () => {
    await withTempStateDir(async () => {
      await writeTelegramLastSentPdf({
        accountId: "primary",
        chatId: "123",
        sha256: "abc",
        bytes: 42,
        fileName: "report.pdf",
      });

      await writeTelegramLastSentPdf({
        accountId: "primary",
        chatId: "123",
        sha256: "def",
        bytes: 43,
        fileName: "report2.pdf",
      });

      const entry = await readTelegramLastSentPdf({ accountId: "primary", chatId: "123" });
      expect(entry?.sha256).toBe("def");
      expect(entry?.bytes).toBe(43);
      expect(entry?.fileName).toBe("report2.pdf");
    });
  });
});
