import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { sendMessageTelegram } from "./send.js";

const loadWebMedia = vi.fn();
const diagnosePdfBuffer = vi.fn();

vi.mock("../web/media.js", () => ({
  loadWebMedia: (...args: unknown[]) => loadWebMedia(...args),
}));

vi.mock("../research/pdf-diagnostics.js", () => ({
  diagnosePdfBuffer: (...args: unknown[]) => diagnosePdfBuffer(...args),
}));

describe("sendMessageTelegram (strict pdf diagnostics)", () => {
  it("refuses to send a PDF when strict diagnostics fail", async () => {
    const api = {
      sendMessage: vi.fn().mockResolvedValue({ message_id: 1, chat: { id: "123" } }),
      sendDocument: vi.fn().mockResolvedValue({ message_id: 2, chat: { id: "123" } }),
    } as unknown as NonNullable<Parameters<typeof sendMessageTelegram>[2]>["api"];

    loadWebMedia.mockResolvedValueOnce({
      buffer: Buffer.from("%PDF-1.7"),
      contentType: "application/pdf",
      fileName: "report.pdf",
    });
    diagnosePdfBuffer.mockResolvedValueOnce({
      metrics: {
        markdownHeadingTokens: 0,
        markdownFenceTokens: 0,
        urlCount: 0,
        citationKeyCount: 0,
        exhibitTokenCount: 0,
        sourcesHeadingPresent: false,
        dashMojibakeDateCount: 0,
        dashMojibakeStandaloneNCount: 0,
        placeholderTokenCount: 1,
        extractedChars: 100,
      },
      errors: ["missing_citation_keys: citationKeyCount=0 (min=1)"],
    });

    const res = await sendMessageTelegram("telegram:123", "caption", {
      token: "tok",
      api,
      mediaUrl: "https://example.com/report.pdf",
      maxBytes: 50 * 1024 * 1024,
    });

    expect(res).toEqual({ messageId: "1", chatId: "123" });
    expect(api.sendDocument).not.toHaveBeenCalled();
    expect(api.sendMessage).toHaveBeenCalledTimes(1);
    expect(
      String((api.sendMessage as unknown as ReturnType<typeof vi.fn>).mock.calls[0]?.[1] ?? ""),
    ).toContain("Refused to send PDF");
  });

  it("retries document send when Telegram encoder requires Uint8Array payload", async () => {
    const api = {
      sendDocument: vi
        .fn()
        .mockRejectedValueOnce(
          new Error(
            "Telegram encoder requirement: needs a Uint8Array payload with the message field populated",
          ),
        )
        .mockResolvedValueOnce({ message_id: 3, chat: { id: "123" } }),
      sendMessage: vi.fn(),
    } as unknown as NonNullable<Parameters<typeof sendMessageTelegram>[2]>["api"];

    loadWebMedia.mockResolvedValueOnce({
      buffer: Buffer.from("not-a-pdf"),
      contentType: "application/octet-stream",
      fileName: "report.bin",
    });

    const res = await sendMessageTelegram("telegram:123", "", {
      token: "tok",
      api,
      mediaUrl: "https://example.com/report.bin",
      maxBytes: 50 * 1024 * 1024,
    });

    expect(api.sendDocument).toHaveBeenCalledTimes(2);
    expect(
      (api.sendDocument as unknown as ReturnType<typeof vi.fn>).mock.calls[1]?.[2],
    ).toMatchObject({
      caption: "report attached",
    });
    expect(res).toEqual({ messageId: "3", chatId: "123" });
    expect(api.sendMessage).not.toHaveBeenCalled();
  });

  it("uses a file-backed upload when the media URL is a local artifact path", async () => {
    const api = {
      sendDocument: vi.fn().mockResolvedValue({ message_id: 4, chat: { id: "123" } }),
      sendMessage: vi.fn(),
    } as unknown as NonNullable<Parameters<typeof sendMessageTelegram>[2]>["api"];

    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "telegram-pdf-"));
    const pdfPath = path.join(tempDir, "report.pdf");
    fs.writeFileSync(pdfPath, "%PDF-1.7");

    loadWebMedia.mockResolvedValueOnce({
      buffer: Buffer.from("%PDF-1.7"),
      contentType: "application/pdf",
      fileName: "report.pdf",
    });
    diagnosePdfBuffer.mockResolvedValueOnce({
      metrics: {
        markdownHeadingTokens: 1,
        markdownFenceTokens: 0,
        urlCount: 1,
        citationKeyCount: 1,
        exhibitTokenCount: 1,
        sourcesHeadingPresent: true,
        dashMojibakeDateCount: 0,
        dashMojibakeStandaloneNCount: 0,
        placeholderTokenCount: 0,
        extractedChars: 100,
      },
      errors: [],
    });

    try {
      const res = await sendMessageTelegram("telegram:123", "caption", {
        token: "tok",
        api,
        mediaUrl: pdfPath,
        maxBytes: 50 * 1024 * 1024,
      });

      expect(res).toEqual({ messageId: "4", chatId: "123" });
      const fileArg = (api.sendDocument as unknown as ReturnType<typeof vi.fn>).mock
        .calls[0]?.[1] as { fileData?: unknown } | undefined;
      expect(fileArg?.fileData).toBe(pdfPath);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });
});
