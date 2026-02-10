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
});
