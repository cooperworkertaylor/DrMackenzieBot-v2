import { describe, expect, it, vi } from "vitest";
import type { ReplyPayload } from "../../auto-reply/types.js";
import type { WebInboundMsg } from "./types.js";
import { deliverWebReply } from "./deliver-reply.js";

vi.mock("../media.js", () => ({
  loadWebMedia: vi.fn(),
}));

vi.mock("../../research/pdf-diagnostics.js", () => ({
  diagnosePdfBuffer: vi.fn(),
}));

const { loadWebMedia } = await import("../media.js");
const { diagnosePdfBuffer } = await import("../../research/pdf-diagnostics.js");

const makeMsg = (overrides: Partial<WebInboundMsg>): WebInboundMsg =>
  ({
    id: "m1",
    from: "+15551234567",
    to: "+15557654321",
    accountId: "default",
    body: "hi",
    chatType: "dm",
    sendComposing: async () => {},
    reply: vi.fn(async () => {}),
    sendMedia: vi.fn(async () => {}),
    ...overrides,
  }) as unknown as WebInboundMsg;

describe("deliverWebReply (strict pdf diagnostics)", () => {
  it("blocks PDFs that fail strict diagnostics and sends refusal text", async () => {
    (loadWebMedia as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
      buffer: Buffer.from("%PDF-1.7"),
      kind: "document",
      contentType: "application/pdf",
      fileName: "report.pdf",
    });
    (diagnosePdfBuffer as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
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

    const msg = makeMsg({});
    const payload: ReplyPayload = {
      text: "here you go",
      mediaUrl: "https://example.com/report.pdf",
    };

    await deliverWebReply({
      replyResult: payload,
      msg,
      maxMediaBytes: 50 * 1024 * 1024,
      textLimit: 5000,
      replyLogger: { info: () => {}, warn: () => {} },
    });

    expect(msg.sendMedia).not.toHaveBeenCalled();
    expect(msg.reply).toHaveBeenCalledTimes(1);
    const text = (msg.reply as unknown as ReturnType<typeof vi.fn>).mock.calls[0]?.[0];
    expect(String(text)).toContain("Refused to send PDF");
    expect(String(text)).toContain("missing_citation_keys");
  });

  it("sends PDFs that pass strict diagnostics", async () => {
    (loadWebMedia as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
      buffer: Buffer.from("%PDF-1.7"),
      kind: "document",
      contentType: "application/pdf",
      fileName: "report.pdf",
    });
    (diagnosePdfBuffer as unknown as ReturnType<typeof vi.fn>).mockResolvedValue({
      metrics: {
        markdownHeadingTokens: 0,
        markdownFenceTokens: 0,
        urlCount: 5,
        citationKeyCount: 12,
        exhibitTokenCount: 7,
        sourcesHeadingPresent: true,
        dashMojibakeDateCount: 0,
        dashMojibakeStandaloneNCount: 0,
        placeholderTokenCount: 0,
        extractedChars: 1000,
      },
      errors: [],
    });

    const msg = makeMsg({});
    const payload: ReplyPayload = {
      text: "caption",
      mediaUrl: "https://example.com/report.pdf",
    };

    await deliverWebReply({
      replyResult: payload,
      msg,
      maxMediaBytes: 50 * 1024 * 1024,
      textLimit: 5000,
      replyLogger: { info: () => {}, warn: () => {} },
    });

    expect(msg.reply).not.toHaveBeenCalled();
    expect(msg.sendMedia).toHaveBeenCalledTimes(1);
    const args = (msg.sendMedia as unknown as ReturnType<typeof vi.fn>).mock.calls[0]?.[0] ?? {};
    expect(args.fileName).toBe("report.pdf");
    expect(args.mimetype).toBe("application/pdf");
    expect(Buffer.isBuffer(args.document)).toBe(true);
  });
});
