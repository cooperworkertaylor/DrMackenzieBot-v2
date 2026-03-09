import { describe, expect, it } from "vitest";
import {
  looksLikeQuickResearchPdfFollowupPrompt,
  looksLikeQuickResearchStatusPrompt,
  resolveQuickResearchRequest,
} from "./get-reply-inline-actions.js";

describe("resolveQuickResearchRequest", () => {
  it("prefers the current inbound command body over expanded session text", () => {
    const result = resolveQuickResearchRequest({
      ctx: {
        BodyForCommands: "NVDA 10 min",
        CommandBody: "NVDA 10 min",
      } as never,
      cleanedBody:
        "[History context]\nEarlier reminder text\n[Current message - respond to this]\nNVDA 10 min",
      command: {
        commandBodyNormalized:
          "[History context]\nEarlier reminder text\n[Current message - respond to this]\nNVDA 10 min",
        rawBodyNormalized: "NVDA 10 min",
      } as never,
    });

    expect(result.request?.kind).toBe("company");
    if (result.request?.kind !== "company") throw new Error("expected company request");
    expect(result.request.ticker).toBe("NVDA");
    expect(result.request.minutes).toBe(10);
    expect(result.source).toBe("NVDA 10 min");
  });

  it("recognizes deterministic quick research status follow-ups", () => {
    expect(looksLikeQuickResearchStatusPrompt("update?")).toBe(true);
    expect(looksLikeQuickResearchStatusPrompt("status")).toBe(true);
    expect(looksLikeQuickResearchStatusPrompt("/qstatus")).toBe(true);
    expect(looksLikeQuickResearchStatusPrompt("what changed with NVDA")).toBe(false);
  });

  it("recognizes deterministic quick research PDF follow-ups", () => {
    expect(looksLikeQuickResearchPdfFollowupPrompt("send the pdf here")).toBe(true);
    expect(looksLikeQuickResearchPdfFollowupPrompt("attach the report")).toBe(true);
    expect(looksLikeQuickResearchPdfFollowupPrompt("post the memo here")).toBe(true);
    expect(looksLikeQuickResearchPdfFollowupPrompt("what changed with NVDA")).toBe(false);
  });
});
