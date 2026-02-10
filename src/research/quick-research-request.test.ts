import { describe, expect, it } from "vitest";
import { parseQuickResearchRequest } from "./quick-research-request.js";

describe("parseQuickResearchRequest", () => {
  it("parses theme request from natural language", () => {
    const res = parseQuickResearchRequest(
      "give me a 5 min reserach run on agentic commerce. send the pdf here after 5 min.",
    );
    expect(res).toEqual({ kind: "theme", minutes: 5, theme: "agentic commerce" });
  });

  it("parses snapshot phrasing (no literal 'research' token)", () => {
    const res = parseQuickResearchRequest(
      "Run a fresh 5-minute optical networking snapshot now and post a single PDF here at T+5.",
    );
    expect(res).toEqual({ kind: "theme", minutes: 5, theme: "optical networking" });
  });

  it("parses company request when subject looks like a ticker", () => {
    const res = parseQuickResearchRequest("give me a 10 min research run on PLTR and send the pdf");
    expect(res?.kind).toBe("company");
    if (res?.kind !== "company") throw new Error("expected company");
    expect(res.minutes).toBe(10);
    expect(res.ticker).toBe("PLTR");
    expect(res.question).toContain("PLTR");
  });

  it("does not trigger without all guard tokens", () => {
    expect(parseQuickResearchRequest("give me a 5 min summary on agentic commerce")).toBeNull();
    expect(parseQuickResearchRequest("send pdf")).toBeNull();
    expect(parseQuickResearchRequest("research on agentic commerce")).toBeNull();
  });
});
