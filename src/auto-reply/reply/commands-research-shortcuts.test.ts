import { describe, expect, it } from "vitest";
import { __testOnly } from "./commands-research-shortcuts.js";

describe("research shortcut command parsing", () => {
  it("parses head/tail args for shortcut commands", () => {
    const parsed = __testOnly.parseArgsWithTail(
      "/icmemo nvda demand remains underpriced",
      "icmemo",
    );
    expect(parsed.first).toBe("nvda");
    expect(parsed.rest).toBe("demand remains underpriced");
  });

  it("returns empty values when no args are provided", () => {
    const parsed = __testOnly.parseArgsWithTail("/ictheme", "ictheme");
    expect(parsed.first).toBe("");
    expect(parsed.rest).toBeUndefined();
  });

  it("parses all tokens after command", () => {
    const parsed = __testOnly.parseArgs("/icsector technology cyclical recovery", "icsector");
    expect(parsed).toEqual(["technology", "cyclical", "recovery"]);
  });
});
