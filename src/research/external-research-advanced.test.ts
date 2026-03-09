import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ingestExternalResearchDocument } from "./external-research.js";
import {
  compareExternalResearchPeers,
  detectExternalResearchSourceConflicts,
} from "./external-research-advanced.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-external-research-advanced-${name}-${Date.now()}-${Math.random()}.db`);

describe("external research advanced analysis", () => {
  it("detects source conflicts from contradictory facts and claims", () => {
    const dbPath = testDbPath("conflicts");

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "cooptaylor1@gmail.com",
      title: "NVDA upside setup",
      subject: "RESEARCH NVDA upside setup",
      ticker: "NVDA",
      content: [
        "NVDA revenue growth could sustain 24% because pricing discipline remains strong and demand remains strong across accelerator programs.",
        "Gross margin could hold around 72% while operating margin remains above 45% if mix stays tilted toward high-end accelerators.",
      ].join(" "),
      url: "https://example.com/research/nvda-upside-setup",
      publishedAt: "2026-03-01T10:00:00Z",
    });

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "semianalysis",
      sender: "digest@semianalysis.com",
      title: "NVDA downside reset",
      subject: "NVDA downside reset",
      ticker: "NVDA",
      content: [
        "NVDA revenue growth may slow to 12% because pricing discipline is weakening and competition risk is rising across custom silicon programs.",
        "Gross margin could slip toward 64% while operating margin could fall below 38% as supply normalizes and pricing pressure increases.",
      ].join(" "),
      url: "https://example.com/research/nvda-downside-reset",
      publishedAt: "2026-03-03T12:00:00Z",
    });

    const report = detectExternalResearchSourceConflicts({
      ticker: "NVDA",
      dbPath,
      lookbackDays: 120,
    });

    expect(report.conflicts.length).toBeGreaterThan(0);
    expect(report.conflicts.some((conflict) => conflict.kind === "fact")).toBe(true);
    expect(report.conflicts.some((conflict) => conflict.kind === "claim")).toBe(true);
    expect(report.markdown).toContain("NVDA Source Conflicts");
  });

  it("compares peers from latest reports and theses", () => {
    const dbPath = testDbPath("compare");

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "cooptaylor1@gmail.com",
      title: "NVDA demand setup",
      subject: "RESEARCH NVDA demand setup",
      ticker: "NVDA",
      content: [
        "NVDA revenue growth could sustain 24% because pricing discipline and demand remains strong across accelerator programs.",
        "Gross margin could hold around 72% while operating margin remains above 45% if mix stays tilted toward high-end accelerators.",
      ].join(" "),
      url: "https://example.com/research/nvda-demand-setup",
      publishedAt: "2026-03-01T10:00:00Z",
    });

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "stratechery",
      sender: "updates@stratechery.com",
      title: "NVDA follow-through",
      subject: "NVDA follow-through",
      ticker: "NVDA",
      content: [
        "Guidance remains constructive and demand remains strong, supporting upside and pricing power into the next quarter.",
        "Competition risk exists, but current demand strength still supports a favorable setup.",
      ].join(" "),
      url: "https://example.com/research/nvda-follow-through",
      publishedAt: "2026-03-04T09:00:00Z",
    });

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "semianalysis",
      sender: "digest@semianalysis.com",
      title: "AMD margin reset",
      subject: "AMD margin reset",
      ticker: "AMD",
      content: [
        "AMD revenue growth may slow to 9% because competition risk is rising and pricing pressure is increasing across AI accelerators.",
        "Gross margin could slip toward 58% while operating margin compresses as investor expectations remain stretched.",
      ].join(" "),
      url: "https://example.com/research/amd-margin-reset",
      publishedAt: "2026-03-02T11:00:00Z",
    });

    const comparison = compareExternalResearchPeers({
      leftTicker: "NVDA",
      rightTicker: "AMD",
      dbPath,
    });

    expect(comparison.left.ticker).toBe("NVDA");
    expect(comparison.right.ticker).toBe("AMD");
    expect(comparison.evidenceEdge).toContain("NVDA");
    expect(comparison.riskEdge).toContain("AMD");
    expect(comparison.markdown).toContain("Peer Comparison: NVDA vs AMD");
    expect(comparison.notableDeltas.length).toBeGreaterThanOrEqual(3);
  });
});
