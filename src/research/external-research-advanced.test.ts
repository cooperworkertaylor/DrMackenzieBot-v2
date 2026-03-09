import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ingestExternalResearchDocument } from "./external-research.js";
import {
  analyzeExternalResearchGuidanceDrift,
  analyzeExternalResearchManagementCredibility,
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

  it("analyzes guidance drift from the stored fact series", () => {
    const dbPath = testDbPath("guidance-drift");

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "cooptaylor1@gmail.com",
      title: "NVDA guide baseline",
      subject: "RESEARCH NVDA guide baseline",
      ticker: "NVDA",
      content: [
        "NVDA revenue growth could sustain 24% because pricing discipline remains strong across accelerator demand.",
        "Gross margin could hold around 72% while operating margin remains above 45% if mix stays favorable.",
      ].join(" "),
      url: "https://example.com/research/nvda-guide-baseline",
      publishedAt: "2026-03-01T10:00:00Z",
    });

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "semianalysis",
      sender: "digest@semianalysis.com",
      title: "NVDA guide revision",
      subject: "NVDA guide revision",
      ticker: "NVDA",
      content: [
        "NVDA revenue growth may slow to 18% as competition risk rises and near-term pricing becomes more promotional.",
        "Gross margin could slip toward 68% while operating margin could compress to 40% as supply normalizes.",
      ].join(" "),
      url: "https://example.com/research/nvda-guide-revision",
      publishedAt: "2026-03-08T10:00:00Z",
    });

    const report = analyzeExternalResearchGuidanceDrift({
      ticker: "NVDA",
      dbPath,
    });

    expect(report.items.length).toBeGreaterThan(0);
    expect(report.items.some((item) => item.metricKey === "revenue_growth_pct")).toBe(true);
    expect(report.markdown).toContain("NVDA Guidance Drift");
  });

  it("tracks management credibility from management and guidance claim contradictions", () => {
    const dbPath = testDbPath("management-credibility");

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "cooptaylor1@gmail.com",
      title: "NVDA management baseline",
      subject: "RESEARCH NVDA management baseline",
      ticker: "NVDA",
      content: [
        "Management guidance remains constructive because pricing discipline should hold and demand remains strong across core accelerator programs.",
        "Management expects gross margin to remain resilient while the current pricing discipline framework holds.",
      ].join(" "),
      url: "https://example.com/research/nvda-management-baseline",
      publishedAt: "2026-03-01T10:00:00Z",
    });

    ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "stratechery",
      sender: "updates@stratechery.com",
      title: "NVDA management reset",
      subject: "NVDA management reset",
      ticker: "NVDA",
      content: [
        "Management guidance is weakening because prior pricing discipline assumptions no longer hold as competition risk rises across accelerator programs.",
        "The earlier management outlook now looks too constructive, and management commentary is being contradicted by current demand softness.",
      ].join(" "),
      url: "https://example.com/research/nvda-management-reset",
      publishedAt: "2026-03-12T10:00:00Z",
    });

    const report = analyzeExternalResearchManagementCredibility({
      ticker: "NVDA",
      dbPath,
    });

    expect(report.trackedClaims).toBeGreaterThan(0);
    expect(report.contradictedClaims).toBeGreaterThan(0);
    expect(report.alerts.length).toBeGreaterThan(0);
    expect(report.markdown).toContain("NVDA Management Credibility");
  });
});
