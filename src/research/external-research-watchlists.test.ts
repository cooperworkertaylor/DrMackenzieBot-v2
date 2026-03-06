import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import { ingestExternalResearchDocument } from "./external-research.js";
import {
  addTickerToResearchWatchlist,
  buildDailyWatchlistBrief,
  buildDailyWatchlistBriefCronJob,
  storeDailyWatchlistBrief,
  upsertResearchWatchlist,
} from "./external-research-watchlists.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-external-watchlists-${name}-${Date.now()}-${Math.random()}.db`);

describe("external research watchlists", () => {
  it("queues watchlist refreshes and builds a daily brief from material changes", () => {
    const dbPath = testDbPath("daily-brief");
    const watchlist = upsertResearchWatchlist({
      name: "Core AI",
      description: "Highest-priority AI names",
      isDefault: true,
      dbPath,
    });
    addTickerToResearchWatchlist({
      watchlistId: watchlist.id,
      ticker: "NVDA",
      priority: 1,
      tags: ["ai", "core"],
      dbPath,
    });

    const first = ingestExternalResearchDocument({
      dbPath,
      sourceType: "email_research",
      provider: "other",
      sender: "cooptaylor1@gmail.com",
      title: "NVDA demand setup",
      subject: "RESEARCH NVDA demand setup",
      ticker: "NVDA",
      content: [
        "NVDA demand strength remains favorable because pricing discipline and demand strength are holding across accelerator programs.",
        "Pricing discipline and demand strength support revenue growth while guidance remains constructive.",
        "Gross margin could hold around 72% while operating margin remains above 45% if mix stays tilted toward high-end accelerators.",
      ].join(" "),
      url: "https://example.com/research/nvda-demand",
      publishedAt: "2026-03-01T10:00:00Z",
    });
    expect(first.watchlistRefreshId).toBeGreaterThan(0);

    const second = ingestExternalResearchDocument({
      dbPath,
      sourceType: "newsletter",
      provider: "semianalysis",
      sender: "digest@semianalysis.com",
      title: "NVDA thesis break",
      subject: "NVDA thesis break",
      ticker: "NVDA",
      content: [
        "Pricing discipline and demand strength are weakening, which contradicts the prior pricing discipline and demand strength assumptions.",
        "Competition risk is rising and margin pressure is increasing as custom silicon programs pressure valuation and guidance.",
        "Investors now face uncertainty because pricing discipline and demand strength may no longer hold in the next quarter.",
      ].join(" "),
      url: "https://example.com/research/nvda-break",
      publishedAt: "2026-03-04T11:00:00Z",
    });
    expect(second.watchlistRefreshId).toBeGreaterThan(first.watchlistRefreshId ?? 0);

    const brief = buildDailyWatchlistBrief({
      watchlistId: watchlist.id,
      lookbackDays: 7,
      dbPath,
    });
    expect(brief.title).toContain("Core AI");
    expect(brief.materialChanges.some((line) => line.startsWith("NVDA:"))).toBe(true);
    expect(
      brief.thesisBreaks.some((line) =>
        line.toLowerCase().includes("external research thesis break"),
      ),
    ).toBe(true);
    expect(brief.pendingRefreshes.some((line) => line.startsWith("NVDA:"))).toBe(true);
    expect(brief.nextActions.some((line) => line.includes("NVDA"))).toBe(true);

    const briefId = storeDailyWatchlistBrief({ brief, dbPath });
    expect(briefId).toBeGreaterThan(0);

    const db = openResearchDb(dbPath);
    const queued = db.prepare(
      `SELECT COUNT(*) AS count FROM research_refresh_queue WHERE watchlist_id=? AND status='queued'`,
    ).get(watchlist.id) as { count: number };
    expect(queued.count).toBe(2);

    const cronJob = buildDailyWatchlistBriefCronJob({
      watchlistId: watchlist.id,
      watchlistName: watchlist.name,
      hourEt: 8,
      minuteEt: 15,
    });
    expect(cronJob.schedule.kind).toBe("cron");
    expect(cronJob.schedule).toMatchObject({
      kind: "cron",
      expr: "15 8 * * *",
      tz: "America/New_York",
    });
    expect(cronJob.payload.kind).toBe("agentTurn");
  });
});
