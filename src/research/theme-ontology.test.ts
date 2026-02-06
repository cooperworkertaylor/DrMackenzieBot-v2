import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";
import {
  getThemeConstituents,
  listThemeDefinitions,
  refreshThemeMembership,
  upsertThemeDefinition,
} from "./theme-ontology.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-theme-ontology-${name}-${Date.now()}-${Math.random()}.db`);

const seedInstruments = (dbPath: string) => {
  const db = openResearchDb(dbPath);
  const now = Date.now();
  db.prepare(
    `INSERT OR REPLACE INTO instruments (ticker, name, sector, industry, updated_at)
     VALUES (?, ?, ?, ?, ?)`,
  ).run("NVDA", "NVIDIA Corp", "Technology", "Semiconductors", now);
  db.prepare(
    `INSERT OR REPLACE INTO instruments (ticker, name, sector, industry, updated_at)
     VALUES (?, ?, ?, ?, ?)`,
  ).run("MSFT", "Microsoft Corp", "Technology", "Software", now + 1);
  db.prepare(
    `INSERT OR REPLACE INTO instruments (ticker, name, sector, industry, updated_at)
     VALUES (?, ?, ?, ?, ?)`,
  ).run("XOM", "Exxon Mobil", "Energy", "Oil & Gas", now + 2);
};

describe("theme ontology", () => {
  it("upserts versioned definitions and toggles active version", () => {
    const dbPath = testDbPath("versions");
    seedInstruments(dbPath);

    const v1 = upsertThemeDefinition({
      theme: "ai-infrastructure",
      displayName: "AI Infrastructure",
      description: "GPU and software stack",
      benchmark: "SOXX",
      rules: {
        includeKeywords: ["ai", "semiconductor"],
        requiredSectors: ["technology"],
        minMembershipScore: 0.6,
      },
      activate: true,
      dbPath,
    });
    const v2 = upsertThemeDefinition({
      theme: "ai-infrastructure",
      displayName: "AI Infra v2",
      rules: {
        includeKeywords: ["software", "cloud"],
        requiredSectors: ["technology"],
      },
      activate: true,
      dbPath,
    });
    expect(v1.version).toBe(1);
    expect(v2.version).toBe(2);

    const rows = listThemeDefinitions({
      theme: "ai-infrastructure",
      includeInactive: true,
      dbPath,
    });
    const latest = rows.find((row) => row.version === 2);
    const prior = rows.find((row) => row.version === 1);
    expect(latest?.status).toBe("active");
    expect(prior?.status).toBe("inactive");
  });

  it("refreshes membership scores and returns active constituents", () => {
    const dbPath = testDbPath("membership");
    seedInstruments(dbPath);
    upsertThemeDefinition({
      theme: "ai-infrastructure",
      displayName: "AI Infrastructure",
      rules: {
        includeKeywords: ["semiconductor", "software"],
        requiredSectors: ["technology"],
        tickerAllowlist: ["NVDA"],
        tickerBlocklist: ["XOM"],
        minMembershipScore: 0.58,
      },
      activate: true,
      dbPath,
    });

    const refresh = refreshThemeMembership({
      theme: "ai-infrastructure",
      dbPath,
    });
    expect(refresh.candidatesScored).toBeGreaterThanOrEqual(3);
    expect(refresh.activeCount).toBeGreaterThanOrEqual(1);

    const active = getThemeConstituents({
      theme: "ai-infrastructure",
      status: "active",
      dbPath,
    });
    expect(active.some((row) => row.ticker === "NVDA")).toBe(true);
    expect(active.every((row) => row.membershipScore >= 0.58)).toBe(true);
    const excluded = getThemeConstituents({
      theme: "ai-infrastructure",
      status: "excluded",
      dbPath,
      includeInactive: true,
    });
    expect(excluded.some((row) => row.ticker === "XOM")).toBe(true);
  });
});
