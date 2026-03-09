import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  buildPersonalizedResearchSnapshot,
  ingestResearchNotebookEntry,
  listResearchNotebookEntries,
  listResearchUserPreferences,
  upsertResearchUserPreference,
} from "./external-research-personalization.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-external-research-personalization-${name}-${Date.now()}-${Math.random()}.db`);

describe("external research personalization", () => {
  it("persists user preferences", () => {
    const dbPath = testDbPath("preferences");

    upsertResearchUserPreference({
      key: "risk_focus",
      valueText: "protect downside and demand durability",
      dbPath,
    });
    upsertResearchUserPreference({
      key: "favorite_sources",
      valueJson: { primary: ["semianalysis", "stratechery"] },
      dbPath,
    });

    const rows = listResearchUserPreferences({ dbPath });
    expect(rows.length).toBe(2);
    expect(rows.find((row) => row.key === "risk_focus")?.valueText).toContain("downside");
    expect(rows.find((row) => row.key === "favorite_sources")?.valueJson).toEqual({
      primary: ["semianalysis", "stratechery"],
    });
  });

  it("ingests notebook entries into the evidence graph and builds personalized snapshots", () => {
    const dbPath = testDbPath("notebook");

    upsertResearchUserPreference({
      key: "preferred_style",
      valueText: "concise with explicit bear case first",
      dbPath,
    });

    const result = ingestResearchNotebookEntry({
      dbPath,
      ticker: "NVDA",
      title: "NVDA notebook variant",
      content: [
        "Management guidance remains constructive because demand remains strong and pricing discipline still supports revenue growth near 24%.",
        "Gross margin could hold around 72% while operating margin remains above 45% if mix stays favorable.",
        "Main risk: custom silicon competition could pressure pricing power in the second half.",
      ].join(" "),
      tags: ["variant", "priority"],
      source: "personal_notes",
    });

    expect(result.entry.externalDocumentId).toBeGreaterThan(0);
    expect(result.ingest.reportId).toBeGreaterThan(0);
    expect(result.ingest.thesisId).toBeGreaterThan(0);

    const notebookRows = listResearchNotebookEntries({
      ticker: "NVDA",
      dbPath,
    });
    expect(notebookRows.length).toBe(1);
    expect(notebookRows[0]?.tags).toContain("notebook");

    const snapshot = buildPersonalizedResearchSnapshot({
      ticker: "NVDA",
      dbPath,
    });
    expect(snapshot.preferences.length).toBe(1);
    expect(snapshot.notebookEntries.length).toBe(1);
    expect(snapshot.summary).toContain("Preference lens:");
    expect(snapshot.markdown).toContain("NVDA Personalized Snapshot");
  });
});
