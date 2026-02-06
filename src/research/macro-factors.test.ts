import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  __testOnly,
  listMacroFactorObservations,
  loadMacroFactorSeries,
  upsertMacroFactorObservations,
} from "./macro-factors.js";

const testDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-macro-factors-${name}-${Date.now()}-${Math.random()}.db`);

describe("macro factors", () => {
  it("normalizes known factor aliases", () => {
    expect(__testOnly.toFactorKey("rate")).toBe("rates");
    expect(__testOnly.toFactorKey("credit-spread")).toBe("credit_spread");
    expect(__testOnly.toFactorKey("usd")).toBe("dollar");
    expect(__testOnly.toFactorKey("wti")).toBe("oil");
    expect(__testOnly.toFactorKey("volatility")).toBe("vix");
  });

  it("upserts, lists, and loads factor observations", () => {
    const dbPath = testDbPath("roundtrip");
    const inserted = upsertMacroFactorObservations({
      factorKey: "rates",
      observations: [
        { date: "2025-01-02", value: 0.01, source: "test_source" },
        { date: "2025-01-03", value: 0.015, source: "test_source" },
      ],
      dbPath,
    });
    expect(inserted).toBe(2);

    upsertMacroFactorObservations({
      factorKey: "rates",
      observations: [{ date: "2025-01-03", value: 0.02, source: "test_source" }],
      dbPath,
    });
    upsertMacroFactorObservations({
      factorKey: "vix",
      observations: [{ date: "2025-01-03", value: 0.03, source: "test_source" }],
      dbPath,
    });

    const listed = listMacroFactorObservations({
      factorKey: "rates",
      limit: 10,
      dbPath,
    });
    expect(listed.length).toBe(2);
    expect(listed[0]?.date).toBe("2025-01-03");
    expect(listed[0]?.value).toBeCloseTo(0.02, 8);

    const series = loadMacroFactorSeries({
      factorKeys: ["rates", "vix"],
      startDate: "2025-01-02",
      endDate: "2025-01-03",
      dbPath,
    });
    expect(series.rates?.get("2025-01-02")).toBeCloseTo(0.01, 8);
    expect(series.rates?.get("2025-01-03")).toBeCloseTo(0.02, 8);
    expect(series.vix?.get("2025-01-03")).toBeCloseTo(0.03, 8);
  });
});
