import { describe, expect, it } from "vitest";
import { EvidenceStore, canonicalizeUrl, inferReliabilityTier } from "./evidence-store.js";

describe("canonicalizeUrl", () => {
  it("strips www, trailing slash, and tracking params", () => {
    expect(canonicalizeUrl("https://www.sec.gov/edgar/search/?utm_source=x&b=2&a=1#ignored")).toBe(
      "https://sec.gov/edgar/search?a=1&b=2",
    );
  });
});

describe("inferReliabilityTier", () => {
  it("classifies sec.gov as tier 1", () => {
    expect(inferReliabilityTier("https://www.sec.gov/Archives/edgar/data/")).toBe(1);
  });

  it("defaults to tier 4", () => {
    expect(inferReliabilityTier("https://example.com/foo")).toBe(4);
  });
});

describe("EvidenceStore", () => {
  it("dedupes on canonical URL and preserves id", () => {
    const store = new EvidenceStore();
    const first = store.add({
      title: "Title A",
      publisher: "Pub",
      date_published: "2026-02-10",
      accessed_at: "2026-02-10T00:00:00Z",
      url: "https://www.sec.gov/edgar/search/?utm_source=x",
      excerpt_or_key_points: ["a"],
      tags: ["t1"],
    });
    const second = store.add({
      title: "Title A (dup)",
      publisher: "Pub",
      date_published: "2026-02-10",
      accessed_at: "2026-02-10T00:00:00Z",
      url: "https://sec.gov/edgar/search/",
      excerpt_or_key_points: ["b"],
      tags: ["t2"],
    });
    expect(second.id).toBe(first.id);
    expect(store.all()).toHaveLength(1);
    expect(store.all()[0]?.excerpt_or_key_points).toEqual(["a", "b"]);
    expect(store.all()[0]?.tags.sort()).toEqual(["t1", "t2"]);
  });
});
