import { describe, expect, it } from "vitest";
import { normalizeLlmReportCandidateV2 } from "./pass4-compile.js";

const demoFallbackThemeReport = () => ({
  version: 2,
  kind: "theme",
  run_id: "run-theme-1",
  generated_at: "2026-02-11T15:00:00Z",
  subject: {
    theme_name: "optical networking",
    universe: ["CIEN", "LITE", "ANET"],
  },
  plan: {
    posture: "long-only",
    horizon: "12-36 months",
    timebox_minutes: 30,
    key_questions: ["What matters?"],
    required_exhibits: ["value_chain_map"],
  },
  sources: [
    {
      id: "S1",
      title: "Source 1",
      publisher: "SEC",
      date_published: "2026-02-11",
      accessed_at: "2026-02-11T15:00:00Z",
      url: "https://example.com/s1",
      reliability_tier: 1,
      excerpt_or_key_points: ["k1"],
      tags: ["theme:optical-networking"],
    },
  ],
  numeric_facts: [],
  sections: [
    {
      key: "executive_summary",
      title: "Executive Summary",
      blocks: [{ tag: "FACT", text: "Fallback fact.", source_ids: ["S1"] }],
    },
    {
      key: "what_it_is_isnt_why_now",
      title: "What It Is / Isn't + Why Now",
      blocks: [{ tag: "INTERPRETATION", text: "Fallback." }],
    },
    {
      key: "value_chain",
      title: "Where Value Accrues",
      blocks: [{ tag: "INTERPRETATION", text: "Fallback." }],
    },
    {
      key: "capture_ledger",
      title: "Capture Ledger",
      blocks: [{ tag: "INTERPRETATION", text: "Fallback." }],
    },
    {
      key: "beneficiaries_vs_left_behind",
      title: "Beneficiaries vs Left Behind",
      blocks: [{ tag: "INTERPRETATION", text: "Fallback." }],
    },
    {
      key: "catalysts_timeline",
      title: "Catalysts + Timeline",
      blocks: [{ tag: "INTERPRETATION", text: "Fallback." }],
    },
    {
      key: "risks_falsifiers",
      title: "Risks + Falsifiers",
      blocks: [{ tag: "INTERPRETATION", text: "Fallback." }],
    },
    {
      key: "portfolio_posture",
      title: "Portfolio Posture",
      blocks: [{ tag: "INTERPRETATION", text: "Fallback." }],
    },
  ],
  exhibits: [
    {
      id: "X1",
      title: "Fallback Exhibit",
      question: "Q?",
      data_summary: ["N/A"],
      takeaway: "T",
      source_ids: ["S1"],
    },
  ],
  appendix: {
    evidence_table: [{ claim: "c1", evidence_ids: ["S1"], source_ids: ["S1"] }],
    whats_missing: ["m1"],
  },
});

describe("normalizeLlmReportCandidateV2", () => {
  it("normalizes malformed theme output into schema-ready shape", () => {
    const fallback = demoFallbackThemeReport();
    const normalized = normalizeLlmReportCandidateV2({
      kind: "theme",
      candidate: {
        plan: { horizon: "0-6 months", foo: "bar" },
        theme_name: "optical networking",
      },
      fallbackReport: fallback,
      now: "2026-02-11T16:00:00Z",
    }) as Record<string, unknown>;

    const plan = normalized.plan as Record<string, unknown>;
    expect(normalized.version).toBe(2);
    expect(normalized.kind).toBe("theme");
    expect(normalized.numeric_facts).toEqual([]);
    expect(Array.isArray(normalized.sections)).toBe(true);
    expect(Array.isArray(normalized.exhibits)).toBe(true);
    expect((normalized.appendix as Record<string, unknown>).evidence_table).toBeTruthy();
    expect(plan.posture).toBe("long-only");
    expect(plan.horizon).toBe("0-6 months");
    expect(plan.foo).toBeUndefined();
  });

  it("unwraps nested report payloads and preserves valid fields", () => {
    const fallback = demoFallbackThemeReport();
    const normalized = normalizeLlmReportCandidateV2({
      kind: "theme",
      candidate: {
        report: {
          run_id: "run-theme-nested",
          subject: { theme_name: "new theme", universe: ["COHR"] },
          plan: {
            horizon: "6-18 months",
            timebox_minutes: 45,
            key_questions: ["k"],
            required_exhibits: ["x"],
          },
          sections: [
            {
              key: "executive_summary",
              title: "Exec",
              blocks: [{ tag: "FACT", text: "Nested fact", source_ids: ["S1"] }],
            },
          ],
        },
      },
      fallbackReport: fallback,
      now: "2026-02-11T16:00:00Z",
    }) as Record<string, unknown>;

    expect(normalized.run_id).toBe("run-theme-nested");
    expect((normalized.subject as Record<string, unknown>).theme_name).toBe("new theme");
    expect((normalized.plan as Record<string, unknown>).horizon).toBe("6-18 months");
    expect(Array.isArray(normalized.sections)).toBe(true);
  });
});
