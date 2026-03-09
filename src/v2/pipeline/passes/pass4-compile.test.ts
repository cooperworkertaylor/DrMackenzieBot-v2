import { afterEach, describe, expect, it, vi } from "vitest";
import { buildPlanCompanyV2 } from "./pass0-plan.js";
import { pass3RiskOfficerV2 } from "./pass3-risk-officer.js";
import { normalizeLlmReportCandidateV2, pass4CompileReportV2 } from "./pass4-compile.js";

const completeJsonWithResearchV2ModelMock = vi.hoisted(() => vi.fn());

vi.mock("../../llm/complete-json.js", () => ({
  completeJsonWithResearchV2Model: completeJsonWithResearchV2ModelMock,
}));

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

  it("falls back to the deterministic report when the writer output fails the quality gate", async () => {
    const previousWriterFlag = process.env.OPENCLAW_RESEARCH_V2_LLM_WRITER;
    process.env.OPENCLAW_RESEARCH_V2_LLM_WRITER = "1";
    completeJsonWithResearchV2ModelMock.mockReset();
    completeJsonWithResearchV2ModelMock.mockResolvedValue({
      version: 2,
      kind: "company",
      run_id: "bad-run",
      generated_at: "2026-02-11T16:00:00Z",
      subject: { ticker: "NVDA" },
      plan: {
        posture: "long-only",
        horizon: "12-36 months",
        timebox_minutes: 5,
        key_questions: ["What matters?"],
        required_exhibits: ["kpi_table"],
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
          raw_text_ref: "filings/nvda-10k.txt",
          tags: ["company:NVDA", "source:sec", "type:filing"],
        },
        {
          id: "S2",
          title: "Source 2",
          publisher: "NVIDIA IR",
          date_published: "2026-02-11",
          accessed_at: "2026-02-11T15:05:00Z",
          url: "https://example.com/s2",
          reliability_tier: 2,
          tags: ["company:NVDA", "type:official_release"],
        },
      ],
      numeric_facts: [],
      sections: [
        {
          key: "executive_summary",
          title: "Executive Summary",
          blocks: [
            { tag: "FACT", text: "Reported growth was 24 percent.", source_ids: ["S1"] },
            { tag: "INTERPRETATION", text: "Demand remains strong.", source_ids: ["S1"] },
          ],
        },
      ],
      exhibits: [
        {
          id: "Exhibit 1",
          title: "Exhibit",
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

    const result = await pass4CompileReportV2({
      kind: "company",
      runId: "run-company-1",
      subject: { ticker: "NVDA", companyName: "NVIDIA" },
      plan: buildPlanCompanyV2({
        runId: "run-company-1",
        ticker: "NVDA",
        question: "What matters?",
        timeboxMinutes: 5,
      }),
      evidence: [
        {
          id: "S1",
          title: "Source 1",
          publisher: "SEC",
          date_published: "2026-02-11",
          accessed_at: "2026-02-11T15:00:00Z",
          url: "https://example.com/s1",
          reliability_tier: 1,
          raw_text_ref: "filings/nvda-10k.txt",
          excerpt_or_key_points: ["k1"],
          tags: ["company:NVDA", "source:sec", "type:filing"],
        },
        {
          id: "S2",
          title: "Source 2",
          publisher: "NVIDIA IR",
          date_published: "2026-02-11",
          accessed_at: "2026-02-11T15:05:00Z",
          url: "https://example.com/s2",
          reliability_tier: 2,
          excerpt_or_key_points: ["k2"],
          tags: ["company:NVDA", "type:official_release"],
        },
      ],
      analyzers: {
        version: 1,
        generated_at: "2026-02-11T16:00:00Z",
        ticker: "NVDA",
        notes: [],
        extracts: { filings: [], transcripts: [] },
        risk_factor_buckets: [],
        accounting_flags: [],
        catalyst_candidates: [],
        catalyst_calendar: [],
        catalyst_ranked: [],
        numeric_facts: [],
        kpi_table: [],
      },
      risk: pass3RiskOfficerV2({ kind: "company", subject: "NVDA" }),
    });

    expect(result.gate.passed).toBe(true);
    const report = result.reportJson as {
      sections: Array<{ key: string; blocks: Array<{ tag: string; text: string }> }>;
    };
    expect(
      report.sections
        .find((section) => section.key === "variant_perception")
        ?.blocks.some((block) => block.tag === "ASSUMPTION"),
    ).toBe(true);
    expect(result.reportMarkdown).toContain("## Executive Summary (Base/Bull/Bear)");
    if (typeof previousWriterFlag === "string") {
      process.env.OPENCLAW_RESEARCH_V2_LLM_WRITER = previousWriterFlag;
    } else {
      delete process.env.OPENCLAW_RESEARCH_V2_LLM_WRITER;
    }
  });

  it("keeps deterministic company reports prose-safe when catalyst labels contain digits", async () => {
    const result = await pass4CompileReportV2({
      kind: "company",
      runId: "run-company-catalysts",
      subject: { ticker: "NVDA", companyName: "NVIDIA" },
      plan: buildPlanCompanyV2({
        runId: "run-company-catalysts",
        ticker: "NVDA",
        question: "What matters?",
        timeboxMinutes: 5,
      }),
      evidence: [
        {
          id: "S1",
          title: "Source 1",
          publisher: "SEC",
          date_published: "2026-02-11",
          accessed_at: "2026-02-11T15:00:00Z",
          url: "https://example.com/s1",
          reliability_tier: 1,
          raw_text_ref: "filings/nvda-10k.txt",
          excerpt_or_key_points: ["k1"],
          tags: ["company:NVDA", "source:sec", "type:filing"],
        },
        {
          id: "S2",
          title: "Source 2",
          publisher: "NVIDIA IR",
          date_published: "2026-02-12",
          accessed_at: "2026-02-12T15:00:00Z",
          url: "https://example.com/s2",
          reliability_tier: 2,
          excerpt_or_key_points: ["k2"],
          tags: ["company:NVDA", "type:official_release"],
        },
      ],
      analyzers: {
        version: 1,
        generated_at: "2026-02-11T16:00:00Z",
        ticker: "NVDA",
        notes: [],
        extracts: { filings: [], transcripts: [] },
        risk_factor_buckets: [],
        accounting_flags: [],
        catalyst_candidates: [
          { label: "Q4 2026 earnings call", source_ids: ["S1", "S2"] },
          { label: "10-Q filing and H2 product launch", source_ids: ["S1", "S2"] },
        ],
        catalyst_calendar: [
          { date: "2026-11-19", label: "Q4 2026 earnings call", source_ids: ["S1", "S2"] },
        ],
        catalyst_ranked: [
          {
            rank: 1,
            date: "2026-11-19",
            label: "Q4 2026 earnings call",
            why_it_matters: "Resets FY2027 expectations",
            what_changes: "Street model confidence",
            what_to_watch: "Revenue guide and margin cadence",
          },
        ],
        numeric_facts: [],
        kpi_table: [],
      },
      risk: pass3RiskOfficerV2({ kind: "company", subject: "NVDA" }),
    });

    expect(result.gate.passed).toBe(true);
  });

  it("builds investor-facing company sections from analyzer outputs instead of generic filler", async () => {
    const result = await pass4CompileReportV2({
      kind: "company",
      runId: "run-company-investor",
      subject: { ticker: "NVDA", companyName: "NVIDIA" },
      plan: buildPlanCompanyV2({
        runId: "run-company-investor",
        ticker: "NVDA",
        question: "What matters?",
        timeboxMinutes: 5,
      }),
      evidence: [
        {
          id: "S1",
          title: "NVDA 10-K",
          publisher: "SEC",
          date_published: "2026-02-11",
          accessed_at: "2026-02-11T15:00:00Z",
          url: "https://example.com/s1",
          reliability_tier: 1,
          raw_text_ref: "filings/nvda-10k.txt",
          excerpt_or_key_points: ["k1"],
          tags: ["company:NVDA", "source:sec", "type:filing"],
        },
        {
          id: "S2",
          title: "NVDA earnings transcript",
          publisher: "NVIDIA IR",
          date_published: "2026-02-12",
          accessed_at: "2026-02-12T15:00:00Z",
          url: "https://example.com/s2",
          reliability_tier: 2,
          raw_text_ref: "transcripts/nvda-q4.txt",
          excerpt_or_key_points: ["k2"],
          tags: ["company:NVDA", "type:transcript"],
        },
      ],
      analyzers: {
        version: 1,
        generated_at: "2026-02-11T16:00:00Z",
        ticker: "NVDA",
        notes: [],
        extracts: {
          filings: [
            {
              source_id: "S1",
              title: "NVDA 10-K",
              url: "https://example.com/s1",
              date_published: "2026-02-11",
              extracted: {
                business_keywords: ["datacenter", "networking", "inference", "software"],
                risk_keywords: ["competition", "pricing", "supply", "customer"],
              },
            },
          ],
          transcripts: [
            {
              source_id: "S2",
              title: "NVDA earnings transcript",
              url: "https://example.com/s2",
              date_published: "2026-02-12",
              extracted: {
                keywords: ["demand", "backlog", "pricing", "capacity"],
              },
            },
          ],
        },
        risk_factor_buckets: [
          {
            bucket: "Competition / pricing",
            keywords: ["competition", "pricing"],
            source_ids: ["S1"],
          },
          {
            bucket: "Supply chain / vendors",
            keywords: ["supply"],
            source_ids: ["S1"],
          },
        ],
        accounting_flags: [
          {
            flag: "Stock-based compensation",
            evidence: "Mentions stock-based compensation in filing text.",
            source_ids: ["S1"],
          },
        ],
        catalyst_candidates: [
          {
            label: "Earnings transcript follow-through",
            rationale: "next transcript can confirm backlog and pricing discipline",
            source_ids: ["S2"],
          },
        ],
        catalyst_calendar: [
          {
            date: "2026-03-20",
            label: "Earnings transcript follow-through",
            source_ids: ["S2"],
          },
        ],
        catalyst_ranked: [
          {
            rank: "A",
            date: "2026-03-20",
            label: "Earnings transcript follow-through",
            why_it_matters: "confirms backlog and pricing",
            what_changes: "confidence in demand durability",
            what_to_watch: "capacity and pricing commentary",
            source_ids: ["S2"],
          },
        ],
        numeric_facts: [
          {
            id: "N1",
            value: 72,
            unit: "percent",
            period: "latest-quarter",
            source_id: "S1",
            accessed_at: "2026-02-11T15:00:00Z",
          },
        ],
        kpi_table: [
          {
            metric: "Gross margin",
            numeric_id: "N1",
            as_of: "2026-02-11",
            form: "10-K",
          },
        ],
      },
      risk: pass3RiskOfficerV2({ kind: "company", subject: "NVDA" }),
    });

    expect(result.gate.passed).toBe(true);
    expect(result.reportMarkdown).toContain("datacenter");
    expect(result.reportMarkdown).toContain("networking");
    expect(result.reportMarkdown).toContain("demand");
    expect(result.reportMarkdown).toContain("Competition / pricing");
    expect(result.reportMarkdown).toContain("Stock-based compensation");
    expect(result.reportMarkdown).toContain("Earnings transcript follow-through");
  });
});

afterEach(() => {
  completeJsonWithResearchV2ModelMock.mockReset();
  delete process.env.OPENCLAW_RESEARCH_V2_LLM_WRITER;
});
