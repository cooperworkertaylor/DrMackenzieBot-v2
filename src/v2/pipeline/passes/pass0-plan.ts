import type { ReportKindV2 } from "../../quality/types.js";

export type ResearchPlanV2 = {
  version: 1;
  run_id: string;
  kind: ReportKindV2;
  generated_at: string;
  posture: "long-only";
  horizon: string;
  timebox_minutes: number;
  key_questions: string[];
  required_exhibits: string[];
  evidence_plan: {
    tier12_minimum: number;
    tier1_minimum: number;
    per_ticker: Array<{
      ticker: string;
      required: {
        filings: { min_items: number; preferred_forms: string[] };
        transcripts: { min_items: number };
        external_documents: { min_items: number; preferred_source_types: string[] };
      };
    }>;
  };
  tool_queries: Array<{
    tool: string;
    query: string;
    priority: "primary" | "secondary";
  }>;
};

const nowIso = (): string => new Date().toISOString();

export function buildPlanCompanyV2(params: {
  runId: string;
  ticker: string;
  question: string;
  horizon?: string;
  timeboxMinutes?: number;
}): ResearchPlanV2 {
  const ticker = params.ticker.trim().toUpperCase();
  const question = params.question.trim();
  return {
    version: 1,
    run_id: params.runId,
    kind: "company",
    generated_at: nowIso(),
    posture: "long-only",
    horizon: params.horizon ?? "12-36 months",
    timebox_minutes: Math.max(15, Math.min(600, params.timeboxMinutes ?? 90)),
    key_questions: [
      question,
      "What would falsify the thesis quickly (measurable triggers)?",
      "What is the bear case and what evidence would support it?",
    ],
    required_exhibits: [
      "kpi_table",
      "margins",
      "fcf",
      "sbc_dilution",
      "scenario_drivers",
      "sensitivity",
    ],
    evidence_plan: {
      tier12_minimum: 2,
      tier1_minimum: 1,
      per_ticker: [
        {
          ticker,
          required: {
            filings: {
              min_items: 4,
              preferred_forms: ["10-K", "10-Q", "8-K", "DEF 14A"],
            },
            transcripts: { min_items: 1 },
            external_documents: {
              min_items: 0,
              preferred_source_types: ["email_research", "newsletter"],
            },
          },
        },
      ],
    },
    tool_queries: [
      { tool: "sec", query: `${ticker} latest annual filing`, priority: "primary" },
      { tool: "sec", query: `${ticker} latest quarterly filing`, priority: "primary" },
      {
        tool: "transcript",
        query: `${ticker} latest earnings call transcript`,
        priority: "secondary",
      },
      { tool: "prices", query: `${ticker} price history`, priority: "secondary" },
    ],
  };
}

export function buildPlanThemeV2(params: {
  runId: string;
  themeName: string;
  universe: string[];
  horizon?: string;
  timeboxMinutes?: number;
}): ResearchPlanV2 {
  const themeName = params.themeName.trim();
  const universe = params.universe.map((t) => t.trim().toUpperCase()).filter(Boolean);
  return {
    version: 1,
    run_id: params.runId,
    kind: "theme",
    generated_at: nowIso(),
    posture: "long-only",
    horizon: params.horizon ?? "12-36 months",
    timebox_minutes: Math.max(15, Math.min(600, params.timeboxMinutes ?? 120)),
    key_questions: [
      `Define the theme precisely: ${themeName}`,
      "Where does value accrue (value chain) and what are the capture mechanisms?",
      "Who benefits vs gets left behind, and what are falsifiers and adoption signposts?",
    ],
    required_exhibits: [
      "value_chain_map",
      "capture_scorecard",
      "adoption_dashboard",
      "catalyst_calendar",
      "risk_heatmap",
    ],
    evidence_plan: {
      tier12_minimum: 2,
      tier1_minimum: 1,
      per_ticker: universe.map((ticker) => ({
        ticker,
        required: {
          filings: { min_items: 2, preferred_forms: ["10-K", "10-Q", "8-K"] },
          transcripts: { min_items: 1 },
          external_documents: {
            min_items: 0,
            preferred_source_types: ["newsletter", "email_research"],
          },
        },
      })),
    },
    tool_queries: [
      { tool: "corpus", query: `${themeName} definition taxonomy`, priority: "primary" },
      ...universe.map((ticker) => ({
        tool: "sec",
        query: `${ticker} latest filing`,
        priority: "secondary" as const,
      })),
    ],
  };
}
