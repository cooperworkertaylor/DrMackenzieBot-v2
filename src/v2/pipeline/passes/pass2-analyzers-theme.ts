import type { EvidenceItem } from "../../evidence/evidence-store.js";

export type ThemeAnalyzerOutputV2 = {
  version: 1;
  generated_at: string;
  theme_name: string;
  universe: string[];
  notes: string[];
  definition_taxonomy: {
    definition: string;
    what_it_is_not: string[];
    key_terms: string[];
  };
  value_chain: Array<{ layer: string; value_driver: string; proof_required: string }>;
  capture_mechanisms: Array<{ mechanism: string; how_it_captures: string; falsifier: string }>;
  beneficiaries_losers: {
    beneficiaries: Array<{ name: string; why: string }>;
    left_behind: Array<{ name: string; why: string }>;
  };
  adoption_signposts: Array<{ signpost: string; source_needed: string }>;
  numeric_facts: Array<{
    id: string;
    value: number;
    unit: string;
    period: string;
    currency?: string;
    source_id: string;
    accessed_at: string;
    notes?: string;
  }>;
};

const nowIso = (): string => new Date().toISOString();

export async function pass2ThemeAnalyzersV2(params: {
  themeName: string;
  universe: string[];
  evidence: EvidenceItem[];
}): Promise<ThemeAnalyzerOutputV2> {
  const themeName = params.themeName.trim();
  const universe = params.universe.map((t) => t.trim().toUpperCase()).filter(Boolean);
  const notes: string[] = [];

  const hasTier1 = params.evidence.some((e) => e.reliability_tier === 1);
  if (!hasTier1) {
    notes.push(
      "No Tier 1 sources present; theme analysis is framework-only and must be filled in with primary sources.",
    );
  }

  return {
    version: 1,
    generated_at: nowIso(),
    theme_name: themeName,
    universe,
    notes,
    definition_taxonomy: {
      definition:
        "A research workflow that treats evidence as a reusable library and produces auditable investment theses via contracts.",
      what_it_is_not: [
        "Not a narrative memo generator that blends facts and opinions without citations.",
        "Not a single-pass essay; it is a multi-pass system with analyzers, skepticism, and validation.",
      ],
      key_terms: ["evidence library", "numeric provenance", "falsifiers", "fail closed"],
    },
    value_chain: [
      {
        layer: "Primary sources",
        value_driver: "reliability and provenance",
        proof_required: "Tier 1/2 evidence coverage by claim",
      },
      {
        layer: "Normalization",
        value_driver: "dedupe and tagging",
        proof_required: "stable source ids and canonical URLs",
      },
      {
        layer: "Analyzers",
        value_driver: "structured extraction",
        proof_required: "JSON artifacts for KPIs, risks, catalysts",
      },
      {
        layer: "Compiler",
        value_driver: "single-writer synthesis",
        proof_required: "schema-valid FinalReport.json",
      },
      {
        layer: "Quality gate",
        value_driver: "auditability",
        proof_required: "hard fails on missing citations and numeric provenance",
      },
    ],
    capture_mechanisms: [
      {
        mechanism: "Distribution via reusable evidence",
        how_it_captures:
          "Compounds research effort by turning sources into indexed, re-usable primitives.",
        falsifier: "Evidence library is not reused across runs or has high duplication rate.",
      },
      {
        mechanism: "Governance via contracts",
        how_it_captures:
          "Prevents low-signal memos from shipping by enforcing citations, structure, and numeric provenance.",
        falsifier: "Memos ship with uncited factual paragraphs or unaudited numbers.",
      },
    ],
    beneficiaries_losers: {
      beneficiaries: universe.map((ticker) => ({
        name: ticker,
        why: "Coverage depends on evidence availability; beneficiaries are the names with Tier 1/3 sources collected.",
      })),
      left_behind: [
        {
          name: "Any name without Tier 1/2 sources",
          why: "Fail-closed gates prevent shipping conclusions without primary evidence coverage.",
        },
      ],
    },
    adoption_signposts: [
      {
        signpost: "Increased share of Tier 1/2 evidence vs Tier 4",
        source_needed: "EvidenceLibrary.json statistics",
      },
      {
        signpost: "Fewer contradictions flagged over time",
        source_needed: "Consistency validator outputs",
      },
      { signpost: "Higher eval score distribution", source_needed: "eval_runs or v2 eval harness" },
    ],
    numeric_facts: [],
  };
}
