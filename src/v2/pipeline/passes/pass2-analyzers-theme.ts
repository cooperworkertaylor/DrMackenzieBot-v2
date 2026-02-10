import type { EvidenceItem } from "../../evidence/evidence-store.js";
import type { ThemeUniverseEntityV2 } from "../../quality/types.js";

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

const parseBoolean = (raw: string | undefined): boolean | undefined => {
  if (typeof raw !== "string") return undefined;
  const normalized = raw.trim().toLowerCase();
  if (!normalized) return undefined;
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return undefined;
};

const v2LlmEnabled = (): boolean =>
  parseBoolean(process.env.OPENCLAW_RESEARCH_V2_LLM_ANALYZERS) ??
  (String(process.env.OPENCLAW_HOST_ROLE ?? "")
    .trim()
    .toLowerCase() === "macmini" &&
    parseBoolean(process.env.OPENCLAW_RESEARCH_V2_LLM) !== false);

const pickEvidenceForLlm = (evidence: EvidenceItem[], maxItems: number): EvidenceItem[] => {
  const sorted = [...evidence].sort((a, b) => {
    const tier = (a.reliability_tier ?? 4) - (b.reliability_tier ?? 4);
    if (tier !== 0) return tier;
    const da = String(a.date_published ?? "");
    const db = String(b.date_published ?? "");
    return db.localeCompare(da);
  });
  return sorted.slice(0, Math.max(1, Math.min(40, maxItems)));
};

export async function pass2ThemeAnalyzersV2(params: {
  themeName: string;
  universe: string[];
  universeEntities?: ThemeUniverseEntityV2[];
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

  if (v2LlmEnabled()) {
    const { completeJsonWithResearchV2Model } = await import("../../llm/complete-json.js");
    const picked = pickEvidenceForLlm(params.evidence, 18);
    const prompt = [
      "You are a senior thematic equity + tech infrastructure analyst.",
      "Goal: produce a structured theme analyzer JSON object for an institutional memo pipeline.",
      "",
      "Hard rules:",
      "- Output JSON only. No markdown fences.",
      "- Do not invent facts. If evidence is missing, write the field as a conservative placeholder and add a note in notes[].",
      "- numeric_facts: only include numbers that are explicitly present in the evidence items below. Every numeric_facts entry must cite source_id (S#) and use that source's accessed_at timestamp.",
      "- Keep text concise and decision-useful.",
      "",
      "Theme:",
      JSON.stringify(
        {
          theme_name: themeName,
          universe,
          universe_entities: params.universeEntities ?? [],
        },
        null,
        2,
      ),
      "",
      "Evidence (prioritized, use these IDs in citations):",
      JSON.stringify(
        picked.map((s) => ({
          id: s.id,
          title: s.title,
          publisher: s.publisher,
          date_published: s.date_published,
          accessed_at: s.accessed_at,
          url: s.url,
          reliability_tier: s.reliability_tier,
          key_points: (s.excerpt_or_key_points ?? []).slice(0, 8),
          tags: (s.tags ?? []).slice(0, 16),
        })),
        null,
        2,
      ),
      "",
      "Return a JSON object with this exact shape:",
      JSON.stringify(
        {
          version: 1,
          generated_at: "ISO_TIMESTAMP",
          theme_name: "string",
          universe: ["TICKER"],
          notes: ["string"],
          definition_taxonomy: {
            definition: "string",
            what_it_is_not: ["string"],
            key_terms: ["string"],
          },
          value_chain: [{ layer: "string", value_driver: "string", proof_required: "string" }],
          capture_mechanisms: [
            { mechanism: "string", how_it_captures: "string", falsifier: "string" },
          ],
          beneficiaries_losers: {
            beneficiaries: [{ name: "string", why: "string" }],
            left_behind: [{ name: "string", why: "string" }],
          },
          adoption_signposts: [{ signpost: "string", source_needed: "string" }],
          numeric_facts: [
            {
              id: "N1",
              value: 0,
              unit: "string",
              period: "string",
              currency: "USD",
              source_id: "S1",
              accessed_at: "ISO_TIMESTAMP",
              notes: "string",
            },
          ],
        },
        null,
        2,
      ),
    ].join("\n");

    const out = (await completeJsonWithResearchV2Model({
      purpose: "analyzer",
      prompt,
      maxTokens: 2200,
      temperature: 0.2,
      profileId: process.env.OPENCLAW_RESEARCH_V2_PROFILE?.trim() || undefined,
    })) as ThemeAnalyzerOutputV2;

    // Ensure stable metadata even if the model forgets.
    out.version = 1;
    out.generated_at = out.generated_at?.trim() ? out.generated_at : nowIso();
    out.theme_name = out.theme_name?.trim() ? out.theme_name : themeName;
    out.universe = Array.isArray(out.universe) && out.universe.length ? out.universe : universe;
    out.notes = Array.isArray(out.notes) ? Array.from(new Set([...notes, ...out.notes])) : notes;
    return out;
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
