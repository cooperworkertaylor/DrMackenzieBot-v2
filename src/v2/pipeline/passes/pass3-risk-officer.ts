import type { ReportKindV2 } from "../../quality/types.js";

export type RiskOfficerOutputV2 = {
  version: 1;
  generated_at: string;
  kind: ReportKindV2;
  subject: string;
  premortem: string[];
  bear_narrative: string[];
  disconfirming_evidence_to_seek: string[];
  falsifiers: Array<{ statement: string; trigger: string; verification_plan: string }>;
};

const nowIso = (): string => new Date().toISOString();

export function pass3RiskOfficerV2(params: {
  kind: ReportKindV2;
  subject: string;
}): RiskOfficerOutputV2 {
  return {
    version: 1,
    generated_at: nowIso(),
    kind: params.kind,
    subject: params.subject,
    premortem: [
      "We shipped a compelling story without sufficient Tier one and Tier two coverage, and the thesis failed on basic accounting or KPI reality.",
      "We anchored on one confirming source and ignored disconfirming data; contradictions accumulated silently.",
      "We sized risk before defining measurable triggers, turning research into opinion rather than a monitored system.",
    ],
    bear_narrative: [
      "The bear case is that the perceived edge is not durable: economics compress, execution misses accumulate, and the market reprices the multiple to reflect lower confidence.",
      "Even if the product is good, the capture mechanism may be weak (pricing pressure, commoditization, or customer concentration).",
    ],
    disconfirming_evidence_to_seek: [
      "Primary filings that contradict key KPI claims (restatements, segment changes, accounting shifts).",
      "Transcripts that reveal churn, pricing pressure, or demand softness versus narrative.",
      "Third-party or official datasets that contradict adoption claims (where applicable).",
    ],
    falsifiers: [
      {
        statement:
          "The thesis remains valid only if the core value driver continues to show up in reported KPIs.",
        trigger:
          "Two consecutive reporting periods with KPI deltas inconsistent with the thesis drivers.",
        verification_plan:
          "Track KPI deltas from Tier one filings and reconcile definitions before interpreting.",
      },
      {
        statement: "The thesis assumes durable capture mechanisms.",
        trigger:
          "Evidence of structural pricing pressure or commoditization that persists across sources.",
        verification_plan:
          "Cross-check filings, transcripts, and competitor disclosures for pricing and margin signals.",
      },
    ],
  };
}
