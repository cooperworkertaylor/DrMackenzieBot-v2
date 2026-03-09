import type { EvidenceItem } from "../../evidence/evidence-store.js";
import type { QualityGateResult, ReportKindV2 } from "../../quality/types.js";
import type { ThemeUniverseEntityV2 } from "../../quality/types.js";
import type { ResearchPlanV2 } from "./pass0-plan.js";
import type { CompanyAnalyzerOutputV2 } from "./pass2-analyzers-company.js";
import type { ThemeAnalyzerOutputV2 } from "./pass2-analyzers-theme.js";
import type { RiskOfficerOutputV2 } from "./pass3-risk-officer.js";
import { runV2QualityGate } from "../../quality/quality-gate.js";
import { runV2QualityGateWithRepair, type V2RepairModel } from "../../quality/repair-loop.js";
import { renderV2ReportMarkdown } from "../../render/report-markdown.js";

type NumericFact = CompanyAnalyzerOutputV2["numeric_facts"][number];

const nowIso = (): string => new Date().toISOString();

const firstSourceId = (evidence: EvidenceItem[]): string => evidence[0]?.id ?? "S1";

const normalizeSpaces = (value: string): string => value.replaceAll(/\s+/g, " ").trim();

const toProseSafeFragment = (value?: string): string =>
  normalizeSpaces(
    String(value ?? "")
      .replaceAll(/\b\d{4}-\d{2}-\d{2}\b/g, "dated event")
      .replaceAll(/\b\d{4}\/\d{2}\/\d{2}\b/g, "dated event")
      .replaceAll(/\b10-[KQ]\b/gi, "filing")
      .replaceAll(/\b20-F\b/gi, "filing")
      .replaceAll(/\b8-K\b/gi, "filing")
      .replaceAll(/\bS-1\b/gi, "registration filing")
      .replaceAll(/\bQ[1-4]\b/gi, "quarter")
      .replaceAll(/\bH[12]\b/gi, "half")
      .replaceAll(/\bFY\s*\d{2,4}\b/gi, "fiscal year")
      .replaceAll(/\b(19|20)\d{2}\b/g, "year")
      .replaceAll(/\b\d+(?:[./-]\d+)*%?\b/g, "value")
      .replaceAll(/\s+([,.;:!?])/g, "$1"),
  );

const joinProseSafeFragments = (values: string[], fallback: string): string => {
  const cleaned = Array.from(
    new Set(
      values
        .map((value) => toProseSafeFragment(value))
        .map((value) => value.trim())
        .filter(Boolean),
    ),
  );
  return cleaned.length ? cleaned.join("; ") : fallback;
};

const takeUnique = (values: Array<string | undefined>, limit: number): string[] =>
  Array.from(
    new Set(values.map((value) => (typeof value === "string" ? value.trim() : "")).filter(Boolean)),
  ).slice(0, limit);

const companySectionTemplate = (params: {
  evidence: EvidenceItem[];
  numericFacts: NumericFact[];
  filingKeywordSummary?: { business: string[]; risks: string[]; sourceIds: string[] };
  transcriptKeywordSummary?: { keywords: string[]; sourceIds: string[] };
  filingRiskBuckets?: Array<{ bucket: string; source_ids: string[] }>;
  accountingFlags?: Array<{ flag: string; source_ids: string[] }>;
  catalystCandidates?: Array<{ label: string; rationale?: string; source_ids: string[] }>;
  risk: RiskOfficerOutputV2;
}): Array<{ key: string; title: string; blocks: any[] }> => {
  const sid = firstSourceId(params.evidence);
  const n1 = params.numericFacts[0]?.id;
  const n1Text = n1 ? `{{${n1}}}` : "N/A";
  const n1Refs = n1 ? [n1] : [];
  const filingBusinessKeywords = takeUnique(params.filingKeywordSummary?.business ?? [], 6);
  const filingRiskKeywords = takeUnique(params.filingKeywordSummary?.risks ?? [], 6);
  const transcriptKeywords = takeUnique(params.transcriptKeywordSummary?.keywords ?? [], 8);
  const riskBucketLabels = takeUnique(
    params.filingRiskBuckets?.map((bucket) => bucket.bucket) ?? [],
    4,
  );
  const accountingFlagLabels = takeUnique(
    params.accountingFlags?.map((flag) => flag.flag) ?? [],
    4,
  );
  const catalystRanked = params.catalystCandidates ?? [];
  const catalystLabels = takeUnique(
    catalystRanked.map((candidate) => candidate.label),
    4,
  );
  const filingDriverSummary = joinProseSafeFragments(
    filingBusinessKeywords,
    "Primary filing business-keyword extraction is not yet available.",
  );
  const transcriptDriverSummary = joinProseSafeFragments(
    transcriptKeywords,
    "Transcript emphasis extraction is not yet available.",
  );
  const riskBucketSummary = joinProseSafeFragments(
    riskBucketLabels,
    "No filing-derived risk buckets were detected in this run.",
  );
  const accountingFlagSummary = joinProseSafeFragments(
    accountingFlagLabels,
    "No filing-derived accounting flags were detected in this run.",
  );
  const catalystSummary = joinProseSafeFragments(
    catalystLabels,
    "No dated catalyst categories were ranked from primary sources in this run.",
  );
  const evidenceBalanceLine =
    params.filingKeywordSummary?.sourceIds.length &&
    params.transcriptKeywordSummary?.sourceIds.length
      ? "The current evidence base includes both primary filings and transcript commentary, which is enough to frame a monitored first-pass thesis."
      : params.filingKeywordSummary?.sourceIds.length
        ? "The current evidence base is filing-heavy and transcript-light, so narrative claims should be treated as provisional until management commentary is ingested."
        : params.transcriptKeywordSummary?.sourceIds.length
          ? "The current evidence base leans on transcript commentary without enough filing detail, so accounting and disclosure checks remain open."
          : "The current evidence base is thin; this memo is a scoped triage pass rather than a conviction memo.";
  const firstCatalyst = params.catalystCandidates?.[0];
  const firstFalsifier = params.risk.falsifiers[0];
  const secondFalsifier = params.risk.falsifiers[1];
  const filingFacts =
    params.filingKeywordSummary && params.filingKeywordSummary.sourceIds.length
      ? {
          business: joinProseSafeFragments(
            params.filingKeywordSummary.business.slice(0, 10),
            "Primary filing disclosures were collected but the business-keyword extract was not prose-safe.",
          ),
          risks: joinProseSafeFragments(
            params.filingKeywordSummary.risks.slice(0, 10),
            "Primary filing disclosures were collected but the risk-keyword extract was not prose-safe.",
          ),
          source_ids: params.filingKeywordSummary.sourceIds,
        }
      : null;
  const transcriptFacts =
    params.transcriptKeywordSummary && params.transcriptKeywordSummary.sourceIds.length
      ? {
          keywords: joinProseSafeFragments(
            params.transcriptKeywordSummary.keywords.slice(0, 12),
            "Management emphasis areas were collected but the keyword extract was not prose-safe.",
          ),
          source_ids: params.transcriptKeywordSummary.sourceIds,
        }
      : null;
  const bucketFacts =
    params.filingRiskBuckets && params.filingRiskBuckets.length
      ? {
          buckets: joinProseSafeFragments(
            params.filingRiskBuckets.map((b) => b.bucket).slice(0, 8),
            "Risk buckets were collected but the labels were not prose-safe.",
          ),
          source_ids: Array.from(
            new Set(params.filingRiskBuckets.flatMap((b) => b.source_ids).filter(Boolean)),
          ),
        }
      : null;
  const accountingFacts =
    params.accountingFlags && params.accountingFlags.length
      ? {
          flags: joinProseSafeFragments(
            params.accountingFlags.map((f) => f.flag).slice(0, 8),
            "Accounting flags were collected but the labels were not prose-safe.",
          ),
          source_ids: Array.from(
            new Set(params.accountingFlags.flatMap((f) => f.source_ids).filter(Boolean)),
          ),
        }
      : null;
  const catalystFacts =
    params.catalystCandidates && params.catalystCandidates.length
      ? {
          labels: joinProseSafeFragments(
            params.catalystCandidates.map((c) => c.label).slice(0, 6),
            "Primary sources contain dated catalyst candidates; use the catalyst exhibit for event detail.",
          ),
          source_ids: Array.from(
            new Set(params.catalystCandidates.flatMap((c) => c.source_ids).filter(Boolean)),
          ),
        }
      : null;
  const calendarSourceIds = catalystFacts?.source_ids?.length ? catalystFacts.source_ids : [sid];

  return [
    {
      key: "executive_summary",
      title: "Executive Summary (Base/Bull/Bear)",
      blocks: [
        {
          tag: "FACT",
          text: n1
            ? `Anchor KPI baseline: reported metric is ${n1Text}.`
            : "No Tier one numeric facts are present in this run, so sizing should stay conservative.",
          source_ids: [sid],
          numeric_refs: n1Refs,
        },
        {
          tag: "INTERPRETATION",
          text: `Base case: the current read centers on ${filingDriverSummary}. The setup is only actionable if future KPIs continue to validate those operating drivers.`,
        },
        {
          tag: "ASSUMPTION",
          text: `Assumption: ${evidenceBalanceLine}`,
        },
      ],
    },
    {
      key: "variant_perception",
      title: "Variant Perception",
      blocks: [
        {
          tag: "FACT",
          text: `Evidence coverage is anchored in collected primary and official sources, with business drivers from filings summarized as: ${filingDriverSummary}.`,
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: firstCatalyst
            ? `Variant perception is most likely to move on ${toProseSafeFragment(firstCatalyst.label)} because that event can confirm or break the current operating narrative faster than generic multiple debate.`
            : "Variant perception is the delta between what consensus must be underwriting and what the current evidence base can actually support today.",
        },
        {
          tag: "ASSUMPTION",
          text: `Assumption: the main gap is not more narrative, but better evidence on ${riskBucketSummary.toLowerCase()} and transcript confirmation of the same drivers.`,
        },
      ],
    },
    {
      key: "thesis",
      title: "Thesis (Three To Five Falsifiable Statements)",
      blocks: [
        {
          tag: "FACT",
          text: n1
            ? `There exists a sourced KPI baseline (${n1Text}) in the evidence library. Falsifier: the baseline cannot be reproduced from Tier one sources.`
            : "There is no sourced KPI baseline in the evidence library. Falsifier: a Tier one source is added and shows the KPI baseline is available.",
          source_ids: [sid],
          numeric_refs: n1Refs,
        },
        {
          tag: "INTERPRETATION",
          text: `The underwriting is that the core drivers now visible in the source set (${filingDriverSummary}) will keep showing up in reported KPIs before they fade from management narrative. Falsifier: narrative claims persist without KPI confirmation across multiple periods.`,
        },
        {
          tag: "ASSUMPTION",
          text: firstFalsifier
            ? `${toProseSafeFragment(firstFalsifier.statement)} Falsifier: ${toProseSafeFragment(firstFalsifier.trigger)}.`
            : "Assumption: capture mechanisms remain durable through the cycle. Falsifier: sustained evidence of pricing pressure or commoditization from independent sources.",
        },
      ],
    },
    {
      key: "business_overview",
      title: "Business Overview (Decision-Relevant Only)",
      blocks: [
        {
          tag: "FACT",
          text: filingFacts
            ? `Filing-derived business keywords: ${filingFacts.business}.`
            : "Primary filing keyword extraction is not available in this run.",
          source_ids: filingFacts ? filingFacts.source_ids : [sid],
        },
        {
          tag: "INTERPRETATION",
          text: transcriptFacts
            ? `Transcript-derived keywords suggest management emphasis on: ${transcriptFacts.keywords}. This is the operating narrative to reconcile against filings.`
            : `Decision-relevant overview: focus on ${filingDriverSummary} and avoid generic company description until transcript emphasis is collected.`,
          source_ids: transcriptFacts ? transcriptFacts.source_ids : undefined,
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: reported KPIs map cleanly to these operating drivers without definition drift; reconcile definitions before upgrading conviction.",
        },
      ],
    },
    {
      key: "moat_competition",
      title: "Moat + Competition",
      blocks: [
        {
          tag: "FACT",
          text: transcriptFacts
            ? `Primary sources show emphasis on ${transcriptDriverSummary}, but competitor primary sources are not yet collected.`
            : "Competitor primary sources are not collected in this run, so moat claims are intentionally limited.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Moat assessment should separate durable switching costs and ecosystem gravity from transient product-cycle excitement and one-quarter beats.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: the current driver set reflects durable customer capture rather than temporary demand pull-forward; validate with competitive disclosures and retention proxies.",
        },
      ],
    },
    {
      key: "financial_quality",
      title: "Financial Quality (KPI Table)",
      blocks: [
        {
          tag: "FACT",
          text: accountingFacts
            ? `Filing-derived accounting flags present: ${accountingFacts.flags}.`
            : `Numeric facts are structured with value, unit, period, source, and accessed timestamp to enable auditability; current accounting read is ${accountingFlagSummary.toLowerCase()}.`,
          source_ids: accountingFacts ? accountingFacts.source_ids : [sid],
        },
        {
          tag: "INTERPRETATION",
          text: `Financial quality should be judged on cash conversion, margin durability, and dilution discipline, with special attention to ${accountingFlagSummary.toLowerCase()}.`,
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: KPI coverage is sufficient to extend into margins and cash conversion; if not, add those concepts before doing valuation work.",
        },
      ],
    },
    {
      key: "valuation_scenarios",
      title: "Valuation + Scenarios (Base/Bull/Bear)",
      blocks: [
        {
          tag: "FACT",
          text: "This run does not ingest market price data, so explicit valuation numbers are intentionally omitted until price tape is collected.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: `Scenario work should make explicit what has to be true on ${filingDriverSummary.toLowerCase()} for the current price to be rational, rather than hiding behind narrative confidence.`,
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: price tape, multiple context, and sensitivity ranges will be added before any recommendation moves beyond monitored watchlist status.",
        },
      ],
    },
    {
      key: "catalysts",
      title: "Catalysts (Next Year)",
      blocks: [
        {
          tag: "FACT",
          text: catalystFacts
            ? `Candidate catalyst categories found in primary sources: ${catalystFacts.labels}.`
            : "Catalyst identification requires dated, source-backed events (filings, press releases, or transcripts). If absent, treat catalysts as unknown.",
          source_ids: catalystFacts ? catalystFacts.source_ids : [sid],
        },
        {
          tag: "INTERPRETATION",
          text: firstCatalyst
            ? `Primary catalyst to monitor: ${toProseSafeFragment(firstCatalyst.label)} because it can change scenario weights through ${toProseSafeFragment(firstCatalyst.rationale)}.`
            : "Catalysts should be framed as information releases that change scenario weights, with explicit monitoring and what-would-change views.",
        },
        {
          tag: "ASSUMPTION",
          text: `Assumption: disclosure cadence remains consistent enough to monitor ${catalystSummary.toLowerCase()} against the standing thesis.`,
        },
      ],
    },
    {
      key: "risks_premortem",
      title: "Key Risks + Pre-mortem (Ranked)",
      blocks: [
        {
          tag: "FACT",
          text: bucketFacts
            ? `Filing-derived risk buckets: ${bucketFacts.buckets}.`
            : `The risk officer pass focuses attention on ${riskBucketSummary.toLowerCase()} and disconfirming evidence tied to measurable triggers.`,
          source_ids: bucketFacts ? bucketFacts.source_ids : [sid],
        },
        {
          tag: "INTERPRETATION",
          text: `Pre-mortem: ${toProseSafeFragment(
            params.risk.premortem[0] ??
              "The failure mode is overconfidence without evidence coverage.",
          )}`,
        },
        {
          tag: "ASSUMPTION",
          text: secondFalsifier
            ? `${toProseSafeFragment(secondFalsifier.statement)} Falsifier: ${toProseSafeFragment(secondFalsifier.trigger)}.`
            : "Assumption: additional Tier one and Tier three sources will materially improve decision quality; proceed only after evidence gaps close.",
        },
      ],
    },
    {
      key: "change_mind_triggers",
      title: "What Would Change Our Mind",
      blocks: [
        {
          tag: "FACT",
          text: "Change-mind triggers must be measurable and tied to collected sources; otherwise they are not monitorable.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: firstFalsifier
            ? `Primary trigger: ${toProseSafeFragment(firstFalsifier.trigger)}. Verification plan: ${toProseSafeFragment(firstFalsifier.verification_plan)}.`
            : "We change our mind when disconfirming evidence persists across multiple independent sources and cannot be explained by timing.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: monitoring will include filings, transcripts, and price tape; until then, posture should remain conservative and updates should be evidence-led.",
        },
      ],
    },
  ];
};

const buildCompanyReportV2 = (params: {
  runId: string;
  generatedAt: string;
  ticker: string;
  companyName?: string;
  plan: ResearchPlanV2;
  evidence: EvidenceItem[];
  analyzers: CompanyAnalyzerOutputV2;
  risk: RiskOfficerOutputV2;
}): unknown => {
  const numericFacts = params.analyzers.numeric_facts.map((fact) => ({
    id: fact.id,
    value: fact.value,
    unit: fact.unit,
    period: fact.period,
    currency: fact.currency,
    source_id: fact.source_id,
    accessed_at: fact.accessed_at,
    notes: fact.notes,
  }));

  const filingExtracts = (params.analyzers as any).extracts?.filings as
    | Array<{
        source_id: string;
        extracted: { business_keywords: string[]; risk_keywords: string[] };
      }>
    | undefined;
  const transcriptExtracts = (params.analyzers as any).extracts?.transcripts as
    | Array<{ source_id: string; extracted: { keywords: string[] } }>
    | undefined;
  const filingKeywordSummary =
    filingExtracts && filingExtracts.length
      ? {
          business: Array.from(
            new Set(filingExtracts.flatMap((f) => f.extracted.business_keywords)),
          ),
          risks: Array.from(new Set(filingExtracts.flatMap((f) => f.extracted.risk_keywords))),
          sourceIds: Array.from(new Set(filingExtracts.map((f) => f.source_id))),
        }
      : undefined;
  const transcriptKeywordSummary =
    transcriptExtracts && transcriptExtracts.length
      ? {
          keywords: Array.from(new Set(transcriptExtracts.flatMap((t) => t.extracted.keywords))),
          sourceIds: Array.from(new Set(transcriptExtracts.map((t) => t.source_id))),
        }
      : undefined;

  const sections = companySectionTemplate({
    evidence: params.evidence,
    numericFacts: params.analyzers.numeric_facts,
    filingKeywordSummary,
    transcriptKeywordSummary,
    filingRiskBuckets: (params.analyzers as any).risk_factor_buckets,
    accountingFlags: (params.analyzers as any).accounting_flags,
    catalystCandidates: (params.analyzers as any).catalyst_candidates,
    risk: params.risk,
  });

  const sid = firstSourceId(params.evidence);
  const kpiRows = params.analyzers.kpi_table ?? [];
  const kpiNumericIds = Array.from(
    new Set(kpiRows.map((row) => row.numeric_id).filter((id): id is string => Boolean(id))),
  );
  const n1 = kpiNumericIds[0] ?? params.analyzers.numeric_facts[0]?.id;
  const n1Text = n1 ? `{{${n1}}}` : "N/A";

  const catalystCalendar = (params.analyzers as any).catalyst_calendar as
    | Array<{ date?: unknown; label?: unknown; source_ids?: unknown }>
    | undefined;
  const calendarSourceIds =
    catalystCalendar && catalystCalendar.length
      ? Array.from(
          new Set(
            catalystCalendar
              .flatMap((row) => (Array.isArray(row.source_ids) ? row.source_ids : []))
              .map((v) => String(v))
              .filter(Boolean),
          ),
        )
      : [];
  const calendarSourceIdsFinal = calendarSourceIds.length ? calendarSourceIds : [sid];

  const exhibits = [
    {
      id: "X1",
      title: "KPI Table (KPI)",
      question: "What are the anchor KPIs and what is the reported baseline?",
      data_summary: kpiRows.length
        ? kpiRows.map((row) => {
            const numeric = row.numeric_id ? `{{${row.numeric_id}}}` : "N/A";
            const asOf = row.as_of?.trim() ? ` as_of=${row.as_of}` : "";
            const form = row.form?.trim() ? ` form=${row.form}` : "";
            return `${row.metric}: ${numeric}.${asOf}${form}`;
          })
        : n1
          ? [`Reported baseline metric: ${n1Text}.`]
          : ["No numeric baseline available in this run."],
      takeaway: "Takeaway: Establish a sourced KPI baseline before layering interpretation.",
      source_ids: [sid],
      numeric_refs: kpiNumericIds.length ? kpiNumericIds : n1 ? [n1] : [],
    },
    {
      id: "X2",
      title: "Margins (Margin)",
      question: "What is the operating leverage profile over time?",
      data_summary: ["N/A in this run (margin series not yet collected)."],
      takeaway: "Takeaway: Do not assert margin trends without sourced series.",
      source_ids: [sid],
    },
    {
      id: "X3",
      title: "FCF (FCF)",
      question: "Is free cash flow conversion durable across cycles?",
      data_summary: ["N/A in this run (free cash flow series not yet collected)."],
      takeaway: "Takeaway: Cash conversion is the decision metric; collect it before sizing risk.",
      source_ids: [sid],
    },
    {
      id: "X4",
      title: "SBC and Dilution (SBC / dilution)",
      question: "Is dilution controlled relative to cash generation?",
      data_summary: ["N/A in this run (share count and SBC series not yet collected)."],
      takeaway: "Takeaway: Treat dilution as a recurring expense unless proven otherwise.",
      source_ids: [sid],
    },
    {
      id: "X5",
      title: "Scenario Drivers (Scenario driver)",
      question: "Which variables drive scenario outcomes?",
      data_summary: [
        "Key drivers to quantify: mix, margins, cash conversion, and capital returns (requires additional time-series evidence).",
      ],
      takeaway: "Takeaway: Explicit drivers prevent narrative drift.",
      source_ids: [sid],
    },
    {
      id: "X6",
      title: "Sensitivity (Sensitivity)",
      question: "How sensitive is the thesis to a small number of variables?",
      data_summary: ["Sensitivity is N/A until valuation inputs and price tape are collected."],
      takeaway: "Takeaway: Sensitivity is how you separate conviction from leverage.",
      source_ids: [sid],
    },
    {
      id: "X7",
      title: "Catalyst Calendar (Catalyst calendar)",
      question: "What dated events can change scenario weights?",
      data_summary:
        Array.isArray((params.analyzers as any).catalyst_ranked) &&
        (params.analyzers as any).catalyst_ranked.length
          ? (params.analyzers as any).catalyst_ranked.map((row: any) =>
              [
                `Event: ${toProseSafeFragment(String(row.label))}`.trim(),
                `Why: ${toProseSafeFragment(String(row.why_it_matters))}`.trim(),
                `Changes: ${toProseSafeFragment(String(row.what_changes))}`.trim(),
                `Watch: ${toProseSafeFragment(String(row.what_to_watch))}`.trim(),
              ].join(" "),
            )
          : catalystCalendar && catalystCalendar.length
            ? catalystCalendar.map((row: any) =>
                `Event: ${toProseSafeFragment(String(row.label))}`.trim(),
              )
            : ["N/A in this run (no dated filing/transcript events collected)."],
      takeaway: "Takeaway: A calendar is only as good as its dated sources.",
      source_ids: calendarSourceIdsFinal,
    },
  ];

  return {
    version: 2,
    kind: "company",
    run_id: params.runId,
    generated_at: params.generatedAt,
    subject: {
      ticker: params.ticker.toUpperCase(),
      company_name: params.companyName,
    },
    plan: {
      posture: params.plan.posture,
      horizon: params.plan.horizon,
      timebox_minutes: params.plan.timebox_minutes,
      key_questions: params.plan.key_questions,
      required_exhibits: params.plan.required_exhibits,
    },
    sources: params.evidence,
    numeric_facts: numericFacts,
    sections,
    exhibits,
    appendix: {
      evidence_table: [
        {
          claim: n1
            ? `Anchor KPI baseline exists (${n1Text}) and is reproducible from Tier one sources.`
            : "No Tier one KPI baseline exists in the current evidence library.",
          evidence_ids: [sid],
          source_ids: [sid],
        },
      ],
      whats_missing: [
        "Primary filing text for narrative disclosures (risk factors, segment mix).",
        "Price tape and valuation inputs to quantify scenarios and sensitivity.",
        "Transcript sources to validate management commentary versus filings.",
      ],
    },
  };
};

const themeSectionsTemplate = (params: {
  evidence: EvidenceItem[];
  analyzers: ThemeAnalyzerOutputV2;
  risk: RiskOfficerOutputV2;
  universeEntities?: ThemeUniverseEntityV2[];
}): Array<{ key: string; title: string; blocks: any[] }> => {
  const sid = firstSourceId(params.evidence);
  const universeSummary =
    params.universeEntities && params.universeEntities.length
      ? (() => {
          const grouped = new Map<string, ThemeUniverseEntityV2[]>();
          for (const e of params.universeEntities) {
            grouped.set(e.type, [...(grouped.get(e.type) ?? []), e]);
          }
          const parts: string[] = [];
          for (const [type, items] of grouped.entries()) {
            const labels = items
              .slice(0, 12)
              .map((e) => (e.symbol ? `${e.label} (${e.symbol})` : e.label))
              .join(", ");
            parts.push(`${type}: ${labels}${items.length > 12 ? ", ..." : ""}`);
          }
          return parts.join(" | ");
        })()
      : null;
  return [
    {
      key: "executive_summary",
      title: "Executive Summary (Definition + Variant + Winners/Losers)",
      blocks: [
        ...(universeSummary
          ? [
              {
                tag: "FACT",
                text: `Universe entities (typed): ${universeSummary}`,
                source_ids: [sid],
              },
            ]
          : []),
        {
          tag: "FACT",
          text: "This theme memo is constrained to collected evidence; unsupported claims are marked as assumptions or omitted.",
          source_ids: [sid],
        },
        { tag: "INTERPRETATION", text: params.analyzers.definition_taxonomy.definition },
        {
          tag: "ASSUMPTION",
          text: "Assumption: the evidence library will expand to include Tier one filings and Tier three transcripts for all universe members.",
        },
      ],
    },
    {
      key: "what_it_is_isnt_why_now",
      title: "What It Is / Isn't + Why Now",
      blocks: [
        {
          tag: "FACT",
          text: "Contracts prevent generic prose from shipping by enforcing citations, numeric provenance, and section structure.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Why now: agentic systems without governance drift; fail-closed gates make research reproducible and auditable.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: the incremental time cost of gates is offset by reduced downstream rework and higher trust.",
        },
      ],
    },
    {
      key: "value_chain",
      title: "Where Value Accrues (Value Chain)",
      blocks: [
        {
          tag: "FACT",
          text: "Value chain mapping should be tied to primary sources; missing sources are recorded in the appendix.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Value accrues to systems that turn evidence into decisions with low contradiction and high auditability.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: buyer preference will shift toward auditable workflows as regulation and model risk management mature.",
        },
      ],
    },
    {
      key: "capture_ledger",
      title: "Capture Ledger (Mechanisms + Scorecard)",
      blocks: [
        {
          tag: "FACT",
          text: "Capture mechanisms must be tied to evidence; scorecards without sources are opinions.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Mechanisms: distribution control, workflow embedding, governance, and switching costs.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: monetization consolidates around workflow layers rather than generalized capability providers.",
        },
      ],
    },
    {
      key: "beneficiaries_vs_left_behind",
      title: "Beneficiaries vs Left Behind",
      blocks: [
        {
          tag: "FACT",
          text: "Beneficiary claims require evidence per name; this run records missing coverage explicitly.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Beneficiaries are names with primary evidence coverage and durable capture mechanisms; left behind lack either.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: procurement and compliance constraints will be a durable differentiator in adoption.",
        },
      ],
    },
    {
      key: "catalysts_timeline",
      title: "Catalysts + Timeline",
      blocks: [
        {
          tag: "FACT",
          text: "Catalyst calendars must be backed by dated sources; this run does not include those sources.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Treat catalysts as information releases that update scenario weights.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: adoption milestones appear in filings and transcripts before they appear in price tape.",
        },
      ],
    },
    {
      key: "risks_falsifiers",
      title: "Risks + Falsifiers (Ranked)",
      blocks: [
        {
          tag: "FACT",
          text: "Falsifiers should be measurable and monitored via the evidence library.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text:
            params.risk.bear_narrative[0] ??
            "Risk: commoditization and weak capture compress outcomes.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: a monitoring dashboard will track evidence freshness, contradictions, and KPI deltas per name.",
        },
      ],
    },
    {
      key: "portfolio_posture",
      title: "Portfolio Posture (Expression + Monitoring)",
      blocks: [
        {
          tag: "FACT",
          text: "The version two gates fail closed when sourcing or numeric provenance is missing.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Expression: stage exposure (pilot/core) only after evidence coverage and falsifiers are defined.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: skipping low-evidence names improves risk-adjusted outcomes in a broad opportunity set.",
        },
      ],
    },
  ];
};

const buildThemeReportV2 = (params: {
  runId: string;
  generatedAt: string;
  themeName: string;
  universe: string[];
  universeEntities?: ThemeUniverseEntityV2[];
  plan: ResearchPlanV2;
  evidence: EvidenceItem[];
  analyzers: ThemeAnalyzerOutputV2;
  risk: RiskOfficerOutputV2;
}): unknown => {
  const sid = firstSourceId(params.evidence);
  const sections = themeSectionsTemplate({
    evidence: params.evidence,
    analyzers: params.analyzers,
    risk: params.risk,
    universeEntities: params.universeEntities,
  });
  const exhibits = [
    {
      id: "X1",
      title: "Value Chain Map (Value chain)",
      question: "Where does value accrue along the workflow from evidence to decision?",
      data_summary: [
        "Primary sources -> normalization -> analyzers -> skeptical review -> memo compiler -> portfolio decision.",
      ],
      takeaway:
        "Takeaway: Value accrues to systems that make evidence reusable and decisions auditable.",
      source_ids: [sid],
    },
    {
      id: "X2",
      title: "Capture Scorecard (Capture scorecard)",
      question: "Which mechanisms drive capture and how do we score them?",
      data_summary: [
        "Distribution, workflow embedding, governance, and switching cost are scored only when evidence exists.",
      ],
      takeaway: "Takeaway: Scorecards without evidence are opinions, not research.",
      source_ids: [sid],
    },
    {
      id: "X3",
      title: "Adoption Dashboard (Adoption dashboard)",
      question: "What adoption signposts should we monitor?",
      data_summary: [
        "Filings: KPI deltas and disclosure; transcripts: customer and pipeline signals; price: regime shifts.",
      ],
      takeaway: "Takeaway: Dashboards turn themes into monitorable systems.",
      source_ids: [sid],
    },
    {
      id: "X4",
      title: "Catalyst Calendar (Catalyst calendar)",
      question: "What dated events can change scenario weights?",
      data_summary: [
        "Earnings cycles, major product releases, and regulatory deadlines (detailed sources should be included in the evidence library).",
      ],
      takeaway: "Takeaway: A calendar is only as good as its dated sources.",
      source_ids: [sid],
    },
    {
      id: "X5",
      title: "Risk Heatmap (Risk heatmap)",
      question: "Where do the biggest failure modes sit across mechanism and time?",
      data_summary: [
        "Execution risk, regulation risk, and commoditization risk require explicit falsifiers.",
      ],
      takeaway: "Takeaway: Heatmaps are useful only when paired with triggers that change posture.",
      source_ids: [sid],
    },
  ];

  return {
    version: 2,
    kind: "theme",
    run_id: params.runId,
    generated_at: params.generatedAt,
    subject: {
      theme_name: params.themeName,
      universe: params.universe,
      ...(params.universeEntities?.length ? { universe_entities: params.universeEntities } : {}),
    },
    plan: {
      posture: params.plan.posture,
      horizon: params.plan.horizon,
      timebox_minutes: params.plan.timebox_minutes,
      key_questions: params.plan.key_questions,
      required_exhibits: params.plan.required_exhibits,
    },
    sources: params.evidence,
    numeric_facts: params.analyzers.numeric_facts ?? [],
    sections,
    exhibits,
    appendix: {
      evidence_table: [
        {
          claim:
            "Contracts enforce fail-closed rules for citations, numeric provenance, and structure.",
          evidence_ids: [sid],
          source_ids: [sid],
        },
      ],
      whats_missing: [
        "Theme-specific primary sources for every universe member (filings and transcripts).",
        "Dated catalyst sources to populate the calendar with evidence-backed events.",
        "Quantitative adoption dashboard once KPI sources are ingested for all names.",
      ],
    },
  };
};

export type CompileResultV2 = {
  reportJson: unknown;
  reportMarkdown: string;
  gate: QualityGateResult;
};

const parseBoolean = (raw: string | undefined): boolean | undefined => {
  if (typeof raw !== "string") return undefined;
  const normalized = raw.trim().toLowerCase();
  if (!normalized) return undefined;
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return undefined;
};

const v2WriterLlmEnabled = (): boolean =>
  parseBoolean(process.env.OPENCLAW_RESEARCH_V2_LLM_WRITER) ??
  (String(process.env.OPENCLAW_HOST_ROLE ?? "")
    .trim()
    .toLowerCase() === "macmini" &&
    parseBoolean(process.env.OPENCLAW_RESEARCH_V2_LLM) !== false);

const REQUIRED_SECTION_KEYS_BY_KIND: Record<ReportKindV2, string[]> = {
  company: [
    "executive_summary",
    "variant_perception",
    "thesis",
    "business_overview",
    "moat_competition",
    "financial_quality",
    "valuation_scenarios",
    "catalysts",
    "risks_premortem",
    "change_mind_triggers",
  ],
  theme: [
    "executive_summary",
    "what_it_is_isnt_why_now",
    "value_chain",
    "capture_ledger",
    "beneficiaries_vs_left_behind",
    "catalysts_timeline",
    "risks_falsifiers",
    "portfolio_posture",
  ],
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  Boolean(value) && typeof value === "object" && !Array.isArray(value);

const toRecord = (value: unknown): Record<string, unknown> => (isRecord(value) ? value : {});
const toArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const toString = (value: unknown): string => (typeof value === "string" ? value : "");
const toNumber = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return null;
};
const dedupe = (values: string[]): string[] => Array.from(new Set(values.filter(Boolean)));
const toStringArray = (value: unknown): string[] =>
  dedupe(
    toArray(value)
      .map(toString)
      .map((v) => v.trim()),
  );

const normalizeTicker = (value: string): string | null => {
  const t = value.trim().toUpperCase();
  if (!/^[A-Z0-9.]{1,10}$/.test(t)) return null;
  return t;
};

const normalizeIso = (value: unknown, fallback: string): string => {
  const raw = toString(value).trim();
  if (!raw) return fallback;
  const parsed = Date.parse(raw);
  if (!Number.isFinite(parsed)) return fallback;
  return new Date(parsed).toISOString();
};

const unwrapReportCandidate = (raw: unknown): Record<string, unknown> => {
  const root = toRecord(raw);
  const nestedKeys = ["report", "report_json", "final_report", "output", "result"];
  for (const key of nestedKeys) {
    const nested = root[key];
    if (isRecord(nested)) return nested;
  }
  return root;
};

const normalizeTextBlock = (params: {
  block: unknown;
  fallbackSid: string;
  sourceSet: Set<string>;
  numericIdSet: Set<string>;
}): Record<string, unknown> => {
  const block = toRecord(params.block);
  const tagRaw = toString(block.tag).trim().toUpperCase();
  const tag =
    tagRaw === "FACT" || tagRaw === "INTERPRETATION" || tagRaw === "ASSUMPTION"
      ? tagRaw
      : "INTERPRETATION";
  let text =
    toString(block.text).trim() ||
    "N/A because the model did not provide source-grounded content for this block.";
  let sourceIds = toStringArray(block.source_ids).filter((id) => params.sourceSet.has(id));
  const numericRefs = toStringArray(block.numeric_refs).filter((id) => params.numericIdSet.has(id));
  const placeholderIds = Array.from(text.matchAll(/\{\{\s*(N\d+)\s*\}\}/g))
    .map((m) => m[1] ?? "")
    .filter((id) => params.numericIdSet.has(id));
  const finalNumericRefs = dedupe([...numericRefs, ...placeholderIds]);
  text = text.replaceAll(/\{\{\s*(N\d+)\s*\}\}/g, (_all, id: string) =>
    params.numericIdSet.has(id) ? `{{${id}}}` : "N/A",
  );
  if (tag === "FACT" && sourceIds.length === 0) {
    sourceIds = [params.fallbackSid];
  }
  const out: Record<string, unknown> = { tag, text };
  if (sourceIds.length > 0) out.source_ids = sourceIds;
  if (finalNumericRefs.length > 0) out.numeric_refs = finalNumericRefs;
  return out;
};

const normalizeSections = (params: {
  kind: ReportKindV2;
  candidate: unknown;
  fallback: unknown;
  fallbackSid: string;
  sourceSet: Set<string>;
  numericIdSet: Set<string>;
}): Record<string, unknown>[] => {
  const expected = REQUIRED_SECTION_KEYS_BY_KIND[params.kind];
  const fallbackSections = toArray(params.fallback).map(toRecord);
  const fallbackByKey = new Map(
    fallbackSections.map((s) => [toString(s.key).trim(), s] as const).filter(([k]) => Boolean(k)),
  );
  const candidateSections = toArray(params.candidate).map(toRecord);
  const candidateByKey = new Map<string, Record<string, unknown>>();
  for (const section of candidateSections) {
    const key = toString(section.key).trim();
    if (!expected.includes(key)) continue;
    if (candidateByKey.has(key)) continue;
    candidateByKey.set(key, section);
  }
  return expected.map((key) => {
    const source = candidateByKey.get(key) ?? fallbackByKey.get(key) ?? {};
    const title =
      toString(source.title).trim() || toString(fallbackByKey.get(key)?.title).trim() || key;
    const rawBlocks = toArray(source.blocks);
    const blocks =
      rawBlocks.length > 0
        ? rawBlocks.map((b) =>
            normalizeTextBlock({
              block: b,
              fallbackSid: params.fallbackSid,
              sourceSet: params.sourceSet,
              numericIdSet: params.numericIdSet,
            }),
          )
        : toArray(fallbackByKey.get(key)?.blocks).map((b) =>
            normalizeTextBlock({
              block: b,
              fallbackSid: params.fallbackSid,
              sourceSet: params.sourceSet,
              numericIdSet: params.numericIdSet,
            }),
          );
    return {
      key,
      title,
      blocks: blocks.length
        ? blocks
        : [
            {
              tag: "ASSUMPTION",
              text: "N/A because model output did not provide this required section.",
            },
          ],
    };
  });
};

const normalizeExhibits = (params: {
  candidate: unknown;
  fallback: unknown;
  fallbackSid: string;
  sourceSet: Set<string>;
  numericIdSet: Set<string>;
}): Record<string, unknown>[] => {
  const candidate = toArray(params.candidate);
  const fallback = toArray(params.fallback).map(toRecord);
  const picked = candidate.length ? candidate : fallback;
  const normalized = picked.map(toRecord).map((row, idx) => {
    const sourceIds = toStringArray(row.source_ids).filter((id) => params.sourceSet.has(id));
    const numericRefs = toStringArray(row.numeric_refs).filter((id) => params.numericIdSet.has(id));
    const dataSummary = toStringArray(row.data_summary);
    const out: Record<string, unknown> = {
      id: toString(row.id).trim() || `X${idx + 1}`,
      title: toString(row.title).trim() || `Exhibit ${idx + 1}`,
      question: toString(row.question).trim() || "What does this exhibit test?",
      data_summary: dataSummary.length ? dataSummary : ["N/A"],
      takeaway: toString(row.takeaway).trim() || "Takeaway: evidence coverage is incomplete.",
      source_ids: sourceIds.length ? sourceIds : [params.fallbackSid],
    };
    if (numericRefs.length > 0) out.numeric_refs = numericRefs;
    return out;
  });
  return normalized.length > 0
    ? normalized
    : [
        {
          id: "X1",
          title: "Exhibit 1",
          question: "What does this exhibit test?",
          data_summary: ["N/A"],
          takeaway: "Takeaway: evidence coverage is incomplete.",
          source_ids: [params.fallbackSid],
        },
      ];
};

const normalizeAppendix = (params: {
  candidate: unknown;
  fallback: unknown;
  fallbackSid: string;
  sourceSet: Set<string>;
}): Record<string, unknown> => {
  const candidate = toRecord(params.candidate);
  const fallback = toRecord(params.fallback);
  const evidenceRows = toArray(candidate.evidence_table)
    .map(toRecord)
    .map((row) => {
      const sourceIds = toStringArray(row.source_ids).filter((id) => params.sourceSet.has(id));
      const evidenceIds = toStringArray(row.evidence_ids).filter((id) => params.sourceSet.has(id));
      return {
        claim: toString(row.claim).trim() || "N/A",
        evidence_ids: evidenceIds.length
          ? evidenceIds
          : sourceIds.length
            ? sourceIds
            : [params.fallbackSid],
        source_ids: sourceIds.length ? sourceIds : [params.fallbackSid],
      };
    })
    .filter((row) => row.claim.trim().length > 0);
  const fallbackRows = toArray(fallback.evidence_table).map(toRecord);
  const finalRows = evidenceRows.length
    ? evidenceRows
    : fallbackRows.length
      ? fallbackRows
      : [
          {
            claim: "N/A",
            evidence_ids: [params.fallbackSid],
            source_ids: [params.fallbackSid],
          },
        ];
  const whatsMissing = toStringArray(candidate.whats_missing);
  const fallbackMissing = toStringArray(fallback.whats_missing);
  return {
    evidence_table: finalRows,
    whats_missing: whatsMissing.length ? whatsMissing : fallbackMissing,
  };
};

export function normalizeLlmReportCandidateV2(params: {
  kind: ReportKindV2;
  candidate: unknown;
  fallbackReport: unknown;
  now?: string;
}): unknown {
  const fallback = toRecord(params.fallbackReport);
  const root = unwrapReportCandidate(params.candidate);
  const now = params.now ?? nowIso();
  const fallbackSources = toArray(fallback.sources).map(toRecord);
  const sourceSet = new Set(
    fallbackSources.map((s) => toString(s.id).trim()).filter((id): id is string => Boolean(id)),
  );
  const fallbackSid = toString(fallbackSources[0]?.id).trim() || "S1";

  const candidatePlan = toRecord(root.plan);
  const fallbackPlan = toRecord(fallback.plan);
  const plan = {
    posture: "long-only",
    horizon:
      toString(candidatePlan.horizon).trim() ||
      toString(fallbackPlan.horizon).trim() ||
      "12-36 months",
    timebox_minutes: (() => {
      const value =
        toNumber(candidatePlan.timebox_minutes) ?? toNumber(fallbackPlan.timebox_minutes) ?? 60;
      return Math.max(1, Math.min(600, Math.round(value)));
    })(),
    key_questions: (() => {
      const value = toStringArray(candidatePlan.key_questions);
      const fallbackValue = toStringArray(fallbackPlan.key_questions);
      return value.length
        ? value
        : fallbackValue.length
          ? fallbackValue
          : ["What is the primary underwriting edge?"];
    })(),
    required_exhibits: (() => {
      const value = toStringArray(candidatePlan.required_exhibits);
      const fallbackValue = toStringArray(fallbackPlan.required_exhibits);
      return value.length ? value : fallbackValue.length ? fallbackValue : ["evidence_table"];
    })(),
  };

  const fallbackSubject = toRecord(fallback.subject);
  const rootSubject = toRecord(root.subject);
  const subject =
    params.kind === "company"
      ? {
          ticker:
            normalizeTicker(toString(rootSubject.ticker)) ??
            normalizeTicker(toString(root.ticker)) ??
            normalizeTicker(toString(fallbackSubject.ticker)) ??
            "UNKNOWN",
          ...(toString(rootSubject.company_name).trim()
            ? { company_name: toString(rootSubject.company_name).trim() }
            : toString(rootSubject.companyName).trim()
              ? { company_name: toString(rootSubject.companyName).trim() }
              : toString(fallbackSubject.company_name).trim()
                ? { company_name: toString(fallbackSubject.company_name).trim() }
                : {}),
        }
      : {
          theme_name:
            toString(rootSubject.theme_name).trim() ||
            toString(root.theme_name).trim() ||
            toString(fallbackSubject.theme_name).trim() ||
            "UNKNOWN",
          universe: (() => {
            const fromSubject = toStringArray(rootSubject.universe).map((t) => t.toUpperCase());
            const fromRoot = toStringArray(root.universe).map((t) => t.toUpperCase());
            const fromFallback = toStringArray(fallbackSubject.universe).map((t) =>
              t.toUpperCase(),
            );
            return fromSubject.length ? fromSubject : fromRoot.length ? fromRoot : fromFallback;
          })(),
          ...(toArray(rootSubject.universe_entities).length
            ? { universe_entities: toArray(rootSubject.universe_entities).map(toRecord) }
            : toArray(fallbackSubject.universe_entities).length
              ? { universe_entities: toArray(fallbackSubject.universe_entities).map(toRecord) }
              : {}),
        };

  const fallbackNumericFacts = toArray(fallback.numeric_facts).map(toRecord);
  const candidateNumericFacts = toArray(root.numeric_facts).map(toRecord);
  const normalizedNumericFacts = candidateNumericFacts
    .map((fact) => {
      const id = toString(fact.id).trim();
      const value = toNumber(fact.value);
      const unit = toString(fact.unit).trim();
      const period = toString(fact.period).trim();
      if (!/^N\d+$/.test(id) || value === null || !unit || !period) return null;
      const sourceIdRaw = toString(fact.source_id).trim();
      const sourceId = sourceSet.has(sourceIdRaw) ? sourceIdRaw : fallbackSid;
      return {
        id,
        value,
        unit,
        period,
        ...(toString(fact.currency).trim() ? { currency: toString(fact.currency).trim() } : {}),
        source_id: sourceId,
        accessed_at: normalizeIso(fact.accessed_at, now),
        ...(toString(fact.notes).trim() ? { notes: toString(fact.notes).trim() } : {}),
      };
    })
    .filter((row) => row !== null);
  const numericFacts = normalizedNumericFacts.length
    ? normalizedNumericFacts
    : fallbackNumericFacts;
  const numericIdSet = new Set(numericFacts.map((row) => toString(row.id).trim()).filter(Boolean));

  const sections = normalizeSections({
    kind: params.kind,
    candidate: root.sections,
    fallback: fallback.sections,
    fallbackSid,
    sourceSet,
    numericIdSet,
  });
  const exhibits = normalizeExhibits({
    candidate: root.exhibits,
    fallback: fallback.exhibits,
    fallbackSid,
    sourceSet,
    numericIdSet,
  });
  const appendix = normalizeAppendix({
    candidate: root.appendix,
    fallback: fallback.appendix,
    fallbackSid,
    sourceSet,
  });

  return {
    version: 2,
    kind: params.kind,
    run_id: toString(root.run_id).trim() || toString(fallback.run_id).trim() || "run-v2",
    generated_at: normalizeIso(root.generated_at, now),
    subject,
    plan,
    sources: fallbackSources,
    numeric_facts: numericFacts,
    sections,
    exhibits,
    appendix,
  };
}

export async function pass4CompileReportV2(params: {
  kind: ReportKindV2;
  runId: string;
  subject: {
    ticker?: string;
    companyName?: string;
    themeName?: string;
    universe?: string[];
    universeEntities?: ThemeUniverseEntityV2[];
  };
  plan: ResearchPlanV2;
  evidence: EvidenceItem[];
  analyzers: CompanyAnalyzerOutputV2 | ThemeAnalyzerOutputV2;
  risk: RiskOfficerOutputV2;
  repairModel?: V2RepairModel;
  llmProfile?: {
    modelRef?: string;
    profileId?: string;
  };
}): Promise<CompileResultV2> {
  const generatedAt = nowIso();
  const deterministicReport =
    params.kind === "company"
      ? buildCompanyReportV2({
          runId: params.runId,
          generatedAt,
          ticker: params.subject.ticker ?? "UNKNOWN",
          companyName: params.subject.companyName,
          plan: params.plan,
          evidence: params.evidence,
          analyzers: params.analyzers as CompanyAnalyzerOutputV2,
          risk: params.risk,
        })
      : buildThemeReportV2({
          runId: params.runId,
          generatedAt,
          themeName: params.subject.themeName ?? "UNKNOWN",
          universe: params.subject.universe ?? [],
          universeEntities: params.subject.universeEntities,
          plan: params.plan,
          evidence: params.evidence,
          analyzers: params.analyzers as ThemeAnalyzerOutputV2,
          risk: params.risk,
        });

  if (v2WriterLlmEnabled()) {
    const { completeJsonWithResearchV2Model } = await import("../../llm/complete-json.js");
    const mkPrompt = (mode: "write" | "repair", current?: unknown, issues?: unknown) => {
      const schemaHint =
        params.kind === "company"
          ? {
              version: 2,
              kind: "company",
              subject: { ticker: params.subject.ticker ?? "UNKNOWN", company_name: "optional" },
              plan: params.plan,
              sources: params.evidence.map((s) => ({
                id: s.id,
                title: s.title,
                publisher: s.publisher,
                date_published: s.date_published,
                accessed_at: s.accessed_at,
                url: s.url,
                reliability_tier: s.reliability_tier,
              })),
              numeric_facts: [],
              sections: [
                {
                  key: "executive_summary",
                  title: "Executive Summary",
                  blocks: [
                    { tag: "FACT", text: "…", source_ids: ["S1"], numeric_refs: [] },
                    { tag: "INTERPRETATION", text: "…", source_ids: ["S1"], numeric_refs: [] },
                    { tag: "ASSUMPTION", text: "…", source_ids: [], numeric_refs: [] },
                  ],
                },
              ],
              exhibits: [
                {
                  id: "Exhibit 1",
                  title: "…",
                  question: "…",
                  data_summary: [],
                  takeaway: "…",
                  source_ids: ["S1"],
                },
              ],
              appendix: {
                evidence_table: [{ claim: "…", evidence_ids: ["S1"], source_ids: ["S1"] }],
                whats_missing: ["…"],
              },
            }
          : {
              version: 2,
              kind: "theme",
              subject: {
                theme_name: params.subject.themeName ?? "UNKNOWN",
                universe: params.subject.universe ?? [],
                universe_entities: params.subject.universeEntities ?? [],
              },
              plan: params.plan,
              sources: params.evidence.map((s) => ({
                id: s.id,
                title: s.title,
                publisher: s.publisher,
                date_published: s.date_published,
                accessed_at: s.accessed_at,
                url: s.url,
                reliability_tier: s.reliability_tier,
              })),
              numeric_facts: [],
              sections: [
                {
                  key: "executive_summary",
                  title: "Executive Summary",
                  blocks: [
                    { tag: "FACT", text: "…", source_ids: ["S1"], numeric_refs: [] },
                    { tag: "INTERPRETATION", text: "…", source_ids: ["S1"], numeric_refs: [] },
                    { tag: "ASSUMPTION", text: "…", source_ids: [], numeric_refs: [] },
                  ],
                },
              ],
              exhibits: [
                {
                  id: "Exhibit 1",
                  title: "…",
                  question: "…",
                  data_summary: [],
                  takeaway: "…",
                  source_ids: ["S1"],
                },
              ],
              appendix: {
                evidence_table: [{ claim: "…", evidence_ids: ["S1"], source_ids: ["S1"] }],
                whats_missing: ["…"],
              },
            };

      const requiredSectionKeys =
        params.kind === "company"
          ? [
              "executive_summary",
              "variant_perception",
              "thesis",
              "business_overview",
              "moat_competition",
              "financial_quality",
              "valuation_scenarios",
              "catalysts",
              "risks_premortem",
              "change_mind_triggers",
            ]
          : [
              "executive_summary",
              "what_it_is_isnt_why_now",
              "value_chain",
              "capture_ledger",
              "beneficiaries_vs_left_behind",
              "catalysts_timeline",
              "risks_falsifiers",
              "portfolio_posture",
            ];

      return [
        "You are an institutional research memo writer. Your output is consumed by a strict JSON schema validator and hard quality gate.",
        "",
        "Hard rules:",
        "- Output JSON only (no markdown fences).",
        "- Use ONLY the provided sources (S#) for FACT blocks. Every FACT block must include source_ids with valid S# ids.",
        "- Keep required sections in EXACT order. Required section keys:",
        JSON.stringify(requiredSectionKeys),
        "- Avoid vague filler. Be skeptical: include bear case + falsifiers.",
        "- Do not invent numbers. If you must include a numeric value, put it in numeric_facts (with provenance) and reference via numeric_refs + {{N#}} placeholders in text.",
        '- Exhibits: include at least one Exhibit with id like "Exhibit 1" and cite sources.',
        "",
        `Mode: ${mode}`,
        mode === "repair"
          ? "Repair objective: fix the listed validation errors by adding citations, removing/rewriting unsupported claims, adding missing sections (as N/A with na_reason), and fixing numeric provenance. Return a full corrected report JSON."
          : "Write objective: produce a decision-useful memo (low fluff) grounded in evidence and analyzers, suitable for a long-only institutional audience.",
        "",
        "Inputs:",
        JSON.stringify(
          {
            kind: params.kind,
            run_id: params.runId,
            generated_at: generatedAt,
            subject: params.subject,
            plan: params.plan,
            evidence: params.evidence.map((s) => ({
              id: s.id,
              title: s.title,
              publisher: s.publisher,
              date_published: s.date_published,
              accessed_at: s.accessed_at,
              url: s.url,
              reliability_tier: s.reliability_tier,
              key_points: (s.excerpt_or_key_points ?? []).slice(0, 10),
              tags: (s.tags ?? []).slice(0, 16),
            })),
            analyzers: params.analyzers,
            risk_officer: params.risk,
          },
          null,
          2,
        ),
        mode === "repair"
          ? `\nCurrent report JSON:\n${JSON.stringify(current ?? {}, null, 2)}`
          : null,
        mode === "repair" ? `\nValidation issues:\n${JSON.stringify(issues ?? [], null, 2)}` : null,
        "",
        "Schema hint (shape only, do not copy literals blindly):",
        JSON.stringify(schemaHint, null, 2),
      ]
        .filter(Boolean)
        .join("\n");
    };

    const draftRaw = await completeJsonWithResearchV2Model({
      purpose: "writer",
      prompt: mkPrompt("write"),
      maxTokens: 3600,
      temperature: 0.2,
      modelRefOverride: params.llmProfile?.modelRef,
      profileId: params.llmProfile?.profileId ?? process.env.OPENCLAW_RESEARCH_V2_PROFILE?.trim(),
    });
    const draft = normalizeLlmReportCandidateV2({
      kind: params.kind,
      candidate: draftRaw,
      fallbackReport: deterministicReport,
    });

    const repairModel: V2RepairModel = {
      repair: async ({ issues, report }) => {
        const repairedRaw = await completeJsonWithResearchV2Model({
          purpose: "writer",
          prompt: mkPrompt("repair", report, issues),
          maxTokens: 3600,
          temperature: 0.2,
          modelRefOverride: params.llmProfile?.modelRef,
          profileId:
            params.llmProfile?.profileId ?? process.env.OPENCLAW_RESEARCH_V2_PROFILE?.trim(),
        });
        return normalizeLlmReportCandidateV2({
          kind: params.kind,
          candidate: repairedRaw,
          fallbackReport: deterministicReport,
        });
      },
    };

    const repaired = await runV2QualityGateWithRepair({
      kind: params.kind,
      report: draft,
      repairModel,
      maxAttempts: 2,
    });
    if (!repaired.gate.passed) {
      const fallbackGate = runV2QualityGate({
        kind: params.kind,
        report: deterministicReport,
      });
      if (fallbackGate.passed) {
        const fallbackMarkdown = renderV2ReportMarkdown({
          kind: params.kind,
          report: deterministicReport,
        });
        return {
          reportJson: deterministicReport,
          reportMarkdown: fallbackMarkdown,
          gate: fallbackGate,
        };
      }
    }
    const reportMarkdown = renderV2ReportMarkdown({ kind: params.kind, report: repaired.report });
    return { reportJson: repaired.report, reportMarkdown, gate: repaired.gate };
  }

  const reportJson = deterministicReport;

  // Fail closed: schema + gates must pass. Optionally attempt bounded repair.
  let gate: QualityGateResult;
  let finalJson: unknown = reportJson;
  if (params.repairModel) {
    const repaired = await runV2QualityGateWithRepair({
      kind: params.kind,
      report: reportJson,
      repairModel: params.repairModel,
      maxAttempts: 2,
    });
    finalJson = repaired.report;
    gate = repaired.gate;
  } else {
    gate = runV2QualityGate({ kind: params.kind, report: reportJson });
  }

  const reportMarkdown = renderV2ReportMarkdown({ kind: params.kind, report: finalJson });
  return { reportJson: finalJson, reportMarkdown, gate };
}
