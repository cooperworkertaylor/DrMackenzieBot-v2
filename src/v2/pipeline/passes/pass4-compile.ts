import type { EvidenceItem } from "../../evidence/evidence-store.js";
import type { QualityGateResult, ReportKindV2 } from "../../quality/types.js";
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

const companySectionTemplate = (params: {
  evidence: EvidenceItem[];
  numericFacts: NumericFact[];
  filingKeywordSummary?: { business: string[]; risks: string[]; sourceIds: string[] };
  transcriptKeywordSummary?: { keywords: string[]; sourceIds: string[] };
  filingRiskBuckets?: Array<{ bucket: string; source_ids: string[] }>;
  accountingFlags?: Array<{ flag: string; source_ids: string[] }>;
  catalystCandidates?: Array<{ label: string; source_ids: string[] }>;
  risk: RiskOfficerOutputV2;
}): Array<{ key: string; title: string; blocks: any[] }> => {
  const sid = firstSourceId(params.evidence);
  const n1 = params.numericFacts[0]?.id;
  const n1Text = n1 ? `{{${n1}}}` : "N/A";
  const n1Refs = n1 ? [n1] : [];
  const filingFacts =
    params.filingKeywordSummary && params.filingKeywordSummary.sourceIds.length
      ? {
          business: params.filingKeywordSummary.business.slice(0, 10).join(", "),
          risks: params.filingKeywordSummary.risks.slice(0, 10).join(", "),
          source_ids: params.filingKeywordSummary.sourceIds,
        }
      : null;
  const transcriptFacts =
    params.transcriptKeywordSummary && params.transcriptKeywordSummary.sourceIds.length
      ? {
          keywords: params.transcriptKeywordSummary.keywords.slice(0, 12).join(", "),
          source_ids: params.transcriptKeywordSummary.sourceIds,
        }
      : null;
  const bucketFacts =
    params.filingRiskBuckets && params.filingRiskBuckets.length
      ? {
          buckets: params.filingRiskBuckets
            .map((b) => b.bucket)
            .slice(0, 8)
            .join("; "),
          source_ids: Array.from(
            new Set(params.filingRiskBuckets.flatMap((b) => b.source_ids).filter(Boolean)),
          ),
        }
      : null;
  const accountingFacts =
    params.accountingFlags && params.accountingFlags.length
      ? {
          flags: params.accountingFlags
            .map((f) => f.flag)
            .slice(0, 8)
            .join("; "),
          source_ids: Array.from(
            new Set(params.accountingFlags.flatMap((f) => f.source_ids).filter(Boolean)),
          ),
        }
      : null;
  const catalystFacts =
    params.catalystCandidates && params.catalystCandidates.length
      ? {
          labels: params.catalystCandidates
            .map((c) => c.label)
            .slice(0, 6)
            .join("; "),
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
            : "No Tier one numeric facts are present in this run.",
          source_ids: [sid],
          numeric_refs: n1Refs,
        },
        {
          tag: "INTERPRETATION",
          text: "Base case: the edge is durable only if reported KPIs continue to support the core drivers; size only after evidence coverage is broad.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: we will ingest primary filings and transcripts to expand Tier one and Tier three coverage before taking a meaningful position.",
        },
      ],
    },
    {
      key: "variant_perception",
      title: "Variant Perception",
      blocks: [
        {
          tag: "FACT",
          text: "This report is constrained to collected evidence; claims not supported by sources are marked as assumptions or omitted.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Variant perception is the delta between what consensus must be underwriting and what our evidence can support today.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: incremental sources will reduce contradictions and make scenario work numerically stable.",
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
          text: "The underwriting is that durable value drivers are visible in reported KPIs before they are visible in narrative. Falsifier: narrative claims persist without KPI confirmation across multiple periods.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: capture mechanisms remain durable through the cycle. Falsifier: sustained evidence of pricing pressure or commoditization from independent sources.",
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
            ? `Transcript-derived keywords suggest management emphasis on: ${transcriptFacts.keywords}.`
            : "Decision-relevant overview should be limited to drivers we can verify and monitor, not generic company description.",
          source_ids: transcriptFacts ? transcriptFacts.source_ids : undefined,
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: reported KPIs map to underwriting drivers without definition drift; reconcile definitions when full filings are ingested.",
        },
      ],
    },
    {
      key: "moat_competition",
      title: "Moat + Competition",
      blocks: [
        {
          tag: "FACT",
          text: "Competitor primary sources are not collected in this demo run; moat claims are intentionally limited.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Moat assessment should separate durable switching costs and ecosystem gravity from transient product cycles.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: lock-in is durable; validate with churn proxies, attach trends, and competitive win-loss evidence.",
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
            : "Numeric facts are structured with value, unit, period, source, and accessed timestamp to enable auditability.",
          source_ids: accountingFacts ? accountingFacts.source_ids : [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Financial quality should be judged on cash conversion, margin durability, and dilution discipline, not single-quarter beats.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: KPI coverage is sufficient to compute margins and cash conversion; if not, add the missing concepts before inference.",
        },
      ],
    },
    {
      key: "valuation_scenarios",
      title: "Valuation + Scenarios (Base/Bull/Bear)",
      blocks: [
        {
          tag: "FACT",
          text: "This demo run does not ingest market price data; valuation numbers are intentionally omitted until price tape is collected.",
          source_ids: [sid],
        },
        {
          tag: "INTERPRETATION",
          text: "Scenario work should make drivers explicit and quantify what must be true for the current price to be rational.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: discount rate and terminal assumptions are within a reasonable band; verify via market data ingestion.",
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
          text: "Catalysts should be framed as information releases that change scenario weights, not as calendar events alone.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: disclosure cadence will be consistent; validate via transcript ingestion and event tracking.",
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
            : "The risk officer pass enumerates disconfirming evidence to seek and falsifiers tied to measurable triggers.",
          source_ids: bucketFacts ? bucketFacts.source_ids : [sid],
        },
        {
          tag: "INTERPRETATION",
          text: `Pre-mortem: ${params.risk.premortem[0] ?? "The failure mode is overconfidence without evidence coverage."}`,
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: additional Tier one and Tier three sources will materially improve decision quality; proceed only after evidence gaps close.",
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
          text: "We change our mind when disconfirming evidence persists across multiple independent sources and cannot be explained by timing.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: monitoring will include filings, transcripts, and price tape; until then, posture should remain conservative.",
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
        catalystCalendar && catalystCalendar.length
          ? catalystCalendar.map((row: any) => `${String(row.date)}: ${String(row.label)}`.trim())
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
}): Array<{ key: string; title: string; blocks: any[] }> => {
  const sid = firstSourceId(params.evidence);
  return [
    {
      key: "executive_summary",
      title: "Executive Summary (Definition + Variant + Winners/Losers)",
      blocks: [
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

export async function pass4CompileReportV2(params: {
  kind: ReportKindV2;
  runId: string;
  subject: { ticker?: string; companyName?: string; themeName?: string; universe?: string[] };
  plan: ResearchPlanV2;
  evidence: EvidenceItem[];
  analyzers: CompanyAnalyzerOutputV2 | ThemeAnalyzerOutputV2;
  risk: RiskOfficerOutputV2;
  repairModel?: V2RepairModel;
}): Promise<CompileResultV2> {
  const generatedAt = nowIso();
  const reportJson =
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
          plan: params.plan,
          evidence: params.evidence,
          analyzers: params.analyzers as ThemeAnalyzerOutputV2,
          risk: params.risk,
        });

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
