import { describe, expect, it } from "vitest";
import { runV2QualityGate } from "./quality-gate.js";

const demoCompanyReport = () => ({
  version: 2,
  kind: "company",
  run_id: "demo001",
  generated_at: "2026-02-10T00:00:00Z",
  subject: {
    ticker: "AAPL",
    company_name: "Apple Inc.",
  },
  plan: {
    posture: "long-only",
    horizon: "12-36 months",
    timebox_minutes: 60,
    key_questions: [
      "What are the durable value drivers and risks over the next business cycle?",
      "What would falsify the thesis quickly?",
    ],
    required_exhibits: [
      "kpi_table",
      "margins",
      "fcf",
      "sbc_dilution",
      "scenario_drivers",
      "sensitivity",
    ],
  },
  sources: [
    {
      id: "S1",
      title: "SEC XBRL time-series (Apple Inc.)",
      publisher: "SEC",
      date_published: "2026-02-06",
      accessed_at: "2026-02-06T00:53:04.010Z",
      url: "https://www.sec.gov/edgar/search/",
      reliability_tier: 1,
      excerpt_or_key_points: [
        "Time-series points for key us-gaap concepts pulled from SEC XBRL submissions.",
        "Includes revenue, operating income, cash from operations, and balance sheet lines by period end and filing date.",
      ],
      raw_text_ref: "examples/fixtures/sec-edgar/aapl/time-series.csv",
      tags: ["company:AAPL", "source:sec", "kpi:revenue"],
    },
  ],
  numeric_facts: [
    {
      id: "N1",
      value: 265595000000,
      unit: "USD",
      period: "FY2018",
      currency: "USD",
      source_id: "S1",
      accessed_at: "2026-02-06T00:53:04.010Z",
      notes: "Apple Revenues (us-gaap:Revenues) for FY2018 from time-series.csv",
    },
  ],
  sections: [
    {
      key: "executive_summary",
      title: "Executive Summary",
      blocks: [
        {
          tag: "FACT",
          text: "Revenue in the SEC XBRL time-series for fiscal year twenty eighteen is {{N1}}.",
          source_ids: ["S1"],
          numeric_refs: ["N1"],
        },
        {
          tag: "INTERPRETATION",
          text: "Scale is not the edge; the edge is translating scale into durable unit economics and cycle resilience.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: segment disclosure remains comparable enough to track mix and margin drivers; verify with filings when available.",
        },
      ],
    },
    {
      key: "variant_perception",
      title: "Variant Perception",
      blocks: [
        {
          tag: "FACT",
          text: "The time-series source is a structured extract from SEC filings and should be treated as Tier one evidence for reported financials.",
          source_ids: ["S1"],
        },
        {
          tag: "INTERPRETATION",
          text: "Variant perception work should focus on what consensus is implicitly underwriting versus what the evidence can actually support.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: near-term narrative dispersion will concentrate around mix and capital return rather than top-line growth; validate via transcript and estimate revisions.",
        },
      ],
    },
    {
      key: "thesis",
      title: "Thesis (Falsifiable)",
      blocks: [
        {
          tag: "FACT",
          text: "Reported revenue base for fiscal year twenty eighteen was {{N1}}. Falsifier: an updated SEC extract or restatement shows materially different reported revenue.",
          source_ids: ["S1"],
          numeric_refs: ["N1"],
        },
        {
          tag: "INTERPRETATION",
          text: "The underwriting is that governance, ecosystem lock-in, and capital return discipline create durable cash generation. Falsifier: multi-period evidence of structurally lower cash conversion versus history.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: services mix and attach rates remain a key stabilizer through the cycle. Falsifier: mix shifts materially against higher-margin lines without offsetting volume.",
        },
      ],
    },
    {
      key: "business_overview",
      title: "Business Overview (Decision-Relevant Only)",
      blocks: [
        {
          tag: "FACT",
          text: "The SEC time-series includes both annual and interim points and encodes form type and filing date for each metric.",
          source_ids: ["S1"],
        },
        {
          tag: "INTERPRETATION",
          text: "Decision relevance: focus on where operating leverage and cash generation are structurally repeatable.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: accounting definitions are stable enough across periods to compare trend lines; confirm when full filing text is ingested.",
        },
      ],
    },
    {
      key: "moat_competition",
      title: "Moat + Competition",
      blocks: [
        {
          tag: "FACT",
          text: "This demo artifact does not include competitor primary sources; moat claims are intentionally limited.",
          source_ids: ["S1"],
        },
        {
          tag: "INTERPRETATION",
          text: "Moat assessment should separate switching costs and ecosystem gravity from transient product cycles.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: customer lock-in is durable; validate with churn proxies and attach trends once those sources are collected.",
        },
      ],
    },
    {
      key: "financial_quality",
      title: "Financial Quality (KPI Table)",
      blocks: [
        {
          tag: "FACT",
          text: "The SEC time-series is sufficient to build KPI trend exhibits with filing provenance.",
          source_ids: ["S1"],
        },
        {
          tag: "INTERPRETATION",
          text: "Financial quality should be judged on cash conversion, margin durability, and dilution discipline, not single-quarter beats.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: KPI definitions map cleanly to underwriting drivers; reconcile when segment-level data is ingested.",
        },
      ],
    },
    {
      key: "valuation_scenarios",
      title: "Valuation + Scenarios",
      blocks: [
        {
          tag: "FACT",
          text: "This demo does not include market price data; valuation numbers are intentionally omitted.",
          source_ids: ["S1"],
        },
        {
          tag: "INTERPRETATION",
          text: "Scenario work should make the key drivers explicit and quantify what the market must believe for the current price to be rational.",
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
          text: "No catalyst calendar sources are included in this demo; catalyst claims are treated as unknown.",
          source_ids: ["S1"],
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
      title: "Key Risks + Pre-mortem",
      blocks: [
        {
          tag: "FACT",
          text: "The current evidence set is thin, which is itself a risk; conclusions are constrained by source coverage.",
          source_ids: ["S1"],
        },
        {
          tag: "INTERPRETATION",
          text: "Pre-mortem: the failure mode is overconfidence in narrative drivers without validating cash conversion and dilution.",
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
          text: "Trigger design must be measurable and tied to collected sources; this demo includes only a single numeric anchor.",
          source_ids: ["S1"],
        },
        {
          tag: "INTERPRETATION",
          text: "We change our mind when disconfirming evidence persists across multiple independent sources and cannot be explained by timing.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: the monitoring stack will include filings, transcripts, and price tape; until then, posture should remain conservative.",
        },
      ],
    },
  ],
  exhibits: [
    {
      id: "X1",
      title: "KPI Table (KPI)",
      question: "What are the anchor KPIs and what is the reported baseline?",
      data_summary: ["Reported revenue baseline: {{N1}} (see numeric facts)."],
      takeaway: "Takeaway: Establish a sourced KPI baseline before layering interpretation.",
      source_ids: ["S1"],
      numeric_refs: ["N1"],
    },
    {
      id: "X2",
      title: "Margins (Margin)",
      question: "What is the operating leverage profile over time?",
      data_summary: ["N/A in this demo (margin series not included in the fixture extract)."],
      takeaway: "Takeaway: Do not assert margin trends without sourced series.",
      source_ids: ["S1"],
    },
    {
      id: "X3",
      title: "FCF (FCF)",
      question: "Is free cash flow conversion durable across cycles?",
      data_summary: [
        "N/A in this demo (free cash flow series not included in the fixture extract).",
      ],
      takeaway: "Takeaway: Cash conversion is the decision metric; collect it before sizing risk.",
      source_ids: ["S1"],
    },
    {
      id: "X4",
      title: "SBC and Dilution (SBC / dilution)",
      question: "Is dilution controlled relative to cash generation?",
      data_summary: ["N/A in this demo (share count series not included in the fixture extract)."],
      takeaway: "Takeaway: Treat dilution as a recurring expense unless proven otherwise.",
      source_ids: ["S1"],
    },
    {
      id: "X5",
      title: "Scenario Drivers (Scenario driver)",
      question: "Which variables drive scenario outcomes?",
      data_summary: [
        "Key drivers to quantify: mix, margins, cash conversion, and capital returns (sources pending).",
      ],
      takeaway: "Takeaway: Explicit drivers prevent narrative drift.",
      source_ids: ["S1"],
    },
    {
      id: "X6",
      title: "Sensitivity (Sensitivity)",
      question: "How sensitive is the thesis to a small number of variables?",
      data_summary: [
        "Sensitivity table is N/A until valuation inputs and price tape are ingested.",
      ],
      takeaway: "Takeaway: Sensitivity is how you separate conviction from leverage.",
      source_ids: ["S1"],
    },
  ],
  appendix: {
    evidence_table: [
      {
        claim:
          "Apple reported revenue of {{N1}} in fiscal year twenty eighteen (per SEC XBRL time-series).",
        evidence_ids: ["S1"],
        source_ids: ["S1"],
      },
    ],
    whats_missing: [
      "Primary filing text for narrative disclosures (risk factors, segment mix).",
      "Price tape and valuation inputs to quantify scenarios and sensitivity.",
      "Transcript sources to validate management commentary versus filings.",
    ],
  },
});

describe("v2 quality gate", () => {
  it("rejects placeholder language", () => {
    const report = demoCompanyReport();
    report.sections[0].blocks.push({
      tag: "INTERPRETATION",
      text: "Exhibits and CSV/queries are available on request.",
    });

    const res = runV2QualityGate({ kind: "company", report });
    expect(res.passed).toBe(false);
    expect(res.issues.map((i) => i.code)).toContain("style_placeholder_language");
  });
});

const demoThemeReport = () => ({
  version: 2,
  kind: "theme",
  run_id: "demo002",
  generated_at: "2026-02-10T00:00:00Z",
  subject: {
    theme_name: "Evidence-First Fundamental Underwriting",
    universe: ["AAPL", "PLTR"],
  },
  plan: {
    posture: "long-only",
    horizon: "12-36 months",
    timebox_minutes: 120,
    key_questions: [
      "Where does value accrue and what are the capture mechanisms?",
      "What are the falsifiers and monitoring signals?",
    ],
    required_exhibits: [
      "value_chain_map",
      "capture_scorecard",
      "adoption_dashboard",
      "catalyst_calendar",
      "risk_heatmap",
    ],
  },
  sources: [
    {
      id: "S1",
      title: "SEC XBRL time-series (Apple Inc.)",
      publisher: "SEC",
      date_published: "2026-02-06",
      accessed_at: "2026-02-06T00:53:04.010Z",
      url: "https://www.sec.gov/edgar/search/",
      reliability_tier: 1,
      excerpt_or_key_points: [
        "Demonstrates how to ground reported KPIs in a reproducible extract with filing provenance.",
      ],
      raw_text_ref: "examples/fixtures/sec-edgar/aapl/time-series.csv",
      tags: ["company:AAPL", "theme:evidence-first", "source:sec"],
    },
    {
      id: "S2",
      title: "Internal: v2 research quality gates specification",
      publisher: "Internal",
      date_published: "2026-02-10",
      accessed_at: "2026-02-10T00:00:00Z",
      url: "https://example.com/internal/v2-quality-gates",
      reliability_tier: 4,
      excerpt_or_key_points: [
        "Defines contracts for citations, numeric provenance, and required exhibits.",
      ],
      tags: ["theme:evidence-first", "internal:spec"],
    },
  ],
  numeric_facts: [],
  sections: [
    {
      key: "executive_summary",
      title: "Executive Summary",
      blocks: [
        {
          tag: "FACT",
          text: "SEC-derived KPI extracts are Tier one evidence for reported historical financials and should anchor any underwriting.",
          source_ids: ["S1"],
        },
        {
          tag: "INTERPRETATION",
          text: "The variant is not the narrative, it is the delta between what is provable and what is priced.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: the stack will ingest filings, transcripts, and price tape for all names in the universe; until then, posture remains limited.",
        },
      ],
    },
    {
      key: "what_it_is_isnt_why_now",
      title: "What It Is / Is Not + Why Now",
      blocks: [
        {
          tag: "FACT",
          text: "This theme memo is a framework demo; it intentionally avoids unsupported claims about companies without collected sources.",
          source_ids: ["S2"],
        },
        {
          tag: "INTERPRETATION",
          text: "Why now: without contracts, agents drift into generic prose; contracts make research reproducible and auditable.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: stricter gates will reduce throughput but increase trust and downstream portfolio usability.",
        },
      ],
    },
    {
      key: "value_chain",
      title: "Where Value Accrues (Value Chain)",
      blocks: [
        {
          tag: "FACT",
          text: "Reliable value-chain mapping requires primary sources for each segment; only one Tier one source is present in this demo.",
          source_ids: ["S1"],
        },
        {
          tag: "INTERPRETATION",
          text: "Value accrues to platforms that control data provenance and to operators that can convert model output into governed workflows.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: buyer preference will shift toward auditable systems as regulation and model risk management mature.",
        },
      ],
    },
    {
      key: "capture_ledger",
      title: "Capture Ledger (Mechanisms + Scorecard)",
      blocks: [
        {
          tag: "FACT",
          text: "Capture scoring should be backed by a claim-to-evidence table so that each score is auditable.",
          source_ids: ["S2"],
        },
        {
          tag: "INTERPRETATION",
          text: "Mechanisms: distribution control, workflow embedding, switching costs, and regulated deployment competence.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: monetization will consolidate around a small number of workflow layers rather than generalized model providers.",
        },
      ],
    },
    {
      key: "beneficiaries_vs_left_behind",
      title: "Beneficiaries vs Left Behind",
      blocks: [
        {
          tag: "FACT",
          text: "Without collected sources for all universe members, this section is limited to a process recommendation.",
          source_ids: ["S2"],
        },
        {
          tag: "INTERPRETATION",
          text: "Beneficiaries are those that can package capability into governed outcomes; left behind are those selling undifferentiated capability.",
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
          text: "Catalyst calendars must be backed by dated sources; this demo does not include those sources.",
          source_ids: ["S2"],
        },
        {
          tag: "INTERPRETATION",
          text: "Use catalysts as information releases that update scenario weights, not as deterministic events.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: adoption milestones will be visible in filings and transcripts before they are visible in price tape.",
        },
      ],
    },
    {
      key: "risks_falsifiers",
      title: "Risks + Falsifiers",
      blocks: [
        {
          tag: "FACT",
          text: "Falsifiers should be measurable and monitored via the evidence library, otherwise they degrade into narrative.",
          source_ids: ["S2"],
        },
        {
          tag: "INTERPRETATION",
          text: "Primary risks: hype outruns workflow integration, regulation increases cost of deployment, and commoditization compresses pricing power.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: a monitoring dashboard will track evidence freshness, contradictions, and KPI deltas for each name.",
        },
      ],
    },
    {
      key: "portfolio_posture",
      title: "Portfolio Posture",
      blocks: [
        {
          tag: "FACT",
          text: "The version two gates fail closed when sourcing or numeric provenance is missing, preventing accidental deployment based on unsourced prose.",
          source_ids: ["S2"],
        },
        {
          tag: "INTERPRETATION",
          text: "Preferred expression is staged exposure: pilot positions only after evidence coverage and falsifiers are defined.",
        },
        {
          tag: "ASSUMPTION",
          text: "Assumption: the opportunity set is wide enough that skipping low-evidence names improves risk-adjusted returns.",
        },
      ],
    },
  ],
  exhibits: [
    {
      id: "X1",
      title: "Value Chain Map (Value chain)",
      question: "Where does value accrue along the workflow from evidence to decision?",
      data_summary: [
        "Evidence collection -> analyzers -> skeptical review -> memo compiler -> portfolio decision.",
      ],
      takeaway:
        "Takeaway: Value accrues to systems that make evidence reusable and decisions auditable.",
      source_ids: ["S2"],
    },
    {
      id: "X2",
      title: "Capture Scorecard (Capture scorecard)",
      question: "Which mechanisms drive capture and how do we score them?",
      data_summary: [
        "Distribution, workflow embedding, governance, and switching cost are scored only when evidence exists.",
      ],
      takeaway: "Takeaway: Scorecards without evidence are opinions, not research.",
      source_ids: ["S2"],
    },
    {
      id: "X3",
      title: "Adoption Dashboard (Adoption dashboard)",
      question: "What adoption signposts should we monitor?",
      data_summary: [
        "Filings: KPI deltas and disclosure; transcripts: customer and pipeline signals; price: regime shifts.",
      ],
      takeaway: "Takeaway: Dashboards turn themes into monitorable systems.",
      source_ids: ["S2"],
    },
    {
      id: "X4",
      title: "Catalyst Calendar (Catalyst calendar)",
      question: "What dated events can change scenario weights?",
      data_summary: [
        "Earnings cycles, major product releases, and regulatory deadlines (sources pending).",
      ],
      takeaway: "Takeaway: A calendar is only as good as its dated sources.",
      source_ids: ["S2"],
    },
    {
      id: "X5",
      title: "Risk Heatmap (Risk heatmap)",
      question: "Where do the biggest failure modes sit across mechanism and time?",
      data_summary: [
        "Execution risk, regulation risk, and commoditization risk require explicit falsifiers.",
      ],
      takeaway: "Takeaway: Heatmaps are useful only when paired with triggers that change posture.",
      source_ids: ["S2"],
    },
  ],
  appendix: {
    evidence_table: [
      {
        claim: "SEC-derived KPI extracts are Tier one evidence for reported historical financials.",
        evidence_ids: ["S1"],
        source_ids: ["S1"],
      },
      {
        claim: "v2 gates define fail-closed rules for citations and numeric provenance.",
        evidence_ids: ["S2"],
        source_ids: ["S2"],
      },
    ],
    whats_missing: [
      "Theme-specific primary sources for every universe member (filings and transcripts).",
      "Dated catalyst sources to populate the calendar with evidence-backed events.",
      "Quantitative adoption dashboard once KPI sources are ingested for all names.",
    ],
  },
});

describe("v2 quality gate", () => {
  it("passes a minimal valid company report", () => {
    const res = runV2QualityGate({ kind: "company", report: demoCompanyReport() });
    expect(res.issues).toEqual([]);
    expect(res.passed).toBe(true);
  });

  it("fails when a FACT block is missing citations", () => {
    const report = demoCompanyReport();
    report.sections[0].blocks[0].source_ids = [];
    const res = runV2QualityGate({ kind: "company", report });
    expect(res.passed).toBe(false);
    expect(res.issues.some((i) => i.code === "citation_missing")).toBe(true);
  });

  it("fails when prose contains raw digits", () => {
    const report = demoCompanyReport();
    report.sections[0].blocks[1].text = "This includes a number 123 without provenance.";
    const res = runV2QualityGate({ kind: "company", report });
    expect(res.passed).toBe(false);
    expect(res.issues.some((i) => i.code === "numeric_in_prose")).toBe(true);
  });

  it("passes a minimal valid theme report", () => {
    const res = runV2QualityGate({ kind: "theme", report: demoThemeReport() });
    expect(res.issues).toEqual([]);
    expect(res.passed).toBe(true);
  });
});
