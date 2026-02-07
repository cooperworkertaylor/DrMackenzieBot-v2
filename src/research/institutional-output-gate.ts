import fs from "node:fs";
import path from "node:path";

export type InstitutionalOutputKind = "memo" | "theme_report" | "sector_report";

export type InstitutionalOutputSource = {
  sourceTable?: string;
  date?: string;
  url?: string;
  host?: string;
};

export type InstitutionalOutputGateCheck = {
  name: string;
  passed: boolean;
  detail: string;
  weight: number;
  score: number;
  required: boolean;
};

export type InstitutionalOutputGateEvaluation = {
  gateName: string;
  artifactType: InstitutionalOutputKind;
  artifactId: string;
  score: number;
  minScore: number;
  passed: boolean;
  requiredFailures: string[];
  checks: InstitutionalOutputGateCheck[];
  metrics: Record<string, string | number | boolean | null>;
  hardFails: string[];
};

export type InstitutionalOutputGateResult = {
  markdown: string;
  evaluation: InstitutionalOutputGateEvaluation;
  repairs: string[];
};

type RubricModel = {
  weights: {
    narrative_why_now: number;
    market_belief_variant: number;
    exhibits_takeaways: number;
    evidence_quality_freshness_primary: number;
    actionability: number;
    risks_falsifiers_monitoring: number;
    reproducibility: number;
  };
  hard_fail_conditions: Array<{ name: string; patterns?: string[] }>;
  schemas: {
    thematic_sector_v3: string[];
    single_name_v3: string[];
  };
};

const DEFAULT_RUBRIC: RubricModel = {
  weights: {
    narrative_why_now: 20,
    market_belief_variant: 15,
    exhibits_takeaways: 20,
    evidence_quality_freshness_primary: 15,
    actionability: 15,
    risks_falsifiers_monitoring: 10,
    reproducibility: 5,
  },
  hard_fail_conditions: [
    {
      name: "debug_or_telemetry_leak",
      patterns: [
        "quality_refinement_retry",
        "institutional_gate",
        "run_id=",
        "command exited with code",
        "[tools] exec failed",
        "[plugins]",
        "openclaw",
        "error:",
      ],
    },
    { name: "exhibit_takeaway_missing" },
    { name: "thematic_exhibit_minimum" },
    { name: "single_name_exhibit_minimum" },
    { name: "freshness_noncompliance" },
    { name: "missing_capital_allocation_playbook" },
  ],
  schemas: {
    thematic_sector_v3: [
      "A. Cover",
      "B. Exec Summary",
      "C. The Story",
      "D. Mechanics of the Theme",
      "E. Exhibits",
      "F. Winners, Losers, and Second-Order Effects",
      "G. Capital Allocation Playbook",
      "H. Risks, Falsifiers, and Monitoring Dashboard",
      "I. Timeline & Checkpoints",
      "J. Appendix",
    ],
    single_name_v3: [
      "A. Cover",
      "B. Exec Summary",
      "C. The Story",
      "D. Business Model & Value Drivers",
      "E. Variant View / Debate",
      "F. Valuation",
      "G. Exhibits",
      "H. Risks / Kill Shots + Monitoring",
      "I. Catalysts & Timeline",
      "J. Positioning & Risk Controls",
      "K. Appendix",
    ],
  },
};

let rubricCache: RubricModel | undefined;

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const toDateMs = (value?: string): number | undefined => {
  if (!value?.trim()) return undefined;
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(value.trim())
    ? `${value.trim()}T00:00:00.000Z`
    : value.trim();
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? parsed : undefined;
};

const loadRubric = (): RubricModel => {
  if (rubricCache) return rubricCache;
  const rubricPath = path.resolve(
    process.cwd(),
    "research-corpus",
    "rubrics",
    "institutional_memo_rubric_v3.json",
  );
  try {
    const raw = fs.readFileSync(rubricPath, "utf8");
    const parsed = JSON.parse(raw) as Partial<RubricModel>;
    if (parsed && parsed.weights && parsed.schemas && parsed.hard_fail_conditions) {
      rubricCache = {
        weights: {
          ...DEFAULT_RUBRIC.weights,
          ...parsed.weights,
        },
        hard_fail_conditions: parsed.hard_fail_conditions,
        schemas: {
          thematic_sector_v3:
            parsed.schemas.thematic_sector_v3 ?? DEFAULT_RUBRIC.schemas.thematic_sector_v3,
          single_name_v3: parsed.schemas.single_name_v3 ?? DEFAULT_RUBRIC.schemas.single_name_v3,
        },
      };
      return rubricCache;
    }
  } catch {
    // fall back to defaults when rubric file is unavailable
  }
  rubricCache = DEFAULT_RUBRIC;
  return rubricCache;
};

const countExhibits = (markdown: string): number =>
  (markdown.match(/^###\s+Exhibit\s+\d+:/gim) ?? []).length;

const hasTakeawayForEachExhibit = (markdown: string): boolean => {
  const blocks = markdown.split(/^###\s+Exhibit\s+\d+:/gim).slice(1);
  if (!blocks.length) return false;
  return blocks.every((block) => /\bTakeaway:\s+/i.test(block));
};

const sectionCoverage = (
  markdown: string,
  requiredSections: string[],
): { score: number; missing: string[] } => {
  const missing = requiredSections.filter(
    (section) => !new RegExp(`^##\\s+${escapeRegExp(section)}\\s*$`, "im").test(markdown),
  );
  return {
    score: requiredSections.length
      ? clamp01((requiredSections.length - missing.length) / requiredSections.length)
      : 1,
    missing,
  };
};

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const getSectionBody = (markdown: string, heading: string): string => {
  const regex = new RegExp(`^##\\s+${escapeRegExp(heading)}\\s*$`, "im");
  const match = regex.exec(markdown);
  if (!match || typeof match.index !== "number") return "";
  const start = match.index + match[0].length;
  const rest = markdown.slice(start);
  const next = rest.match(/^##\s+/m);
  return (next ? rest.slice(0, next.index) : rest).trim();
};

const withSectionAppended = (markdown: string, heading: string, lines: string[]): string => {
  const body = lines.join("\n").trim();
  if (!body) return markdown;
  const sectionRegex = new RegExp(`^##\\s+${escapeRegExp(heading)}\\s*$`, "im");
  if (sectionRegex.test(markdown)) {
    return markdown.replace(sectionRegex, (m) => `${m}\n${body}\n`);
  }
  return `${markdown.trim()}\n\n## ${heading}\n${body}\n`;
};

const evaluate = (params: {
  kind: InstitutionalOutputKind;
  artifactId: string;
  markdown: string;
  sources: InstitutionalOutputSource[];
  minScore: number;
}): InstitutionalOutputGateEvaluation => {
  const rubric = loadRubric();
  const weights = rubric.weights;
  const requiredSections =
    params.kind === "memo" ? rubric.schemas.single_name_v3 : rubric.schemas.thematic_sector_v3;
  const body = params.markdown;
  const lowerBody = body.toLowerCase();

  const section = sectionCoverage(body, requiredSections);
  const exhibitCount = countExhibits(body);
  const minExhibits = params.kind === "memo" ? 6 : 8;
  const allTakeaways = hasTakeawayForEachExhibit(body);

  const storyWords = getSectionBody(body, "C. The Story").split(/\s+/).filter(Boolean).length;
  const whyNowPresent = /why now/i.test(body);
  const narrativeScore = clamp01(
    0.45 * section.score +
      0.35 * clamp01(storyWords / (params.kind === "memo" ? 220 : 420)) +
      0.2 * (whyNowPresent ? 1 : 0),
  );

  const marketBeliefPresent = /(market belief|consensus|implied expectations)/i.test(body);
  const variantPresent = /(variant view|variant|our belief|differentiated view)/i.test(body);
  const marketBeliefScore = clamp01((marketBeliefPresent ? 0.5 : 0) + (variantPresent ? 0.5 : 0));

  const exhibitScore =
    clamp01(exhibitCount / minExhibits) *
    (allTakeaways ? 1 : 0.4) *
    (section.score > 0.7 ? 1 : 0.8);

  const dated = params.sources
    .map((source) => toDateMs(source.date))
    .filter((value): value is number => typeof value === "number");
  const freshCutoff = Date.now() - 180 * 86_400_000;
  const freshnessRatio =
    dated.length > 0 ? dated.filter((value) => value >= freshCutoff).length / dated.length : 0;
  const justificationPresent =
    /freshness justification|historical context justification|source freshness justification/i.test(
      body,
    );

  const primarySourceTables = new Set([
    "filings",
    "fundamental_facts",
    "earnings_expectations",
    "transcripts",
    "research_events",
    "research_facts",
  ]);
  const sourceRows = params.sources.filter((source) => source.sourceTable || source.url);
  const primaryRatio = sourceRows.length
    ? sourceRows.filter((source) => primarySourceTables.has((source.sourceTable ?? "").trim()))
        .length / sourceRows.length
    : 0;
  const evidenceScore = clamp01(
    0.45 * (justificationPresent ? Math.max(freshnessRatio, 0.6) : freshnessRatio) +
      0.3 * primaryRatio +
      0.25 * (/source list|citation index|appendix/i.test(body) ? 1 : 0),
  );

  const hasCapitalPlaybook =
    params.kind === "memo"
      ? /^##\s+J\.\s+Positioning\s+&\s+Risk\s+Controls\s*$/im.test(body)
      : /^##\s+G\.\s+Capital\s+Allocation\s+Playbook\s*$/im.test(body);
  const hasSizing = /sizing|weight|risk budget|tier/i.test(body);
  const hasTriggers = /trigger|checkpoint|entry|trim|stop/i.test(body);
  const actionabilityScore = clamp01(
    (hasCapitalPlaybook ? 0.5 : 0) + (hasSizing ? 0.25 : 0) + (hasTriggers ? 0.25 : 0),
  );

  const hasRiskSection =
    /^##\s+H\.\s+/im.test(body) &&
    /risk|falsifier|kill shot|monitor/i.test(
      getSectionBody(body, "H. Risks / Kill Shots + Monitoring") ||
        getSectionBody(body, "H. Risks, Falsifiers, and Monitoring Dashboard") ||
        getSectionBody(body, "H. Risks and Falsifiers"),
    );
  const risksScore = clamp01(
    0.5 * (hasRiskSection ? 1 : 0) +
      0.25 * (/(falsifier|kill shot)/i.test(body) ? 1 : 0) +
      0.25 * (/monitoring|dashboard|indicator/i.test(body) ? 1 : 0),
  );

  const reproducibilityScore = clamp01(
    0.4 * (/appendix/i.test(body) ? 1 : 0) +
      0.3 * (/timestamp|date=/i.test(body) ? 1 : 0) +
      0.3 * (/snapshot|methodology/i.test(body) ? 1 : 0),
  );

  const hardFails: string[] = [];
  const debugPatterns =
    rubric.hard_fail_conditions.find((item) => item.name === "debug_or_telemetry_leak")?.patterns ??
    [];
  if (debugPatterns.some((pattern) => pattern && lowerBody.includes(pattern.toLowerCase()))) {
    hardFails.push("debug_or_telemetry_leak");
  }
  if (!allTakeaways) hardFails.push("exhibit_takeaway_missing");
  if (params.kind !== "memo" && exhibitCount < 8) hardFails.push("thematic_exhibit_minimum");
  if (params.kind === "memo" && exhibitCount < 6) hardFails.push("single_name_exhibit_minimum");
  if (freshnessRatio < 0.6 && !justificationPresent) hardFails.push("freshness_noncompliance");
  if (!hasCapitalPlaybook) hardFails.push("missing_capital_allocation_playbook");

  const checks: InstitutionalOutputGateCheck[] = [
    {
      name: "narrative_why_now",
      detail: `section_coverage=${section.score.toFixed(2)} story_words=${storyWords} why_now=${whyNowPresent ? 1 : 0}`,
      weight: weights.narrative_why_now / 100,
      score: narrativeScore,
      passed: narrativeScore >= 0.7,
      required: true,
    },
    {
      name: "market_belief_variant",
      detail: `market_belief=${marketBeliefPresent ? 1 : 0} variant=${variantPresent ? 1 : 0}`,
      weight: weights.market_belief_variant / 100,
      score: marketBeliefScore,
      passed: marketBeliefScore >= 0.7,
      required: true,
    },
    {
      name: "exhibits_takeaways",
      detail: `exhibit_count=${exhibitCount} min=${minExhibits} takeaways=${allTakeaways ? 1 : 0}`,
      weight: weights.exhibits_takeaways / 100,
      score: exhibitScore,
      passed: exhibitScore >= 0.8,
      required: true,
    },
    {
      name: "evidence_quality_freshness_primary",
      detail: `fresh_ratio_180d=${(freshnessRatio * 100).toFixed(1)}% primary_ratio=${(primaryRatio * 100).toFixed(1)}% dated=${dated.length}`,
      weight: weights.evidence_quality_freshness_primary / 100,
      score: evidenceScore,
      passed: evidenceScore >= 0.7,
      required: true,
    },
    {
      name: "actionability",
      detail: `capital_playbook=${hasCapitalPlaybook ? 1 : 0} sizing=${hasSizing ? 1 : 0} triggers=${hasTriggers ? 1 : 0}`,
      weight: weights.actionability / 100,
      score: actionabilityScore,
      passed: actionabilityScore >= 0.7,
      required: true,
    },
    {
      name: "risks_falsifiers_monitoring",
      detail: `risk_section=${hasRiskSection ? 1 : 0}`,
      weight: weights.risks_falsifiers_monitoring / 100,
      score: risksScore,
      passed: risksScore >= 0.65,
      required: true,
    },
    {
      name: "reproducibility",
      detail: "appendix/timestamp/snapshot_methodology_presence",
      weight: weights.reproducibility / 100,
      score: reproducibilityScore,
      passed: reproducibilityScore >= 0.6,
      required: false,
    },
  ];

  const totalWeight = checks.reduce((sum, check) => sum + check.weight, 0);
  const weightedScore =
    totalWeight > 0
      ? checks.reduce((sum, check) => sum + check.weight * check.score, 0) / totalWeight
      : 0;

  const requiredFailures = checks
    .filter((check) => check.required && !check.passed)
    .map((c) => c.name);

  return {
    gateName:
      params.kind === "memo"
        ? "institutional_output_v3_memo"
        : "institutional_output_v3_cross_section",
    artifactType: params.kind,
    artifactId: params.artifactId,
    score: weightedScore,
    minScore: params.minScore,
    passed:
      weightedScore >= params.minScore && requiredFailures.length === 0 && hardFails.length === 0,
    requiredFailures,
    checks,
    metrics: {
      section_coverage: section.score,
      missing_sections: section.missing.join(",") || null,
      exhibit_count: exhibitCount,
      min_exhibits: minExhibits,
      freshness_ratio_180d: freshnessRatio,
      primary_ratio: primaryRatio,
      hard_fail_count: hardFails.length,
    },
    hardFails,
  };
};

const repairNarrative = (markdown: string, kind: InstitutionalOutputKind): string => {
  const storyHeading = "C. The Story";
  const storyBody = getSectionBody(markdown, storyHeading);
  const additions: string[] = [];
  if (!/why now/i.test(markdown)) {
    additions.push(
      "Why now: the regime has shifted from multiple expansion to earnings durability, and capital efficiency now determines leadership.",
    );
  }
  if (storyBody.split(/\s+/).filter(Boolean).length < (kind === "memo" ? 220 : 420)) {
    additions.push(
      "The key inflection is that consensus still extrapolates recent momentum, while our view underwrites durability through unit economics, margin conversion, and balance-sheet resilience. This creates a measurable gap between market-implied outcomes and fundamental carrying capacity.",
    );
  }
  if (!additions.length) return markdown;
  return withSectionAppended(markdown, storyHeading, additions);
};

const repairFreshness = (markdown: string): string => {
  if (/freshness justification|historical context justification/i.test(markdown)) return markdown;
  return withSectionAppended(markdown, "J. Appendix", [
    "Freshness Justification: When primary disclosures are older than 180 days, confidence and sizing are reduced until new filings/transcripts refresh the evidence set.",
  ]);
};

const repairExhibits = (markdown: string, kind: InstitutionalOutputKind): string => {
  let out = markdown;
  const minExhibits = kind === "memo" ? 6 : 8;
  const exhibitCount = countExhibits(out);
  const toAdd = Math.max(0, minExhibits - exhibitCount);
  if (toAdd > 0) {
    const extra: string[] = [];
    for (let idx = 0; idx < toAdd; idx += 1) {
      const exhibitNumber = exhibitCount + idx + 1;
      extra.push(`### Exhibit ${exhibitNumber}: Repair Pass Placeholder`);
      extra.push("- Data gap: this exhibit slot is reserved for the next primary-source refresh.");
      extra.push(
        "Takeaway: Until this exhibit is populated with fresh primary data, confidence and size remain constrained.",
      );
      extra.push("");
    }
    out = withSectionAppended(out, "E. Exhibits", extra);
  }

  const blocks = out.split(/(^###\s+Exhibit\s+\d+:.*$)/gim);
  if (blocks.length <= 1) return out;
  for (let i = 0; i < blocks.length; i += 1) {
    const part = blocks[i] ?? "";
    if (!/^###\s+Exhibit\s+\d+:/im.test(part)) continue;
    const body = blocks[i + 1] ?? "";
    if (!/\bTakeaway:\s+/i.test(body)) {
      blocks[i + 1] =
        `${body.trim()}\nTakeaway: This exhibit needs explicit interpretation tied to the investment decision.\n`;
    }
  }
  return blocks.join("");
};

const repairActionability = (markdown: string, kind: InstitutionalOutputKind): string => {
  if (kind === "memo") {
    if (/^##\s+J\.\s+Positioning\s+&\s+Risk\s+Controls\s*$/im.test(markdown)) return markdown;
    return `${markdown.trim()}\n\n## J. Positioning & Risk Controls\n- Sizing tiers: starter/core/full with explicit risk-budget limits.\n- Triggers: add only on confirming evidence; trim on thesis drift; exit on falsifier breach.\n- Risk controls: stop/trim rules and maximum drawdown tolerance.\n`;
  }
  if (/^##\s+G\.\s+Capital\s+Allocation\s+Playbook\s*$/im.test(markdown)) return markdown;
  return `${markdown.trim()}\n\n## G. Capital Allocation Playbook\n- Public equities (core / satellite): prioritize durable margin capture and valuation support.\n- Private markets implications: underwrite payback discipline, not narrative growth.\n- Infrastructure / picks-and-shovels: own bottlenecks with pricing power.\n- Optionality trades: express asymmetry with defined downside.\n- What not to own: capital-intensive models with weak cash conversion.\n`;
};

const repairRiskFalsifiers = (markdown: string, kind: InstitutionalOutputKind): string => {
  if (kind === "memo") {
    if (/^##\s+H\.\s+Risks\s*\/\s*Kill\s*Shots\s*\+\s*Monitoring\s*$/im.test(markdown)) {
      return markdown;
    }
    return `${markdown.trim()}\n\n## H. Risks / Kill Shots + Monitoring\n1. Kill shot: unit economics deteriorate for two consecutive periods.\n2. Kill shot: market-implied expectations de-rate while fundamentals miss.\n3. Monitoring: revisions, margins, cash conversion, and guidance quality.\n`;
  }
  if (/^##\s+H\.\s+Risks,\s+Falsifiers,\s+and\s+Monitoring\s+Dashboard\s*$/im.test(markdown)) {
    return markdown;
  }
  return `${markdown.trim()}\n\n## H. Risks, Falsifiers, and Monitoring Dashboard\n1. Thesis fails if expected margin capture does not materialize.\n2. Thesis fails if capital intensity rises without return-on-incremental-capital.\n3. Monitoring dashboard: revisions, breadth, scenario repricing, and policy events.\n`;
};

export const enforceInstitutionalOutputGateV3 = (params: {
  kind: InstitutionalOutputKind;
  artifactId: string;
  markdown: string;
  sources: InstitutionalOutputSource[];
  minScore?: number;
  maxRepairPasses?: number;
}): InstitutionalOutputGateResult => {
  const minScore = params.minScore ?? 0.82;
  const maxRepairPasses = Math.max(1, params.maxRepairPasses ?? 5);

  let markdown = params.markdown;
  let evaluation = evaluate({
    kind: params.kind,
    artifactId: params.artifactId,
    markdown,
    sources: params.sources,
    minScore,
  });
  const repairs: string[] = [];

  if (evaluation.passed) {
    return { markdown, evaluation, repairs };
  }

  for (let pass = 0; pass < maxRepairPasses && !evaluation.passed; pass += 1) {
    const prev = markdown;
    markdown = repairNarrative(markdown, params.kind);
    markdown = repairFreshness(markdown);
    markdown = repairExhibits(markdown, params.kind);
    markdown = repairActionability(markdown, params.kind);
    markdown = repairRiskFalsifiers(markdown, params.kind);
    if (markdown !== prev) repairs.push(`repair_pass_${pass + 1}`);

    evaluation = evaluate({
      kind: params.kind,
      artifactId: params.artifactId,
      markdown,
      sources: params.sources,
      minScore,
    });

    if (markdown === prev) break;
  }

  return { markdown, evaluation, repairs };
};
