import type { EvidenceItem } from "../evidence/evidence-store.js";
import type { ReportKindV2 } from "../quality/types.js";

type NumericFact = {
  id: string;
  value: number;
  unit: string;
  period: string;
  currency?: string;
  source_id: string;
  accessed_at: string;
  notes?: string;
};

type ReportV2 = {
  version: number;
  kind: ReportKindV2;
  run_id: string;
  generated_at: string;
  subject: Record<string, unknown>;
  plan: Record<string, unknown>;
  sources: EvidenceItem[];
  numeric_facts: NumericFact[];
  sections: Array<{
    key: string;
    title: string;
    na_reason?: string;
    blocks: Array<{
      tag: "FACT" | "INTERPRETATION" | "ASSUMPTION";
      text: string;
      source_ids?: string[];
      numeric_refs?: string[];
    }>;
  }>;
  exhibits: Array<{
    id: string;
    title: string;
    question: string;
    data_summary: string[];
    takeaway: string;
    source_ids: string[];
    numeric_refs?: string[];
    notes?: string;
  }>;
  appendix: {
    evidence_table: Array<{
      claim: string;
      evidence_ids: string[];
      source_ids: string[];
    }>;
    whats_missing: string[];
  };
};

const asArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const asString = (value: unknown): string => (typeof value === "string" ? value : "");

const formatLargeNumber = (value: number): { text: string; short: string } => {
  const abs = Math.abs(value);
  if (abs >= 1e12)
    return { text: value.toLocaleString("en-US"), short: `${(value / 1e12).toFixed(2)}T` };
  if (abs >= 1e9)
    return { text: value.toLocaleString("en-US"), short: `${(value / 1e9).toFixed(2)}B` };
  if (abs >= 1e6)
    return { text: value.toLocaleString("en-US"), short: `${(value / 1e6).toFixed(2)}M` };
  if (abs >= 1e3)
    return { text: value.toLocaleString("en-US"), short: `${(value / 1e3).toFixed(2)}K` };
  return { text: String(value), short: String(value) };
};

const formatNumericValue = (fact: NumericFact): string => {
  if (!Number.isFinite(fact.value)) return "n/a";
  const unit = (fact.unit ?? "").trim();
  const currency = (fact.currency ?? "").trim();

  // Keep this conservative; formatting rules can evolve without breaking schema.
  if (currency.toUpperCase() === "USD" || unit.toUpperCase() === "USD") {
    const { short } = formatLargeNumber(fact.value);
    return `$${short}`;
  }
  if (unit.includes("%")) {
    return `${fact.value.toFixed(2)}%`;
  }
  const { text } = formatLargeNumber(fact.value);
  return `${text} ${unit}`.trim();
};

const PLACEHOLDER_LANGUAGE_RE =
  /\b(to appear|appendix pass|provided in appendix|full appendix|appendix available|appendix to follow|sources to follow|sources? pending|pending sources?|link(?:s)? to follow|csv\/queries|queries\/csv|pinned query|query ids?|ready for live|swap(?:ped)? for live|can be swapped|available (?:on|upon|by) request|(?:on|upon|by) request|provided (?:on|upon|by) request|placeholder(?:s)?|tbd|todo|coming soon|to be added|to be provided|to be attached|will (?:add|attach|provide)|tktk|tk|lorem ipsum)\b/gi;

const sanitizeRenderedText = (text: string): string =>
  text
    .replaceAll(/\{\{\s*N\d+\s*\}\}/g, "n/a")
    .replace(PLACEHOLDER_LANGUAGE_RE, "omitted")
    .replaceAll(/\s+/g, " ")
    .trim();

const renderTextWithNumeric = (
  text: string,
  numericById: Map<string, NumericFact>,
  used: Set<string>,
): string => {
  const rendered = text.replaceAll(/\{\{\s*(N\d+)\s*\}\}/g, (_match, idRaw: string) => {
    const id = String(idRaw || "").trim();
    const fact = numericById.get(id);
    if (!fact) return "n/a";
    used.add(id);
    return formatNumericValue(fact);
  });
  return sanitizeRenderedText(rendered);
};

const joinIds = (ids: string[]): string => ids.filter(Boolean).join(", ");

export function renderV2ReportMarkdown(params: { kind: ReportKindV2; report: unknown }): string {
  const report = params.report as ReportV2;
  const sources = Array.isArray(report?.sources) ? report.sources : [];
  const numericFacts = Array.isArray(report?.numeric_facts) ? report.numeric_facts : [];
  const numericById = new Map(numericFacts.map((fact) => [fact.id, fact]));
  const usedNumeric = new Set<string>();

  const subjectTitle =
    params.kind === "company"
      ? `Company Memo (v2): ${asString((report.subject as { ticker?: unknown } | undefined)?.ticker).toUpperCase() || "UNKNOWN"}`
      : `Theme Memo (v2): ${asString((report.subject as { theme_name?: unknown } | undefined)?.theme_name) || "UNKNOWN"}`;

  const lines: string[] = [];
  lines.push(`# ${subjectTitle}`);
  lines.push("");
  lines.push(`- Run: ${report.run_id}`);
  lines.push(`- Generated at: ${report.generated_at}`);
  lines.push("");

  // Sections
  for (const section of report.sections ?? []) {
    lines.push(`## ${section.title}`);
    if (asString(section.na_reason).trim()) {
      lines.push(`_N/A: ${asString(section.na_reason).trim()}_`);
      lines.push("");
      continue;
    }
    for (const block of section.blocks ?? []) {
      const tag = block.tag;
      const sourceIds = asArray(block.source_ids).map(asString).filter(Boolean);
      const numericRefs = asArray(block.numeric_refs).map(asString).filter(Boolean);
      const rendered = renderTextWithNumeric(block.text, numericById, usedNumeric);
      const cite = sourceIds.length ? ` [${joinIds(sourceIds)}]` : "";
      const nums = numericRefs.length ? ` ${numericRefs.map((id) => `[^${id}]`).join("")}` : "";
      lines.push(`- **${tag}:** ${rendered}${cite}${nums}`);
    }
    lines.push("");
  }

  // Exhibits
  lines.push("## Exhibits");
  lines.push("");
  for (const [idx, exhibit] of (report.exhibits ?? []).entries()) {
    const numericRefs = asArray(exhibit.numeric_refs).map(asString).filter(Boolean);
    numericRefs.forEach((id) => usedNumeric.add(id));
    lines.push(`### Exhibit ${idx + 1}: ${exhibit.title}`);
    if (asString(exhibit.id).trim()) {
      lines.push(`- Exhibit ID: ${asString(exhibit.id).trim()}`);
    }
    lines.push(`- Question: ${exhibit.question}`);
    for (const row of exhibit.data_summary ?? []) {
      const rendered = renderTextWithNumeric(row, numericById, usedNumeric);
      lines.push(`- ${rendered}`);
    }
    lines.push(
      `- ${sanitizeRenderedText(renderTextWithNumeric(exhibit.takeaway, numericById, usedNumeric))}`,
    );
    lines.push(`- Sources: [${joinIds(exhibit.source_ids ?? [])}]`);
    if (numericRefs.length) {
      lines.push(`- Numbers: ${numericRefs.map((id) => `[^${id}]`).join(" ")}`);
    }
    if (asString(exhibit.notes).trim()) {
      lines.push(`- Notes: ${sanitizeRenderedText(asString(exhibit.notes).trim())}`);
    }
    lines.push("");
  }

  // Appendix
  lines.push("## Appendix");
  lines.push("");
  lines.push("### Evidence Table");
  for (const row of report.appendix?.evidence_table ?? []) {
    lines.push(`- Claim: ${row.claim}`);
    lines.push(`  - Sources: [${joinIds(row.source_ids ?? [])}]`);
  }
  lines.push("");
  lines.push("### What's Missing");
  for (const item of report.appendix?.whats_missing ?? []) {
    lines.push(`- ${sanitizeRenderedText(item)}`);
  }
  lines.push("");

  // Sources
  lines.push("## Sources (timestamped)");
  lines.push("");
  for (const s of sources) {
    const host = (() => {
      try {
        return new URL(s.url).hostname.replace(/^www\./, "");
      } catch {
        return "";
      }
    })();
    lines.push(
      `- ${s.id}: tier=${s.reliability_tier} publisher=${s.publisher} date=${s.date_published} accessed_at=${s.accessed_at} host=${host} | ${s.url}`,
    );
  }
  lines.push("");

  // Numeric footnotes (only ones that were used).
  const usedNumericList = Array.from(usedNumeric).filter((id) => numericById.has(id));
  if (usedNumericList.length) {
    lines.push("## Numeric Footnotes");
    lines.push("");
    usedNumericList
      .sort((a, b) => a.localeCompare(b))
      .forEach((id) => {
        const fact = numericById.get(id);
        if (!fact) return;
        lines.push(
          `[^${id}]: ${id}=${formatNumericValue(fact)} unit=${fact.unit} period=${fact.period} source=${fact.source_id} accessed_at=${fact.accessed_at}${fact.notes ? ` notes=${fact.notes}` : ""}`,
        );
      });
    lines.push("");
  }

  return lines.join("\n").replaceAll("\r\n", "\n");
}
