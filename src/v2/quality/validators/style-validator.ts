import type { QualityIssue } from "../types.js";

const asArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const asString = (value: unknown): string => (typeof value === "string" ? value : "");
const asObject = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const BANNED_PHRASES = [
  "best-in-class",
  "world class",
  "world-class",
  "game changer",
  "game-changer",
  "paradigm shift",
  "unprecedented",
  "no brainer",
  "obviously",
  "clearly",
  "massive opportunity",
  "huge opportunity",
  "can't miss",
];

const PLACEHOLDER_PHRASES_RE =
  /\b(to appear|appendix pass|provided in appendix|full appendix|appendix available|appendix to follow|sources to follow|sources? pending|pending sources?|link(?:s)? to follow|csv\/queries|queries\/csv|pinned query|query ids?|ready for live|swap(?:ped)? for live|can be swapped|available (?:on|upon|by) request|(?:on|upon|by) request|provided (?:on|upon|by) request|placeholder(?:s)?|tbd|todo|coming soon|to be added|to be provided|to be attached|will (?:add|attach|provide)|tktk|tk|lorem ipsum)\b/i;

const collectText = (report: unknown): Array<{ path: string; text: string }> => {
  const out: Array<{ path: string; text: string }> = [];
  const root = asObject(report);

  const push = (path: string, value: unknown) => {
    const text = asString(value);
    if (text.trim()) out.push({ path, text });
  };

  push("/subject/ticker", asObject(root.subject).ticker);
  push("/subject/theme_name", asObject(root.subject).theme_name);

  asArray(root.sections)
    .map(asObject)
    .forEach((section, si) => {
      push(`/sections/${si}/title`, section.title);
      push(`/sections/${si}/na_reason`, section.na_reason);
      asArray(section.blocks)
        .map(asObject)
        .forEach((block, bi) => {
          push(`/sections/${si}/blocks/${bi}/text`, block.text);
        });
    });

  asArray(root.exhibits)
    .map(asObject)
    .forEach((ex, xi) => {
      push(`/exhibits/${xi}/title`, ex.title);
      push(`/exhibits/${xi}/question`, ex.question);
      push(`/exhibits/${xi}/takeaway`, ex.takeaway);
      asArray(ex.data_summary)
        .map(asString)
        .forEach((line, li) => push(`/exhibits/${xi}/data_summary/${li}`, line));
    });

  const appendix = asObject(root.appendix);
  asArray(appendix.evidence_table)
    .map(asObject)
    .forEach((row, ri) => {
      push(`/appendix/evidence_table/${ri}/claim`, row.claim);
    });

  return out;
};

export function validateStyle(report: unknown): QualityIssue[] {
  const issues: QualityIssue[] = [];
  const texts = collectText(report);

  // Ban placeholder language ("we'll attach later").
  for (const { path, text } of texts) {
    if (PLACEHOLDER_PHRASES_RE.test(text)) {
      issues.push({
        severity: "error",
        code: "style_placeholder_language",
        path,
        message:
          "Placeholder language detected (e.g., 'on request', 'to appear', 'appendix pass').",
        fix: "Replace placeholders with actual exhibits/sources, or mark the claim as unknown and add it to whats_missing.",
      });
    }
  }

  // Ban fluff.
  for (const { path, text } of texts) {
    const lowered = text.toLowerCase();
    for (const phrase of BANNED_PHRASES) {
      if (lowered.includes(phrase)) {
        issues.push({
          severity: "error",
          code: "style_banned_phrase",
          path,
          message: `Banned fluff phrase detected: ${JSON.stringify(phrase)}.`,
          fix: "Replace with specific, falsifiable language tied to evidence.",
        });
      }
    }
  }

  // Require explicit FACT/INTERPRETATION/ASSUMPTION tagging at least once per section
  // (skipped when section is explicitly N/A).
  const root = asObject(report);
  const sections = asArray(root.sections).map(asObject);
  sections.forEach((section, si) => {
    const naReason = asString(section.na_reason).trim();
    if (naReason) return;
    const tags = new Set(
      asArray(section.blocks)
        .map(asObject)
        .map((block) => asString(block.tag))
        .filter(Boolean),
    );
    const required = ["FACT", "INTERPRETATION", "ASSUMPTION"] as const;
    required.forEach((tag) => {
      if (!tags.has(tag)) {
        issues.push({
          severity: "error",
          code: "style_missing_tag",
          path: `/sections/${si}`,
          message: `Section is missing at least one ${tag} block (must label facts vs interpretation vs assumptions).`,
          fix: `Add a block with tag=${tag} (and citations if FACT).`,
        });
      }
    });
  });

  return issues;
}
