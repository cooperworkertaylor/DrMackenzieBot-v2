import type { QualityIssue } from "../types.js";

const asArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const asString = (value: unknown): string => (typeof value === "string" ? value : "");
const asObject = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const hasDigits = (text: string): boolean => /\d/.test(text);

// Allow ISO dates in exhibits without requiring numeric provenance. Dates are not investment
// numbers and forcing them into numeric_facts would be counterproductive. Keep this narrow.
const stripIsoDates = (text: string): string =>
  text
    // YYYY-MM-DD
    .replaceAll(/\b\d{4}-\d{2}-\d{2}\b/g, "DATE")
    // YYYY/MM/DD
    .replaceAll(/\b\d{4}\/\d{2}\/\d{2}\b/g, "DATE");

// v2 convention: numbers should not appear directly in prose blocks. Instead, embed a placeholder
// token like "{{N12}}" and include "N12" in numeric_refs. The renderer will substitute/footnote.
const extractNumericPlaceholders = (text: string): string[] => {
  const out: string[] = [];
  const re = /\{\{\s*(N\d+)\s*\}\}/g;
  let match: RegExpExecArray | null = null;
  while ((match = re.exec(text))) {
    const id = match[1];
    if (id) out.push(id);
  }
  return out;
};

const stripNumericPlaceholders = (text: string): string =>
  text.replaceAll(/\{\{\s*N\d+\s*\}\}/g, "{{N}}");

export function validateNumericProvenance(report: unknown): QualityIssue[] {
  const issues: QualityIssue[] = [];
  const root = asObject(report);

  const sources = asArray(root.sources)
    .map((item) => asObject(item).id)
    .map(asString)
    .filter(Boolean);
  const sourceSet = new Set(sources);

  const numericFacts = asArray(root.numeric_facts).map((item) => asObject(item));
  const numericIds = numericFacts.map((fact) => asString(fact.id)).filter(Boolean);
  const numericIdSet = new Set(numericIds);

  // Uniqueness + referential integrity for numeric facts.
  const seen = new Set<string>();
  numericIds.forEach((id, idx) => {
    if (seen.has(id)) {
      issues.push({
        severity: "error",
        code: "numeric_duplicate_id",
        path: `/numeric_facts/${idx}/id`,
        message: `Duplicate numeric fact id ${JSON.stringify(id)}.`,
        fix: "Ensure every numeric fact id is unique (N1, N2, ...).",
      });
    }
    seen.add(id);
  });
  numericFacts.forEach((fact, idx) => {
    const id = asString(fact.id);
    const sourceId = asString(fact.source_id);
    const accessedAt = asString(fact.accessed_at);
    if (id && !/^N\d+$/.test(id)) {
      issues.push({
        severity: "error",
        code: "numeric_bad_id",
        path: `/numeric_facts/${idx}/id`,
        message: `Numeric fact id must be N#, got ${JSON.stringify(id)}.`,
        fix: "Rename to N1/N2/... and update all numeric_refs.",
      });
    }
    if (sourceId && !sourceSet.has(sourceId)) {
      issues.push({
        severity: "error",
        code: "numeric_unknown_source",
        path: `/numeric_facts/${idx}/source_id`,
        message: `Numeric fact references missing source ${JSON.stringify(sourceId)}.`,
        fix: "Add the source to sources[] or update numeric_facts[].source_id.",
      });
    }
    if (!accessedAt) {
      issues.push({
        severity: "error",
        code: "numeric_missing_accessed_at",
        path: `/numeric_facts/${idx}/accessed_at`,
        message: "Numeric fact is missing accessed_at timestamp.",
        fix: "Set accessed_at to an ISO-8601 timestamp (UTC preferred).",
      });
    }
  });

  // Enforce: no raw digits in prose blocks outside of numeric placeholders like {{N1}}.
  const sections = asArray(root.sections).map((item) => asObject(item));
  sections.forEach((section, sectionIndex) => {
    const blocks = asArray(section.blocks).map((item) => asObject(item));
    blocks.forEach((block, blockIndex) => {
      const text = asString(block.text);
      const placeholderIds = extractNumericPlaceholders(text);
      const sanitized = stripNumericPlaceholders(text);
      if (hasDigits(sanitized)) {
        issues.push({
          severity: "error",
          code: "numeric_in_prose",
          path: `/sections/${sectionIndex}/blocks/${blockIndex}/text`,
          message:
            "Raw digits are not allowed in prose blocks in v2. Use numeric placeholders like {{N1}} and back them with numeric_facts + numeric_refs.",
          fix: "Move the number into numeric_facts (with value/unit/period/source/accessed_at). Replace the digits in text with {{N#}} and include N# in numeric_refs.",
        });
      }
      const numericRefs = asArray(block.numeric_refs).map(asString).filter(Boolean);
      placeholderIds.forEach((id) => {
        if (!numericRefs.includes(id)) {
          issues.push({
            severity: "error",
            code: "numeric_placeholder_missing_ref",
            path: `/sections/${sectionIndex}/blocks/${blockIndex}/numeric_refs`,
            message: `Prose contains placeholder {{${id}}} but numeric_refs does not include ${id}.`,
            fix: `Add ${id} to numeric_refs so the renderer can substitute and footnote it.`,
          });
        }
      });
      numericRefs.forEach((ref) => {
        if (!numericIdSet.has(ref)) {
          issues.push({
            severity: "error",
            code: "numeric_ref_missing",
            path: `/sections/${sectionIndex}/blocks/${blockIndex}/numeric_refs`,
            message: `numeric_ref ${JSON.stringify(ref)} is not present in numeric_facts[].`,
            fix: "Add the referenced numeric fact or remove the ref.",
          });
        }
      });
    });
  });

  // Exhibits may contain digits, but only if numeric_refs are present.
  const exhibits = asArray(root.exhibits).map((item) => asObject(item));
  exhibits.forEach((exhibit, exhibitIndex) => {
    const numericRefs = asArray(exhibit.numeric_refs).map(asString).filter(Boolean);
    const summary = asArray(exhibit.data_summary).map(asString).filter(Boolean);
    const takeaway = asString(exhibit.takeaway);
    const containsDigits =
      summary.some((line) => hasDigits(stripIsoDates(line))) || hasDigits(stripIsoDates(takeaway));
    if (containsDigits && numericRefs.length === 0) {
      issues.push({
        severity: "error",
        code: "exhibit_numeric_missing_provenance",
        path: `/exhibits/${exhibitIndex}`,
        message:
          "Exhibit contains digits but has no numeric_refs. Every numeric value must be backed by numeric_facts.",
        fix: 'Add numeric_refs (e.g. ["N1"]) and ensure numeric_facts contains the referenced metrics with provenance.',
      });
    }
    numericRefs.forEach((ref) => {
      if (!numericIdSet.has(ref)) {
        issues.push({
          severity: "error",
          code: "exhibit_numeric_ref_missing",
          path: `/exhibits/${exhibitIndex}/numeric_refs`,
          message: `Exhibit numeric_ref ${JSON.stringify(ref)} is missing from numeric_facts[].`,
          fix: "Add the referenced numeric fact or remove the ref.",
        });
      }
    });
  });

  // Warn when numeric facts exist but are unused.
  const referenced = new Set<string>();
  sections.forEach((section) => {
    asArray(section.blocks)
      .map(asObject)
      .forEach((block) => {
        asArray(block.numeric_refs)
          .map(asString)
          .filter(Boolean)
          .forEach((id) => referenced.add(id));
      });
  });
  exhibits.forEach((exhibit) => {
    asArray(exhibit.numeric_refs)
      .map(asString)
      .filter(Boolean)
      .forEach((id) => referenced.add(id));
  });
  numericIds.forEach((id, idx) => {
    if (id && !referenced.has(id)) {
      issues.push({
        severity: "warn",
        code: "numeric_unused",
        path: `/numeric_facts/${idx}/id`,
        message: `Numeric fact ${id} is never referenced (numeric_refs).`,
        fix: "Either reference it from a section/exhibit via numeric_refs or remove it.",
      });
    }
  });

  return issues;
}
