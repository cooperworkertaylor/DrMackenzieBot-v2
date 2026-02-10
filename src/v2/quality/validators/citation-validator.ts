import type { QualityIssue } from "../types.js";

const asArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const asString = (value: unknown): string => (typeof value === "string" ? value : "");
const asObject = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

export function validateCitationCoverage(report: unknown): QualityIssue[] {
  const issues: QualityIssue[] = [];
  const root = asObject(report);
  const sources = asArray(root.sources)
    .map((item) => asObject(item).id)
    .map((id) => asString(id))
    .filter(Boolean);
  const sourceSet = new Set(sources);

  const sections = asArray(root.sections).map((item) => asObject(item));
  sections.forEach((section, sectionIndex) => {
    const blocks = asArray(section.blocks).map((item) => asObject(item));
    blocks.forEach((block, blockIndex) => {
      const tag = asString(block.tag);
      const sourceIds = asArray(block.source_ids).map(asString).filter(Boolean);
      if (tag === "FACT") {
        if (sourceIds.length === 0) {
          issues.push({
            severity: "error",
            code: "citation_missing",
            path: `/sections/${sectionIndex}/blocks/${blockIndex}`,
            message: "FACT block is missing source_ids (must cite at least one source).",
            fix: 'Attach one or more source_ids like ["S1"]. If unknown, downgrade to ASSUMPTION and add what would verify it.',
          });
          return;
        }
      }
      sourceIds.forEach((id) => {
        if (!sourceSet.has(id)) {
          issues.push({
            severity: "error",
            code: "citation_unknown_source",
            path: `/sections/${sectionIndex}/blocks/${blockIndex}/source_ids`,
            message: `source_id ${JSON.stringify(id)} does not exist in top-level sources[].`,
            fix: "Add the missing source to sources[] or replace the citation with a valid S# id.",
          });
        }
      });
    });
  });

  const exhibits = asArray(root.exhibits).map((item) => asObject(item));
  exhibits.forEach((exhibit, exhibitIndex) => {
    const sourceIds = asArray(exhibit.source_ids).map(asString).filter(Boolean);
    sourceIds.forEach((id) => {
      if (!sourceSet.has(id)) {
        issues.push({
          severity: "error",
          code: "exhibit_unknown_source",
          path: `/exhibits/${exhibitIndex}/source_ids`,
          message: `Exhibit cites ${JSON.stringify(id)} but it is missing from sources[].`,
          fix: "Add the missing source to sources[] or update the exhibit source_ids.",
        });
      }
    });
  });

  const appendix = asObject(root.appendix);
  const evidenceTable = asArray(appendix.evidence_table).map((item) => asObject(item));
  evidenceTable.forEach((row, rowIndex) => {
    const sourceIds = asArray(row.source_ids).map(asString).filter(Boolean);
    sourceIds.forEach((id) => {
      if (!sourceSet.has(id)) {
        issues.push({
          severity: "error",
          code: "appendix_unknown_source",
          path: `/appendix/evidence_table/${rowIndex}/source_ids`,
          message: `Appendix cites ${JSON.stringify(id)} but it is missing from sources[].`,
          fix: "Add the missing source to sources[] or update the appendix source_ids.",
        });
      }
    });
  });

  return issues;
}
