import type { QualityIssue, ReportKindV2 } from "../types.js";

const asArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
const asString = (value: unknown): string => (typeof value === "string" ? value : "");
const asNumber = (value: unknown): number => (typeof value === "number" ? value : Number.NaN);
const asObject = (value: unknown): Record<string, unknown> =>
  value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const hasTag = (tags: unknown, tag: string): boolean =>
  asArray(tags)
    .map((t) => asString(t).trim())
    .includes(tag);

const countBy = <T>(items: T[], pred: (i: T) => boolean): number =>
  items.reduce((n, i) => n + (pred(i) ? 1 : 0), 0);

/**
 * Lane 1 (Evidence Depth): institutional memos fail closed if primary evidence coverage is missing.
 *
 * We intentionally make this conservative. If the evidence isn't in the library, we don't ship the memo.
 */
export function validateEvidenceCoverage(params: {
  kind: ReportKindV2;
  report: unknown;
}): QualityIssue[] {
  const issues: QualityIssue[] = [];
  const root = asObject(params.report);

  const sources = asArray(root.sources).map(asObject);

  const tier1Count = countBy(sources, (s) => asNumber(s.reliability_tier) === 1);
  const tier2Count = countBy(sources, (s) => asNumber(s.reliability_tier) === 2);
  const tier3Count = countBy(sources, (s) => asNumber(s.reliability_tier) === 3);
  const tier4Count = countBy(sources, (s) => asNumber(s.reliability_tier) === 4);
  const nonInternalCount = countBy(sources, (s) => !hasTag(s.tags, "internal:spec"));

  const tier12 = tier1Count + tier2Count;

  // Universal minimums (company + theme).
  if (tier1Count < 1) {
    issues.push({
      severity: "error",
      code: "evidence_missing_tier1",
      path: "/sources",
      message: `Evidence library is missing Tier 1 sources (tier1=${tier1Count}).`,
      fix: "Ingest SEC filings / audited reports / official releases into PASS 1 before compiling.",
    });
  }
  if (tier12 < 2) {
    issues.push({
      severity: "error",
      code: "evidence_missing_tier12_minimum",
      path: "/sources",
      message: `Evidence library has insufficient Tier 1/2 sources (tier1_2=${tier12}, require>=2).`,
      fix: "Collect at least two Tier 1/2 sources (e.g., filings + official dataset) before compiling.",
    });
  }
  if (nonInternalCount < 1) {
    issues.push({
      severity: "error",
      code: "evidence_only_internal",
      path: "/sources",
      message: "Evidence library contains only internal sources.",
      fix: "Add at least one external primary source with URL + accessed_at.",
    });
  }

  // Company-specific requirements.
  if (params.kind === "company") {
    const ticker = asString(asObject(root.subject).ticker).trim().toUpperCase();
    if (ticker) {
      const companyTaggedTier12 = countBy(
        sources,
        (s) =>
          (asNumber(s.reliability_tier) === 1 || asNumber(s.reliability_tier) === 2) &&
          hasTag(s.tags, `company:${ticker}`),
      );
      const secTagged = countBy(
        sources,
        (s) =>
          asNumber(s.reliability_tier) === 1 &&
          hasTag(s.tags, `company:${ticker}`) &&
          (hasTag(s.tags, "source:sec") || hasTag(s.tags, "type:filing")),
      );

      if (companyTaggedTier12 < 1) {
        issues.push({
          severity: "error",
          code: "evidence_missing_company_tier12",
          path: "/sources",
          message: `Missing Tier 1/2 evidence tagged to company:${ticker} (found=${companyTaggedTier12}).`,
          fix: "Ingest company filings / audited statements / official releases for the target ticker.",
        });
      }
      if (secTagged < 1) {
        issues.push({
          severity: "error",
          code: "evidence_missing_company_sec",
          path: "/sources",
          message: `Missing SEC filing evidence for ${ticker} (need at least one Tier 1 SEC source).`,
          fix: "Ingest recent SEC filings and include them as Tier 1 evidence in the library.",
        });
      }
    }
  }

  // Theme-specific requirements.
  if (params.kind === "theme") {
    const universe = asArray(asObject(root.subject).universe)
      .map(asString)
      .map((t) => t.trim().toUpperCase())
      .filter(Boolean);

    if (universe.length > 0) {
      const missing: string[] = [];
      for (const ticker of universe) {
        const ok = countBy(
          sources,
          (s) =>
            (asNumber(s.reliability_tier) === 1 || asNumber(s.reliability_tier) === 2) &&
            hasTag(s.tags, `company:${ticker}`),
        );
        if (ok < 1) missing.push(ticker);
      }
      if (missing.length > 0) {
        issues.push({
          severity: "error",
          code: "evidence_missing_universe_coverage",
          path: "/subject/universe",
          message: `Theme universe missing Tier 1/2 evidence coverage for: ${missing.join(", ")}.`,
          fix: "Ingest at least one Tier 1/2 source per universe ticker, or remove tickers until covered.",
        });
      }
    } else {
      // If no universe is provided, still require at least one Tier 1/2 external source.
      if (tier12 < 1) {
        issues.push({
          severity: "error",
          code: "evidence_missing_theme_tier12",
          path: "/sources",
          message: "Theme report has no universe and lacks Tier 1/2 evidence.",
          fix: "Add official sources (Tier 1/2) to support the theme definition and value chain.",
        });
      }
    }
  }

  // Useful warnings (do not fail closed).
  if (tier3Count < 1) {
    issues.push({
      severity: "warn",
      code: "evidence_missing_transcript_or_journalism",
      path: "/sources",
      message:
        "No Tier 3 sources found (e.g., transcripts / high-quality journalism). This can reduce variant-perception depth.",
      fix: "Add at least one transcript or high-quality third-party source and tag it appropriately.",
    });
  }
  if (tier4Count > 0 && tier12 < 2) {
    issues.push({
      severity: "warn",
      code: "evidence_tier4_dominant",
      path: "/sources",
      message:
        "Tier 4 sources present while Tier 1/2 coverage is thin. Treat Tier 4 as supplemental only.",
      fix: "Prioritize primary sources first; demote or remove Tier 4 items that aren't adding unique signal.",
    });
  }

  return issues;
}
