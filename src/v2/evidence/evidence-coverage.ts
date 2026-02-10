import type { QualityIssue, ReportKindV2 } from "../quality/types.js";
import type { EvidenceItem } from "./evidence-store.js";

const hasTag = (tags: string[], tag: string): boolean => tags.includes(tag);

const countBy = <T>(items: T[], pred: (i: T) => boolean): number =>
  items.reduce((n, i) => n + (pred(i) ? 1 : 0), 0);

export function evaluateEvidenceCoverage(params: {
  kind: ReportKindV2;
  sources: EvidenceItem[];
  subject: { ticker?: string; universe?: string[] };
}): QualityIssue[] {
  const sources = params.sources ?? [];
  const tier1Count = countBy(sources, (s) => s.reliability_tier === 1);
  const tier2Count = countBy(sources, (s) => s.reliability_tier === 2);
  const tier3Count = countBy(sources, (s) => s.reliability_tier === 3);
  const tier4Count = countBy(sources, (s) => s.reliability_tier === 4);
  const nonInternalCount = countBy(sources, (s) => !hasTag(s.tags, "internal:spec"));

  const tier12 = tier1Count + tier2Count;
  const issues: QualityIssue[] = [];

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

  if (params.kind === "company") {
    const ticker = (params.subject.ticker ?? "").trim().toUpperCase();
    if (ticker) {
      const companyTaggedTier12 = countBy(
        sources,
        (s) =>
          (s.reliability_tier === 1 || s.reliability_tier === 2) &&
          hasTag(s.tags, `company:${ticker}`),
      );
      const secTagged = countBy(
        sources,
        (s) =>
          s.reliability_tier === 1 &&
          hasTag(s.tags, `company:${ticker}`) &&
          (hasTag(s.tags, "source:sec") || hasTag(s.tags, "type:filing")),
      );
      const secTaggedWithRawText = countBy(
        sources,
        (s) =>
          s.reliability_tier === 1 &&
          hasTag(s.tags, `company:${ticker}`) &&
          (hasTag(s.tags, "source:sec") || hasTag(s.tags, "type:filing")) &&
          Boolean((s.raw_text_ref ?? "").trim()),
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
      if (secTagged >= 1 && secTaggedWithRawText < 1) {
        issues.push({
          severity: "error",
          code: "evidence_missing_company_sec_raw_text",
          path: "/sources",
          message: `SEC filing sources exist for ${ticker}, but none have raw_text_ref available (needed for extraction).`,
          fix: "Ensure filing text is ingested/stored (filings.text) and exported into runs/<run_id>/sources via PASS 1.",
        });
      }
    }
  }

  if (params.kind === "theme") {
    const universe = (params.subject.universe ?? [])
      .map((t) => t.trim().toUpperCase())
      .filter(Boolean);
    if (universe.length > 0) {
      const missing: string[] = [];
      for (const ticker of universe) {
        const ok = countBy(
          sources,
          (s) =>
            (s.reliability_tier === 1 || s.reliability_tier === 2) &&
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

      // Warn (for now) when we have Tier 1 SEC evidence but no raw text to extract.
      const missingRaw: string[] = [];
      for (const ticker of universe) {
        const secTagged = countBy(
          sources,
          (s) =>
            s.reliability_tier === 1 &&
            hasTag(s.tags, `company:${ticker}`) &&
            (hasTag(s.tags, "source:sec") || hasTag(s.tags, "type:filing")),
        );
        if (secTagged < 1) continue;
        const secWithText = countBy(
          sources,
          (s) =>
            s.reliability_tier === 1 &&
            hasTag(s.tags, `company:${ticker}`) &&
            (hasTag(s.tags, "source:sec") || hasTag(s.tags, "type:filing")) &&
            Boolean((s.raw_text_ref ?? "").trim()),
        );
        if (secWithText < 1) missingRaw.push(ticker);
      }
      if (missingRaw.length > 0) {
        issues.push({
          severity: "warn",
          code: "evidence_missing_universe_sec_raw_text",
          path: "/sources",
          message: `Universe has SEC sources but missing raw_text_ref for: ${missingRaw.join(", ")}.`,
          fix: "Ensure filings.text is stored and exported into run sources for extraction-driven analyzers.",
        });
      }
    } else if (tier12 < 1) {
      issues.push({
        severity: "error",
        code: "evidence_missing_theme_tier12",
        path: "/sources",
        message: "Theme report has no universe and lacks Tier 1/2 evidence.",
        fix: "Add official sources (Tier 1/2) to support the theme definition and value chain.",
      });
    }
  }

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
