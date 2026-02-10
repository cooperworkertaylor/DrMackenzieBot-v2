export type PdfDiagnosticsMetrics = {
  markdownHeadingTokens: number;
  markdownFenceTokens: number;
  urlCount: number;
  citationKeyCount: number;
  exhibitTokenCount: number;
  sourcesHeadingPresent: boolean;
  dashMojibakeDateCount: number;
  dashMojibakeStandaloneNCount: number;
  placeholderTokenCount: number;
  extractedChars: number;
};

export type PdfDiagnosticsThresholds = {
  minUrls: number;
  minCitations: number;
  minExhibits: number;
  maxMarkdownHeadingTokens: number;
  maxMarkdownFenceTokens: number;
  maxDashMojibakeDateCount: number;
  maxDashMojibakeStandaloneNCount: number;
  maxPlaceholderTokenCount: number;
  requireSourcesHeading: boolean;
};

export const DEFAULT_PDF_DIAGNOSTICS_THRESHOLDS_STRICT: PdfDiagnosticsThresholds = {
  minUrls: 1,
  minCitations: 1,
  minExhibits: 1,
  maxMarkdownHeadingTokens: 0,
  maxMarkdownFenceTokens: 0,
  maxDashMojibakeDateCount: 0,
  // A standalone "n" token is almost always a broken hyphen in these PDFs.
  maxDashMojibakeStandaloneNCount: 0,
  // Shipping "appendix pass" / "to appear" placeholders is a hard failure.
  maxPlaceholderTokenCount: 0,
  requireSourcesHeading: true,
};

export type PdfDiagnosticsResult = {
  metrics: PdfDiagnosticsMetrics;
  errors: string[];
};

// Shipping reports with "we'll attach this later" language is a hard failure.
// Keep this list intentionally biased towards false positives over false negatives.
const PLACEHOLDER_RE =
  /\b(to appear|appendix pass|provided in appendix|full appendix|appendix available|appendix to follow|sources to follow|sources? pending|pending sources?|link(?:s)? to follow|csv\/queries|queries\/csv|pinned query|query ids?|ready for live|swap(?:ped)? for live|can be swapped|available (?:on|upon|by) request|(?:on|upon|by) request|provided (?:on|upon|by) request|placeholder|tbd|todo|coming soon|to be added|to be provided|to be attached|will (?:add|attach|provide)|tktk|tk|lorem ipsum)\b/gi;

export function computePdfDiagnostics(text: string): PdfDiagnosticsMetrics {
  const citationKeysBracketed = (text.match(/\[(?:S|C)\d+\]/gi) ?? []).length;
  const citationKeysPrefixed = (text.match(/\bC\d+:/gi) ?? []).length;
  const citationKeyCount = citationKeysBracketed + citationKeysPrefixed;

  return {
    markdownHeadingTokens: (text.match(/###\s+/g) ?? []).length,
    markdownFenceTokens: (text.match(/```/g) ?? []).length,
    urlCount: (text.match(/https?:\/\/\S+/gi) ?? []).length,
    citationKeyCount,
    // Accept "Exhibit 1" and "Exhibit L2-1" style ids.
    exhibitTokenCount: (text.match(/\bExhibit\s+[A-Za-z0-9][A-Za-z0-9_-]*\b/gi) ?? []).length,
    sourcesHeadingPresent: /Source List\b|Sources\b/i.test(text),
    dashMojibakeDateCount: (text.match(/\b\d{4}\s+n\s+\d{2}\s+n\s+\d{2}\b/g) ?? []).length,
    dashMojibakeStandaloneNCount: (text.match(/\bn\b/g) ?? []).length,
    placeholderTokenCount: (text.match(PLACEHOLDER_RE) ?? []).length,
    extractedChars: text.length,
  };
}

export function validatePdfDiagnosticsStrict(params: {
  metrics: PdfDiagnosticsMetrics;
  thresholds?: Partial<PdfDiagnosticsThresholds>;
}): string[] {
  const thresholds: PdfDiagnosticsThresholds = {
    ...DEFAULT_PDF_DIAGNOSTICS_THRESHOLDS_STRICT,
    ...(params.thresholds ?? {}),
  };
  const m = params.metrics;
  const errors: string[] = [];

  if (m.markdownHeadingTokens > thresholds.maxMarkdownHeadingTokens) {
    errors.push(
      `raw_markdown_headings: markdownHeadingTokens=${m.markdownHeadingTokens} (max=${thresholds.maxMarkdownHeadingTokens})`,
    );
  }
  if (m.markdownFenceTokens > thresholds.maxMarkdownFenceTokens) {
    errors.push(
      `raw_markdown_fences: markdownFenceTokens=${m.markdownFenceTokens} (max=${thresholds.maxMarkdownFenceTokens})`,
    );
  }
  if (thresholds.requireSourcesHeading && !m.sourcesHeadingPresent) {
    errors.push("missing_sources_heading: Sources/Source List section not detected");
  }
  if (m.urlCount < thresholds.minUrls) {
    errors.push(`missing_urls: urlCount=${m.urlCount} (min=${thresholds.minUrls})`);
  }
  if (m.citationKeyCount < thresholds.minCitations) {
    errors.push(
      `missing_citation_keys: citationKeyCount=${m.citationKeyCount} (min=${thresholds.minCitations})`,
    );
  }
  if (m.exhibitTokenCount < thresholds.minExhibits) {
    errors.push(
      `missing_exhibits: exhibitTokenCount=${m.exhibitTokenCount} (min=${thresholds.minExhibits})`,
    );
  }
  if (m.dashMojibakeDateCount > thresholds.maxDashMojibakeDateCount) {
    errors.push(
      `dash_mojibake_dates: dashMojibakeDateCount=${m.dashMojibakeDateCount} (max=${thresholds.maxDashMojibakeDateCount})`,
    );
  }
  if (m.dashMojibakeStandaloneNCount > thresholds.maxDashMojibakeStandaloneNCount) {
    errors.push(
      `dash_mojibake_tokens: dashMojibakeStandaloneNCount=${m.dashMojibakeStandaloneNCount} (max=${thresholds.maxDashMojibakeStandaloneNCount})`,
    );
  }
  if (m.placeholderTokenCount > thresholds.maxPlaceholderTokenCount) {
    errors.push(
      `placeholder_language: placeholderTokenCount=${m.placeholderTokenCount} (max=${thresholds.maxPlaceholderTokenCount})`,
    );
  }

  return errors;
}

export async function extractPdfTextFromBuffer(params: {
  buffer: Uint8Array;
  maxPages: number;
}): Promise<{ pages: number; scannedPages: number; text: string }> {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const pdf = await getDocument({
    data: params.buffer,
    disableWorker: true,
  }).promise;
  const scannedPages = Math.min(pdf.numPages, Math.max(1, params.maxPages));

  const parts: string[] = [];
  for (let pageNum = 1; pageNum <= scannedPages; pageNum += 1) {
    const page = await pdf.getPage(pageNum);
    const textContent = await page.getTextContent();
    const pageText = textContent.items
      .map((item: unknown) =>
        typeof item === "object" && item && "str" in item
          ? String((item as { str?: unknown }).str ?? "")
          : "",
      )
      .filter(Boolean)
      .join(" ");
    if (pageText) parts.push(pageText);
  }

  return {
    pages: pdf.numPages,
    scannedPages,
    text: parts.join("\n"),
  };
}

export async function diagnosePdfBuffer(params: {
  buffer: Uint8Array;
  maxPages: number;
  strict?: boolean;
  thresholds?: Partial<PdfDiagnosticsThresholds>;
}): Promise<PdfDiagnosticsResult> {
  const { text } = await extractPdfTextFromBuffer({
    buffer: params.buffer,
    maxPages: params.maxPages,
  });
  const metrics = computePdfDiagnostics(text);
  const errors = params.strict
    ? validatePdfDiagnosticsStrict({ metrics, thresholds: params.thresholds })
    : [];
  return { metrics, errors };
}
