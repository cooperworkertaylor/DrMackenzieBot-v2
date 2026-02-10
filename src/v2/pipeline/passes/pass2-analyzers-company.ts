import fs from "node:fs/promises";
import type { EvidenceItem } from "../../evidence/evidence-store.js";

export type CompanyAnalyzerOutputV2 = {
  version: 1;
  generated_at: string;
  ticker: string;
  notes: string[];
  extracts: {
    filings: Array<{
      source_id: string;
      title: string;
      url: string;
      date_published: string;
      extracted: {
        business_keywords: string[];
        risk_keywords: string[];
      };
    }>;
    transcripts: Array<{
      source_id: string;
      title: string;
      url: string;
      date_published: string;
      extracted: {
        keywords: string[];
      };
    }>;
  };
  numeric_facts: Array<{
    id: string;
    value: number;
    unit: string;
    period: string;
    currency?: string;
    source_id: string;
    accessed_at: string;
    notes?: string;
  }>;
  kpi_table: Array<{
    metric: string;
    numeric_id: string;
    as_of: string;
    form?: string;
  }>;
};

type CsvRow = Record<string, string>;

const nowIso = (): string => new Date().toISOString();

const STOPWORDS = new Set(
  [
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "its",
    "may",
    "not",
    "of",
    "on",
    "or",
    "our",
    "that",
    "the",
    "their",
    "these",
    "they",
    "this",
    "to",
    "was",
    "we",
    "were",
    "will",
    "with",
  ].map((w) => w.toLowerCase()),
);

const topKeywords = (text: string, max: number): string[] => {
  const counts = new Map<string, number>();
  const cleaned = text
    .toLowerCase()
    .replaceAll(/[^a-z\\s]+/g, " ")
    .replaceAll(/\\s+/g, " ")
    .trim();
  for (const token of cleaned.split(" ")) {
    const t = token.trim();
    if (!t) continue;
    if (t.length < 4) continue;
    if (STOPWORDS.has(t)) continue;
    counts.set(t, (counts.get(t) ?? 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, max)
    .map(([t]) => t);
};

const extractSection = (text: string, startRe: RegExp, endRe: RegExp, maxChars: number): string => {
  const start = startRe.exec(text);
  if (!start || start.index < 0) return "";
  const slice = text.slice(start.index);
  const end = endRe.exec(slice);
  const raw = end && end.index > 0 ? slice.slice(0, end.index) : slice;
  return raw.slice(0, Math.max(0, maxChars)).replaceAll(/\\s+/g, " ").trim();
};

const parseCsvLine = (line: string): string[] => {
  const out: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i] ?? "";
    if (ch === '"') {
      const next = line[i + 1] ?? "";
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(current);
      current = "";
      continue;
    }
    current += ch;
  }
  out.push(current);
  return out;
};

const parseCsv = (csv: string): CsvRow[] => {
  const lines = csv
    .split(/\r?\n/)
    .map((l) => l.trimEnd())
    .filter((l) => l.length > 0);
  if (lines.length < 2) return [];
  const header = parseCsvLine(lines[0] ?? "").map((h) => h.trim());
  const rows: CsvRow[] = [];
  for (const line of lines.slice(1)) {
    const fields = parseCsvLine(line);
    const row: CsvRow = {};
    for (let i = 0; i < header.length; i += 1) {
      row[header[i] ?? String(i)] = fields[i] ?? "";
    }
    rows.push(row);
  }
  return rows;
};

const pickSecFixture = (evidence: EvidenceItem[]): EvidenceItem | undefined =>
  evidence.find(
    (item) => (item.publisher ?? "").toLowerCase() === "sec" && Boolean(item.raw_text_ref),
  );

const toFinite = (value: string): number | undefined => {
  const n = Number.parseFloat((value ?? "").trim());
  return Number.isFinite(n) ? n : undefined;
};

export async function pass2CompanyAnalyzersV2(params: {
  ticker: string;
  evidence: EvidenceItem[];
}): Promise<CompanyAnalyzerOutputV2> {
  const ticker = params.ticker.trim().toUpperCase();
  const notes: string[] = [];
  const numeric_facts: CompanyAnalyzerOutputV2["numeric_facts"] = [];
  const kpi_table: CompanyAnalyzerOutputV2["kpi_table"] = [];
  const extracts: CompanyAnalyzerOutputV2["extracts"] = { filings: [], transcripts: [] };

  // Qualitative extraction (no prose): derive keyword-level signal from filings/transcripts.
  const filings = params.evidence
    .filter(
      (e) =>
        e.reliability_tier === 1 &&
        e.tags.includes(`company:${ticker}`) &&
        (e.tags.includes("type:filing") || e.tags.includes("source:sec")) &&
        Boolean(e.raw_text_ref),
    )
    .slice(0, 3);
  for (const filing of filings) {
    try {
      const raw = await fs.readFile(filing.raw_text_ref ?? "", "utf8");
      const business = extractSection(
        raw,
        /\bitem\s+1[\.\s:-]*business\b/i,
        /\bitem\s+1a\b/i,
        40_000,
      );
      const risk = extractSection(
        raw,
        /\bitem\s+1a[\.\s:-]*risk\s+factors\b/i,
        /\bitem\s+1b\b/i,
        40_000,
      );
      const businessKeywords = topKeywords(business || raw.slice(0, 50_000), 12);
      const riskKeywords = topKeywords(risk || raw.slice(0, 50_000), 12);
      extracts.filings.push({
        source_id: filing.id,
        title: filing.title,
        url: filing.url,
        date_published: filing.date_published,
        extracted: { business_keywords: businessKeywords, risk_keywords: riskKeywords },
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      notes.push(`filing_extract_skip source_id=${filing.id} reason=${msg}`);
    }
  }

  const transcripts = params.evidence
    .filter(
      (e) =>
        e.reliability_tier === 3 &&
        e.tags.includes(`company:${ticker}`) &&
        e.tags.includes("type:transcript") &&
        Boolean(e.raw_text_ref),
    )
    .slice(0, 2);
  for (const tr of transcripts) {
    try {
      const raw = await fs.readFile(tr.raw_text_ref ?? "", "utf8");
      const keywords = topKeywords(raw.slice(0, 80_000), 14);
      extracts.transcripts.push({
        source_id: tr.id,
        title: tr.title,
        url: tr.url,
        date_published: tr.date_published,
        extracted: { keywords },
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      notes.push(`transcript_extract_skip source_id=${tr.id} reason=${msg}`);
    }
  }

  const sec = pickSecFixture(params.evidence);
  if (!sec?.raw_text_ref) {
    notes.push("No SEC time-series fixture present; numeric_facts are empty.");
    return {
      version: 1,
      generated_at: nowIso(),
      ticker,
      notes,
      extracts,
      numeric_facts,
      kpi_table,
    };
  }

  const raw = await fs.readFile(sec.raw_text_ref, "utf8");
  const rows = parseCsv(raw);
  if (!rows.length) {
    notes.push("SEC time-series fixture could not be parsed; numeric_facts are empty.");
    return {
      version: 1,
      generated_at: nowIso(),
      ticker,
      notes,
      extracts,
      numeric_facts,
      kpi_table,
    };
  }

  // Minimal extract: grab a few anchor metrics (if present).
  const wanted: Array<{ metric: string; concept: string }> = [
    { metric: "Revenue (reported)", concept: "Revenues" },
    { metric: "Operating income (reported)", concept: "OperatingIncomeLoss" },
    { metric: "Net income (reported)", concept: "NetIncomeLoss" },
    { metric: "Cash from ops (reported)", concept: "NetCashProvidedByUsedInOperatingActivities" },
  ];

  let nextN = 1;
  for (const item of wanted) {
    const match = rows.find(
      (row) =>
        (row.concept ?? "").trim() === item.concept &&
        (row.taxonomy ?? "").trim() === "us-gaap" &&
        (row.value ?? "").trim().length > 0,
    );
    if (!match) continue;
    const value = toFinite(match.value ?? "");
    if (typeof value !== "number") continue;
    const unit = (match.unit ?? "").trim() || "unit";
    const end = (match.end ?? "").trim() || "unknown";
    const form = (match.form ?? "").trim() || undefined;
    const fy = (match.fy ?? "").trim();
    const fp = (match.fp ?? "").trim();
    const periodParts = [fy ? `FY${fy}` : "", fp ? fp : "", end ? `end=${end}` : ""].filter(
      Boolean,
    );
    const period = periodParts.length ? periodParts.join(" ") : end;

    const id = `N${nextN}`;
    nextN += 1;

    numeric_facts.push({
      id,
      value,
      unit,
      period,
      currency: unit.toUpperCase() === "USD" ? "USD" : undefined,
      source_id: sec.id,
      accessed_at: sec.accessed_at,
      notes: form ? `form=${form}` : undefined,
    });
    kpi_table.push({
      metric: item.metric,
      numeric_id: id,
      as_of: end,
      form,
    });
  }

  if (!numeric_facts.length) {
    notes.push(
      "SEC fixture did not include any of the expected concepts; numeric_facts are empty.",
    );
  }

  return {
    version: 1,
    generated_at: nowIso(),
    ticker,
    notes,
    extracts,
    numeric_facts,
    kpi_table,
  };
}
