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
  risk_factor_buckets: Array<{
    bucket: string;
    keywords: string[];
    source_ids: string[];
  }>;
  accounting_flags: Array<{
    flag: string;
    evidence: string;
    source_ids: string[];
  }>;
  catalyst_candidates: Array<{
    label: string;
    rationale: string;
    source_ids: string[];
  }>;
  catalyst_calendar: Array<{
    date: string; // YYYY-MM-DD
    label: string;
    source_ids: string[];
  }>;
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

const anyIncludes = (keywords: string[], needles: string[]): boolean => {
  const set = new Set(keywords.map((k) => k.toLowerCase()));
  return needles.some((n) => set.has(n.toLowerCase()));
};

const summarizeRiskBuckets = (params: {
  filingExtracts: Array<{
    source_id: string;
    extracted: { risk_keywords: string[] };
  }>;
}): CompanyAnalyzerOutputV2["risk_factor_buckets"] => {
  const allKeywords = params.filingExtracts.flatMap((f) => f.extracted.risk_keywords);
  const uniq = Array.from(new Set(allKeywords)).filter(Boolean);
  const buckets: Array<{ bucket: string; needles: string[] }> = [
    {
      bucket: "Regulatory / legal",
      needles: ["regulatory", "regulation", "compliance", "legal", "litigation", "privacy"],
    },
    {
      bucket: "Macro / rates",
      needles: ["inflation", "recession", "macroeconomic", "rates", "interest", "currency"],
    },
    {
      bucket: "Competition / pricing",
      needles: ["competition", "competitors", "pricing", "price", "substitute", "commoditization"],
    },
    {
      bucket: "Security / reliability",
      needles: ["security", "cybersecurity", "breach", "outage", "reliability", "vulnerability"],
    },
    {
      bucket: "Customer / demand",
      needles: ["customer", "demand", "retention", "churn", "pipeline", "backlog"],
    },
    {
      bucket: "Execution / product",
      needles: ["execution", "launch", "product", "quality", "integration", "implementation"],
    },
    {
      bucket: "Supply chain / vendors",
      needles: ["supply", "supplier", "manufacturing", "inventory", "vendor", "third-party"],
    },
    { bucket: "Talent", needles: ["talent", "hiring", "retention", "employees", "labor"] },
    {
      bucket: "Capital allocation / dilution",
      needles: ["dilution", "stock-based", "sbc", "share", "repurchase", "buyback"],
    },
  ];

  const sourceIds = Array.from(new Set(params.filingExtracts.map((f) => f.source_id)));
  return buckets
    .filter((b) => anyIncludes(uniq, b.needles))
    .map((b) => ({
      bucket: b.bucket,
      keywords: uniq
        .filter((k) => b.needles.some((n) => k.toLowerCase() === n.toLowerCase()))
        .slice(0, 10),
      source_ids: sourceIds,
    }));
};

const detectAccountingFlags = (params: {
  filingTexts: Array<{ source_id: string; text: string }>;
}): CompanyAnalyzerOutputV2["accounting_flags"] => {
  const flags: Array<{ flag: string; re: RegExp; evidence: string }> = [
    {
      flag: "Stock-based compensation",
      re: /\bstock[-\s]*based compensation\b/i,
      evidence: "Mentions stock-based compensation in filing text.",
    },
    {
      flag: "Non-GAAP emphasis",
      re: /\bnon[-\s]*gaap\b/i,
      evidence: "Mentions non-GAAP measures in filing text.",
    },
    {
      flag: "Goodwill / impairment",
      re: /\bgoodwill\b|\bimpairment\b/i,
      evidence: "Mentions goodwill or impairment in filing text.",
    },
    {
      flag: "Deferred revenue / RPO",
      re: /\bdeferred revenue\b|\bremaining performance obligations?\b|\brpo\b/i,
      evidence: "Mentions deferred revenue or remaining performance obligations.",
    },
    {
      flag: "Customer concentration",
      re: /\bcustomer concentration\b|\bmaterial customer\b/i,
      evidence: "Mentions customer concentration.",
    },
    { flag: "Restructuring", re: /\brestructuring\b/i, evidence: "Mentions restructuring." },
  ];
  const out: CompanyAnalyzerOutputV2["accounting_flags"] = [];
  for (const f of flags) {
    const matchedSources = params.filingTexts
      .filter((t) => f.re.test(t.text))
      .map((t) => t.source_id);
    if (!matchedSources.length) continue;
    out.push({
      flag: f.flag,
      evidence: f.evidence,
      source_ids: Array.from(new Set(matchedSources)),
    });
  }
  return out;
};

const buildCatalystCandidates = (params: {
  filingMeta: Array<{ source_id: string; title: string; formHint?: string }>;
  transcriptMeta: Array<{ source_id: string; title: string }>;
}): CompanyAnalyzerOutputV2["catalyst_candidates"] => {
  const out: CompanyAnalyzerOutputV2["catalyst_candidates"] = [];
  const filingSourceIds = Array.from(new Set(params.filingMeta.map((f) => f.source_id)));
  const transcriptSourceIds = Array.from(new Set(params.transcriptMeta.map((t) => t.source_id)));

  // Heuristic: if 8-K exists, likely contains event-driven updates.
  const has8k = params.filingMeta.some(
    (f) => /\b8-k\b/i.test(f.title) || /\b8-k\b/i.test(f.formHint ?? ""),
  );
  if (has8k) {
    out.push({
      label: "Event-driven disclosure (8-K)",
      rationale:
        "Recent 8-K filings may contain discrete events (contracts, guidance, governance changes) that can shift scenario weights.",
      source_ids: filingSourceIds,
    });
  }
  // Heuristic: if DEF 14A exists, governance/capital allocation items may be catalyst-relevant.
  const hasProxy = params.filingMeta.some((f) => /\bdef\s*14a\b/i.test(f.title));
  if (hasProxy) {
    out.push({
      label: "Governance / capital allocation updates (proxy)",
      rationale:
        "Proxy filings can surface governance proposals, compensation structure, and capital allocation posture.",
      source_ids: filingSourceIds,
    });
  }
  if (params.transcriptMeta.length) {
    out.push({
      label: "Next earnings cycle updates",
      rationale:
        "Transcripts indicate which operating levers management is emphasizing; subsequent earnings can confirm or falsify those claims.",
      source_ids: transcriptSourceIds,
    });
  }
  return out;
};

const sanitizeCatalystLabel = (raw: string): string => {
  return raw
    .replaceAll(/\b\d{1,2}\s*-\s*[kq]\b/gi, "filing")
    .replaceAll(/\bdef\s*14a\b/gi, "proxy")
    .replaceAll(/\bq\d\b/gi, "quarter")
    .replaceAll(/\s+/g, " ")
    .trim();
};

const buildCatalystCalendar = (params: {
  filings: Array<{ source_id: string; date_published: string; title: string }>;
  transcripts: Array<{ source_id: string; date_published: string; title: string }>;
}): CompanyAnalyzerOutputV2["catalyst_calendar"] => {
  const rows: CompanyAnalyzerOutputV2["catalyst_calendar"] = [];
  params.filings.slice(0, 6).forEach((f) => {
    const date = String(f.date_published ?? "").slice(0, 10);
    if (!date) return;
    rows.push({
      date,
      label: sanitizeCatalystLabel(`SEC filing: ${f.title}`) || "SEC filing",
      source_ids: [f.source_id],
    });
  });
  params.transcripts.slice(0, 4).forEach((t) => {
    const date = String(t.date_published ?? "").slice(0, 10);
    if (!date) return;
    rows.push({
      date,
      label: sanitizeCatalystLabel(`Earnings transcript: ${t.title}`) || "Earnings transcript",
      source_ids: [t.source_id],
    });
  });
  return Array.from(new Map(rows.map((r) => [`${r.date}|${r.label}`, r] as const)).values())
    .sort((a, b) => String(b.date).localeCompare(String(a.date)))
    .slice(0, 10);
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
  const filingTextsForFlags: Array<{ source_id: string; text: string }> = [];

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
      filingTextsForFlags.push({ source_id: filing.id, text: raw.slice(0, 120_000) });
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

  const risk_factor_buckets = summarizeRiskBuckets({
    filingExtracts: extracts.filings.map((f) => ({
      source_id: f.source_id,
      extracted: f.extracted,
    })),
  });
  const accounting_flags = detectAccountingFlags({ filingTexts: filingTextsForFlags });
  const catalyst_candidates = buildCatalystCandidates({
    filingMeta: extracts.filings.map((f) => ({ source_id: f.source_id, title: f.title })),
    transcriptMeta: extracts.transcripts.map((t) => ({ source_id: t.source_id, title: t.title })),
  });
  const catalyst_calendar = buildCatalystCalendar({
    filings: extracts.filings.map((f) => ({
      source_id: f.source_id,
      date_published: f.date_published,
      title: f.title,
    })),
    transcripts: extracts.transcripts.map((t) => ({
      source_id: t.source_id,
      date_published: t.date_published,
      title: t.title,
    })),
  });

  const sec = pickSecFixture(params.evidence);
  if (!sec?.raw_text_ref) {
    notes.push("No SEC time-series fixture present; numeric_facts are empty.");
    return {
      version: 1,
      generated_at: nowIso(),
      ticker,
      notes,
      extracts,
      risk_factor_buckets,
      accounting_flags,
      catalyst_candidates,
      catalyst_calendar,
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
      risk_factor_buckets,
      accounting_flags,
      catalyst_candidates,
      catalyst_calendar,
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
    risk_factor_buckets,
    accounting_flags,
    catalyst_candidates,
    catalyst_calendar,
    numeric_facts,
    kpi_table,
  };
}
