import fs from "node:fs/promises";
import type { EvidenceItem } from "../../evidence/evidence-store.js";

export type CompanyAnalyzerOutputV2 = {
  version: 1;
  generated_at: string;
  ticker: string;
  notes: string[];
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

  const sec = pickSecFixture(params.evidence);
  if (!sec?.raw_text_ref) {
    notes.push("No SEC time-series fixture present; numeric_facts are empty.");
    return {
      version: 1,
      generated_at: nowIso(),
      ticker,
      notes,
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
    numeric_facts,
    kpi_table,
  };
}
