type FetchLike = typeof fetch;

export type FilingFact = {
  accn?: string;
  end?: string;
  filed?: string;
  form?: string;
  fp?: string;
  frame?: string;
  fy?: number;
  val?: number;
};

export type CompanyFactsResponse = {
  cik?: number;
  entityName?: string;
  facts?: Record<string, Record<string, { label?: string; units?: Record<string, FilingFact[]> }>>;
};

export type TimeSeriesPoint = {
  taxonomy: string;
  concept: string;
  label: string;
  unit: string;
  value: number;
  end: string;
  filed?: string;
  form?: string;
  accn?: string;
  fy?: number;
  fp?: string;
  frame?: string;
};

export type TimeSeriesSeries = {
  key: string;
  taxonomy: string;
  concept: string;
  label: string;
  points: TimeSeriesPoint[];
};

export type TimeSeriesExhibit = {
  cik: string;
  entityName: string;
  generatedAt: string;
  includeForms: string[];
  series: TimeSeriesSeries[];
};

export type BuildTimeSeriesOptions = {
  includeForms?: string[];
  perSeriesLimit?: number;
};

const DEFAULT_FORMS = ["10-K", "10-Q", "20-F", "40-F"];
const SEC_DATA_BASE = "https://data.sec.gov";
const SEC_WWW_BASE = "https://www.sec.gov";

export const DEFAULT_CONCEPTS = [
  "us-gaap:Revenues",
  "us-gaap:NetIncomeLoss",
  "us-gaap:OperatingIncomeLoss",
  "us-gaap:EarningsPerShareDiluted",
  "us-gaap:Assets",
  "us-gaap:Liabilities",
  "us-gaap:StockholdersEquity",
  "us-gaap:NetCashProvidedByUsedInOperatingActivities",
];

export const padCik = (value: string | number): string =>
  String(value).replace(/\D/g, "").padStart(10, "0");

const toIsoDate = (raw?: string): string => {
  if (!raw) return "";
  const trimmed = raw.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
  const date = new Date(trimmed);
  if (Number.isNaN(date.getTime())) return "";
  return date.toISOString().slice(0, 10);
};

const parseConceptKey = (raw: string): { taxonomy: string; concept: string } => {
  const value = raw.trim();
  if (!value) return { taxonomy: "us-gaap", concept: "" };
  const [left, right] = value.split(":");
  if (!right) return { taxonomy: "us-gaap", concept: left };
  return { taxonomy: left, concept: right };
};

const comparePoints = (a: TimeSeriesPoint, b: TimeSeriesPoint): number => {
  const endCmp = a.end.localeCompare(b.end);
  if (endCmp !== 0) return endCmp;
  const filedCmp = (a.filed ?? "").localeCompare(b.filed ?? "");
  if (filedCmp !== 0) return filedCmp;
  return (a.accn ?? "").localeCompare(b.accn ?? "");
};

const pickLatestPerPeriod = (points: TimeSeriesPoint[]): TimeSeriesPoint[] => {
  const keyed = new Map<string, TimeSeriesPoint>();
  for (const point of points) {
    const key = [
      point.taxonomy,
      point.concept,
      point.unit,
      point.end,
      point.fy ?? "",
      point.fp ?? "",
      point.form ?? "",
      point.frame ?? "",
    ].join("|");
    const current = keyed.get(key);
    if (!current || comparePoints(current, point) < 0) {
      keyed.set(key, point);
    }
  }
  return [...keyed.values()].sort(comparePoints);
};

export const createSecHeaders = (userAgent?: string): Record<string, string> => ({
  Accept: "application/json",
  "User-Agent": userAgent?.trim() || "DrMackenzieBot/0.1 (research@local.invalid)",
});

export const resolveTickerToCik = async (
  ticker: string,
  fetchImpl: FetchLike = fetch,
  userAgent?: string,
): Promise<string> => {
  const symbol = ticker.trim().toUpperCase();
  const res = await fetchImpl(`${SEC_WWW_BASE}/files/company_tickers.json`, {
    headers: createSecHeaders(userAgent),
  });
  if (!res.ok) {
    throw new Error(`Failed to load SEC ticker map: HTTP ${res.status}`);
  }
  const body = (await res.json()) as Record<
    string,
    {
      cik_str?: number;
      ticker?: string;
    }
  >;
  for (const row of Object.values(body)) {
    if (row.ticker?.toUpperCase() === symbol && typeof row.cik_str === "number") {
      return padCik(row.cik_str);
    }
  }
  throw new Error(`Ticker not found in SEC mapping: ${symbol}`);
};

export const fetchCompanyFacts = async (
  cik: string,
  fetchImpl: FetchLike = fetch,
  userAgent?: string,
): Promise<CompanyFactsResponse> => {
  const normalized = padCik(cik);
  const res = await fetchImpl(`${SEC_DATA_BASE}/api/xbrl/companyfacts/CIK${normalized}.json`, {
    headers: createSecHeaders(userAgent),
  });
  if (!res.ok) {
    throw new Error(`Failed to load SEC companyfacts for CIK ${normalized}: HTTP ${res.status}`);
  }
  return (await res.json()) as CompanyFactsResponse;
};

export const buildTimeSeriesExhibit = (
  companyFacts: CompanyFactsResponse,
  requestedConcepts: string[],
  options: BuildTimeSeriesOptions = {},
): TimeSeriesExhibit => {
  const includeForms = (options.includeForms?.length ? options.includeForms : DEFAULT_FORMS).map(
    (value) => value.trim().toUpperCase(),
  );
  const includeFormsSet = new Set(includeForms);
  const perSeriesLimit = Math.max(1, options.perSeriesLimit ?? 64);

  const series: TimeSeriesSeries[] = [];
  const facts = companyFacts.facts ?? {};
  const cik = padCik(companyFacts.cik ?? "");

  for (const rawKey of requestedConcepts) {
    const { taxonomy, concept } = parseConceptKey(rawKey);
    if (!concept) continue;

    const conceptData = facts[taxonomy]?.[concept];
    if (!conceptData?.units) {
      continue;
    }

    const points: TimeSeriesPoint[] = [];
    for (const [unit, rows] of Object.entries(conceptData.units)) {
      for (const row of rows) {
        if (typeof row.val !== "number") continue;
        const end = toIsoDate(row.end);
        if (!end) continue;
        const form = row.form?.trim().toUpperCase();
        if (form && !includeFormsSet.has(form)) continue;
        points.push({
          taxonomy,
          concept,
          label: conceptData.label?.trim() || concept,
          unit,
          value: row.val,
          end,
          filed: toIsoDate(row.filed) || undefined,
          form,
          accn: row.accn,
          fy: row.fy,
          fp: row.fp,
          frame: row.frame,
        });
      }
    }

    const deduped = pickLatestPerPeriod(points);
    if (!deduped.length) continue;
    const limited = deduped.slice(Math.max(0, deduped.length - perSeriesLimit));

    series.push({
      key: `${taxonomy}:${concept}`,
      taxonomy,
      concept,
      label: limited[0]?.label || concept,
      points: limited,
    });
  }

  series.sort((a, b) => a.key.localeCompare(b.key));
  return {
    cik,
    entityName: companyFacts.entityName?.trim() || "Unknown Entity",
    generatedAt: new Date().toISOString(),
    includeForms,
    series,
  };
};

const csvEscape = (raw: string): string => {
  if (raw.includes(",") || raw.includes('"') || raw.includes("\n")) {
    return `"${raw.replaceAll('"', '""')}"`;
  }
  return raw;
};

export const timeSeriesToCsv = (exhibit: TimeSeriesExhibit): string => {
  const lines = [
    [
      "cik",
      "entityName",
      "seriesKey",
      "label",
      "taxonomy",
      "concept",
      "unit",
      "end",
      "filed",
      "form",
      "fy",
      "fp",
      "frame",
      "value",
      "accn",
    ].join(","),
  ];

  for (const series of exhibit.series) {
    for (const point of series.points) {
      lines.push(
        [
          exhibit.cik,
          exhibit.entityName,
          series.key,
          series.label,
          point.taxonomy,
          point.concept,
          point.unit,
          point.end,
          point.filed ?? "",
          point.form ?? "",
          String(point.fy ?? ""),
          point.fp ?? "",
          point.frame ?? "",
          String(point.value),
          point.accn ?? "",
        ]
          .map(csvEscape)
          .join(","),
      );
    }
  }
  return lines.join("\n");
};

export const timeSeriesToMarkdown = (exhibit: TimeSeriesExhibit, tableSize = 8): string => {
  const lines: string[] = [];
  lines.push(`# SEC/XBRL Time-Series Exhibits`);
  lines.push("");
  lines.push(`- Entity: ${exhibit.entityName}`);
  lines.push(`- CIK: ${exhibit.cik}`);
  lines.push(`- Generated: ${exhibit.generatedAt}`);
  lines.push(`- Forms: ${exhibit.includeForms.join(", ")}`);
  lines.push("");

  for (const series of exhibit.series) {
    const recent = series.points.slice(Math.max(0, series.points.length - tableSize));
    lines.push(`## ${series.key} (${series.label})`);
    lines.push("");
    lines.push("| End | Value | Unit | Form | Filed | FY | FP |");
    lines.push("|---|---:|---|---|---|---:|---|");
    for (const point of recent) {
      lines.push(
        `| ${point.end} | ${point.value} | ${point.unit} | ${point.form ?? ""} | ${point.filed ?? ""} | ${point.fy ?? ""} | ${point.fp ?? ""} |`,
      );
    }
    lines.push("");
  }

  if (!exhibit.series.length) {
    lines.push("No matching SEC/XBRL points found for the requested concepts/forms.");
    lines.push("");
  }

  return lines.join("\n");
};
