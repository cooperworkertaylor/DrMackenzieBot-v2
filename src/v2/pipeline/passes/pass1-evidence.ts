import fs from "node:fs/promises";
import path from "node:path";
import { DEFAULT_CONCEPTS } from "../../../agents/sec-xbrl-timeseries.js";
import { openResearchDb } from "../../../research/db.js";
import {
  EvidenceStore,
  type EvidenceInsert,
  type EvidenceItem,
} from "../../evidence/evidence-store.js";

const toYmd = (date: Date): string => date.toISOString().slice(0, 10);

const epochMsFromSecondsOrMs = (value: number): number =>
  // 10-digit epoch values are almost certainly seconds; 13-digit are ms.
  value < 10_000_000_000 ? value * 1000 : value;

const resolveEpochMs = (value: unknown): number => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return epochMsFromSecondsOrMs(value);
  }
  return Date.now();
};

const safeMkdir = async (dir: string): Promise<void> => {
  await fs.mkdir(dir, { recursive: true });
};

const writeTextIntoRun = async (params: {
  runDir: string;
  evidenceId: string;
  text: string;
  ext?: string;
}): Promise<string> => {
  const sourcesDir = path.join(params.runDir, "sources");
  await safeMkdir(sourcesDir);
  const ext = (params.ext ?? ".txt").trim();
  const normalizedExt = ext.startsWith(".") ? ext : `.${ext}`;
  const dest = path.join(sourcesDir, `${params.evidenceId}${normalizedExt}`);
  await fs.writeFile(dest, params.text, "utf8");
  return dest;
};

const copySourceIntoRun = async (params: {
  runDir: string;
  evidenceId: string;
  fixturePath: string;
}): Promise<string> => {
  const src = path.resolve(params.fixturePath);
  const ext = path.extname(src) || ".txt";
  const sourcesDir = path.join(params.runDir, "sources");
  await safeMkdir(sourcesDir);
  const dest = path.join(sourcesDir, `${params.evidenceId}${ext}`);
  await fs.copyFile(src, dest);
  return dest;
};

const findCompanyFixture = async (params: {
  fixtureDir?: string;
  ticker: string;
}): Promise<string | null> => {
  const dir = (params.fixtureDir ?? "").trim();
  if (!dir) return null;
  const ticker = params.ticker.trim().toLowerCase();
  const candidates = [
    path.join(dir, "sec-edgar", ticker, "time-series.csv"),
    path.join(dir, "sec-edgar", ticker, "time-series.json"),
    path.join(dir, "sec-edgar", ticker, "time-series.md"),
  ];
  for (const p of candidates) {
    try {
      await fs.access(p);
      return p;
    } catch {
      // continue
    }
  }
  return null;
};

const csvEscape = (value: unknown): string => {
  const s = value == null ? "" : String(value);
  if (/[",\n\r]/.test(s)) {
    return `"${s.replaceAll('"', '""')}"`;
  }
  return s;
};

const buildSecXbrlTimeSeriesCsv = (
  rows: Array<{
    cik?: string;
    entity_name?: string;
    taxonomy: string;
    concept: string;
    label?: string;
    unit?: string;
    period_end?: string;
    filing_date?: string;
    form?: string;
    fiscal_year?: number;
    fiscal_period?: string;
    frame?: string;
    value?: number;
    accession?: string;
  }>,
): string => {
  const header = [
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
  ];
  const lines = [header.join(",")];
  for (const row of rows) {
    const cik = String(row.cik ?? "").trim();
    const entityName = String(row.entity_name ?? "").trim();
    const taxonomy = String(row.taxonomy ?? "").trim();
    const concept = String(row.concept ?? "").trim();
    const seriesKey = taxonomy && concept ? `${taxonomy}:${concept}` : "";
    const label = String(row.label ?? "").trim();
    const unit = String(row.unit ?? "").trim();
    const end = String(row.period_end ?? "").slice(0, 10);
    const filed = String(row.filing_date ?? "").slice(0, 10);
    const form = String(row.form ?? "").trim();
    const fy = typeof row.fiscal_year === "number" && row.fiscal_year > 0 ? row.fiscal_year : "";
    const fp = String(row.fiscal_period ?? "").trim();
    const frame = String(row.frame ?? "").trim();
    const value = typeof row.value === "number" && Number.isFinite(row.value) ? row.value : "";
    const accn = String(row.accession ?? "").trim();
    lines.push(
      [
        cik,
        entityName,
        seriesKey,
        label,
        taxonomy,
        concept,
        unit,
        end,
        filed,
        form,
        fy,
        fp,
        frame,
        value,
        accn,
      ]
        .map(csvEscape)
        .join(","),
    );
  }
  return `${lines.join("\n")}\n`;
};

const maybeAddSecXbrlTimeSeriesFromDb = async (params: {
  runDir: string;
  ticker: string;
  instrumentId?: number;
  dbPath?: string;
  store: EvidenceStore;
  now: Date;
}): Promise<boolean> => {
  if (typeof params.instrumentId !== "number") return false;

  const ticker = params.ticker.trim().toUpperCase();
  const parsedConcepts = DEFAULT_CONCEPTS.map((raw) => raw.trim())
    .filter(Boolean)
    .map((raw) => {
      const [taxonomy, concept] = raw.split(":");
      return { taxonomy: (taxonomy ?? "").trim(), concept: (concept ?? "").trim() };
    })
    .filter((item) => item.taxonomy.toLowerCase() === "us-gaap" && item.concept);
  const conceptList = Array.from(new Set(parsedConcepts.map((c) => c.concept)));
  if (!conceptList.length) return false;

  const db = openResearchDb(params.dbPath);
  const placeholders = conceptList.map(() => "?").join(", ");
  const facts = db
    .prepare(
      `SELECT cik, entity_name, taxonomy, concept, label, unit, value, period_end, filing_date, form, fiscal_year, fiscal_period, frame, accession, fetched_at
       FROM fundamental_facts
       WHERE instrument_id=?
         AND is_latest=1
         AND lower(taxonomy)='us-gaap'
         AND concept IN (${placeholders})
       ORDER BY period_end ASC, filing_date ASC`,
    )
    .all(params.instrumentId, ...conceptList) as Array<{
    cik?: string;
    entity_name?: string;
    taxonomy: string;
    concept: string;
    label?: string;
    unit?: string;
    value?: number;
    period_end?: string;
    filing_date?: string;
    form?: string;
    fiscal_year?: number;
    fiscal_period?: string;
    frame?: string;
    accession?: string;
    fetched_at?: number;
  }>;
  if (!facts.length) return false;

  const cik = String(facts.find((r) => (r.cik ?? "").trim())?.cik ?? "").trim();
  const entityName = String(
    facts.find((r) => (r.entity_name ?? "").trim())?.entity_name ?? "",
  ).trim();
  const maxFetchedAtMs = Math.max(
    ...facts.map((row) => resolveEpochMs(row.fetched_at)),
    params.now.getTime(),
  );
  const accessedAt = new Date(maxFetchedAtMs).toISOString();
  const datePublished = toYmd(new Date(maxFetchedAtMs));
  const url = cik
    ? `https://data.sec.gov/api/xbrl/companyfacts/CIK${cik}.json`
    : "https://www.sec.gov/edgar/search/";

  const baseInsert: EvidenceInsert = {
    title: `SEC XBRL time-series (${ticker})`,
    publisher: "SEC",
    date_published: datePublished,
    accessed_at: accessedAt,
    url,
    reliability_tier: 1,
    excerpt_or_key_points: [
      `instrument_id=${params.instrumentId}`,
      entityName ? `entity=${entityName}` : "entity=?",
      `concepts=${conceptList.length}`,
      `rows=${facts.length}`,
    ],
    tags: [`company:${ticker}`, "source:sec", "type:sec-xbrl-timeseries", "db:fundamental_facts"],
  };

  const evidence = params.store.add(baseInsert);
  const csv = buildSecXbrlTimeSeriesCsv(facts);
  const rawRef = await writeTextIntoRun({
    runDir: params.runDir,
    evidenceId: evidence.id,
    text: csv,
    ext: ".csv",
  });
  params.store.add({ ...baseInsert, raw_text_ref: rawRef });
  return true;
};

export type EvidencePassV2Result = {
  evidence: EvidenceItem[];
  claim_backlog: Array<{ id: string; claim: string; status: "open" | "dropped"; note?: string }>;
  coverage: EvidenceCoverageV2;
};

export type EvidenceCoverageV2 = {
  kind: "company" | "theme";
  tickers: string[];
  per_ticker: Array<{
    ticker: string;
    instrument_id?: number;
    counts: {
      filings_available: number;
      filings_selected: number;
      transcripts_available: number;
      transcripts_selected: number;
      external_documents_available: number;
      external_documents_selected: number;
    };
    selected: {
      filings: Array<{
        id: number;
        form?: string;
        filed?: string;
        period_end?: string;
        accession?: string;
        url?: string;
      }>;
      transcripts: Array<{
        id: number;
        event_date?: string;
        event_type?: string;
        source?: string;
        url?: string;
        title?: string;
      }>;
      external_documents: Array<{
        id: number;
        source_type?: string;
        provider?: string;
        received_at?: string;
        published_at?: string;
        url?: string;
        title?: string;
      }>;
    };
  }>;
};

const addDefaultTier2MacroEvidence = (params: {
  store: EvidenceStore;
  now: Date;
  tags?: string[];
}) => {
  params.store.add({
    title: "FRED: Effective Federal Funds Rate (EFFR)",
    publisher: "FRED",
    date_published: toYmd(params.now),
    accessed_at: params.now.toISOString(),
    url: "https://fred.stlouisfed.org/series/EFFR",
    reliability_tier: 2,
    excerpt_or_key_points: [
      "Tier 2 macro context for discount-rate regime; used to sanity-check valuation and scenario sensitivity.",
    ],
    tags: ["macro:rates", "source:fred", ...(params.tags ?? [])].filter(Boolean),
  });
};

const collectDbEvidenceForTicker = async (params: {
  runDir: string;
  ticker: string;
  dbPath?: string;
  store: EvidenceStore;
}): Promise<{
  ticker: string;
  instrumentId?: number;
  selected: EvidenceCoverageV2["per_ticker"][number]["selected"];
  counts: EvidenceCoverageV2["per_ticker"][number]["counts"];
}> => {
  const ticker = params.ticker.trim().toUpperCase();
  const db = openResearchDb(params.dbPath);
  const localFallbackUrl = (kind: "external_documents" | "filings" | "transcripts", id: number) =>
    `https://local.openclaw.ai/${kind}/${id}`;
  const selected: EvidenceCoverageV2["per_ticker"][number]["selected"] = {
    filings: [],
    transcripts: [],
    external_documents: [],
  };
  const counts: EvidenceCoverageV2["per_ticker"][number]["counts"] = {
    filings_available: 0,
    filings_selected: 0,
    transcripts_available: 0,
    transcripts_selected: 0,
    external_documents_available: 0,
    external_documents_selected: 0,
  };
  const collectExternalDocuments = async (): Promise<void> => {
    const rowsA = db
      .prepare(
        `SELECT id, source_type, provider, sender, title, subject, url, published_at, received_at, content, fetched_at
         FROM external_documents
         WHERE upper(ticker)=? AND source_type IN ('email_research','newsletter')
         ORDER BY received_at DESC, fetched_at DESC
         LIMIT 10`,
      )
      .all(ticker) as Array<{
      id: number;
      source_type?: string;
      provider?: string;
      sender?: string;
      title?: string;
      subject?: string;
      url?: string;
      published_at?: string;
      received_at?: string;
      content?: string;
      fetched_at: number;
    }>;
    const rowsB = db
      .prepare(
        `SELECT id, source_type, provider, sender, title, subject, url, published_at, received_at, content, fetched_at
         FROM external_documents
         WHERE upper(ticker)=? AND source_type NOT IN ('email_research','newsletter')
         ORDER BY received_at DESC, fetched_at DESC
         LIMIT 10`,
      )
      .all(ticker) as Array<{
      id: number;
      source_type?: string;
      provider?: string;
      sender?: string;
      title?: string;
      subject?: string;
      url?: string;
      published_at?: string;
      received_at?: string;
      content?: string;
      fetched_at: number;
    }>;
    counts.external_documents_available = rowsA.length + rowsB.length;

    const docs = Array.from(
      new Map<number, (typeof rowsA)[number]>([...rowsA, ...rowsB].map((r) => [r.id, r])).values(),
    ).slice(0, 16);

    for (const row of docs) {
      const fetchedAt = resolveEpochMs(row.fetched_at);
      const published =
        (row.published_at ?? row.received_at ?? "").slice(0, 10) || toYmd(new Date(fetchedAt));
      const accessedAt = new Date(fetchedAt).toISOString();
      const url = (row.url ?? "").trim() || localFallbackUrl("external_documents", row.id);
      const publisher = (row.provider ?? "").trim() || "External";
      const title =
        (row.title ?? "").trim() ||
        (row.subject ?? "").trim() ||
        `External research (${publisher})`;

      const baseInsert: EvidenceInsert = {
        title,
        publisher,
        date_published: published,
        accessed_at: accessedAt,
        url,
        // Intentionally leave tier inference to EvidenceStore when URL is meaningful;
        // local fallbacks will remain Tier 4.
        reliability_tier: undefined,
        excerpt_or_key_points: [
          `source_type=${(row.source_type ?? "").trim() || "unknown"}`,
          (row.sender ?? "").trim() ? `sender=${(row.sender ?? "").trim()}` : "sender=unknown",
          (row.provider ?? "").trim()
            ? `provider=${(row.provider ?? "").trim()}`
            : "provider=unknown",
        ],
        tags: [
          `company:${ticker}`,
          "type:external_document",
          row.source_type ? `source_type:${row.source_type}` : "",
          row.provider ? `provider:${row.provider}` : "",
        ].filter(Boolean),
      };

      const evidence = params.store.add(baseInsert);
      selected.external_documents.push({
        id: row.id,
        source_type: row.source_type,
        provider: row.provider,
        received_at: row.received_at,
        published_at: row.published_at,
        url: evidence.url,
        title,
      });
      if ((row.content ?? "").trim()) {
        const rawRef = await writeTextIntoRun({
          runDir: params.runDir,
          evidenceId: evidence.id,
          text: row.content ?? "",
        });
        params.store.add({ ...baseInsert, raw_text_ref: rawRef });
      }
    }
    counts.external_documents_selected = selected.external_documents.length;
  };
  const instrument = db.prepare("SELECT id FROM instruments WHERE ticker=?").get(ticker) as
    | { id?: number }
    | undefined;
  const instrumentId = instrument?.id;
  if (typeof instrumentId !== "number") {
    await collectExternalDocuments();
    return { ticker, selected, counts };
  }

  const filings = db
    .prepare(
      `SELECT id, accession, form, is_amendment, filed, period_end, title, url, source_url, text, fetched_at
       FROM filings
       WHERE instrument_id=?
       ORDER BY filed DESC
       LIMIT 40`,
    )
    .all(instrumentId) as Array<{
    id: number;
    accession?: string;
    form?: string;
    is_amendment?: number;
    filed?: string;
    period_end?: string;
    title?: string;
    url?: string;
    source_url?: string;
    text?: string;
    fetched_at: number;
  }>;
  counts.filings_available = filings.length;

  // Pick a small, high-signal filing set rather than "any 6" (Lane 1: evidence depth).
  const pickByForms = (forms: string[], limit: number) => {
    const wanted = new Set(forms.map((f) => f.toUpperCase()));
    return filings
      .filter((f) =>
        wanted.has(
          String(f.form ?? "")
            .trim()
            .toUpperCase(),
        ),
      )
      .sort((a, b) => String(b.filed ?? "").localeCompare(String(a.filed ?? "")))
      .slice(0, limit);
  };
  const pickLatest = (limit: number) =>
    [...filings]
      .sort((a, b) => String(b.filed ?? "").localeCompare(String(a.filed ?? "")))
      .slice(0, limit);

  const pickedFilings = Array.from(
    new Map<number, (typeof filings)[number]>(
      [
        ...pickByForms(["10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"], 2),
        ...pickByForms(["10-Q", "10-Q/A"], 4),
        ...pickByForms(["8-K", "8-K/A"], 6),
        ...pickByForms(["DEF 14A", "DEFA14A"], 2),
        ...pickLatest(4),
      ].map((r) => [r.id, r]),
    ).values(),
  ).slice(0, 14);

  for (const row of pickedFilings) {
    const fetchedAt = resolveEpochMs(row.fetched_at);
    const filed = (row.filed ?? "").slice(0, 10) || toYmd(new Date(fetchedAt));
    const accessedAt = new Date(fetchedAt).toISOString();
    const url =
      (row.url ?? row.source_url ?? "").trim() ||
      (typeof row.id === "number" ? localFallbackUrl("filings", row.id) : "") ||
      "https://www.sec.gov/edgar/search/";
    const title =
      (row.title ?? "").trim() || `SEC filing ${(row.form ?? "").trim()}`.trim() || "SEC filing";

    const evidence = params.store.add({
      title,
      publisher: "SEC",
      date_published: filed,
      accessed_at: accessedAt,
      url,
      reliability_tier: 1,
      excerpt_or_key_points: [
        `form=${(row.form ?? "").trim() || "unknown"}`,
        `amendment=${Number(row.is_amendment ?? 0) ? "1" : "0"}`,
        (row.accession ?? "").trim() ? `accession=${String(row.accession).trim()}` : "accession=?",
        row.period_end ? `period_end=${String(row.period_end).slice(0, 10)}` : "period_end=unknown",
      ],
      tags: [`company:${ticker}`, "source:sec", "type:filing"],
    });
    selected.filings.push({
      id: row.id,
      form: row.form,
      filed,
      period_end: row.period_end,
      accession: row.accession,
      url: evidence.url,
    });

    if ((row.text ?? "").trim()) {
      const rawRef = await writeTextIntoRun({
        runDir: params.runDir,
        evidenceId: evidence.id,
        text: row.text ?? "",
      });
      params.store.add({
        title,
        publisher: "SEC",
        date_published: filed,
        accessed_at: accessedAt,
        url,
        reliability_tier: 1,
        excerpt_or_key_points: [],
        raw_text_ref: rawRef,
        tags: [],
      });
    }
  }
  counts.filings_selected = selected.filings.length;

  const transcripts = db
    .prepare(
      `SELECT id, event_date, event_type, source, url, title, content, fetched_at
       FROM transcripts
       WHERE instrument_id=?
       ORDER BY event_date DESC
       LIMIT 10`,
    )
    .all(instrumentId) as Array<{
    id: number;
    event_date?: string;
    event_type?: string;
    source?: string;
    url?: string;
    title?: string;
    content?: string;
    fetched_at: number;
  }>;
  counts.transcripts_available = transcripts.length;

  for (const row of transcripts) {
    const fetchedAt = resolveEpochMs(row.fetched_at);
    const published = (row.event_date ?? "").slice(0, 10) || toYmd(new Date(fetchedAt));
    const accessedAt = new Date(fetchedAt).toISOString();
    const url =
      (row.url ?? "").trim() ||
      (typeof row.id === "number" ? localFallbackUrl("transcripts", row.id) : "") ||
      "https://example.com/transcript";
    const title = (row.title ?? "").trim() || `${ticker} transcript`.trim();
    const publisher = (row.source ?? "").trim() || "Transcript";

    const evidence = params.store.add({
      title,
      publisher,
      date_published: published,
      accessed_at: accessedAt,
      url,
      reliability_tier: 3,
      excerpt_or_key_points: [
        row.event_type ? `event_type=${String(row.event_type)}` : "event_type=unknown",
      ],
      tags: [`company:${ticker}`, "type:transcript"],
    });
    selected.transcripts.push({
      id: row.id,
      event_date: row.event_date,
      event_type: row.event_type,
      source: row.source,
      url: evidence.url,
      title,
    });

    if ((row.content ?? "").trim()) {
      const rawRef = await writeTextIntoRun({
        runDir: params.runDir,
        evidenceId: evidence.id,
        text: row.content ?? "",
      });
      params.store.add({
        title,
        publisher,
        date_published: published,
        accessed_at: accessedAt,
        url,
        reliability_tier: 3,
        excerpt_or_key_points: [],
        raw_text_ref: rawRef,
        tags: [],
      });
    }
  }
  counts.transcripts_selected = selected.transcripts.length;

  await collectExternalDocuments();
  return { ticker, instrumentId, selected, counts };
};

export async function pass1EvidenceCompanyV2(params: {
  runDir: string;
  ticker: string;
  fixtureDir?: string;
  dbPath?: string;
}): Promise<EvidencePassV2Result> {
  const store = new EvidenceStore();
  const now = new Date();
  const ticker = params.ticker.trim().toUpperCase();

  // Even in offline fixture mode, require at least two Tier 1/2 sources to prevent one-source memos.
  addDefaultTier2MacroEvidence({
    store,
    now,
    tags: [`company:${ticker}`],
  });

  const dbCoverage = await collectDbEvidenceForTicker({
    runDir: params.runDir,
    ticker: params.ticker,
    dbPath: params.dbPath,
    store,
  });

  const hasDbTimeSeries = await maybeAddSecXbrlTimeSeriesFromDb({
    runDir: params.runDir,
    ticker,
    instrumentId: dbCoverage.instrumentId,
    dbPath: params.dbPath,
    store,
    now,
  });

  const fixturePath = hasDbTimeSeries
    ? null
    : await findCompanyFixture({
        fixtureDir: params.fixtureDir,
        ticker: params.ticker,
      });
  const hasFixtureTimeSeries = Boolean(fixturePath);
  if (fixturePath) {
    const baseInsert: EvidenceInsert = {
      title: `SEC XBRL time-series (${params.ticker.toUpperCase()})`,
      publisher: "SEC",
      date_published: toYmd(now),
      accessed_at: now.toISOString(),
      url: "https://www.sec.gov/edgar/search/",
      reliability_tier: 1,
      excerpt_or_key_points: [
        "Fixture: time-series extract representing SEC XBRL submissions (for offline demo).",
      ],
      tags: [
        `company:${params.ticker.toUpperCase()}`,
        "source:sec",
        "type:sec-xbrl-timeseries",
        "fixture:sec-xbrl-timeseries",
      ],
    };
    const evidence = store.add(baseInsert);
    const rawRef = await copySourceIntoRun({
      runDir: params.runDir,
      evidenceId: evidence.id,
      fixturePath,
    });
    store.add({ ...baseInsert, raw_text_ref: rawRef });
  }

  const hasTimeSeries = hasDbTimeSeries || hasFixtureTimeSeries;
  return {
    evidence: store.all(),
    claim_backlog: [
      {
        id: "C1",
        claim:
          "Build a KPI baseline from SEC-derived time-series (revenue, operating income, cash from ops).",
        status: "open",
        ...(hasTimeSeries
          ? {}
          : {
              note: "Missing SEC XBRL time-series evidence. Ensure OPENCLAW_RESEARCH_V2_HYDRATE is enabled on macmini (or run `openclaw research ingest fundamentals <TICKER>`).",
            }),
      },
    ],
    coverage: {
      kind: "company",
      tickers: [ticker],
      per_ticker: [
        {
          ticker,
          instrument_id: dbCoverage.instrumentId,
          counts: dbCoverage.counts,
          selected: dbCoverage.selected,
        },
      ],
    },
  };
}

export async function pass1EvidenceThemeV2(params: {
  runDir: string;
  themeName: string;
  universe: string[];
  universeEntities?: import("../../quality/types.js").ThemeUniverseEntityV2[];
  fixtureDir?: string;
  dbPath?: string;
}): Promise<EvidencePassV2Result> {
  const store = new EvidenceStore();
  const now = new Date();
  const universe = params.universe.map((t) => t.trim().toUpperCase()).filter(Boolean);

  addDefaultTier2MacroEvidence({ store, now, tags: [`theme:${params.themeName}`] });

  // Always include a Tier 4 internal spec source so the memo can be generated even when the universe is empty.
  store.add({
    title: "Internal: v2 research contracts and quality gates",
    publisher: "Internal",
    date_published: toYmd(now),
    accessed_at: now.toISOString(),
    url: "https://example.com/internal/v2-quality-gates",
    reliability_tier: 4,
    excerpt_or_key_points: [
      "Defines contracts for citations, numeric provenance, structure, and repair loop behavior.",
    ],
    tags: ["internal:spec", "v2:quality", `theme:${params.themeName}`],
  });

  const perTicker: EvidenceCoverageV2["per_ticker"] = [];
  const maxTimeSeriesTickers = 10;
  for (const ticker of universe) {
    const row = await collectDbEvidenceForTicker({
      runDir: params.runDir,
      ticker,
      dbPath: params.dbPath,
      store,
    });
    perTicker.push({
      ticker,
      instrument_id: row.instrumentId,
      counts: row.counts,
      selected: row.selected,
    });
    const shouldTryTimeSeries = perTicker.length <= maxTimeSeriesTickers;
    const hasDbTimeSeries = shouldTryTimeSeries
      ? await maybeAddSecXbrlTimeSeriesFromDb({
          runDir: params.runDir,
          ticker,
          instrumentId: row.instrumentId,
          dbPath: params.dbPath,
          store,
          now,
        })
      : false;

    const fixturePath = hasDbTimeSeries
      ? null
      : await findCompanyFixture({ fixtureDir: params.fixtureDir, ticker });
    if (!fixturePath) continue;
    const baseInsert: EvidenceInsert = {
      title: `SEC XBRL time-series (${ticker.toUpperCase()})`,
      publisher: "SEC",
      date_published: toYmd(now),
      accessed_at: now.toISOString(),
      url: "https://www.sec.gov/edgar/search/",
      reliability_tier: 1,
      excerpt_or_key_points: [
        "Fixture: time-series extract representing SEC XBRL submissions (for offline demo).",
      ],
      tags: [
        `company:${ticker.toUpperCase()}`,
        "source:sec",
        "type:sec-xbrl-timeseries",
        "fixture:sec-xbrl-timeseries",
      ],
    };
    const evidence = store.add(baseInsert);
    const rawRef = await copySourceIntoRun({
      runDir: params.runDir,
      evidenceId: evidence.id,
      fixturePath,
    });
    store.add({ ...baseInsert, raw_text_ref: rawRef });
  }

  // Theme entities (protocols/assets/private cos): collect external_documents evidence by keyword/domain match.
  const entities = params.universeEntities ?? [];
  if (entities.length) {
    const db = openResearchDb(params.dbPath);
    const localFallbackUrl = (id: number) => `https://local.openclaw.ai/external_documents/${id}`;
    for (const entity of entities) {
      if (entity.type === "equity") {
        // Equity evidence is handled via the ticker universe.
        continue;
      }
      const keys = Array.from(
        new Set(
          [entity.label, entity.symbol ?? "", ...(entity.urls ?? [])]
            .map((v) => String(v ?? "").trim())
            .filter(Boolean)
            .flatMap((v) => {
              if (/^https?:\/\//i.test(v)) {
                try {
                  const host = new URL(v).hostname.replace(/^www\./i, "");
                  return [v, host];
                } catch {
                  return [v];
                }
              }
              return [v];
            }),
        ),
      )
        .filter(Boolean)
        .slice(0, 6);
      if (!keys.length) continue;

      const like = (k: string) => `%${k.toLowerCase()}%`;
      const rowsById = new Map<
        number,
        {
          id: number;
          source_type?: string;
          provider?: string;
          sender?: string;
          title?: string;
          subject?: string;
          url?: string;
          published_at?: string;
          received_at?: string;
          content?: string;
          fetched_at: number;
        }
      >();

      const stmt = db.prepare(
        `SELECT id, source_type, provider, sender, title, subject, url, published_at, received_at, content, fetched_at
         FROM external_documents
         WHERE (
           lower(coalesce(title,'')) LIKE ?
           OR lower(coalesce(subject,'')) LIKE ?
           OR lower(coalesce(url,'')) LIKE ?
           OR lower(coalesce(content,'')) LIKE ?
         )
         ORDER BY received_at DESC, fetched_at DESC
         LIMIT 8`,
      );

      for (const key of keys) {
        const fetched = stmt.all(like(key), like(key), like(key), like(key)) as Array<{
          id: number;
          source_type?: string;
          provider?: string;
          sender?: string;
          title?: string;
          subject?: string;
          url?: string;
          published_at?: string;
          received_at?: string;
          content?: string;
          fetched_at: number;
        }>;
        for (const row of fetched) {
          rowsById.set(row.id, row);
        }
      }

      const rows = Array.from(rowsById.values()).slice(0, 12);
      for (const row of rows) {
        const fetchedAt = resolveEpochMs(row.fetched_at);
        const published =
          (row.published_at ?? row.received_at ?? "").slice(0, 10) || toYmd(new Date(fetchedAt));
        const accessedAt = new Date(fetchedAt).toISOString();
        const url = (row.url ?? "").trim() || localFallbackUrl(row.id);
        const publisher = (row.provider ?? "").trim() || "External";
        const title =
          (row.title ?? "").trim() ||
          (row.subject ?? "").trim() ||
          `External research (${publisher})`;

        const baseInsert: EvidenceInsert = {
          title,
          publisher,
          date_published: published,
          accessed_at: accessedAt,
          url,
          reliability_tier: undefined,
          excerpt_or_key_points: [
            `entity_id=${entity.id}`,
            `entity_type=${entity.type}`,
            entity.symbol ? `symbol=${entity.symbol}` : "symbol=NA",
            `source_type=${(row.source_type ?? "").trim() || "unknown"}`,
            (row.sender ?? "").trim() ? `sender=${(row.sender ?? "").trim()}` : "sender=unknown",
          ],
          tags: [
            `theme:${params.themeName}`,
            "type:external_document",
            `entity_id:${entity.id}`,
            `entity_type:${entity.type}`,
            entity.symbol ? `symbol:${entity.symbol}` : "",
            row.source_type ? `source_type:${row.source_type}` : "",
            row.provider ? `provider:${row.provider}` : "",
          ].filter(Boolean),
        };
        const evidence = store.add(baseInsert);
        if ((row.content ?? "").trim()) {
          const rawRef = await writeTextIntoRun({
            runDir: params.runDir,
            evidenceId: evidence.id,
            text: row.content ?? "",
          });
          store.add({ ...baseInsert, raw_text_ref: rawRef });
        }
      }
    }
  }

  return {
    evidence: store.all(),
    claim_backlog: [
      { id: "C1", claim: `Define theme precisely: ${params.themeName}`, status: "open" },
      { id: "C2", claim: "Map value chain and where value accrues.", status: "open" },
      {
        id: "C3",
        claim: "Define adoption signposts and falsifiers tied to sources.",
        status: "open",
      },
    ],
    coverage: {
      kind: "theme",
      tickers: universe,
      per_ticker: perTicker,
    },
  };
}
