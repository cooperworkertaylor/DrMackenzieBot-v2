import fs from "node:fs/promises";
import path from "node:path";
import { openResearchDb } from "../../../research/db.js";
import {
  EvidenceStore,
  type EvidenceInsert,
  type EvidenceItem,
} from "../../evidence/evidence-store.js";

const toYmd = (date: Date): string => date.toISOString().slice(0, 10);

const safeMkdir = async (dir: string): Promise<void> => {
  await fs.mkdir(dir, { recursive: true });
};

const writeTextIntoRun = async (params: {
  runDir: string;
  evidenceId: string;
  text: string;
}): Promise<string> => {
  const sourcesDir = path.join(params.runDir, "sources");
  await safeMkdir(sourcesDir);
  const dest = path.join(sourcesDir, `${params.evidenceId}.txt`);
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
      const fetchedAt =
        typeof row.fetched_at === "number" ? row.fetched_at : Math.floor(Date.now() / 1000);
      const published =
        (row.published_at ?? row.received_at ?? "").slice(0, 10) ||
        toYmd(new Date(fetchedAt * 1000));
      const accessedAt = new Date(fetchedAt * 1000).toISOString();
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
    const fetchedAt =
      typeof row.fetched_at === "number" ? row.fetched_at : Math.floor(Date.now() / 1000);
    const filed = (row.filed ?? "").slice(0, 10) || toYmd(new Date(fetchedAt * 1000));
    const accessedAt = new Date(fetchedAt * 1000).toISOString();
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
    const fetchedAt =
      typeof row.fetched_at === "number" ? row.fetched_at : Math.floor(Date.now() / 1000);
    const published = (row.event_date ?? "").slice(0, 10) || toYmd(new Date(fetchedAt * 1000));
    const accessedAt = new Date(fetchedAt * 1000).toISOString();
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

  const fixturePath = await findCompanyFixture({
    fixtureDir: params.fixtureDir,
    ticker: params.ticker,
  });
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
      tags: [`company:${params.ticker.toUpperCase()}`, "source:sec", "fixture:sec-xbrl-timeseries"],
    };
    const evidence = store.add(baseInsert);
    const rawRef = await copySourceIntoRun({
      runDir: params.runDir,
      evidenceId: evidence.id,
      fixturePath,
    });
    store.add({ ...baseInsert, raw_text_ref: rawRef });
    return {
      evidence: store.all(),
      claim_backlog: [
        {
          id: "C1",
          claim:
            "Build a KPI baseline from SEC-derived time-series (revenue, operating income, cash from ops).",
          status: "open",
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

  return {
    evidence: store.all(),
    claim_backlog: [
      {
        id: "C1",
        claim:
          "No evidence available. Ingest filings/transcripts into the research DB or provide --v2-fixture-dir.",
        status: "open",
        note: "Fail closed at compile if sources are missing.",
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
    const fixturePath = await findCompanyFixture({ fixtureDir: params.fixtureDir, ticker });
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
      tags: [`company:${ticker.toUpperCase()}`, "source:sec", "fixture:sec-xbrl-timeseries"],
    };
    const evidence = store.add(baseInsert);
    const rawRef = await copySourceIntoRun({
      runDir: params.runDir,
      evidenceId: evidence.id,
      fixturePath,
    });
    store.add({ ...baseInsert, raw_text_ref: rawRef });
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
