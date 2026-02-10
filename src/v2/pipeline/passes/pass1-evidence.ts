import fs from "node:fs/promises";
import path from "node:path";
import { openResearchDb } from "../../../research/db.js";
import { EvidenceStore, type EvidenceItem } from "../../evidence/evidence-store.js";

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
};

const collectDbEvidenceForTicker = async (params: {
  runDir: string;
  ticker: string;
  dbPath?: string;
  store: EvidenceStore;
}): Promise<void> => {
  const ticker = params.ticker.trim().toUpperCase();
  const db = openResearchDb(params.dbPath);
  const instrument = db.prepare("SELECT id FROM instruments WHERE ticker=?").get(ticker) as
    | { id?: number }
    | undefined;
  const instrumentId = instrument?.id;
  if (typeof instrumentId !== "number") return;

  const filings = db
    .prepare(
      `SELECT form, filed, period_end, title, url, source_url, text, fetched_at
       FROM filings
       WHERE instrument_id=?
       ORDER BY filed DESC
       LIMIT 6`,
    )
    .all(instrumentId) as Array<{
    form?: string;
    filed?: string;
    period_end?: string;
    title?: string;
    url?: string;
    source_url?: string;
    text?: string;
    fetched_at: number;
  }>;

  for (const row of filings) {
    const fetchedAt =
      typeof row.fetched_at === "number" ? row.fetched_at : Math.floor(Date.now() / 1000);
    const filed = (row.filed ?? "").slice(0, 10) || toYmd(new Date(fetchedAt * 1000));
    const accessedAt = new Date(fetchedAt * 1000).toISOString();
    const url = (row.url ?? row.source_url ?? "").trim() || "https://www.sec.gov/edgar/search/";
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
        row.period_end ? `period_end=${String(row.period_end).slice(0, 10)}` : "period_end=unknown",
      ],
      tags: [`company:${ticker}`, "source:sec", "type:filing"],
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

  const transcripts = db
    .prepare(
      `SELECT event_date, event_type, source, url, title, content, fetched_at
       FROM transcripts
       WHERE instrument_id=?
       ORDER BY event_date DESC
       LIMIT 6`,
    )
    .all(instrumentId) as Array<{
    event_date?: string;
    event_type?: string;
    source?: string;
    url?: string;
    title?: string;
    content?: string;
    fetched_at: number;
  }>;

  for (const row of transcripts) {
    const fetchedAt =
      typeof row.fetched_at === "number" ? row.fetched_at : Math.floor(Date.now() / 1000);
    const published = (row.event_date ?? "").slice(0, 10) || toYmd(new Date(fetchedAt * 1000));
    const accessedAt = new Date(fetchedAt * 1000).toISOString();
    const url = (row.url ?? "").trim() || "https://example.com/transcript";
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
};

export async function pass1EvidenceCompanyV2(params: {
  runDir: string;
  ticker: string;
  fixtureDir?: string;
  dbPath?: string;
}): Promise<EvidencePassV2Result> {
  const store = new EvidenceStore();
  const now = new Date();

  await collectDbEvidenceForTicker({
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
    const baseInsert = {
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
    } as const;
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

  for (const ticker of params.universe) {
    await collectDbEvidenceForTicker({
      runDir: params.runDir,
      ticker,
      dbPath: params.dbPath,
      store,
    });
    const fixturePath = await findCompanyFixture({ fixtureDir: params.fixtureDir, ticker });
    if (!fixturePath) continue;
    const baseInsert = {
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
    } as const;
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
  };
}
