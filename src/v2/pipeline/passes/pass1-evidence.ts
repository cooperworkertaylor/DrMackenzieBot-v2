import fs from "node:fs/promises";
import path from "node:path";
import { EvidenceStore, type EvidenceItem } from "../../evidence/evidence-store.js";

const toYmd = (date: Date): string => date.toISOString().slice(0, 10);

const safeMkdir = async (dir: string): Promise<void> => {
  await fs.mkdir(dir, { recursive: true });
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

export async function pass1EvidenceCompanyV2(params: {
  runDir: string;
  ticker: string;
  fixtureDir?: string;
}): Promise<EvidencePassV2Result> {
  const store = new EvidenceStore();
  const now = new Date();
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
          "No fixture evidence available. Ingest filings/transcripts into the research DB or provide fixtureDir.",
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
