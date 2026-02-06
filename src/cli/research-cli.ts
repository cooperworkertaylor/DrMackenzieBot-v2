import type { Command } from "commander";
import fs from "node:fs/promises";
import path from "node:path";
import {
  DEFAULT_CONCEPTS,
  buildTimeSeriesExhibit,
  fetchCompanyFacts,
  resolveTickerToCik,
  timeSeriesToCsv,
  timeSeriesToMarkdown,
} from "../agents/sec-xbrl-timeseries.js";
import { openResearchDb, resolveResearchDbPath } from "../research/db.js";
import {
  latestEvalReport,
  runCodingEval,
  runFinanceEval,
  runRetrievalEval,
} from "../research/eval.js";
import {
  ingestFilings,
  ingestExpectations,
  ingestFundamentals,
  ingestPrices,
  ingestTranscript,
} from "../research/ingest.js";
import { generateMemoAsync } from "../research/memo.js";
import { indexRepo } from "../research/repo-index.js";
import { computeVariantPerception } from "../research/variant.js";
import { searchResearch, syncEmbeddings, writeBackup } from "../research/vector-search.js";
import { defaultRuntime } from "../runtime.js";
import { formatDocsLink } from "../terminal/links.js";
import { theme } from "../terminal/theme.js";
import { runCommandWithRuntime } from "./cli-utils.js";

type SecXbrlOptions = {
  ticker?: string;
  cik?: string;
  concept?: string[];
  forms?: string;
  outDir?: string;
  limit?: string;
  userAgent?: string;
};

const collectOption = (value: string, previous: string[]): string[] => [...previous, value];

export function registerResearchCli(program: Command) {
  const research = program
    .command("research")
    .description("Research data tools")
    .addHelpText(
      "after",
      () => `\n${theme.muted("Docs:")} ${formatDocsLink("/cli", "docs.openclaw.ai/cli")}\n`,
    );

  research
    .command("sec-xbrl")
    .description("Generate SEC/XBRL time-series exhibits from SEC companyfacts")
    .option("--ticker <symbol>", "SEC ticker symbol (e.g., AAPL)")
    .option("--cik <cik>", "SEC CIK (zero-padded or not)")
    .option("--concept <key>", "Repeatable concept key (e.g., us-gaap:Revenues)", collectOption, [])
    .option("--forms <csv>", "Comma-separated form filter (default: 10-K,10-Q,20-F,40-F)")
    .option("--out-dir <path>", "Output root directory", "sec-edgar-filings")
    .option("--limit <n>", "Max points per series (default: 64)", "64")
    .option("--user-agent <value>", "SEC-compliant user agent (or use SEC_USER_AGENT env)")
    .action(async (opts: SecXbrlOptions) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const ticker = opts.ticker?.trim();
        const cik = opts.cik?.trim();
        if (!ticker && !cik) {
          throw new Error("Provide --ticker or --cik.");
        }

        const forms = (opts.forms ?? "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean);
        const perSeriesLimit = Math.max(1, Number.parseInt(opts.limit ?? "64", 10) || 64);
        const concepts =
          opts.concept && opts.concept.length > 0
            ? opts.concept.map((value) => value.trim()).filter(Boolean)
            : DEFAULT_CONCEPTS;
        const userAgent = opts.userAgent?.trim() || process.env.SEC_USER_AGENT;
        const resolvedCik = cik ?? (await resolveTickerToCik(ticker ?? "", fetch, userAgent));

        const companyFacts = await fetchCompanyFacts(resolvedCik, fetch, userAgent);
        const exhibit = buildTimeSeriesExhibit(companyFacts, concepts, {
          includeForms: forms,
          perSeriesLimit,
        });

        const outputRoot = path.resolve(opts.outDir ?? "sec-edgar-filings", exhibit.cik);
        await fs.mkdir(outputRoot, { recursive: true });

        const jsonPath = path.join(outputRoot, "time-series.json");
        const csvPath = path.join(outputRoot, "time-series.csv");
        const mdPath = path.join(outputRoot, "time-series.md");

        await fs.writeFile(jsonPath, `${JSON.stringify(exhibit, null, 2)}\n`, "utf8");
        await fs.writeFile(csvPath, `${timeSeriesToCsv(exhibit)}\n`, "utf8");
        await fs.writeFile(mdPath, `${timeSeriesToMarkdown(exhibit)}\n`, "utf8");

        const pointCount = exhibit.series.reduce((sum, item) => sum + item.points.length, 0);
        defaultRuntime.log(
          `Wrote SEC/XBRL exhibits for ${exhibit.entityName} (CIK ${exhibit.cik})`,
        );
        defaultRuntime.log(`Series: ${exhibit.series.length}, points: ${pointCount}`);
        defaultRuntime.log(`JSON: ${jsonPath}`);
        defaultRuntime.log(`CSV: ${csvPath}`);
        defaultRuntime.log(`MD: ${mdPath}`);
      });
    });

  research
    .command("init-db")
    .description("Create research sqlite db and run migrations")
    .option("--path <db>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const db = openResearchDb(opts.path);
        void db;
        defaultRuntime.log(`Research DB ready at ${resolveResearchDbPath(opts.path as string)}`);
      });
    });

  research
    .command("prices")
    .description("Ingest daily prices from Alpha Vantage")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const count = await ingestPrices(opts.ticker as string);
        defaultRuntime.log(`Saved ${count} price rows for ${opts.ticker}`);
      });
    });

  research
    .command("filings")
    .description("Fetch recent SEC filings and chunk them")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--limit <n>", "Max filings", "20")
    .option("--user-agent <ua>", "SEC User-Agent header")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const results = await ingestFilings(opts.ticker as string, {
          limit: Number.parseInt(opts.limit as string, 10) || 20,
          userAgent: opts.userAgent as string | undefined,
        });
        const ok = results.filter((r) => r.ok).length;
        const failed = results.filter((r) => !r.ok);
        defaultRuntime.log(`Filings ingested: ${ok}/${results.length}`);
        if (failed.length) {
          failed.forEach((f) =>
            defaultRuntime.error(`Failed ${f.accession}: ${"error" in f ? f.error : "unknown"}`),
          );
        }
      });
    });

  research
    .command("fundamentals")
    .description("Ingest SEC companyfacts as point-in-time fundamentals")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--concept <key>", "Repeatable concept key (e.g., us-gaap:Revenues)", collectOption, [])
    .option("--forms <csv>", "Comma-separated form filter (default: 10-K,10-Q,20-F,40-F)")
    .option("--user-agent <ua>", "SEC User-Agent header")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const forms = (opts.forms as string | undefined)
          ?.split(",")
          .map((value) => value.trim().toUpperCase())
          .filter(Boolean);
        const concepts = (opts.concept as string[] | undefined)
          ?.map((value) => value.trim())
          .filter(Boolean);
        const result = await ingestFundamentals(opts.ticker as string, {
          userAgent: opts.userAgent as string | undefined,
          includeForms: forms?.length ? forms : undefined,
          concepts: concepts?.length ? concepts : undefined,
        });
        defaultRuntime.log(
          `Fundamentals ingested for ${result.ticker} (CIK ${result.cik}): observations=${result.observations}, concepts=${result.conceptCount}`,
        );
      });
    });

  research
    .command("expectations")
    .description("Ingest analyst expectations and EPS surprise history")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = await ingestExpectations(opts.ticker as string);
        defaultRuntime.log(
          `Expectations ingested for ${(opts.ticker as string).toUpperCase()}: rows=${result.rows}, quarterly=${result.quarterly}, annual=${result.annual}`,
        );
      });
    });

  research
    .command("transcript")
    .description("Scrape a transcript URL (HTML or PDF) and chunk it")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .requiredOption("--url <url>", "Transcript URL")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const res = await ingestTranscript(opts.ticker as string, opts.url as string);
        defaultRuntime.log(`Transcript ingested (${res.chunks} chunks)`);
      });
    });

  research
    .command("repo-index")
    .description("Index a code repo into research db (text chunks, no embeddings)")
    .requiredOption("--root <path>", "Repo root to index")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const res = indexRepo({ root: opts.root as string, dbPath: opts.db as string });
        defaultRuntime.log(`Indexed ${res.filesIndexed} files into ${res.dbPath}`);
      });
    });

  research
    .command("embed")
    .description("Create/update sqlite-vec embeddings for research/code chunks")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const res = await syncEmbeddings(opts.db as string);
        defaultRuntime.log(
          `Embedded chunks=${res.chunkCount}, repo_chunks=${res.repoCount} provider=${res.provider} model=${res.model} dims=${res.dims}`,
        );
        if (res.warning) defaultRuntime.error(`WARN: ${res.warning}`);
      });
    });

  research
    .command("search")
    .description("Vector search research or code corpus with citations")
    .requiredOption("--query <text>", "Search query")
    .option("--ticker <symbol>", "Ticker filter for research docs")
    .option("--limit <n>", "Result limit", "8")
    .option("--source <kind>", "research|code", "research")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const hits = await searchResearch({
          query: opts.query as string,
          ticker: opts.ticker as string | undefined,
          limit: Number.parseInt(opts.limit as string, 10) || 8,
          source: (opts.source as "research" | "code") ?? "research",
          dbPath: opts.db as string,
        });
        if (!hits.length) {
          defaultRuntime.log("No results.");
          return;
        }
        hits.forEach((h, i) => {
          defaultRuntime.log(
            `${i + 1}. [${h.table}:${h.id}] score=${h.score.toFixed(3)} vector=${h.vectorScore.toFixed(3)} lexical=${h.lexicalScore.toFixed(3)}`,
          );
          defaultRuntime.log(`   ${h.text.slice(0, 220).replace(/\s+/g, " ")}...`);
          if (h.sourceTable) defaultRuntime.log(`   source_table=${h.sourceTable}`);
          if (h.citationUrl) defaultRuntime.log(`   citation=${h.citationUrl}`);
          if (h.meta) defaultRuntime.log(`   meta=${h.meta}`);
        });
      });
    });

  research
    .command("variant")
    .description("Compute variant-perception score vs consensus expectations")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = computeVariantPerception({
          ticker: opts.ticker as string,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`ticker=${result.ticker}`);
        defaultRuntime.log(`stance=${result.stance}`);
        defaultRuntime.log(`variant_gap_score=${result.variantGapScore.toFixed(2)}`);
        defaultRuntime.log(`expectation_score=${result.expectationScore.toFixed(2)}`);
        defaultRuntime.log(`fundamental_score=${result.fundamentalScore.toFixed(2)}`);
        defaultRuntime.log(`confidence=${result.confidence.toFixed(2)}`);
        defaultRuntime.log(`expectation_observations=${result.expectationObservations}`);
        defaultRuntime.log(`fundamental_observations=${result.fundamentalObservations}`);
        if (result.notes.length) {
          result.notes.forEach((note) => defaultRuntime.error(`NOTE: ${note}`));
        }
      });
    });

  research
    .command("memo")
    .description("Generate citation-enforced research memo")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .requiredOption("--question <text>", "Research question")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .option("--out <path>", "Write memo markdown to file")
    .option("--allow-draft", "Allow output even if institutional quality gate fails", false)
    .option("--min-score <n>", "Institutional quality threshold [0-1]", "0.8")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = await generateMemoAsync({
          ticker: opts.ticker as string,
          question: opts.question as string,
          dbPath: opts.db as string,
          enforceInstitutionalGrade: !Boolean(opts.allowDraft),
          minQualityScore: Number.parseFloat(opts.minScore as string) || 0.8,
        });
        if (opts.out) {
          await fs.writeFile(path.resolve(opts.out as string), `${result.memo}\n`, "utf8");
          defaultRuntime.log(`Memo written to ${path.resolve(opts.out as string)}`);
        } else {
          defaultRuntime.log(result.memo);
        }
        defaultRuntime.log(`claims=${result.claims} citations=${result.citations}`);
        defaultRuntime.log(`quality_score=${result.quality.score.toFixed(2)}`);
      });
    });

  research
    .command("eval")
    .description("Run finance/coding evals and store scores")
    .requiredOption("--type <kind>", "finance|coding|retrieval|all")
    .option("--repo-root <path>", "Repo root for coding eval", process.cwd())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const kind = (opts.type as string).toLowerCase();
        if (kind === "finance" || kind === "all") {
          const res = await runFinanceEval();
          defaultRuntime.log(
            `finance eval score=${(res.score * 100).toFixed(1)}% (${res.passed}/${res.total})`,
          );
        }
        if (kind === "coding" || kind === "all") {
          const res = await runCodingEval(path.resolve(opts.repoRoot as string));
          defaultRuntime.log(
            `coding eval score=${(res.score * 100).toFixed(1)}% (${res.passed}/${res.total})`,
          );
        }
        if (kind === "retrieval" || kind === "all") {
          const res = await runRetrievalEval();
          defaultRuntime.log(
            `retrieval eval score=${(res.score * 100).toFixed(1)}% (${res.passed}/${res.total})`,
          );
        }
      });
    });

  research
    .command("eval-report")
    .description("Show recent eval trend")
    .action(async () => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rows = latestEvalReport();
        if (!rows.length) {
          defaultRuntime.log("No eval runs yet.");
          return;
        }
        rows.forEach((r) => {
          defaultRuntime.log(
            `${new Date(r.created_at).toISOString()} ${r.run_type} score=${(r.score * 100).toFixed(1)}% (${r.passed}/${r.total})`,
          );
        });
      });
    });

  research
    .command("health")
    .description("Check staleness of research data")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const db = openResearchDb(opts.db as string);
        const latestPrice = db.prepare(`SELECT MAX(fetched_at) as ts FROM prices`).get() as {
          ts?: number;
        };
        const latestFiling = db.prepare(`SELECT MAX(fetched_at) as ts FROM filings`).get() as {
          ts?: number;
        };
        const latestFundamentals = db
          .prepare(`SELECT MAX(fetched_at) as ts FROM fundamental_facts`)
          .get() as {
          ts?: number;
        };
        const latestExpectations = db
          .prepare(`SELECT MAX(fetched_at) as ts FROM earnings_expectations`)
          .get() as {
          ts?: number;
        };
        const now = Date.now();
        const priceAgeH = latestPrice.ts ? (now - latestPrice.ts) / 36e5 : Infinity;
        const filingAgeH = latestFiling.ts ? (now - latestFiling.ts) / 36e5 : Infinity;
        const fundamentalsAgeH = latestFundamentals.ts
          ? (now - latestFundamentals.ts) / 36e5
          : Infinity;
        const expectationsAgeH = latestExpectations.ts
          ? (now - latestExpectations.ts) / 36e5
          : Infinity;
        defaultRuntime.log(
          `prices age: ${Number.isFinite(priceAgeH) ? priceAgeH.toFixed(1) : "none"}h`,
        );
        defaultRuntime.log(
          `filings age: ${Number.isFinite(filingAgeH) ? filingAgeH.toFixed(1) : "none"}h`,
        );
        defaultRuntime.log(
          `fundamentals age: ${Number.isFinite(fundamentalsAgeH) ? fundamentalsAgeH.toFixed(1) : "none"}h`,
        );
        defaultRuntime.log(
          `expectations age: ${Number.isFinite(expectationsAgeH) ? expectationsAgeH.toFixed(1) : "none"}h`,
        );
        if (priceAgeH > 48) defaultRuntime.error("WARN: prices stale (>48h)");
        if (filingAgeH > 24 * 14) defaultRuntime.error("WARN: filings stale (>14d)");
        if (fundamentalsAgeH > 24 * 14) defaultRuntime.error("WARN: fundamentals stale (>14d)");
        if (expectationsAgeH > 24 * 14) defaultRuntime.error("WARN: expectations stale (>14d)");
      });
    });

  research
    .command("backup")
    .description("Backup research sqlite db")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .option("--dest <dir>", "Backup destination", path.join(process.cwd(), "data", "backups"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const out = writeBackup({ dbPath: opts.db as string, destDir: opts.dest as string });
        defaultRuntime.log(`Backup created: ${out}`);
      });
    });
}
