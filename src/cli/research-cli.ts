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
import {
  addCatalyst,
  cancelCatalyst,
  getCatalystSummary,
  listCatalysts,
  resolveCatalyst,
} from "../research/catalyst.js";
import { openResearchDb, resolveResearchDbPath } from "../research/db.js";
import {
  latestEvalReport,
  runCodingEval,
  runDecisionEval,
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
import { monitorTicker, monitorTickers } from "../research/monitor.js";
import { computePortfolioPlan } from "../research/portfolio.js";
import { indexRepo } from "../research/repo-index.js";
import { computeValuation, resolveMatureForecasts } from "../research/valuation.js";
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
const parseTickersOption = (value: string): string[] =>
  value
    .split(",")
    .map((ticker) => ticker.trim().toUpperCase())
    .filter(Boolean);

const parseBooleanOption = (value: string): boolean => {
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "y"].includes(normalized)) return true;
  if (["0", "false", "no", "n"].includes(normalized)) return false;
  throw new Error(`Invalid boolean value: ${value}`);
};

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
    .command("valuation")
    .description("Compute base/bull/bear valuation and market-implied expectations")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const valuation = computeValuation({
          ticker: opts.ticker as string,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`ticker=${valuation.ticker}`);
        if (typeof valuation.currentPrice === "number") {
          defaultRuntime.log(`current_price=${valuation.currentPrice.toFixed(2)}`);
          if (valuation.currentPriceDate) {
            defaultRuntime.log(`current_price_date=${valuation.currentPriceDate}`);
          }
        } else {
          defaultRuntime.log("current_price=n/a");
        }
        defaultRuntime.log(`confidence=${valuation.confidence.toFixed(2)}`);
        if (typeof valuation.expectedSharePrice === "number") {
          defaultRuntime.log(`expected_share_price=${valuation.expectedSharePrice.toFixed(2)}`);
        }
        if (typeof valuation.expectedUpsidePct === "number") {
          defaultRuntime.log(
            `expected_upside_pct=${(valuation.expectedUpsidePct * 100).toFixed(1)}%`,
          );
        }
        if (typeof valuation.expectedUpsideWithCatalystsPct === "number") {
          defaultRuntime.log(
            `expected_upside_with_catalysts_pct=${(valuation.expectedUpsideWithCatalystsPct * 100).toFixed(1)}%`,
          );
        }
        if (valuation.catalystSummary) {
          defaultRuntime.log(
            `catalysts_open=${valuation.catalystSummary.openCount} expected_impact_pct=${(valuation.catalystSummary.expectedImpactPct * 100).toFixed(2)}% weighted_confidence=${valuation.catalystSummary.weightedConfidence.toFixed(2)}`,
          );
        }
        valuation.scenarios.forEach((scenario) => {
          defaultRuntime.log(
            `${scenario.name}: growth=${(scenario.revenueGrowth * 100).toFixed(1)}% margin=${(scenario.operatingMargin * 100).toFixed(1)}% wacc=${(scenario.wacc * 100).toFixed(1)}% price=${typeof scenario.impliedSharePrice === "number" ? scenario.impliedSharePrice.toFixed(2) : "n/a"}`,
          );
        });
        if (valuation.impliedExpectations) {
          defaultRuntime.log(`implied_stance=${valuation.impliedExpectations.stance}`);
          defaultRuntime.log(
            `implied_growth=${(valuation.impliedExpectations.impliedRevenueGrowth * 100).toFixed(1)}% model_growth=${(valuation.impliedExpectations.modelRevenueGrowth * 100).toFixed(1)}%`,
          );
          defaultRuntime.log(
            `implied_margin=${(valuation.impliedExpectations.impliedOperatingMargin * 100).toFixed(1)}% model_margin=${(valuation.impliedExpectations.modelOperatingMargin * 100).toFixed(1)}%`,
          );
        }
        if (valuation.notes.length) {
          valuation.notes.forEach((note) => defaultRuntime.error(`NOTE: ${note}`));
        }
      });
    });

  research
    .command("catalyst-add")
    .description("Add a thesis catalyst with expected probability/impact")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .requiredOption("--name <text>", "Catalyst name")
    .requiredOption("--probability <n>", "Probability [0-1]")
    .requiredOption("--impact-bps <n>", "Expected impact in basis points (+/-)")
    .option("--confidence <n>", "Confidence [0-1]", "0.6")
    .option("--category <text>", "Category tag", "company")
    .option("--start <date>", "Window start date (YYYY-MM-DD)")
    .option("--end <date>", "Window end date (YYYY-MM-DD)")
    .option("--direction <kind>", "up|down|both", "both")
    .option("--source <text>", "Source provenance", "manual")
    .option("--notes <text>", "Optional notes", "")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const probability = Number.parseFloat(opts.probability as string);
        const impactBps = Number.parseFloat(opts["impactBps"] as string);
        const confidence = Number.parseFloat(opts.confidence as string);
        if (
          !Number.isFinite(probability) ||
          !Number.isFinite(impactBps) ||
          !Number.isFinite(confidence)
        ) {
          throw new Error("probability, impact-bps, and confidence must be numeric");
        }
        const direction = String(opts.direction ?? "both").toLowerCase();
        if (!["up", "down", "both"].includes(direction)) {
          throw new Error("direction must be one of: up, down, both");
        }
        const id = addCatalyst({
          ticker: opts.ticker as string,
          name: opts.name as string,
          probability,
          impactBps,
          confidence,
          category: opts.category as string,
          dateWindowStart: opts.start as string | undefined,
          dateWindowEnd: opts.end as string | undefined,
          direction: direction as "up" | "down" | "both",
          source: opts.source as string,
          notes: opts.notes as string,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`catalyst_id=${id}`);
      });
    });

  research
    .command("catalyst-list")
    .description("List catalysts for a ticker")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--status <kind>", "open|resolved|cancelled|all", "open")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const status = String(opts.status ?? "open").toLowerCase();
        if (!["open", "resolved", "cancelled", "all"].includes(status)) {
          throw new Error("status must be one of: open, resolved, cancelled, all");
        }
        const rows = listCatalysts({
          ticker: opts.ticker as string,
          status: status as "open" | "resolved" | "cancelled" | "all",
          dbPath: opts.db as string,
        });
        if (!rows.length) {
          defaultRuntime.log("No catalysts found.");
          return;
        }
        rows.forEach((row) => {
          defaultRuntime.log(
            `id=${row.id} status=${row.status} category=${row.category} name=${row.name}`,
          );
          defaultRuntime.log(
            `  probability=${row.probability.toFixed(2)} impact_bps=${row.impactBps.toFixed(0)} confidence=${row.confidence.toFixed(2)} direction=${row.direction}`,
          );
          defaultRuntime.log(
            `  window_start=${row.dateWindowStart || "n/a"} window_end=${row.dateWindowEnd || "n/a"} source=${row.source}`,
          );
          if (row.notes) defaultRuntime.log(`  notes=${row.notes}`);
        });
      });
    });

  research
    .command("catalyst-resolve")
    .description("Resolve a catalyst as occurred/not-occurred")
    .requiredOption("--catalyst-id <id>", "Catalyst id")
    .option("--occurred <bool>", "true|false", "true")
    .option("--impact-bps <n>", "Realized impact bps")
    .option("--notes <text>", "Resolution notes", "")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const catalystId = Number.parseInt(opts["catalystId"] as string, 10);
        const occurred = parseBooleanOption(opts.occurred as string);
        const realizedImpactBps =
          opts["impactBps"] === undefined
            ? undefined
            : Number.parseFloat(opts["impactBps"] as string);
        if (!Number.isFinite(catalystId)) throw new Error("catalyst-id must be an integer");
        if (opts["impactBps"] !== undefined && !Number.isFinite(realizedImpactBps)) {
          throw new Error("impact-bps must be numeric");
        }
        resolveCatalyst({
          catalystId,
          occurred,
          realizedImpactBps,
          notes: opts.notes as string,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`resolved_catalyst_id=${catalystId}`);
      });
    });

  research
    .command("catalyst-cancel")
    .description("Cancel an open catalyst")
    .requiredOption("--catalyst-id <id>", "Catalyst id")
    .option("--reason <text>", "Cancellation reason", "")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const catalystId = Number.parseInt(opts["catalystId"] as string, 10);
        if (!Number.isFinite(catalystId)) throw new Error("catalyst-id must be an integer");
        cancelCatalyst({
          catalystId,
          reason: opts.reason as string,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`cancelled_catalyst_id=${catalystId}`);
      });
    });

  research
    .command("catalyst-score")
    .description("Show aggregate catalyst score for a ticker")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const summary = getCatalystSummary({
          ticker: opts.ticker as string,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`ticker=${summary.ticker}`);
        defaultRuntime.log(`open_count=${summary.openCount}`);
        defaultRuntime.log(`expected_impact_bps=${summary.expectedImpactBps.toFixed(1)}`);
        defaultRuntime.log(`expected_impact_pct=${(summary.expectedImpactPct * 100).toFixed(2)}%`);
        defaultRuntime.log(`weighted_confidence=${summary.weightedConfidence.toFixed(2)}`);
        defaultRuntime.log(`high_impact_count=${summary.highImpactCount}`);
        if (summary.nearestCatalystDate) {
          defaultRuntime.log(`nearest_date=${summary.nearestCatalystDate}`);
        }
      });
    });

  research
    .command("position")
    .description("Compute portfolio sizing and risk plan for a ticker")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const plan = computePortfolioPlan({
          ticker: opts.ticker as string,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`ticker=${plan.ticker}`);
        defaultRuntime.log(`stance=${plan.stance}`);
        defaultRuntime.log(`confidence=${plan.confidence.toFixed(2)}`);
        if (typeof plan.expectedUpsidePct === "number") {
          defaultRuntime.log(`expected_upside_pct=${(plan.expectedUpsidePct * 100).toFixed(1)}%`);
        }
        defaultRuntime.log(`recommended_weight_pct=${plan.recommendedWeightPct.toFixed(2)}`);
        defaultRuntime.log(`max_risk_budget_pct=${plan.maxRiskBudgetPct.toFixed(2)}`);
        defaultRuntime.log(`stop_loss_pct=${(plan.stopLossPct * 100).toFixed(1)}%`);
        defaultRuntime.log(`horizon_days=${plan.timeHorizonDays}`);
        defaultRuntime.log(
          `catalyst_expected_impact_pct=${(plan.catalystExpectedImpactPct * 100).toFixed(2)}%`,
        );
        plan.reviewTriggers.forEach((trigger) => defaultRuntime.log(`trigger=${trigger}`));
      });
    });

  research
    .command("monitor")
    .description("Run thesis monitoring checks for one ticker and persist alerts")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = monitorTicker({
          ticker: opts.ticker as string,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `ticker=${result.ticker} alerts=${result.alerts.length} persisted=${result.persisted}`,
        );
        result.alerts.forEach((alert) => {
          defaultRuntime.log(
            `${alert.severity.toUpperCase()} ${alert.alertType}: ${alert.message} (${alert.details})`,
          );
        });
      });
    });

  research
    .command("monitor-all")
    .description("Run thesis monitoring checks for multiple tickers")
    .option("--tickers <csv>", "Ticker list (defaults to RESEARCH_TICKERS)")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const tickers =
          typeof opts.tickers === "string" && opts.tickers.trim()
            ? parseTickersOption(opts.tickers as string)
            : parseTickersOption(process.env.RESEARCH_TICKERS ?? "");
        if (!tickers.length) {
          throw new Error("Provide --tickers or set RESEARCH_TICKERS");
        }
        const results = monitorTickers({
          tickers,
          dbPath: opts.db as string,
        });
        const totalAlerts = results.reduce((sumValue, row) => sumValue + row.alerts.length, 0);
        defaultRuntime.log(`tickers=${results.length} alerts=${totalAlerts}`);
        results.forEach((result) => {
          defaultRuntime.log(
            `ticker=${result.ticker} alerts=${result.alerts.length} persisted=${result.persisted}`,
          );
        });
      });
    });

  research
    .command("forecast-sync")
    .description("Resolve matured valuation forecasts against realized prices")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const outcome = resolveMatureForecasts({ dbPath: opts.db as string });
        defaultRuntime.log(
          `forecast_unresolved_scanned=${outcome.unresolvedCount} forecast_resolved_now=${outcome.resolvedNow}`,
        );
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
    .requiredOption("--type <kind>", "finance|coding|retrieval|decision|all")
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
        if (kind === "decision" || kind === "all") {
          const res = await runDecisionEval();
          defaultRuntime.log(
            `decision eval score=${(res.score * 100).toFixed(1)}% (${res.passed}/${res.total})`,
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
