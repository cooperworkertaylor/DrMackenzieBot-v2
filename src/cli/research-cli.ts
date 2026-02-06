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
  applyBenchmarkGovernance,
  benchmarkReport,
  listBenchmarkCases,
  listBenchmarkSuites,
  runAllBenchmarksWithGovernance,
  runBenchmarkReplay,
  upsertBenchmarkCase,
  upsertBenchmarkSuite,
} from "../research/benchmark.js";
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
  executionTraceReport,
  logExecutionTrace,
  type ExecutionTraceStepInput,
} from "../research/execution-trace.js";
import {
  ingestFilings,
  ingestExpectations,
  ingestFundamentals,
  ingestPrices,
  ingestTranscript,
} from "../research/ingest.js";
import { learningReport, logTaskOutcome, runLearningCalibration } from "../research/learning.js";
import { generateMemoAsync } from "../research/memo.js";
import { listEntityClaims, updateClaimStatus } from "../research/memory-graph.js";
import { monitorTicker, monitorTickers } from "../research/monitor.js";
import {
  listPolicyVariants,
  policyPerformanceReport,
  registerPolicyVariant,
  routePolicyAssignment,
  runPolicyGovernance,
} from "../research/policy.js";
import { computePortfolioPlan } from "../research/portfolio.js";
import { provenanceReport } from "../research/provenance.js";
import { indexRepo } from "../research/repo-index.js";
import { runResearchSecurityAudit } from "../research/security.js";
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

const parseSourceMixOption = (value: string): Record<string, number> => {
  const out: Record<string, number> = {};
  value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .forEach((entry) => {
      const [sourceRaw, weightRaw] = entry.split(":", 2);
      const source = sourceRaw?.trim() ?? "";
      const weight = Number.parseFloat((weightRaw ?? "").trim());
      if (!source || !Number.isFinite(weight) || weight <= 0) return;
      out[source] = weight;
    });
  return out;
};

const parseOptionalNumber = (value: unknown): number | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string" || !value.trim()) return undefined;
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : undefined;
};

const parseOptionalJsonObject = (value: unknown): Record<string, unknown> | undefined => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return undefined;
    return parsed as Record<string, unknown>;
  } catch {
    throw new Error("Invalid JSON object");
  }
};

const parseOptionalJsonArray = (value: unknown): unknown[] | undefined => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!Array.isArray(parsed)) throw new Error("Invalid JSON array");
    return parsed;
  } catch {
    throw new Error("Invalid JSON array");
  }
};

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
    .command("learn-log")
    .description("Log or update a task outcome for learning-loop grading")
    .option("--id <n>", "Existing task outcome id to update")
    .option("--task-type <kind>", "investment|coding|other")
    .option("--archetype <name>", "Task archetype (e.g., sector-deep-dive, bugfix-hotpath)")
    .option("--policy <name>", "Policy/variant name used for this run")
    .option("--policy-role <kind>", "primary|shadow", "primary")
    .option("--experiment-group <name>", "Experiment group identifier")
    .option("--ticker <symbol>", "Ticker (investment tasks)")
    .option("--repo-root <path>", "Repo path (coding tasks)")
    .option("--input <text>", "Input summary")
    .option("--output <text>", "Output text (used for output hash if hash not passed)")
    .option("--output-hash <hex>", "Precomputed output hash")
    .option("--confidence <n>", "Predicted confidence [0-1]")
    .option("--citations <n>", "Citation count")
    .option("--latency-ms <n>", "Latency in milliseconds")
    .option("--user-score <n>", "User quality score [0-1]")
    .option("--realized-score <n>", "Realized outcome score [0-1]")
    .option("--outcome <label>", "Outcome label/status note")
    .option("--source-mix <csv>", "Source usage like filings:3,transcripts:2,code:5")
    .option("--contradictions <n>", "Investment metric: contradiction count")
    .option("--falsification-count <n>", "Investment metric: falsification trigger count")
    .option("--calibration-error <n>", "Investment metric: calibration error [0-1]")
    .option("--tests-pass-rate <n>", "Coding metric: test pass rate [0-1]")
    .option("--regressions <n>", "Coding metric: post-merge regressions")
    .option("--review-findings <n>", "Coding metric: review findings count")
    .option("--rollback-rate <n>", "Coding metric: rollback rate [0-1]")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const gradingMetrics: Record<string, number> = {};
        const contradictions = parseOptionalNumber(opts.contradictions);
        const falsificationCount = parseOptionalNumber(opts["falsificationCount"]);
        const calibrationError = parseOptionalNumber(opts["calibrationError"]);
        const testsPassRate = parseOptionalNumber(opts["testsPassRate"]);
        const regressions = parseOptionalNumber(opts.regressions);
        const reviewFindings = parseOptionalNumber(opts["reviewFindings"]);
        const rollbackRate = parseOptionalNumber(opts["rollbackRate"]);
        if (typeof contradictions === "number") gradingMetrics.contradictions = contradictions;
        if (typeof falsificationCount === "number") {
          gradingMetrics.falsification_count = falsificationCount;
        }
        if (typeof calibrationError === "number") {
          gradingMetrics.calibration_error = calibrationError;
        }
        if (typeof testsPassRate === "number") gradingMetrics.tests_pass_rate = testsPassRate;
        if (typeof regressions === "number") gradingMetrics.regressions = regressions;
        if (typeof reviewFindings === "number") gradingMetrics.review_findings = reviewFindings;
        if (typeof rollbackRate === "number") gradingMetrics.rollback_rate = rollbackRate;

        const id = parseOptionalNumber(opts.id);
        const result = logTaskOutcome({
          id: typeof id === "number" ? Math.round(id) : undefined,
          taskType: opts["taskType"] as string | undefined,
          taskArchetype: opts.archetype as string | undefined,
          policyName: opts.policy as string | undefined,
          policyRole: opts["policyRole"] as string | undefined,
          experimentGroup: opts["experimentGroup"] as string | undefined,
          ticker: opts.ticker as string | undefined,
          repoRoot: opts["repoRoot"] as string | undefined,
          inputSummary: opts.input as string | undefined,
          outputText: opts.output as string | undefined,
          outputHash: opts["outputHash"] as string | undefined,
          confidence: parseOptionalNumber(opts.confidence),
          citationCount: parseOptionalNumber(opts.citations),
          latencyMs: parseOptionalNumber(opts["latencyMs"]),
          userScore: parseOptionalNumber(opts["userScore"]),
          realizedOutcomeScore: parseOptionalNumber(opts["realizedScore"]),
          outcomeLabel: opts.outcome as string | undefined,
          sourceMix:
            typeof opts["sourceMix"] === "string" && opts["sourceMix"].trim()
              ? parseSourceMixOption(opts["sourceMix"] as string)
              : undefined,
          gradingMetrics,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`id=${result.id}`);
        defaultRuntime.log(`task_type=${result.taskType}`);
        defaultRuntime.log(`grader_score=${result.graderScore.toFixed(3)}`);
        defaultRuntime.log(`status=${result.status}`);
        defaultRuntime.log(`status_reason=${result.statusReason}`);
        defaultRuntime.log(`output_hash=${result.outputHash}`);
      });
    });

  research
    .command("learn-report")
    .description("Report learning-loop quality, routing, and calibration metrics")
    .option("--days <n>", "Lookback window in days", "30")
    .option("--task-type <kind>", "investment|coding|other")
    .option("--min-samples <n>", "Minimum samples for archetype routing", "3")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const report = learningReport({
          days: Number.parseInt(opts.days as string, 10) || 30,
          taskType: opts["taskType"] as string | undefined,
          minSamples: Number.parseInt(opts["minSamples"] as string, 10) || 3,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `learning window=${report.lookbackDays}d tasks=${report.totalTasks} avg_score=${typeof report.avgGraderScore === "number" ? report.avgGraderScore.toFixed(3) : "n/a"}`,
        );
        if (typeof report.trustedRate === "number") {
          defaultRuntime.log(`trusted_rate=${(report.trustedRate * 100).toFixed(1)}%`);
        }
        if (typeof report.quarantineRate === "number") {
          defaultRuntime.log(`quarantine_rate=${(report.quarantineRate * 100).toFixed(1)}%`);
        }
        report.byTaskType.forEach((row) => {
          defaultRuntime.log(
            `type=${row.taskType} count=${row.count} avg=${row.avgScore.toFixed(3)} trusted=${(row.trustedRate * 100).toFixed(1)}% quarantine=${(row.quarantineRate * 100).toFixed(1)}% win=${(row.winRate * 100).toFixed(1)}%`,
          );
        });
        report.routing.forEach((row) => {
          defaultRuntime.log(
            `routing type=${row.taskType} archetype=${row.bestArchetype ?? "n/a"} samples=${row.archetypeSampleCount ?? 0} win=${typeof row.archetypeWinRate === "number" ? `${(row.archetypeWinRate * 100).toFixed(1)}%` : "n/a"} avg=${typeof row.archetypeAvgScore === "number" ? row.archetypeAvgScore.toFixed(3) : "n/a"}`,
          );
          if (row.topSources.length) {
            defaultRuntime.log(
              `routing_sources ${row.taskType}: ${row.topSources
                .map((entry) => `${entry.source}:${entry.score.toFixed(3)}`)
                .join(", ")}`,
            );
          }
        });
        if (report.sourceEffectiveness.length) {
          defaultRuntime.log(
            `source_effectiveness: ${report.sourceEffectiveness
              .map((entry) => `${entry.source}:${entry.score.toFixed(3)}`)
              .join(", ")}`,
          );
        }
        defaultRuntime.log(
          `learning_dynamics outcome_coverage=${(report.learningDynamics.outcomeCoverageRate * 100).toFixed(1)}% pending=${(report.learningDynamics.pendingOutcomeRate * 100).toFixed(1)}% delayed_feedback=${(report.learningDynamics.delayedFeedbackRate * 100).toFixed(1)}% low_confidence=${(report.learningDynamics.lowConfidenceRate * 100).toFixed(1)}%`,
        );
        if (typeof report.learningDynamics.avgFeedbackLagDays === "number") {
          defaultRuntime.log(
            `learning_feedback_lag_days=${report.learningDynamics.avgFeedbackLagDays.toFixed(2)}`,
          );
        }
        if (typeof report.learningDynamics.confidenceCalibrationMae === "number") {
          defaultRuntime.log(
            `learning_confidence_mae=${report.learningDynamics.confidenceCalibrationMae.toFixed(3)}`,
          );
        }
        if (report.rewardModels.length) {
          report.rewardModels.slice(0, 10).forEach((model) => {
            defaultRuntime.log(
              `reward_model type=${model.taskType} archetype=${model.taskArchetype} policy=${model.policyName} reward=${model.reward.toFixed(3)} samples=${model.samples} outcome=${model.avgOutcome.toFixed(3)} trust=${(model.trustRate * 100).toFixed(1)}% quarantine=${(model.quarantineRate * 100).toFixed(1)}%`,
            );
          });
        }
        defaultRuntime.log(
          `calibration forecast_samples=${report.calibration.forecastSampleCount} forecast_mae=${typeof report.calibration.forecastMae === "number" ? report.calibration.forecastMae.toFixed(3) : "n/a"} directional=${typeof report.calibration.forecastDirectionalAccuracy === "number" ? report.calibration.forecastDirectionalAccuracy.toFixed(3) : "n/a"}`,
        );
        defaultRuntime.log(
          `calibration catalyst_samples=${report.calibration.catalystSampleCount} catalyst_brier=${typeof report.calibration.catalystBrier === "number" ? report.calibration.catalystBrier.toFixed(3) : "n/a"} catalyst_impact_mae_bps=${typeof report.calibration.catalystImpactMaeBps === "number" ? report.calibration.catalystImpactMaeBps.toFixed(1) : "n/a"} open_high_alerts=${report.calibration.openHighAlerts}`,
        );
      });
    });

  research
    .command("trace-log")
    .description("Log execution trace telemetry for reliability benchmarking")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .requiredOption("--policy <name>", "Policy name")
    .option("--archetype <name>", "Task archetype", "")
    .option("--policy-role <kind>", "primary|shadow", "primary")
    .option("--experiment-group <name>", "Experiment group id")
    .option("--ticker <symbol>", "Ticker")
    .option("--repo-root <path>", "Repo root")
    .option("--seed <text>", "Deterministic seed")
    .option("--trace-hash <hex>", "Precomputed trace hash")
    .option("--output <text>", "Output text to hash for reproducibility")
    .option("--output-hash <hex>", "Precomputed output hash")
    .option("--success <bool>", "true|false")
    .option("--retries <n>", "Retry count")
    .option("--errors <n>", "Error count")
    .option("--timeouts <n>", "Timeout count")
    .option("--latency-ms <n>", "Total latency in milliseconds")
    .option("--started-at <ms>", "Start timestamp (epoch ms)")
    .option("--completed-at <ms>", "Completed timestamp (epoch ms)")
    .option("--steps <json>", "JSON array of trace steps")
    .option("--metadata <json>", "JSON metadata object")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const parsedSteps = (parseOptionalJsonArray(opts.steps) ?? []).map((raw) => {
          if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
            throw new Error("Each step must be a JSON object.");
          }
          const step = raw as Record<string, unknown>;
          if (typeof step.toolName !== "string" || !step.toolName.trim()) {
            throw new Error("Each step requires non-empty toolName.");
          }
          const normalized: ExecutionTraceStepInput = {
            seq: parseOptionalNumber(step.seq),
            toolName: step.toolName,
            action: typeof step.action === "string" ? step.action : undefined,
            status: typeof step.status === "string" ? step.status : undefined,
            latencyMs: parseOptionalNumber(step.latencyMs),
            retries: parseOptionalNumber(step.retries),
            errorType: typeof step.errorType === "string" ? step.errorType : undefined,
            inputHash: typeof step.inputHash === "string" ? step.inputHash : undefined,
            outputHash: typeof step.outputHash === "string" ? step.outputHash : undefined,
            details:
              step.details && typeof step.details === "object" && !Array.isArray(step.details)
                ? (step.details as Record<string, unknown>)
                : undefined,
          };
          return normalized;
        });

        const success =
          typeof opts.success === "string" ? parseBooleanOption(opts.success as string) : undefined;
        const trace = logExecutionTrace({
          taskType: opts["taskType"] as string,
          taskArchetype: opts.archetype as string,
          policyName: opts.policy as string,
          policyRole: opts["policyRole"] as string,
          experimentGroup: opts["experimentGroup"] as string | undefined,
          ticker: opts.ticker as string | undefined,
          repoRoot: opts["repoRoot"] as string | undefined,
          seed: opts.seed as string | undefined,
          traceHash: opts["traceHash"] as string | undefined,
          outputText: opts.output as string | undefined,
          outputHash: opts["outputHash"] as string | undefined,
          success,
          retryCount: parseOptionalNumber(opts.retries),
          errorCount: parseOptionalNumber(opts.errors),
          timeoutCount: parseOptionalNumber(opts.timeouts),
          totalLatencyMs: parseOptionalNumber(opts["latencyMs"]),
          startedAt: parseOptionalNumber(opts["startedAt"]),
          completedAt: parseOptionalNumber(opts["completedAt"]),
          steps: parsedSteps,
          metadata: parseOptionalJsonObject(opts.metadata),
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `trace_id=${trace.id} task_type=${trace.taskType} policy=${trace.policyName} success=${trace.success ? 1 : 0} steps=${trace.stepCount} retries=${trace.retryCount} timeouts=${trace.timeoutCount}`,
        );
        defaultRuntime.log(`trace_hash=${trace.traceHash}`);
        if (trace.outputHash) defaultRuntime.log(`output_hash=${trace.outputHash}`);
      });
    });

  research
    .command("trace-report")
    .description("Show execution trace reliability metrics for a policy")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .requiredOption("--policy <name>", "Policy name")
    .option("--archetype <name>", "Task archetype", "")
    .option("--ticker <symbol>", "Ticker filter")
    .option("--repo-root <path>", "Repo filter")
    .option("--lookback-days <n>", "Lookback days", "90")
    .option("--seed-limit <n>", "Max seeds to display", "20")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const report = executionTraceReport({
          taskType: opts["taskType"] as string,
          taskArchetype: opts.archetype as string,
          policyName: opts.policy as string,
          ticker: opts.ticker as string | undefined,
          repoRoot: opts["repoRoot"] as string | undefined,
          lookbackDays: Number.parseInt(opts["lookbackDays"] as string, 10) || 90,
          seedLimit: Number.parseInt(opts["seedLimit"] as string, 10) || 20,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `trace_report task_type=${report.taskType} policy=${report.policyName} archetype=${report.taskArchetype || "default"} traces=${report.summary.traceCount} completion=${(report.summary.completionRate * 100).toFixed(1)}% timeout=${(report.summary.timeoutRate * 100).toFixed(1)}% reproducibility=${(report.summary.reproducibilityScore * 100).toFixed(1)}% retries=${report.summary.avgRetries.toFixed(2)}`,
        );
        if (typeof report.summary.avgLatencyMs === "number") {
          defaultRuntime.log(`avg_latency_ms=${report.summary.avgLatencyMs.toFixed(1)}`);
        }
        if (report.seedStats.length) {
          report.seedStats.forEach((seed) => {
            defaultRuntime.log(
              `seed=${seed.seed} runs=${seed.runCount} stable=${(seed.stableRatio * 100).toFixed(1)}% latest=${new Date(seed.latestAt).toISOString()}`,
            );
          });
        }
      });
    });

  research
    .command("memory-claims")
    .description("List structured memory-graph claims and evidence counts")
    .option("--ticker <symbol>", "Ticker filter")
    .option("--entity <name>", "Entity name filter")
    .option("--status <value>", "active|draft|contested|retired")
    .option("--limit <n>", "Result limit", "100")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rows = listEntityClaims({
          ticker: opts.ticker as string | undefined,
          entityName: opts.entity as string | undefined,
          status: opts.status as string | undefined,
          limit: Number.parseInt(opts.limit as string, 10) || 100,
          dbPath: opts.db as string,
        });
        if (!rows.length) {
          defaultRuntime.log("No claims found.");
          return;
        }
        rows.forEach((row) => {
          defaultRuntime.log(
            `claim_id=${row.claim.id} entity=${row.entity.canonicalName} ticker=${row.entity.ticker || "n/a"} status=${row.claim.status} confidence=${row.claim.confidence.toFixed(2)} evidence=${row.evidenceCount}`,
          );
          defaultRuntime.log(`  claim=${row.claim.claimText}`);
        });
      });
    });

  research
    .command("claim-status")
    .description("Update a memory-graph claim status")
    .requiredOption("--claim-id <id>", "Claim id")
    .requiredOption("--status <value>", "active|draft|contested|retired")
    .option("--reason <text>", "Status update reason", "")
    .option("--confidence <n>", "Optional confidence [0-1]")
    .option("--metadata <json>", "JSON metadata")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const claim = updateClaimStatus({
          claimId: Number.parseInt(opts["claimId"] as string, 10),
          status: opts.status as string,
          reason: opts.reason as string,
          confidence: parseOptionalNumber(opts.confidence),
          metadata: parseOptionalJsonObject(opts.metadata),
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `claim_id=${claim.id} status=${claim.status} confidence=${claim.confidence.toFixed(2)}`,
        );
      });
    });

  research
    .command("provenance-report")
    .description("Audit tamper-evident provenance chain and signatures")
    .option("--event-type <name>", "Filter by event type")
    .option("--entity-type <name>", "Filter by entity type")
    .option("--entity-id <id>", "Filter by entity id")
    .option("--limit <n>", "Max events", "200")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const report = provenanceReport({
          eventType: opts["eventType"] as string | undefined,
          entityType: opts["entityType"] as string | undefined,
          entityId: opts["entityId"] as string | undefined,
          limit: Number.parseInt(opts.limit as string, 10) || 200,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `provenance total=${report.totalEvents} chain_valid=${report.chainValid ? 1 : 0} signature_coverage=${(report.signatureCoverage * 100).toFixed(1)}% signature_valid=${typeof report.signatureValidRate === "number" ? `${(report.signatureValidRate * 100).toFixed(1)}%` : "n/a"}`,
        );
        report.issues.slice(0, 20).forEach((issue) => defaultRuntime.error(`ISSUE: ${issue}`));
      });
    });

  research
    .command("security-audit")
    .description("Run research-stack security baseline checks")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const report = runResearchSecurityAudit({
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `security_audit pass=${report.passCount} warn=${report.warnCount} fail=${report.failCount} generated=${report.generatedAt}`,
        );
        report.controls.forEach((control) => {
          const prefix = control.status.toUpperCase();
          if (control.status === "fail") {
            defaultRuntime.error(`${prefix} ${control.id}: ${control.detail}`);
          } else {
            defaultRuntime.log(`${prefix} ${control.id}: ${control.detail}`);
          }
        });
      });
    });

  research
    .command("learn-calibrate")
    .description("Run learning calibration cycle (forecast sync + regrade + report)")
    .option("--days <n>", "Lookback window for report", "90")
    .option("--min-samples <n>", "Minimum samples for routing", "3")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = runLearningCalibration({
          days: Number.parseInt(opts.days as string, 10) || 90,
          minSamples: Number.parseInt(opts["minSamples"] as string, 10) || 3,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `forecast_unresolved_scanned=${result.forecastResolution.unresolvedCount} forecast_resolved_now=${result.forecastResolution.resolvedNow}`,
        );
        defaultRuntime.log(
          `regrade_scanned=${result.refresh.scanned} regrade_updated=${result.refresh.updated}`,
        );
        defaultRuntime.log(
          `learning tasks=${result.report.totalTasks} avg_score=${typeof result.report.avgGraderScore === "number" ? result.report.avgGraderScore.toFixed(3) : "n/a"} trusted_rate=${typeof result.report.trustedRate === "number" ? `${(result.report.trustedRate * 100).toFixed(1)}%` : "n/a"}`,
        );
      });
    });

  research
    .command("policy-register")
    .description("Register or update a champion/challenger policy variant")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .option("--archetype <name>", "Task archetype", "")
    .requiredOption("--policy <name>", "Policy/variant name")
    .option("--status <kind>", "champion|challenger|retired", "challenger")
    .option("--active <bool>", "true|false", "true")
    .option("--traffic <n>", "Primary traffic weight [0-1]")
    .option("--shadow <n>", "Shadow traffic weight [0-1]")
    .option("--min-samples <n>", "Minimum samples for promotion", "25")
    .option("--min-lift <n>", "Required score lift for promotion", "0.03")
    .option("--max-quarantine-rate <n>", "Guardrail quarantine rate [0-1]", "0.2")
    .option("--max-calibration-error <n>", "Guardrail calibration error [0-1]", "0.25")
    .option("--metadata <json>", "JSON metadata for prompt/retrieval/workflow profile")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const variant = registerPolicyVariant({
          taskType: opts["taskType"] as string,
          taskArchetype: opts.archetype as string,
          policyName: opts.policy as string,
          status: opts.status as "champion" | "challenger" | "retired",
          active: parseBooleanOption(opts.active as string),
          trafficWeight: parseOptionalNumber(opts.traffic),
          shadowWeight: parseOptionalNumber(opts.shadow),
          minSamples: Number.parseInt(opts["minSamples"] as string, 10) || 25,
          minLift: parseOptionalNumber(opts["minLift"]),
          maxQuarantineRate: parseOptionalNumber(opts["maxQuarantineRate"]),
          maxCalibrationError: parseOptionalNumber(opts["maxCalibrationError"]),
          metadata: parseOptionalJsonObject(opts.metadata),
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `policy=${variant.policyName} status=${variant.status} active=${variant.active ? 1 : 0} traffic=${variant.trafficWeight.toFixed(2)} shadow=${variant.shadowWeight.toFixed(2)}`,
        );
      });
    });

  research
    .command("policy-list")
    .description("List policy variants for a task type/archetype")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .option("--archetype <name>", "Task archetype", "")
    .option("--include-retired", "Include retired variants", false)
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const variants = listPolicyVariants({
          taskType: opts["taskType"] as string,
          taskArchetype: opts.archetype as string,
          includeRetired: Boolean(opts["includeRetired"]),
          dbPath: opts.db as string,
        });
        if (!variants.length) {
          defaultRuntime.log("No policy variants found.");
          return;
        }
        variants.forEach((variant) => {
          defaultRuntime.log(
            `policy=${variant.policyName} status=${variant.status} active=${variant.active ? 1 : 0} traffic=${variant.trafficWeight.toFixed(2)} shadow=${variant.shadowWeight.toFixed(2)} min_samples=${variant.minSamples} min_lift=${variant.minLift.toFixed(3)}`,
          );
        });
      });
    });

  research
    .command("policy-route")
    .description("Route a task to primary + shadow policy variants")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .option("--archetype <name>", "Task archetype", "")
    .option("--seed <value>", "Deterministic routing seed")
    .option("--exploration-rate <n>", "Exploration rate [0-1]", "0.15")
    .option("--max-shadows <n>", "Max shadow variants", "2")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const route = routePolicyAssignment({
          taskType: opts["taskType"] as string,
          taskArchetype: opts.archetype as string,
          seed: opts.seed as string | undefined,
          explorationRate: parseOptionalNumber(opts["explorationRate"]) ?? 0.15,
          maxShadows: Number.parseInt(opts["maxShadows"] as string, 10) || 2,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`experiment_group=${route.experimentGroup}`);
        defaultRuntime.log(
          `dynamic_exploration_rate=${route.dynamicExplorationRate.toFixed(3)} uncertainty=${route.uncertaintyScore.toFixed(3)}`,
        );
        defaultRuntime.log(`primary=${route.primary?.policyName ?? "none"}`);
        if (route.shadows.length) {
          defaultRuntime.log(
            `shadows=${route.shadows.map((variant) => variant.policyName).join(",")}`,
          );
        } else {
          defaultRuntime.log("shadows=none");
        }
      });
    });

  research
    .command("policy-report")
    .description("Show champion/challenger performance report")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .option("--archetype <name>", "Task archetype", "")
    .option("--days <n>", "Lookback window in days", "60")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const report = policyPerformanceReport({
          taskType: opts["taskType"] as string,
          taskArchetype: opts.archetype as string,
          days: Number.parseInt(opts.days as string, 10) || 60,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `policy_report task_type=${report.taskType} archetype=${report.taskArchetype || "default"} champion=${report.champion ?? "none"} lookback_days=${report.lookbackDays}`,
        );
        report.variants.forEach((variant) => {
          defaultRuntime.log(
            `policy=${variant.policyName} status=${variant.status} samples=${variant.sampleCount} primary=${variant.primarySampleCount} shadow=${variant.shadowSampleCount} score=${typeof variant.score === "number" ? variant.score.toFixed(3) : "n/a"} win=${typeof variant.winRate === "number" ? `${(variant.winRate * 100).toFixed(1)}%` : "n/a"} quarantine=${typeof variant.quarantineRate === "number" ? `${(variant.quarantineRate * 100).toFixed(1)}%` : "n/a"} calibration=${typeof variant.calibrationError === "number" ? variant.calibrationError.toFixed(3) : "n/a"}`,
          );
        });
        if (report.recentDecisions.length) {
          report.recentDecisions.slice(0, 10).forEach((decision) => {
            defaultRuntime.log(
              `${new Date(decision.createdAt).toISOString()} decision=${decision.decisionType} before=${decision.championBefore || "none"} after=${decision.championAfter || "none"} challenger=${decision.challenger || "none"} reason=${decision.reason}`,
            );
          });
        }
      });
    });

  research
    .command("policy-govern")
    .description("Run automatic champion/challenger promotion and rollback governance")
    .option("--task-type <kind>", "investment|coding|other")
    .option("--archetype <name>", "Task archetype", "")
    .option("--days <n>", "Baseline lookback days", "60")
    .option("--recent-days <n>", "Recent degradation window days", "14")
    .option("--min-samples <n>", "Minimum sample count", "25")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = runPolicyGovernance({
          taskType: opts["taskType"] as string | undefined,
          taskArchetype: opts.archetype as string,
          days: Number.parseInt(opts.days as string, 10) || 60,
          recentDays: Number.parseInt(opts["recentDays"] as string, 10) || 14,
          minSamples: Number.parseInt(opts["minSamples"] as string, 10) || 25,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `governance promoted=${result.promoted} rolled_back=${result.rolledBack} held=${result.held}`,
        );
        result.decisions.slice(0, 20).forEach((decision) => {
          defaultRuntime.log(
            `${new Date(decision.createdAt).toISOString()} decision=${decision.decisionType} task=${decision.taskType}:${decision.taskArchetype || "default"} before=${decision.championBefore || "none"} after=${decision.championAfter || "none"} reason=${decision.reason}`,
          );
        });
      });
    });

  research
    .command("benchmark-suite-upsert")
    .description("Create or update a benchmark suite")
    .requiredOption("--name <text>", "Suite name")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .option("--archetype <name>", "Task archetype", "")
    .option("--description <text>", "Suite description", "")
    .option("--active <bool>", "true|false", "true")
    .option("--gating-min-samples <n>", "Promotion gate minimum samples", "25")
    .option("--gating-min-lift <n>", "Promotion gate minimum score lift", "0.03")
    .option("--gating-max-risk-breaches <n>", "Promotion gate max risk breaches", "0")
    .option("--canary-drop-threshold <n>", "Canary rollback threshold [0-1]", "0.07")
    .option("--reliability-min-completion <n>", "Reliability min completion rate [0-1]", "0.9")
    .option("--reliability-max-timeout-rate <n>", "Reliability max timeout rate [0-1]", "0.1")
    .option(
      "--reliability-min-reproducibility <n>",
      "Reliability min reproducibility score [0-1]",
      "0.8",
    )
    .option("--reliability-max-avg-retries <n>", "Reliability max average retries", "1.5")
    .option("--metadata <json>", "JSON metadata")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const suite = upsertBenchmarkSuite({
          name: opts.name as string,
          taskType: opts["taskType"] as string,
          taskArchetype: opts.archetype as string,
          description: opts.description as string,
          active: parseBooleanOption(opts.active as string),
          gatingMinSamples: Number.parseInt(opts["gatingMinSamples"] as string, 10) || 25,
          gatingMinLift: parseOptionalNumber(opts["gatingMinLift"]),
          gatingMaxRiskBreaches: Number.parseInt(opts["gatingMaxRiskBreaches"] as string, 10) || 0,
          canaryDropThreshold: parseOptionalNumber(opts["canaryDropThreshold"]),
          reliabilityMinCompletion: parseOptionalNumber(opts["reliabilityMinCompletion"]),
          reliabilityMaxTimeoutRate: parseOptionalNumber(opts["reliabilityMaxTimeoutRate"]),
          reliabilityMinReproducibility: parseOptionalNumber(opts["reliabilityMinReproducibility"]),
          reliabilityMaxAvgRetries: parseOptionalNumber(opts["reliabilityMaxAvgRetries"]),
          metadata: parseOptionalJsonObject(opts.metadata),
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `suite=${suite.name} task_type=${suite.taskType} archetype=${suite.taskArchetype || "default"} active=${suite.active ? 1 : 0} min_samples=${suite.gatingMinSamples} min_lift=${suite.gatingMinLift.toFixed(3)} canary_drop=${suite.canaryDropThreshold.toFixed(3)} reliability_completion=${suite.reliabilityMinCompletion.toFixed(3)} reliability_timeout=${suite.reliabilityMaxTimeoutRate.toFixed(3)} reliability_repro=${suite.reliabilityMinReproducibility.toFixed(3)} reliability_retries=${suite.reliabilityMaxAvgRetries.toFixed(2)}`,
        );
      });
    });

  research
    .command("benchmark-suite-list")
    .description("List benchmark suites")
    .option("--task-type <kind>", "investment|coding|other")
    .option("--archetype <name>", "Task archetype", "")
    .option("--include-inactive", "Include inactive suites", false)
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const suites = listBenchmarkSuites({
          taskType: opts["taskType"] as string | undefined,
          taskArchetype: opts.archetype as string,
          activeOnly: !Boolean(opts["includeInactive"]),
          dbPath: opts.db as string,
        });
        if (!suites.length) {
          defaultRuntime.log("No benchmark suites found.");
          return;
        }
        suites.forEach((suite) => {
          defaultRuntime.log(
            `suite=${suite.name} task_type=${suite.taskType} archetype=${suite.taskArchetype || "default"} active=${suite.active ? 1 : 0} min_samples=${suite.gatingMinSamples} min_lift=${suite.gatingMinLift.toFixed(3)} max_risk_breaches=${suite.gatingMaxRiskBreaches} canary_drop=${suite.canaryDropThreshold.toFixed(3)} reliability_completion=${suite.reliabilityMinCompletion.toFixed(3)} reliability_timeout=${suite.reliabilityMaxTimeoutRate.toFixed(3)} reliability_repro=${suite.reliabilityMinReproducibility.toFixed(3)} reliability_retries=${suite.reliabilityMaxAvgRetries.toFixed(2)}`,
          );
        });
      });
    });

  research
    .command("benchmark-case-upsert")
    .description("Create or update a benchmark case in a suite")
    .requiredOption("--suite <name>", "Suite name")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .option("--suite-archetype <name>", "Suite archetype", "")
    .requiredOption("--case <name>", "Case name")
    .option("--case-archetype <name>", "Case archetype", "")
    .option("--ticker <symbol>", "Ticker filter")
    .option("--repo-root <path>", "Repo root filter")
    .option("--input <text>", "Input summary")
    .option("--prompt <text>", "Prompt template used by case")
    .option("--expected <json>", "Expected metrics JSON")
    .option("--weight <n>", "Case weight", "1")
    .option("--active <bool>", "true|false", "true")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const benchmarkCase = upsertBenchmarkCase({
          suiteName: opts.suite as string,
          taskType: opts["taskType"] as string,
          taskArchetype: opts["suiteArchetype"] as string,
          caseName: opts.case as string,
          caseArchetype: opts["caseArchetype"] as string,
          ticker: opts.ticker as string | undefined,
          repoRoot: opts["repoRoot"] as string | undefined,
          inputSummary: opts.input as string | undefined,
          promptText: opts.prompt as string | undefined,
          expected: parseOptionalJsonObject(opts.expected),
          weight: parseOptionalNumber(opts.weight),
          active: parseBooleanOption(opts.active as string),
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `case=${benchmarkCase.caseName} suite_id=${benchmarkCase.suiteId} archetype=${benchmarkCase.taskArchetype || "default"} ticker=${benchmarkCase.ticker || "n/a"} repo_root=${benchmarkCase.repoRoot || "n/a"} weight=${benchmarkCase.weight.toFixed(2)} active=${benchmarkCase.active ? 1 : 0}`,
        );
      });
    });

  research
    .command("benchmark-case-list")
    .description("List benchmark cases in a suite")
    .requiredOption("--suite <name>", "Suite name")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .option("--suite-archetype <name>", "Suite archetype", "")
    .option("--include-inactive", "Include inactive cases", false)
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const cases = listBenchmarkCases({
          suiteName: opts.suite as string,
          taskType: opts["taskType"] as string,
          taskArchetype: opts["suiteArchetype"] as string,
          activeOnly: !Boolean(opts["includeInactive"]),
          dbPath: opts.db as string,
        });
        if (!cases.length) {
          defaultRuntime.log("No benchmark cases found.");
          return;
        }
        cases.forEach((benchmarkCase) => {
          defaultRuntime.log(
            `case=${benchmarkCase.caseName} archetype=${benchmarkCase.taskArchetype || "default"} ticker=${benchmarkCase.ticker || "n/a"} repo_root=${benchmarkCase.repoRoot || "n/a"} weight=${benchmarkCase.weight.toFixed(2)} active=${benchmarkCase.active ? 1 : 0}`,
          );
        });
      });
    });

  research
    .command("benchmark-run")
    .description("Run deterministic benchmark replay against champion/challengers")
    .requiredOption("--suite <name>", "Suite name")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .option("--suite-archetype <name>", "Suite archetype", "")
    .option(
      "--mode <kind>",
      "champion_vs_challenger|all_policies|champion_only",
      "champion_vs_challenger",
    )
    .option("--lookback-days <n>", "Lookback days for replay samples", "90")
    .option("--seed <text>", "Deterministic seed")
    .option("--apply-governance", "Apply benchmark gate decision to policy champion", false)
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const run = runBenchmarkReplay({
          suiteName: opts.suite as string,
          taskType: opts["taskType"] as string,
          taskArchetype: opts["suiteArchetype"] as string,
          mode: opts.mode as string,
          lookbackDays: Number.parseInt(opts["lookbackDays"] as string, 10) || 90,
          seed: opts.seed as string | undefined,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `benchmark_run id=${run.runId} suite=${run.suite.name} mode=${run.mode} cases=${run.caseCount} lookback_days=${run.lookbackDays}`,
        );
        run.policySummaries.forEach((summary) => {
          defaultRuntime.log(
            `policy=${summary.policyName} status=${summary.status} score=${summary.weightedScore.toFixed(3)} win=${(summary.weightedWinRate * 100).toFixed(1)}% risk_breaches=${summary.riskBreaches} samples=${summary.totalSamples} traces=${summary.reliabilityTraceCount} completion=${typeof summary.reliabilityCompletionRate === "number" ? `${(summary.reliabilityCompletionRate * 100).toFixed(1)}%` : "n/a"} timeout=${typeof summary.reliabilityTimeoutRate === "number" ? `${(summary.reliabilityTimeoutRate * 100).toFixed(1)}%` : "n/a"} repro=${typeof summary.reliabilityReproducibility === "number" ? `${(summary.reliabilityReproducibility * 100).toFixed(1)}%` : "n/a"} retries=${typeof summary.reliabilityAvgRetries === "number" ? summary.reliabilityAvgRetries.toFixed(2) : "n/a"}`,
          );
        });
        defaultRuntime.log(
          `gate champion=${run.gate.championPolicy ?? "none"} promote_allowed=${run.gate.promoteAllowed ? 1 : 0} promote_candidate=${run.gate.promoteCandidate ?? "none"} promote_reliability_pass=${run.gate.promoteReliabilityPass ? 1 : 0} canary_breach=${run.gate.canaryBreach ? 1 : 0} canary_reliability_breach=${run.gate.canaryReliabilityBreach ? 1 : 0} rollback_candidate=${run.gate.rollbackCandidate ?? "none"}`,
        );
        if (run.gate.promoteReliabilityReason) {
          defaultRuntime.log(
            `gate_promote_reliability_reason=${run.gate.promoteReliabilityReason}`,
          );
        }
        if (run.gate.canaryReliabilityReason) {
          defaultRuntime.log(`gate_canary_reliability_reason=${run.gate.canaryReliabilityReason}`);
        }
        if (Boolean(opts["applyGovernance"])) {
          const decision = applyBenchmarkGovernance({
            runId: run.runId,
            dbPath: opts.db as string,
          });
          defaultRuntime.log(
            `benchmark_governance decision=${decision.decisionType} applied=${decision.applied ? 1 : 0} before=${decision.championBefore || "none"} after=${decision.championAfter || "none"} reason=${decision.reason}`,
          );
        }
      });
    });

  research
    .command("benchmark-report")
    .description("Show benchmark run history and gate outcomes")
    .requiredOption("--suite <name>", "Suite name")
    .requiredOption("--task-type <kind>", "investment|coding|other")
    .option("--suite-archetype <name>", "Suite archetype", "")
    .option("--limit <n>", "Number of runs", "20")
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const report = benchmarkReport({
          suiteName: opts.suite as string,
          taskType: opts["taskType"] as string,
          taskArchetype: opts["suiteArchetype"] as string,
          limit: Number.parseInt(opts.limit as string, 10) || 20,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `benchmark_report suite=${report.suite.name} task_type=${report.suite.taskType} archetype=${report.suite.taskArchetype || "default"} runs=${report.runs.length}`,
        );
        report.runs.forEach((run) => {
          defaultRuntime.log(
            `${new Date(run.completedAt).toISOString()} run_id=${run.id} mode=${run.mode} cases=${run.summary.caseCount} champion=${run.summary.gate.championPolicy ?? "none"} promote_allowed=${run.summary.gate.promoteAllowed ? 1 : 0} promote_reliability_pass=${run.summary.gate.promoteReliabilityPass ? 1 : 0} canary_breach=${run.summary.gate.canaryBreach ? 1 : 0} canary_reliability_breach=${run.summary.gate.canaryReliabilityBreach ? 1 : 0}`,
          );
          run.summary.policySummaries.forEach((summary) => {
            defaultRuntime.log(
              `  policy=${summary.policyName} score=${summary.weightedScore.toFixed(3)} win=${(summary.weightedWinRate * 100).toFixed(1)}% risk_breaches=${summary.riskBreaches} samples=${summary.totalSamples} traces=${summary.reliabilityTraceCount} completion=${typeof summary.reliabilityCompletionRate === "number" ? `${(summary.reliabilityCompletionRate * 100).toFixed(1)}%` : "n/a"} timeout=${typeof summary.reliabilityTimeoutRate === "number" ? `${(summary.reliabilityTimeoutRate * 100).toFixed(1)}%` : "n/a"} repro=${typeof summary.reliabilityReproducibility === "number" ? `${(summary.reliabilityReproducibility * 100).toFixed(1)}%` : "n/a"} retries=${typeof summary.reliabilityAvgRetries === "number" ? summary.reliabilityAvgRetries.toFixed(2) : "n/a"}`,
            );
          });
        });
      });
    });

  research
    .command("benchmark-nightly")
    .description("Run all active benchmark suites and apply governance decisions")
    .option("--task-type <kind>", "investment|coding|other")
    .option("--suite-archetype <name>", "Task archetype", "")
    .option("--lookback-days <n>", "Lookback days", "90")
    .option(
      "--mode <kind>",
      "champion_vs_challenger|all_policies|champion_only",
      "champion_vs_challenger",
    )
    .option("--db <path>", "Database path", path.join(process.cwd(), "data", "research.db"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = runAllBenchmarksWithGovernance({
          taskType: opts["taskType"] as string | undefined,
          taskArchetype: opts["suiteArchetype"] as string,
          lookbackDays: Number.parseInt(opts["lookbackDays"] as string, 10) || 90,
          mode: opts.mode as string,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `benchmark_nightly suites=${result.suiteCount} runs=${result.runCount} failures=${result.failures}`,
        );
        result.decisions.forEach((decision) => {
          defaultRuntime.log(
            `  run_id=${decision.runId} decision=${decision.decisionType} applied=${decision.applied ? 1 : 0} before=${decision.championBefore || "none"} after=${decision.championAfter || "none"} reason=${decision.reason}`,
          );
        });
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
        defaultRuntime.log(`quality_passed=${result.quality.passed ? 1 : 0}`);
        defaultRuntime.log(`quality_min_score=${result.quality.minScore.toFixed(2)}`);
        defaultRuntime.log(
          `actionability_score=${result.quality.actionabilityScore.toFixed(2)} calibration_mode=${result.quality.calibration.mode} calibration_score=${result.quality.calibration.score.toFixed(2)} calibration_samples=${result.quality.calibration.sampleCount}`,
        );
        if (result.quality.requiredFailures.length) {
          defaultRuntime.error(`required_failures=${result.quality.requiredFailures.join(",")}`);
        }
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
