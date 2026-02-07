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
import {
  buildTickerPointInTimeGraph,
  getTickerPointInTimeSnapshot,
} from "../research/knowledge-graph.js";
import { learningReport, logTaskOutcome, runLearningCalibration } from "../research/learning.js";
import {
  DEFAULT_MACRO_FACTOR_KEYS,
  ingestDefaultMacroFactors,
  listMacroFactorObservations,
} from "../research/macro-factors.js";
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
import {
  computePortfolioDecision,
  type PortfolioDecisionConstraints,
} from "../research/portfolio-decision.js";
import {
  computePortfolioOptimization,
  type PortfolioOptimizerConstraints,
} from "../research/portfolio-optimizer.js";
import { computePortfolioReplay } from "../research/portfolio-replay.js";
import { computePortfolioPlan } from "../research/portfolio.js";
import { provenanceReport } from "../research/provenance.js";
import {
  evaluateCrossSectionQualityGate,
  evaluateQualityGateRegression,
  listQualityGateRuns,
  recordQualityGateRun,
  summarizeQualityGateRuns,
  type QualityGateArtifactType,
} from "../research/quality-gate.js";
import { indexRepo } from "../research/repo-index.js";
import { runResearchSecurityAudit } from "../research/security.js";
import {
  getThemeConstituents,
  listThemeDefinitions,
  refreshThemeMembership,
  upsertThemeDefinition,
} from "../research/theme-ontology.js";
import {
  buildSectorInstitutionalReport,
  buildThemeInstitutionalReport,
} from "../research/theme-sector-report.js";
import { computeSectorResearch, computeThemeResearch } from "../research/theme-sector.js";
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
const parseCsvListOption = (value: string): string[] =>
  value
    .split(",")
    .map((entry) => entry.trim())
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

const parseOptionalNumericMap = (value: unknown): Record<string, number> | undefined => {
  const objectValue = parseOptionalJsonObject(value);
  if (!objectValue) return undefined;
  const out: Record<string, number> = {};
  for (const [key, raw] of Object.entries(objectValue)) {
    if (!key.trim()) continue;
    const numeric = parseOptionalNumber(raw);
    if (typeof numeric !== "number") continue;
    out[key.trim().toUpperCase()] = numeric;
  }
  return out;
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

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const mean = (values: number[]): number =>
  values.length > 0 ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const THEME_HYDRATION_ATTEMPTS = new Set<string>();

const hydrateThemeEvidence = async (params: {
  theme: string;
  tickers: string[];
  dbPath?: string;
}): Promise<string> => {
  const key = `${params.theme.trim().toLowerCase()}::${params.dbPath ?? "default"}`;
  if (THEME_HYDRATION_ATTEMPTS.has(key)) {
    return "skipped(already-attempted)";
  }
  THEME_HYDRATION_ATTEMPTS.add(key);
  const userAgent =
    process.env.SEC_USER_AGENT?.trim() || process.env.SEC_EDGAR_USER_AGENT?.trim() || undefined;
  const tickers = Array.from(new Set(params.tickers.map((ticker) => ticker.trim().toUpperCase())));
  if (!tickers.length) {
    return "skipped(no-tickers)";
  }
  const notes: string[] = [];
  const run = async (label: string, task: () => Promise<unknown>) => {
    try {
      await task();
      notes.push(`${label}=ok`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      notes.push(`${label}=skip(${message})`);
    }
  };
  for (const ticker of tickers) {
    await run(`${ticker}:prices`, async () => {
      await ingestPrices(ticker, { dbPath: params.dbPath });
    });
    await run(`${ticker}:fundamentals`, async () => {
      await ingestFundamentals(ticker, {
        userAgent,
        dbPath: params.dbPath,
      });
    });
    await run(`${ticker}:expectations`, async () => {
      await ingestExpectations(ticker, { dbPath: params.dbPath });
    });
    await run(`${ticker}:filings`, async () => {
      await ingestFilings(ticker, {
        limit: 20,
        userAgent,
        dbPath: params.dbPath,
      });
    });
  }
  await run("embed", async () => {
    await syncEmbeddings(params.dbPath);
  });
  return notes.join(",");
};

const parseQualityGateArtifactType = (value?: string): QualityGateArtifactType | undefined => {
  const normalized = (value ?? "").trim().toLowerCase();
  if (!normalized || normalized === "all") return undefined;
  if (normalized === "memo" || normalized === "sector_report" || normalized === "theme_report") {
    return normalized;
  }
  throw new Error(`Invalid artifact type: ${value}`);
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
    .option("--path <db>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const db = openResearchDb(opts.path);
        void db;
        defaultRuntime.log(`Research DB ready at ${resolveResearchDbPath(opts.path as string)}`);
      });
    });

  research
    .command("macro-ingest")
    .description("Ingest default external macro factor proxies (Massive/Polygon)")
    .option("--api-key <value>", "Massive API key (or MASSIVE_API_KEY / POLYGON_API_KEY env)")
    .option("--retries <n>", "Retry count", "3")
    .option("--pause-ms <n>", "Pause between requests (ms)", "12500")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = await ingestDefaultMacroFactors({
          apiKey: opts["apiKey"] as string | undefined,
          retries: parseOptionalNumber(opts.retries),
          pauseMs: parseOptionalNumber(opts["pauseMs"]),
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`macro_ingest fetched_at=${result.fetchedAt}`);
        result.factors.forEach((factor) => {
          defaultRuntime.log(
            `macro_factor factor=${factor.factorKey} observations=${factor.observations} start_date=${factor.startDate || "n/a"} end_date=${factor.endDate || "n/a"}`,
          );
        });
      });
    });

  research
    .command("macro-list")
    .description("List stored macro factor observations")
    .option("--factor <key>", `Macro factor key (${DEFAULT_MACRO_FACTOR_KEYS.join(",")})`)
    .option("--start-date <date>", "Start date (YYYY-MM-DD)")
    .option("--end-date <date>", "End date (YYYY-MM-DD)")
    .option("--limit <n>", "Result limit", "200")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rows = listMacroFactorObservations({
          factorKey: opts.factor as string | undefined,
          startDate: opts["startDate"] as string | undefined,
          endDate: opts["endDate"] as string | undefined,
          limit: parseOptionalNumber(opts.limit) ?? 200,
          dbPath: opts.db as string,
        });
        if (!rows.length) {
          defaultRuntime.log("No macro factor observations found.");
          return;
        }
        rows.forEach((row) => {
          defaultRuntime.log(
            `macro_factor factor=${row.factorKey} date=${row.date} value=${row.value.toFixed(6)} source=${row.source} fetched_at=${new Date(row.fetchedAt).toISOString()}`,
          );
        });
      });
    });

  research
    .command("prices")
    .description("Ingest daily prices from Massive/Polygon")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const count = await ingestPrices(opts.ticker as string, { dbPath: opts.db as string });
        defaultRuntime.log(`Saved ${count} price rows for ${opts.ticker}`);
      });
    });

  research
    .command("filings")
    .description("Fetch recent SEC filings and chunk them")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--limit <n>", "Max filings", "20")
    .option("--user-agent <ua>", "SEC User-Agent header")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const results = await ingestFilings(opts.ticker as string, {
          limit: Number.parseInt(opts.limit as string, 10) || 20,
          userAgent: opts.userAgent as string | undefined,
          dbPath: opts.db as string,
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
          dbPath: opts.db as string,
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = await ingestExpectations(opts.ticker as string, {
          dbPath: opts.db as string,
        });
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const res = await ingestTranscript(opts.ticker as string, opts.url as string, {
          dbPath: opts.db as string,
        });
        defaultRuntime.log(`Transcript ingested (${res.chunks} chunks)`);
      });
    });

  research
    .command("repo-index")
    .description("Index a code repo into research db (text chunks, no embeddings)")
    .requiredOption("--root <path>", "Repo root to index")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const res = indexRepo({ root: opts.root as string, dbPath: opts.db as string });
        defaultRuntime.log(`Indexed ${res.filesIndexed} files into ${res.dbPath}`);
      });
    });

  research
    .command("embed")
    .description("Create/update sqlite-vec embeddings for research/code chunks")
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .command("position-decision")
    .description("Generate production-grade position candidates with scenario stress")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--question <text>", "Decision framing question")
    .option("--max-weight <n>", "Max single-name weight %")
    .option("--max-risk-budget <n>", "Max risk budget %")
    .option("--max-stop-loss <n>", "Max stop-loss % (absolute, e.g. 0.24)")
    .option("--min-confidence <n>", "Minimum confidence [0-1]")
    .option("--min-coverage <n>", "Minimum adversarial debate coverage [0-1]")
    .option("--max-downside-loss <n>", "Max downside PnL % allowed in stress")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const constraints: Partial<PortfolioDecisionConstraints> = {};
        const maxWeight = parseOptionalNumber(opts["maxWeight"]);
        const maxRiskBudget = parseOptionalNumber(opts["maxRiskBudget"]);
        const maxStopLoss = parseOptionalNumber(opts["maxStopLoss"]);
        const minConfidence = parseOptionalNumber(opts["minConfidence"]);
        const minCoverage = parseOptionalNumber(opts["minCoverage"]);
        const maxDownsideLoss = parseOptionalNumber(opts["maxDownsideLoss"]);
        if (typeof maxWeight === "number") constraints.maxSingleNameWeightPct = maxWeight;
        if (typeof maxRiskBudget === "number") constraints.maxRiskBudgetPct = maxRiskBudget;
        if (typeof maxStopLoss === "number") constraints.maxStopLossPct = maxStopLoss;
        if (typeof minConfidence === "number") constraints.minConfidence = minConfidence;
        if (typeof minCoverage === "number") constraints.requiredDebateCoverage = minCoverage;
        if (typeof maxDownsideLoss === "number") constraints.maxDownsideLossPct = maxDownsideLoss;

        const decision = computePortfolioDecision({
          ticker: opts.ticker as string,
          question: opts.question as string | undefined,
          constraints,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `ticker=${decision.ticker} recommendation=${decision.recommendation} stance=${decision.finalStance} score=${decision.decisionScore.toFixed(2)} confidence=${decision.confidence.toFixed(2)}`,
        );
        defaultRuntime.log(
          `expected_return_pct=${decision.expectedReturnPct.toFixed(2)} downside_risk_pct=${decision.downsideRiskPct.toFixed(2)} debate_coverage=${decision.researchCell.debate.adversarialCoverageScore.toFixed(2)}`,
        );
        defaultRuntime.log(
          `constraints max_weight_pct=${decision.constraints.maxSingleNameWeightPct.toFixed(2)} max_risk_budget_pct=${decision.constraints.maxRiskBudgetPct.toFixed(2)} max_stop_loss_pct=${decision.constraints.maxStopLossPct.toFixed(2)} min_confidence=${decision.constraints.minConfidence.toFixed(2)} min_coverage=${decision.constraints.requiredDebateCoverage.toFixed(2)} max_downside_loss_pct=${decision.constraints.maxDownsideLossPct.toFixed(2)}`,
        );
        if (decision.riskBreaches.length) {
          decision.riskBreaches.forEach((issue) => defaultRuntime.error(`RISK_BREACH: ${issue}`));
        }
        decision.sizeCandidates.forEach((candidate) => {
          defaultRuntime.log(
            `candidate=${candidate.label} recommendation=${candidate.recommendation} weight_pct=${candidate.weightPct.toFixed(2)} risk_budget_pct=${candidate.riskBudgetPct.toFixed(2)} expected_pnl_pct=${candidate.expectedPnlPct.toFixed(2)} downside_pnl_pct=${candidate.downsidePnlPct.toFixed(2)} score=${candidate.score.toFixed(2)}`,
          );
        });
        decision.stress.forEach((stress) => {
          defaultRuntime.log(
            `stress scenario=${stress.scenario} probability=${stress.probability.toFixed(2)} return_pct=${stress.returnPct.toFixed(2)} weighted_return_pct=${stress.weightedReturnPct.toFixed(2)} pnl_pct=${stress.pnlPct.toFixed(2)} risk_breach=${stress.breachesRiskBudget ? 1 : 0}`,
          );
        });
      });
    });

  research
    .command("portfolio-optimize")
    .description("Optimize multi-name portfolio allocations with correlation and exposure controls")
    .requiredOption("--tickers <csv>", "Comma-separated tickers")
    .option("--question <text>", "Portfolio construction framing question")
    .option("--lookback-days <n>", "Price history lookback days", "252")
    .option("--max-gross <n>", "Max gross exposure %")
    .option("--max-net <n>", "Max net exposure %")
    .option("--max-portfolio-risk <n>", "Max aggregate risk budget %")
    .option("--max-sector <n>", "Max sector exposure %")
    .option("--max-single <n>", "Max single-name weight %")
    .option("--max-pair-corr <n>", "Max pairwise correlation threshold [0-1]")
    .option("--max-weighted-corr <n>", "Max weighted portfolio correlation [0-1]")
    .option("--min-position <n>", "Minimum tradable position size %")
    .option("--min-corr-history <n>", "Minimum overlapping history days for correlation")
    .option("--max-stress-loss <n>", "Max allowed portfolio stress loss %")
    .option("--portfolio-nav <n>", "Portfolio NAV in USD for participation/cost model")
    .option("--min-adv-usd <n>", "Minimum average daily dollar volume in USD")
    .option("--max-adv-participation <n>", "Max ADV participation fraction [0-1]")
    .option("--max-turnover <n>", "Max turnover per rebalance % of NAV")
    .option("--spread-bps <n>", "Base spread cost in bps")
    .option("--impact-bps <n>", "Impact cost in bps at max participation")
    .option("--liquidity-lookback <n>", "Liquidity lookback window in trading days")
    .option(
      "--current-weights <json>",
      'Current signed weights map JSON (e.g. {"AAPL":2.5,"MSFT":-1})',
    )
    .option("--max-weight <n>", "Pass-through single-name cap % for decision layer")
    .option("--max-risk-budget <n>", "Pass-through single-name risk budget %")
    .option("--max-stop-loss <n>", "Pass-through stop-loss cap % (absolute)")
    .option("--min-confidence <n>", "Pass-through single-name confidence floor [0-1]")
    .option("--min-coverage <n>", "Pass-through debate coverage floor [0-1]")
    .option("--max-downside-loss <n>", "Pass-through single-name downside loss cap %")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const tickers = parseTickersOption(opts.tickers as string);
        if (!tickers.length) throw new Error("Provide at least one ticker.");

        const decisionConstraints: Partial<PortfolioDecisionConstraints> = {};
        const passMaxWeight = parseOptionalNumber(opts["maxWeight"]);
        const passMaxRiskBudget = parseOptionalNumber(opts["maxRiskBudget"]);
        const passMaxStopLoss = parseOptionalNumber(opts["maxStopLoss"]);
        const passMinConfidence = parseOptionalNumber(opts["minConfidence"]);
        const passMinCoverage = parseOptionalNumber(opts["minCoverage"]);
        const passMaxDownsideLoss = parseOptionalNumber(opts["maxDownsideLoss"]);
        if (typeof passMaxWeight === "number")
          decisionConstraints.maxSingleNameWeightPct = passMaxWeight;
        if (typeof passMaxRiskBudget === "number")
          decisionConstraints.maxRiskBudgetPct = passMaxRiskBudget;
        if (typeof passMaxStopLoss === "number")
          decisionConstraints.maxStopLossPct = passMaxStopLoss;
        if (typeof passMinConfidence === "number")
          decisionConstraints.minConfidence = passMinConfidence;
        if (typeof passMinCoverage === "number")
          decisionConstraints.requiredDebateCoverage = passMinCoverage;
        if (typeof passMaxDownsideLoss === "number")
          decisionConstraints.maxDownsideLossPct = passMaxDownsideLoss;

        const constraints: Partial<PortfolioOptimizerConstraints> = {};
        const maxGross = parseOptionalNumber(opts["maxGross"]);
        const maxNet = parseOptionalNumber(opts["maxNet"]);
        const maxPortfolioRisk = parseOptionalNumber(opts["maxPortfolioRisk"]);
        const maxSector = parseOptionalNumber(opts["maxSector"]);
        const maxSingle = parseOptionalNumber(opts["maxSingle"]);
        const maxPairCorr = parseOptionalNumber(opts["maxPairCorr"]);
        const maxWeightedCorr = parseOptionalNumber(opts["maxWeightedCorr"]);
        const minPosition = parseOptionalNumber(opts["minPosition"]);
        const minCorrHistory = parseOptionalNumber(opts["minCorrHistory"]);
        const maxStressLoss = parseOptionalNumber(opts["maxStressLoss"]);
        const portfolioNav = parseOptionalNumber(opts["portfolioNav"]);
        const minAdvUsd = parseOptionalNumber(opts["minAdvUsd"]);
        const maxAdvParticipation = parseOptionalNumber(opts["maxAdvParticipation"]);
        const maxTurnover = parseOptionalNumber(opts["maxTurnover"]);
        const spreadBps = parseOptionalNumber(opts["spreadBps"]);
        const impactBps = parseOptionalNumber(opts["impactBps"]);
        const liquidityLookback = parseOptionalNumber(opts["liquidityLookback"]);
        if (typeof maxGross === "number") constraints.maxGrossExposurePct = maxGross;
        if (typeof maxNet === "number") constraints.maxNetExposurePct = maxNet;
        if (typeof maxPortfolioRisk === "number")
          constraints.maxPortfolioRiskBudgetPct = maxPortfolioRisk;
        if (typeof maxSector === "number") constraints.maxSectorExposurePct = maxSector;
        if (typeof maxSingle === "number") constraints.maxSingleNameWeightPct = maxSingle;
        if (typeof maxPairCorr === "number") constraints.maxPairwiseCorrelation = maxPairCorr;
        if (typeof maxWeightedCorr === "number")
          constraints.maxWeightedCorrelation = maxWeightedCorr;
        if (typeof minPosition === "number") constraints.minPositionWeightPct = minPosition;
        if (typeof minCorrHistory === "number")
          constraints.minCorrelationHistoryDays = minCorrHistory;
        if (typeof maxStressLoss === "number") constraints.maxStressLossPct = maxStressLoss;
        if (typeof portfolioNav === "number") constraints.portfolioNavUsd = portfolioNav;
        if (typeof minAdvUsd === "number") constraints.minAvgDailyDollarVolumeUsd = minAdvUsd;
        if (typeof maxAdvParticipation === "number")
          constraints.maxAdvParticipationPct = maxAdvParticipation;
        if (typeof maxTurnover === "number") constraints.maxTurnoverPct = maxTurnover;
        if (typeof spreadBps === "number") constraints.spreadBps = spreadBps;
        if (typeof impactBps === "number") constraints.impactBpsAtMaxParticipation = impactBps;
        if (typeof liquidityLookback === "number")
          constraints.liquidityLookbackDays = liquidityLookback;
        const currentWeightsSignedPct = parseOptionalNumericMap(opts["currentWeights"]);

        const result = computePortfolioOptimization({
          tickers,
          question: opts.question as string | undefined,
          decisionConstraints,
          constraints,
          currentWeightsSignedPct,
          dbPath: opts.db as string,
          lookbackDays: parseOptionalNumber(opts["lookbackDays"]) ?? 252,
        });

        defaultRuntime.log(
          `portfolio_positions=${result.positions.length} dropped=${result.dropped.length} constraints_breaches=${result.constraintBreaches.length}`,
        );
        defaultRuntime.log(
          `gross_exposure_pct=${result.metrics.grossExposurePct.toFixed(2)} net_exposure_pct=${result.metrics.netExposurePct.toFixed(2)} risk_budget_pct=${result.metrics.portfolioRiskBudgetPct.toFixed(2)} expected_pnl_pct=${result.metrics.expectedPnlPct.toFixed(2)} expected_net_pnl_pct=${result.metrics.expectedNetPnlPct.toFixed(2)} transaction_cost_pct=${result.metrics.transactionCostPct.toFixed(2)} turnover_pct=${result.metrics.turnoverPct.toFixed(2)} worst_scenario_pnl_pct=${result.metrics.worstScenarioPnlPct.toFixed(2)} weighted_correlation=${result.metrics.weightedCorrelation.toFixed(2)} diversification_score=${result.metrics.diversificationScore.toFixed(2)} effective_names=${result.metrics.effectiveNames.toFixed(2)} liquidity_coverage_pct=${(result.metrics.liquidityCoveragePct * 100).toFixed(1)}%`,
        );
        result.positions.forEach((position) => {
          defaultRuntime.log(
            `position ticker=${position.ticker} stance=${position.stance} recommendation=${position.recommendation} signed_weight_pct=${position.signedWeightPct.toFixed(2)} abs_weight_pct=${position.absWeightPct.toFixed(2)} current_signed_weight_pct=${position.currentSignedWeightPct.toFixed(2)} turnover_trade_pct=${position.turnoverTradePct.toFixed(2)} risk_budget_pct=${position.riskBudgetPct.toFixed(2)} expected_return_pct=${position.directionalExpectedReturnPct.toFixed(2)} expected_pnl_pct=${position.expectedPnlPct.toFixed(2)} expected_net_pnl_pct=${position.expectedNetPnlPct.toFixed(2)} txn_cost_bps=${position.estimatedTransactionCostBps.toFixed(2)} txn_cost_pct=${position.estimatedTransactionCostPct.toFixed(3)} adv_participation_pct=${typeof position.advParticipationPct === "number" ? (position.advParticipationPct * 100).toFixed(2) : "n/a"} adv_usd=${typeof position.avgDailyDollarVolumeUsd === "number" ? position.avgDailyDollarVolumeUsd.toFixed(0) : "n/a"} worst_scenario_pnl_pct=${position.worstScenarioPnlPct.toFixed(2)} score=${position.decisionScore.toFixed(2)} confidence=${position.confidence.toFixed(2)} sector=${position.sector} corr_penalty=${position.correlationPenalty.toFixed(2)} vol_annualized_pct=${typeof position.volatilityAnnualizedPct === "number" ? position.volatilityAnnualizedPct.toFixed(2) : "n/a"}`,
          );
        });
        result.scenarioStress.forEach((scenario) => {
          defaultRuntime.log(
            `stress scenario=${scenario.scenario} portfolio_pnl_pct=${scenario.portfolioPnlPct.toFixed(2)} stress_breach=${scenario.breachesStressLossLimit ? 1 : 0}`,
          );
        });
        const sectorEntries = Object.entries(result.sectorExposurePct).sort(
          (left, right) => right[1] - left[1],
        );
        sectorEntries.forEach(([sector, exposure]) => {
          defaultRuntime.log(
            `sector_exposure sector=${sector} exposure_pct=${exposure.toFixed(2)}`,
          );
        });
        result.pairwiseCorrelation.slice(0, 8).forEach((edge) => {
          defaultRuntime.log(
            `correlation left=${edge.left} right=${edge.right} corr=${edge.correlation.toFixed(3)} overlap_days=${edge.overlapDays}`,
          );
        });
        result.dropped.forEach((row) => {
          defaultRuntime.error(`DROPPED ${row.ticker}: ${row.reason}`);
        });
        result.constraintBreaches.forEach((breach) => {
          defaultRuntime.error(`CONSTRAINT_BREACH: ${breach}`);
        });
      });
    });

  research
    .command("portfolio-replay")
    .description("Replay portfolio allocation policy on realized forward returns")
    .requiredOption("--tickers <csv>", "Comma-separated tickers")
    .option("--question <text>", "Replay framing question")
    .option("--start-date <date>", "Replay start date (YYYY-MM-DD)")
    .option("--end-date <date>", "Replay end date (YYYY-MM-DD)")
    .option("--rebalance-days <n>", "Rebalance frequency in trading days", "21")
    .option("--horizon-days <n>", "Forward return horizon in trading days", "21")
    .option("--lookback-signal <n>", "Signal lookback in trading days", "84")
    .option("--lookback-corr <n>", "Correlation lookback in trading days", "126")
    .option("--max-gross <n>", "Max gross exposure %")
    .option("--max-net <n>", "Max net exposure %")
    .option("--max-portfolio-risk <n>", "Max aggregate risk budget %")
    .option("--max-sector <n>", "Max sector exposure %")
    .option("--max-single <n>", "Max single-name weight %")
    .option("--max-pair-corr <n>", "Max pairwise correlation threshold [0-1]")
    .option("--max-weighted-corr <n>", "Max weighted portfolio correlation [0-1]")
    .option("--min-position <n>", "Minimum tradable position size %")
    .option("--max-stress-loss <n>", "Max allowed stress loss %")
    .option("--portfolio-nav <n>", "Portfolio NAV in USD for participation/cost model")
    .option("--min-adv-usd <n>", "Minimum average daily dollar volume in USD")
    .option("--max-adv-participation <n>", "Max ADV participation fraction [0-1]")
    .option("--max-turnover <n>", "Max turnover per rebalance % of NAV")
    .option("--spread-bps <n>", "Base spread cost in bps")
    .option("--impact-bps <n>", "Impact cost in bps at max participation")
    .option("--liquidity-lookback <n>", "Liquidity lookback window in trading days")
    .option("--max-weight <n>", "Pass-through single-name cap % for decision layer")
    .option("--max-risk-budget <n>", "Pass-through single-name risk budget %")
    .option("--max-stop-loss <n>", "Pass-through stop-loss cap % (absolute)")
    .option("--min-confidence <n>", "Pass-through single-name confidence floor [0-1]")
    .option("--min-coverage <n>", "Pass-through debate coverage floor [0-1]")
    .option("--max-downside-loss <n>", "Pass-through single-name downside loss cap %")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const tickers = parseTickersOption(opts.tickers as string);
        if (!tickers.length) throw new Error("Provide at least one ticker.");

        const decisionConstraints: Partial<PortfolioDecisionConstraints> = {};
        const passMaxWeight = parseOptionalNumber(opts["maxWeight"]);
        const passMaxRiskBudget = parseOptionalNumber(opts["maxRiskBudget"]);
        const passMaxStopLoss = parseOptionalNumber(opts["maxStopLoss"]);
        const passMinConfidence = parseOptionalNumber(opts["minConfidence"]);
        const passMinCoverage = parseOptionalNumber(opts["minCoverage"]);
        const passMaxDownsideLoss = parseOptionalNumber(opts["maxDownsideLoss"]);
        if (typeof passMaxWeight === "number")
          decisionConstraints.maxSingleNameWeightPct = passMaxWeight;
        if (typeof passMaxRiskBudget === "number")
          decisionConstraints.maxRiskBudgetPct = passMaxRiskBudget;
        if (typeof passMaxStopLoss === "number")
          decisionConstraints.maxStopLossPct = passMaxStopLoss;
        if (typeof passMinConfidence === "number")
          decisionConstraints.minConfidence = passMinConfidence;
        if (typeof passMinCoverage === "number")
          decisionConstraints.requiredDebateCoverage = passMinCoverage;
        if (typeof passMaxDownsideLoss === "number")
          decisionConstraints.maxDownsideLossPct = passMaxDownsideLoss;

        const constraints: Partial<PortfolioOptimizerConstraints> = {};
        const maxGross = parseOptionalNumber(opts["maxGross"]);
        const maxNet = parseOptionalNumber(opts["maxNet"]);
        const maxPortfolioRisk = parseOptionalNumber(opts["maxPortfolioRisk"]);
        const maxSector = parseOptionalNumber(opts["maxSector"]);
        const maxSingle = parseOptionalNumber(opts["maxSingle"]);
        const maxPairCorr = parseOptionalNumber(opts["maxPairCorr"]);
        const maxWeightedCorr = parseOptionalNumber(opts["maxWeightedCorr"]);
        const minPosition = parseOptionalNumber(opts["minPosition"]);
        const maxStressLoss = parseOptionalNumber(opts["maxStressLoss"]);
        const portfolioNav = parseOptionalNumber(opts["portfolioNav"]);
        const minAdvUsd = parseOptionalNumber(opts["minAdvUsd"]);
        const maxAdvParticipation = parseOptionalNumber(opts["maxAdvParticipation"]);
        const maxTurnover = parseOptionalNumber(opts["maxTurnover"]);
        const spreadBps = parseOptionalNumber(opts["spreadBps"]);
        const impactBps = parseOptionalNumber(opts["impactBps"]);
        const liquidityLookback = parseOptionalNumber(opts["liquidityLookback"]);
        if (typeof maxGross === "number") constraints.maxGrossExposurePct = maxGross;
        if (typeof maxNet === "number") constraints.maxNetExposurePct = maxNet;
        if (typeof maxPortfolioRisk === "number")
          constraints.maxPortfolioRiskBudgetPct = maxPortfolioRisk;
        if (typeof maxSector === "number") constraints.maxSectorExposurePct = maxSector;
        if (typeof maxSingle === "number") constraints.maxSingleNameWeightPct = maxSingle;
        if (typeof maxPairCorr === "number") constraints.maxPairwiseCorrelation = maxPairCorr;
        if (typeof maxWeightedCorr === "number")
          constraints.maxWeightedCorrelation = maxWeightedCorr;
        if (typeof minPosition === "number") constraints.minPositionWeightPct = minPosition;
        if (typeof maxStressLoss === "number") constraints.maxStressLossPct = maxStressLoss;
        if (typeof portfolioNav === "number") constraints.portfolioNavUsd = portfolioNav;
        if (typeof minAdvUsd === "number") constraints.minAvgDailyDollarVolumeUsd = minAdvUsd;
        if (typeof maxAdvParticipation === "number")
          constraints.maxAdvParticipationPct = maxAdvParticipation;
        if (typeof maxTurnover === "number") constraints.maxTurnoverPct = maxTurnover;
        if (typeof spreadBps === "number") constraints.spreadBps = spreadBps;
        if (typeof impactBps === "number") constraints.impactBpsAtMaxParticipation = impactBps;
        if (typeof liquidityLookback === "number")
          constraints.liquidityLookbackDays = liquidityLookback;

        const replay = computePortfolioReplay({
          tickers,
          question: opts.question as string | undefined,
          dbPath: opts.db as string,
          startDate: opts["startDate"] as string | undefined,
          endDate: opts["endDate"] as string | undefined,
          rebalanceEveryDays: parseOptionalNumber(opts["rebalanceDays"]) ?? 21,
          horizonDays: parseOptionalNumber(opts["horizonDays"]) ?? 21,
          lookbackSignalDays: parseOptionalNumber(opts["lookbackSignal"]) ?? 84,
          lookbackCorrelationDays: parseOptionalNumber(opts["lookbackCorr"]) ?? 126,
          decisionConstraints,
          constraints,
        });

        defaultRuntime.log(
          `replay_windows=${replay.summary.sampleCount} score=${replay.summary.score.toFixed(3)} passed=${replay.summary.passed ? 1 : 0} mean_expected_net_pnl_pct=${replay.summary.meanExpectedNetPnlPct.toFixed(3)} mean_realized_net_pnl_pct=${replay.summary.meanRealizedNetPnlPct.toFixed(3)} mae_pct=${replay.summary.maePct.toFixed(3)} rmse_pct=${replay.summary.rmsePct.toFixed(3)} directional_accuracy=${replay.summary.directionalAccuracy.toFixed(3)} win_rate=${replay.summary.winRate.toFixed(3)} expected_realized_corr=${replay.summary.expectedRealizedCorrelation.toFixed(3)}`,
        );
        replay.windows.slice(-20).forEach((window) => {
          defaultRuntime.log(
            `window rebalance_date=${window.rebalanceDate} exit_date=${window.exitDate} positions=${window.positions} expected_net_pnl_pct=${window.expectedNetPnlPct.toFixed(3)} realized_net_pnl_pct=${window.realizedNetPnlPct.toFixed(3)} turnover_pct=${window.turnoverPct.toFixed(2)} txn_cost_pct=${window.transactionCostPct.toFixed(3)} gross_exposure_pct=${window.grossExposurePct.toFixed(2)} net_exposure_pct=${window.netExposurePct.toFixed(2)} weighted_correlation=${window.weightedCorrelation.toFixed(3)} direction_match=${window.directionMatched ? 1 : 0}`,
          );
        });
        replay.evaluationChecks.forEach((check) => {
          const label = check.passed ? "PASS" : "FAIL";
          defaultRuntime.log(`${label} ${check.name}: ${check.detail}`);
        });
      });
    });

  research
    .command("theme-upsert")
    .description("Create or update a versioned theme taxonomy definition")
    .requiredOption("--theme <key>", "Theme key or name (e.g., ai-infrastructure)")
    .option("--display-name <text>", "Display name")
    .option("--description <text>", "Theme description")
    .option("--parent <key>", "Optional parent theme key")
    .option("--benchmark <text>", "Optional benchmark reference")
    .option("--version <n>", "Explicit version to upsert")
    .option("--activate", "Mark this version as active", false)
    .option("--status <kind>", "active|inactive|draft")
    .option("--include-keywords <csv>", "Include keyword list")
    .option("--exclude-keywords <csv>", "Exclude keyword list")
    .option("--required-sectors <csv>", "Required sector list")
    .option("--excluded-sectors <csv>", "Excluded sector list")
    .option("--required-industries <csv>", "Required industry terms")
    .option("--excluded-industries <csv>", "Excluded industry terms")
    .option("--allowlist <csv>", "Ticker allowlist")
    .option("--blocklist <csv>", "Ticker blocklist")
    .option("--min-membership-score <n>", "Membership score threshold [0-1]")
    .option("--effective-from <date>", "YYYY-MM-DD")
    .option("--effective-to <date>", "YYYY-MM-DD")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const includeKeywords = parseCsvListOption((opts["includeKeywords"] as string) ?? "");
        const excludeKeywords = parseCsvListOption((opts["excludeKeywords"] as string) ?? "");
        const requiredSectors = parseCsvListOption((opts["requiredSectors"] as string) ?? "");
        const excludedSectors = parseCsvListOption((opts["excludedSectors"] as string) ?? "");
        const requiredIndustries = parseCsvListOption((opts["requiredIndustries"] as string) ?? "");
        const excludedIndustries = parseCsvListOption((opts["excludedIndustries"] as string) ?? "");
        const allowlist = parseTickersOption((opts.allowlist as string) ?? "");
        const blocklist = parseTickersOption((opts.blocklist as string) ?? "");
        const minMembershipScore = parseOptionalNumber(opts["minMembershipScore"]);
        const version = parseOptionalNumber(opts.version);
        const statusRaw = typeof opts.status === "string" ? opts.status.trim().toLowerCase() : "";
        const status =
          statusRaw === "active" || statusRaw === "inactive" || statusRaw === "draft"
            ? statusRaw
            : undefined;
        const definition = upsertThemeDefinition({
          theme: opts.theme as string,
          displayName: opts["displayName"] as string | undefined,
          description: opts.description as string | undefined,
          parentTheme: opts.parent as string | undefined,
          benchmark: opts.benchmark as string | undefined,
          version,
          activate: Boolean(opts.activate),
          status,
          effectiveFrom: opts["effectiveFrom"] as string | undefined,
          effectiveTo: opts["effectiveTo"] as string | undefined,
          rules: {
            includeKeywords,
            excludeKeywords,
            requiredSectors,
            excludedSectors,
            requiredIndustries,
            excludedIndustries,
            tickerAllowlist: allowlist,
            tickerBlocklist: blocklist,
            minMembershipScore:
              typeof minMembershipScore === "number" ? minMembershipScore : undefined,
          },
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `theme=${definition.themeKey} version=${definition.version} status=${definition.status} benchmark=${definition.benchmark || "n/a"} min_membership_score=${definition.rules.minMembershipScore.toFixed(2)}`,
        );
        defaultRuntime.log(
          `rules include_keywords=${definition.rules.includeKeywords.length} exclude_keywords=${definition.rules.excludeKeywords.length} required_sectors=${definition.rules.requiredSectors.length} required_industries=${definition.rules.requiredIndustries.length} allowlist=${definition.rules.tickerAllowlist.length} blocklist=${definition.rules.tickerBlocklist.length}`,
        );
      });
    });

  research
    .command("theme-list")
    .description("List theme taxonomy definitions")
    .option("--theme <key>", "Optional theme key filter")
    .option("--all", "Include inactive versions", false)
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rows = listThemeDefinitions({
          theme: opts.theme as string | undefined,
          includeInactive: Boolean(opts.all),
          dbPath: opts.db as string,
        });
        if (!rows.length) {
          defaultRuntime.log("No themes found.");
          return;
        }
        rows.forEach((row) => {
          defaultRuntime.log(
            `theme=${row.themeKey} version=${row.version} status=${row.status} display_name=${row.displayName} benchmark=${row.benchmark || "n/a"} min_membership_score=${row.rules.minMembershipScore.toFixed(2)}`,
          );
        });
      });
    });

  research
    .command("theme-membership-refresh")
    .description("Recompute theme constituent membership scores from taxonomy rules")
    .requiredOption("--theme <key>", "Theme key")
    .option("--version <n>", "Optional theme version")
    .option("--tickers <csv>", "Optional explicit candidate ticker universe")
    .option("--min-score <n>", "Override minimum membership score [0-1]")
    .option("--source <text>", "Membership source tag", "rule_engine")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const tickers =
          typeof opts.tickers === "string" && opts.tickers.trim()
            ? parseTickersOption(opts.tickers as string)
            : undefined;
        const result = refreshThemeMembership({
          theme: opts.theme as string,
          version: parseOptionalNumber(opts.version),
          tickers,
          minMembershipScore: parseOptionalNumber(opts["minScore"]),
          source: opts.source as string | undefined,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `theme=${result.theme.themeKey} version=${result.theme.version} scored=${result.candidatesScored} active=${result.activeCount} candidate=${result.candidateCount} excluded=${result.excludedCount}`,
        );
        result.constituents.slice(0, 20).forEach((row) => {
          defaultRuntime.log(
            `member ticker=${row.ticker} score=${row.membershipScore.toFixed(3)} confidence=${row.confidence.toFixed(3)} status=${row.status} source=${row.source}`,
          );
        });
      });
    });

  research
    .command("theme-membership-list")
    .description("List scored theme constituents")
    .requiredOption("--theme <key>", "Theme key")
    .option("--version <n>", "Optional theme version")
    .option("--status <kind>", "active|candidate|excluded|inactive|all", "active")
    .option("--min-score <n>", "Minimum membership score", "0")
    .option("--limit <n>", "Result limit", "200")
    .option("--all", "Include inactive rows", false)
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const statusRaw = String(opts.status ?? "active")
          .trim()
          .toLowerCase();
        const status =
          statusRaw === "active" ||
          statusRaw === "candidate" ||
          statusRaw === "excluded" ||
          statusRaw === "inactive" ||
          statusRaw === "all"
            ? statusRaw
            : "active";
        const rows = getThemeConstituents({
          theme: opts.theme as string,
          version: parseOptionalNumber(opts.version),
          status,
          minMembershipScore: parseOptionalNumber(opts["minScore"]) ?? 0,
          limit: parseOptionalNumber(opts.limit) ?? 200,
          includeInactive: Boolean(opts.all),
          dbPath: opts.db as string,
        });
        if (!rows.length) {
          defaultRuntime.log("No theme constituents found.");
          return;
        }
        rows.forEach((row) => {
          defaultRuntime.log(
            `theme=${row.themeKey} version=${row.themeVersion} ticker=${row.ticker} score=${row.membershipScore.toFixed(3)} confidence=${row.confidence.toFixed(3)} status=${row.status} source=${row.source}`,
          );
        });
      });
    });

  research
    .command("sector-research")
    .description("Generate institutional sector cross-sectional research")
    .requiredOption("--sector <name>", "Sector name")
    .option("--tickers <csv>", "Optional explicit ticker universe override")
    .option("--benchmark <ticker>", "Optional benchmark ticker for factor attribution")
    .option("--lookback-days <n>", "Price lookback window", "365")
    .option("--top <n>", "Leaders/laggards count", "5")
    .option("--quality-attempts <n>", "Quality refinement attempts before hard fail", "4")
    .option("--allow-draft", "Allow output even if institutional quality gate fails", false)
    .option("--min-score <n>", "Institutional quality threshold [0-1]", "0.82")
    .option("--out <path>", "Write institutional report markdown to file")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const overrideTickers =
          typeof opts.tickers === "string" && opts.tickers.trim()
            ? parseTickersOption(opts.tickers as string)
            : undefined;
        const qualityAttempts = Math.max(1, parseOptionalNumber(opts["qualityAttempts"]) ?? 4);
        const strictMode = !Boolean(opts.allowDraft);
        const minScore = parseOptionalNumber(opts["minScore"]) ?? 0.82;
        let final:
          | {
              result: ReturnType<typeof computeSectorResearch>;
              report: ReturnType<typeof buildSectorInstitutionalReport>;
              gate: ReturnType<typeof evaluateCrossSectionQualityGate>;
              runId: number;
            }
          | undefined;
        let lastError: Error | undefined;
        for (let attempt = 1; attempt <= qualityAttempts; attempt += 1) {
          const result = computeSectorResearch({
            sector: opts.sector as string,
            tickers: overrideTickers,
            benchmarkTicker: opts.benchmark as string | undefined,
            lookbackDays: parseOptionalNumber(opts["lookbackDays"]) ?? 365,
            topN: parseOptionalNumber(opts.top) ?? 5,
            dbPath: opts.db as string,
          });
          const report = buildSectorInstitutionalReport({
            result,
            dbPath: opts.db as string,
            attempt,
          });
          const sectorArtifactId = `${result.sector}:${result.generatedAt.slice(0, 10)}:${result.tickers.length}`;
          const scenarioCoverageRatio =
            result.constituents.length > 0
              ? result.constituents.filter(
                  (row) =>
                    typeof row.expectedUpsideWithCatalystsPct === "number" ||
                    typeof row.expectedUpsidePct === "number",
                ).length / result.constituents.length
              : 0;
          const uniqueIndustryCount = new Set(
            result.constituents.map((row) => row.industry).filter(Boolean),
          ).size;
          const benchmarkContextScore = result.factorAttribution
            ? clamp01(
                0.65 * clamp01(result.factorAttribution.sampleSize / 63) +
                  0.35 * clamp01(result.factorAttribution.rSquared / 0.5),
              )
            : 0;
          const factorStabilityScore = result.factorAttribution?.rollingWindows.length
            ? clamp01(
                0.6 *
                  mean(
                    result.factorAttribution.rollingWindows.map((window) =>
                      clamp01(window.rSquared),
                    ),
                  ) +
                  0.4 * clamp01(result.factorAttribution.rollingWindows.length / 4),
              )
            : 0;
          const macroCoveragePct = result.factorAttribution?.macroFactors.length
            ? mean(result.factorAttribution.macroFactors.map((factor) => factor.coveragePct))
            : undefined;
          const sectorGate = evaluateCrossSectionQualityGate({
            artifactType: "sector_report",
            artifactId: sectorArtifactId,
            evidenceCoverageScore: result.metrics.evidenceCoverageScore,
            institutionalReadinessScore: result.metrics.institutionalReadinessScore,
            avgVariantConfidence: result.metrics.avgVariantConfidence,
            avgValuationConfidence: result.metrics.avgValuationConfidence,
            avgPortfolioConfidence: result.metrics.avgPortfolioConfidence,
            benchmarkContextScore,
            scenarioCoverageRatio,
            riskFlagCount: result.riskFlags.length,
            uniqueGroupCount: uniqueIndustryCount,
            factorStabilityScore,
            macroCoveragePct,
            narrativeClarityScore: report.quality.narrativeClarityScore,
            exhibitCount: report.quality.exhibitCount,
            minExhibitCount: 8,
            actionabilityScore: report.quality.actionabilityScore,
            freshness180dRatio: report.quality.freshness180dRatio,
            generatedAt: result.generatedAt,
            minScore,
          });
          const sectorGateRun = recordQualityGateRun({
            evaluation: sectorGate,
            metadata: {
              sector: result.sector,
              ticker_count: result.tickers.length,
              benchmark: opts.benchmark ?? null,
              quality_attempt: attempt,
            },
            dbPath: opts.db as string,
          });
          if (!strictMode || sectorGate.passed) {
            final = {
              result,
              report,
              gate: sectorGate,
              runId: sectorGateRun.id,
            };
            break;
          }
          const failed = sectorGate.checks.filter((check) => !check.passed);
          const details = failed.map((check) => `${check.name}: ${check.detail}`).join("; ");
          const err = new Error(
            `Institutional sector gate failed (run_id=${sectorGateRun.id} score=${sectorGate.score.toFixed(2)} min=${sectorGate.minScore.toFixed(2)} required_failures=${sectorGate.requiredFailures.join(",") || "none"}): ${details}`,
          );
          lastError = err;
          if (attempt < qualityAttempts) {
            defaultRuntime.error(
              `quality_refinement_retry attempt=${attempt}/${qualityAttempts} reason=${err.message}`,
            );
            continue;
          }
          throw err;
        }
        if (!final) throw lastError ?? new Error("Sector report generation failed");
        if (opts.out) {
          const outPath = path.resolve(opts.out as string);
          await fs.writeFile(outPath, `${final.report.markdown}\n`, "utf8");
          defaultRuntime.log(`Report written to ${outPath}`);
        } else {
          defaultRuntime.log(final.report.markdown);
        }
        defaultRuntime.log(
          `institutional_gate run_id=${final.runId} gate=${final.gate.gateName} score=${final.gate.score.toFixed(2)} min_score=${final.gate.minScore.toFixed(2)} passed=${final.gate.passed ? 1 : 0} artifact_id=${final.gate.artifactId}`,
        );
        defaultRuntime.log(
          `report_quality narrative_clarity=${final.report.quality.narrativeClarityScore.toFixed(2)} exhibits=${final.report.quality.exhibitCount} actionability=${final.report.quality.actionabilityScore.toFixed(2)} freshness_180d=${(final.report.quality.freshness180dRatio * 100).toFixed(1)}%`,
        );
        if (final.gate.requiredFailures.length) {
          defaultRuntime.error(
            `institutional_gate_required_failures=${final.gate.requiredFailures.join(",")}`,
          );
        }
      });
    });

  research
    .command("theme-research")
    .description("Generate institutional thematic cross-sector research")
    .requiredOption("--theme <name>", "Theme name")
    .option("--tickers <csv>", "Optional thematic ticker basket override")
    .option("--theme-version <n>", "Optional theme taxonomy version")
    .option("--min-membership-score <n>", "Minimum active membership score for theme registry")
    .option("--max-constituents <n>", "Max constituents to load from theme registry")
    .option("--lookback-days <n>", "Price lookback window", "365")
    .option("--top <n>", "Leaders/laggards count", "5")
    .option("--quality-attempts <n>", "Quality refinement attempts before hard fail", "4")
    .option("--allow-draft", "Allow output even if institutional quality gate fails", false)
    .option("--min-score <n>", "Institutional quality threshold [0-1]", "0.82")
    .option("--out <path>", "Write institutional report markdown to file")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const tickers =
          typeof opts.tickers === "string" && opts.tickers.trim()
            ? parseTickersOption(opts.tickers as string)
            : undefined;
        const qualityAttempts = Math.max(1, parseOptionalNumber(opts["qualityAttempts"]) ?? 4);
        const strictMode = !Boolean(opts.allowDraft);
        const minScore = parseOptionalNumber(opts["minScore"]) ?? 0.82;
        let final:
          | {
              result: ReturnType<typeof computeThemeResearch>;
              report: ReturnType<typeof buildThemeInstitutionalReport>;
              gate: ReturnType<typeof evaluateCrossSectionQualityGate>;
              runId: number;
            }
          | undefined;
        let lastError: Error | undefined;
        for (let attempt = 1; attempt <= qualityAttempts; attempt += 1) {
          const result = computeThemeResearch({
            theme: opts.theme as string,
            tickers,
            themeVersion: parseOptionalNumber(opts["themeVersion"]),
            minMembershipScore: parseOptionalNumber(opts["minMembershipScore"]),
            maxConstituents: parseOptionalNumber(opts["maxConstituents"]),
            lookbackDays: parseOptionalNumber(opts["lookbackDays"]) ?? 365,
            topN: parseOptionalNumber(opts.top) ?? 5,
            dbPath: opts.db as string,
          });
          const report = buildThemeInstitutionalReport({
            result,
            dbPath: opts.db as string,
            attempt,
          });
          const themeArtifactId = `${result.theme}:${result.generatedAt.slice(0, 10)}:${result.tickers.length}`;
          const scenarioCoverageRatio =
            result.constituents.length > 0
              ? result.constituents.filter(
                  (row) =>
                    typeof row.expectedUpsideWithCatalystsPct === "number" ||
                    typeof row.expectedUpsidePct === "number",
                ).length / result.constituents.length
              : 0;
          const uniqueSectorCount = new Set(
            result.constituents.map((row) => row.sector).filter(Boolean),
          ).size;
          const benchmarkRelativeScore = result.benchmarkRelative
            ? clamp01(
                0.65 * clamp01(result.benchmarkRelative.sampleSize / 63) +
                  0.35 * clamp01((result.benchmarkRelative.correlation ?? 0.35) / 0.8),
              )
            : 0;
          const factorAttributionScore = result.factorAttribution
            ? clamp01(
                0.7 * clamp01(result.factorAttribution.sampleSize / 63) +
                  0.3 * clamp01(result.factorAttribution.rSquared / 0.5),
              )
            : 0;
          const benchmarkContextScore =
            result.benchmarkRelative || result.factorAttribution
              ? clamp01(0.55 * benchmarkRelativeScore + 0.45 * factorAttributionScore)
              : 0;
          const factorStabilityScore = result.factorAttribution?.rollingWindows.length
            ? clamp01(
                0.6 *
                  mean(
                    result.factorAttribution.rollingWindows.map((window) =>
                      clamp01(window.rSquared),
                    ),
                  ) +
                  0.4 * clamp01(result.factorAttribution.rollingWindows.length / 4),
              )
            : 0;
          const macroCoveragePct = result.factorAttribution?.macroFactors.length
            ? mean(result.factorAttribution.macroFactors.map((factor) => factor.coveragePct))
            : undefined;
          const themeGate = evaluateCrossSectionQualityGate({
            artifactType: "theme_report",
            artifactId: themeArtifactId,
            evidenceCoverageScore: result.metrics.evidenceCoverageScore,
            institutionalReadinessScore: result.metrics.institutionalReadinessScore,
            avgVariantConfidence: result.metrics.avgVariantConfidence,
            avgValuationConfidence: result.metrics.avgValuationConfidence,
            avgPortfolioConfidence: result.metrics.avgPortfolioConfidence,
            benchmarkContextScore,
            scenarioCoverageRatio,
            riskFlagCount: result.riskFlags.length,
            uniqueGroupCount: uniqueSectorCount,
            factorStabilityScore,
            macroCoveragePct,
            narrativeClarityScore: report.quality.narrativeClarityScore,
            exhibitCount: report.quality.exhibitCount,
            minExhibitCount: 8,
            actionabilityScore: report.quality.actionabilityScore,
            freshness180dRatio: report.quality.freshness180dRatio,
            generatedAt: result.generatedAt,
            minScore,
          });
          const themeGateRun = recordQualityGateRun({
            evaluation: themeGate,
            metadata: {
              theme: result.theme,
              ticker_count: result.tickers.length,
              used_theme_registry: result.usedThemeRegistry,
              quality_attempt: attempt,
            },
            dbPath: opts.db as string,
          });
          if (!strictMode || themeGate.passed) {
            final = {
              result,
              report,
              gate: themeGate,
              runId: themeGateRun.id,
            };
            break;
          }
          const failed = themeGate.checks.filter((check) => !check.passed);
          const details = failed.map((check) => `${check.name}: ${check.detail}`).join("; ");
          const err = new Error(
            `Institutional theme gate failed (run_id=${themeGateRun.id} score=${themeGate.score.toFixed(2)} min=${themeGate.minScore.toFixed(2)} required_failures=${themeGate.requiredFailures.join(",") || "none"}): ${details}`,
          );
          lastError = err;
          if (attempt < qualityAttempts) {
            let hydration = "";
            if (attempt === 1) {
              hydration = await hydrateThemeEvidence({
                theme: opts.theme as string,
                tickers: result.tickers,
                dbPath: opts.db as string,
              });
            }
            defaultRuntime.error(
              `quality_refinement_retry attempt=${attempt}/${qualityAttempts} reason=${err.message}${hydration ? ` hydration=${hydration}` : ""}`,
            );
            continue;
          }
          throw err;
        }
        if (!final) throw lastError ?? new Error("Theme report generation failed");
        if (opts.out) {
          const outPath = path.resolve(opts.out as string);
          await fs.writeFile(outPath, `${final.report.markdown}\n`, "utf8");
          defaultRuntime.log(`Report written to ${outPath}`);
        } else {
          defaultRuntime.log(final.report.markdown);
        }
        defaultRuntime.log(
          `institutional_gate run_id=${final.runId} gate=${final.gate.gateName} score=${final.gate.score.toFixed(2)} min_score=${final.gate.minScore.toFixed(2)} passed=${final.gate.passed ? 1 : 0} artifact_id=${final.gate.artifactId}`,
        );
        defaultRuntime.log(
          `report_quality narrative_clarity=${final.report.quality.narrativeClarityScore.toFixed(2)} exhibits=${final.report.quality.exhibitCount} actionability=${final.report.quality.actionabilityScore.toFixed(2)} freshness_180d=${(final.report.quality.freshness180dRatio * 100).toFixed(1)}%`,
        );
        if (final.gate.requiredFailures.length) {
          defaultRuntime.error(
            `institutional_gate_required_failures=${final.gate.requiredFailures.join(",")}`,
          );
        }
      });
    });

  research
    .command("monitor")
    .description("Run thesis monitoring checks for one ticker and persist alerts")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .command("graph-build")
    .description("Build/update point-in-time research graph for a ticker")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--max-fundamentals <n>", "Max fundamental fact rows", "500")
    .option("--max-expectations <n>", "Max expectations rows", "160")
    .option("--max-filings <n>", "Max filing rows", "160")
    .option("--max-transcripts <n>", "Max transcript rows", "120")
    .option("--max-catalysts <n>", "Max catalyst rows", "120")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const summary = buildTickerPointInTimeGraph({
          ticker: opts.ticker as string,
          maxFundamentalFacts: Number.parseInt(opts["maxFundamentals"] as string, 10) || 500,
          maxExpectations: Number.parseInt(opts["maxExpectations"] as string, 10) || 160,
          maxFilings: Number.parseInt(opts["maxFilings"] as string, 10) || 160,
          maxTranscripts: Number.parseInt(opts["maxTranscripts"] as string, 10) || 120,
          maxCatalysts: Number.parseInt(opts["maxCatalysts"] as string, 10) || 120,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `graph_build ticker=${summary.ticker} entity_id=${summary.entityId} rows=${summary.rowsScanned} events_inserted=${summary.eventsInserted} events_updated=${summary.eventsUpdated} facts_inserted=${summary.factsInserted} facts_updated=${summary.factsUpdated}`,
        );
        summary.sourceStats.forEach((row) => {
          defaultRuntime.log(
            `  source=${row.source} rows=${row.rowsScanned} events_inserted=${row.eventsInserted} events_updated=${row.eventsUpdated} facts_inserted=${row.factsInserted} facts_updated=${row.factsUpdated}`,
          );
        });
      });
    });

  research
    .command("graph-snapshot")
    .description("Query point-in-time graph snapshot for a ticker as of a date")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--as-of <date>", "As-of date (YYYY-MM-DD); default=today")
    .option("--lookback-days <n>", "History window size", "730")
    .option("--event-limit <n>", "Event row limit", "40")
    .option("--fact-limit <n>", "Fact row limit", "300")
    .option("--metric-limit <n>", "Metric summary limit", "40")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const snapshot = getTickerPointInTimeSnapshot({
          ticker: opts.ticker as string,
          asOfDate: opts["asOf"] as string | undefined,
          lookbackDays: Number.parseInt(opts["lookbackDays"] as string, 10) || 730,
          eventLimit: Number.parseInt(opts["eventLimit"] as string, 10) || 40,
          factLimit: Number.parseInt(opts["factLimit"] as string, 10) || 300,
          metricLimit: Number.parseInt(opts["metricLimit"] as string, 10) || 40,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `graph_snapshot ticker=${snapshot.ticker} as_of=${snapshot.asOfDate} window_start=${snapshot.windowStartDate} entity=${snapshot.entityName ?? "n/a"} events=${snapshot.events.length} facts=${snapshot.facts.length} metrics=${snapshot.metrics.length}`,
        );
        if (!snapshot.metrics.length && !snapshot.events.length) {
          defaultRuntime.log(
            "No graph data found. Run `openclaw research graph-build --ticker <TICKER>` first.",
          );
          return;
        }
        snapshot.metrics.forEach((metric) => {
          if (typeof metric.latestValueNum === "number") {
            defaultRuntime.log(
              `metric=${metric.metricKey} latest=${metric.latestValueNum.toFixed(4)}${typeof metric.previousValueNum === "number" ? ` prev=${metric.previousValueNum.toFixed(4)}` : ""}${typeof metric.deltaValueNum === "number" ? ` delta=${metric.deltaValueNum.toFixed(4)}` : ""} unit=${metric.unit || "n/a"} as_of=${metric.latestAsOfDate} samples=${metric.samples}`,
            );
            return;
          }
          defaultRuntime.log(
            `metric=${metric.metricKey} latest_text=${metric.latestValueText ?? "n/a"} as_of=${metric.latestAsOfDate} samples=${metric.samples}`,
          );
        });
        snapshot.events.slice(0, 20).forEach((event) => {
          defaultRuntime.log(
            `event=${new Date(event.eventTime).toISOString()} type=${event.eventType} source=${event.sourceTable}:${event.sourceRefId} title=${event.title || "n/a"}`,
          );
        });
      });
    });

  research
    .command("provenance-report")
    .description("Audit tamper-evident provenance chain and signatures")
    .option("--event-type <name>", "Filter by event type")
    .option("--entity-type <name>", "Filter by entity type")
    .option("--entity-id <id>", "Filter by entity id")
    .option("--limit <n>", "Max events", "200")
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .command("quality-gate")
    .description("List institutional quality-gate runs and summary")
    .option("--artifact-type <kind>", "memo|sector_report|theme_report|all", "all")
    .option("--artifact-id <id>", "Optional artifact id")
    .option("--days <n>", "Lookback window in days", "30")
    .option("--limit <n>", "Result limit", "100")
    .option("--details", "Print failed-check details", false)
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const artifactType = parseQualityGateArtifactType(opts["artifactType"] as string);
        const runs = listQualityGateRuns({
          artifactType,
          artifactId: opts["artifactId"] as string | undefined,
          days: parseOptionalNumber(opts.days) ?? 30,
          limit: parseOptionalNumber(opts.limit) ?? 100,
          dbPath: opts.db as string,
        });
        const summary = summarizeQualityGateRuns(runs);
        defaultRuntime.log(
          `quality_gate_summary total=${summary.total} passed=${summary.passed} pass_rate=${(summary.passRate * 100).toFixed(1)}% avg_score=${summary.avgScore.toFixed(3)} avg_min_score=${summary.avgMinScore.toFixed(3)}`,
        );
        if (!runs.length) {
          defaultRuntime.log("No quality-gate runs found.");
          return;
        }
        runs.forEach((run) => {
          defaultRuntime.log(
            `quality_gate_run id=${run.id} gate=${run.gateName} artifact_type=${run.artifactType} artifact_id=${run.artifactId} score=${run.score.toFixed(3)} min_score=${run.minScore.toFixed(3)} passed=${run.passed ? 1 : 0} required_failures=${run.requiredFailures.length ? run.requiredFailures.join(",") : "none"} created_at=${new Date(run.createdAt).toISOString()}`,
          );
          if (Boolean(opts.details)) {
            run.checks
              .filter((check) => !check.passed)
              .forEach((check) => {
                defaultRuntime.log(
                  `quality_gate_failure run_id=${run.id} check=${check.name} detail=${check.detail}`,
                );
              });
          }
        });
      });
    });

  research
    .command("quality-gate-ci")
    .description("Fail when quality-gate pass rate or score regresses")
    .option("--artifact-type <kind>", "memo|sector_report|theme_report|all", "all")
    .option("--lookback-days <n>", "Lookback window in days", "90")
    .option("--recent-days <n>", "Recent window in days", "14")
    .option("--min-recent-samples <n>", "Minimum samples in recent window", "10")
    .option("--min-recent-pass-rate <n>", "Minimum recent pass rate [0-1]", "0.8")
    .option("--min-recent-avg-score <n>", "Minimum recent average score [0-1]", "0.82")
    .option("--max-pass-rate-drop <n>", "Maximum pass-rate drop vs baseline [0-1]", "0.08")
    .option("--max-avg-score-drop <n>", "Maximum avg-score drop vs baseline [0-1]", "0.05")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const artifactType = parseQualityGateArtifactType(opts["artifactType"] as string);
        const result = evaluateQualityGateRegression({
          artifactType,
          lookbackDays: parseOptionalNumber(opts["lookbackDays"]),
          recentDays: parseOptionalNumber(opts["recentDays"]),
          minRecentSamples: parseOptionalNumber(opts["minRecentSamples"]),
          minRecentPassRate: parseOptionalNumber(opts["minRecentPassRate"]),
          minRecentAvgScore: parseOptionalNumber(opts["minRecentAvgScore"]),
          maxPassRateDrop: parseOptionalNumber(opts["maxPassRateDrop"]),
          maxAvgScoreDrop: parseOptionalNumber(opts["maxAvgScoreDrop"]),
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `quality_gate_regression artifact_type=${result.artifactType ?? "all"} recent_count=${result.recentCount} baseline_count=${result.baselineCount} recent_pass_rate=${(result.recentPassRate * 100).toFixed(1)}% baseline_pass_rate=${(result.baselinePassRate * 100).toFixed(1)}% recent_avg_score=${result.recentAvgScore.toFixed(3)} baseline_avg_score=${result.baselineAvgScore.toFixed(3)} passed=${result.passed ? 1 : 0}`,
        );
        if (!result.passed) {
          throw new Error(`Quality gate regression failed: ${result.reasons.join("; ")}`);
        }
      });
    });

  research
    .command("memo")
    .description("Generate citation-enforced research memo")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .requiredOption("--question <text>", "Research question")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--out <path>", "Write memo markdown to file")
    .option("--allow-draft", "Allow output even if institutional quality gate fails", false)
    .option("--min-score <n>", "Institutional quality threshold [0-1]", "0.8")
    .option("--quality-attempts <n>", "Quality refinement attempts before hard fail", "4")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const qualityAttempts = Math.max(1, parseOptionalNumber(opts["qualityAttempts"]) ?? 4);
        const strictMode = !Boolean(opts.allowDraft);
        const baseQuestion = (opts.question as string).trim();
        const refinementHints = [
          "",
          "Prioritize multi-source evidence (filings, transcript, expectations) and resolve contradictions explicitly.",
          "Strengthen actionability with entry trigger, sizing bands, and falsification thresholds.",
          "Stress-test bear/base/bull scenarios and justify final stance against disconfirming evidence.",
        ];

        let result: Awaited<ReturnType<typeof generateMemoAsync>> | undefined;
        let lastError: Error | undefined;

        for (let attempt = 1; attempt <= qualityAttempts; attempt += 1) {
          const hint =
            refinementHints[Math.min(refinementHints.length - 1, attempt - 1)] ??
            refinementHints[refinementHints.length - 1] ??
            "";
          const question = hint ? `${baseQuestion} ${hint}` : baseQuestion;
          const maxEvidence = Math.min(40, 12 + (attempt - 1) * 6);
          try {
            result = await generateMemoAsync({
              ticker: opts.ticker as string,
              question,
              dbPath: opts.db as string,
              maxEvidence,
              enforceInstitutionalGrade: strictMode,
              minQualityScore: Number.parseFloat(opts.minScore as string) || 0.8,
            });
            if (attempt > 1) {
              defaultRuntime.log(`quality_refinement attempts_used=${attempt}`);
            }
            break;
          } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error));
            lastError = err;
            const canRetry =
              strictMode &&
              attempt < qualityAttempts &&
              (err.message.includes("Institutional-grade quality gate failed") ||
                err.message.includes("Insufficient evidence"));
            if (!canRetry) {
              throw err;
            }
            defaultRuntime.error(
              `quality_refinement_retry attempt=${attempt}/${qualityAttempts} reason=${err.message}`,
            );
          }
        }

        if (!result) {
          throw (
            lastError ??
            new Error(`Memo generation failed after ${qualityAttempts} quality attempts`)
          );
        }
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
          `institutional_gate run_id=${result.qualityGateRunId} gate=${result.qualityGate.gateName} score=${result.qualityGate.score.toFixed(2)} min_score=${result.qualityGate.minScore.toFixed(2)} passed=${result.qualityGate.passed ? 1 : 0} artifact_id=${result.qualityGate.artifactId}`,
        );
        defaultRuntime.log(
          `actionability_score=${result.quality.actionabilityScore.toFixed(2)} adversarial_coverage_score=${result.quality.adversarialCoverageScore.toFixed(2)} calibration_mode=${result.quality.calibration.mode} calibration_score=${result.quality.calibration.score.toFixed(2)} calibration_samples=${result.quality.calibration.sampleCount}`,
        );
        defaultRuntime.log(
          `research_cell_passed=${result.researchCell.debate.passed ? 1 : 0} research_cell_consensus=${result.researchCell.debate.consensusScore.toFixed(2)} research_cell_final_stance=${result.researchCell.allocator.finalStance}`,
        );
        defaultRuntime.log(
          `decision_recommendation=${result.portfolioDecision.recommendation} decision_score=${result.portfolioDecision.decisionScore.toFixed(2)} decision_confidence=${result.portfolioDecision.confidence.toFixed(2)} decision_breaches=${result.portfolioDecision.riskBreaches.length}`,
        );
        if (result.quality.requiredFailures.length) {
          defaultRuntime.error(`required_failures=${result.quality.requiredFailures.join(",")}`);
        }
        if (result.qualityGate.requiredFailures.length) {
          defaultRuntime.error(
            `institutional_gate_required_failures=${result.qualityGate.requiredFailures.join(",")}`,
          );
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
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
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--dest <dir>", "Backup destination", path.join(process.cwd(), "data", "backups"))
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const out = writeBackup({ dbPath: opts.db as string, destDir: opts.dest as string });
        defaultRuntime.log(`Backup created: ${out}`);
      });
    });
}
