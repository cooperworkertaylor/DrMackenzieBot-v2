import type { Command } from "commander";
import * as fsSync from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  DEFAULT_CONCEPTS,
  buildTimeSeriesExhibit,
  fetchCompanyFacts,
  resolveTickerToCik,
  timeSeriesToCsv,
  timeSeriesToMarkdown,
} from "../agents/sec-xbrl-timeseries.js";
import { writeArtifactManifest, writeFileArtifactManifest } from "../research/artifact-manifest.js";
import { buildArtifactPreflight } from "../research/artifact-preflight.js";
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
  renderResearchEvalScorecard,
  latestResearchEvalHarnessRuns,
  loadResearchEvalTaskSet,
  runResearchEvalTaskSet,
} from "../research/eval-harness.js";
import {
  executionTraceReport,
  logExecutionTrace,
  type ExecutionTraceStepInput,
} from "../research/execution-trace.js";
import {
  analyzeExternalResearchGuidanceDrift,
  analyzeExternalResearchManagementCredibility,
  compareExternalResearchPeers,
  detectExternalResearchSourceConflicts,
} from "../research/external-research-advanced.js";
import {
  buildPersonalizedResearchSnapshot,
  ingestResearchNotebookEntry,
  listResearchNotebookEntries,
  listResearchUserPreferences,
  upsertResearchUserPreference,
} from "../research/external-research-personalization.js";
import {
  computeWeeklyNewsletterDigest,
  ingestExternalResearchDocument,
  parseNewsletterSourceSpecs,
  resolveNewsletterSourcesFromEnv,
  renderWeeklyNewsletterDigestMarkdown,
  syncNewsletterSources,
} from "../research/external-research.js";
import {
  createResearchBackup,
  getResearchHealthSnapshot,
  replayFailedQuickrunJobs,
  restoreResearchBackup,
  runResearchSchedulerLoop,
  runResearchSchedulerPass,
  runResearchWorkerLoop,
} from "../research/ops.js";
import {
  runResearchServiceInstall,
  runResearchServiceRestart,
  runResearchServiceStart,
  runResearchServiceStatus,
  runResearchServiceStop,
  runResearchServiceUninstall,
} from "./research-daemon.js";
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
import { searchResearch, syncEmbeddings } from "../research/vector-search.js";
import { defaultRuntime } from "../runtime.js";
import { formatDocsLink } from "../terminal/links.js";
import { theme } from "../terminal/theme.js";
import { runCompanyPipelineV2, runThemePipelineV2 } from "../v2/pipeline/v2-pipeline.js";
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

const deriveArtifactSeriesKey = (outPath: string): string => {
  const parsed = path.parse(outPath);
  return parsed.name.replace(/\.v\d+$/i, "");
};

const deriveSeriesManifestPath = (outPath: string): string =>
  path.join(path.dirname(outPath), "artifact.json");

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const escapeHtml = (value: string): string =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

const formatBuiltAtEt = (date: Date): string => {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? "";
  const yyyy = get("year");
  const mm = get("month");
  const dd = get("day");
  const hh = get("hour");
  const min = get("minute");
  return `${yyyy}-${mm}-${dd} ${hh}:${min} ET`;
};

const resolveChromeExecutablePath = (explicit?: string): string | undefined => {
  const candidate = (explicit ?? "").trim();
  if (candidate && fsSync.existsSync(candidate)) return candidate;
  const env = (process.env.OPENCLAW_CHROME_PATH ?? process.env.CHROME_PATH ?? "").trim();
  if (env && fsSync.existsSync(env)) return env;
  if (process.platform === "darwin") {
    const macChrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
    if (fsSync.existsSync(macChrome)) return macChrome;
    const macCanary = "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary";
    if (fsSync.existsSync(macCanary)) return macCanary;
  }
  return undefined;
};

const assertMacminiBrowserAllowed = (feature: string): void => {
  const hostRole =
    process.env.OPENCLAW_HOST_ROLE ??
    process.env.OPENCLAW_AGENT_ROLE ??
    process.env.OPENCLAW_AGENT ??
    "";
  const allow =
    process.env.OPENCLAW_ALLOW_BROWSER ??
    process.env.OPENCLAW_ALLOW_CHROME ??
    process.env.OPENCLAW_RENDER_PDF_ALLOWED ??
    "";
  const normalizedRole = hostRole.trim().toLowerCase();
  const normalizedAllow = allow.trim();
  if (normalizedRole === "macmini" && normalizedAllow === "1") return;
  throw new Error(
    `${feature} uses a local browser (Chrome/Playwright) and is restricted to the macmini agent. Refusing to run here. Set OPENCLAW_HOST_ROLE=macmini and OPENCLAW_ALLOW_BROWSER=1 to proceed (on macmini only).`,
  );
};

const assertMacminiAgentOnly = (feature: string): void => {
  const hostRole =
    process.env.OPENCLAW_HOST_ROLE ??
    process.env.OPENCLAW_AGENT_ROLE ??
    process.env.OPENCLAW_AGENT ??
    "";
  const normalizedRole = hostRole.trim().toLowerCase();
  if (normalizedRole === "macmini") return;

  // Secondary check to reduce misconfig pain on the macmini if env isn't set.
  const hostname = os.hostname().trim().toLowerCase();
  if (hostname.includes("coopers") && hostname.includes("mini")) return;

  throw new Error(
    `${feature} is restricted to the macmini agent. Refusing to run here. Set OPENCLAW_HOST_ROLE=macmini (and run the agent work on macmini only).`,
  );
};

const BAD_DASHES = /[\u2010\u2011\u2012\u2013\u2014\u2212]/g;

const resolveNextVersionedArtifactPath = async (params: {
  dir: string;
  base: string;
  ext: string;
}): Promise<{ outPath: string; version: number }> => {
  const dir = path.resolve(params.dir);
  const base = params.base.trim();
  const ext = params.ext.replace(/^\./, "").trim() || "pdf";
  let maxVersion = 0;
  try {
    const entries = await fs.readdir(dir);
    const re = new RegExp(`^${escapeRegExp(base)}\\.v(\\d+)\\.${escapeRegExp(ext)}$`, "i");
    for (const entry of entries) {
      const match = re.exec(entry);
      if (!match) continue;
      const version = Number.parseInt(match[1] ?? "", 10);
      if (Number.isFinite(version) && version > maxVersion) maxVersion = version;
    }
  } catch {
    // directory may not exist yet; treat as v0
  }
  const version = maxVersion + 1;
  return { outPath: path.join(dir, `${base}.v${version}.${ext}`), version };
};

const parseSourceTypeOption = (value?: string) => {
  const normalized = (value ?? "").trim().toLowerCase();
  if (normalized === "email_research" || normalized === "newsletter" || normalized === "manual") {
    return normalized;
  }
  throw new Error(`Invalid source type: ${value}`);
};

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

const resolveResearchOutboundFetchEnabled = (): boolean => {
  const raw = process.env.OPENCLAW_RESEARCH_FORCE_OUTBOUND_FETCH?.trim();
  if (!raw) return true;
  try {
    return parseBooleanOption(raw);
  } catch {
    return true;
  }
};

const clamp01 = (value: number): number => Math.max(0, Math.min(1, value));

const mean = (values: number[]): number =>
  values.length > 0 ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const MAX_HYDRATION_ATTEMPTS_PER_KEY = 3;
const THEME_HYDRATION_ATTEMPTS = new Map<string, number>();
const MEMO_HYDRATION_ATTEMPTS = new Map<string, number>();

const canAttemptHydration = (params: {
  attempts: Map<string, number>;
  key: string;
  maxAttempts?: number;
}): boolean => {
  const maxAttempts = Math.max(1, Math.round(params.maxAttempts ?? MAX_HYDRATION_ATTEMPTS_PER_KEY));
  const current = params.attempts.get(params.key) ?? 0;
  if (current >= maxAttempts) return false;
  params.attempts.set(params.key, current + 1);
  return true;
};

const hydrateTickerEvidence = async (params: {
  ticker: string;
  dbPath?: string;
}): Promise<string> => {
  const ticker = params.ticker.trim().toUpperCase();
  if (!ticker) return "skipped(no-ticker)";
  const key = `${ticker}::${params.dbPath ?? "default"}`;
  if (!canAttemptHydration({ attempts: MEMO_HYDRATION_ATTEMPTS, key })) {
    return "skipped(max-hydration-attempts)";
  }
  const userAgent =
    process.env.SEC_USER_AGENT?.trim() || process.env.SEC_EDGAR_USER_AGENT?.trim() || undefined;
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
  await run("prices", async () => {
    await ingestPrices(ticker, { dbPath: params.dbPath });
  });
  await run("fundamentals", async () => {
    await ingestFundamentals(ticker, {
      userAgent,
      dbPath: params.dbPath,
    });
  });
  await run("expectations", async () => {
    await ingestExpectations(ticker, { dbPath: params.dbPath });
  });
  await run("filings", async () => {
    await ingestFilings(ticker, {
      limit: 20,
      userAgent,
      dbPath: params.dbPath,
    });
  });
  await run("macro", async () => {
    await ingestDefaultMacroFactors({
      dbPath: params.dbPath,
    });
  });
  await run("embed", async () => {
    await syncEmbeddings(params.dbPath);
  });
  return notes.join(",");
};

const hydrateThemeEvidence = async (params: {
  theme: string;
  tickers: string[];
  benchmarkTicker?: string;
  dbPath?: string;
}): Promise<string> => {
  const key = `${params.theme.trim().toLowerCase()}::${params.dbPath ?? "default"}`;
  if (!canAttemptHydration({ attempts: THEME_HYDRATION_ATTEMPTS, key })) {
    return "skipped(max-hydration-attempts)";
  }
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
  const benchmarkTicker = params.benchmarkTicker?.trim().toUpperCase() ?? "";
  if (benchmarkTicker && !tickers.includes(benchmarkTicker)) {
    await run(`${benchmarkTicker}:prices`, async () => {
      await ingestPrices(benchmarkTicker, { dbPath: params.dbPath });
    });
  }
  await run("macro:default", async () => {
    await ingestDefaultMacroFactors({
      dbPath: params.dbPath,
    });
  });
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

const parseArtifactKind = (value?: string): "memo" | "sector_report" | "theme_report" => {
  const normalized = (value ?? "").trim().toLowerCase();
  if (normalized === "memo" || normalized === "sector_report" || normalized === "theme_report") {
    return normalized;
  }
  throw new Error(`Invalid artifact kind: ${value}`);
};

const parsePdfFormat = (value?: string): "Letter" | "A4" => {
  const normalized = (value ?? "").trim().toLowerCase();
  if (!normalized || normalized === "letter" || normalized === "us-letter") return "Letter";
  if (normalized === "a4") return "A4";
  throw new Error(`Invalid PDF format: ${value} (expected: letter|a4)`);
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
        assertMacminiAgentOnly("research sec-xbrl");
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
    .command("ingest-external")
    .description("Ingest external research text into research DB for retrieval/memos")
    .requiredOption("--title <text>", "Document title")
    .option("--content <text>", "Document content")
    .option("--content-file <path>", "Path to content file")
    .option("--subject <text>", "Original subject line")
    .option("--source-type <kind>", "email_research|newsletter|manual", "manual")
    .option(
      "--provider <name>",
      "Provider tag (substack|stratechery|diff|semianalysis|other)",
      "other",
    )
    .option("--sender <value>", "Sender/author email")
    .option("--external-id <id>", "External message/document id")
    .option("--url <url>", "Source URL")
    .option("--ticker <symbol>", "Ticker tag (optional)")
    .option("--published-at <iso>", "Published timestamp (ISO)")
    .option("--received-at <iso>", "Received timestamp (ISO)")
    .option("--tags <csv>", "Comma-separated tags")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const contentInline = (opts.content as string | undefined)?.trim();
        const contentFile = (opts["contentFile"] as string | undefined)?.trim();
        let content = contentInline ?? "";
        if (!content && contentFile) {
          content = await fs.readFile(path.resolve(contentFile), "utf8");
          content = content.trim();
        }
        if (!content) {
          throw new Error("Provide --content or --content-file");
        }
        const result = ingestExternalResearchDocument({
          sourceType: parseSourceTypeOption(opts["sourceType"] as string),
          provider: opts.provider as string,
          sender: opts.sender as string | undefined,
          externalId: opts["externalId"] as string | undefined,
          title: opts.title as string,
          subject: opts.subject as string | undefined,
          content,
          url: opts.url as string | undefined,
          ticker: opts.ticker as string | undefined,
          publishedAt: opts["publishedAt"] as string | undefined,
          receivedAt: opts["receivedAt"] as string | undefined,
          tags: parseCsvListOption((opts.tags as string | undefined) ?? ""),
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `external_ingest id=${result.id} source_type=${result.sourceType} provider=${result.provider} chunks=${result.chunks} title="${result.title}"`,
        );
      });
    });

  research
    .command("newsletter-sync")
    .description(
      "Fetch newsletter sources (including paywalled archives with cookies) and ingest full text",
    )
    .option("--source <spec>", "Source spec: provider|url|ticker (repeatable)", collectOption, [])
    .option("--sources-file <path>", "Text file of source specs (one per line)")
    .option("--providers <csv>", "Provider filter (substack,stratechery,diff,semianalysis,other)")
    .option("--since-date <yyyy-mm-dd>", "Only ingest docs published on/after this date")
    .option("--sitemap-max-urls <n>", "Max sitemap article URLs considered per source", "5000")
    .option("--no-sitemaps", "Disable sitemap crawl expansion")
    .option("--max-links-per-source <n>", "Max article links fetched from each source", "10")
    .option("--max-docs <n>", "Max article pages fetched in this run", "50")
    .option("--user-agent <ua>", "HTTP user-agent override")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const inlineSourceSpecs = (opts.source as string[] | undefined) ?? [];
        let specBlob = inlineSourceSpecs.join("\n");
        const sourcesFile = (opts["sourcesFile"] as string | undefined)?.trim();
        if (sourcesFile) {
          const fromFile = await fs.readFile(path.resolve(sourcesFile), "utf8");
          specBlob = specBlob ? `${specBlob}\n${fromFile}` : fromFile;
        }
        const sources = specBlob.trim()
          ? parseNewsletterSourceSpecs(specBlob)
          : resolveNewsletterSourcesFromEnv(process.env);

        const result = await syncNewsletterSources({
          dbPath: opts.db as string,
          sources,
          providers:
            typeof opts.providers === "string" && opts.providers.trim()
              ? parseCsvListOption(opts.providers as string)
              : undefined,
          sinceDate: opts["sinceDate"] as string | undefined,
          useSitemaps: opts.sitemaps as boolean,
          sitemapMaxUrls: parseOptionalNumber(opts["sitemapMaxUrls"]) ?? 5000,
          maxLinksPerSource: parseOptionalNumber(opts["maxLinksPerSource"]) ?? 10,
          maxDocs: parseOptionalNumber(opts["maxDocs"]) ?? 50,
          userAgent: opts["userAgent"] as string | undefined,
        });

        defaultRuntime.log(
          `newsletter_sync sources=${result.sources} attempted=${result.attempted} ingested=${result.ingested} skipped=${result.skipped} failures=${result.failures}`,
        );
        const preview = result.docs.slice(0, 40);
        preview.forEach((doc) => {
          if (doc.ingested) {
            defaultRuntime.log(
              `- ingested provider=${doc.provider} title="${doc.title}" url=${doc.url} id=${doc.documentId} chunks=${doc.chunks}`,
            );
            return;
          }
          defaultRuntime.error(
            `- skipped provider=${doc.provider} url=${doc.url} reason=${doc.reason ?? "unknown"}`,
          );
        });
      });
    });

  research
    .command("newsletter-summary")
    .description(
      "Generate weekly Substack/Stratechery/The Diff/Semianalysis digest with read-in-full recommendations",
    )
    .option("--lookback-days <n>", "Lookback window in days", "7")
    .option("--limit <n>", "Max documents to evaluate", "80")
    .option("--providers <csv>", "Provider filter (substack,stratechery,diff,semianalysis,other)")
    .option("--out <path>", "Write digest markdown to file")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const digest = computeWeeklyNewsletterDigest({
          dbPath: opts.db as string,
          lookbackDays: parseOptionalNumber(opts["lookbackDays"]) ?? 7,
          limit: parseOptionalNumber(opts.limit) ?? 80,
          providers:
            typeof opts.providers === "string" && opts.providers.trim()
              ? parseCsvListOption(opts.providers as string)
              : undefined,
        });
        const markdown = renderWeeklyNewsletterDigestMarkdown(digest);
        if (opts.out) {
          const outPath = path.resolve(opts.out as string);
          await fs.writeFile(outPath, `${markdown}\n`, "utf8");
          defaultRuntime.log(`Digest written to ${outPath}`);
        } else {
          defaultRuntime.log(markdown);
        }
        defaultRuntime.log(
          `newsletter_digest total=${digest.totalDocs} read_in_full=${digest.readInFull.length} quick_scan=${digest.quickScan.length} lookback_days=${digest.lookbackDays}`,
        );
      });
    });

  research
    .command("source-conflicts")
    .description("Detect material conflicts across external research sources for a ticker")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--lookback-days <n>", "Lookback window in days", "90")
    .option("--limit <n>", "Max conflicts to return", "8")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const report = detectExternalResearchSourceConflicts({
          ticker: opts.ticker as string,
          dbPath: opts.db as string,
          lookbackDays: parseOptionalNumber(opts["lookbackDays"]) ?? 90,
          maxConflicts: parseOptionalNumber(opts.limit) ?? 8,
        });
        if (opts.json) {
          defaultRuntime.log(`${JSON.stringify(report, null, 2)}\n`);
          return;
        }
        defaultRuntime.log(report.markdown);
      });
    });

  research
    .command("compare-peers")
    .description("Compare the latest external research posture across two tickers")
    .requiredOption("--left <symbol>", "Left ticker")
    .requiredOption("--right <symbol>", "Right ticker")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const comparison = compareExternalResearchPeers({
          leftTicker: opts.left as string,
          rightTicker: opts.right as string,
          dbPath: opts.db as string,
        });
        if (opts.json) {
          defaultRuntime.log(`${JSON.stringify(comparison, null, 2)}\n`);
          return;
        }
        defaultRuntime.log(comparison.markdown);
      });
    });

  research
    .command("guidance-drift")
    .description("Analyze drift in external-research guidance and key metrics for a ticker")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--limit <n>", "Max drift items to return", "6")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const report = analyzeExternalResearchGuidanceDrift({
          ticker: opts.ticker as string,
          dbPath: opts.db as string,
          limit: parseOptionalNumber(opts.limit) ?? 6,
        });
        if (opts.json) {
          defaultRuntime.log(`${JSON.stringify(report, null, 2)}\n`);
          return;
        }
        defaultRuntime.log(report.markdown);
      });
    });

  research
    .command("management-credibility")
    .description("Track contradictions in management/guidance language across external research evidence")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--limit <n>", "Max contradiction alerts to return", "6")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const report = analyzeExternalResearchManagementCredibility({
          ticker: opts.ticker as string,
          dbPath: opts.db as string,
          maxAlerts: parseOptionalNumber(opts.limit) ?? 6,
        });
        if (opts.json) {
          defaultRuntime.log(`${JSON.stringify(report, null, 2)}\n`);
          return;
        }
        defaultRuntime.log(report.markdown);
      });
    });

  research
    .command("prefs-set")
    .description("Set a persisted research preference")
    .requiredOption("--key <text>", "Preference key")
    .option("--value <text>", "Preference text value")
    .option("--value-json <json>", "Preference JSON value")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const valueJson =
          typeof opts["valueJson"] === "string" && opts["valueJson"].trim()
            ? (JSON.parse(opts["valueJson"] as string) as Record<string, unknown>)
            : undefined;
        const row = upsertResearchUserPreference({
          key: opts.key as string,
          valueText: opts.value as string | undefined,
          valueJson,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `preference key=${row.key} value=${row.valueText || JSON.stringify(row.valueJson)}`,
        );
      });
    });

  research
    .command("prefs-list")
    .description("List persisted research preferences")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rows = listResearchUserPreferences({ dbPath: opts.db as string });
        if (opts.json) {
          defaultRuntime.log(`${JSON.stringify(rows, null, 2)}\n`);
          return;
        }
        if (!rows.length) {
          defaultRuntime.log("No research preferences stored.");
          return;
        }
        rows.forEach((row) => {
          defaultRuntime.log(`- ${row.key}: ${row.valueText || JSON.stringify(row.valueJson)}`);
        });
      });
    });

  research
    .command("notebook-add")
    .description("Add a notebook entry and ingest it into the research evidence graph")
    .requiredOption("--title <text>", "Notebook entry title")
    .option("--content <text>", "Notebook entry content")
    .option("--content-file <path>", "Path to notebook content file")
    .option("--ticker <symbol>", "Optional ticker tag")
    .option("--tags <csv>", "Comma-separated tags")
    .option("--source <text>", "Notebook source tag", "manual")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const contentInline = (opts.content as string | undefined)?.trim();
        const contentFile = (opts["contentFile"] as string | undefined)?.trim();
        let content = contentInline ?? "";
        if (!content && contentFile) {
          content = (await fs.readFile(path.resolve(contentFile), "utf8")).trim();
        }
        if (!content) throw new Error("Provide --content or --content-file");
        const result = ingestResearchNotebookEntry({
          title: opts.title as string,
          content,
          ticker: opts.ticker as string | undefined,
          tags: parseCsvListOption((opts.tags as string | undefined) ?? ""),
          source: opts.source as string | undefined,
          dbPath: opts.db as string,
        });
        defaultRuntime.log(
          `notebook_entry id=${result.entry.id} external_document_id=${result.entry.externalDocumentId ?? "n/a"} report_id=${result.ingest.reportId ?? "n/a"}`,
        );
      });
    });

  research
    .command("notebook-list")
    .description("List stored research notebook entries")
    .option("--ticker <symbol>", "Optional ticker filter")
    .option("--limit <n>", "Rows to return", "20")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rows = listResearchNotebookEntries({
          ticker: opts.ticker as string | undefined,
          limit: parseOptionalNumber(opts.limit) ?? 20,
          dbPath: opts.db as string,
        });
        if (opts.json) {
          defaultRuntime.log(`${JSON.stringify(rows, null, 2)}\n`);
          return;
        }
        if (!rows.length) {
          defaultRuntime.log("No notebook entries found.");
          return;
        }
        rows.forEach((row) => {
          defaultRuntime.log(
            `- ${new Date(row.createdAt).toISOString()} ${row.ticker ?? "unscoped"} ${row.title}${row.tags.length ? ` tags=${row.tags.join(",")}` : ""}`,
          );
        });
      });
    });

  research
    .command("personalized")
    .description("Build a personalized ticker snapshot from preferences, notebook entries, and current thesis state")
    .requiredOption("--ticker <symbol>", "Ticker symbol")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const snapshot = buildPersonalizedResearchSnapshot({
          ticker: opts.ticker as string,
          dbPath: opts.db as string,
        });
        if (opts.json) {
          defaultRuntime.log(`${JSON.stringify(snapshot, null, 2)}\n`);
          return;
        }
        defaultRuntime.log(snapshot.markdown);
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
        assertMacminiAgentOnly("research position-decision");
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
    .option("--print-metrics", "Print quality/telemetry metrics to stdout", false)
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        assertMacminiAgentOnly("research sector-research");
        const overrideTickers =
          typeof opts.tickers === "string" && opts.tickers.trim()
            ? parseTickersOption(opts.tickers as string)
            : undefined;
        const qualityAttempts = Math.max(1, parseOptionalNumber(opts["qualityAttempts"]) ?? 4);
        const strictMode = !Boolean(opts.allowDraft);
        const minScore = parseOptionalNumber(opts["minScore"]) ?? 0.82;
        const forceOutboundFetch = resolveResearchOutboundFetchEnabled();
        if (forceOutboundFetch) {
          try {
            const hydrationUniverse = computeSectorResearch({
              sector: opts.sector as string,
              tickers: overrideTickers,
              benchmarkTicker: opts.benchmark as string | undefined,
              lookbackDays: parseOptionalNumber(opts["lookbackDays"]) ?? 365,
              topN: parseOptionalNumber(opts.top) ?? 5,
              dbPath: opts.db as string,
            });
            const hydration = await hydrateThemeEvidence({
              theme: `sector:${String(opts.sector)}`,
              tickers: hydrationUniverse.tickers,
              benchmarkTicker: (opts.benchmark as string | undefined) ?? undefined,
              dbPath: opts.db as string,
            });
            if (hydration) {
              defaultRuntime.log(`research_outbound_hydration sector=${opts.sector} ${hydration}`);
            }
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            defaultRuntime.error(
              `research_outbound_hydration sector=${opts.sector} skip(${message})`,
            );
          }
        }
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
          const sectorGate =
            report.outputGate ??
            evaluateCrossSectionQualityGate({
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
              output_gate_repairs: report.outputGateRepairs ?? [],
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
            const remediation = await hydrateThemeEvidence({
              theme: `sector:${String(opts.sector)}`,
              tickers: result.tickers,
              benchmarkTicker: (opts.benchmark as string | undefined) ?? undefined,
              dbPath: opts.db as string,
            });
            defaultRuntime.error(
              `quality_refinement_retry attempt=${attempt}/${qualityAttempts} reason=${err.message}${remediation ? ` remediation=${remediation}` : ""}`,
            );
            continue;
          }
          throw err;
        }
        if (!final) throw lastError ?? new Error("Sector report generation failed");
        const shouldPrintMetrics = Boolean(opts.printMetrics);
        if (opts.out) {
          const outPath = path.resolve(opts.out as string);
          await fs.writeFile(outPath, `${final.report.markdown}\n`, "utf8");
          const manifestRes = await writeArtifactManifest({
            kind: "sector_report",
            outPath,
            markdown: final.report.markdown,
            seriesKey: deriveArtifactSeriesKey(outPath),
            seriesManifestPath: deriveSeriesManifestPath(outPath),
            metrics: {
              sector: String(opts.sector),
              tickers: final.result.tickers.length,
              narrative_clarity: Number(final.report.quality.narrativeClarityScore.toFixed(4)),
              exhibits: final.report.quality.exhibitCount,
              actionability: Number(final.report.quality.actionabilityScore.toFixed(4)),
              freshness_180d_ratio: Number(final.report.quality.freshness180dRatio.toFixed(4)),
            },
            gate: { runId: final.runId, ...final.gate },
          });
          defaultRuntime.log(`Report written to ${outPath}`);
          defaultRuntime.log(`Manifest written to ${manifestRes.manifestPath}`);
          if (manifestRes.seriesManifestPath) {
            defaultRuntime.log(`Series manifest written to ${manifestRes.seriesManifestPath}`);
          }
          const unicodeDashCount = Number(manifestRes.manifest.metrics.unicodeDashCount ?? 0);
          if (Number.isFinite(unicodeDashCount) && unicodeDashCount > 0) {
            throw new Error(
              `Unicode dash characters detected (count=${unicodeDashCount}). Replace with ASCII hyphens before delivery.`,
            );
          }
          const sourcesCount = Number(manifestRes.manifest.metrics.sourcesCount ?? 0);
          if (!Number.isFinite(sourcesCount) || sourcesCount <= 0) {
            throw new Error(
              `Sources appendix missing or empty (sourcesCount=${sourcesCount}). Refusing to proceed.`,
            );
          }
          if (manifestRes.manifest.unchangedFromPrevious) {
            defaultRuntime.error(`artifact_unchanged=1 sha256=${manifestRes.manifest.sha256}`);
            throw new Error(
              `Artifact unchanged (sha256=${manifestRes.manifest.sha256}). Refusing to proceed.`,
            );
          }
        } else {
          defaultRuntime.log(final.report.markdown);
        }

        if (shouldPrintMetrics) {
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
    .option("--print-metrics", "Print quality/telemetry metrics to stdout", false)
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--run-id <id>", "Optional explicit run id (v2 pipeline only)")
    .option("--v2-fixture-dir <path>", "Optional fixture directory for v2 evidence collection")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        assertMacminiAgentOnly("research theme-research");
        const allowLegacy = process.env.OPENCLAW_ALLOW_LEGACY_RESEARCH === "1";
        const pipeline = ((process.env.RESEARCH_PIPELINE ?? "") || "v2").trim().toLowerCase();
        if (pipeline !== "v2" && !allowLegacy) {
          throw new Error(
            `Legacy research pipeline disabled. Set RESEARCH_PIPELINE=v2 (recommended) or set OPENCLAW_ALLOW_LEGACY_RESEARCH=1 to allow legacy runs.`,
          );
        }
        if (pipeline === "v2") {
          const tickersOverride =
            typeof opts.tickers === "string" && opts.tickers.trim()
              ? parseTickersOption(opts.tickers as string)
              : [];
          const themeUniverse = computeThemeResearch({
            theme: opts.theme as string,
            tickers: tickersOverride.length ? tickersOverride : undefined,
            themeVersion: parseOptionalNumber(opts["themeVersion"]),
            minMembershipScore: parseOptionalNumber(opts["minMembershipScore"]),
            maxConstituents: parseOptionalNumber(opts["maxConstituents"]),
            lookbackDays: parseOptionalNumber(opts["lookbackDays"]) ?? 365,
            topN: parseOptionalNumber(opts.top) ?? 5,
            dbPath: opts.db as string,
          });
          const tickers = themeUniverse.tickers;
          if (!tickers.length) {
            throw new Error(
              "v2 theme memo: resolved universe is empty (pass --tickers to override)",
            );
          }
          const result = await runThemePipelineV2({
            themeName: opts.theme as string,
            universe: tickers,
            fixtureDir: (opts.v2FixtureDir as string | undefined) ?? undefined,
            runId: (opts.runId as string | undefined) ?? undefined,
            dbPath: opts.db as string,
          });
          if (!result.passed) {
            const reasons = result.issues
              .filter((i) => i.severity === "error")
              .slice(0, 12)
              .map((i) => `${i.code}${i.path ? `@${i.path}` : ""}: ${i.message}`)
              .join("; ");
            throw new Error(
              `v2 quality gate failed (run_id=${result.runId}): ${reasons || "unknown"}`,
            );
          }
          if (opts.out) {
            const outPath = path.resolve(opts.out as string);
            await fs.writeFile(
              outPath,
              await fs.readFile(result.reportMarkdownPath, "utf8"),
              "utf8",
            );
            defaultRuntime.log(`v2 theme memo written to ${outPath}`);
            defaultRuntime.log(`v2 run dir: ${result.runDir}`);
          } else {
            defaultRuntime.log(await fs.readFile(result.reportMarkdownPath, "utf8"));
          }
          return;
        }

        const tickers =
          typeof opts.tickers === "string" && opts.tickers.trim()
            ? parseTickersOption(opts.tickers as string)
            : undefined;
        const qualityAttempts = Math.max(1, parseOptionalNumber(opts["qualityAttempts"]) ?? 4);
        const strictMode = !Boolean(opts.allowDraft);
        const minScore = parseOptionalNumber(opts["minScore"]) ?? 0.82;
        const forceOutboundFetch = resolveResearchOutboundFetchEnabled();
        if (forceOutboundFetch) {
          try {
            const hydrationUniverse = computeThemeResearch({
              theme: opts.theme as string,
              tickers,
              themeVersion: parseOptionalNumber(opts["themeVersion"]),
              minMembershipScore: parseOptionalNumber(opts["minMembershipScore"]),
              maxConstituents: parseOptionalNumber(opts["maxConstituents"]),
              lookbackDays: parseOptionalNumber(opts["lookbackDays"]) ?? 365,
              topN: parseOptionalNumber(opts.top) ?? 5,
              dbPath: opts.db as string,
            });
            const hydration = await hydrateThemeEvidence({
              theme: opts.theme as string,
              tickers: hydrationUniverse.tickers,
              benchmarkTicker:
                hydrationUniverse.benchmarkTicker ||
                hydrationUniverse.benchmarkRelative?.benchmarkTicker ||
                hydrationUniverse.factorAttribution?.benchmarkTicker,
              dbPath: opts.db as string,
            });
            if (hydration) {
              defaultRuntime.log(`research_outbound_hydration theme=${opts.theme} ${hydration}`);
            }
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            defaultRuntime.error(
              `research_outbound_hydration theme=${opts.theme} skip(${message})`,
            );
          }
        }
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
          const uniqueIndustryCount = new Set(
            result.constituents.map((row) => row.industry).filter(Boolean),
          ).size;
          const uniqueSectorIndustryCount = new Set(
            result.constituents
              .map((row) => `${row.sector || "Unknown"}::${row.industry || "Unknown"}`)
              .filter(Boolean),
          ).size;
          const uniqueGroupCount = Math.max(
            uniqueSectorCount,
            uniqueIndustryCount,
            uniqueSectorIndustryCount,
          );
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
          const themeGate =
            report.outputGate ??
            evaluateCrossSectionQualityGate({
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
              uniqueGroupCount,
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
              output_gate_repairs: report.outputGateRepairs ?? [],
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
            const failedNames = new Set(failed.map((check) => check.name));
            const remediationParts: string[] = [];
            if (failedNames.has("source_diversity")) {
              try {
                const refreshed = refreshThemeMembership({
                  theme: opts.theme as string,
                  version: result.themeVersion,
                  tickers,
                  minMembershipScore: parseOptionalNumber(opts["minMembershipScore"]),
                  source: "quality_retry",
                  dbPath: opts.db as string,
                });
                remediationParts.push(
                  `membership_refresh=ok(active=${refreshed.activeCount},candidates=${refreshed.candidateCount})`,
                );
              } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                remediationParts.push(`membership_refresh=skip(${message})`);
              }
            }
            const hydration = await hydrateThemeEvidence({
              theme: opts.theme as string,
              tickers: result.tickers,
              benchmarkTicker:
                result.benchmarkTicker ||
                result.benchmarkRelative?.benchmarkTicker ||
                result.factorAttribution?.benchmarkTicker,
              dbPath: opts.db as string,
            });
            if (hydration) remediationParts.push(`hydration=${hydration}`);
            defaultRuntime.error(
              `quality_refinement_retry attempt=${attempt}/${qualityAttempts} reason=${err.message}${remediationParts.length ? ` remediation=${remediationParts.join(",")}` : ""}`,
            );
            continue;
          }
          throw err;
        }
        if (!final) throw lastError ?? new Error("Theme report generation failed");
        const shouldPrintMetrics = Boolean(opts.printMetrics);
        if (opts.out) {
          const outPath = path.resolve(opts.out as string);
          await fs.writeFile(outPath, `${final.report.markdown}\n`, "utf8");
          const manifestRes = await writeArtifactManifest({
            kind: "theme_report",
            outPath,
            markdown: final.report.markdown,
            seriesKey: deriveArtifactSeriesKey(outPath),
            seriesManifestPath: deriveSeriesManifestPath(outPath),
            metrics: {
              theme: String(opts.theme),
              tickers: final.result.tickers.length,
              used_theme_registry: final.result.usedThemeRegistry,
              narrative_clarity: Number(final.report.quality.narrativeClarityScore.toFixed(4)),
              exhibits: final.report.quality.exhibitCount,
              actionability: Number(final.report.quality.actionabilityScore.toFixed(4)),
              freshness_180d_ratio: Number(final.report.quality.freshness180dRatio.toFixed(4)),
            },
            gate: { runId: final.runId, ...final.gate },
          });
          defaultRuntime.log(`Report written to ${outPath}`);
          defaultRuntime.log(`Manifest written to ${manifestRes.manifestPath}`);
          if (manifestRes.seriesManifestPath) {
            defaultRuntime.log(`Series manifest written to ${manifestRes.seriesManifestPath}`);
          }
          const unicodeDashCount = Number(manifestRes.manifest.metrics.unicodeDashCount ?? 0);
          if (Number.isFinite(unicodeDashCount) && unicodeDashCount > 0) {
            throw new Error(
              `Unicode dash characters detected (count=${unicodeDashCount}). Replace with ASCII hyphens before delivery.`,
            );
          }
          const sourcesCount = Number(manifestRes.manifest.metrics.sourcesCount ?? 0);
          if (!Number.isFinite(sourcesCount) || sourcesCount <= 0) {
            throw new Error(
              `Sources appendix missing or empty (sourcesCount=${sourcesCount}). Refusing to proceed.`,
            );
          }
          if (manifestRes.manifest.unchangedFromPrevious) {
            defaultRuntime.error(`artifact_unchanged=1 sha256=${manifestRes.manifest.sha256}`);
            throw new Error(
              `Artifact unchanged (sha256=${manifestRes.manifest.sha256}). Refusing to proceed.`,
            );
          }
        } else {
          defaultRuntime.log(final.report.markdown);
        }
        if (shouldPrintMetrics) {
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
    .option("--run-id <id>", "Optional explicit run id (v2 pipeline only)")
    .option("--v2-fixture-dir <path>", "Optional fixture directory for v2 evidence collection")
    .option("--out <path>", "Write memo markdown to file")
    .option("--print-metrics", "Print quality/telemetry metrics to stdout", false)
    .option("--allow-draft", "Allow output even if institutional quality gate fails", false)
    .option("--min-score <n>", "Institutional quality threshold [0-1]", "0.8")
    .option("--quality-attempts <n>", "Quality refinement attempts before hard fail", "4")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        assertMacminiAgentOnly("research memo");
        const allowLegacy = process.env.OPENCLAW_ALLOW_LEGACY_RESEARCH === "1";
        const pipeline = ((process.env.RESEARCH_PIPELINE ?? "") || "v2").trim().toLowerCase();
        if (pipeline !== "v2" && !allowLegacy) {
          throw new Error(
            `Legacy research pipeline disabled. Set RESEARCH_PIPELINE=v2 (recommended) or set OPENCLAW_ALLOW_LEGACY_RESEARCH=1 to allow legacy runs.`,
          );
        }
        if (pipeline === "v2") {
          const result = await runCompanyPipelineV2({
            ticker: opts.ticker as string,
            question: opts.question as string,
            fixtureDir: (opts.v2FixtureDir as string | undefined) ?? undefined,
            runId: (opts.runId as string | undefined) ?? undefined,
            dbPath: opts.db as string,
          });

          if (!result.passed) {
            const reasons = result.issues
              .filter((i) => i.severity === "error")
              .slice(0, 12)
              .map((i) => `${i.code}${i.path ? `@${i.path}` : ""}: ${i.message}`)
              .join("; ");
            throw new Error(
              `v2 quality gate failed (run_id=${result.runId}): ${reasons || "unknown"}`,
            );
          }

          if (opts.out) {
            const outPath = path.resolve(opts.out as string);
            await fs.writeFile(
              outPath,
              await fs.readFile(result.reportMarkdownPath, "utf8"),
              "utf8",
            );
            defaultRuntime.log(`v2 memo written to ${outPath}`);
            defaultRuntime.log(`v2 run dir: ${result.runDir}`);
          } else {
            defaultRuntime.log(await fs.readFile(result.reportMarkdownPath, "utf8"));
          }
          return;
        }

        const qualityAttempts = Math.max(1, parseOptionalNumber(opts["qualityAttempts"]) ?? 4);
        const strictMode = !Boolean(opts.allowDraft);
        const baseQuestion = (opts.question as string).trim();
        const forceOutboundFetch = resolveResearchOutboundFetchEnabled();
        const refinementHints = [
          "",
          "Prioritize multi-source evidence (filings, transcript, expectations) and resolve contradictions explicitly.",
          "Strengthen actionability with entry trigger, sizing bands, and falsification thresholds.",
          "Stress-test bear/base/bull scenarios and justify final stance against disconfirming evidence.",
        ];

        if (forceOutboundFetch) {
          const hydration = await hydrateTickerEvidence({
            ticker: opts.ticker as string,
            dbPath: opts.db as string,
          });
          if (hydration) {
            defaultRuntime.log(
              `research_outbound_hydration ticker=${String(opts.ticker).toUpperCase()} ${hydration}`,
            );
          }
        }

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
              attempt < qualityAttempts &&
              (err.message.includes("Institutional-grade quality gate failed") ||
                err.message.includes("Insufficient evidence"));
            if (!canRetry) {
              throw err;
            }
            const remediation = await hydrateTickerEvidence({
              ticker: opts.ticker as string,
              dbPath: opts.db as string,
            });
            defaultRuntime.error(
              `quality_refinement_retry attempt=${attempt}/${qualityAttempts} reason=${err.message}${remediation ? ` remediation=${remediation}` : ""}`,
            );
          }
        }

        if (!result) {
          throw (
            lastError ??
            new Error(`Memo generation failed after ${qualityAttempts} quality attempts`)
          );
        }
        const shouldPrintMetrics = Boolean(opts.printMetrics);
        if (opts.out) {
          const outPath = path.resolve(opts.out as string);
          await fs.writeFile(outPath, `${result.memo}\n`, "utf8");
          const manifestRes = await writeArtifactManifest({
            kind: "memo",
            outPath,
            markdown: result.memo,
            seriesKey: deriveArtifactSeriesKey(outPath),
            seriesManifestPath: deriveSeriesManifestPath(outPath),
            metrics: {
              claims: result.claims,
              citations: result.citations,
              quality_score: Number(result.quality.score.toFixed(4)),
              quality_passed: result.quality.passed,
              quality_min_score: Number(result.quality.minScore.toFixed(4)),
            },
            gate: {
              runId: result.qualityGateRunId,
              ...result.qualityGate,
            },
          });
          defaultRuntime.log(`Memo written to ${outPath}`);
          defaultRuntime.log(`Manifest written to ${manifestRes.manifestPath}`);
          if (manifestRes.seriesManifestPath) {
            defaultRuntime.log(`Series manifest written to ${manifestRes.seriesManifestPath}`);
          }
          const unicodeDashCount = Number(manifestRes.manifest.metrics.unicodeDashCount ?? 0);
          if (Number.isFinite(unicodeDashCount) && unicodeDashCount > 0) {
            throw new Error(
              `Unicode dash characters detected (count=${unicodeDashCount}). Replace with ASCII hyphens before delivery.`,
            );
          }
          const sourcesCount = Number(manifestRes.manifest.metrics.sourcesCount ?? 0);
          if (!Number.isFinite(sourcesCount) || sourcesCount <= 0) {
            throw new Error(
              `Sources appendix missing or empty (sourcesCount=${sourcesCount}). Refusing to proceed.`,
            );
          }
          if (manifestRes.manifest.unchangedFromPrevious) {
            defaultRuntime.error(`artifact_unchanged=1 sha256=${manifestRes.manifest.sha256}`);
            throw new Error(
              `Artifact unchanged (sha256=${manifestRes.manifest.sha256}). Refusing to proceed.`,
            );
          }
        } else {
          // By default, print only the memo body (clean deliverable). Metrics belong in the DB/manifest.
          defaultRuntime.log(result.memo);
        }

        if (shouldPrintMetrics) {
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
        }
      });
    });

  research
    .command("preflight")
    .description(
      "Generate a manifest-first preflight bundle (expected counts, exhibits, sources) for a markdown -> PDF artifact",
    )
    .requiredOption("--kind <kind>", "memo|sector_report|theme_report")
    .requiredOption("--in <path>", "Input markdown path")
    .requiredOption("--out <path>", "Expected output PDF path")
    .option("--write <path>", "Write preflight JSON (default: <out-dir>/artifact.preflight.json)")
    .option(
      "--strict",
      "Fail-closed when unicode dashes, placeholders, exhibits, or sources are missing/invalid",
      false,
    )
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const kind = parseArtifactKind(opts.kind as string);
        const inPath = path.resolve(opts.in as string);
        const outPath = path.resolve(opts.out as string);
        const outDir = path.dirname(outPath);
        await fs.mkdir(outDir, { recursive: true });

        const preflight = await buildArtifactPreflight({ kind, inPath, outPath });
        const writePath =
          typeof opts.write === "string" && opts.write.trim()
            ? path.resolve(opts.write as string)
            : path.join(outDir, "artifact.preflight.json");

        await fs.writeFile(writePath, `${JSON.stringify(preflight, null, 2)}\n`, "utf8");
        defaultRuntime.log(`Preflight written to ${writePath}`);
        defaultRuntime.log(
          `expected exhibits=${preflight.metrics.exhibitCount} footnotes=${preflight.metrics.footnoteCount} sources=${preflight.metrics.sourcesCount} unicode_dashes=${preflight.metrics.unicodeDashCount} placeholders=${preflight.metrics.placeholderTokenCount}`,
        );
        const assetCount = preflight.exhibits.reduce(
          (sum, exhibit) => sum + exhibit.assets.length,
          0,
        );
        defaultRuntime.log(
          `exhibit_assets=${assetCount} sources_parsed=${preflight.sources.length} sources_with_url=${preflight.metrics.sourcesWithUrlCount} sources_with_key=${preflight.metrics.sourcesWithKeyCount}`,
        );

        if (Boolean(opts.strict)) {
          if (preflight.metrics.unicodeDashCount > 0) {
            throw new Error(
              `Unicode dash characters detected in markdown (count=${preflight.metrics.unicodeDashCount}). Replace with ASCII hyphens before delivery.`,
            );
          }
          if (preflight.metrics.placeholderTokenCount > 0) {
            throw new Error(
              `Placeholder language detected in markdown (count=${preflight.metrics.placeholderTokenCount}). Remove "on request"/"to appear"/"appendix pass" style text before delivery.`,
            );
          }
          if (preflight.metrics.exhibitCount <= 0) {
            throw new Error(
              `Exhibits missing or unnumbered (exhibitCount=${preflight.metrics.exhibitCount}). Refusing to proceed.`,
            );
          }
          if (preflight.metrics.sourcesCount <= 0) {
            throw new Error(
              `Sources appendix missing or empty (sourcesCount=${preflight.metrics.sourcesCount}). Refusing to proceed.`,
            );
          }
          if (preflight.metrics.sourcesWithUrlCount <= 0) {
            throw new Error(
              `Sources list has no parseable URLs (sourcesWithUrlCount=${preflight.metrics.sourcesWithUrlCount}). Refusing to proceed.`,
            );
          }
          if (preflight.metrics.sourcesWithKeyCount <= 0) {
            throw new Error(
              `Sources list has no parseable source keys (e.g., C1:) (sourcesWithKeyCount=${preflight.metrics.sourcesWithKeyCount}). Refusing to proceed.`,
            );
          }
        }
      });
    });

  research
    .command("pdf-diagnostics")
    .description(
      "Inspect a PDF for common pre-send failures (markdown tokens, missing sources, dash encoding issues)",
    )
    .requiredOption("--in <path>", "Input PDF path")
    .option("--max-pages <n>", "Max pages to scan for text extraction", "12")
    .option("--dump-text <path>", "Write extracted text to a file for inspection")
    .option("--strict", "Fail-closed if PDF violates institutional pre-send checks", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const inPath = path.resolve(opts.in as string);
        const maxPages = Math.max(1, parseOptionalNumber(opts["maxPages"]) ?? 12);
        const buffer = await fs.readFile(inPath);
        const { extractPdfTextFromBuffer, diagnosePdfBuffer } =
          await import("../research/pdf-diagnostics.js");
        const { pages, scannedPages, text } = await extractPdfTextFromBuffer({
          buffer: new Uint8Array(buffer),
          maxPages,
        });
        const strict = Boolean(opts.strict);
        const { metrics, errors } = await diagnosePdfBuffer({
          buffer: new Uint8Array(buffer),
          maxPages,
          strict,
        });

        const dumpPath = (opts["dumpText"] as string | undefined)?.trim();
        if (dumpPath) {
          const resolvedDumpPath = path.resolve(dumpPath);
          await fs.mkdir(path.dirname(resolvedDumpPath), { recursive: true });
          await fs.writeFile(resolvedDumpPath, `${text}\n`, "utf8");
          defaultRuntime.log(`Extracted text written to ${resolvedDumpPath}`);
        }

        defaultRuntime.log(
          `${JSON.stringify(
            {
              inPath,
              pages,
              scannedPages,
              byteSize: buffer.byteLength,
              metrics,
              ...(strict ? { errors } : {}),
            },
            null,
            2,
          )}\n`,
        );

        if (strict && errors.length > 0) {
          throw new Error(`PDF failed strict diagnostics:\n- ${errors.join("\n- ")}`);
        }
      });
    });

  research
    .command("render-pdf")
    .description("Render markdown to a styled PDF and write an artifact manifest (fail-closed)")
    .requiredOption("--kind <kind>", "memo|sector_report|theme_report")
    .requiredOption("--in <path>", "Input markdown path")
    .requiredOption("--out <path>", "Output PDF path")
    .option("--title <text>", "Header title override")
    .option("--format <fmt>", "letter|a4", "letter")
    .option("--chrome <path>", "Chrome executable path (defaults to OPENCLAW_CHROME_PATH)")
    .option("--series-key <key>", "Optional stable series key (defaults to basename without .vN)")
    .option(
      "--series-manifest <path>",
      "Optional series manifest path (default: <out-dir>/artifact.json)",
    )
    .option(
      "--skip-pdf-diagnostics",
      "Skip strict PDF diagnostics (URLs/citations/exhibits/mojibake/placeholders)",
      false,
    )
    .option("--pdf-diagnostics-max-pages <n>", "Max pages to scan for strict PDF diagnostics", "50")
    .option("--pdf-diagnostics-dump-text <path>", "Write extracted PDF text to a file")
    .option("--allow-no-sources", "Allow rendering without a non-empty Sources appendix", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        assertMacminiBrowserAllowed("research render-pdf");
        const kind = parseArtifactKind(opts.kind as string);
        const inPath = path.resolve(opts.in as string);
        const outPath = path.resolve(opts.out as string);
        const pdfFormat = parsePdfFormat(opts.format as string | undefined);
        const titleOverride = (opts.title as string | undefined)?.trim();
        const chromePath = resolveChromeExecutablePath(opts.chrome as string | undefined);
        if (!chromePath) {
          throw new Error(
            "Chrome executable not found. Set OPENCLAW_CHROME_PATH (or pass --chrome).",
          );
        }

        await fs.mkdir(path.dirname(outPath), { recursive: true });

        const markdown = await fs.readFile(inPath, "utf8");
        const unicodeDashCount = (markdown.match(BAD_DASHES) ?? []).length;
        if (unicodeDashCount > 0) {
          throw new Error(
            `Unicode dash characters detected in markdown (count=${unicodeDashCount}). Replace with ASCII hyphens before rendering.`,
          );
        }

        const builtAtEt = formatBuiltAtEt(new Date());
        // Avoid hyphen mojibake in PDF text extraction (e.g. "-" -> standalone "n").
        const builtAtEtFooter = builtAtEt.replaceAll("-", "/");

        const { default: MarkdownIt } = await import("markdown-it");
        const md = new MarkdownIt({
          html: true,
          linkify: true,
          breaks: false,
          typographer: false,
        });
        const bodyHtml = md.render(markdown);
        const baseHref = pathToFileURL(path.dirname(inPath) + path.sep).href;
        const title =
          titleOverride ||
          markdown
            .split(/\r?\n/)
            .find((line) => line.startsWith("# "))
            ?.replace(/^#\s+/, "")
            .trim() ||
          path.basename(inPath);

        const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <base href="${escapeHtml(baseHref)}" />
    <title>${escapeHtml(title)}</title>
    <style>
      :root {
        --font-sans: ui-sans-serif, -apple-system, BlinkMacSystemFont, "SF Pro Text", "Helvetica Neue", Arial, sans-serif;
        --font-serif: ui-serif, "Iowan Old Style", "Palatino", "Georgia", serif;
        --font-mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
        --text: #111827;
        --muted: #6b7280;
        --rule: #e5e7eb;
        --accent: #0b3d91;
        --bg: #ffffff;
      }

      html {
        -webkit-print-color-adjust: exact;
        print-color-adjust: exact;
        background: var(--bg);
      }

      body {
        margin: 0;
        padding: 0;
        color: var(--text);
        background: var(--bg);
        font-family: var(--font-serif);
        font-size: 11.5pt;
        line-height: 1.38;
      }

      .doc {
        max-width: 7.2in;
        margin: 0 auto;
        padding: 0.15in 0;
      }

      h1, h2, h3, h4 {
        font-family: var(--font-sans);
        letter-spacing: -0.01em;
        margin: 0;
      }
      h1 {
        font-size: 22pt;
        line-height: 1.15;
        margin-bottom: 0.18in;
      }
      h2 {
        font-size: 14pt;
        margin-top: 0.26in;
        padding-top: 0.16in;
        border-top: 1px solid var(--rule);
        margin-bottom: 0.1in;
      }
      h3 {
        font-size: 11.75pt;
        margin-top: 0.16in;
        margin-bottom: 0.06in;
      }

      p {
        margin: 0 0 0.12in 0;
      }

      ul, ol {
        margin: 0 0 0.12in 0.22in;
        padding: 0;
      }
      li {
        margin: 0.04in 0;
      }

      blockquote {
        margin: 0.12in 0;
        padding: 0.12in 0.16in;
        border-left: 3px solid var(--rule);
        background: #fafafa;
        color: #111827;
      }

      table {
        width: 100%;
        border-collapse: collapse;
        margin: 0.14in 0;
        font-family: var(--font-sans);
        font-size: 10pt;
      }
      th, td {
        border-bottom: 1px solid var(--rule);
        padding: 0.06in 0.08in;
        vertical-align: top;
      }
      th {
        text-align: left;
        color: #111827;
        font-weight: 600;
      }

      code {
        font-family: var(--font-mono);
        font-size: 0.95em;
        background: #f3f4f6;
        padding: 0.03em 0.2em;
        border-radius: 4px;
      }
      pre {
        background: #0b1020;
        color: #e5e7eb;
        padding: 0.14in;
        border-radius: 8px;
        overflow-x: auto;
        margin: 0.14in 0;
      }
      pre code {
        background: transparent;
        padding: 0;
        border-radius: 0;
        font-size: 9.5pt;
      }

      hr {
        border: none;
        border-top: 1px solid var(--rule);
        margin: 0.2in 0;
      }

      a {
        color: var(--accent);
        text-decoration: none;
      }
      a:hover {
        text-decoration: underline;
      }

      /* Pagination hints */
      h2, h3, h4 { break-after: avoid-page; }
      table, blockquote, pre { break-inside: avoid; }
    </style>
  </head>
  <body>
    <main class="doc">
      ${bodyHtml}
    </main>
  </body>
</html>`;

        const { chromium } = await import("playwright-core");
        const browser = await chromium.launch({
          headless: true,
          executablePath: chromePath,
        });
        try {
          const page = await browser.newPage();
          await page.setContent(html, { waitUntil: "load" });
          await page.emulateMedia({ media: "print" });

          const headerTemplate = `<div style=\"font-size:8px;padding:0 24px;width:100%;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;color:#6b7280;display:flex;align-items:center;justify-content:space-between;\"><span>${escapeHtml(
            title,
          )}</span><span></span></div>`;
          const footerTemplate = `<div style=\"font-size:8px;padding:0 24px;width:100%;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;color:#6b7280;display:flex;align-items:center;justify-content:space-between;\"><span>FOR DISCUSSION PURPOSES ONLY; NOT INVESTMENT ADVICE.</span><span>Built: ${escapeHtml(
            builtAtEtFooter,
          )}</span><span>Page <span class=\"pageNumber\"></span> / <span class=\"totalPages\"></span></span></div>`;

          await page.pdf({
            path: outPath,
            format: pdfFormat,
            printBackground: true,
            displayHeaderFooter: true,
            headerTemplate,
            footerTemplate,
            margin: {
              top: "0.9in",
              bottom: "0.9in",
              left: "0.9in",
              right: "0.9in",
            },
          });
        } finally {
          await browser.close();
        }

        const preSendErrors: string[] = [];

        const skipPdfDiagnostics = Boolean(opts["skipPdfDiagnostics"]);
        const pdfDiagnosticsMaxPages = Math.max(
          1,
          parseOptionalNumber(opts["pdfDiagnosticsMaxPages"]) ?? 50,
        );
        const pdfDiagnosticsDumpText = (opts["pdfDiagnosticsDumpText"] as string | undefined)
          ?.trim()
          ?.trim();

        let pdfDiagnostics:
          | {
              pages: number;
              scannedPages: number;
              metrics: {
                markdownHeadingTokens: number;
                markdownFenceTokens: number;
                urlCount: number;
                citationKeyCount: number;
                exhibitTokenCount: number;
                sourcesHeadingPresent: boolean;
                dashMojibakeDateCount: number;
                dashMojibakeStandaloneNCount: number;
                placeholderTokenCount: number;
                extractedChars: number;
              };
              errors: string[];
            }
          | undefined;

        if (!skipPdfDiagnostics) {
          const pdfBuffer = await fs.readFile(outPath);
          const { extractPdfTextFromBuffer, computePdfDiagnostics, validatePdfDiagnosticsStrict } =
            await import("../research/pdf-diagnostics.js");
          const extracted = await extractPdfTextFromBuffer({
            buffer: new Uint8Array(pdfBuffer),
            maxPages: pdfDiagnosticsMaxPages,
          });
          const metrics = computePdfDiagnostics(extracted.text);
          const errors = validatePdfDiagnosticsStrict({ metrics });
          pdfDiagnostics = {
            pages: extracted.pages,
            scannedPages: extracted.scannedPages,
            metrics,
            errors,
          };

          if (pdfDiagnosticsDumpText) {
            const resolvedDumpPath = path.resolve(pdfDiagnosticsDumpText);
            await fs.mkdir(path.dirname(resolvedDumpPath), { recursive: true });
            await fs.writeFile(resolvedDumpPath, `${extracted.text}\n`, "utf8");
            defaultRuntime.log(`Extracted PDF text written to ${resolvedDumpPath}`);
          }
        }

        const seriesKey =
          typeof opts.seriesKey === "string" && opts.seriesKey.trim()
            ? opts.seriesKey.trim()
            : deriveArtifactSeriesKey(outPath);
        const seriesManifestPath =
          typeof opts.seriesManifest === "string" && opts.seriesManifest.trim()
            ? path.resolve(opts.seriesManifest as string)
            : deriveSeriesManifestPath(outPath);

        const manifestRes = await writeFileArtifactManifest({
          kind,
          outPath,
          seriesKey,
          seriesManifestPath,
          markdownForMetrics: markdown,
          metrics: pdfDiagnostics
            ? {
                pdfDiagnosticsPages: pdfDiagnostics.pages,
                pdfDiagnosticsScannedPages: pdfDiagnostics.scannedPages,
                pdfMarkdownHeadingTokens: pdfDiagnostics.metrics.markdownHeadingTokens,
                pdfMarkdownFenceTokens: pdfDiagnostics.metrics.markdownFenceTokens,
                pdfUrlCount: pdfDiagnostics.metrics.urlCount,
                pdfCitationKeyCount: pdfDiagnostics.metrics.citationKeyCount,
                pdfExhibitTokenCount: pdfDiagnostics.metrics.exhibitTokenCount,
                pdfSourcesHeadingPresent: pdfDiagnostics.metrics.sourcesHeadingPresent,
                pdfDashMojibakeDateCount: pdfDiagnostics.metrics.dashMojibakeDateCount,
                pdfDashMojibakeStandaloneNCount:
                  pdfDiagnostics.metrics.dashMojibakeStandaloneNCount,
                pdfPlaceholderTokenCount: pdfDiagnostics.metrics.placeholderTokenCount,
                pdfExtractedChars: pdfDiagnostics.metrics.extractedChars,
                pdfStrictDiagnosticsErrorCount: pdfDiagnostics.errors.length,
              }
            : undefined,
          gate: pdfDiagnostics ? { pdfDiagnostics } : undefined,
        });
        defaultRuntime.log(`PDF written to ${outPath}`);
        defaultRuntime.log(`Manifest written to ${manifestRes.manifestPath}`);
        if (manifestRes.seriesManifestPath) {
          defaultRuntime.log(`Series manifest written to ${manifestRes.seriesManifestPath}`);
        }
        defaultRuntime.log(
          `artifact sha256=${manifestRes.manifest.sha256} bytes=${manifestRes.manifest.byteSize}`,
        );

        const pdfDashCount = Number(manifestRes.manifest.metrics.unicodeDashCount ?? 0);
        if (Number.isFinite(pdfDashCount) && pdfDashCount > 0) {
          preSendErrors.push(
            `unicode_dashes: count=${pdfDashCount} (replace with ASCII hyphens before delivery)`,
          );
        }

        if (!Boolean(opts["allowNoSources"])) {
          const sourcesCount = Number(manifestRes.manifest.metrics.sourcesCount ?? 0);
          if (!Number.isFinite(sourcesCount) || sourcesCount <= 0) {
            preSendErrors.push(
              `sources_missing: sourcesCount=${sourcesCount} (appendix missing or empty)`,
            );
          }
        }

        if (manifestRes.manifest.unchangedFromPrevious) {
          defaultRuntime.error(`artifact_unchanged=1 sha256=${manifestRes.manifest.sha256}`);
          preSendErrors.push(
            `artifact_unchanged: sha256=${manifestRes.manifest.sha256} (refusing to proceed)`,
          );
        }

        if (pdfDiagnostics && pdfDiagnostics.errors.length > 0) {
          preSendErrors.push(...pdfDiagnostics.errors);
        }

        if (preSendErrors.length > 0) {
          defaultRuntime.error(`artifact_presend_failed=1 errors=${preSendErrors.length}`);
          throw new Error(`Artifact failed pre-send checks:\n- ${preSendErrors.join("\n- ")}`);
        }
      });
    });

  research
    .command("artifact-path")
    .description("Compute the next versioned artifact path (base.vN.ext) inside a directory")
    .requiredOption("--dir <path>", "Output directory")
    .requiredOption("--base <name>", "Base filename (no version, no extension)")
    .option("--ext <ext>", "File extension", "pdf")
    .option("--mkdir", "Create the directory if missing", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const dir = path.resolve(opts.dir as string);
        if (Boolean(opts.mkdir)) {
          await fs.mkdir(dir, { recursive: true });
        }
        const res = await resolveNextVersionedArtifactPath({
          dir,
          base: opts.base as string,
          ext: opts.ext as string,
        });
        defaultRuntime.log(res.outPath);
        defaultRuntime.log(`artifact_version=v${res.version}`);
        defaultRuntime.log(`series_manifest=${deriveSeriesManifestPath(res.outPath)}`);
      });
    });

  research
    .command("manifest-file")
    .description("Write artifact manifest for an existing file (e.g., a PDF)")
    .requiredOption("--kind <kind>", "memo|sector_report|theme_report")
    .requiredOption("--out <path>", "Artifact file path (e.g., /path/to/report.pdf)")
    .option("--markdown <path>", "Optional markdown to compute exhibit/source/footnote counts")
    .option("--series-key <key>", "Optional stable series key (defaults to basename without .vN)")
    .option(
      "--series-manifest <path>",
      "Optional series manifest path (default: <out-dir>/artifact.json)",
    )
    .option("--metrics <json>", "Optional JSON object merged into manifest.metrics")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const kind = parseArtifactKind(opts.kind as string);
        const outPath = path.resolve(opts.out as string);
        const seriesKey =
          typeof opts.seriesKey === "string" && opts.seriesKey.trim()
            ? opts.seriesKey.trim()
            : deriveArtifactSeriesKey(outPath);
        const seriesManifestPath =
          typeof opts.seriesManifest === "string" && opts.seriesManifest.trim()
            ? path.resolve(opts.seriesManifest as string)
            : deriveSeriesManifestPath(outPath);

        const markdownForMetrics =
          typeof opts.markdown === "string" && opts.markdown.trim()
            ? await fs.readFile(path.resolve(opts.markdown as string), "utf8")
            : undefined;
        const extraMetrics = parseOptionalJsonObject(opts.metrics) ?? undefined;

        const manifestRes = await writeFileArtifactManifest({
          kind,
          outPath,
          seriesKey,
          seriesManifestPath,
          markdownForMetrics,
          metrics: extraMetrics as Record<string, number | string | boolean | null> | undefined,
        });

        defaultRuntime.log(`Manifest written to ${manifestRes.manifestPath}`);
        if (manifestRes.seriesManifestPath) {
          defaultRuntime.log(`Series manifest written to ${manifestRes.seriesManifestPath}`);
        }
        defaultRuntime.log(
          `artifact sha256=${manifestRes.manifest.sha256} bytes=${manifestRes.manifest.byteSize}`,
        );
        const unicodeDashCount = Number(manifestRes.manifest.metrics.unicodeDashCount ?? 0);
        if (Number.isFinite(unicodeDashCount) && unicodeDashCount > 0) {
          throw new Error(
            `Unicode dash characters detected (count=${unicodeDashCount}). Replace with ASCII hyphens before delivery.`,
          );
        }
        const rawSourcesCount = manifestRes.manifest.metrics.sourcesCount;
        if (rawSourcesCount !== null && rawSourcesCount !== undefined) {
          const sourcesCount = Number(rawSourcesCount);
          if (!Number.isFinite(sourcesCount) || sourcesCount <= 0) {
            throw new Error(
              `Sources appendix missing or empty (sourcesCount=${sourcesCount}). Refusing to proceed.`,
            );
          }
        }
        if (manifestRes.manifest.unchangedFromPrevious) {
          defaultRuntime.error(`artifact_unchanged=1 sha256=${manifestRes.manifest.sha256}`);
          throw new Error(
            `Artifact unchanged (sha256=${manifestRes.manifest.sha256}). Refusing to proceed.`,
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
    .option("--run-type <type>", "Optional run type filter")
    .option("--limit <n>", "Rows to show", "20")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rows = latestEvalReport({
          runType: opts["runType"] as string | undefined,
          limit: parseOptionalNumber(opts.limit) ?? 20,
        });
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
    .command("eval-harness")
    .description("Run a deterministic research eval task set from JSON")
    .requiredOption("--taskset <path>", "Path to eval task set JSON")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--out <path>", "Write markdown scorecard to file")
    .option("--json", "Emit JSON result", false)
    .option("--require-pass", "Exit non-zero if the eval gate fails", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const taskSet = loadResearchEvalTaskSet(opts.taskset as string);
        const result = await runResearchEvalTaskSet({
          taskSet,
          dbPath: opts.db as string,
        });
        if (opts.json) {
          defaultRuntime.log(`${JSON.stringify(result, null, 2)}\n`);
        } else {
          defaultRuntime.log(
            `eval_harness name=${result.taskSetName} score=${(result.score * 100).toFixed(1)}% passed=${result.passed}/${result.total} gate=${result.passedGate ? "pass" : "fail"}`,
          );
          result.checks.forEach((check) => {
            defaultRuntime.log(
              `- ${check.passed ? "ok" : "fail"} ${check.name}: ${check.detail}`,
            );
          });
          if (result.reasons.length) {
            result.reasons.forEach((reason) => defaultRuntime.error(`GATE: ${reason}`));
          }
        }
        if (opts.out) {
          const outPath = path.resolve(opts.out as string);
          await fs.mkdir(path.dirname(outPath), { recursive: true });
          await fs.writeFile(outPath, `${renderResearchEvalScorecard(result)}\n`, "utf8");
          defaultRuntime.log(`Scorecard written to ${outPath}`);
        }
        if (opts.requirePass && !result.passedGate) {
          defaultRuntime.exit(1);
        }
      });
    });

  research
    .command("eval-harness-report")
    .description("Show recent research eval harness runs")
    .option("--taskset-name <name>", "Optional task set name filter")
    .option("--limit <n>", "Rows to show", "20")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const rows = latestResearchEvalHarnessRuns({
          taskSetName: opts["tasksetName"] as string | undefined,
          limit: parseOptionalNumber(opts.limit) ?? 20,
        });
        if (!rows.length) {
          defaultRuntime.log("No eval harness runs yet.");
          return;
        }
        rows.forEach((row) => {
          defaultRuntime.log(
            `${new Date(row.created_at).toISOString()} ${row.run_type} score=${(row.score * 100).toFixed(1)}% (${row.passed}/${row.total})`,
          );
        });
      });
    });

  research
    .command("health")
    .description("Check research runtime health")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--backup-dir <path>", "Backup directory")
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const snapshot = getResearchHealthSnapshot({
          dbPath: opts.db as string,
          backupDir: opts["backupDir"] as string | undefined,
        });
        if (opts.json) {
          defaultRuntime.log(`${JSON.stringify(snapshot, null, 2)}\n`);
          return;
        }
        defaultRuntime.log(`checked_at=${snapshot.checkedAt}`);
        defaultRuntime.log(`db_path=${snapshot.dbPath}`);
        defaultRuntime.log(`state_dir=${snapshot.stateDir}`);
        defaultRuntime.log(
          `quickrun queued=${snapshot.quickrun.queued} running=${snapshot.quickrun.running} failed=${snapshot.quickrun.failed} completed=${snapshot.quickrun.completed} stale_running=${snapshot.quickrun.staleRunning}`,
        );
        defaultRuntime.log(
          `refresh_queue queued=${snapshot.refreshQueue.queued} high_priority=${snapshot.refreshQueue.highPriorityQueued} completed=${snapshot.refreshQueue.completed} skipped=${snapshot.refreshQueue.skipped}`,
        );
        defaultRuntime.log(
          `external latest_document=${snapshot.externalResearch.latestDocument.iso ?? "none"} latest_report=${snapshot.externalResearch.latestReport.iso ?? "none"} latest_brief=${snapshot.externalResearch.latestBrief.iso ?? "none"} unresolved_thesis_alerts=${snapshot.externalResearch.unresolvedThesisAlerts}`,
        );
        defaultRuntime.log(
          `backups latest=${snapshot.backups.latestBackupPath ?? "none"} created_at=${snapshot.backups.latestBackupAt ?? "none"}`,
        );
        snapshot.warnings.forEach((warning) => defaultRuntime.error(`WARN: ${warning}`));
      });
    });

  research
    .command("backup")
    .description("Backup the research runtime (db plus local research artifacts)")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option(
      "--dest <dir>",
      "Backup destination",
      path.join(process.cwd(), "data", "research-backups"),
    )
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const out = createResearchBackup({
          dbPath: opts.db as string,
          destDir: opts.dest as string,
        });
        defaultRuntime.log(`Backup created: ${out.backupDir}`);
        defaultRuntime.log(`Database copy: ${out.dbPath}`);
        defaultRuntime.log(`Manifest: ${out.manifestPath}`);
      });
    });

  research
    .command("restore")
    .description("Restore the research runtime from a backup directory")
    .requiredOption("--src <dir>", "Backup directory to restore from")
    .option("--db <path>", "Destination database path", resolveResearchDbPath())
    .option("--state-dir <path>", "Destination research state dir")
    .option("--force", "Overwrite destination files if they already exist", false)
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const out = restoreResearchBackup({
          backupDir: opts.src as string,
          dbPath: opts.db as string,
          stateDir: opts["stateDir"] as string | undefined,
          force: Boolean(opts.force),
        });
        defaultRuntime.log(`Restored backup: ${out.backupDir}`);
        defaultRuntime.log(`Database: ${out.restoredDbPath}`);
        defaultRuntime.log(`State dir: ${out.restoredStateDir}`);
      });
    });

  research
    .command("replay-failed")
    .description("Requeue failed quick research jobs")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--limit <n>", "Max failed jobs to replay", "25")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const jobs = replayFailedQuickrunJobs({
          dbPath: opts.db as string,
          limit: Number.parseInt(opts.limit as string, 10) || 25,
        });
        defaultRuntime.log(`replayed=${jobs.length}`);
        jobs.forEach((job) => {
          defaultRuntime.log(`- ${job.id} status=${job.status}`);
        });
      });
    });

  research
    .command("scheduler-pass")
    .description("Run one research scheduler pass (brief generation + refresh completion)")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--brief-date <yyyy-mm-dd>", "Override brief date")
    .option("--lookback-days <n>", "Brief lookback window in days")
    .option("--limit <n>", "Max refresh queue items to process", "50")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const result = runResearchSchedulerPass({
          dbPath: opts.db as string,
          briefDate: opts["briefDate"] as string | undefined,
          lookbackDays: parseOptionalNumber(opts["lookbackDays"]) ?? undefined,
          limit: Number.parseInt(opts.limit as string, 10) || 50,
        });
        defaultRuntime.log(
          `processed_refreshes=${result.processedRefreshes} generated_briefs=${result.generatedBriefs} queued_remaining=${result.queuedRefreshesRemaining}`,
        );
      });
    });

  const service = research
    .command("service")
    .description("Manage research worker/scheduler services (launchd/systemd/schtasks)");

  service
    .command("status")
    .requiredOption("--kind <kind>", "worker|scheduler")
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runResearchServiceStatus({
        kind: opts.kind as "worker" | "scheduler",
        json: Boolean(opts.json),
      });
    });

  service
    .command("install")
    .requiredOption("--kind <kind>", "worker|scheduler")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--interval-ms <n>", "Scheduler interval in ms", "300000")
    .option("--runtime <runtime>", "Service runtime (node|bun). Default: node")
    .option("--force", "Reinstall/overwrite if already installed", false)
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runResearchServiceInstall({
        kind: opts.kind as "worker" | "scheduler",
        db: opts.db as string,
        intervalMs: opts["intervalMs"] as string,
        runtime: opts.runtime as string | undefined,
        force: Boolean(opts.force),
        json: Boolean(opts.json),
      });
    });

  service
    .command("start")
    .requiredOption("--kind <kind>", "worker|scheduler")
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runResearchServiceStart({
        kind: opts.kind as "worker" | "scheduler",
        json: Boolean(opts.json),
      });
    });

  service
    .command("stop")
    .requiredOption("--kind <kind>", "worker|scheduler")
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runResearchServiceStop({
        kind: opts.kind as "worker" | "scheduler",
        json: Boolean(opts.json),
      });
    });

  service
    .command("restart")
    .requiredOption("--kind <kind>", "worker|scheduler")
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runResearchServiceRestart({
        kind: opts.kind as "worker" | "scheduler",
        json: Boolean(opts.json),
      });
    });

  service
    .command("uninstall")
    .requiredOption("--kind <kind>", "worker|scheduler")
    .option("--json", "Output JSON", false)
    .action(async (opts) => {
      await runResearchServiceUninstall({
        kind: opts.kind as "worker" | "scheduler",
        json: Boolean(opts.json),
      });
    });

  research
    .command("scheduler")
    .description("Run the research scheduler loop for Mac mini deployment")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .option("--interval-ms <n>", "Scheduler poll interval in ms", "300000")
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        await runResearchSchedulerLoop({
          dbPath: opts.db as string,
          intervalMs: Number.parseInt(opts["intervalMs"] as string, 10) || 300_000,
          runtime: defaultRuntime,
        });
      });
    });

  research
    .command("worker")
    .description("Run the dedicated quick research worker loop for Mac mini deployment")
    .option("--db <path>", "Database path", resolveResearchDbPath())
    .action(async (opts) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        await runResearchWorkerLoop({
          dbPath: opts.db as string,
          runtime: defaultRuntime,
        });
      });
    });
}

export const __testOnly = {
  resolveResearchOutboundFetchEnabled,
};
