import fs from "node:fs/promises";
import path from "node:path";
import {
  DEFAULT_CONCEPTS,
  buildTimeSeriesExhibit,
  fetchCompanyFacts,
  resolveTickerToCik,
  timeSeriesToCsv,
  timeSeriesToMarkdown,
} from "../src/agents/sec-xbrl-timeseries.js";

type CliOptions = {
  ticker?: string;
  cik?: string;
  concepts: string[];
  forms: string[];
  outDir: string;
  perSeriesLimit: number;
  userAgent?: string;
};

const parseArgs = (argv: string[]): CliOptions => {
  const options: CliOptions = {
    concepts: [],
    forms: [],
    outDir: "sec-edgar-filings",
    perSeriesLimit: 64,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === "--ticker" && next) {
      options.ticker = next;
      i += 1;
      continue;
    }
    if (arg === "--cik" && next) {
      options.cik = next;
      i += 1;
      continue;
    }
    if (arg === "--concept" && next) {
      options.concepts.push(next);
      i += 1;
      continue;
    }
    if (arg === "--forms" && next) {
      options.forms = next
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
      i += 1;
      continue;
    }
    if (arg === "--out-dir" && next) {
      options.outDir = next;
      i += 1;
      continue;
    }
    if (arg === "--limit" && next) {
      const parsed = Number(next);
      if (!Number.isNaN(parsed) && parsed > 0) {
        options.perSeriesLimit = parsed;
      }
      i += 1;
      continue;
    }
    if (arg === "--user-agent" && next) {
      options.userAgent = next;
      i += 1;
      continue;
    }
    if (arg === "--help") {
      printHelp();
      process.exit(0);
    }
  }

  if (!options.ticker && !options.cik) {
    throw new Error("Provide --ticker or --cik.");
  }
  if (!options.concepts.length) {
    options.concepts = [...DEFAULT_CONCEPTS];
  }

  return options;
};

const printHelp = (): void => {
  const text = `
SEC/XBRL time-series exhibit extractor

Usage:
  node --import tsx scripts/sec-xbrl-extract.ts --ticker AAPL [options]
  node --import tsx scripts/sec-xbrl-extract.ts --cik 0000320193 [options]

Options:
  --ticker <symbol>      SEC ticker symbol (e.g., AAPL)
  --cik <cik>            SEC CIK (zero-padded or not)
  --concept <key>        Repeatable concept key (e.g., us-gaap:Revenues)
  --forms <csv>          Filing forms filter (default: 10-K,10-Q,20-F,40-F)
  --out-dir <path>       Output root (default: sec-edgar-filings)
  --limit <n>            Max points per series (default: 64)
  --user-agent <value>   SEC-compliant user agent; also supports SEC_USER_AGENT env
  --help                 Show this help
`;
  console.log(text.trim());
};

const run = async (): Promise<void> => {
  const options = parseArgs(process.argv.slice(2));
  const userAgent = options.userAgent || process.env.SEC_USER_AGENT;
  const cik = options.cik ?? (await resolveTickerToCik(options.ticker ?? "", fetch, userAgent));

  const companyFacts = await fetchCompanyFacts(cik, fetch, userAgent);
  const exhibit = buildTimeSeriesExhibit(companyFacts, options.concepts, {
    includeForms: options.forms,
    perSeriesLimit: options.perSeriesLimit,
  });

  const target = path.resolve(options.outDir, exhibit.cik);
  await fs.mkdir(target, { recursive: true });

  const jsonPath = path.join(target, "time-series.json");
  const csvPath = path.join(target, "time-series.csv");
  const mdPath = path.join(target, "time-series.md");

  await fs.writeFile(jsonPath, `${JSON.stringify(exhibit, null, 2)}\n`, "utf8");
  await fs.writeFile(csvPath, `${timeSeriesToCsv(exhibit)}\n`, "utf8");
  await fs.writeFile(mdPath, `${timeSeriesToMarkdown(exhibit)}\n`, "utf8");

  const seriesCount = exhibit.series.length;
  const pointCount = exhibit.series.reduce((sum, item) => sum + item.points.length, 0);
  console.log(`Wrote SEC/XBRL exhibits for ${exhibit.entityName} (CIK ${exhibit.cik})`);
  console.log(`Series: ${seriesCount}, points: ${pointCount}`);
  console.log(`JSON: ${jsonPath}`);
  console.log(`CSV: ${csvPath}`);
  console.log(`MD: ${mdPath}`);
};

run().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`SEC/XBRL extraction failed: ${message}`);
  process.exitCode = 1;
});
