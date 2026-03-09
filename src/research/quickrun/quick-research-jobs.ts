import fsSync from "node:fs";
import os from "node:os";
import type { OpenClawConfig } from "../../config/config.js";
import type { QuickResearchRequest } from "../quick-research-request.js";
import { resolveStateDir } from "../../config/paths.js";
import { getLogger } from "../../logging/logger.js";
import { writeFileArtifactManifest } from "../artifact-manifest.js";
import { diagnosePdfBuffer } from "../pdf-diagnostics.js";
import { computeThemeResearch } from "../theme-sector.js";
import {
  inferThemeUniverseFromDb,
  inferThemeUniverseFromInstruments,
  normalizeThemeTickerUniverse,
} from "../theme-universe-infer.js";
import { createQuickrunJobRunner } from "./job-runner.js";
import { QuickrunJobStore, type QuickrunJobRecord } from "./job-store.js";

const QUICK_RESEARCH_JOB_TYPE = "quick_research_pdf_v2";

export type QuickResearchJobPayload = {
  jobId: string;
  request: QuickResearchRequest;
  createdAtMs: number;
  deliverAtMs: number;
  researchProfile?: {
    key: string;
    label: string;
    modelRef: string;
    profileId?: string;
  };
  route: {
    channel: string;
    to: string;
    accountId?: string;
    threadId?: string;
    sessionKey?: string;
  };
};

const escapeHtml = (value: string): string =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

const slugify = (value: string): string =>
  value
    .trim()
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/g, "-")
    .replaceAll(/(^-+|-+$)/g, "")
    .slice(0, 64) || "report";

export const formatBuiltAtEt = (dt: Date): string => {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(dt);
  const pick = (type: string) => parts.find((p) => p.type === type)?.value ?? "";
  return `${pick("year")}-${pick("month")}-${pick("day")} ${pick("hour")}:${pick("minute")} ET`;
};

const sanitizeThemeLabel = (value: string): string => {
  const raw = value.trim();
  const m = raw.match(/^\s*\[([^\]]{1,800})\]\s*(.+)$/s);
  if (!m) return raw;
  const header = (m[1] ?? "").toLowerCase();
  if (
    header.includes("telegram") ||
    header.includes("whatsapp") ||
    header.includes("signal") ||
    header.includes("discord") ||
    header.includes("slack") ||
    header.includes(" id:") ||
    header.includes(" est") ||
    header.includes(" et") ||
    header.match(/\+\s*\d{1,3}\s*m\b/) ||
    header.match(/\b\d{4}-\d{2}-\d{2}\b/) ||
    header.match(/\b\d{2}:\d{2}\b/)
  ) {
    return (m[2] ?? "").trim();
  }
  return raw;
};

const resolveChromeExecutablePath = (): string | undefined => {
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

const delay = async (ms: number) => {
  if (ms <= 0) return;
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
};

const normalizeTickerList = (values: Iterable<unknown>): string[] =>
  Array.from(values)
    .map((value) => (typeof value === "string" ? value.trim().toUpperCase() : ""))
    .filter(Boolean);

type ReportSectionBlock = {
  tag?: string;
  text?: string;
};

type ReportSection = {
  key?: string;
  title?: string;
  blocks?: ReportSectionBlock[];
};

type QuickResearchReportShape = {
  sections?: ReportSection[];
  appendix?: {
    whats_missing?: string[];
  };
};

const normalizeTextLine = (value: string): string =>
  value
    .replaceAll(/\s+/g, " ")
    .replaceAll(/\s+([,.;:!?])/g, "$1")
    .trim();

const takeDistinctLines = (values: Array<string | undefined>, limit: number): string[] => {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const normalized = normalizeTextLine(value ?? "");
    if (!normalized) continue;
    const key = normalized.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(normalized);
    if (out.length >= limit) break;
  }
  return out;
};

const collectSectionLines = (
  report: QuickResearchReportShape,
  sectionKeys: string[],
  preferredTags: string[],
  limit: number,
): string[] => {
  const keySet = new Set(sectionKeys.map((value) => value.trim().toLowerCase()));
  const tagSet = new Set(preferredTags.map((value) => value.trim().toUpperCase()));
  const matchingSections = (report.sections ?? []).filter((section) =>
    keySet.has(
      String(section.key ?? "")
        .trim()
        .toLowerCase(),
    ),
  );
  const tagged = matchingSections.flatMap((section) =>
    (section.blocks ?? [])
      .filter((block) =>
        tagSet.has(
          String(block.tag ?? "")
            .trim()
            .toUpperCase(),
        ),
      )
      .map((block) => block.text),
  );
  const fallback = matchingSections.flatMap((section) =>
    (section.blocks ?? []).map((block) => block.text),
  );
  return takeDistinctLines(tagged.length ? tagged : fallback, limit);
};

export const buildQuickResearchTelegramSummary = (params: {
  kind: QuickResearchRequest["kind"];
  subject: string;
  jobId: string;
  runId: string;
  builtAtEt: string;
  pdfBytes: number;
  sha256: string;
  report: QuickResearchReportShape;
}): string => {
  const overview = collectSectionLines(
    params.report,
    ["executive_summary", "what_it_is_isnt_why_now"],
    ["INTERPRETATION", "FACT"],
    2,
  );
  const thesis = collectSectionLines(
    params.report,
    ["thesis", "variant_perception"],
    ["INTERPRETATION", "FACT", "ASSUMPTION"],
    2,
  );
  const risks = collectSectionLines(
    params.report,
    ["risks_premortem", "risks_falsifiers", "change_mind_triggers"],
    ["INTERPRETATION", "FACT", "ASSUMPTION"],
    2,
  );
  const catalysts = collectSectionLines(
    params.report,
    ["catalysts", "catalysts_timeline"],
    ["FACT", "INTERPRETATION"],
    2,
  );
  const missing = takeDistinctLines(params.report.appendix?.whats_missing ?? [], 2);

  const lines: string[] = [];
  lines.push(
    params.kind === "company"
      ? `Company memo ready: ${params.subject}`
      : `Theme memo ready: ${params.subject}`,
  );
  lines.push(`Built: ${params.builtAtEt}`);
  lines.push("");
  lines.push("Top line");
  for (const line of overview) lines.push(`- ${line}`);
  if (!overview.length) lines.push("- Memo completed; see PDF for the full investment case.");
  lines.push("");
  lines.push("Thesis / variant");
  for (const line of thesis) lines.push(`- ${line}`);
  if (!thesis.length) lines.push("- See thesis section in the attached PDF.");
  lines.push("");
  lines.push("Risks / change-mind triggers");
  for (const line of risks) lines.push(`- ${line}`);
  if (!risks.length) lines.push("- No concise risk lines extracted; use the PDF risks section.");
  if (catalysts.length) {
    lines.push("");
    lines.push("Catalysts");
    for (const line of catalysts) lines.push(`- ${line}`);
  }
  if (missing.length) {
    lines.push("");
    lines.push("Missing / next diligence");
    for (const line of missing) lines.push(`- ${line}`);
  }
  lines.push("");
  lines.push(`job_id=${params.jobId}`);
  lines.push(`run_id=${params.runId}`);
  lines.push(`sha256=${params.sha256}`);
  lines.push(`bytes=${params.pdfBytes}`);
  return lines.join("\n");
};

const safeSend = async (params: {
  cfg: OpenClawConfig;
  route: QuickResearchJobPayload["route"];
  text: string;
  isError?: boolean;
  mediaUrl?: string;
}) => {
  const { routeReply } = await import("../../auto-reply/reply/route-reply.js");
  await routeReply({
    payload: {
      text: params.text,
      ...(params.isError ? { isError: true } : {}),
      ...(params.mediaUrl ? { mediaUrl: params.mediaUrl } : {}),
    },
    channel: params.route.channel,
    to: params.route.to,
    accountId: params.route.accountId,
    threadId: params.route.threadId,
    cfg: params.cfg,
    sessionKey: params.route.sessionKey,
    mirror: false,
  });
};

const renderPdfFromMarkdown = async (params: {
  inPath: string;
  outPath: string;
  title: string;
  chromePath: string;
  builtAtEt: string;
}) => {
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const { pathToFileURL } = await import("node:url");
  const { chromium } = await import("playwright-core");
  const { default: MarkdownIt } = await import("markdown-it");

  const markdown = await fs.readFile(params.inPath, "utf8");
  const unicodeDashCount = (markdown.match(/[\u2010-\u2015\u2212]/g) ?? []).length;
  if (unicodeDashCount > 0) {
    throw new Error(
      `Unicode dash characters detected in markdown (count=${unicodeDashCount}). Replace with ASCII hyphens before rendering.`,
    );
  }

  const md = new MarkdownIt({
    html: true,
    linkify: true,
    breaks: false,
    typographer: false,
  });
  const bodyHtml = md.render(markdown);
  const baseHref = pathToFileURL(path.dirname(params.inPath) + path.sep).href;
  const builtAtEtFooter = params.builtAtEt.replaceAll("-", "/");

  const html = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <base href="${escapeHtml(baseHref)}" />
    <title>${escapeHtml(params.title)}</title>
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
      html { -webkit-print-color-adjust: exact; print-color-adjust: exact; background: var(--bg); }
      body {
        margin: 0; padding: 0; color: var(--text); background: var(--bg);
        font-family: var(--font-serif); font-size: 11.5pt; line-height: 1.38;
      }
      .doc { max-width: 7.2in; margin: 0 auto; padding: 0.15in 0; }
      h1, h2, h3, h4 { font-family: var(--font-sans); letter-spacing: -0.01em; margin: 0; }
      h1 { font-size: 22pt; line-height: 1.15; margin-bottom: 0.18in; }
      h2 { font-size: 14pt; margin-top: 0.26in; padding-top: 0.16in; border-top: 1px solid var(--rule); margin-bottom: 0.1in; }
      h3 { font-size: 11.75pt; margin-top: 0.16in; margin-bottom: 0.06in; }
      p { margin: 0 0 0.12in 0; }
      ul, ol { margin: 0 0 0.12in 0.22in; padding: 0; }
      li { margin: 0.04in 0; }
      blockquote { margin: 0.12in 0; padding: 0.12in 0.16in; border-left: 3px solid var(--rule); background: #fafafa; }
      table { width: 100%; border-collapse: collapse; margin: 0.14in 0; font-family: var(--font-sans); font-size: 10pt; }
      th, td { border-bottom: 1px solid var(--rule); padding: 0.06in 0.08in; vertical-align: top; }
      th { text-align: left; font-weight: 600; }
      code { font-family: var(--font-mono); font-size: 0.95em; background: #f3f4f6; padding: 0.03em 0.2em; border-radius: 4px; }
      pre { background: #0b1020; color: #e5e7eb; padding: 0.14in; border-radius: 8px; overflow-x: auto; margin: 0.14in 0; }
      pre code { background: transparent; padding: 0; border-radius: 0; font-size: 9.5pt; }
      a { color: var(--accent); text-decoration: none; }
      a:hover { text-decoration: underline; }
      h2, h3, h4 { break-after: avoid-page; }
      table, blockquote, pre { break-inside: avoid; }
    </style>
  </head>
  <body>
    <main class="doc">${bodyHtml}</main>
  </body>
</html>`;

  const browser = await chromium.launch({ headless: true, executablePath: params.chromePath });
  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "load" });
    await page.emulateMedia({ media: "print" });
    const headerTemplate = `<div style="font-size:8px;padding:0 24px;width:100%;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;color:#6b7280;display:flex;align-items:center;justify-content:space-between;"><span>${escapeHtml(params.title)}</span><span></span></div>`;
    const footerTemplate = `<div style="font-size:8px;padding:0 24px;width:100%;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',Helvetica,Arial,sans-serif;color:#6b7280;display:flex;align-items:center;justify-content:space-between;"><span>FOR DISCUSSION PURPOSES ONLY; NOT INVESTMENT ADVICE.</span><span>Built: ${escapeHtml(builtAtEtFooter)}</span><span>Page <span class="pageNumber"></span> / <span class="totalPages"></span></span></div>`;
    await page.pdf({
      path: params.outPath,
      format: "letter",
      printBackground: true,
      displayHeaderFooter: true,
      headerTemplate,
      footerTemplate,
      margin: { top: "0.9in", bottom: "0.9in", left: "0.9in", right: "0.9in" },
    });
  } finally {
    await browser.close();
  }
};

const runCompanyQuickResearch = async (params: {
  payload: QuickResearchJobPayload;
  cfg: OpenClawConfig;
  chromePath: string;
  builtAtEt: string;
}) => {
  if (params.payload.request.kind !== "company") {
    throw new Error("Company quick research runner received a non-company payload");
  }
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const { runCompanyPipelineV2 } = await import("../../v2/pipeline/v2-pipeline.js");

  const result = await runCompanyPipelineV2({
    ticker: params.payload.request.ticker,
    question: params.payload.request.question,
    timeboxMinutes: params.payload.request.minutes,
    llmProfile: params.payload.researchProfile,
  });
  if (!result.passed) {
    await delay(Math.max(0, params.payload.deliverAtMs - Date.now()));
    await safeSend({
      cfg: params.cfg,
      route: params.payload.route,
      isError: true,
      text: `FAILED QUALITY GATE (v2)\njob_id=${params.payload.jobId}\nrun_id=${result.runId}\nTop issues:\n- ${result.issues
        .filter((i: { severity: string }) => i.severity === "error")
        .slice(0, 8)
        .map(
          (i: { code: string; message: string; path?: string }) =>
            `${i.code}${i.path ? `@${i.path}` : ""}: ${i.message}`,
        )
        .join("\n- ")}`,
    });
    return;
  }

  const outDir = path.join(path.dirname(result.reportMarkdownPath), "artifacts");
  await fs.mkdir(outDir, { recursive: true });
  const pdfPath = path.join(
    outDir,
    `${slugify(params.payload.request.ticker)}-${new Date().toISOString().replaceAll(/[:.]/g, "-")}.pdf`,
  );
  await renderPdfFromMarkdown({
    inPath: result.reportMarkdownPath,
    outPath: pdfPath,
    title: `${params.payload.request.ticker} (v2)`,
    chromePath: params.chromePath,
    builtAtEt: params.builtAtEt,
  });

  const pdfBuffer = await fs.readFile(pdfPath);
  const reportJson = JSON.parse(
    await fs.readFile(result.reportJsonPath, "utf8"),
  ) as QuickResearchReportShape;
  const diag = await diagnosePdfBuffer({
    buffer: new Uint8Array(pdfBuffer),
    maxPages: 50,
    strict: true,
  });
  if (diag.errors.length) {
    await delay(Math.max(0, params.payload.deliverAtMs - Date.now()));
    await safeSend({
      cfg: params.cfg,
      route: params.payload.route,
      isError: true,
      text: `FAILED PDF DIAGNOSTICS (strict)\njob_id=${params.payload.jobId}\nrun_id=${result.runId}\n- ${diag.errors.slice(0, 12).join("\n- ")}`,
    });
    return;
  }

  const stateDir = resolveStateDir(process.env, os.homedir);
  const seriesKey = `quickrun_company_${slugify(params.payload.request.ticker)}`;
  const seriesManifestPath = path.join(
    stateDir,
    "research",
    "quickrun",
    `${seriesKey}.artifact.json`,
  );
  await fs.mkdir(path.dirname(seriesManifestPath), { recursive: true, mode: 0o700 });
  const manifestRes = await writeFileArtifactManifest({
    kind: "memo",
    outPath: pdfPath,
    seriesKey,
    seriesManifestPath,
    markdownForMetrics: await fs.readFile(result.reportMarkdownPath, "utf8"),
    metrics: {
      job_id: params.payload.jobId,
      run_id: result.runId,
      built_at_et: params.builtAtEt,
      kind: "company",
    },
  });
  if (manifestRes.manifest.unchangedFromPrevious) {
    await delay(Math.max(0, params.payload.deliverAtMs - Date.now()));
    await safeSend({
      cfg: params.cfg,
      route: params.payload.route,
      isError: true,
      text: `❌ Refusing to send: unchanged PDF artifact detected\njob_id=${params.payload.jobId}\nrun_id=${result.runId}\nsha256=${manifestRes.manifest.sha256}`,
    });
    return;
  }

  await delay(Math.max(0, params.payload.deliverAtMs - Date.now()));
  await safeSend({
    cfg: params.cfg,
    route: params.payload.route,
    mediaUrl: pdfPath,
    text: buildQuickResearchTelegramSummary({
      kind: "company",
      subject: params.payload.request.ticker,
      jobId: params.payload.jobId,
      runId: result.runId,
      builtAtEt: params.builtAtEt,
      pdfBytes: pdfBuffer.length,
      sha256: manifestRes.manifest.sha256,
      report: reportJson,
    }),
  });
};

const runThemeQuickResearch = async (params: {
  payload: QuickResearchJobPayload;
  cfg: OpenClawConfig;
  chromePath: string;
  builtAtEt: string;
}) => {
  if (params.payload.request.kind !== "theme") {
    throw new Error("Theme quick research runner received a non-theme payload");
  }
  const fs = await import("node:fs/promises");
  const path = await import("node:path");
  const { runThemePipelineV2 } = await import("../../v2/pipeline/v2-pipeline.js");

  const themeLabel = sanitizeThemeLabel(params.payload.request.theme);
  const explicitUniverse = Array.from(
    new Set(normalizeTickerList(params.payload.request.tickers ?? [])),
  ).slice(0, 60);
  let inferredUniverse: {
    scanned_docs: number;
    inferred_tickers: string[];
    inferred_domains: string[];
    inferred_entities?: Array<{
      id: string;
      type: "equity" | "crypto_asset" | "protocol" | "private_company" | "index" | "other";
      label: string;
      symbol?: string;
      urls?: string[];
    }>;
  } | null = null;

  let themeRes: { tickers: string[] } | undefined;
  if (explicitUniverse.length) {
    themeRes = computeThemeResearch({ theme: themeLabel, tickers: explicitUniverse });
  } else {
    try {
      themeRes = computeThemeResearch({ theme: themeLabel });
    } catch {
      const inferred = inferThemeUniverseFromDb({ theme: themeLabel });
      inferredUniverse = inferred;
      let tickers = normalizeTickerList(inferred.inferred_tickers ?? []);
      if (!tickers.length) {
        const instrumentGuess = inferThemeUniverseFromInstruments({ theme: themeLabel });
        tickers = normalizeTickerList(instrumentGuess.inferred_tickers ?? []);
      }
      if (tickers.length) {
        themeRes = computeThemeResearch({ theme: themeLabel, tickers });
      }
    }
  }

  const maxUniverse =
    params.payload.request.minutes <= 10
      ? 5
      : params.payload.request.minutes <= 30
        ? 10
        : params.payload.request.minutes <= 60
          ? 15
          : 25;
  let universe = normalizeThemeTickerUniverse({
    tickers: themeRes?.tickers ?? [],
    maxTickers: maxUniverse,
  }).tickers;
  if (!universe.length) {
    const instrumentGuess = inferThemeUniverseFromInstruments({
      theme: themeLabel,
      maxTickers: Math.max(10, maxUniverse * 3),
    });
    universe = normalizeThemeTickerUniverse({
      tickers: normalizeTickerList(instrumentGuess.inferred_tickers ?? []),
      maxTickers: maxUniverse,
    }).tickers;
  }
  if (!universe.length) {
    await delay(Math.max(0, params.payload.deliverAtMs - Date.now()));
    await safeSend({
      cfg: params.cfg,
      route: params.payload.route,
      isError: true,
      text: [
        `❌ Could not resolve a ticker universe for theme="${themeLabel}".`,
        "",
        "Re-run with an explicit universe list, e.g.:",
        `- "${themeLabel} tickers: CIEN, LITE, COHR, INFN, ANET 5"`,
        `- "${themeLabel} universe: NVDA, AVGO, ANET 10"`,
      ].join("\n"),
    });
    return;
  }

  const result = await runThemePipelineV2({
    themeName: themeLabel,
    universe,
    universeEntities:
      inferredUniverse?.inferred_entities && inferredUniverse.inferred_entities.length
        ? inferredUniverse.inferred_entities
        : undefined,
    timeboxMinutes: params.payload.request.minutes,
    llmProfile: params.payload.researchProfile,
  });
  if (!result.passed) {
    await delay(Math.max(0, params.payload.deliverAtMs - Date.now()));
    await safeSend({
      cfg: params.cfg,
      route: params.payload.route,
      isError: true,
      text: `FAILED QUALITY GATE (v2)\njob_id=${params.payload.jobId}\nrun_id=${result.runId}\nTop issues:\n- ${result.issues
        .filter((i: { severity: string }) => i.severity === "error")
        .slice(0, 8)
        .map(
          (i: { code: string; message: string; path?: string }) =>
            `${i.code}${i.path ? `@${i.path}` : ""}: ${i.message}`,
        )
        .join("\n- ")}`,
    });
    return;
  }

  const outDir = path.join(path.dirname(result.reportMarkdownPath), "artifacts");
  await fs.mkdir(outDir, { recursive: true });
  const pdfPath = path.join(
    outDir,
    `${slugify(themeLabel)}-${new Date().toISOString().replaceAll(/[:.]/g, "-")}.pdf`,
  );
  await renderPdfFromMarkdown({
    inPath: result.reportMarkdownPath,
    outPath: pdfPath,
    title: `${themeLabel} (v2)`,
    chromePath: params.chromePath,
    builtAtEt: params.builtAtEt,
  });

  const pdfBuffer = await fs.readFile(pdfPath);
  const reportJson = JSON.parse(
    await fs.readFile(result.reportJsonPath, "utf8"),
  ) as QuickResearchReportShape;
  const diag = await diagnosePdfBuffer({
    buffer: new Uint8Array(pdfBuffer),
    maxPages: 50,
    strict: true,
  });
  if (diag.errors.length) {
    await delay(Math.max(0, params.payload.deliverAtMs - Date.now()));
    await safeSend({
      cfg: params.cfg,
      route: params.payload.route,
      isError: true,
      text: `FAILED PDF DIAGNOSTICS (strict)\njob_id=${params.payload.jobId}\nrun_id=${result.runId}\n- ${diag.errors.slice(0, 12).join("\n- ")}`,
    });
    return;
  }

  const stateDir = resolveStateDir(process.env, os.homedir);
  const seriesKey = `quickrun_theme_${slugify(themeLabel)}`;
  const seriesManifestPath = path.join(
    stateDir,
    "research",
    "quickrun",
    `${seriesKey}.artifact.json`,
  );
  await fs.mkdir(path.dirname(seriesManifestPath), { recursive: true, mode: 0o700 });
  const manifestRes = await writeFileArtifactManifest({
    kind: "theme_report",
    outPath: pdfPath,
    seriesKey,
    seriesManifestPath,
    markdownForMetrics: await fs.readFile(result.reportMarkdownPath, "utf8"),
    metrics: {
      job_id: params.payload.jobId,
      run_id: result.runId,
      built_at_et: params.builtAtEt,
      kind: "theme",
    },
  });
  if (manifestRes.manifest.unchangedFromPrevious) {
    await delay(Math.max(0, params.payload.deliverAtMs - Date.now()));
    await safeSend({
      cfg: params.cfg,
      route: params.payload.route,
      isError: true,
      text: `❌ Refusing to send: unchanged PDF artifact detected\njob_id=${params.payload.jobId}\nrun_id=${result.runId}\nsha256=${manifestRes.manifest.sha256}`,
    });
    return;
  }

  await delay(Math.max(0, params.payload.deliverAtMs - Date.now()));
  await safeSend({
    cfg: params.cfg,
    route: params.payload.route,
    mediaUrl: pdfPath,
    text: [
      buildQuickResearchTelegramSummary({
        kind: "theme",
        subject: themeLabel,
        jobId: params.payload.jobId,
        runId: result.runId,
        builtAtEt: params.builtAtEt,
        pdfBytes: pdfBuffer.length,
        sha256: manifestRes.manifest.sha256,
        report: reportJson,
      }),
      `Universe: ${universe.join(", ")}`,
      inferredUniverse
        ? `Universe bootstrap: scanned_docs=${inferredUniverse.scanned_docs} inferred_domains=${inferredUniverse.inferred_domains.slice(0, 10).join(", ")}`
        : null,
    ]
      .filter(Boolean)
      .join("\n"),
  });
};

export const executeQuickResearchJob = async (
  job: QuickrunJobRecord<QuickResearchJobPayload>,
  cfg: OpenClawConfig,
) => {
  const payload = job.payload;
  try {
    const chromePath = resolveChromeExecutablePath();
    if (!chromePath) {
      throw new Error(
        "Chrome executable not found. Set OPENCLAW_CHROME_PATH on macmini and restart the gateway.",
      );
    }
    const builtAtEt = formatBuiltAtEt(new Date());

    if (payload.request.kind === "company") {
      await runCompanyQuickResearch({ payload, cfg, chromePath, builtAtEt });
    } else {
      await runThemeQuickResearch({ payload, cfg, chromePath, builtAtEt });
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await delay(Math.max(0, payload.deliverAtMs - Date.now()));
    await safeSend({
      cfg,
      route: payload.route,
      isError: true,
      text: `❌ quick research failed\njob_id=${payload.jobId}\nerror=${message}`,
    });
    throw err;
  }
};

let singleton: ReturnType<typeof createQuickrunJobRunner<QuickResearchJobPayload>> | null = null;
let currentCfg: OpenClawConfig | null = null;

const getOrCreateRunner = (cfg: OpenClawConfig, dbPath?: string) => {
  currentCfg = cfg;
  if (singleton) return singleton;
  const store = QuickrunJobStore.open(dbPath);
  singleton = createQuickrunJobRunner<QuickResearchJobPayload>({
    store,
    jobType: QUICK_RESEARCH_JOB_TYPE,
    handler: (job) => {
      if (!currentCfg) {
        throw new Error("Quick research worker started without config");
      }
      return executeQuickResearchJob(job, currentCfg);
    },
    pollIntervalMs: 1_000,
    staleAfterMs: 90_000,
    concurrency: 1,
  });
  singleton.start();
  getLogger().info({ jobType: QUICK_RESEARCH_JOB_TYPE }, "quickrun worker started");
  return singleton;
};

export const startQuickResearchWorker = (params: { cfg: OpenClawConfig; dbPath?: string }) =>
  getOrCreateRunner(params.cfg, params.dbPath);

export const stopQuickResearchWorker = () => {
  singleton?.stop();
  singleton = null;
  currentCfg = null;
};

export const enqueueQuickResearchJob = (params: {
  payload: QuickResearchJobPayload;
  cfg: OpenClawConfig;
}) => {
  const runner = getOrCreateRunner(params.cfg);
  return runner.enqueue({
    id: params.payload.jobId,
    jobType: QUICK_RESEARCH_JOB_TYPE,
    payload: params.payload,
    runAfterMs: params.payload.createdAtMs,
    maxAttempts: 3,
  });
};
