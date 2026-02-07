#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

const repoRoot = process.cwd();
const corpusRoot = path.join(repoRoot, "research-corpus");
const seedFile = path.join(corpusRoot, "annotations", "seed_urls.txt");
const failuresPath = path.join(corpusRoot, "annotations", "failures.json");
const goldPath = path.join(corpusRoot, "annotations", "gold_standard.jsonl");
const localReferencePdf = "/mnt/data/world-class-investment-memos-corpus-2026-02-07.pdf";

const ensureDir = async (dir) => fs.mkdir(dir, { recursive: true });

const slugify = (value) =>
  String(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64) || "doc";

const inferFirm = (source) => {
  const text = source.toLowerCase();
  if (text.includes("altimeter")) return "Altimeter";
  if (text.includes("sequoia")) return "Sequoia";
  if (text.includes("a16z") || text.includes("andreessen")) return "a16z";
  if (text.includes("goldmansachs")) return "Goldman Sachs";
  if (text.includes("abovethecrowd") || text.includes("gurley")) return "Benchmark";
  if (text.includes("thiel")) return "Founders Fund / Thiel proxy";
  if (text.includes("colossus") || text.includes("durable")) return "Durable Capital proxy";
  return "Unknown";
};

const inferDocType = (url, contentType) => {
  const lowered = url.toLowerCase();
  if (contentType.includes("pdf") || lowered.endsWith(".pdf")) return "report";
  if (lowered.includes("letter")) return "memo";
  if (lowered.includes("state-of") || lowered.includes("report")) return "report";
  if (lowered.includes("episode") || lowered.includes("crowd")) return "essay";
  return "deck";
};

const guessTitleFromHtml = (html) => {
  const m = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  if (!m) return null;
  return m[1].replace(/\s+/g, " ").trim().slice(0, 180);
};

const stripHtml = (html) =>
  html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();

const extractPdfTextBestEffort = (buffer) => {
  const latin = buffer.toString("latin1");
  const strings = latin.match(/\(([^\)]{20,})\)/g) || [];
  const joined = strings
    .slice(0, 500)
    .map((s) => s.slice(1, -1))
    .join(" ")
    .replace(/\\\d{3}/g, " ")
    .replace(/\\[nrt]/g, " ")
    .replace(/\\\(|\\\)|\\\\/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (joined.length >= 120) return joined;
  return "PDF parsed text unavailable in this environment; use Poppler/pdfplumber for high-fidelity extraction.";
};

const buildGoldRecord = ({ id, firm, title, type, publishedDate, url, contentType }) => ({
  id,
  firm,
  title,
  type,
  published_date: publishedDate,
  url,
  tags: [
    "institutional_research",
    type,
    contentType.includes("pdf") ? "pdf" : "web",
    firm.toLowerCase().replace(/\s+/g, "_")
  ],
  why_world_class: [
    "Clear investment thesis with explicit framing.",
    "High information density with decision-relevant structure.",
    "Strong narrative-to-implication linkage for capital allocation."
  ],
  section_map: [
    "thesis",
    "market_context",
    "evidence",
    "implications",
    "risks"
  ],
  writing_patterns: [
    "top-down framing then bottom-up evidence",
    "explicit consensus vs variant contrast",
    "concise, high-signal prose"
  ],
  exhibit_patterns: [
    "few but high-impact exhibits",
    "clear chart title and interpretation",
    "quant and narrative alignment"
  ],
  anti_patterns: [
    "data dump without synthesis",
    "unsupported claims",
    "generic recommendations"
  ],
  status: "fetched"
});

const safeWriteJson = async (filePath, value) => {
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
};

const ensureCorpusBase = async () => {
  await ensureDir(path.join(corpusRoot, "sources", "pdf"));
  await ensureDir(path.join(corpusRoot, "sources", "html"));
  await ensureDir(path.join(corpusRoot, "sources", "text"));
  await ensureDir(path.join(corpusRoot, "snapshots"));
  try {
    await fs.access(failuresPath);
  } catch {
    await safeWriteJson(failuresPath, []);
  }
  try {
    await fs.access(goldPath);
  } catch {
    await fs.writeFile(goldPath, "", "utf8");
  }
};

const loadFailures = async () => {
  try {
    const raw = await fs.readFile(failuresPath, "utf8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

const appendGold = async (record) => {
  await fs.appendFile(goldPath, `${JSON.stringify(record)}\n`, "utf8");
};

const loadExistingGoldIds = async () => {
  try {
    const raw = await fs.readFile(goldPath, "utf8");
    const ids = new Set();
    for (const line of raw.split(/\r?\n/)) {
      if (!line.trim()) continue;
      try {
        const parsed = JSON.parse(line);
        if (parsed && typeof parsed.id === "string") ids.add(parsed.id);
      } catch {
        // ignore malformed lines; keep ingest resilient
      }
    }
    return ids;
  } catch {
    return new Set();
  }
};

const inferPublishedDate = (url, content) => {
  const fromUrl = url.match(/(20\d{2})[-/](\d{2})[-/](\d{2})/);
  if (fromUrl) return `${fromUrl[1]}-${fromUrl[2]}-${fromUrl[3]}`;
  const fromContent = content.match(/\b(20\d{2})-(\d{2})-(\d{2})\b/);
  if (fromContent) return `${fromContent[1]}-${fromContent[2]}-${fromContent[3]}`;
  return null;
};

const snapshotId = (prefix, source) => {
  const hash = crypto.createHash("sha1").update(source).digest("hex").slice(0, 10);
  return `${prefix}-${hash}`;
};

const saveSnapshot = async ({ id, rawBuffer, rawExt, parsedText, metadata, sourceKind }) => {
  const snapRoot = path.join(corpusRoot, "snapshots", id);
  const rawDir = path.join(snapRoot, "raw");
  const parsedDir = path.join(snapRoot, "parsed");
  await ensureDir(rawDir);
  await ensureDir(parsedDir);

  const rawName = `document${rawExt}`;
  await fs.writeFile(path.join(rawDir, rawName), rawBuffer);
  await fs.writeFile(path.join(parsedDir, "document.txt"), `${parsedText}\n`, "utf8");
  await safeWriteJson(path.join(snapRoot, "metadata.json"), metadata);

  const targetDir =
    sourceKind === "pdf"
      ? path.join(corpusRoot, "sources", "pdf")
      : sourceKind === "html"
        ? path.join(corpusRoot, "sources", "html")
        : path.join(corpusRoot, "sources", "text");
  await fs.writeFile(path.join(targetDir, `${id}${rawExt}`), rawBuffer);
};

const ingestUrl = async (url, failures, existingGoldIds) => {
  const id = snapshotId("url", url);
  const nowIso = new Date().toISOString();
  const response = await fetch(url, {
    redirect: "follow",
    headers: {
      "user-agent": "OpenClaw-ResearchCorpus/1.0"
    }
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const contentType = (response.headers.get("content-type") || "").toLowerCase();
  const buffer = Buffer.from(await response.arrayBuffer());
  const isPdf = contentType.includes("pdf") || url.toLowerCase().endsWith(".pdf");
  const isHtml = contentType.includes("html") || /\.html?($|\?)/i.test(url) || (!isPdf && !contentType.includes("json"));

  let parsedText = "";
  let rawExt = ".bin";
  let sourceKind = "text";
  let title = path.basename(url.split("?")[0] || "document");

  if (isPdf) {
    parsedText = extractPdfTextBestEffort(buffer);
    rawExt = ".pdf";
    sourceKind = "pdf";
  } else if (isHtml) {
    const html = buffer.toString("utf8");
    parsedText = stripHtml(html);
    rawExt = ".html";
    sourceKind = "html";
    title = guessTitleFromHtml(html) || title;
  } else {
    parsedText = buffer.toString("utf8");
    rawExt = ".txt";
    sourceKind = "text";
  }

  const firm = inferFirm(url);
  const publishedDate = inferPublishedDate(url, parsedText);
  const metadata = {
    id,
    url,
    fetched_at: nowIso,
    firm,
    title,
    content_type: contentType || "unknown",
    source_kind: sourceKind
  };

  await saveSnapshot({
    id,
    rawBuffer: buffer,
    rawExt,
    parsedText,
    metadata,
    sourceKind
  });

  if (!existingGoldIds.has(id)) {
    await appendGold(
      buildGoldRecord({
        id,
        firm,
        title,
        type: inferDocType(url, contentType),
        publishedDate,
        url,
        contentType
      })
    );
    existingGoldIds.add(id);
  }

  return { id, title };
};

const ingestLocalPdf = async (filePath, failures, existingGoldIds) => {
  const id = snapshotId("local", filePath);
  const nowIso = new Date().toISOString();
  try {
    const buffer = await fs.readFile(filePath);
    const parsedText = extractPdfTextBestEffort(buffer);
    const title = path.basename(filePath);
    const metadata = {
      id,
      url: filePath,
      fetched_at: nowIso,
      firm: "reference_corpus",
      title,
      content_type: "application/pdf",
      source_kind: "pdf"
    };

    await saveSnapshot({
      id,
      rawBuffer: buffer,
      rawExt: ".pdf",
      parsedText,
      metadata,
      sourceKind: "pdf"
    });

    if (!existingGoldIds.has(id)) {
      await appendGold(
        buildGoldRecord({
          id,
          firm: "reference_corpus",
          title,
          type: "report",
          publishedDate: null,
          url: filePath,
          contentType: "application/pdf"
        })
      );
      existingGoldIds.add(id);
    }
    return { id, title };
  } catch (error) {
    failures.push({
      id,
      source: filePath,
      stage: "local_pdf",
      reason: error instanceof Error ? error.message : String(error),
      timestamp: nowIso
    });
    if (!existingGoldIds.has(id)) {
      const fallback = buildGoldRecord({
        id,
        firm: "reference_corpus",
        title: path.basename(filePath),
        type: "report",
        publishedDate: null,
        url: filePath,
        contentType: "application/pdf"
      });
      fallback.status = "fetch_failed";
      fallback.fetch_error = error instanceof Error ? error.message : String(error);
      await appendGold(fallback);
      existingGoldIds.add(id);
    }
    return null;
  }
};

const main = async () => {
  await ensureCorpusBase();
  const failures = await loadFailures();
  const existingGoldIds = await loadExistingGoldIds();
  const rawSeeds = await fs.readFile(seedFile, "utf8");
  const seeds = rawSeeds
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"));

  for (const url of seeds) {
    try {
      await ingestUrl(url, failures, existingGoldIds);
      console.log(`ingested ${url}`);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      const id = snapshotId("url", url);
      failures.push({
        id,
        source: url,
        stage: "fetch",
        reason,
        timestamp: new Date().toISOString()
      });
      if (!existingGoldIds.has(id)) {
        const fallback = buildGoldRecord({
          id,
          firm: inferFirm(url),
          title: path.basename(url.split("?")[0] || url) || slugify(url),
          type: inferDocType(url, ""),
          publishedDate: null,
          url,
          contentType: "unknown"
        });
        fallback.status = "fetch_failed";
        fallback.fetch_error = reason;
        await appendGold(fallback);
        existingGoldIds.add(id);
      }
      console.error(`failed ${url}: ${reason}`);
    }
  }

  await ingestLocalPdf(localReferencePdf, failures, existingGoldIds);
  await safeWriteJson(failuresPath, failures);
};

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
