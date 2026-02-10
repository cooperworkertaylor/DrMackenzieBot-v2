import crypto from "node:crypto";
import * as fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import type { ArtifactKind } from "./artifact-manifest.js";

const BAD_DASHES = /[\u2010\u2011\u2012\u2013\u2014\u2212]/g;
const PLACEHOLDER_RE =
  /\b(to appear|appendix pass|provided in appendix|full appendix|appendix available|appendix to follow|sources to follow|sources? pending|pending sources?|link(?:s)? to follow|csv\/queries|queries\/csv|pinned query|query ids?|ready for live|swap(?:ped)? for live|can be swapped|available (?:on|upon|by) request|(?:on|upon|by) request|provided (?:on|upon|by) request|placeholder|tbd|todo|coming soon|to be added|to be provided|to be attached|will (?:add|attach|provide)|tktk|tk|lorem ipsum)\b/gi;

export type ArtifactPreflightSource = {
  key?: string;
  source?: string;
  ref?: string;
  date?: string;
  host?: string;
  url?: string;
  raw: string;
};

export type ArtifactPreflightAsset = {
  path: string;
  kind: "file" | "url" | "data";
  resolvedPath?: string;
  exists: boolean;
  byteSize?: number;
  fileModifiedAt?: string;
  sha256?: string;
};

export type ArtifactPreflightExhibit = {
  index: number;
  title: string;
  assets: ArtifactPreflightAsset[];
};

export type ArtifactPreflight = {
  version: 1;
  kind: ArtifactKind;
  inPath: string;
  outPath: string;
  builtAt: string;
  builtAtEt: string;
  markdownSha256: string;
  markdownBytes: number;
  metrics: {
    exhibitCount: number;
    takeawayCount: number;
    footnoteCount: number;
    sourcesCount: number;
    unicodeDashCount: number;
    sourcesWithUrlCount: number;
    sourcesWithKeyCount: number;
    placeholderTokenCount: number;
  };
  exhibits: ArtifactPreflightExhibit[];
  sources: ArtifactPreflightSource[];
};

const sha256 = (value: string): string => crypto.createHash("sha256").update(value).digest("hex");

const sha256File = async (filePath: string): Promise<string> => {
  const hash = crypto.createHash("sha256");
  await new Promise<void>((resolve, reject) => {
    const stream = fsSync.createReadStream(filePath);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve());
  });
  return hash.digest("hex");
};

const countExhibits = (markdown: string): number =>
  (markdown.match(/^###\s+Exhibit\s+\d+:/gim) ?? []).length;

const countTakeaways = (markdown: string): number =>
  (markdown.match(/\bTakeaway:\s+/gi) ?? []).length;

const countFootnotes = (markdown: string): number =>
  (markdown.match(/^\[\^[^\]]+\]:/gm) ?? []).length;

const countSources = (markdown: string): number => {
  const lines = markdown.split(/\r?\n/);
  let inSources = false;
  let count = 0;
  for (const line of lines) {
    if (/^###\s+Source\s+List\b/i.test(line) || /^###\s+Sources\b/i.test(line)) {
      inSources = true;
      continue;
    }
    if (!inSources) continue;
    if (/^#{1,3}\s+/.test(line)) break;
    if (/^- \S/.test(line.trimEnd())) count += 1;
  }
  return count;
};

const countUnicodeDashes = (markdown: string): number => (markdown.match(BAD_DASHES) ?? []).length;
const countPlaceholderTokens = (markdown: string): number =>
  (markdown.match(PLACEHOLDER_RE) ?? []).length;

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

const extractSourcesFromMarkdown = (markdown: string): ArtifactPreflightSource[] => {
  const lines = markdown.split(/\r?\n/);
  let inSources = false;
  const sources: ArtifactPreflightSource[] = [];
  for (const line of lines) {
    if (!inSources) {
      if (/^###\s+Source\s+List\b/i.test(line) || /^###\s+Sources\b/i.test(line)) {
        inSources = true;
      }
      continue;
    }
    if (/^#{1,3}\s+/.test(line)) break;
    const trimmed = line.trimEnd();
    if (!/^- \S/.test(trimmed)) continue;
    const raw = trimmed.replace(/^-+\s+/, "").trim();
    const [metaPartRaw, urlPartRaw] = raw.split("|", 2);
    const metaPart = (metaPartRaw ?? "").trim();
    const urlPart = (urlPartRaw ?? "").trim();

    const keyMatch = metaPart.match(/^(C\d+):\s*/i);
    const key = keyMatch?.[1]?.toUpperCase();
    const rest = metaPart.replace(/^(C\d+):\s*/i, "").trim();

    const extractField = (name: string): string | undefined => {
      const match = rest.match(new RegExp(`\\b${name}=([^\\s|]+)`, "i"));
      return match?.[1]?.trim();
    };

    const urlCandidate = urlPart && /^https?:\/\//i.test(urlPart) ? urlPart : undefined;
    const urlInlineMatch = rest.match(/https?:\/\/\S+/i);
    const url = urlCandidate ?? urlInlineMatch?.[0];

    sources.push({
      key,
      source: extractField("source"),
      ref: extractField("ref"),
      date: extractField("date"),
      host: extractField("host"),
      url,
      raw,
    });
  }
  return sources;
};

const parseMarkdownImagePaths = (line: string): string[] => {
  const out: string[] = [];
  const imageRe = /!\[[^\]]*]\(([^)]+)\)/g;
  for (;;) {
    const match = imageRe.exec(line);
    if (!match) break;
    const value = (match[1] ?? "").trim();
    if (value) out.push(value);
  }
  const htmlImgRe = /<img\s+[^>]*src=(\"([^\"]+)\"|'([^']+)'|([^\\s>]+))[^>]*>/gi;
  for (;;) {
    const match = htmlImgRe.exec(line);
    if (!match) break;
    const value = (match[2] ?? match[3] ?? match[4] ?? "").trim();
    if (value) out.push(value);
  }
  return out;
};

const resolveAsset = async (params: {
  rawPath: string;
  baseDir: string;
}): Promise<ArtifactPreflightAsset> => {
  const raw = params.rawPath.trim();
  if (/^data:/i.test(raw)) {
    return { path: raw, kind: "data", exists: true };
  }
  if (/^https?:\/\//i.test(raw)) {
    return { path: raw, kind: "url", exists: true };
  }
  const resolvedPath = path.resolve(params.baseDir, raw);
  try {
    const stat = await fs.stat(resolvedPath);
    const sha = await sha256File(resolvedPath);
    return {
      path: raw,
      kind: "file",
      resolvedPath,
      exists: true,
      byteSize: stat.size,
      fileModifiedAt: stat.mtime.toISOString(),
      sha256: sha,
    };
  } catch {
    return { path: raw, kind: "file", resolvedPath, exists: false };
  }
};

const extractExhibitsFromMarkdown = async (params: {
  markdown: string;
  baseDir: string;
}): Promise<ArtifactPreflightExhibit[]> => {
  const lines = params.markdown.split(/\r?\n/);
  const exhibits: ArtifactPreflightExhibit[] = [];
  let current: ArtifactPreflightExhibit | null = null;
  const pendingAssets: string[] = [];

  const flush = async () => {
    if (!current) return;
    const unique = Array.from(new Set(pendingAssets));
    const assets = await Promise.all(
      unique.map((rawPath) => resolveAsset({ rawPath, baseDir: params.baseDir })),
    );
    exhibits.push({ ...current, assets });
    pendingAssets.length = 0;
    current = null;
  };

  for (const line of lines) {
    const exhibitMatch = line.match(/^###\s+Exhibit\s+(\d+):\s*(.+)\s*$/i);
    if (exhibitMatch) {
      await flush();
      const index = Number.parseInt(exhibitMatch[1] ?? "", 10);
      current = {
        index: Number.isFinite(index) ? index : exhibits.length + 1,
        title: (exhibitMatch[2] ?? "").trim() || `Exhibit ${exhibitMatch[1] ?? "?"}`,
        assets: [],
      };
      continue;
    }
    if (!current) continue;
    if (/^#{1,3}\s+/.test(line)) {
      await flush();
      continue;
    }
    parseMarkdownImagePaths(line).forEach((assetPath) => pendingAssets.push(assetPath));
  }

  await flush();
  return exhibits;
};

export const buildArtifactPreflight = async (params: {
  kind: ArtifactKind;
  inPath: string;
  outPath: string;
}): Promise<ArtifactPreflight> => {
  const inPath = path.resolve(params.inPath);
  const outPath = path.resolve(params.outPath);
  const markdown = await fs.readFile(inPath, "utf8");
  const builtAt = new Date();
  const sources = extractSourcesFromMarkdown(markdown);
  const exhibits = await extractExhibitsFromMarkdown({
    markdown,
    baseDir: path.dirname(inPath),
  });
  const markdownBytes = Buffer.byteLength(markdown, "utf8");
  const sourcesWithUrlCount = sources.filter(
    (s) => typeof s.url === "string" && s.url.trim(),
  ).length;
  const sourcesWithKeyCount = sources.filter(
    (s) => typeof s.key === "string" && s.key.trim(),
  ).length;

  return {
    version: 1,
    kind: params.kind,
    inPath,
    outPath,
    builtAt: builtAt.toISOString(),
    builtAtEt: formatBuiltAtEt(builtAt),
    markdownSha256: sha256(markdown),
    markdownBytes,
    metrics: {
      exhibitCount: countExhibits(markdown),
      takeawayCount: countTakeaways(markdown),
      footnoteCount: countFootnotes(markdown),
      sourcesCount: countSources(markdown),
      unicodeDashCount: countUnicodeDashes(markdown),
      sourcesWithUrlCount,
      sourcesWithKeyCount,
      placeholderTokenCount: countPlaceholderTokens(markdown),
    },
    exhibits,
    sources,
  };
};
