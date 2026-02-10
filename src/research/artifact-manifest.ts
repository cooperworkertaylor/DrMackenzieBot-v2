import crypto from "node:crypto";
import * as fsSync from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";

export type ArtifactKind = "memo" | "theme_report" | "sector_report";

export type ArtifactManifest = {
  version: 1;
  kind: ArtifactKind;
  /**
   * Optional stable identifier for an artifact series (e.g. basename without version suffix),
   * used to prevent false "unchanged" matches when a shared series manifest file is reused.
   */
  seriesKey?: string;
  outPath: string;
  builtAt: string;
  /** When available (file-based artifacts), the filesystem mtime for outPath. */
  fileModifiedAt?: string;
  bytes: number;
  /** Alias for bytes (matches external tooling terminology). */
  byteSize: number;
  sha256: string;
  unchangedFromPrevious: boolean;
  previousSha256?: string;
  metrics: Record<string, number | string | boolean | null>;
  gate?: Record<string, unknown>;
};

const sha256 = (value: string): string => crypto.createHash("sha256").update(value).digest("hex");

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

const BAD_DASHES = /[\u2010\u2011\u2012\u2013\u2014\u2212]/g;
const countUnicodeDashes = (markdown: string): number => (markdown.match(BAD_DASHES) ?? []).length;

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

const readPreviousSha = async (params: {
  manifestPath: string;
  expectedKind?: ArtifactKind;
  expectedSeriesKey?: string;
}): Promise<string | undefined> => {
  try {
    const raw = await fs.readFile(params.manifestPath, "utf8");
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object") return undefined;
    if (params.expectedKind) {
      const kind = (parsed as { kind?: unknown }).kind;
      if (typeof kind === "string" && kind !== params.expectedKind) return undefined;
    }
    if (params.expectedSeriesKey) {
      const seriesKey = (parsed as { seriesKey?: unknown }).seriesKey;
      if (typeof seriesKey !== "string") return undefined;
      if (seriesKey !== params.expectedSeriesKey) return undefined;
    }
    const value = (parsed as { sha256?: unknown }).sha256;
    return typeof value === "string" && value.trim() ? value.trim() : undefined;
  } catch {
    return undefined;
  }
};

export const writeArtifactManifest = async (params: {
  kind: ArtifactKind;
  outPath: string;
  markdown: string;
  /** Stable identifier for a versioned artifact series (optional). */
  seriesKey?: string;
  /**
   * Optional "series" manifest path (e.g. <outDir>/artifact.json).
   * When set, unchanged detection is performed against this file and the latest manifest is
   * also written there, enabling versioned outPaths while still blocking unchanged artifacts.
   */
  seriesManifestPath?: string;
  metrics?: Record<string, number | string | boolean | null>;
  gate?: Record<string, unknown>;
}): Promise<{ manifestPath: string; manifest: ArtifactManifest; seriesManifestPath?: string }> => {
  const outPath = path.resolve(params.outPath);
  const manifestPath = `${outPath}.artifact.json`;
  const seriesManifestPath = params.seriesManifestPath
    ? path.resolve(params.seriesManifestPath)
    : undefined;
  const previousSha = await readPreviousSha({
    manifestPath: seriesManifestPath ?? manifestPath,
    expectedKind: params.kind,
    expectedSeriesKey: params.seriesKey,
  });
  const currentSha = sha256(params.markdown);
  const unchangedFromPrevious = Boolean(previousSha && previousSha === currentSha);
  const bytes = Buffer.byteLength(params.markdown, "utf8");

  const manifest: ArtifactManifest = {
    version: 1,
    kind: params.kind,
    ...(params.seriesKey ? { seriesKey: params.seriesKey } : {}),
    outPath,
    builtAt: new Date().toISOString(),
    bytes,
    byteSize: bytes,
    sha256: currentSha,
    unchangedFromPrevious,
    previousSha256: previousSha,
    metrics: {
      exhibitCount: countExhibits(params.markdown),
      takeawayCount: countTakeaways(params.markdown),
      footnoteCount: countFootnotes(params.markdown),
      sourcesCount: countSources(params.markdown),
      unicodeDashCount: countUnicodeDashes(params.markdown),
      ...(params.metrics ?? {}),
    },
    gate: params.gate,
  };

  await fs.writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  if (seriesManifestPath) {
    await fs.writeFile(seriesManifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  }
  return { manifestPath, manifest, ...(seriesManifestPath ? { seriesManifestPath } : {}) };
};

export const writeFileArtifactManifest = async (params: {
  kind: ArtifactKind;
  outPath: string;
  /** Stable identifier for a versioned artifact series (optional). */
  seriesKey?: string;
  /** Optional shared series manifest path (e.g. <outDir>/artifact.json). */
  seriesManifestPath?: string;
  /** Optional markdown used only to compute quality metrics (exhibit/source/footnote counts). */
  markdownForMetrics?: string;
  metrics?: Record<string, number | string | boolean | null>;
  gate?: Record<string, unknown>;
}): Promise<{ manifestPath: string; manifest: ArtifactManifest; seriesManifestPath?: string }> => {
  const outPath = path.resolve(params.outPath);
  const manifestPath = `${outPath}.artifact.json`;
  const seriesManifestPath = params.seriesManifestPath
    ? path.resolve(params.seriesManifestPath)
    : undefined;
  const previousSha = await readPreviousSha({
    manifestPath: seriesManifestPath ?? manifestPath,
    expectedKind: params.kind,
    expectedSeriesKey: params.seriesKey,
  });
  const stat = await fs.stat(outPath);
  const bytes = stat.size;
  const currentSha = await sha256File(outPath);
  const unchangedFromPrevious = Boolean(previousSha && previousSha === currentSha);

  const markdown = params.markdownForMetrics;
  const manifest: ArtifactManifest = {
    version: 1,
    kind: params.kind,
    ...(params.seriesKey ? { seriesKey: params.seriesKey } : {}),
    outPath,
    builtAt: new Date().toISOString(),
    fileModifiedAt: stat.mtime.toISOString(),
    bytes,
    byteSize: bytes,
    sha256: currentSha,
    unchangedFromPrevious,
    previousSha256: previousSha,
    metrics: {
      exhibitCount: markdown ? countExhibits(markdown) : null,
      takeawayCount: markdown ? countTakeaways(markdown) : null,
      footnoteCount: markdown ? countFootnotes(markdown) : null,
      sourcesCount: markdown ? countSources(markdown) : null,
      unicodeDashCount: markdown ? countUnicodeDashes(markdown) : null,
      ...(params.metrics ?? {}),
    },
    gate: params.gate,
  };

  await fs.writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  if (seriesManifestPath) {
    await fs.writeFile(seriesManifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  }
  return { manifestPath, manifest, ...(seriesManifestPath ? { seriesManifestPath } : {}) };
};
