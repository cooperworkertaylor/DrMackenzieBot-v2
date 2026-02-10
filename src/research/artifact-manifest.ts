import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

export type ArtifactKind = "memo" | "theme_report" | "sector_report";

export type ArtifactManifest = {
  version: 1;
  kind: ArtifactKind;
  outPath: string;
  builtAt: string;
  bytes: number;
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

const readPreviousSha = async (manifestPath: string): Promise<string | undefined> => {
  try {
    const raw = await fs.readFile(manifestPath, "utf8");
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object") return undefined;
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
  metrics?: Record<string, number | string | boolean | null>;
  gate?: Record<string, unknown>;
}): Promise<{ manifestPath: string; manifest: ArtifactManifest }> => {
  const outPath = path.resolve(params.outPath);
  const manifestPath = `${outPath}.artifact.json`;
  const previousSha = await readPreviousSha(manifestPath);
  const currentSha = sha256(params.markdown);
  const unchangedFromPrevious = Boolean(previousSha && previousSha === currentSha);

  const manifest: ArtifactManifest = {
    version: 1,
    kind: params.kind,
    outPath,
    builtAt: new Date().toISOString(),
    bytes: Buffer.byteLength(params.markdown, "utf8"),
    sha256: currentSha,
    unchangedFromPrevious,
    previousSha256: previousSha,
    metrics: {
      exhibitCount: countExhibits(params.markdown),
      takeawayCount: countTakeaways(params.markdown),
      ...(params.metrics ?? {}),
    },
    gate: params.gate,
  };

  await fs.writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  return { manifestPath, manifest };
};
