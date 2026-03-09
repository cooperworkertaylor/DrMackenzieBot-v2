import { createHash } from "node:crypto";
import fsSync from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { resolveStateDir } from "../config/paths.js";
import { canonicalizeUrl, inferReliabilityTier, type ReliabilityTier } from "../v2/evidence/evidence-store.js";

export type ExternalDocumentFingerprintInput = {
  sourceType: string;
  provider: string;
  sender: string;
  title: string;
  subject: string;
  url?: string;
  content: string;
  ticker?: string;
};

export const normalizeDocumentText = (value: string): string =>
  value
    .replaceAll(/\r\n/g, "\n")
    .replaceAll(/\u00a0/g, " ")
    .replaceAll(/[ \t]+/g, " ")
    .replaceAll(/\n{3,}/g, "\n\n")
    .trim();

export const safeCanonicalizeUrl = (value?: string): string => {
  const raw = (value ?? "").trim();
  if (!raw) return "";
  try {
    return canonicalizeUrl(raw);
  } catch {
    return raw;
  }
};

export const inferDocumentTrustTier = (params: {
  canonicalUrl?: string;
  sourceType: string;
  provider: string;
}): ReliabilityTier => {
  if (params.canonicalUrl) {
    return inferReliabilityTier(params.canonicalUrl);
  }
  if (params.sourceType === "email_research") return 2;
  if (params.sourceType === "newsletter") {
    const provider = params.provider.trim().toLowerCase();
    if (provider === "semianalysis" || provider === "stratechery") return 3;
  }
  return 4;
};

export const fingerprintExternalDocument = (input: ExternalDocumentFingerprintInput): string => {
  const canonicalUrl = safeCanonicalizeUrl(input.url);
  const normalizedContent = normalizeDocumentText(input.content);
  return createHash("sha256")
    .update(
      JSON.stringify({
        sourceType: input.sourceType,
        provider: input.provider.trim().toLowerCase(),
        sender: input.sender.trim().toLowerCase(),
        title: input.title.trim(),
        subject: input.subject.trim(),
        canonicalUrl,
        normalizedContent,
        ticker: (input.ticker ?? "").trim().toUpperCase(),
      }),
    )
    .digest("hex");
};

export const buildSourceKey = (params: {
  sourceType: string;
  provider: string;
  sender: string;
  canonicalUrl?: string;
}): string => {
  const provider = params.provider.trim().toLowerCase() || "other";
  const sender = params.sender.trim().toLowerCase();
  const sourceType = params.sourceType.trim().toLowerCase();
  const host = (() => {
    try {
      return params.canonicalUrl ? new URL(params.canonicalUrl).hostname.replace(/^www\./i, "") : "";
    } catch {
      return "";
    }
  })();
  return [sourceType, provider, sender || host || "unknown"].filter(Boolean).join(":");
};

export const scoreExternalDocumentMateriality = (params: {
  sourceType: string;
  ticker?: string;
  title: string;
  content: string;
  tags?: string[];
  canonicalUrl?: string;
}): number => {
  let score = 0.15;
  if ((params.ticker ?? "").trim()) score += 0.3;
  if (params.sourceType === "email_research") score += 0.25;
  if (params.sourceType === "newsletter") score += 0.15;
  const haystack = `${params.title}\n${params.content}`.toLowerCase();
  if (/\b(earnings|guidance|estimate|margin|valuation|thesis|catalyst|risk|regulation|acquisition|financing)\b/.test(haystack)) {
    score += 0.2;
  }
  if ((params.tags ?? []).some((tag) => /research|variant|competition|newsletter-sync/i.test(tag))) {
    score += 0.1;
  }
  if (params.canonicalUrl && params.canonicalUrl.includes("/p/")) {
    score += 0.05;
  }
  return Math.max(0, Math.min(1, Number(score.toFixed(3))));
};

export const writeRawExternalDocumentArtifact = async (params: {
  sourceKey: string;
  contentHash: string;
  payload: Record<string, unknown>;
}): Promise<string> => {
  const stateDir = resolveStateDir(process.env, os.homedir);
  const dir = path.join(stateDir, "research", "raw-artifacts", params.sourceKey.replaceAll(/[^a-z0-9:_-]+/gi, "-"));
  await fs.mkdir(dir, { recursive: true, mode: 0o700 });
  const outPath = path.join(dir, `${params.contentHash}.json`);
  await fs.writeFile(outPath, `${JSON.stringify(params.payload, null, 2)}\n`, "utf8");
  return outPath;
};

export const writeRawExternalDocumentArtifactSync = (params: {
  sourceKey: string;
  contentHash: string;
  payload: Record<string, unknown>;
}): string => {
  const stateDir = resolveStateDir(process.env, os.homedir);
  const dir = path.join(
    stateDir,
    "research",
    "raw-artifacts",
    params.sourceKey.replaceAll(/[^a-z0-9:_-]+/gi, "-"),
  );
  fsSync.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const outPath = path.join(dir, `${params.contentHash}.json`);
  fsSync.writeFileSync(outPath, `${JSON.stringify(params.payload, null, 2)}\n`, "utf8");
  return outPath;
};
