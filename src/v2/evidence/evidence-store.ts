import { createHash } from "node:crypto";

export type ReliabilityTier = 1 | 2 | 3 | 4;

export type EvidenceItem = {
  id: `S${number}`;
  title: string;
  publisher: string;
  date_published: string; // YYYY-MM-DD (validated elsewhere)
  accessed_at: string; // ISO timestamp (validated elsewhere)
  url: string;
  reliability_tier: ReliabilityTier;
  excerpt_or_key_points: string[];
  raw_text_ref?: string;
  tags: string[];
};

export type EvidenceInsert = Omit<EvidenceItem, "id" | "reliability_tier"> & {
  reliability_tier?: ReliabilityTier;
};

const stripTrackingParams = (url: URL): void => {
  const params = url.searchParams;
  // Common tracking params.
  const dropPrefixes = ["utm_", "ref_", "fbclid", "gclid", "mc_cid", "mc_eid"];
  const toDelete: string[] = [];
  for (const key of params.keys()) {
    const lowered = key.toLowerCase();
    if (dropPrefixes.some((prefix) => lowered.startsWith(prefix))) {
      toDelete.push(key);
    }
  }
  toDelete.forEach((key) => params.delete(key));
};

export const canonicalizeUrl = (raw: string): string => {
  const url = new URL(raw);
  const protocol = url.protocol.toLowerCase();
  const hostname = url.hostname.replace(/^www\./i, "").toLowerCase();
  const port =
    (protocol === "https:" && url.port === "443") || (protocol === "http:" && url.port === "80")
      ? ""
      : url.port;
  const pathname = url.pathname.replace(/\/+$/, "") || "/";
  stripTrackingParams(url);
  // Sort query params for stable dedupe.
  const sorted = new URLSearchParams();
  Array.from(url.searchParams.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .forEach(([k, v]) => sorted.append(k, v));
  const query = sorted.toString();
  return `${protocol}//${hostname}${port ? `:${port}` : ""}${pathname}${query ? `?${query}` : ""}`;
};

export const inferReliabilityTier = (url: string): ReliabilityTier => {
  try {
    const host = new URL(url).hostname.replace(/^www\./, "").toLowerCase();
    if (host === "sec.gov" || host.endsWith(".sec.gov")) return 1;
    if (
      host === "fred.stlouisfed.org" ||
      host.endsWith(".bls.gov") ||
      host.endsWith(".bea.gov") ||
      host.endsWith(".census.gov") ||
      host.endsWith(".treasury.gov")
    ) {
      return 2;
    }
    // Transcripts and reputable journalism are Tier 3 by default; this list is intentionally narrow.
    if (
      host.endsWith(".fool.com") ||
      host.endsWith(".seekingalpha.com") ||
      host.endsWith(".bloomberg.com") ||
      host.endsWith(".reuters.com") ||
      host.endsWith(".ft.com") ||
      host.endsWith(".wsj.com")
    ) {
      return 3;
    }
  } catch {
    // ignore
  }
  return 4;
};

const stableIdForUrl = (url: string): string =>
  createHash("sha256").update(url).digest("hex").slice(0, 16);

export class EvidenceStore {
  private nextId = 1;
  private byCanonicalUrl = new Map<string, EvidenceItem>();
  private byStableKey = new Map<string, EvidenceItem>();
  private items: EvidenceItem[] = [];

  add(insert: EvidenceInsert): EvidenceItem {
    const canonicalUrl = canonicalizeUrl(insert.url);
    const stableKey = stableIdForUrl(canonicalUrl);

    const existing = this.byCanonicalUrl.get(canonicalUrl) ?? this.byStableKey.get(stableKey);
    if (existing) {
      // Merge tags + key points conservatively; keep original id.
      const merged: EvidenceItem = {
        ...existing,
        excerpt_or_key_points: Array.from(
          new Set([
            ...(existing.excerpt_or_key_points ?? []),
            ...(insert.excerpt_or_key_points ?? []),
          ]),
        ).filter(Boolean),
        tags: Array.from(new Set([...(existing.tags ?? []), ...(insert.tags ?? [])])).filter(
          Boolean,
        ),
        // Prefer having a raw text ref if we didn't have one.
        raw_text_ref: existing.raw_text_ref ?? insert.raw_text_ref,
      };
      this.byCanonicalUrl.set(canonicalUrl, merged);
      this.byStableKey.set(stableKey, merged);
      const idx = this.items.findIndex((item) => item.id === existing.id);
      if (idx >= 0) this.items[idx] = merged;
      return merged;
    }

    const id = `S${this.nextId}` as const;
    this.nextId += 1;
    const item: EvidenceItem = {
      id,
      title: insert.title,
      publisher: insert.publisher,
      date_published: insert.date_published,
      accessed_at: insert.accessed_at,
      url: canonicalUrl,
      reliability_tier: insert.reliability_tier ?? inferReliabilityTier(canonicalUrl),
      excerpt_or_key_points: insert.excerpt_or_key_points,
      raw_text_ref: insert.raw_text_ref,
      tags: insert.tags,
    };
    this.byCanonicalUrl.set(canonicalUrl, item);
    this.byStableKey.set(stableKey, item);
    this.items.push(item);
    return item;
  }

  all(): EvidenceItem[] {
    return [...this.items];
  }
}
