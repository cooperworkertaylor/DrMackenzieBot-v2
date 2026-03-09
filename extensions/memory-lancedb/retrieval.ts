import type { Table } from "@lancedb/lancedb";
import { rerankers } from "@lancedb/lancedb";
import { planQuery } from "./query-planner.js";
import type {
  CitationFirstBundle,
  CitationRecord,
  RetrievedItem,
  RetrievedSnippet,
  RetrievalBudget,
  RetrievalFilters,
} from "./types.js";

export type EmbeddingClient = {
  embed(text: string): Promise<number[]>;
};

type RankedRow = {
  id: string;
  rank: number;
  row: Record<string, unknown>;
};

type TableSearchOptions = {
  table: Table;
  tableKind: "fact_card" | "raw_chunk";
  textColumn: string;
  rewrites: string[];
  embeddings: EmbeddingClient;
  filters?: RetrievalFilters;
  budget: RetrievalBudget;
  vectorLimit: number;
  ftsLimit: number;
  rrfK: number;
};

export type HybridSearchOptions = {
  query: string;
  table: Table;
  embeddings: EmbeddingClient;
  filters?: RetrievalFilters;
  budget: RetrievalBudget;
  vectorLimit?: number;
  ftsLimit?: number;
  rrfK?: number;
  rewriteCount?: number;
};

export type MultiTableHybridSearchOptions = {
  query: string;
  rawChunksTable: Table;
  factCardsTable: Table;
  embeddings: EmbeddingClient;
  filters?: RetrievalFilters;
  budget: RetrievalBudget;
  vectorLimit?: number;
  ftsLimit?: number;
  rrfK?: number;
  rewriteCount?: number;
};

function tokenize(text: string): string[] {
  return text.trim().split(/\s+/).filter(Boolean);
}

function truncateToApproxTokens(text: string, maxTokens: number): string {
  if (maxTokens <= 0) {
    return "";
  }
  const words = tokenize(text);
  if (words.length <= maxTokens) {
    return words.join(" ");
  }
  return `${words.slice(0, maxTokens).join(" ")} …`;
}

function normalizeForDuplicateCheck(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildBigrams(text: string): Set<string> {
  const out = new Set<string>();
  for (let i = 0; i < text.length - 1; i++) {
    out.add(text.slice(i, i + 2));
  }
  return out;
}

function nearDuplicate(a: string, b: string, threshold = 0.9): boolean {
  const na = normalizeForDuplicateCheck(a);
  const nb = normalizeForDuplicateCheck(b);
  if (!na || !nb) {
    return false;
  }
  if (na === nb) {
    return true;
  }
  if (na.length > 40 && (na.includes(nb) || nb.includes(na))) {
    return true;
  }
  const ba = buildBigrams(na);
  const bb = buildBigrams(nb);
  if (ba.size === 0 || bb.size === 0) {
    return false;
  }
  let intersect = 0;
  for (const gram of ba) {
    if (bb.has(gram)) {
      intersect++;
    }
  }
  const union = ba.size + bb.size - intersect;
  return union > 0 ? intersect / union >= threshold : false;
}

function quote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function buildFilterSql(filters?: RetrievalFilters): string | null {
  if (!filters) {
    return null;
  }
  const clauses: string[] = [];
  if (filters.tickers && filters.tickers.length > 0) {
    const tickers = filters.tickers.map((value) => quote(value.toUpperCase()));
    clauses.push(`ticker IN (${tickers.join(",")})`);
  }
  if (filters.docTypes && filters.docTypes.length > 0) {
    const docTypes = filters.docTypes.map((value) => quote(value.toLowerCase()));
    clauses.push(`doc_type IN (${docTypes.join(",")})`);
  }
  if (filters.sourceUrls && filters.sourceUrls.length > 0) {
    const sourceUrls = filters.sourceUrls.map((value) => quote(value));
    clauses.push(`source_url IN (${sourceUrls.join(",")})`);
  }
  if (typeof filters.publishedAtMin === "number") {
    clauses.push(`published_at >= ${Math.floor(filters.publishedAtMin)}`);
  }
  if (typeof filters.publishedAtMax === "number") {
    clauses.push(`published_at <= ${Math.floor(filters.publishedAtMax)}`);
  }
  return clauses.length > 0 ? clauses.join(" AND ") : null;
}

function shouldUsePrefilter(filters?: RetrievalFilters): boolean {
  if (!filters) {
    return false;
  }
  let strictClauses = 0;
  if (filters.tickers?.length) {
    strictClauses++;
  }
  if (filters.docTypes?.length) {
    strictClauses++;
  }
  if (filters.sourceUrls?.length) {
    strictClauses++;
  }
  if (typeof filters.publishedAtMin === "number" || typeof filters.publishedAtMax === "number") {
    strictClauses++;
  }
  return strictClauses >= 2;
}

function rowMatchesFilters(row: Record<string, unknown>, filters?: RetrievalFilters): boolean {
  if (!filters) {
    return true;
  }
  if (filters.tickers && filters.tickers.length > 0) {
    const ticker = String(row.ticker ?? "").toUpperCase();
    if (!filters.tickers.some((value) => value.toUpperCase() === ticker)) {
      return false;
    }
  }
  if (filters.docTypes && filters.docTypes.length > 0) {
    const docType = String(row.doc_type ?? "").toLowerCase();
    if (!filters.docTypes.some((value) => value.toLowerCase() === docType)) {
      return false;
    }
  }
  if (filters.sourceUrls && filters.sourceUrls.length > 0) {
    const sourceUrl = String(row.source_url ?? "");
    if (!filters.sourceUrls.includes(sourceUrl)) {
      return false;
    }
  }
  const publishedAt = Number(row.published_at ?? 0);
  if (typeof filters.publishedAtMin === "number" && publishedAt < filters.publishedAtMin) {
    return false;
  }
  if (typeof filters.publishedAtMax === "number" && publishedAt > filters.publishedAtMax) {
    return false;
  }
  return true;
}

function pickRowId(row: Record<string, unknown>): string {
  const idFields = ["card_id", "chunk_id", "_rowid", "id"];
  for (const field of idFields) {
    const value = row[field];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
    if (typeof value === "number" && Number.isFinite(value)) {
      return `${field}:${value}`;
    }
  }
  return `row:${Math.random().toString(36).slice(2)}`;
}

function rankRows(
  rows: Record<string, unknown>[],
  scoreField?: "_distance" | "_score",
  desc = false,
): RankedRow[] {
  const sorted =
    scoreField == null
      ? [...rows]
      : [...rows].sort((a, b) => {
          const av = Number(a[scoreField] ?? 0);
          const bv = Number(b[scoreField] ?? 0);
          if (!Number.isFinite(av) && !Number.isFinite(bv)) {
            return 0;
          }
          if (!Number.isFinite(av)) {
            return 1;
          }
          if (!Number.isFinite(bv)) {
            return -1;
          }
          return desc ? bv - av : av - bv;
        });
  return sorted.map((row, idx) => ({
    id: pickRowId(row),
    rank: idx + 1,
    row,
  }));
}

function applyRrf(rankedGroups: RankedRow[][], rrfK: number): RankedRow[] {
  const acc = new Map<string, { score: number; row: Record<string, unknown> }>();
  for (const ranked of rankedGroups) {
    for (const item of ranked) {
      const current = acc.get(item.id) ?? { score: 0, row: item.row };
      current.score += 1 / (rrfK + item.rank);
      current.row = item.row;
      acc.set(item.id, current);
    }
  }
  return Array.from(acc.entries())
    .map(([id, value]) => ({ id, rank: 0, row: { ...value.row, _rrf: value.score } }))
    .sort((a, b) => Number(b.row._rrf ?? 0) - Number(a.row._rrf ?? 0))
    .map((row, idx) => ({ ...row, rank: idx + 1 }));
}

function applyRrfToItems(groups: RetrievedItem[][], rrfK: number): RetrievedItem[] {
  const acc = new Map<string, { score: number; item: RetrievedItem }>();
  for (const group of groups) {
    for (let i = 0; i < group.length; i++) {
      const item = group[i];
      const rank = i + 1;
      const current = acc.get(item.id) ?? { score: 0, item };
      current.score += 1 / (rrfK + rank);
      current.item = item;
      acc.set(item.id, current);
    }
  }
  return Array.from(acc.values())
    .sort((a, b) => b.score - a.score)
    .map((entry) => ({ ...entry.item, score: Math.max(entry.item.score, entry.score) }));
}

function parseCitations(row: Record<string, unknown>, fallback: CitationRecord): CitationRecord[] {
  const citationsRaw = row.citations;
  if (Array.isArray(citationsRaw)) {
    return citationsRaw
      .map((entry) => {
        if (!entry || typeof entry !== "object") {
          return null;
        }
        const candidate = entry as Record<string, unknown>;
        return {
          doc_id: String(candidate.doc_id ?? fallback.doc_id),
          chunk_id: String(candidate.chunk_id ?? fallback.chunk_id),
          page: typeof candidate.page === "number" ? candidate.page : fallback.page,
          section: typeof candidate.section === "string" ? candidate.section : fallback.section,
          url: String(candidate.url ?? fallback.url),
          published_at:
            typeof candidate.published_at === "number"
              ? candidate.published_at
              : fallback.published_at,
        } satisfies CitationRecord;
      })
      .filter((value): value is CitationRecord => Boolean(value));
  }
  return [fallback];
}

function rowToItem(row: Record<string, unknown>, type: "fact_card" | "raw_chunk"): RetrievedItem {
  const chunkId = String(row.chunk_id ?? "");
  const cardId = typeof row.card_id === "string" ? row.card_id : undefined;
  const sourceUrl = String(row.source_url ?? "");
  const sourceTitle = String(row.source_title ?? sourceUrl);
  const section = String(row.section ?? "");
  const page = Number(row.page ?? 0);
  const publishedAt = Number(row.published_at ?? 0);
  const fallbackCitation: CitationRecord = {
    doc_id: String(row.doc_id ?? ""),
    chunk_id: chunkId,
    page,
    section,
    url: sourceUrl,
    published_at: publishedAt,
  };
  const citations = parseCitations(row, fallbackCitation);
  const snippet =
    type === "fact_card"
      ? `${String(row.claim ?? "").trim()}\n${String(row.evidence_text ?? "").trim()}`.trim()
      : String(row.text ?? "").trim();

  return {
    id: cardId ?? chunkId ?? pickRowId(row),
    type,
    snippet,
    score: Number(row._rrf ?? row._score ?? row._distance ?? 0),
    metadata: {
      doc_id: String(row.doc_id ?? ""),
      chunk_id: chunkId,
      card_id: cardId,
      source_url: sourceUrl,
      source_title: sourceTitle,
      company: String(row.company ?? ""),
      ticker: String(row.ticker ?? ""),
      doc_type: String(row.doc_type ?? ""),
      section,
      page,
      published_at: publishedAt,
      text_norm_hash: String(row.text_norm_hash ?? row.chunk_hash ?? ""),
      confidence:
        typeof row.confidence === "string" && ["high", "med", "low"].includes(row.confidence)
          ? (row.confidence as "high" | "med" | "low")
          : undefined,
    },
    citations,
  };
}

function enforceBudgetAndDedup(items: RetrievedItem[], budget: RetrievalBudget): RetrievedItem[] {
  const dedupHash = new Set<string>();
  const chosen: RetrievedItem[] = [];
  let tokenBudget = Math.max(120, budget.maxTotalTokens);

  for (const candidate of items) {
    const sourceHash = `${candidate.metadata.source_url}::${candidate.metadata.text_norm_hash}`;
    if (sourceHash !== "::" && dedupHash.has(sourceHash)) {
      continue;
    }
    if (chosen.some((existing) => nearDuplicate(existing.snippet, candidate.snippet))) {
      continue;
    }

    const snippetWords = tokenize(candidate.snippet).length;
    if (tokenBudget <= 0) {
      break;
    }
    const maxSnippetTokens = Math.min(budget.maxTokensPerSnippet, tokenBudget);
    const snippet = truncateToApproxTokens(candidate.snippet, maxSnippetTokens);
    const used = Math.max(1, tokenize(snippet).length);

    chosen.push({ ...candidate, snippet });
    if (sourceHash !== "::") {
      dedupHash.add(sourceHash);
    }
    tokenBudget -= used;

    if (chosen.length >= budget.maxResults) {
      break;
    }
  }

  return chosen;
}

async function runHybridQueryForRewrite(
  table: Table,
  textColumn: string,
  rewrite: string,
  embedding: number[],
  filterSql: string | null,
  prefilter: boolean,
  vectorLimit: number,
  ftsLimit: number,
  rrfK: number,
): Promise<Record<string, unknown>[]> {
  const vectorQueryBase = table.vectorSearch(embedding).column("embedding").withRowId().limit(vectorLimit);
  const ftsQueryBase = table.search(rewrite, "fts", textColumn).withRowId().limit(ftsLimit);

  let vectorQuery = vectorQueryBase;
  let ftsQuery = ftsQueryBase;
  if (filterSql) {
    if (prefilter) {
      vectorQuery = vectorQuery.where(filterSql);
      ftsQuery = ftsQuery.where(filterSql);
    } else {
      vectorQuery = vectorQuery.where(filterSql).postfilter();
    }
  }

  const nativeHybridPromise = (async () => {
    try {
      const reranker = await rerankers.RRFReranker.create(rrfK);
      let hybrid = table
        .vectorSearch(embedding)
        .column("embedding")
        .fullTextSearch(rewrite, { columns: textColumn })
        .rerank(reranker)
        .withRowId()
        .limit(Math.max(vectorLimit, ftsLimit));
      if (filterSql) {
        hybrid = prefilter ? hybrid.where(filterSql) : hybrid.where(filterSql).postfilter();
      }
      return (await hybrid.toArray()) as Record<string, unknown>[];
    } catch {
      return [];
    }
  })();

  const [vectorRows, ftsRows, hybridRows] = await Promise.all([
    vectorQuery.toArray() as Promise<Record<string, unknown>[]>,
    ftsQuery.toArray() as Promise<Record<string, unknown>[]>,
    nativeHybridPromise,
  ]);

  const rankedGroups: RankedRow[][] = [
    rankRows(vectorRows, "_distance", false),
    rankRows(ftsRows, "_score", true),
  ];
  if (hybridRows.length > 0) {
    rankedGroups.push(rankRows(hybridRows));
  }
  const fused = applyRrf(rankedGroups, rrfK);
  return fused.map((row) => row.row);
}

async function searchTable(options: TableSearchOptions): Promise<RetrievedItem[]> {
  const {
    table,
    tableKind,
    textColumn,
    rewrites,
    embeddings,
    filters,
    budget,
    vectorLimit,
    ftsLimit,
    rrfK,
  } = options;
  if (rewrites.length === 0) {
    return [];
  }

  const filterSql = buildFilterSql(filters);
  const prefilter = shouldUsePrefilter(filters) && Boolean(filterSql);
  const rankedGroups: RankedRow[][] = [];

  await Promise.all(
    rewrites.map(async (rewrite) => {
      const embedding = await embeddings.embed(rewrite);
      const rows = await runHybridQueryForRewrite(
        table,
        textColumn,
        rewrite,
        embedding,
        filterSql,
        prefilter,
        vectorLimit,
        ftsLimit,
        rrfK,
      );
      const filteredRows = prefilter ? rows : rows.filter((row) => rowMatchesFilters(row, filters));
      rankedGroups.push(rankRows(filteredRows));
    }),
  );

  const fused = applyRrf(rankedGroups, rrfK);
  const items = fused.map((row) => rowToItem(row.row, tableKind));
  return enforceBudgetAndDedup(items, {
    maxResults: Math.max(budget.maxResults * 2, budget.maxResults),
    maxTokensPerSnippet: budget.maxTokensPerSnippet,
    maxTotalTokens: Math.max(budget.maxTotalTokens, budget.maxResults * budget.maxTokensPerSnippet),
  });
}

export async function retrieveHybridAcrossTables(
  options: MultiTableHybridSearchOptions,
): Promise<RetrievedItem[]> {
  const {
    query,
    rawChunksTable,
    factCardsTable,
    embeddings,
    filters,
    budget,
    vectorLimit = 40,
    ftsLimit = 40,
    rrfK = 60,
    rewriteCount = 5,
  } = options;

  const plan = planQuery(query, filters);
  const rewrites = plan.rewrites.slice(0, Math.max(3, Math.min(5, rewriteCount)));
  const strict = plan.strictFilters;
  const loose = plan.looseFilters;

  const [factCards, rawChunks] = await Promise.all([
    searchTable({
      table: factCardsTable,
      tableKind: "fact_card",
      textColumn: "search_text",
      rewrites,
      embeddings,
      filters: strict,
      budget,
      vectorLimit,
      ftsLimit,
      rrfK,
    }),
    searchTable({
      table: rawChunksTable,
      tableKind: "raw_chunk",
      textColumn: "text",
      rewrites,
      embeddings,
      filters: loose,
      budget,
      vectorLimit,
      ftsLimit,
      rrfK,
    }),
  ]);
  const merged = applyRrfToItems([factCards, rawChunks], rrfK);
  return enforceBudgetAndDedup(merged, budget);
}

export async function hybridSearch(options: HybridSearchOptions): Promise<RetrievedSnippet[]> {
  const {
    query,
    table,
    embeddings,
    filters,
    budget,
    vectorLimit = 40,
    ftsLimit = 40,
    rrfK = 60,
    rewriteCount = 5,
  } = options;
  const plan = planQuery(query, filters);
  const rewrites = plan.rewrites.slice(0, Math.max(3, Math.min(5, rewriteCount)));

  const items = await searchTable({
    table,
    tableKind: "raw_chunk",
    textColumn: "text",
    rewrites,
    embeddings,
    filters: plan.strictFilters,
    budget,
    vectorLimit,
    ftsLimit,
    rrfK,
  });

  return items.map((item) => {
    const firstCitation = item.citations[0];
    const chunkId = item.metadata.chunk_id;
    const page = firstCitation?.page ?? item.metadata.page;
    const sourceUrl = firstCitation?.url ?? item.metadata.source_url;
    return {
      chunkId,
      text: item.snippet,
      score: item.score,
      sourceUrl,
      company: item.metadata.company,
      ticker: item.metadata.ticker,
      docType: item.metadata.doc_type,
      section: firstCitation?.section ?? item.metadata.section,
      page,
      publishedAt: item.metadata.published_at,
      chunkHash: item.metadata.text_norm_hash,
      type: item.type,
      citation: {
        key: `${sourceUrl}#${chunkId}:p${page}`,
        chunkId,
        sourceUrl,
        section: firstCitation?.section,
        page,
        charStart: 0,
        charEnd: 0,
        docId: item.metadata.doc_id,
        publishedAt: item.metadata.published_at,
      },
    } satisfies RetrievedSnippet;
  });
}

export function buildCitationFirstBundle(
  question: string,
  retrievedItems: RetrievedItem[],
): CitationFirstBundle {
  return {
    question,
    retrieved_items: retrievedItems,
  };
}

export function validateCitationFirstAnswer(answer: string, bundle: CitationFirstBundle): {
  ok: boolean;
  missingParagraphIndexes: number[];
} {
  const citationTokens = new Set<string>();
  for (const item of bundle.retrieved_items) {
    for (const citation of item.citations) {
      citationTokens.add(citation.chunk_id);
      citationTokens.add(citation.doc_id);
    }
  }
  const paragraphs = answer
    .split(/\n{2,}/)
    .map((line) => line.trim())
    .filter(Boolean);
  const missingParagraphIndexes: number[] = [];
  paragraphs.forEach((paragraph, idx) => {
    const hasCitation = Array.from(citationTokens).some((token) => paragraph.includes(token));
    if (!hasCitation) {
      missingParagraphIndexes.push(idx);
    }
  });
  return { ok: missingParagraphIndexes.length === 0, missingParagraphIndexes };
}
