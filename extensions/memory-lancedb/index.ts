/**
 * OpenClaw Memory (LanceDB) Plugin
 *
 * Long-term memory with vector search for AI conversations.
 * Uses LanceDB for storage and OpenAI for embeddings.
 * Provides seamless auto-recall and auto-capture via lifecycle hooks.
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import * as lancedb from "@lancedb/lancedb";
import { Index } from "@lancedb/lancedb";
import { Type } from "@sinclair/typebox";
import { randomUUID } from "node:crypto";
import OpenAI from "openai";
import { stringEnum } from "openclaw/plugin-sdk";
import {
  MEMORY_CATEGORIES,
  type MemoryCategory,
  memoryConfigSchema,
  vectorDimsForModel,
} from "./config.js";
import {
  chunkDocument,
  extractPublishedAt,
  hashNormalizedText,
  normalizeText,
  type ChunkSeed,
} from "./ingestion.js";
import { hybridSearch, type EmbeddingClient } from "./retrieval.js";
import type { MemoryChunkRow, RetrievedSnippet, RetrievalFilters } from "./types.js";

// ============================================================================
// Types
// ============================================================================

type MemoryEntry = {
  id: string;
  text: string;
  importance: number;
  category: MemoryCategory;
  createdAt: number;
  citationKey: string;
  sourceUrl: string;
  page: number;
  charStart: number;
  charEnd: number;
  ticker: string;
  company: string;
  docType: string;
  publishedAt: number;
  chunkHash: string;
};

type MemorySearchResult = {
  entry: MemoryEntry;
  score: number;
};

// ============================================================================
// LanceDB Provider
// ============================================================================

const TABLE_NAME = "memories_v2";
const LEGACY_TABLE_NAME = "memories";

function sqlQuote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

class MemoryDB {
  private db: lancedb.Connection | null = null;
  private table: lancedb.Table | null = null;
  private initPromise: Promise<void> | null = null;

  constructor(
    private readonly dbPath: string,
    private readonly vectorDim: number,
  ) {}

  private async ensureInitialized(): Promise<void> {
    if (this.table) {
      return;
    }
    if (this.initPromise) {
      return this.initPromise;
    }

    this.initPromise = this.doInitialize();
    return this.initPromise;
  }

  private async doInitialize(): Promise<void> {
    this.db = await lancedb.connect(this.dbPath);
    const tables = await this.db.tableNames();

    if (tables.includes(TABLE_NAME)) {
      this.table = await this.db.openTable(TABLE_NAME);
    } else {
      this.table = await this.db.createTable(TABLE_NAME, [
        {
          chunk_id: "__schema__",
          doc_id: "__schema__",
          text: "",
          text_norm: "",
          vector: Array.from({ length: this.vectorDim }).fill(0),
          importance: 0,
          category: "other",
          company: "",
          ticker: "",
          doc_type: "other",
          published_at: 0,
          ingested_at: 0,
          source_url: "",
          section: "",
          page: 0,
          chunk_index: 0,
          chunk_hash: "",
          char_start: 0,
          char_end: 0,
          token_count: 0,
          citation_key: "",
        },
      ]);
      await this.table.delete('chunk_id = "__schema__"');
      await this.backfillLegacyTableIfPresent(tables);
    }

    await this.ensureIndices();
  }

  private async backfillLegacyTableIfPresent(existingTables: string[]): Promise<void> {
    if (!this.db || !this.table || !existingTables.includes(LEGACY_TABLE_NAME)) {
      return;
    }

    const legacy = await this.db.openTable(LEGACY_TABLE_NAME);
    const legacyRowsRaw = await legacy.query().limit(200_000).toArray();
    const legacyRows = legacyRowsRaw as Array<Record<string, unknown>>;
    if (legacyRows.length === 0) {
      return;
    }

    const mapped: MemoryChunkRow[] = legacyRows
      .filter((row) => typeof row.id === "string" && String(row.id) !== "__schema__")
      .map((row, index) => {
        const text = String(row.text ?? "");
        const createdAt = Number(row.createdAt ?? Date.now());
        const chunkHash = hashNormalizedText(text);
        const sourceUrl = `openclaw://memory/${String(row.id ?? index)}`;
        return {
          chunk_id: String(row.id),
          doc_id: `legacy:${String(row.id)}`,
          text,
          text_norm: normalizeText(text),
          vector: (row.vector as number[]) ?? [],
          importance: Number(row.importance ?? 0.7),
          category: String(row.category ?? "other"),
          company: "",
          ticker: "",
          doc_type: "other",
          published_at: createdAt,
          ingested_at: Date.now(),
          source_url: sourceUrl,
          section: "",
          page: 1,
          chunk_index: index,
          chunk_hash: chunkHash,
          char_start: 0,
          char_end: text.length,
          token_count: Math.max(1, text.split(/\s+/).filter(Boolean).length),
          citation_key: `${sourceUrl}#${chunkHash.slice(0, 12)}:p1:c0-${text.length}`,
        };
      });

    if (mapped.length > 0) {
      await this.table.add(mapped);
    }
  }

  private async ensureIndices(): Promise<void> {
    if (!this.table) {
      return;
    }

    const specs: Array<{ column: string; options?: Partial<lancedb.IndexOptions> }> = [
      {
        column: "vector",
        options: {
          config: Index.ivfFlat({ distanceType: "cosine", numPartitions: 64 }),
          replace: false,
          name: "idx_memories_v2_vector",
        },
      },
      {
        column: "text",
        options: {
          config: Index.fts({
            baseTokenizer: "simple",
            lowercase: true,
            removeStopWords: false,
            withPosition: true,
          }),
          replace: false,
          name: "idx_memories_v2_text_fts",
        },
      },
      { column: "ticker", options: { config: Index.btree(), replace: false } },
      { column: "doc_type", options: { config: Index.bitmap(), replace: false } },
      { column: "published_at", options: { config: Index.btree(), replace: false } },
      { column: "source_url", options: { config: Index.btree(), replace: false } },
      { column: "chunk_hash", options: { config: Index.btree(), replace: false } },
    ];

    for (const spec of specs) {
      try {
        await this.table.createIndex(spec.column, spec.options);
      } catch {
        // best-effort index creation; table remains queryable without index
      }
    }
  }

  async store(entry: {
    text: string;
    vector: number[];
    importance: number;
    category: MemoryCategory;
    company?: string;
    ticker?: string;
    docType?: string;
    sourceUrl?: string;
    section?: string;
    page?: number;
    publishedAt?: number;
  }): Promise<MemoryEntry> {
    await this.ensureInitialized();

    const now = Date.now();
    const text = entry.text.trim();
    const sourceUrl = entry.sourceUrl?.trim() || "local://memory/manual";
    const page = Number.isFinite(entry.page) ? Math.max(1, Number(entry.page)) : 1;
    const chunkHash = hashNormalizedText(text);
    const chunkId = randomUUID();
    const publishedAt =
      typeof entry.publishedAt === "number" && Number.isFinite(entry.publishedAt)
        ? Math.floor(entry.publishedAt)
        : now;

    const row: MemoryChunkRow = {
      chunk_id: chunkId,
      doc_id: `doc:${chunkId}`,
      text,
      text_norm: normalizeText(text),
      vector: entry.vector,
      importance: entry.importance,
      category: entry.category,
      company: entry.company?.trim() ?? "",
      ticker: entry.ticker?.trim().toUpperCase() ?? "",
      doc_type: (entry.docType?.trim().toLowerCase() as MemoryChunkRow["doc_type"]) || "other",
      published_at: publishedAt,
      ingested_at: now,
      source_url: sourceUrl,
      section: entry.section?.trim() ?? "",
      page,
      chunk_index: 0,
      chunk_hash: chunkHash,
      char_start: 0,
      char_end: text.length,
      token_count: Math.max(1, text.split(/\s+/).filter(Boolean).length),
      citation_key: `${sourceUrl}#${chunkHash.slice(0, 12)}:p${page}:c0-${text.length}`,
    };

    await this.table!.add([row]);
    return {
      id: chunkId,
      text: row.text,
      importance: row.importance,
      category: row.category as MemoryCategory,
      createdAt: row.ingested_at,
      citationKey: row.citation_key,
      sourceUrl: row.source_url,
      page: row.page,
      charStart: row.char_start,
      charEnd: row.char_end,
      ticker: row.ticker,
      company: row.company,
      docType: row.doc_type,
      publishedAt: row.published_at,
      chunkHash: row.chunk_hash,
    };
  }

  async ingestDocument(params: {
    text: string;
    sourceUrl: string;
    section?: string;
    company?: string;
    ticker?: string;
    docType?: MemoryChunkRow["doc_type"];
    publishedAt?: number;
    category?: MemoryCategory;
    importance?: number;
    chunking: {
      targetTokens: number;
      minTokens: number;
      maxTokens: number;
      overlapRatio: number;
    };
    embeddings: EmbeddingClient;
  }): Promise<{ ingested: number; deduped: number; docId: string }> {
    await this.ensureInitialized();
    if (!this.table) {
      throw new Error("memory table not initialized");
    }

    const publishedAt =
      typeof params.publishedAt === "number" && Number.isFinite(params.publishedAt)
        ? Math.floor(params.publishedAt)
        : extractPublishedAt(params.text) ?? Date.now();
    const docId = `doc:${randomUUID()}`;
    const base: ChunkSeed = {
      doc_id: docId,
      text: params.text,
      importance: params.importance ?? 0.7,
      category: params.category ?? "other",
      company: params.company?.trim() ?? "",
      ticker: params.ticker?.trim().toUpperCase() ?? "",
      doc_type: params.docType ?? "research",
      published_at: publishedAt,
      source_url: params.sourceUrl,
      section: params.section ?? "",
      page: 1,
      chunk_index: 0,
      char_start: 0,
    };

    const chunked = chunkDocument(base, params.chunking);
    if (chunked.length === 0) {
      return { ingested: 0, deduped: 0, docId };
    }

    const seen = new Set<string>();
    let ingested = 0;
    let deduped = 0;

    for (const chunk of chunked) {
      const dedupKey = `${chunk.source_url}::${chunk.chunk_hash}`;
      if (seen.has(dedupKey)) {
        deduped++;
        continue;
      }
      seen.add(dedupKey);
      const predicate = `source_url = ${sqlQuote(chunk.source_url)} AND chunk_hash = ${sqlQuote(chunk.chunk_hash)}`;
      const existing = await this.table.query().where(predicate).limit(1).toArray();
      if ((existing as unknown[]).length > 0) {
        deduped++;
        continue;
      }

      const vector = await params.embeddings.embed(chunk.text);
      chunk.vector = vector;
      chunk.chunk_id = randomUUID();
      chunk.ingested_at = Date.now();
      await this.table.add([chunk]);
      ingested++;
    }

    return { ingested, deduped, docId };
  }

  async search(params: {
    query: string;
    embeddings: EmbeddingClient;
    hybridEnabled?: boolean;
    budget: {
      maxResults: number;
      maxTokensPerSnippet: number;
    };
    vectorLimit: number;
    ftsLimit: number;
    rewriteCount: number;
    rrfK: number;
    filters?: RetrievalFilters;
  }): Promise<MemorySearchResult[]> {
    await this.ensureInitialized();
    if (!this.table) {
      return [];
    }

    let snippets: RetrievedSnippet[];
    if (params.hybridEnabled === false) {
      let query = this.table.vectorSearch(await params.embeddings.embed(params.query)).withRowId();
      if (params.filters?.tickers?.length) {
        const tickers = params.filters.tickers.map((value) => sqlQuote(value.toUpperCase()));
        query = query.where(`ticker IN (${tickers.join(",")})`);
      }
      if (params.filters?.docTypes?.length) {
        const docTypes = params.filters.docTypes.map((value) => sqlQuote(value.toLowerCase()));
        query = query.where(`doc_type IN (${docTypes.join(",")})`);
      }
      if (typeof params.filters?.publishedAtMin === "number") {
        query = query.where(`published_at >= ${Math.floor(params.filters.publishedAtMin)}`);
      }
      query = query.limit(Math.max(params.budget.maxResults, params.vectorLimit));
      const rowsRaw = await query.toArray();
      const rows = rowsRaw as Array<Record<string, unknown>>;
      snippets = rows.slice(0, params.budget.maxResults).map((row, idx) => {
        const text = String(row.text ?? "");
        const chunkId = String(row.chunk_id ?? `row:${idx}`);
        const sourceUrl = String(row.source_url ?? "");
        const page = Number(row.page ?? 0);
        const charStart = Number(row.char_start ?? 0);
        const charEnd = Number(row.char_end ?? 0);
        const citationKey =
          String(row.citation_key ?? "").trim() ||
          `${sourceUrl || "local://memory"}#${chunkId}:p${page}:c${charStart}-${charEnd}`;
        return {
          chunkId,
          text: truncateApproxTokens(text, params.budget.maxTokensPerSnippet),
          score: 1 / (1 + Number(row._distance ?? 0)),
          sourceUrl,
          company: String(row.company ?? ""),
          ticker: String(row.ticker ?? ""),
          docType: String(row.doc_type ?? ""),
          section: String(row.section ?? ""),
          page,
          publishedAt: Number(row.published_at ?? 0),
          chunkHash: String(row.chunk_hash ?? ""),
          citation: {
            key: citationKey,
            chunkId,
            sourceUrl,
            page,
            charStart,
            charEnd,
          },
        };
      });
    } else {
      snippets = await hybridSearch({
        query: params.query,
        table: this.table,
        embeddings: params.embeddings,
        budget: params.budget,
        filters: params.filters,
        vectorLimit: params.vectorLimit,
        ftsLimit: params.ftsLimit,
        rewriteCount: params.rewriteCount,
        rrfK: params.rrfK,
      });
    }

    return snippets.map((snippet) => ({
      entry: {
        id: snippet.chunkId,
        text: snippet.text,
        importance: 0.7,
        category: "other",
        createdAt: Date.now(),
        citationKey: snippet.citation.key,
        sourceUrl: snippet.sourceUrl,
        page: snippet.page,
        charStart: snippet.citation.charStart,
        charEnd: snippet.citation.charEnd,
        ticker: snippet.ticker,
        company: snippet.company,
        docType: snippet.docType,
        publishedAt: snippet.publishedAt,
        chunkHash: snippet.chunkHash,
      },
      score: snippet.score,
    }));
  }

  async searchSnippets(params: {
    query: string;
    embeddings: EmbeddingClient;
    budget: {
      maxResults: number;
      maxTokensPerSnippet: number;
    };
    vectorLimit: number;
    ftsLimit: number;
    rewriteCount: number;
    rrfK: number;
    filters?: RetrievalFilters;
  }): Promise<RetrievedSnippet[]> {
    await this.ensureInitialized();
    if (!this.table) {
      return [];
    }
    return hybridSearch({
      query: params.query,
      table: this.table,
      embeddings: params.embeddings,
      budget: params.budget,
      filters: params.filters,
      vectorLimit: params.vectorLimit,
      ftsLimit: params.ftsLimit,
      rewriteCount: params.rewriteCount,
      rrfK: params.rrfK,
    });
  }

  async delete(id: string): Promise<boolean> {
    await this.ensureInitialized();
    // Validate UUID format to prevent injection
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!uuidRegex.test(id)) {
      throw new Error(`Invalid memory ID format: ${id}`);
    }
    await this.table!.delete(`chunk_id = ${sqlQuote(id)}`);
    return true;
  }

  async count(): Promise<number> {
    await this.ensureInitialized();
    return this.table!.countRows();
  }
}

// ============================================================================
// OpenAI Embeddings
// ============================================================================

class Embeddings {
  private client: OpenAI;

  constructor(
    apiKey: string,
    private model: string,
  ) {
    this.client = new OpenAI({ apiKey });
  }

  async embed(text: string): Promise<number[]> {
    const response = await this.client.embeddings.create({
      model: this.model,
      input: text,
    });
    return response.data[0].embedding;
  }
}

// ============================================================================
// Rule-based capture filter
// ============================================================================

const MEMORY_TRIGGERS = [
  /zapamatuj si|pamatuj|remember/i,
  /preferuji|radši|nechci|prefer/i,
  /rozhodli jsme|budeme používat/i,
  /\+\d{10,}/,
  /[\w.-]+@[\w.-]+\.\w+/,
  /můj\s+\w+\s+je|je\s+můj/i,
  /my\s+\w+\s+is|is\s+my/i,
  /i (like|prefer|hate|love|want|need)/i,
  /always|never|important/i,
];

function shouldCapture(text: string): boolean {
  if (text.length < 10 || text.length > 500) {
    return false;
  }
  // Skip injected context from memory recall
  if (text.includes("<relevant-memories>")) {
    return false;
  }
  // Skip system-generated content
  if (text.startsWith("<") && text.includes("</")) {
    return false;
  }
  // Skip agent summary responses (contain markdown formatting)
  if (text.includes("**") && text.includes("\n-")) {
    return false;
  }
  // Skip emoji-heavy responses (likely agent output)
  const emojiCount = (text.match(/[\u{1F300}-\u{1F9FF}]/gu) || []).length;
  if (emojiCount > 3) {
    return false;
  }
  return MEMORY_TRIGGERS.some((r) => r.test(text));
}

function detectCategory(text: string): MemoryCategory {
  const lower = text.toLowerCase();
  if (/prefer|radši|like|love|hate|want/i.test(lower)) {
    return "preference";
  }
  if (/rozhodli|decided|will use|budeme/i.test(lower)) {
    return "decision";
  }
  if (/\+\d{10,}|@[\w.-]+\.\w+|is called|jmenuje se/i.test(lower)) {
    return "entity";
  }
  if (/is|are|has|have|je|má|jsou/i.test(lower)) {
    return "fact";
  }
  return "other";
}

function truncateApproxTokens(text: string, maxTokens: number): string {
  const words = text.split(/\s+/).filter(Boolean);
  if (words.length <= maxTokens) {
    return words.join(" ");
  }
  return `${words.slice(0, maxTokens).join(" ")} …`;
}

// ============================================================================
// Plugin Definition
// ============================================================================

const memoryPlugin = {
  id: "memory-lancedb",
  name: "Memory (LanceDB)",
  description: "LanceDB-backed long-term memory with auto-recall/capture",
  kind: "memory" as const,
  configSchema: memoryConfigSchema,

  register(api: OpenClawPluginApi) {
    const cfg = memoryConfigSchema.parse(api.pluginConfig);
    const resolvedDbPath = api.resolvePath(cfg.dbPath!);
    const vectorDim = vectorDimsForModel(cfg.embedding.model ?? "text-embedding-3-small");
    const db = new MemoryDB(resolvedDbPath, vectorDim);
    const embeddings = new Embeddings(cfg.embedding.apiKey, cfg.embedding.model!);
    const retrievalBudget = {
      maxResults: Math.max(1, cfg.retrieval?.maxResults ?? 8),
      maxTokensPerSnippet: Math.max(40, cfg.retrieval?.maxTokensPerSnippet ?? 220),
    };
    const retrievalTuning = {
      hybridEnabled: cfg.retrieval?.hybridEnabled !== false,
      vectorLimit: Math.max(retrievalBudget.maxResults, cfg.retrieval?.vectorLimit ?? 40),
      ftsLimit: Math.max(retrievalBudget.maxResults, cfg.retrieval?.ftsLimit ?? 40),
      rewriteCount: Math.max(3, cfg.retrieval?.rewriteCount ?? 5),
      rrfK: Math.max(10, cfg.retrieval?.rrfK ?? 60),
    };

    api.logger.info(`memory-lancedb: plugin registered (db: ${resolvedDbPath}, lazy init)`);

    // ========================================================================
    // Tools
    // ========================================================================

    api.registerTool(
      {
        name: "memory_recall",
        label: "Memory Recall",
        description:
          "Search through long-term memories. Use when you need context about user preferences, past decisions, or previously discussed topics.",
        parameters: Type.Object({
          query: Type.String({ description: "Search query" }),
          limit: Type.Optional(Type.Number({ description: "Max results (default: 5)" })),
          ticker: Type.Optional(Type.String({ description: "Ticker prefilter (e.g., NVDA)" })),
          docType: Type.Optional(
            Type.String({ description: "Document type prefilter (memo|filing|news|transcript)" }),
          ),
          publishedAfter: Type.Optional(
            Type.String({ description: "ISO date prefilter, e.g. 2025-01-01" }),
          ),
        }),
        async execute(_toolCallId, params) {
          const { query, limit = 5, ticker, docType, publishedAfter } = params as {
            query: string;
            limit?: number;
            ticker?: string;
            docType?: string;
            publishedAfter?: string;
          };
          const filters: RetrievalFilters = {};
          if (ticker?.trim()) {
            filters.tickers = [ticker.trim().toUpperCase()];
          }
          if (docType?.trim()) {
            filters.docTypes = [docType.trim().toLowerCase()];
          }
          if (publishedAfter?.trim()) {
            const ts = Date.parse(`${publishedAfter.trim()}T00:00:00Z`);
            if (Number.isFinite(ts)) {
              filters.publishedAtMin = ts;
            }
          }
          const results = await db.search({
            query,
            embeddings,
            hybridEnabled: retrievalTuning.hybridEnabled,
            budget: {
              maxResults: Math.min(Math.max(1, limit), retrievalBudget.maxResults),
              maxTokensPerSnippet: retrievalBudget.maxTokensPerSnippet,
            },
            vectorLimit: retrievalTuning.vectorLimit,
            ftsLimit: retrievalTuning.ftsLimit,
            rewriteCount: retrievalTuning.rewriteCount,
            rrfK: retrievalTuning.rrfK,
            filters: Object.keys(filters).length > 0 ? filters : undefined,
          });

          if (results.length === 0) {
            return {
              content: [{ type: "text", text: "No relevant memories found." }],
              details: { count: 0 },
            };
          }

          const text = results
            .map(
              (r, i) =>
                `${i + 1}. [${r.entry.category}] ${r.entry.text}\n   source=${r.entry.sourceUrl} citation=${r.entry.citationKey} score=${(r.score * 100).toFixed(1)}%`,
            )
            .join("\n");

          const sanitizedResults = results.map((r) => ({
            id: r.entry.id,
            text: r.entry.text,
            category: r.entry.category,
            importance: r.entry.importance,
            score: r.score,
            sourceUrl: r.entry.sourceUrl,
            ticker: r.entry.ticker,
            docType: r.entry.docType,
            citationKey: r.entry.citationKey,
            page: r.entry.page,
            charStart: r.entry.charStart,
            charEnd: r.entry.charEnd,
          }));

          return {
            content: [{ type: "text", text: `Found ${results.length} memories:\n\n${text}` }],
            details: { count: results.length, memories: sanitizedResults },
          };
        },
      },
      { name: "memory_recall" },
    );

    api.registerTool(
      {
        name: "memory_store",
        label: "Memory Store",
        description:
          "Save important information in long-term memory. Use for preferences, facts, decisions.",
        parameters: Type.Object({
          text: Type.String({ description: "Information to remember" }),
          importance: Type.Optional(Type.Number({ description: "Importance 0-1 (default: 0.7)" })),
          category: Type.Optional(stringEnum(MEMORY_CATEGORIES)),
          ticker: Type.Optional(Type.String({ description: "Ticker (e.g., NVDA)" })),
          company: Type.Optional(Type.String({ description: "Company name" })),
          docType: Type.Optional(
            Type.String({ description: "memo|filing|transcript|news|note|research|other" }),
          ),
          sourceUrl: Type.Optional(Type.String({ description: "Canonical source URL" })),
          publishedAt: Type.Optional(Type.String({ description: "YYYY-MM-DD" })),
        }),
        async execute(_toolCallId, params) {
          const {
            text,
            importance = 0.7,
            category = "other",
            ticker,
            company,
            docType,
            sourceUrl,
            publishedAt,
          } = params as {
            text: string;
            importance?: number;
            category?: MemoryEntry["category"];
            ticker?: string;
            company?: string;
            docType?: string;
            sourceUrl?: string;
            publishedAt?: string;
          };

          const vector = await embeddings.embed(text);

          const existing = await db.search({
            query: text,
            embeddings,
            hybridEnabled: retrievalTuning.hybridEnabled,
            budget: { maxResults: 1, maxTokensPerSnippet: 160 },
            vectorLimit: Math.max(4, retrievalTuning.vectorLimit / 2),
            ftsLimit: Math.max(4, retrievalTuning.ftsLimit / 2),
            rewriteCount: 3,
            rrfK: retrievalTuning.rrfK,
            filters: ticker ? { tickers: [ticker.toUpperCase()] } : undefined,
          });
          if (existing.length > 0) {
            return {
              content: [
                {
                  type: "text",
                  text: `Similar memory already exists: "${existing[0].entry.text}"`,
                },
              ],
              details: {
                action: "duplicate",
                existingId: existing[0].entry.id,
                existingText: existing[0].entry.text,
              },
            };
          }

          const entry = await db.store({
            text,
            vector,
            importance,
            category,
            ticker,
            company,
            docType,
            sourceUrl,
            publishedAt: publishedAt ? Date.parse(`${publishedAt}T00:00:00Z`) : undefined,
          });

          return {
            content: [{ type: "text", text: `Stored: "${text.slice(0, 100)}..."` }],
            details: {
              action: "created",
              id: entry.id,
              citationKey: entry.citationKey,
              sourceUrl: entry.sourceUrl,
            },
          };
        },
      },
      { name: "memory_store" },
    );

    api.registerTool(
      {
        name: "memory_ingest_document",
        label: "Memory Ingest Document",
        description:
          "Ingest long-form research text with chunking, metadata, deduplication, and citation pointers.",
        parameters: Type.Object({
          text: Type.String({ description: "Raw document text" }),
          sourceUrl: Type.String({ description: "Canonical source URL" }),
          ticker: Type.Optional(Type.String({ description: "Ticker metadata" })),
          company: Type.Optional(Type.String({ description: "Company metadata" })),
          docType: Type.Optional(
            Type.String({ description: "memo|filing|transcript|news|note|research|other" }),
          ),
          section: Type.Optional(Type.String({ description: "Section title" })),
          publishedAt: Type.Optional(Type.String({ description: "YYYY-MM-DD" })),
        }),
        async execute(_toolCallId, params) {
          const { text, sourceUrl, ticker, company, docType, section, publishedAt } = params as {
            text: string;
            sourceUrl: string;
            ticker?: string;
            company?: string;
            docType?: MemoryChunkRow["doc_type"];
            section?: string;
            publishedAt?: string;
          };
          const publishedAtTs = publishedAt ? Date.parse(`${publishedAt}T00:00:00Z`) : undefined;
          const summary = await db.ingestDocument({
            text,
            sourceUrl,
            ticker,
            company,
            docType: docType ?? "research",
            section,
            publishedAt: Number.isFinite(publishedAtTs) ? publishedAtTs : undefined,
            chunking: {
              targetTokens: cfg.chunking?.targetTokens ?? 700,
              minTokens: cfg.chunking?.minTokens ?? 500,
              maxTokens: cfg.chunking?.maxTokens ?? 900,
              overlapRatio: cfg.chunking?.overlapRatio ?? 0.12,
            },
            embeddings,
          });

          return {
            content: [
              {
                type: "text",
                text: `Ingested ${summary.ingested} chunks (${summary.deduped} deduped) for ${sourceUrl}.`,
              },
            ],
            details: summary,
          };
        },
      },
      { name: "memory_ingest_document" },
    );

    api.registerTool(
      {
        name: "memory_forget",
        label: "Memory Forget",
        description: "Delete specific memories. GDPR-compliant.",
        parameters: Type.Object({
          query: Type.Optional(Type.String({ description: "Search to find memory" })),
          memoryId: Type.Optional(Type.String({ description: "Specific memory ID" })),
        }),
        async execute(_toolCallId, params) {
          const { query, memoryId } = params as { query?: string; memoryId?: string };

          if (memoryId) {
            await db.delete(memoryId);
            return {
              content: [{ type: "text", text: `Memory ${memoryId} forgotten.` }],
              details: { action: "deleted", id: memoryId },
            };
          }

          if (query) {
            const results = await db.search({
              query,
              embeddings,
              hybridEnabled: retrievalTuning.hybridEnabled,
              budget: { maxResults: 5, maxTokensPerSnippet: 120 },
              vectorLimit: Math.max(10, retrievalTuning.vectorLimit / 2),
              ftsLimit: Math.max(10, retrievalTuning.ftsLimit / 2),
              rewriteCount: 3,
              rrfK: retrievalTuning.rrfK,
            });

            if (results.length === 0) {
              return {
                content: [{ type: "text", text: "No matching memories found." }],
                details: { found: 0 },
              };
            }

            if (results.length === 1 && results[0].score > 0.9) {
              await db.delete(results[0].entry.id);
              return {
                content: [{ type: "text", text: `Forgotten: "${results[0].entry.text}"` }],
                details: { action: "deleted", id: results[0].entry.id },
              };
            }

            const list = results
              .map((r) => `- [${r.entry.id.slice(0, 8)}] ${r.entry.text.slice(0, 60)}...`)
              .join("\n");

            // Strip vector data for serialization
            const sanitizedCandidates = results.map((r) => ({
              id: r.entry.id,
              text: r.entry.text,
              category: r.entry.category,
              score: r.score,
            }));

            return {
              content: [
                {
                  type: "text",
                  text: `Found ${results.length} candidates. Specify memoryId:\n${list}`,
                },
              ],
              details: { action: "candidates", candidates: sanitizedCandidates },
            };
          }

          return {
            content: [{ type: "text", text: "Provide query or memoryId." }],
            details: { error: "missing_param" },
          };
        },
      },
      { name: "memory_forget" },
    );

    // ========================================================================
    // CLI Commands
    // ========================================================================

    api.registerCli(
      ({ program }) => {
        const memory = program.command("ltm").description("LanceDB memory plugin commands");

        memory
          .command("list")
          .description("List memories")
          .action(async () => {
            const count = await db.count();
            console.log(`Total memories: ${count}`);
          });

        memory
          .command("search")
          .description("Hybrid search memories (BM25 + vector + RRF)")
          .argument("<query>", "Search query")
          .option("--limit <n>", "Max results", "5")
          .option("--ticker <symbol>", "Ticker prefilter")
          .option("--doc-type <type>", "Document type prefilter")
          .option("--published-after <iso>", "Published-at lower bound (YYYY-MM-DD)")
          .action(async (query, opts) => {
            const filters: RetrievalFilters = {};
            if (typeof opts.ticker === "string" && opts.ticker.trim()) {
              filters.tickers = [opts.ticker.trim().toUpperCase()];
            }
            if (typeof opts.docType === "string" && opts.docType.trim()) {
              filters.docTypes = [opts.docType.trim().toLowerCase()];
            }
            if (typeof opts.publishedAfter === "string" && opts.publishedAfter.trim()) {
              const ts = Date.parse(`${opts.publishedAfter.trim()}T00:00:00Z`);
              if (Number.isFinite(ts)) {
                filters.publishedAtMin = ts;
              }
            }
            const results = await db.search({
              query,
              embeddings,
              hybridEnabled: retrievalTuning.hybridEnabled,
              budget: {
                maxResults: Math.min(parseInt(opts.limit, 10) || 5, retrievalBudget.maxResults),
                maxTokensPerSnippet: retrievalBudget.maxTokensPerSnippet,
              },
              vectorLimit: retrievalTuning.vectorLimit,
              ftsLimit: retrievalTuning.ftsLimit,
              rewriteCount: retrievalTuning.rewriteCount,
              rrfK: retrievalTuning.rrfK,
              filters: Object.keys(filters).length > 0 ? filters : undefined,
            });
            const output = results.map((r) => ({
              id: r.entry.id,
              text: r.entry.text,
              category: r.entry.category,
              importance: r.entry.importance,
              score: r.score,
              sourceUrl: r.entry.sourceUrl,
              citationKey: r.entry.citationKey,
              ticker: r.entry.ticker,
              docType: r.entry.docType,
            }));
            console.log(JSON.stringify(output, null, 2));
          });

        memory
          .command("ingest")
          .description("Ingest a long-form document into chunked LanceDB memory records")
          .requiredOption("--file <path>", "Path to text/markdown file")
          .requiredOption("--source-url <url>", "Source URL")
          .option("--ticker <symbol>", "Ticker metadata")
          .option("--company <name>", "Company metadata")
          .option("--doc-type <type>", "memo|filing|transcript|news|note|research|other", "research")
          .option("--section <name>", "Section name", "")
          .option("--published-at <iso>", "Published date (YYYY-MM-DD)")
          .action(async (opts) => {
            const fs = await import("node:fs/promises");
            const text = await fs.readFile(String(opts.file), "utf8");
            const publishedAt = opts.publishedAt
              ? Date.parse(`${String(opts.publishedAt)}T00:00:00Z`)
              : undefined;
            const summary = await db.ingestDocument({
              text,
              sourceUrl: String(opts.sourceUrl),
              ticker: typeof opts.ticker === "string" ? opts.ticker : undefined,
              company: typeof opts.company === "string" ? opts.company : undefined,
              docType: String(opts.docType).toLowerCase() as MemoryChunkRow["doc_type"],
              section: String(opts.section ?? ""),
              publishedAt: Number.isFinite(publishedAt) ? publishedAt : undefined,
              chunking: {
                targetTokens: cfg.chunking?.targetTokens ?? 700,
                minTokens: cfg.chunking?.minTokens ?? 500,
                maxTokens: cfg.chunking?.maxTokens ?? 900,
                overlapRatio: cfg.chunking?.overlapRatio ?? 0.12,
              },
              embeddings,
            });
            console.log(JSON.stringify(summary, null, 2));
          });

        memory
          .command("stats")
          .description("Show memory statistics")
          .action(async () => {
            const count = await db.count();
            console.log(`Total memories: ${count}`);
          });
      },
      { commands: ["ltm"] },
    );

    // ========================================================================
    // Lifecycle Hooks
    // ========================================================================

    // Auto-recall: inject relevant memories before agent starts
    if (cfg.autoRecall) {
      api.on("before_agent_start", async (event) => {
        if (!event.prompt || event.prompt.length < 5) {
          return;
        }

        try {
          const results = await db.search({
            query: event.prompt,
            embeddings,
            hybridEnabled: retrievalTuning.hybridEnabled,
            budget: { maxResults: 3, maxTokensPerSnippet: retrievalBudget.maxTokensPerSnippet },
            vectorLimit: retrievalTuning.vectorLimit,
            ftsLimit: retrievalTuning.ftsLimit,
            rewriteCount: 3,
            rrfK: retrievalTuning.rrfK,
          });

          if (results.length === 0) {
            return;
          }

          const memoryContext = results
            .map(
              (r) =>
                `- [${r.entry.category}] ${r.entry.text}\n  citation=${r.entry.citationKey} source=${r.entry.sourceUrl}`,
            )
            .join("\n");

          api.logger.info?.(`memory-lancedb: injecting ${results.length} memories into context`);

          return {
            prependContext: `<relevant-memories>\nThe following memories may be relevant to this conversation:\n${memoryContext}\n</relevant-memories>`,
          };
        } catch (err) {
          api.logger.warn(`memory-lancedb: recall failed: ${String(err)}`);
        }
      });
    }

    // Auto-capture: analyze and store important information after agent ends
    if (cfg.autoCapture) {
      api.on("agent_end", async (event) => {
        if (!event.success || !event.messages || event.messages.length === 0) {
          return;
        }

        try {
          // Extract text content from messages (handling unknown[] type)
          const texts: string[] = [];
          for (const msg of event.messages) {
            // Type guard for message object
            if (!msg || typeof msg !== "object") {
              continue;
            }
            const msgObj = msg as Record<string, unknown>;

            // Only process user and assistant messages
            const role = msgObj.role;
            if (role !== "user" && role !== "assistant") {
              continue;
            }

            const content = msgObj.content;

            // Handle string content directly
            if (typeof content === "string") {
              texts.push(content);
              continue;
            }

            // Handle array content (content blocks)
            if (Array.isArray(content)) {
              for (const block of content) {
                if (
                  block &&
                  typeof block === "object" &&
                  "type" in block &&
                  (block as Record<string, unknown>).type === "text" &&
                  "text" in block &&
                  typeof (block as Record<string, unknown>).text === "string"
                ) {
                  texts.push((block as Record<string, unknown>).text as string);
                }
              }
            }
          }

          // Filter for capturable content
          const toCapture = texts.filter((text) => text && shouldCapture(text));
          if (toCapture.length === 0) {
            return;
          }

          // Store each capturable piece (limit to 3 per conversation)
          let stored = 0;
          for (const text of toCapture.slice(0, 3)) {
            const category = detectCategory(text);
            const vector = await embeddings.embed(text);

            // Check for duplicates (high similarity threshold)
            const existing = await db.search({
              query: text,
              embeddings,
              hybridEnabled: retrievalTuning.hybridEnabled,
              budget: { maxResults: 1, maxTokensPerSnippet: 120 },
              vectorLimit: Math.max(8, retrievalTuning.vectorLimit / 2),
              ftsLimit: Math.max(8, retrievalTuning.ftsLimit / 2),
              rewriteCount: 3,
              rrfK: retrievalTuning.rrfK,
            });
            if (existing.length > 0) {
              continue;
            }

            await db.store({
              text,
              vector,
              importance: 0.7,
              category,
            });
            stored++;
          }

          if (stored > 0) {
            api.logger.info(`memory-lancedb: auto-captured ${stored} memories`);
          }
        } catch (err) {
          api.logger.warn(`memory-lancedb: capture failed: ${String(err)}`);
        }
      });
    }

    // ========================================================================
    // Service
    // ========================================================================

    api.registerService({
      id: "memory-lancedb",
      start: () => {
        api.logger.info(
          `memory-lancedb: initialized (db: ${resolvedDbPath}, model: ${cfg.embedding.model})`,
        );
      },
      stop: () => {
        api.logger.info("memory-lancedb: stopped");
      },
    });
  },
};

export default memoryPlugin;
