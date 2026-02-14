import {
  Field,
  FixedSizeList,
  Float32,
  Float64,
  Int32,
  List,
  Schema,
  Struct,
  Utf8,
} from "apache-arrow";
import type { SchemaLike } from "@lancedb/lancedb";
import type { FactCardRow, RawChunkRow } from "./types.js";

export type ArrowFieldSpec = {
  name: string;
  type: string;
  nullable?: boolean;
  children?: ArrowFieldSpec[];
};

export type ArrowSchemaSpec = {
  name: string;
  fields: ArrowFieldSpec[];
};

export const RAW_CHUNKS_TABLE = "raw_chunks";
export const FACT_CARDS_TABLE = "fact_cards";
export const LEGACY_RAW_CHUNKS_TABLES = ["memories_v2", "memories"] as const;

export const rawChunksArrowSchemaSpec: ArrowSchemaSpec = {
  name: RAW_CHUNKS_TABLE,
  fields: [
    { name: "chunk_id", type: "utf8", nullable: false },
    { name: "doc_id", type: "utf8", nullable: false },
    { name: "text", type: "utf8", nullable: false },
    { name: "text_norm_hash", type: "utf8", nullable: false },
    { name: "source_url", type: "utf8", nullable: false },
    { name: "source_title", type: "utf8", nullable: false },
    { name: "doc_type", type: "utf8", nullable: false },
    { name: "company", type: "utf8", nullable: false },
    { name: "ticker", type: "utf8", nullable: false },
    { name: "published_at", type: "float64", nullable: false },
    { name: "retrieved_at", type: "float64", nullable: false },
    { name: "section", type: "utf8", nullable: false },
    { name: "page", type: "int32", nullable: false },
    { name: "char_start", type: "int32", nullable: false },
    { name: "char_end", type: "int32", nullable: false },
    { name: "embedding", type: "fixed_size_list<float32>", nullable: false },
    { name: "tags", type: "list<utf8>", nullable: false },
    { name: "chunk_hash", type: "utf8", nullable: false },
    { name: "text_norm", type: "utf8", nullable: false },
    { name: "citation_key", type: "utf8", nullable: false },
    { name: "search_text", type: "utf8", nullable: false },
    { name: "chunk_index", type: "int32", nullable: false },
    { name: "token_count", type: "int32", nullable: false },
    { name: "ingested_at", type: "float64", nullable: false },
    { name: "category", type: "utf8", nullable: false },
    { name: "importance", type: "float64", nullable: false },
    { name: "vector", type: "fixed_size_list<float32>", nullable: false },
  ],
};

export const factCardsArrowSchemaSpec: ArrowSchemaSpec = {
  name: FACT_CARDS_TABLE,
  fields: [
    { name: "card_id", type: "utf8", nullable: false },
    { name: "doc_id", type: "utf8", nullable: false },
    { name: "chunk_id", type: "utf8", nullable: false },
    { name: "claim", type: "utf8", nullable: false },
    { name: "evidence_text", type: "utf8", nullable: false },
    { name: "entities", type: "list<utf8>", nullable: false },
    {
      name: "metrics",
      type: "list<struct>",
      nullable: false,
      children: [
        { name: "name", type: "utf8", nullable: false },
        { name: "value", type: "utf8", nullable: false },
        { name: "unit", type: "utf8", nullable: false },
        { name: "period", type: "utf8", nullable: false },
        { name: "as_of", type: "float64", nullable: true },
      ],
    },
    { name: "as_of", type: "float64", nullable: true },
    { name: "doc_date", type: "float64", nullable: true },
    { name: "confidence", type: "utf8", nullable: false },
    { name: "tags", type: "list<utf8>", nullable: false },
    { name: "source_url", type: "utf8", nullable: false },
    { name: "source_title", type: "utf8", nullable: false },
    { name: "doc_type", type: "utf8", nullable: false },
    { name: "company", type: "utf8", nullable: false },
    { name: "ticker", type: "utf8", nullable: false },
    { name: "published_at", type: "float64", nullable: false },
    { name: "retrieved_at", type: "float64", nullable: false },
    {
      name: "citations",
      type: "list<struct>",
      nullable: false,
      children: [
        { name: "doc_id", type: "utf8", nullable: false },
        { name: "chunk_id", type: "utf8", nullable: false },
        { name: "page", type: "int32", nullable: true },
        { name: "section", type: "utf8", nullable: true },
        { name: "url", type: "utf8", nullable: false },
        { name: "published_at", type: "float64", nullable: true },
      ],
    },
    { name: "embedding", type: "fixed_size_list<float32>", nullable: false },
    { name: "text_norm_hash", type: "utf8", nullable: false },
    { name: "search_text", type: "utf8", nullable: false },
  ],
};

export function buildRawChunksSchema(vectorDim: number): SchemaLike {
  return new Schema([
    new Field("chunk_id", new Utf8(), false),
    new Field("doc_id", new Utf8(), false),
    new Field("text", new Utf8(), false),
    new Field("text_norm_hash", new Utf8(), false),
    new Field("source_url", new Utf8(), false),
    new Field("source_title", new Utf8(), false),
    new Field("doc_type", new Utf8(), false),
    new Field("company", new Utf8(), false),
    new Field("ticker", new Utf8(), false),
    new Field("published_at", new Float64(), false),
    new Field("retrieved_at", new Float64(), false),
    new Field("section", new Utf8(), false),
    new Field("page", new Int32(), false),
    new Field("char_start", new Int32(), false),
    new Field("char_end", new Int32(), false),
    new Field(
      "embedding",
      new FixedSizeList(vectorDim, new Field("item", new Float32(), true)),
      false,
    ),
    new Field("tags", new List(new Field("item", new Utf8(), true)), false),
    new Field("chunk_hash", new Utf8(), false),
    new Field("text_norm", new Utf8(), false),
    new Field("citation_key", new Utf8(), false),
    new Field("search_text", new Utf8(), false),
    new Field("chunk_index", new Int32(), false),
    new Field("token_count", new Int32(), false),
    new Field("ingested_at", new Float64(), false),
    new Field("category", new Utf8(), false),
    new Field("importance", new Float64(), false),
    new Field("vector", new FixedSizeList(vectorDim, new Field("item", new Float32(), true)), false),
  ]);
}

export function buildFactCardsSchema(vectorDim: number): SchemaLike {
  const metricStruct = new Struct([
    new Field("name", new Utf8(), false),
    new Field("value", new Utf8(), false),
    new Field("unit", new Utf8(), false),
    new Field("period", new Utf8(), false),
    new Field("as_of", new Float64(), true),
  ]);
  const citationStruct = new Struct([
    new Field("doc_id", new Utf8(), false),
    new Field("chunk_id", new Utf8(), false),
    new Field("page", new Int32(), true),
    new Field("section", new Utf8(), true),
    new Field("url", new Utf8(), false),
    new Field("published_at", new Float64(), true),
  ]);
  return new Schema([
    new Field("card_id", new Utf8(), false),
    new Field("doc_id", new Utf8(), false),
    new Field("chunk_id", new Utf8(), false),
    new Field("claim", new Utf8(), false),
    new Field("evidence_text", new Utf8(), false),
    new Field("entities", new List(new Field("item", new Utf8(), true)), false),
    new Field("metrics", new List(new Field("item", metricStruct, true)), false),
    new Field("as_of", new Float64(), true),
    new Field("doc_date", new Float64(), true),
    new Field("confidence", new Utf8(), false),
    new Field("tags", new List(new Field("item", new Utf8(), true)), false),
    new Field("source_url", new Utf8(), false),
    new Field("source_title", new Utf8(), false),
    new Field("doc_type", new Utf8(), false),
    new Field("company", new Utf8(), false),
    new Field("ticker", new Utf8(), false),
    new Field("published_at", new Float64(), false),
    new Field("retrieved_at", new Float64(), false),
    new Field("citations", new List(new Field("item", citationStruct, true)), false),
    new Field(
      "embedding",
      new FixedSizeList(vectorDim, new Field("item", new Float32(), true)),
      false,
    ),
    new Field("text_norm_hash", new Utf8(), false),
    new Field("search_text", new Utf8(), false),
  ]);
}

export function rawChunkSeedRow(vectorDim: number): RawChunkRow {
  return {
    chunk_id: "__schema__",
    doc_id: "__schema__",
    text: "__schema__",
    text_norm_hash: "__schema__",
    source_url: "__schema__",
    source_title: "__schema__",
    doc_type: "other",
    company: "__schema__",
    ticker: "__schema__",
    published_at: 0,
    retrieved_at: 0,
    section: "__schema__",
    page: 0,
    char_start: 0,
    char_end: 0,
    embedding: Array.from({ length: vectorDim }).fill(0),
    tags: ["__schema__"],
    chunk_hash: "__schema__",
    text_norm: "__schema__",
    citation_key: "__schema__",
    search_text: "__schema__",
    chunk_index: 0,
    token_count: 0,
    ingested_at: 0,
    category: "other",
    importance: 0,
    vector: Array.from({ length: vectorDim }).fill(0),
  };
}

export function factCardSeedRow(vectorDim: number): FactCardRow {
  return {
    card_id: "__schema__",
    doc_id: "__schema__",
    chunk_id: "__schema__",
    claim: "__schema__",
    evidence_text: "__schema__",
    entities: ["__schema__"],
    metrics: [{ name: "schema", value: "0", unit: "none", period: "", as_of: 0 }],
    as_of: 0,
    doc_date: 0,
    confidence: "low",
    tags: ["__schema__"],
    source_url: "__schema__",
    source_title: "__schema__",
    doc_type: "other",
    company: "__schema__",
    ticker: "__schema__",
    published_at: 0,
    retrieved_at: 0,
    citations: [
      {
        doc_id: "__schema__",
        chunk_id: "__schema__",
        page: 0,
        section: "__schema__",
        url: "__schema__",
        published_at: 0,
      },
    ],
    embedding: Array.from({ length: vectorDim }).fill(0),
    text_norm_hash: "__schema__",
    search_text: "__schema__",
  };
}
