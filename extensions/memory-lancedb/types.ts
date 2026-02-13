export type MemoryDocType =
  | "memo"
  | "filing"
  | "transcript"
  | "news"
  | "note"
  | "research"
  | "other";

export type MemoryChunkRow = {
  chunk_id: string;
  doc_id: string;
  text: string;
  text_norm: string;
  vector: number[];
  importance: number;
  category: string;
  company: string;
  ticker: string;
  doc_type: MemoryDocType;
  published_at: number;
  ingested_at: number;
  source_url: string;
  section: string;
  page: number;
  chunk_index: number;
  chunk_hash: string;
  char_start: number;
  char_end: number;
  token_count: number;
  citation_key: string;
};

export type MemoryChunkInput = Omit<
  MemoryChunkRow,
  "chunk_id" | "text_norm" | "chunk_hash" | "citation_key" | "ingested_at"
>;

export type CitationPointer = {
  key: string;
  chunkId: string;
  sourceUrl: string;
  page: number;
  charStart: number;
  charEnd: number;
};

export type RetrievedSnippet = {
  chunkId: string;
  text: string;
  score: number;
  sourceUrl: string;
  company: string;
  ticker: string;
  docType: string;
  section: string;
  page: number;
  publishedAt: number;
  chunkHash: string;
  citation: CitationPointer;
};

export type RetrievalBudget = {
  maxResults: number;
  maxTokensPerSnippet: number;
};

export type RetrievalFilters = {
  tickers?: string[];
  docTypes?: string[];
  sourceUrls?: string[];
  publishedAtMin?: number;
  publishedAtMax?: number;
};

