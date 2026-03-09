export type MemoryDocType =
  | "memo"
  | "filing"
  | "transcript"
  | "news"
  | "note"
  | "research"
  | "other";

export type FactCardConfidence = "high" | "med" | "low";

export type MetricValue = {
  name: string;
  value: string;
  unit: string;
  period: string;
  as_of: number | null;
};

export type CitationRecord = {
  doc_id: string;
  chunk_id: string;
  page?: number;
  section?: string;
  url: string;
  published_at?: number;
};

export type RawChunkRow = {
  // Required citation-first/raw fields
  chunk_id: string;
  doc_id: string;
  text: string;
  text_norm_hash: string;
  source_url: string;
  source_title: string;
  doc_type: MemoryDocType;
  company: string;
  ticker: string;
  published_at: number;
  retrieved_at: number;
  section: string;
  page: number;
  char_start: number;
  char_end: number;
  embedding: number[];
  tags: string[];
  // Compatibility + retrieval quality fields
  chunk_hash: string;
  text_norm: string;
  citation_key: string;
  search_text: string;
  chunk_index: number;
  token_count: number;
  ingested_at: number;
  category: string;
  importance: number;
  vector: number[];
};

export type FactCardRow = {
  card_id: string;
  doc_id: string;
  chunk_id: string;
  claim: string;
  evidence_text: string;
  entities: string[];
  metrics: MetricValue[];
  as_of: number | null;
  doc_date: number | null;
  confidence: FactCardConfidence;
  tags: string[];
  source_url: string;
  source_title: string;
  doc_type: MemoryDocType;
  company: string;
  ticker: string;
  published_at: number;
  retrieved_at: number;
  citations: CitationRecord[];
  embedding: number[];
  text_norm_hash: string;
  search_text: string;
};

export type MemoryChunkRow = RawChunkRow;

export type MemoryChunkInput = Omit<
  RawChunkRow,
  | "chunk_id"
  | "text_norm"
  | "chunk_hash"
  | "text_norm_hash"
  | "citation_key"
  | "ingested_at"
  | "vector"
  | "embedding"
  | "search_text"
>;

export type CitationPointer = {
  key: string;
  chunkId: string;
  sourceUrl: string;
  page: number;
  section?: string;
  charStart: number;
  charEnd: number;
  docId?: string;
  publishedAt?: number;
};

export type RetrievedItem = {
  id: string;
  type: "fact_card" | "raw_chunk";
  snippet: string;
  score: number;
  metadata: {
    doc_id: string;
    chunk_id: string;
    card_id?: string;
    source_url: string;
    source_title: string;
    company: string;
    ticker: string;
    doc_type: string;
    section: string;
    page: number;
    published_at: number;
    text_norm_hash: string;
    confidence?: FactCardConfidence;
  };
  citations: CitationRecord[];
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
  type?: "fact_card" | "raw_chunk";
};

export type CitationFirstBundle = {
  question: string;
  retrieved_items: RetrievedItem[];
};

export type RetrievalBudget = {
  maxResults: number;
  maxTokensPerSnippet: number;
  maxTotalTokens: number;
};

export type RetrievalFilters = {
  tickers?: string[];
  docTypes?: string[];
  sourceUrls?: string[];
  publishedAtMin?: number;
  publishedAtMax?: number;
};

export type QueryPlan = {
  entity: string;
  ticker: string;
  metrics: string[];
  timeframe: {
    from?: number;
    to?: number;
    label: string;
  };
  docTypes: string[];
  rewrites: string[];
  strictFilters: RetrievalFilters;
  looseFilters: RetrievalFilters;
};
