# LanceDB Retrieval Rollout (Phased)

## Scope
- Plugin: `extensions/memory-lancedb`
- Goal: raise recall and citation grounding without breaking current workflows.

## P0: Schema + Compatibility (Low Risk)
### Changes
- Introduced `memories_v2` table with citation-safe chunk schema.
- Added legacy backfill from `memories` into `memories_v2` on initialization.
- Added index creation (vector, BM25/FTS, scalar metadata indices) best-effort.

### Acceptance
- Plugin starts cleanly.
- Existing legacy data remains available.
- `ltm stats` returns count from `memories_v2`.

### Rollback
- Revert plugin code to previous commit.
- Legacy `memories` table remains intact and unchanged.

## P1: Hybrid Retrieval + Grounding
### Changes
- Hybrid retrieval (`vector + BM25`) with query rewrites and RRF fusion.
- Dedup by `(source_url, chunk_hash)` and near-duplicate text.
- Retrieval budget guardrails:
  - max results (default `8`)
  - max tokens/snippet (default `220`)
- Every snippet returns citation pointer (`citation_key`, page, char offsets).
- Added `retrieval.hybridEnabled` switch for safe rollback to vector-only.

### Acceptance
- Retrieval output always includes citation metadata.
- Context payload size constrained by budget.
- `retrieval.hybridEnabled=false` falls back to vector-only.

### Rollback
- Set `retrieval.hybridEnabled=false` in plugin config.
- Keep schema/index upgrades; only retrieval behavior changes.

## P2: Ingestion Quality
### Changes
- New ingestion pipeline:
  - boilerplate removal (nav/footer/cookie/newsletter lines)
  - heading-aware chunking (`500–900` tokens, `~12%` overlap)
  - normalized-text hash dedup
  - published date extraction fallback
- Added tool + CLI ingestion entrypoint:
  - tool: `memory_ingest_document`
  - CLI: `ltm ingest ...`

### Acceptance
- Lower duplicate chunk rate.
- Higher hit rate on company/metric/timeframe queries.
- Ingested chunks contain stable citation keys.

### Rollback
- Use `memory_store` only (disable document ingestion operationally).
- Existing ingested chunks remain queryable.

## P3: Evaluation + Regression Control
### Changes
- Added benchmark corpus and 25 retrieval checks.
- Added evaluation harness with metrics:
  - recall@k
  - MRR
  - citation coverage
  - latency (avg/p50/p95)
- Added script: `pnpm eval:lancedb-retrieval`.
- Added unit tests for retrieval + ingestion behavior.

### Acceptance
- Eval script runs in CI/local with deterministic thresholds.
- No regression in retrieval metrics against baseline.

### Rollback
- Remove eval gate thresholds from CI if blocked.
- Keep harness as diagnostics tool.

## Operational Defaults
- `retrieval.hybridEnabled=true`
- `retrieval.maxResults=8`
- `retrieval.maxTokensPerSnippet=220`
- `retrieval.vectorLimit=40`
- `retrieval.ftsLimit=40`
- `retrieval.rewriteCount=5`
- `retrieval.rrfK=60`
- `chunking.targetTokens=700`
- `chunking.minTokens=500`
- `chunking.maxTokens=900`
- `chunking.overlapRatio=0.12`

## Validation Commands
```bash
pnpm vitest run \
  extensions/memory-lancedb/index.test.ts \
  extensions/memory-lancedb/retrieval.test.ts \
  extensions/memory-lancedb/ingestion.test.ts

pnpm eval:lancedb-retrieval
```

