# LanceDB Retrieval Upgrade (Hybrid + Fact Cards)

## Assumptions
- Runtime: Node 18+ with `pnpm`.
- Plugin: `extensions/memory-lancedb`.
- Existing deployments may already have `memories` or `memories_v2`; migration must be additive and reversible.

## P0 — Schema + Compatibility
### Changes
- Add two additive tables:
  - `raw_chunks` (auditability + full metadata filters)
  - `fact_cards` (citation-first atomic claims)
- Keep legacy backfill support from `memories_v2`/`memories`.
- Add idempotent indexes for vector, BM25/FTS, and scalar filters.

### Acceptance Criteria
- Plugin boots with existing DB.
- `raw_chunks` and `fact_cards` exist after startup.
- Existing legacy rows are queryable through `raw_chunks`.

### Rollback
- Set retrieval to vector-only (`retrieval.hybridEnabled=false`) and use `raw_chunks` only.
- No destructive migration was applied; legacy tables remain untouched.

## P1 — Ingestion Quality + Fact Cards
### Changes
- Dedup on `source_url + text_norm_hash`.
- Chunking: heading-aware, 500–900 token windows, ~12% overlap.
- Boilerplate removal for web-origin text.
- Metadata extraction for company/ticker/doc type/date/source/section.
- Fact-card generation:
  - `claim` (single atomic sentence)
  - `evidence_text` (max 2 bullets)
  - `entities[]`, `metrics[]`, `confidence`
  - stable `citations[]` linking to `doc_id/chunk_id/page/section/url`

### Acceptance Criteria
- Re-ingestion of same source does not duplicate rows.
- `fact_cards` gets populated during ingest/store.
- Every fact card has at least one citation.

### Rollback
- Disable card generation operationally by using raw ingest only.
- Existing rows stay readable.

## P2 — Hybrid Retrieval + Citation Contract
### Changes
- Query planner extracts ticker/metric/timeframe/doc-type hints and generates 3–5 rewrites.
- Parallel retrieval channels:
  - fact cards vector (primary)
  - fact cards BM25 (primary)
  - raw chunks vector (fallback)
  - raw chunks BM25 (fallback)
- LanceDB hybrid flow with `fullTextSearch + RRFReranker` used per rewrite, with fallback fusion.
- Prefilter used for strict constraints; postfilter used for looser constraints to preserve recall.
- Dedup:
  - exact by `(source_url, text_norm_hash)`
  - near-duplicate snippet suppression
- Retrieval budget guardrails:
  - max results to LLM = 8
  - per-snippet token truncation
  - total retrieval token cap (default 800)
- Citation-first bundle contract:
  - `question + retrieved_items[]` with stable citation pointers
  - output validator flags paragraphs lacking citation ids

### Acceptance Criteria
- Retrieved payload always includes citations.
- Context budget respected across all returned items.
- Fact cards are preferred; raw chunks still recover misses.

### Rollback
- `retrieval.hybridEnabled=false` for vector-only fallback.
- Keep indexes and schema; rollback is behavioral, not destructive.

## P3 — Evaluation + Regression Gates
### Changes
- Eval dataset: `extensions/memory-lancedb/eval/eval_set.json` (25 checks).
- Runner: `extensions/memory-lancedb/eval/eval_runner.ts`.
- Metrics:
  - `recall@k`
  - `MRR`
  - `citation_coverage_rate`
  - latency (`avg/p50/p95`)
- CLI:
  - `pnpm eval:before`
  - `pnpm eval:after`
  - `pnpm eval:lancedb-retrieval` (comparison table)

### Acceptance Criteria
- `eval:after` improves or matches `eval:before` on recall and MRR.
- Citation coverage remains at or above baseline.
- Runner prints machine-readable JSON and human-readable comparison.

### Rollback
- Keep eval harness for diagnostics.
- Remove CI gate if needed without reverting runtime code.

## Operator Tuning
- `retrieval.maxResults`: default 8.
- `retrieval.maxTokensPerSnippet`: default 220.
- `retrieval.maxTotalTokens`: default 800.
- `retrieval.vectorLimit`: start at 40, raise to 80 for higher recall.
- `retrieval.ftsLimit`: start at 40, raise to 80 when lexical misses are high.
- `retrieval.rewriteCount`: 3–5 (default 5).
- `retrieval.rrfK`: default 60; increase for flatter rank blending.
- Filter strategy:
  - strict ticker/doc_type/date windows => prefilter.
  - exploratory theme/entity queries => postfilter for recall.

## Validation Commands
```bash
pnpm vitest run \
  extensions/memory-lancedb/ingestion.test.ts \
  extensions/memory-lancedb/retrieval.test.ts

pnpm eval:before
pnpm eval:after
pnpm eval:lancedb-retrieval
```
