# Research Service and Module Layout v1

This layout is anchored to the current repo. It does not assume a clean-room rewrite. It maps the next architecture onto the existing `src/research`, `src/v2`, Telegram channel code, and gateway stack.

## Current Baseline

- Telegram already exists as a supported channel.
- `src/research` contains the current research DB, ingestion, provenance, quality, and quickrun queue logic.
- `src/v2` contains evidence, render, quality, and pipeline pieces already moving toward a more structured research pipeline.
- `src/research/quickrun/background-queue.ts` is in-memory today and must become durable.

## Target Runtime Topology

### `telegram-gateway`
Primary home:
- `src/telegram`
- `src/channels`
- `src/infra/outbound`

Responsibilities:
- parse Telegram requests and commands
- create `research_requests`
- enqueue jobs
- send acknowledgments, progress, and final reports

### `research-worker`
Primary home:
- `src/research`
- `src/v2`

Responsibilities:
- claim pending jobs
- execute workflow handlers
- persist status, outputs, lineage, and tool runs

### `scheduler`
Primary home:
- `src/cron`

Responsibilities:
- schedule daily briefs
- trigger watchlist refreshes
- run ingest and maintenance jobs

### `admin-tools`
Primary home:
- `src/cli`
- `scripts`

Responsibilities:
- inspect queue state
- replay dead-letter jobs
- seed prompts and source registry
- run evals and nightly jobs

## Research Engine Boundaries

### `src/research`
Keep or evolve:
- source-specific ingestion
- research DB compatibility layer
- provenance logic
- monitoring
- policy and security gates

### `src/v2`
Use as the structured report pipeline surface:
- evidence normalization
- quality gates
- render
- schema validation
- pipeline orchestration

### Proposed boundary
- `src/research` remains the ingestion and domain layer
- `src/v2` becomes the structured report assembly and quality layer
- durable queue and durable job state become shared infrastructure, not a quickrun-only utility

## Future Shared Modules

Recommended additions:

- `src/research/jobs`
  - job repository
  - claim/heartbeat/retry helpers

- `src/research/contracts`
  - typed research request, report, thesis, claim, and event contracts

- `src/research/supabase`
  - Supabase/Postgres repositories
  - storage helpers
  - vector retrieval helpers

- `src/research/workflows`
  - `research-request`
  - `refresh-entity`
  - `daily-brief`
  - `ingest-source`

## Migration Rule

Do not replace SQLite immediately. Introduce Supabase as the new durable system of record for jobs, reports, and research memory while keeping compatibility with the current SQLite path during migration.
