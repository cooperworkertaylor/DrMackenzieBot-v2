# Supabase Foundation Schema

This schema is the durable Postgres target for the next research stack. It is additive to the current SQLite-based `src/research/db.ts` path and is intended to become the durable system of record for requests, jobs, provenance, and report lineage.

## Core Tables

### `entities`
Canonical companies and other tracked research targets.

Fields:
- `id uuid primary key`
- `entity_type text not null`
- `ticker text`
- `name text not null`
- `exchange text`
- `sector text`
- `industry text`
- `country text`
- `status text not null default 'active'`
- `metadata jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

### `research_requests`
Top-level requests originating from Telegram or future channels.

Fields:
- `id uuid primary key`
- `request_type text not null`
- `status text not null`
- `telegram_chat_id text`
- `telegram_message_id text`
- `requested_entity_id uuid references entities(id)`
- `input_text text not null`
- `normalized_input jsonb not null default '{}'::jsonb`
- `idempotency_key text`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

### `jobs`
Durable queue and execution state.

Fields:
- `id uuid primary key`
- `research_request_id uuid references research_requests(id) on delete set null`
- `job_type text not null`
- `status text not null`
- `priority integer not null default 100`
- `attempt_count integer not null default 0`
- `max_attempts integer not null default 3`
- `run_after timestamptz not null default now()`
- `locked_by text`
- `locked_at timestamptz`
- `heartbeat_at timestamptz`
- `started_at timestamptz`
- `finished_at timestamptz`
- `idempotency_key text`
- `input_payload jsonb not null default '{}'::jsonb`
- `output_payload jsonb not null default '{}'::jsonb`
- `error_payload jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null default now()`
- `updated_at timestamptz not null default now()`

### `sources`
Registry of external and internal information sources.

### `documents`
Canonical ingested artifacts with raw storage linkage and normalized text.

### `document_chunks`
Chunked retrieval units with embeddings.

### `claims`
Atomic extracted facts with source linkage and dates.

### `events`
Structured business events tied to documents and entities.

### `metrics`
Typed extracted numeric values.

### `theses`
Versioned thesis state by entity.

### `reports`
Rendered research outputs delivered to the user.

### `report_claims`
Join table for report lineage to supporting claims.

### `tool_runs`
Audit trail for API and tool usage.

### `approvals`
Human approval gates for sensitive or costly actions.

### `prompts`
Versioned prompt definitions.

## Required Indexes

- `jobs(status, run_after, priority desc)`
- `jobs(idempotency_key)` partial unique
- `research_requests(idempotency_key)` partial unique
- `documents(content_hash)`
- `documents(entity_id, published_at desc)`
- `claims(entity_id, published_at desc)`
- `events(entity_id, event_at desc)`
- `metrics(entity_id, as_of_date desc)`
- `reports(entity_id, created_at desc)`

## Storage Buckets

- `raw-artifacts`
- `rendered-reports`
- `debug-artifacts`

## Migration Intent

Sprint 1 adds the initial foundation migration only. Sprint 2 and Sprint 3 should add the durable queue repository, raw artifact storage integration, and the bridge between the current SQLite research data and the new Postgres-backed execution model.
