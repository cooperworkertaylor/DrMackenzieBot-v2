# Sprint 1 Foundation

Sprint 1 defines the architecture and data contract baseline for the next version of DrMackenzieBot.

## Sprint Goal

Define a durable, source-backed, Telegram-first research operating model that fits the existing repo and prepares it for a Supabase-backed execution layer.

## Current-State Notes

The repo already has three important ingredients:

- a SQLite-backed research database in `src/research/db.ts`
- an in-memory queue in `src/research/quickrun/background-queue.ts`
- structured research and rendering pieces in `src/v2`

Sprint 1 does not replace those immediately. It defines the target model so the next sprints can migrate them cleanly.

## Product Guardrails

- the bot is a research system, not an autonomous trading system
- every substantive claim requires a source
- every time-sensitive claim requires a normalized date
- facts, inference, and speculation must be distinguishable
- long-running workflows must be asynchronous
- important state must live in durable storage, not only in chat history

## Canonical Workflows

### `research_request`
Statuses:
- `queued`
- `planning`
- `retrieving`
- `extracting`
- `synthesizing`
- `critic_review`
- `completed`
- `failed`
- `cancelled`

Idempotency key:
- normalized request plus target entity plus freshness window

### `refresh_entity`
Statuses:
- `queued`
- `checking_freshness`
- `ingesting`
- `extracting`
- `diffing`
- `synthesizing`
- `completed`
- `failed`

Idempotency key:
- entity id plus refresh horizon

### `daily_brief`
Statuses:
- `queued`
- `collecting`
- `ranking`
- `summarizing`
- `completed`
- `failed`

Idempotency key:
- date plus watchlist id

### `ingest_source`
Statuses:
- `queued`
- `fetching`
- `normalizing`
- `deduping`
- `extracting_metadata`
- `completed`
- `failed`

Idempotency key:
- source id plus external id or canonical url plus content hash

## Core Contracts

### `Claim`
- `claim_type`
- `claim_text`
- `entity_ref`
- `published_at`
- `effective_at`
- `support_span`
- `confidence`
- `source_document_id`

### `Event`
- `event_type`
- `event_title`
- `event_summary`
- `entity_ref`
- `event_at`
- `effective_at`
- `confidence`
- `source_document_id`

### `Metric`
- `metric_name`
- `metric_period`
- `metric_value`
- `metric_unit`
- `currency`
- `as_of_date`
- `confidence`
- `source_document_id`

### `ResearchReport`
- `report_type`
- `title`
- `summary`
- `what_changed`
- `evidence`
- `bull_case`
- `bear_case`
- `unknowns`
- `next_actions`
- `sources`
- `confidence`

## Structured Logging Standard

Required fields:

- `timestamp`
- `level`
- `service`
- `event_name`
- `request_id`
- `job_id`
- `research_request_id`
- `entity_id`
- `tool_name`
- `duration_ms`
- `status`
- `error_code`

Canonical event names:

- `telegram.request.received`
- `telegram.response.sent`
- `job.enqueued`
- `job.claimed`
- `job.completed`
- `job.failed`
- `connector.fetch.started`
- `connector.fetch.completed`
- `extraction.claims.completed`
- `report.generated`
- `critic.rejected`

## Sprint Exit Criteria

- the repo contains the Sprint 1 architecture docs
- the repo contains the initial Supabase foundation migration
- the repo has a GitHub Issues-ready backlog for follow-on sprints
- the repo can proceed into durable queue and async Telegram work in Sprint 2
