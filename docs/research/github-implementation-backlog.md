# DrMackenzieBot Implementation Backlog

This backlog is written in a GitHub Issues-ready format and is scoped to the current repo shape. The existing research stack already includes a SQLite-backed research DB, a background queue, evidence schemas, and a v2 pipeline. The plan below formalizes the next architecture and phases in Supabase without forcing a big-bang rewrite.

## Sprint 1: Foundation

### P0-01 Product Spec and Guardrails
Goal: define the bot's primary job as a Telegram-first investment research system that returns source-backed outputs with explicit freshness and confidence.

Acceptance criteria:
- product definition and non-goals are documented
- final report rules require sources and dates
- unsupported claims are explicitly disallowed

### P0-02 Domain Model and ERD
Goal: define the canonical research objects across the existing SQLite layer and the future Supabase layer.

Acceptance criteria:
- `entities`, `watchlists`, `research_requests`, `jobs`, `sources`, `documents`, `claims`, `events`, `metrics`, `theses`, `reports`, `tool_runs`, `approvals`, and `prompts` are documented
- provenance and lineage rules are documented

### P0-03 Workflow State Machines
Goal: define deterministic states for `research_request`, `refresh_entity`, `daily_brief`, and `ingest_source`.

Acceptance criteria:
- each workflow has statuses, transitions, retries, terminal failures, and an idempotency key

### P0-04 Report and Extraction Contracts
Goal: define the stable output contracts that the current research and v2 pipelines should converge on.

Acceptance criteria:
- typed contract exists for `Claim`, `Event`, `Metric`, `ThesisUpdate`, and `ResearchReport`

### P1-01 Runtime and Module Layout
Goal: define how Telegram gateway, worker, connectors, research engine, and shared modules fit into the current repo.

Acceptance criteria:
- service/module layout doc exists and maps onto current `src/research`, `src/v2`, and channel code

### P1-02 Structured Logging Standard
Goal: standardize log fields across Telegram requests, jobs, extraction, synthesis, and tool runs.

Acceptance criteria:
- required log fields and canonical event names are documented

### P1-03 Supabase Client Layer
Goal: define the future data-access boundary for Supabase while preserving the current SQLite path during migration.

Acceptance criteria:
- bridge strategy from SQLite to Supabase is documented
- repository conventions are documented

### P1-04 Initial Supabase Foundation Migration
Goal: add the initial Postgres schema for async jobs, research memory, provenance, and reporting.

Acceptance criteria:
- initial SQL migration exists
- schema includes jobs, requests, documents, claims, events, metrics, theses, reports, tool runs, approvals, and prompts

## Sprint 2: Execution Skeleton

### P1-05 Durable Job Queue
Goal: replace the in-memory queue in `src/research/quickrun/background-queue.ts` with a durable Postgres-backed queue.

Acceptance criteria:
- jobs can be claimed, heartbeated, retried, and recovered after restart

### P1-06 Worker Process
Goal: move long-running research work out of inline request flow and into a durable worker loop.

Acceptance criteria:
- worker executes queued jobs and writes durable status

### P2-01 Telegram Command Router
Goal: make `/research`, `/refresh`, `/status`, `/sources`, and `/why` first-class workflows.

Acceptance criteria:
- Telegram commands enqueue deterministic jobs instead of relying on free-form chat only

### P2-02 Async Telegram Response Flow
Goal: acknowledge immediately, stream progress, and return a final report asynchronously.

Acceptance criteria:
- long-running research jobs do not block the channel response path

### P2-03 Telegram Report Formatter
Goal: standardize Telegram output shape for investable research.

Acceptance criteria:
- final outputs include `Summary`, `What changed`, `Evidence`, `Bull case`, `Bear case`, `Unknowns`, `Next actions`, and `Sources`

## Sprint 3: Ingestion Core

### P3-01 Connector Interface
### P3-02 Raw Artifact Storage
### P3-03 Dedupe and Fingerprinting
### P3-04 Source Registry and Trust Tiers
### P3-05 Document Normalization Pipeline
### P3-06 Chunking and Embeddings
### P3-07 Materiality Classifier

Acceptance criteria:
- all ingested artifacts have raw storage, normalized text, metadata, provenance, and dedupe behavior
- material documents can trigger refresh jobs

## Sprint 4: Structured Extraction

### P4-01 Entity Resolution
### P4-02 Date and Time Extraction
### P4-03 Claim Extraction
### P4-04 Event Extraction
### P4-05 Metric Extraction
### P4-06 Contradiction and Duplicate Claim Detection
### P4-07 Extraction Regression Harness

Acceptance criteria:
- structured extraction is typed, source-backed, and regression-tested

## Sprint 5: Research Synthesis

### P5-01 Retrieval Layer
### P5-02 Report Composer
### P5-03 Unsupported Claim Critic
### P5-04 Freshness Critic
### P5-05 Confidence Scoring
### P5-06 Report Lineage Storage

Acceptance criteria:
- final reports are reproducible, citation-backed, and freshness-aware

## Sprint 6: Thesis Engine and Watchlists

### P6-01 Thesis Tables and Versioning
### P6-02 Thesis Update Workflow
### P6-03 What-Changed Diffing
### P6-04 Thesis Break Detection
### P6-05 Open Question Tracker
### P7-01 Watchlist Management
### P7-02 Daily Brief Workflow

Acceptance criteria:
- entity coverage persists over time and reports emphasize changes, not static rewrites

## Sprint 7: Tool Governance and Ops

### P8-01 Capability Registry
### P8-02 1Password Secret Injection Service
### P8-03 Tool Run Auditing
### P8-04 Approval Gates
### P9-01 launchd Service Definitions
### P9-02 Health Checks
### P9-03 Dead-Letter and Replay Tooling
### P9-04 Backup and Retention Policies
### P9-05 Observability Dashboard

Acceptance criteria:
- tool usage is scoped, logged, auditable, and operationally recoverable

## Sprint 8: Evaluation and Differentiators

### P10-01 Gold Task Set
### P10-02 Evaluation Runner
### P10-03 Research Quality Metrics
### P10-04 Extraction Regression Suite
### P11-01 Management Credibility Tracker
### P11-02 Estimate and Guidance Drift Analyzer
### P11-03 Source Conflict Detector
### P11-04 Peer Comparison Engine
### P11-05 Catalyst Calendar
### P12-01 User Preference Model
### P12-02 Research Notebook Integration
### P12-03 Thesis Memory Compounding

Acceptance criteria:
- the system has objective quality measurement and differentiated analyst-grade features
