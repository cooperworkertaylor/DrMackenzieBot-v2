create extension if not exists pgcrypto;
create extension if not exists vector;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.entities (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null,
  ticker text,
  name text not null,
  exchange text,
  sector text,
  industry text,
  country text,
  status text not null default 'active',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.research_requests (
  id uuid primary key default gen_random_uuid(),
  request_type text not null,
  status text not null,
  telegram_chat_id text,
  telegram_message_id text,
  requested_entity_id uuid references public.entities(id),
  input_text text not null,
  normalized_input jsonb not null default '{}'::jsonb,
  idempotency_key text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists research_requests_idempotency_uidx
  on public.research_requests (idempotency_key)
  where idempotency_key is not null;

create table if not exists public.jobs (
  id uuid primary key default gen_random_uuid(),
  research_request_id uuid references public.research_requests(id) on delete set null,
  job_type text not null,
  status text not null,
  priority integer not null default 100,
  attempt_count integer not null default 0,
  max_attempts integer not null default 3,
  run_after timestamptz not null default now(),
  locked_by text,
  locked_at timestamptz,
  heartbeat_at timestamptz,
  started_at timestamptz,
  finished_at timestamptz,
  idempotency_key text,
  input_payload jsonb not null default '{}'::jsonb,
  output_payload jsonb not null default '{}'::jsonb,
  error_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists jobs_status_run_after_priority_idx
  on public.jobs (status, run_after, priority desc);

create unique index if not exists jobs_idempotency_uidx
  on public.jobs (idempotency_key)
  where idempotency_key is not null;

create table if not exists public.sources (
  id uuid primary key default gen_random_uuid(),
  source_key text not null unique,
  name text not null,
  source_type text not null,
  trust_tier text not null,
  freshness_hours integer,
  connector_name text not null,
  is_active boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.documents (
  id uuid primary key default gen_random_uuid(),
  source_id uuid not null references public.sources(id),
  entity_id uuid references public.entities(id),
  external_id text,
  canonical_url text,
  title text,
  document_type text not null,
  published_at timestamptz,
  effective_at timestamptz,
  content_hash text not null,
  raw_storage_path text,
  normalized_text text,
  language text,
  metadata jsonb not null default '{}'::jsonb,
  ingestion_status text not null default 'pending',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists documents_entity_published_idx
  on public.documents (entity_id, published_at desc);

create index if not exists documents_content_hash_idx
  on public.documents (content_hash);

create table if not exists public.document_chunks (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.documents(id) on delete cascade,
  chunk_index integer not null,
  content text not null,
  token_count integer,
  embedding vector(1536),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create unique index if not exists document_chunks_document_chunk_uidx
  on public.document_chunks (document_id, chunk_index);

create table if not exists public.claims (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.documents(id) on delete cascade,
  entity_id uuid references public.entities(id),
  claim_type text not null,
  claim_text text not null,
  normalized_claim jsonb not null default '{}'::jsonb,
  support_span text,
  published_at timestamptz,
  effective_at timestamptz,
  confidence numeric(5,4),
  extraction_method text not null,
  is_conflicted boolean not null default false,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists claims_entity_published_idx
  on public.claims (entity_id, published_at desc);

create table if not exists public.events (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.documents(id) on delete cascade,
  entity_id uuid references public.entities(id),
  event_type text not null,
  event_title text not null,
  event_summary text,
  event_at timestamptz,
  effective_at timestamptz,
  confidence numeric(5,4),
  materiality_score numeric(6,4),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists events_entity_event_at_idx
  on public.events (entity_id, event_at desc);

create table if not exists public.metrics (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references public.documents(id) on delete cascade,
  entity_id uuid references public.entities(id),
  metric_name text not null,
  metric_period text,
  metric_value numeric,
  metric_unit text,
  currency text,
  as_of_date date,
  confidence numeric(5,4),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists metrics_entity_as_of_date_idx
  on public.metrics (entity_id, as_of_date desc);

create table if not exists public.theses (
  id uuid primary key default gen_random_uuid(),
  entity_id uuid not null references public.entities(id) on delete cascade,
  version_number integer not null,
  summary text not null,
  bull_case text,
  bear_case text,
  key_drivers jsonb not null default '[]'::jsonb,
  risks jsonb not null default '[]'::jsonb,
  open_questions jsonb not null default '[]'::jsonb,
  confidence numeric(5,4),
  reviewed_at timestamptz,
  created_at timestamptz not null default now()
);

create unique index if not exists theses_entity_version_uidx
  on public.theses (entity_id, version_number);

create table if not exists public.reports (
  id uuid primary key default gen_random_uuid(),
  research_request_id uuid references public.research_requests(id) on delete set null,
  entity_id uuid references public.entities(id),
  report_type text not null,
  status text not null,
  title text,
  summary text,
  body_markdown text,
  confidence numeric(5,4),
  prompt_version_id uuid,
  model_name text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists reports_entity_created_idx
  on public.reports (entity_id, created_at desc);

create table if not exists public.report_claims (
  report_id uuid not null references public.reports(id) on delete cascade,
  claim_id uuid not null references public.claims(id) on delete cascade,
  primary key (report_id, claim_id)
);

create table if not exists public.tool_runs (
  id uuid primary key default gen_random_uuid(),
  job_id uuid references public.jobs(id) on delete set null,
  tool_name text not null,
  capability_key text,
  status text not null,
  latency_ms integer,
  cost_usd numeric(10,4),
  request_metadata jsonb not null default '{}'::jsonb,
  response_metadata jsonb not null default '{}'::jsonb,
  error_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.approvals (
  id uuid primary key default gen_random_uuid(),
  job_id uuid not null references public.jobs(id) on delete cascade,
  approval_type text not null,
  status text not null,
  requested_reason text,
  requested_at timestamptz not null default now(),
  resolved_at timestamptz,
  resolved_by text
);

create table if not exists public.prompts (
  id uuid primary key default gen_random_uuid(),
  prompt_key text not null,
  version text not null,
  purpose text not null,
  body text not null,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  unique (prompt_key, version)
);

create or replace trigger set_updated_at_entities
before update on public.entities
for each row execute function public.set_updated_at();

create or replace trigger set_updated_at_research_requests
before update on public.research_requests
for each row execute function public.set_updated_at();

create or replace trigger set_updated_at_jobs
before update on public.jobs
for each row execute function public.set_updated_at();

create or replace trigger set_updated_at_documents
before update on public.documents
for each row execute function public.set_updated_at();

create or replace trigger set_updated_at_reports
before update on public.reports
for each row execute function public.set_updated_at();
