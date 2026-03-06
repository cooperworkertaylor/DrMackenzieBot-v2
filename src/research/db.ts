import type { DatabaseSync } from "node:sqlite";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { requireNodeSqlite } from "../memory/sqlite.js";

export type ResearchDb = DatabaseSync;

const resolveUserPath = (input: string): string => {
  const trimmed = input.trim();
  if (!trimmed) return trimmed;
  if (trimmed.startsWith("~")) {
    return path.resolve(trimmed.replace(/^~(?=$|[\\/])/, os.homedir()));
  }
  return path.resolve(trimmed);
};

const resolveDefaultDbPath = (env: NodeJS.ProcessEnv = process.env): string => {
  const explicit = env.RESEARCH_DB_PATH?.trim() || env.OPENCLAW_RESEARCH_DB_PATH?.trim();
  if (explicit) {
    return resolveUserPath(explicit);
  }
  const stateDir =
    env.OPENCLAW_STATE_DIR?.trim() ||
    env.CLAWDBOT_STATE_DIR?.trim() ||
    path.join(os.homedir(), ".openclaw");
  return path.join(resolveUserPath(stateDir), "research", "research.db");
};

const DEFAULT_DB_PATH = resolveDefaultDbPath();

export type ResearchDbOptions = {
  allowExtensions?: boolean;
  encryptionKey?: string;
  requireEncryption?: boolean;
};

export const ensureDir = (dir: string) => {
  fs.mkdirSync(dir, { recursive: true });
};

export const resolveResearchDbPath = (dbPath = DEFAULT_DB_PATH) => path.resolve(dbPath);

const parseBoolEnv = (value: string | undefined): boolean | undefined => {
  if (typeof value !== "string") return undefined;
  const normalized = value.trim().toLowerCase();
  if (!normalized) return undefined;
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return undefined;
};

const sqlQuote = (value: string): string => `'${value.replaceAll("'", "''")}'`;

const resolveAllowExtensions = (options?: ResearchDbOptions): boolean =>
  options?.allowExtensions ?? parseBoolEnv(process.env.RESEARCH_DB_ALLOW_EXTENSIONS) ?? false;

const resolveEncryptionKey = (options?: ResearchDbOptions): string | undefined => {
  const fromOptions = options?.encryptionKey?.trim();
  if (fromOptions) return fromOptions;
  const fromEnv = process.env.RESEARCH_DB_KEY?.trim();
  return fromEnv || undefined;
};

const resolveRequireEncryption = (options?: ResearchDbOptions): boolean =>
  options?.requireEncryption ?? parseBoolEnv(process.env.RESEARCH_DB_REQUIRE_ENCRYPTION) ?? false;

const applyDbEncryption = (
  db: ResearchDb,
  params: { encryptionKey?: string; require: boolean },
) => {
  if (!params.encryptionKey) {
    if (params.require) {
      throw new Error(
        "Research DB encryption required but RESEARCH_DB_KEY/encryptionKey is not set.",
      );
    }
    return;
  }

  try {
    db.exec(`PRAGMA key = ${sqlQuote(params.encryptionKey)};`);
  } catch (err) {
    if (params.require) {
      throw new Error(
        `Research DB encryption key failed to apply: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
    return;
  }

  const cipherVersionRow = db.prepare("PRAGMA cipher_version;").get() as
    | { cipher_version?: string }
    | undefined;
  const cipherVersion = cipherVersionRow?.cipher_version?.trim();
  if (!cipherVersion && params.require) {
    throw new Error(
      "Research DB encryption required but SQLCipher codec is unavailable (PRAGMA cipher_version returned empty).",
    );
  }
};

export const openResearchDb = (
  dbPath = DEFAULT_DB_PATH,
  options?: ResearchDbOptions,
): ResearchDb => {
  const abs = resolveResearchDbPath(dbPath);
  ensureDir(path.dirname(abs));
  if (!fs.existsSync(abs)) {
    fs.closeSync(fs.openSync(abs, "a", 0o600));
  } else {
    fs.chmodSync(abs, 0o600);
  }
  const { DatabaseSync } = requireNodeSqlite();
  const db = new DatabaseSync(abs, { allowExtension: resolveAllowExtensions(options) });
  applyDbEncryption(db, {
    encryptionKey: resolveEncryptionKey(options),
    require: resolveRequireEncryption(options),
  });
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA synchronous = NORMAL;");
  db.exec("PRAGMA foreign_keys = ON;");
  db.exec("PRAGMA trusted_schema = OFF;");
  migrate(db);
  return db;
};

const migrate = (db: ResearchDb) => {
  db.exec(`
    create table if not exists instruments (
      id integer primary key,
      ticker text unique,
      cik text,
      name text,
      exchange text,
      currency text,
      sector text,
      industry text,
      updated_at integer
    );

    create table if not exists prices (
      id integer primary key,
      instrument_id integer not null,
      date text not null,
      open real,
      high real,
      low real,
      close real,
      volume real,
      source text,
      fetched_at integer not null,
      unique (instrument_id, date, source)
    );

    create table if not exists filings (
      id integer primary key,
      instrument_id integer,
      cik text,
      accession text unique,
      accession_raw text,
      form text,
      is_amendment integer not null default 0,
      filed text,
      accepted_at text,
      period_end text,
      as_of_date text,
      title text,
      url text,
      source_url text,
      source text,
      text text,
      filing_hash text,
      fetched_at integer not null
    );

    create table if not exists filing_versions (
      id integer primary key,
      filing_id integer not null,
      instrument_id integer,
      cik text,
      accession text not null,
      accession_raw text,
      form text not null default '',
      is_amendment integer not null default 0,
      filing_date text not null default '',
      accepted_at text not null default '',
      period_end text not null default '',
      as_of_date text not null default '',
      source_url text not null default '',
      primary_doc text not null default '',
      filing_hash text not null default '',
      fetched_at integer not null,
      unique (accession, accepted_at, filing_hash)
    );

    create table if not exists fundamental_facts (
      id integer primary key,
      instrument_id integer,
      ticker text,
      cik text not null,
      entity_name text,
      taxonomy text not null,
      concept text not null,
      label text not null,
      unit text not null,
      value real not null,
      as_of_date text not null,
      period_start text not null default '',
      period_end text not null,
      filing_date text not null default '',
      accepted_at text not null default '',
      accession text not null default '',
      accession_nodash text not null default '',
      form text not null default '',
      frame text not null default '',
      fiscal_year integer not null default 0,
      fiscal_period text not null default '',
      revision_number integer not null default 1,
      is_latest integer not null default 0,
      source text not null default 'sec_companyfacts',
      source_url text not null default '',
      fetched_at integer not null,
      unique (
        cik,
        taxonomy,
        concept,
        unit,
        period_end,
        filing_date,
        accepted_at,
        accession_nodash,
        form,
        frame,
        fiscal_year,
        fiscal_period,
        value,
        source_url
      )
    );

    create table if not exists transcripts (
      id integer primary key,
      instrument_id integer,
      event_date text,
      event_type text,
      source text,
      url text unique,
      title text,
      speakers text,
      content text,
      fetched_at integer not null
    );

    create table if not exists earnings_expectations (
      id integer primary key,
      instrument_id integer not null,
      ticker text not null,
      period_type text not null,
      fiscal_date_ending text not null,
      reported_date text not null default '',
      reported_eps real,
      estimated_eps real,
      surprise real,
      surprise_pct real,
      report_time text,
      source text not null,
      source_url text not null default '',
      fetched_at integer not null,
      unique (instrument_id, period_type, fiscal_date_ending, reported_date, source)
    );

    create table if not exists documents (
      id integer primary key,
      instrument_id integer,
      doc_type text,
      ref text,
      period text,
      url text,
      title text,
      content text,
      fetched_at integer not null,
      unique (doc_type, ref, url)
    );

    create table if not exists catalysts (
      id integer primary key,
      instrument_id integer not null,
      ticker text not null,
      category text not null default 'company',
      name text not null,
      date_window_start text not null default '',
      date_window_end text not null default '',
      probability real not null,
      impact_bps real not null,
      confidence real not null,
      direction text not null default 'both',
      source text not null default 'manual',
      status text not null default 'open',
      notes text not null default '',
      created_at integer not null,
      updated_at integer not null
    );

    create table if not exists catalyst_outcomes (
      id integer primary key,
      catalyst_id integer not null unique,
      occurred integer not null,
      realized_impact_bps real,
      resolved_at integer not null,
      notes text not null default ''
    );

    create table if not exists thesis_forecasts (
      id integer primary key,
      instrument_id integer,
      ticker text not null,
      forecast_type text not null default 'valuation_upside',
      horizon_days integer not null,
      predicted_return real not null,
      start_price real not null,
      base_price_date text not null,
      source text not null default 'memo',
      created_at integer not null,
      resolved integer not null default 0,
      resolved_at integer,
      end_price real,
      realized_return real,
      resolution_note text not null default ''
    );

    create table if not exists thesis_alerts (
      id integer primary key,
      instrument_id integer,
      ticker text not null,
      severity text not null,
      alert_type text not null,
      message text not null,
      details text not null default '',
      created_at integer not null,
      resolved integer not null default 0,
      resolved_at integer
    );

    create table if not exists task_outcomes (
      id integer primary key,
      task_type text not null,
      task_archetype text not null default '',
      policy_name text not null default '',
      policy_role text not null default 'primary',
      experiment_group text not null default '',
      ticker text not null default '',
      repo_root text not null default '',
      input_summary text not null default '',
      output_hash text not null default '',
      confidence real,
      citation_count integer,
      latency_ms integer,
      user_score real,
      realized_outcome_score real,
      outcome_label text not null default '',
      source_mix text not null default '{}',
      grading_metrics text not null default '{}',
      grader_score real not null default 0,
      grader_details text not null default '{}',
      grader_version text not null default 'v1',
      status text not null default 'pending',
      status_reason text not null default '',
      created_at integer not null,
      updated_at integer not null
    );

    create table if not exists policy_variants (
      id integer primary key,
      task_type text not null,
      task_archetype text not null default '',
      policy_name text not null,
      status text not null default 'challenger',
      active integer not null default 1,
      traffic_weight real not null default 0.0,
      shadow_weight real not null default 0.0,
      min_samples integer not null default 25,
      min_lift real not null default 0.03,
      max_quarantine_rate real not null default 0.2,
      max_calibration_error real not null default 0.25,
      metadata text not null default '{}',
      created_at integer not null,
      updated_at integer not null,
      unique (task_type, task_archetype, policy_name)
    );

    create table if not exists policy_decisions (
      id integer primary key,
      task_type text not null,
      task_archetype text not null default '',
      decision_type text not null,
      champion_before text not null default '',
      champion_after text not null default '',
      challenger text not null default '',
      reason text not null default '',
      metrics text not null default '{}',
      created_at integer not null
    );

    create table if not exists benchmark_suites (
      id integer primary key,
      name text not null,
      task_type text not null,
      task_archetype text not null default '',
      description text not null default '',
      active integer not null default 1,
      gating_min_samples integer not null default 25,
      gating_min_lift real not null default 0.03,
      gating_max_risk_breaches integer not null default 0,
      canary_drop_threshold real not null default 0.07,
      reliability_min_completion real not null default 0.9,
      reliability_max_timeout_rate real not null default 0.1,
      reliability_min_reproducibility real not null default 0.8,
      reliability_max_avg_retries real not null default 1.5,
      metadata text not null default '{}',
      created_at integer not null,
      updated_at integer not null,
      unique (name, task_type, task_archetype)
    );

    create table if not exists benchmark_cases (
      id integer primary key,
      suite_id integer not null,
      case_name text not null,
      task_archetype text not null default '',
      ticker text not null default '',
      repo_root text not null default '',
      input_summary text not null default '',
      prompt_text text not null default '',
      expected text not null default '{}',
      weight real not null default 1.0,
      active integer not null default 1,
      created_at integer not null,
      updated_at integer not null,
      unique (suite_id, case_name)
    );

    create table if not exists benchmark_runs (
      id integer primary key,
      suite_id integer not null,
      run_mode text not null default 'champion_vs_challenger',
      seed text not null default '',
      status text not null default 'completed',
      summary text not null default '{}',
      started_at integer not null,
      completed_at integer not null,
      created_at integer not null
    );

    create table if not exists benchmark_results (
      id integer primary key,
      run_id integer not null,
      case_id integer not null,
      task_type text not null,
      task_archetype text not null default '',
      policy_name text not null,
      policy_role text not null default 'primary',
      sample_count integer not null default 0,
      score real not null default 0,
      win integer not null default 0,
      risk_breach integer not null default 0,
      metrics text not null default '{}',
      created_at integer not null,
      unique (run_id, case_id, policy_name, policy_role)
    );

    create table if not exists execution_traces (
      id integer primary key,
      task_type text not null,
      task_archetype text not null default '',
      policy_name text not null default '',
      policy_role text not null default 'primary',
      experiment_group text not null default '',
      ticker text not null default '',
      repo_root text not null default '',
      seed text not null default '',
      trace_hash text not null default '',
      output_hash text not null default '',
      success integer not null default 0,
      step_count integer not null default 0,
      retry_count integer not null default 0,
      error_count integer not null default 0,
      timeout_count integer not null default 0,
      total_latency_ms integer not null default 0,
      metadata text not null default '{}',
      started_at integer not null,
      completed_at integer not null,
      created_at integer not null
    );

    create table if not exists execution_trace_steps (
      id integer primary key,
      trace_id integer not null,
      seq integer not null,
      tool_name text not null,
      action text not null default '',
      status text not null,
      latency_ms integer not null default 0,
      retries integer not null default 0,
      error_type text not null default '',
      input_hash text not null default '',
      output_hash text not null default '',
      details text not null default '{}',
      created_at integer not null,
      unique (trace_id, seq)
    );

    create table if not exists provenance_events (
      id integer primary key,
      event_type text not null,
      entity_type text not null default '',
      entity_id text not null default '',
      payload_hash text not null default '',
      prev_hash text not null default '',
      event_hash text not null default '',
      signature text not null default '',
      key_id text not null default '',
      metadata text not null default '{}',
      created_at integer not null
    );

    create table if not exists research_entities (
      id integer primary key,
      kind text not null default 'company',
      canonical_name text not null,
      ticker text not null default '',
      metadata text not null default '{}',
      created_at integer not null,
      updated_at integer not null,
      unique (kind, canonical_name, ticker)
    );

    create table if not exists research_claims (
      id integer primary key,
      entity_id integer not null,
      claim_text text not null,
      claim_type text not null default 'thesis',
      confidence real not null default 0.5,
      valid_from text not null default '',
      valid_to text not null default '',
      status text not null default 'active',
      source_task_outcome_id integer,
      metadata text not null default '{}',
      created_at integer not null,
      updated_at integer not null
    );

    create table if not exists research_claim_evidence (
      id integer primary key,
      claim_id integer not null,
      source_table text not null default '',
      ref_id integer not null default 0,
      citation_url text not null default '',
      excerpt_hash text not null default '',
      metadata text not null default '{}',
      created_at integer not null
    );

    create table if not exists research_claim_status_history (
      id integer primary key,
      claim_id integer not null,
      status text not null,
      reason text not null default '',
      confidence real,
      changed_at integer not null,
      metadata text not null default '{}'
    );

    create table if not exists theme_taxonomy (
      id integer primary key,
      theme_key text not null,
      version integer not null,
      display_name text not null default '',
      description text not null default '',
      parent_theme_key text not null default '',
      benchmark text not null default '',
      rules text not null default '{}',
      status text not null default 'active',
      effective_from text not null default '',
      effective_to text not null default '',
      created_at integer not null,
      updated_at integer not null,
      unique (theme_key, version)
    );

    create table if not exists theme_constituents (
      id integer primary key,
      theme_key text not null,
      theme_version integer not null,
      ticker text not null,
      membership_score real not null default 0,
      confidence real not null default 0,
      status text not null default 'candidate',
      rationale text not null default '',
      source text not null default 'rule_engine',
      valid_from text not null default '',
      valid_to text not null default '',
      metadata text not null default '{}',
      created_at integer not null,
      updated_at integer not null,
      unique (theme_key, theme_version, ticker)
    );

    create table if not exists macro_factor_observations (
      id integer primary key,
      factor_key text not null,
      date text not null,
      value real not null,
      source text not null default '',
      source_url text not null default '',
      metadata text not null default '{}',
      fetched_at integer not null,
      unique (factor_key, date, source)
    );

    create table if not exists quality_gate_runs (
      id integer primary key,
      artifact_type text not null,
      artifact_id text not null,
      gate_name text not null default 'institutional_v1',
      score real not null,
      min_score real not null,
      passed integer not null default 0,
      required_failures text not null default '[]',
      checks text not null default '[]',
      metrics text not null default '{}',
      metadata text not null default '{}',
      created_at integer not null
    );

    create table if not exists research_events (
      id integer primary key,
      entity_id integer not null,
      event_type text not null,
      event_time integer not null,
      period_start text not null default '',
      period_end text not null default '',
      source_table text not null default '',
      source_ref_id integer not null default 0,
      source_url text not null default '',
      title text not null default '',
      payload text not null default '{}',
      event_hash text not null default '',
      created_at integer not null,
      updated_at integer not null,
      unique (entity_id, event_hash)
    );

    create table if not exists research_facts (
      id integer primary key,
      entity_id integer not null,
      event_id integer,
      metric_key text not null,
      metric_kind text not null default 'numeric',
      value_num real,
      value_text text not null default '',
      unit text not null default '',
      direction text not null default '',
      confidence real not null default 0.5,
      as_of_date text not null default '',
      valid_from text not null default '',
      valid_to text not null default '',
      source_table text not null default '',
      source_ref_id integer not null default 0,
      source_url text not null default '',
      metadata text not null default '{}',
      fact_hash text not null default '',
      created_at integer not null,
      updated_at integer not null,
      unique (entity_id, fact_hash)
    );

    create table if not exists research_reports (
      id integer primary key,
      entity_id integer not null,
      ticker text not null default '',
      report_type text not null default 'external_structured',
      title text not null default '',
      summary text not null default '',
      markdown text not null default '',
      report_json text not null default '{}',
      confidence real not null default 0,
      source_count integer not null default 0,
      lookback_days integer not null default 30,
      generated_at integer not null,
      created_at integer not null,
      updated_at integer not null
    );

    create table if not exists research_theses (
      id integer primary key,
      entity_id integer not null,
      ticker text not null default '',
      thesis_type text not null default 'external_structured',
      version_number integer not null,
      stance text not null default 'neutral',
      summary text not null default '',
      confidence real not null default 0,
      bull_case text not null default '[]',
      bear_case text not null default '[]',
      open_questions text not null default '[]',
      supporting_evidence text not null default '[]',
      report_id integer,
      created_at integer not null,
      updated_at integer not null,
      unique (ticker, thesis_type, version_number)
    );

    create table if not exists research_thesis_diffs (
      id integer primary key,
      entity_id integer not null,
      ticker text not null default '',
      thesis_type text not null default 'external_structured',
      previous_thesis_id integer,
      current_thesis_id integer not null,
      report_id integer,
      thesis_break integer not null default 0,
      confidence_delta real not null default 0,
      summary text not null default '',
      delta_json text not null default '{}',
      created_at integer not null
    );

    create table if not exists research_watchlists (
      id integer primary key,
      name text not null unique,
      description text not null default '',
      is_default integer not null default 0,
      created_at integer not null,
      updated_at integer not null
    );

    create table if not exists research_watchlist_memberships (
      id integer primary key,
      watchlist_id integer not null,
      ticker text not null,
      priority integer not null default 3,
      tags text not null default '[]',
      created_at integer not null,
      updated_at integer not null,
      unique (watchlist_id, ticker)
    );

    create table if not exists research_refresh_queue (
      id integer primary key,
      watchlist_id integer not null,
      ticker text not null,
      source_document_id integer not null,
      priority text not null default 'medium',
      reason text not null default '',
      status text not null default 'queued',
      created_at integer not null,
      updated_at integer not null,
      unique (watchlist_id, ticker, source_document_id)
    );

    create table if not exists research_briefs (
      id integer primary key,
      watchlist_id integer not null,
      brief_type text not null default 'daily_watchlist',
      brief_date text not null,
      title text not null default '',
      markdown text not null default '',
      brief_json text not null default '{}',
      created_at integer not null,
      updated_at integer not null,
      unique (watchlist_id, brief_type, brief_date)
    );

    create table if not exists research_tool_runs (
      id integer primary key,
      workflow text not null default '',
      tool_name text not null default '',
      capability_key text not null default '',
      status text not null default 'ok',
      subject text not null default '',
      latency_ms integer not null default 0,
      request_metadata text not null default '{}',
      response_metadata text not null default '{}',
      error_text text not null default '',
      created_at integer not null
    );

    create table if not exists research_approval_requests (
      id integer primary key,
      workflow text not null default '',
      capability_key text not null default '',
      subject text not null default '',
      status text not null default 'pending',
      requested_by text not null default '',
      resolved_by text not null default '',
      details text not null default '{}',
      created_at integer not null,
      updated_at integer not null
    );

    create table if not exists external_documents (
      id integer primary key,
      source_type text not null default 'manual',
      provider text not null default 'other',
      source_key text not null default '',
      external_id text not null default '',
      sender text not null default '',
      title text not null default '',
      subject text not null default '',
      url text not null default '',
      canonical_url text not null default '',
      ticker text not null default '',
      published_at text not null default '',
      received_at text not null default '',
      content text not null default '',
      normalized_content text not null default '',
      content_hash text not null unique,
      trust_tier integer not null default 4,
      materiality_score real not null default 0,
      raw_artifact_path text not null default '',
      metadata text not null default '{}',
      fetched_at integer not null
    );

    create table if not exists research_sources (
      id integer primary key,
      source_key text not null unique,
      source_type text not null default '',
      provider text not null default '',
      sender text not null default '',
      base_url text not null default '',
      trust_tier integer not null default 4,
      active integer not null default 1,
      metadata text not null default '{}',
      created_at integer not null,
      updated_at integer not null
    );

    create table if not exists chunks (
      id integer primary key,
      source_table text not null,
      ref_id integer not null,
      seq integer not null,
      text text not null,
      pending_embedding integer default 1,
      metadata text,
      unique (source_table, ref_id, seq)
    );

    create table if not exists repo_files (
      id integer primary key,
      root text not null,
      rel_path text not null,
      mtime integer not null,
      size integer not null,
      lang text,
      unique (root, rel_path)
    );

    create table if not exists repo_chunks (
      id integer primary key,
      file_id integer not null,
      seq integer not null,
      text text not null,
      start_line integer,
      end_line integer,
      pending_embedding integer default 1,
      unique (file_id, seq)
    );

    create table if not exists eval_runs (
      id integer primary key,
      run_type text not null,
      score real not null,
      passed integer not null,
      total integer not null,
      details text,
      created_at integer not null
    );

    create table if not exists research_vectors (
      source_table text not null,
      row_id integer not null,
      dims integer not null,
      provider text not null default '',
      model text not null default '',
      embedding text not null,
      updated_at integer not null,
      primary key (source_table, row_id)
    );

    create table if not exists quickrun_jobs (
      id text primary key,
      job_type text not null,
      status text not null default 'queued',
      payload text not null default '{}',
      run_after_ms integer not null,
      attempts integer not null default 0,
      max_attempts integer not null default 3,
      locked_by text not null default '',
      locked_at_ms integer,
      heartbeat_at_ms integer,
      completed_at_ms integer,
      last_error text not null default '',
      created_at_ms integer not null,
      updated_at_ms integer not null
    );

    create index if not exists idx_prices_instrument_date
      on prices (instrument_id, date desc);

    create index if not exists idx_filings_instrument_filed
      on filings (instrument_id, filed desc);

    create index if not exists idx_filing_versions_accession
      on filing_versions (accession, accepted_at desc);

    create index if not exists idx_fundamental_facts_lookup
      on fundamental_facts (
        instrument_id,
        taxonomy,
        concept,
        unit,
        period_end,
        filing_date
      );

    create index if not exists idx_fundamental_facts_latest
      on fundamental_facts (instrument_id, is_latest, as_of_date desc);

    create index if not exists idx_earnings_expectations_lookup
      on earnings_expectations (instrument_id, period_type, fiscal_date_ending desc, reported_date desc);

    create index if not exists idx_catalysts_open_window
      on catalysts (instrument_id, status, date_window_start, date_window_end);

    create index if not exists idx_catalyst_outcomes_catalyst
      on catalyst_outcomes (catalyst_id);

    create index if not exists idx_external_documents_source_fetched
      on external_documents (source_type, fetched_at desc);

    create index if not exists idx_external_documents_provider_fetched
      on external_documents (provider, fetched_at desc);

    create index if not exists idx_external_documents_ticker_fetched
      on external_documents (ticker, fetched_at desc);

    create index if not exists idx_external_documents_source_key_fetched
      on external_documents (source_key, fetched_at desc);

    create index if not exists idx_external_documents_canonical_url
      on external_documents (canonical_url, fetched_at desc);

    create index if not exists idx_research_sources_type_provider
      on research_sources (source_type, provider, active, updated_at desc);

    create index if not exists idx_thesis_forecasts_resolution
      on thesis_forecasts (ticker, resolved, created_at);

    create index if not exists idx_thesis_alerts_lookup
      on thesis_alerts (ticker, resolved, created_at desc);

    create index if not exists idx_task_outcomes_lookup
      on task_outcomes (task_type, status, created_at desc);

    create index if not exists idx_task_outcomes_archetype
      on task_outcomes (task_type, task_archetype, created_at desc);

    create index if not exists idx_task_outcomes_hash
      on task_outcomes (output_hash, created_at desc);

    create index if not exists idx_task_outcomes_policy
      on task_outcomes (task_type, task_archetype, policy_name, policy_role, created_at desc);

    create index if not exists idx_policy_variants_lookup
      on policy_variants (task_type, task_archetype, status, active, updated_at desc);

    create index if not exists idx_policy_decisions_lookup
      on policy_decisions (task_type, task_archetype, created_at desc);

    create index if not exists idx_benchmark_suites_lookup
      on benchmark_suites (task_type, task_archetype, active, updated_at desc);

    create index if not exists idx_benchmark_cases_lookup
      on benchmark_cases (suite_id, active, updated_at desc);

    create index if not exists idx_benchmark_runs_lookup
      on benchmark_runs (suite_id, created_at desc);

    create index if not exists idx_benchmark_results_lookup
      on benchmark_results (run_id, task_type, task_archetype, policy_name, score desc);

    create index if not exists idx_execution_traces_lookup
      on execution_traces (task_type, task_archetype, policy_name, created_at desc);

    create index if not exists idx_execution_traces_seed
      on execution_traces (task_type, task_archetype, policy_name, seed, output_hash, created_at desc);

    create index if not exists idx_execution_trace_steps_lookup
      on execution_trace_steps (trace_id, seq);

    create index if not exists idx_provenance_events_lookup
      on provenance_events (event_type, entity_type, entity_id, created_at desc);

    create index if not exists idx_provenance_events_hash
      on provenance_events (event_hash, created_at desc);

    create index if not exists idx_research_entities_lookup
      on research_entities (kind, ticker, canonical_name);

    create index if not exists idx_research_claims_lookup
      on research_claims (entity_id, status, confidence desc, updated_at desc);

    create index if not exists idx_research_claims_validity
      on research_claims (valid_from, valid_to, updated_at desc);

    create index if not exists idx_research_claim_evidence_lookup
      on research_claim_evidence (claim_id, source_table, ref_id);

    create index if not exists idx_research_claim_status_lookup
      on research_claim_status_history (claim_id, changed_at desc);

    create index if not exists idx_theme_taxonomy_lookup
      on theme_taxonomy (theme_key, status, version desc, updated_at desc);

    create index if not exists idx_theme_taxonomy_parent
      on theme_taxonomy (parent_theme_key, status, updated_at desc);

    create index if not exists idx_theme_constituents_lookup
      on theme_constituents (theme_key, theme_version, status, membership_score desc);

    create index if not exists idx_theme_constituents_ticker
      on theme_constituents (ticker, status, membership_score desc);

    create index if not exists idx_macro_factor_lookup
      on macro_factor_observations (factor_key, date desc, source);

    create index if not exists idx_macro_factor_date
      on macro_factor_observations (date desc, factor_key);

    create index if not exists idx_quality_gate_runs_lookup
      on quality_gate_runs (artifact_type, created_at desc);

    create index if not exists idx_quality_gate_runs_artifact
      on quality_gate_runs (artifact_type, artifact_id, created_at desc);

    create index if not exists idx_quality_gate_runs_pass
      on quality_gate_runs (passed, created_at desc);

    create index if not exists idx_research_events_lookup
      on research_events (entity_id, event_time desc, event_type);

    create index if not exists idx_research_events_source
      on research_events (source_table, source_ref_id, event_time desc);

    create index if not exists idx_research_facts_lookup
      on research_facts (entity_id, metric_key, as_of_date desc, updated_at desc);

    create index if not exists idx_research_facts_validity
      on research_facts (entity_id, valid_from, valid_to, as_of_date desc);

    create index if not exists idx_research_facts_event
      on research_facts (event_id, metric_key, as_of_date desc);

    create index if not exists idx_research_reports_lookup
      on research_reports (entity_id, report_type, generated_at desc);

    create index if not exists idx_research_reports_ticker
      on research_reports (ticker, report_type, generated_at desc);

    create index if not exists idx_research_theses_lookup
      on research_theses (ticker, thesis_type, version_number desc);

    create index if not exists idx_research_thesis_diffs_lookup
      on research_thesis_diffs (ticker, thesis_type, created_at desc);

    create index if not exists idx_research_watchlists_default
      on research_watchlists (is_default, updated_at desc);

    create index if not exists idx_research_watchlist_memberships_lookup
      on research_watchlist_memberships (watchlist_id, priority, ticker);

    create index if not exists idx_research_refresh_queue_lookup
      on research_refresh_queue (watchlist_id, status, priority, created_at desc);

    create index if not exists idx_research_briefs_lookup
      on research_briefs (watchlist_id, brief_type, brief_date desc);

    create index if not exists idx_research_tool_runs_lookup
      on research_tool_runs (workflow, capability_key, created_at desc);

    create index if not exists idx_research_approval_requests_lookup
      on research_approval_requests (workflow, capability_key, status, created_at desc);

    create index if not exists idx_quickrun_jobs_status_run_after
      on quickrun_jobs (status, run_after_ms, created_at_ms);

    create index if not exists idx_quickrun_jobs_type_status_run_after
      on quickrun_jobs (job_type, status, run_after_ms, created_at_ms);
  `);

  ensureColumn(db, "filings", "accession_raw", "TEXT");
  ensureColumn(db, "filings", "is_amendment", "INTEGER NOT NULL DEFAULT 0");
  ensureColumn(db, "filings", "accepted_at", "TEXT");
  ensureColumn(db, "filings", "as_of_date", "TEXT");
  ensureColumn(db, "filings", "source_url", "TEXT");
  ensureColumn(db, "filings", "filing_hash", "TEXT");
  ensureColumn(db, "external_documents", "source_key", "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, "external_documents", "canonical_url", "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, "external_documents", "normalized_content", "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, "external_documents", "trust_tier", "INTEGER NOT NULL DEFAULT 4");
  ensureColumn(db, "external_documents", "materiality_score", "REAL NOT NULL DEFAULT 0");
  ensureColumn(db, "external_documents", "raw_artifact_path", "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, "research_vectors", "provider", "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, "research_vectors", "model", "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, "task_outcomes", "policy_name", "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, "task_outcomes", "policy_role", "TEXT NOT NULL DEFAULT 'primary'");
  ensureColumn(db, "task_outcomes", "experiment_group", "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, "benchmark_suites", "reliability_min_completion", "REAL NOT NULL DEFAULT 0.9");
  ensureColumn(db, "benchmark_suites", "reliability_max_timeout_rate", "REAL NOT NULL DEFAULT 0.1");
  ensureColumn(
    db,
    "benchmark_suites",
    "reliability_min_reproducibility",
    "REAL NOT NULL DEFAULT 0.8",
  );
  ensureColumn(db, "benchmark_suites", "reliability_max_avg_retries", "REAL NOT NULL DEFAULT 1.5");
};

const ensureColumn = (db: ResearchDb, table: string, column: string, definition: string) => {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  if (rows.some((row) => row.name === column)) return;
  db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
};
