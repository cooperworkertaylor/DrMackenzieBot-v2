import type { DatabaseSync } from "node:sqlite";
import fs from "node:fs";
import path from "node:path";
import { requireNodeSqlite } from "../memory/sqlite.js";

export type ResearchDb = DatabaseSync;

const DEFAULT_DB_PATH = path.join(process.cwd(), "data", "research.db");

export const ensureDir = (dir: string) => {
  fs.mkdirSync(dir, { recursive: true });
};

export const resolveResearchDbPath = (dbPath = DEFAULT_DB_PATH) => path.resolve(dbPath);

export const openResearchDb = (dbPath = DEFAULT_DB_PATH): ResearchDb => {
  const abs = resolveResearchDbPath(dbPath);
  ensureDir(path.dirname(abs));
  const { DatabaseSync } = requireNodeSqlite();
  const db = new DatabaseSync(abs, { allowExtension: true });
  db.exec("PRAGMA journal_mode = WAL;");
  db.exec("PRAGMA synchronous = NORMAL;");
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
  `);

  ensureColumn(db, "filings", "accession_raw", "TEXT");
  ensureColumn(db, "filings", "is_amendment", "INTEGER NOT NULL DEFAULT 0");
  ensureColumn(db, "filings", "accepted_at", "TEXT");
  ensureColumn(db, "filings", "as_of_date", "TEXT");
  ensureColumn(db, "filings", "source_url", "TEXT");
  ensureColumn(db, "filings", "filing_hash", "TEXT");
  ensureColumn(db, "research_vectors", "provider", "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, "research_vectors", "model", "TEXT NOT NULL DEFAULT ''");
};

const ensureColumn = (db: ResearchDb, table: string, column: string, definition: string) => {
  const rows = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  if (rows.some((row) => row.name === column)) return;
  db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
};
