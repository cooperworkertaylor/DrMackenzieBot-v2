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
      form text,
      filed text,
      period_end text,
      title text,
      url text,
      source text,
      text text,
      fetched_at integer not null
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
      embedding text not null,
      updated_at integer not null,
      primary key (source_table, row_id)
    );
  `);
};
