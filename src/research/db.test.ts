import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { openResearchDb } from "./db.js";

const makeDbPath = (name: string) =>
  path.join(os.tmpdir(), `openclaw-research-db-${name}-${Date.now()}-${Math.random()}.db`);

describe("research db migration", () => {
  it("upgrades legacy external_documents tables before creating new indexes", () => {
    const dbPath = makeDbPath("legacy-external-docs");
    const db = openResearchDb(dbPath);
    db.exec(`
      DROP TABLE IF EXISTS external_documents;
      CREATE TABLE external_documents (
        id integer primary key,
        source_type text not null default 'manual',
        provider text not null default 'other',
        external_id text not null default '',
        sender text not null default '',
        title text not null default '',
        subject text not null default '',
        url text not null default '',
        ticker text not null default '',
        published_at text not null default '',
        received_at text not null default '',
        content text not null default '',
        content_hash text not null unique,
        metadata text not null default '{}',
        fetched_at integer not null
      );
    `);
    db[Symbol.dispose]?.();

    const migrated = openResearchDb(dbPath);
    const columns = migrated.prepare(`PRAGMA table_info(external_documents)`).all() as Array<{
      name: string;
    }>;
    const indexes = migrated.prepare(`PRAGMA index_list(external_documents)`).all() as Array<{
      name: string;
    }>;

    expect(columns.map((column) => column.name)).toContain("source_key");
    expect(columns.map((column) => column.name)).toContain("canonical_url");
    expect(
      indexes.some((index) => index.name === "idx_external_documents_source_key_fetched"),
    ).toBe(true);

    migrated[Symbol.dispose]?.();
    fs.rmSync(dbPath, { force: true });
  });
});
