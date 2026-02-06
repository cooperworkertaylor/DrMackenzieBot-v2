import fs from "node:fs";
import path from "node:path";
import { chunkText } from "./chunker.js";
import { openResearchDb, resolveResearchDbPath } from "./db.js";

type IndexOptions = {
  root: string;
  include?: string[];
  exclude?: string[];
  dbPath?: string;
};

const DEFAULT_INCLUDE = [".ts", ".tsx", ".js", ".jsx", ".py", ".go", ".rs", ".md"];
const DEFAULT_EXCLUDE = ["node_modules", ".git", "dist", "build", ".turbo", ".next"];

export const indexRepo = (opts: IndexOptions) => {
  const root = path.resolve(opts.root);
  const include = opts.include ?? DEFAULT_INCLUDE;
  const exclude = opts.exclude ?? DEFAULT_EXCLUDE;
  const dbPath = resolveResearchDbPath(opts.dbPath);
  const db = openResearchDb(dbPath);
  const files = walkFiles(root, include, exclude);
  const insertFile = db.prepare(`
    insert into repo_files (root, rel_path, mtime, size, lang)
    values (@root, @rel_path, @mtime, @size, @lang)
    on conflict(root, rel_path) do update set mtime=excluded.mtime, size=excluded.size, lang=excluded.lang
    returning id;
  `);
  const insertChunk = db.prepare(`
    insert into repo_chunks (file_id, seq, text, start_line, end_line)
    values (@file_id, @seq, @text, @start_line, @end_line)
    on conflict(file_id, seq) do update set text=excluded.text, start_line=excluded.start_line, end_line=excluded.end_line;
  `);
  const deleteChunks = db.prepare(`DELETE FROM repo_chunks WHERE file_id = ?`);

  db.exec("BEGIN");
  try {
    for (const file of files) {
      const text = fs.readFileSync(file.abs, "utf8");
      const chunks = chunkText(text, 200);
      const row = insertFile.get({
        root,
        rel_path: file.rel,
        mtime: file.mtime,
        size: file.size,
        lang: detectLang(file.rel),
      }) as { id: number };
      deleteChunks.run(row.id);
      for (const chunk of chunks) {
        insertChunk.run({
          file_id: row.id,
          seq: chunk.seq,
          text: chunk.text,
          start_line: chunk.startLine ?? null,
          end_line: chunk.endLine ?? null,
        });
      }
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return { filesIndexed: files.length, dbPath };
};

const walkFiles = (root: string, include: string[], exclude: string[]) => {
  const results: Array<{ abs: string; rel: string; mtime: number; size: number }> = [];
  const stack = [root];
  while (stack.length) {
    const dir = stack.pop()!;
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
      const abs = path.join(dir, entry.name);
      const rel = path.relative(root, abs);
      if (exclude.some((p) => rel === p || rel.startsWith(`${p}${path.sep}`))) continue;
      if (entry.isDirectory()) {
        stack.push(abs);
        continue;
      }
      if (!include.some((ext) => entry.name.endsWith(ext))) continue;
      const stat = fs.statSync(abs);
      results.push({ abs, rel, mtime: stat.mtimeMs, size: stat.size });
    }
  }
  return results;
};

const detectLang = (rel: string): string => {
  const ext = path.extname(rel).toLowerCase();
  switch (ext) {
    case ".ts":
    case ".tsx":
      return "ts";
    case ".js":
    case ".jsx":
      return "js";
    case ".py":
      return "py";
    case ".go":
      return "go";
    case ".rs":
      return "rs";
    case ".md":
      return "md";
    default:
      return ext.replace(/^\./, "") || "plain";
  }
};
