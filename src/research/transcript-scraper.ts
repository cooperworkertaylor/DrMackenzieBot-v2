import { Readability } from "@mozilla/readability";
import { parseHTML } from "linkedom";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { chunkText } from "./chunker.js";

export type TranscriptDoc = {
  url: string;
  title: string;
  content: string;
  source: string;
};

export const fetchTranscript = async (url: string): Promise<TranscriptDoc> => {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Transcript fetch failed: HTTP ${res.status}`);
  const contentType = res.headers.get("content-type") ?? "";
  if (contentType.includes("pdf")) {
    const buf = Buffer.from(await res.arrayBuffer());
    const tmp = await writeTempPdf(buf);
    const text = await ocrPdf(tmp);
    await fs.unlink(tmp).catch(() => {});
    return { url, title: path.basename(url), content: text, source: "pdf-ocr" };
  }
  const html = await res.text();
  const { document } = parseHTML(html);
  const reader = new Readability(document);
  const article = reader.parse();
  const title = article?.title ?? path.basename(url);
  const content = (article?.textContent ?? html).trim();
  return { url, title, content, source: "html" };
};

const writeTempPdf = async (buf: Buffer) => {
  const tmp = path.join(os.tmpdir(), `transcript-${Date.now()}.pdf`);
  await fs.writeFile(tmp, buf);
  return tmp;
};

export const ocrPdf = async (pdfPath: string): Promise<string> => {
  const tesseract = process.env.TESSERACT_PATH ?? "tesseract";
  const txtPath = `${pdfPath}.txt`;
  await new Promise<void>((resolve, reject) => {
    const proc = spawn(tesseract, [pdfPath, txtPath.replace(/\.txt$/, "")]);
    let stderr = "";
    proc.stderr.on("data", (d) => {
      stderr += String(d);
    });
    proc.on("error", reject);
    proc.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`tesseract failed (code ${code}): ${stderr.trim()}`));
    });
  });
  const text = await fs.readFile(txtPath, "utf8");
  await fs.unlink(txtPath).catch(() => {});
  return text;
};

export const chunkTranscript = (doc: TranscriptDoc) => chunkText(doc.content, 256);
