import { padCik, resolveTickerToCik } from "../agents/sec-xbrl-timeseries.js";
import { ensureApiKey, fetchAlphaVantageDaily } from "./alpha-vantage.js";
import { chunkText } from "./chunker.js";
import { openResearchDb } from "./db.js";
import { fetchRecentFilings, downloadFilingText, politeThrottle } from "./sec-filings.js";
import { fetchTranscript } from "./transcript-scraper.js";

export const upsertInstrument = (params: {
  ticker: string;
  cik?: string;
  name?: string;
  exchange?: string;
  currency?: string;
}) => {
  const db = openResearchDb();
  const stmt = db.prepare(`
    insert into instruments (ticker, cik, name, exchange, currency, updated_at)
    values (@ticker, @cik, @name, @exchange, @currency, @updated_at)
    on conflict(ticker) do update set
      cik=excluded.cik,
      name=coalesce(excluded.name, instruments.name),
      exchange=coalesce(excluded.exchange, instruments.exchange),
      currency=coalesce(excluded.currency, instruments.currency),
      updated_at=excluded.updated_at
    returning id;
  `);
  const row = stmt.get({
    ...params,
    cik: params.cik ? padCik(params.cik) : null,
    updated_at: Date.now(),
  }) as { id: number };
  return row.id;
};

export const ingestPrices = async (ticker: string) => {
  const instrumentId = upsertInstrument({ ticker });
  const apiKey = ensureApiKey();
  const prices = await fetchAlphaVantageDaily(ticker, apiKey);
  const db = openResearchDb();
  const stmt = db.prepare(`
    insert into prices (instrument_id, date, open, high, low, close, volume, source, fetched_at)
    values (@instrument_id, @date, @open, @high, @low, @close, @volume, @source, @fetched_at)
    on conflict(instrument_id, date, source) do update set
      open=excluded.open,
      high=excluded.high,
      low=excluded.low,
      close=excluded.close,
      volume=excluded.volume,
      fetched_at=excluded.fetched_at;
  `);
  db.exec("BEGIN");
  try {
    for (const row of prices) {
      stmt.run({
        instrument_id: instrumentId,
        ...row,
        fetched_at: Date.now(),
      });
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return prices.length;
};

export const ingestFilings = async (
  ticker: string,
  opts: { limit?: number; userAgent?: string },
) => {
  const instrumentId = upsertInstrument({ ticker });
  const cik = await resolveTickerToCik(ticker, fetch, opts.userAgent);
  const filings = await fetchRecentFilings(cik, opts.userAgent, opts.limit);
  const db = openResearchDb();
  const insertFiling = db.prepare(`
    insert into filings (instrument_id, cik, accession, form, filed, period_end, title, url, source, text, fetched_at)
    values (@instrument_id, @cik, @accession, @form, @filed, @period_end, @title, @url, @source, @text, @fetched_at)
    on conflict(accession) do update set
      form=excluded.form,
      filed=excluded.filed,
      period_end=excluded.period_end,
      url=excluded.url,
      text=excluded.text,
      fetched_at=excluded.fetched_at;
  `);
  const insertChunk = db.prepare(`
    insert into chunks (source_table, ref_id, seq, text, metadata)
    values ('filings', @ref_id, @seq, @text, @metadata)
    on conflict(source_table, ref_id, seq) do update set text=excluded.text, metadata=excluded.metadata;
  `);

  const results = [];
  for (const meta of filings) {
    const full = await downloadFilingText(meta, opts.userAgent).catch((err) => {
      results.push({ accession: meta.accession, ok: false, error: err.message });
      return null;
    });
    if (!full) continue;
    const info = insertFiling.get({
      instrument_id: instrumentId,
      ...full,
      cik: null,
      period_end: meta.periodEnd ?? null,
      title: meta.title ?? null,
      source: "sec",
      fetched_at: Date.now(),
    }) as { id: number };
    const chunks = chunkText(full.text, 256);
    db.exec("BEGIN");
    try {
      for (const chunk of chunks) {
        insertChunk.run({
          ref_id: info.id,
          seq: chunk.seq,
          text: chunk.text,
          metadata: JSON.stringify({
            accession: meta.accession,
            form: meta.form,
            filed: meta.filed,
          }),
        });
      }
      db.exec("COMMIT");
    } catch (err) {
      db.exec("ROLLBACK");
      throw err;
    }
    results.push({ accession: meta.accession, ok: true });
    await politeThrottle();
  }
  return results;
};

export const ingestTranscript = async (ticker: string, url: string) => {
  const instrumentId = upsertInstrument({ ticker });
  const doc = await fetchTranscript(url);
  const db = openResearchDb();
  const insertTranscript = db.prepare(`
    insert into transcripts (instrument_id, event_date, event_type, source, url, title, speakers, content, fetched_at)
    values (@instrument_id, @event_date, @event_type, @source, @url, @title, @speakers, @content, @fetched_at)
    on conflict(url) do update set
      title=excluded.title,
      content=excluded.content,
      fetched_at=excluded.fetched_at;
  `);
  const row = insertTranscript.get({
    instrument_id: instrumentId,
    event_date: null,
    event_type: null,
    source: doc.source,
    url: doc.url,
    title: doc.title,
    speakers: null,
    content: doc.content,
    fetched_at: Date.now(),
  }) as { id: number };

  const chunks = chunkText(doc.content, 256);
  const insertChunk = db.prepare(`
    insert into chunks (source_table, ref_id, seq, text, metadata)
    values ('transcripts', @ref_id, @seq, @text, @metadata)
    on conflict(source_table, ref_id, seq) do update set text=excluded.text, metadata=excluded.metadata;
  `);
  db.exec("BEGIN");
  try {
    for (const chunk of chunks) {
      insertChunk.run({
        ref_id: row.id,
        seq: chunk.seq,
        text: chunk.text,
        metadata: JSON.stringify({ url: doc.url, title: doc.title }),
      });
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return { chunks: chunks.length };
};
