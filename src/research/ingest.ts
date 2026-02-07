import { createHash } from "node:crypto";
import {
  fetchCompanyFacts,
  flattenCompanyFacts,
  padCik,
  resolveTickerToCik,
} from "../agents/sec-xbrl-timeseries.js";
import {
  ensureApiKey,
  fetchAlphaVantageDaily,
  fetchAlphaVantageEarnings,
} from "./alpha-vantage.js";
import { chunkText } from "./chunker.js";
import { openResearchDb, type ResearchDb } from "./db.js";
import { fetchRecentFilings, downloadFilingText, politeThrottle } from "./sec-filings.js";
import { fetchTranscript } from "./transcript-scraper.js";

const normalizeTicker = (ticker: string) => ticker.trim().toUpperCase();
const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");

export const upsertInstrument = (params: {
  ticker: string;
  cik?: string;
  name?: string;
  exchange?: string;
  currency?: string;
  dbPath?: string;
}) => {
  const db = openResearchDb(params.dbPath);
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
    ticker: normalizeTicker(params.ticker),
    cik: params.cik ? padCik(params.cik) : null,
    updated_at: Date.now(),
  }) as { id: number };
  return row.id;
};

export const ingestPrices = async (
  ticker: string,
  opts: {
    dbPath?: string;
  } = {},
) => {
  const normalizedTicker = normalizeTicker(ticker);
  const instrumentId = upsertInstrument({ ticker: normalizedTicker, dbPath: opts.dbPath });
  const apiKey = ensureApiKey();
  const prices = await fetchAlphaVantageDaily(normalizedTicker, apiKey);
  const db = openResearchDb(opts.dbPath);
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

export const ingestExpectations = async (
  ticker: string,
  opts: {
    dbPath?: string;
  } = {},
) => {
  const normalizedTicker = normalizeTicker(ticker);
  const instrumentId = upsertInstrument({ ticker: normalizedTicker, dbPath: opts.dbPath });
  const apiKey = ensureApiKey();
  const rows = await fetchAlphaVantageEarnings(normalizedTicker, apiKey);
  const db = openResearchDb(opts.dbPath);
  const insertExpectation = db.prepare(`
    insert into earnings_expectations (
      instrument_id,
      ticker,
      period_type,
      fiscal_date_ending,
      reported_date,
      reported_eps,
      estimated_eps,
      surprise,
      surprise_pct,
      report_time,
      source,
      source_url,
      fetched_at
    )
    values (
      @instrument_id,
      @ticker,
      @period_type,
      @fiscal_date_ending,
      @reported_date,
      @reported_eps,
      @estimated_eps,
      @surprise,
      @surprise_pct,
      @report_time,
      @source,
      @source_url,
      @fetched_at
    )
    on conflict(instrument_id, period_type, fiscal_date_ending, reported_date, source) do update set
      reported_eps=excluded.reported_eps,
      estimated_eps=excluded.estimated_eps,
      surprise=excluded.surprise,
      surprise_pct=excluded.surprise_pct,
      report_time=excluded.report_time,
      source_url=excluded.source_url,
      fetched_at=excluded.fetched_at
    returning id, period_type;
  `);
  const insertChunk = db.prepare(`
    insert into chunks (source_table, ref_id, seq, text, metadata)
    values ('earnings_expectations', @ref_id, 0, @text, @metadata)
    on conflict(source_table, ref_id, seq) do update set
      text=excluded.text,
      metadata=excluded.metadata,
      pending_embedding=1;
  `);
  const now = Date.now();
  let quarterly = 0;
  let annual = 0;
  db.exec("BEGIN");
  try {
    for (const row of rows) {
      const upserted = insertExpectation.get({
        instrument_id: instrumentId,
        ticker: normalizedTicker,
        period_type: row.periodType,
        fiscal_date_ending: row.fiscalDateEnding,
        reported_date: row.reportedDate ?? "",
        reported_eps: row.reportedEps ?? null,
        estimated_eps: row.estimatedEps ?? null,
        surprise: row.surprise ?? null,
        surprise_pct: row.surprisePct ?? null,
        report_time: row.reportTime ?? null,
        source: row.source,
        source_url: row.sourceUrl,
        fetched_at: now,
      }) as { id: number; period_type: "quarterly" | "annual" };
      if (upserted.period_type === "quarterly") quarterly += 1;
      if (upserted.period_type === "annual") annual += 1;

      const statement = [
        `${normalizedTicker} earnings expectations ${row.periodType}`,
        `fiscal_date_ending=${row.fiscalDateEnding}`,
        row.reportedDate ? `reported_date=${row.reportedDate}` : "",
        typeof row.reportedEps === "number" ? `reported_eps=${row.reportedEps}` : "",
        typeof row.estimatedEps === "number" ? `estimated_eps=${row.estimatedEps}` : "",
        typeof row.surprisePct === "number" ? `surprise_pct=${row.surprisePct}` : "",
        row.reportTime ? `report_time=${row.reportTime}` : "",
      ]
        .filter(Boolean)
        .join(" | ");
      insertChunk.run({
        ref_id: upserted.id,
        text: statement,
        metadata: JSON.stringify({
          ticker: normalizedTicker,
          periodType: row.periodType,
          fiscalDateEnding: row.fiscalDateEnding,
          reportedDate: row.reportedDate,
          reportedEps: row.reportedEps,
          estimatedEps: row.estimatedEps,
          surprise: row.surprise,
          surprisePct: row.surprisePct,
          reportTime: row.reportTime,
          sourceUrl: row.sourceUrl,
        }),
      });
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return { rows: rows.length, quarterly, annual };
};

export const ingestFilings = async (
  ticker: string,
  opts: { limit?: number; userAgent?: string; dbPath?: string },
) => {
  const normalizedTicker = normalizeTicker(ticker);
  const instrumentId = upsertInstrument({ ticker: normalizedTicker, dbPath: opts.dbPath });
  const cik = await resolveTickerToCik(normalizedTicker, fetch, opts.userAgent);
  upsertInstrument({ ticker: normalizedTicker, cik, dbPath: opts.dbPath });
  const filings = await fetchRecentFilings(cik, opts.userAgent, opts.limit);
  const db = openResearchDb(opts.dbPath);
  const insertFiling = db.prepare(`
    insert into filings (
      instrument_id,
      cik,
      accession,
      accession_raw,
      form,
      is_amendment,
      filed,
      accepted_at,
      period_end,
      as_of_date,
      title,
      url,
      source_url,
      source,
      text,
      filing_hash,
      fetched_at
    )
    values (
      @instrument_id,
      @cik,
      @accession,
      @accession_raw,
      @form,
      @is_amendment,
      @filed,
      @accepted_at,
      @period_end,
      @as_of_date,
      @title,
      @url,
      @source_url,
      @source,
      @text,
      @filing_hash,
      @fetched_at
    )
    on conflict(accession) do update set
      cik=excluded.cik,
      accession_raw=excluded.accession_raw,
      form=excluded.form,
      is_amendment=excluded.is_amendment,
      filed=excluded.filed,
      accepted_at=excluded.accepted_at,
      period_end=excluded.period_end,
      as_of_date=excluded.as_of_date,
      url=excluded.url,
      source_url=excluded.source_url,
      text=excluded.text,
      filing_hash=excluded.filing_hash,
      fetched_at=excluded.fetched_at
    returning id;
  `);
  const insertVersion = db.prepare(`
    insert into filing_versions (
      filing_id,
      instrument_id,
      cik,
      accession,
      accession_raw,
      form,
      is_amendment,
      filing_date,
      accepted_at,
      period_end,
      as_of_date,
      source_url,
      primary_doc,
      filing_hash,
      fetched_at
    )
    values (
      @filing_id,
      @instrument_id,
      @cik,
      @accession,
      @accession_raw,
      @form,
      @is_amendment,
      @filing_date,
      @accepted_at,
      @period_end,
      @as_of_date,
      @source_url,
      @primary_doc,
      @filing_hash,
      @fetched_at
    )
    on conflict(accession, accepted_at, filing_hash) do nothing;
  `);
  const insertChunk = db.prepare(`
    insert into chunks (source_table, ref_id, seq, text, metadata)
    values ('filings', @ref_id, @seq, @text, @metadata)
    on conflict(source_table, ref_id, seq) do update set
      text=excluded.text,
      metadata=excluded.metadata,
      pending_embedding=1;
  `);
  const pruneChunks = db.prepare(
    `delete from chunks where source_table='filings' and ref_id=? and seq >= ?`,
  );

  const results = [];
  for (const meta of filings) {
    const full = await downloadFilingText(meta, opts.userAgent).catch((err) => {
      results.push({ accession: meta.accession, ok: false, error: err.message });
      return null;
    });
    if (!full) continue;
    const filingHash = sha256(full.text);
    const chunks = chunkText(full.text, 256);
    db.exec("BEGIN");
    try {
      const info = insertFiling.get({
        instrument_id: instrumentId,
        cik,
        accession: meta.accession,
        accession_raw: meta.accessionRaw,
        form: meta.form,
        is_amendment: meta.isAmendment ? 1 : 0,
        filed: meta.filed,
        accepted_at: meta.acceptedAt ?? null,
        period_end: meta.periodEnd ?? null,
        as_of_date: meta.periodEnd ?? null,
        title: meta.title ?? null,
        url: meta.url,
        source_url: meta.sourceUrl,
        source: "sec",
        text: full.text,
        filing_hash: filingHash,
        fetched_at: Date.now(),
      }) as { id: number };
      insertVersion.run({
        filing_id: info.id,
        instrument_id: instrumentId,
        cik,
        accession: meta.accession,
        accession_raw: meta.accessionRaw,
        form: meta.form,
        is_amendment: meta.isAmendment ? 1 : 0,
        filing_date: meta.filed,
        accepted_at: meta.acceptedAt ?? "",
        period_end: meta.periodEnd ?? "",
        as_of_date: meta.periodEnd ?? "",
        source_url: meta.sourceUrl,
        primary_doc: meta.primaryDoc ?? "",
        filing_hash: filingHash,
        fetched_at: Date.now(),
      });
      for (const chunk of chunks) {
        insertChunk.run({
          ref_id: info.id,
          seq: chunk.seq,
          text: chunk.text,
          metadata: JSON.stringify({
            accession: meta.accession,
            accessionRaw: meta.accessionRaw,
            form: meta.form,
            filed: meta.filed,
            acceptedAt: meta.acceptedAt,
            periodEnd: meta.periodEnd,
            sourceUrl: meta.sourceUrl,
            filingHash,
          }),
        });
      }
      pruneChunks.run(info.id, chunks.length);
      db.exec("COMMIT");
      results.push({ accession: meta.accession, ok: true });
    } catch (err) {
      db.exec("ROLLBACK");
      throw err;
    }
    await politeThrottle();
  }
  return results;
};

export const ingestTranscript = async (
  ticker: string,
  url: string,
  opts: { dbPath?: string } = {},
) => {
  const instrumentId = upsertInstrument({ ticker: normalizeTicker(ticker), dbPath: opts.dbPath });
  const doc = await fetchTranscript(url);
  const db = openResearchDb(opts.dbPath);
  const insertTranscript = db.prepare(`
    insert into transcripts (instrument_id, event_date, event_type, source, url, title, speakers, content, fetched_at)
    values (@instrument_id, @event_date, @event_type, @source, @url, @title, @speakers, @content, @fetched_at)
    on conflict(url) do update set
      title=excluded.title,
      speakers=excluded.speakers,
      content=excluded.content,
      fetched_at=excluded.fetched_at
    returning id;
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
    on conflict(source_table, ref_id, seq) do update set
      text=excluded.text,
      metadata=excluded.metadata,
      pending_embedding=1;
  `);
  const pruneChunks = db.prepare(
    `delete from chunks where source_table='transcripts' and ref_id=? and seq >= ?`,
  );
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
    pruneChunks.run(row.id, chunks.length);
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
  return { chunks: chunks.length };
};

const recomputeFactRevisions = (
  db: ResearchDb,
  instrumentId: number,
  taxonomy: string,
  concept: string,
) => {
  db.prepare(
    `
      WITH ranked AS (
        SELECT
          id,
          ROW_NUMBER() OVER (
            PARTITION BY instrument_id, taxonomy, concept, unit, period_end, fiscal_year, fiscal_period, form, frame
            ORDER BY filing_date ASC, accepted_at ASC, accession_nodash ASC, id ASC
          ) AS revision_number,
          ROW_NUMBER() OVER (
            PARTITION BY instrument_id, taxonomy, concept, unit, period_end, fiscal_year, fiscal_period, form, frame
            ORDER BY filing_date DESC, accepted_at DESC, accession_nodash DESC, id DESC
          ) AS latest_rank
        FROM fundamental_facts
        WHERE instrument_id = @instrument_id
          AND taxonomy = @taxonomy
          AND concept = @concept
      )
      UPDATE fundamental_facts
      SET
        revision_number = (
          SELECT revision_number
          FROM ranked
          WHERE ranked.id = fundamental_facts.id
        ),
        is_latest = (
          SELECT CASE WHEN latest_rank = 1 THEN 1 ELSE 0 END
          FROM ranked
          WHERE ranked.id = fundamental_facts.id
        )
      WHERE id IN (SELECT id FROM ranked)
    `,
  ).run({
    instrument_id: instrumentId,
    taxonomy,
    concept,
  });
};

export const ingestFundamentals = async (
  ticker: string,
  opts: {
    userAgent?: string;
    includeForms?: string[];
    concepts?: string[];
    dbPath?: string;
  } = {},
) => {
  const normalizedTicker = normalizeTicker(ticker);
  const instrumentId = upsertInstrument({ ticker: normalizedTicker, dbPath: opts.dbPath });
  const cik = await resolveTickerToCik(normalizedTicker, fetch, opts.userAgent);
  upsertInstrument({ ticker: normalizedTicker, cik, dbPath: opts.dbPath });
  const companyFacts = await fetchCompanyFacts(cik, fetch, opts.userAgent);
  const observations = flattenCompanyFacts(companyFacts, {
    includeForms: opts.includeForms,
    concepts: opts.concepts,
  });
  const db = openResearchDb(opts.dbPath);
  const insertFact = db.prepare(`
    insert into fundamental_facts (
      instrument_id,
      ticker,
      cik,
      entity_name,
      taxonomy,
      concept,
      label,
      unit,
      value,
      as_of_date,
      period_start,
      period_end,
      filing_date,
      accepted_at,
      accession,
      accession_nodash,
      form,
      frame,
      fiscal_year,
      fiscal_period,
      source,
      source_url,
      fetched_at
    )
    values (
      @instrument_id,
      @ticker,
      @cik,
      @entity_name,
      @taxonomy,
      @concept,
      @label,
      @unit,
      @value,
      @as_of_date,
      @period_start,
      @period_end,
      @filing_date,
      @accepted_at,
      @accession,
      @accession_nodash,
      @form,
      @frame,
      @fiscal_year,
      @fiscal_period,
      @source,
      @source_url,
      @fetched_at
    )
    on conflict(
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
    ) do update set
      label=excluded.label,
      entity_name=excluded.entity_name,
      source=excluded.source,
      fetched_at=excluded.fetched_at
    returning id;
  `);
  const insertChunk = db.prepare(`
    insert into chunks (source_table, ref_id, seq, text, metadata)
    values ('fundamental_facts', @ref_id, 0, @text, @metadata)
    on conflict(source_table, ref_id, seq) do update set
      text=excluded.text,
      metadata=excluded.metadata,
      pending_embedding=1;
  `);
  const touchedConcepts = new Set<string>();
  const now = Date.now();

  db.exec("BEGIN");
  try {
    for (const obs of observations) {
      const row = insertFact.get({
        instrument_id: instrumentId,
        ticker: normalizedTicker,
        cik,
        entity_name: companyFacts.entityName?.trim() ?? null,
        taxonomy: obs.taxonomy,
        concept: obs.concept,
        label: obs.label,
        unit: obs.unit,
        value: obs.value,
        as_of_date: obs.asOfDate,
        period_start: "",
        period_end: obs.periodEnd,
        filing_date: obs.filingDate || "",
        accepted_at: obs.acceptedAt || "",
        accession: obs.accession || "",
        accession_nodash: obs.accessionNoDash || "",
        form: obs.form || "",
        frame: obs.frame || "",
        fiscal_year: obs.fiscalYear || 0,
        fiscal_period: obs.fiscalPeriod || "",
        source: "sec_companyfacts",
        source_url: obs.sourceUrl || "",
        fetched_at: now,
      }) as { id: number };
      const statement = [
        `${normalizedTicker} ${obs.taxonomy}:${obs.concept} (${obs.label})`,
        `value=${obs.value} ${obs.unit}`,
        `as_of=${obs.asOfDate}`,
        `period_end=${obs.periodEnd}`,
        `filed=${obs.filingDate || "unknown"}`,
        obs.form ? `form=${obs.form}` : "",
        obs.fiscalYear ? `fy=${obs.fiscalYear}` : "",
        obs.fiscalPeriod ? `fp=${obs.fiscalPeriod}` : "",
        obs.accessionNoDash ? `accession=${obs.accessionNoDash}` : "",
      ]
        .filter(Boolean)
        .join(" | ");
      insertChunk.run({
        ref_id: row.id,
        text: statement,
        metadata: JSON.stringify({
          ticker: normalizedTicker,
          cik,
          taxonomy: obs.taxonomy,
          concept: obs.concept,
          label: obs.label,
          unit: obs.unit,
          asOfDate: obs.asOfDate,
          periodEnd: obs.periodEnd,
          filingDate: obs.filingDate,
          form: obs.form,
          fiscalYear: obs.fiscalYear,
          fiscalPeriod: obs.fiscalPeriod,
          accession: obs.accession,
          sourceUrl: obs.sourceUrl,
        }),
      });
      touchedConcepts.add(`${obs.taxonomy}::${obs.concept}`);
    }

    for (const key of touchedConcepts) {
      const [taxonomy, concept] = key.split("::");
      if (!taxonomy || !concept) continue;
      recomputeFactRevisions(db, instrumentId, taxonomy, concept);
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }

  return {
    ticker: normalizedTicker,
    cik,
    observations: observations.length,
    conceptCount: touchedConcepts.size,
  };
};
