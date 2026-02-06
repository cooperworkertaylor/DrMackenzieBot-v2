import { ingestFilings, ingestPrices } from "../src/research/ingest.js";
import { openResearchDb } from "../src/research/db.js";

const run = async () => {
  const tickersRaw = process.env.RESEARCH_TICKERS || "";
  const tickers = tickersRaw
    .split(",")
    .map((t) => t.trim().toUpperCase())
    .filter(Boolean);
  if (tickers.length === 0) {
    console.error("Set RESEARCH_TICKERS=ticker1,ticker2");
    process.exit(1);
  }
  openResearchDb(); // ensure migrations
  for (const ticker of tickers) {
    console.log(`=== ${ticker} ===`);
    try {
      const priceCount = await ingestPrices(ticker);
      console.log(`prices: ${priceCount} rows`);
    } catch (err) {
      console.error(`prices failed: ${String(err)}`);
    }
    try {
      const filings = await ingestFilings(ticker, { limit: 20, userAgent: process.env.SEC_USER_AGENT });
      const ok = filings.filter((f) => f.ok).length;
      const failed = filings.length - ok;
      console.log(`filings: ${ok} ingested, ${failed} failed`);
    } catch (err) {
      console.error(`filings failed: ${String(err)}`);
    }
  }
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
