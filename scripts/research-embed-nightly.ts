import { syncEmbeddings } from "../src/research/vector-search.js";

const run = async () => {
  const res = await syncEmbeddings(process.env.RESEARCH_DB_PATH);
  console.log(`Embedded chunks=${res.chunkCount}, repo_chunks=${res.repoCount}`);
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
