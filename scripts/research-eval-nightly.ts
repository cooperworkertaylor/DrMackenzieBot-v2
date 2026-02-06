import path from "node:path";
import { runFinanceEval, runCodingEval } from "../src/research/eval.js";

const run = async () => {
  const finance = await runFinanceEval();
  console.log(`finance score=${(finance.score * 100).toFixed(1)}% (${finance.passed}/${finance.total})`);
  const repoRoot = process.env.RESEARCH_REPO_ROOT
    ? path.resolve(process.env.RESEARCH_REPO_ROOT)
    : process.cwd();
  const coding = await runCodingEval(repoRoot);
  console.log(`coding score=${(coding.score * 100).toFixed(1)}% (${coding.passed}/${coding.total})`);
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
