import fs from "node:fs";
import path from "node:path";
import {
  loadResearchEvalTaskSet,
  renderResearchEvalScorecard,
  runResearchEvalTaskSet,
} from "../src/research/eval-harness.js";
import { resolveResearchDbPath } from "../src/research/db.js";

const taskSetPath = process.env.OPENCLAW_RESEARCH_EVAL_TASKSET?.trim()
  ? path.resolve(process.env.OPENCLAW_RESEARCH_EVAL_TASKSET.trim())
  : path.resolve(process.cwd(), "eval", "research-harness.example.json");

const dbPath = resolveResearchDbPath(process.env.OPENCLAW_RESEARCH_DB_PATH);
const outDir = process.env.OPENCLAW_RESEARCH_EVAL_OUT_DIR?.trim()
  ? path.resolve(process.env.OPENCLAW_RESEARCH_EVAL_OUT_DIR.trim())
  : path.resolve(process.cwd(), "data", "research-eval");

const run = async () => {
  const taskSet = loadResearchEvalTaskSet(taskSetPath);
  const result = await runResearchEvalTaskSet({ taskSet, dbPath });
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  fs.mkdirSync(outDir, { recursive: true });
  const scorecardPath = path.join(outDir, `${taskSet.name}-${ts}.md`);
  const jsonPath = path.join(outDir, `${taskSet.name}-${ts}.json`);
  fs.writeFileSync(scorecardPath, `${renderResearchEvalScorecard(result)}\n`, "utf8");
  fs.writeFileSync(jsonPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");

  console.log(
    `research_eval_nightly taskset=${result.taskSetName} score=${(result.score * 100).toFixed(1)}% passed=${result.passed}/${result.total} gate=${result.passedGate ? 1 : 0} scorecard=${scorecardPath} json=${jsonPath}`,
  );

  if (!result.passedGate) {
    throw new Error(`research_eval_gate_failed reasons=${result.reasons.join("; ")}`);
  }
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
