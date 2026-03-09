# Sprint 10: Eval Harness

## Scope

Sprint 10 adds a deterministic research eval harness on top of the existing `eval_runs` table.

## Deliverables

- JSON task-set format for named eval suites
- Thresholded gate evaluation:
  - minimum score
  - maximum failed checks
- Task runner for:
  - retrieval tasks
  - structured report tasks
  - watchlist brief tasks
- CLI commands:
  - `openclaw research eval-harness --taskset <path>`
  - `openclaw research eval-harness-report`
- Markdown scorecard rendering
- Nightly runner:
  - `scripts/research-eval-nightly.ts`
- Example task set:
  - `eval/research-harness.example.json`

## Why this matters

The earlier eval commands mostly checked system-level metrics. This harness evaluates real user-facing outputs:

- can retrieval return enough cited evidence
- does the ticker memo have the expected sections and minimum quality
- does the daily brief contain actionable changes

That makes prompt/model/workflow regressions measurable before rollout.

The harness now also supports release gating instead of just observation. A task set can define score thresholds, and the CLI/nightly runner can fail the run if those thresholds are not met.
