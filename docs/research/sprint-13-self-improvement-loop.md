# Sprint 13: Constrained Self-Improvement Loop

This sprint adds a bounded optimizer around the research eval harness.

## Scope

The loop only mutates a whitelisted JSON profile. It does not rewrite arbitrary code, prompts, or secret/tool policy.

Mutable surface:

- report build knobs
  - `lookbackDays`
  - `maxSources`
  - `maxClaims`
  - `maxEvents`
  - `maxFacts`
- watchlist brief lookback

Profile example:

- `eval/research-improvement-profile.example.json`

## CLI

- `openclaw research eval-self-improve --taskset <path> --profile <path>`

Key options:

- `--attempts <n>`
- `--min-improvement <n>`
- `--seed <text>`
- `--out <path>`
- `--json`
- `--no-write-best`
- `--require-pass`

## Keep / Revert Rules

Keep:

- candidate must pass the eval gate
- candidate must improve score by the configured minimum delta
- or hold score while reducing failed checks
- or hold score and failed checks while increasing passed checks

Revert:

- any candidate that fails the eval gate
- any candidate that does not beat the current best result
- if no candidate improves on baseline, restore the original profile file

## Why this is useful

This borrows the useful part of a self-improving agent architecture without introducing uncontrolled autonomy:

- one bounded mutable surface
- one explicit metric
- deterministic keep/discard rules
- durable written profile only when the candidate genuinely wins

## Verification

- `./node_modules/.bin/tsc -p tsconfig.json --noEmit`
- `./node_modules/.bin/vitest run src/research/eval-self-improve.test.ts src/research/eval-harness.test.ts src/research/external-research-personalization.test.ts src/research/external-research-advanced.test.ts src/v2/llm/complete-json.test.ts src/research/external-research-report.test.ts src/research/external-research-thesis.test.ts src/research/external-research.test.ts src/cli/research-cli.test.ts`
