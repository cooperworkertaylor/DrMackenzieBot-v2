# Sprint 11: Analyst Differentiators

This sprint adds two analyst-facing workflows on top of the external research evidence graph and sets the research v2 base model to `openai/gpt-5.4`.

## Added

- Research v2 default model:
  - `resolveResearchV2ModelRef` now defaults to `openai/gpt-5.4`
  - GPT-5 family fallback resolution now keeps a stable downgrade path to `openai/gpt-5.2` and `openai-codex/gpt-5.2-codex`
- Source conflict detection:
  - new module: `src/research/external-research-advanced.ts`
  - CLI: `openclaw research source-conflicts --ticker <symbol>`
  - detects:
    - conflicting extracted facts for the same metric within a short date window
    - conflicting claim polarity across external sources on the same topic
- Peer comparison:
  - CLI: `openclaw research compare-peers --left <symbol> --right <symbol>`
  - compares the latest stored report + thesis for both tickers
  - highlights evidence edge, risk edge, monitoring burden, and next actions

## Why it matters

The research stack now does more than summarize a single ticker. It can:

- show when sources materially disagree
- compare two names on evidence quality and thesis drift
- keep research-model defaults aligned with the intended OpenAI base model

## Verification

- `./node_modules/.bin/tsc -p tsconfig.json --noEmit`
- `./node_modules/.bin/vitest run src/research/external-research-advanced.test.ts src/v2/llm/complete-json.test.ts src/research/external-research-report.test.ts src/research/external-research-thesis.test.ts src/research/external-research.test.ts src/cli/research-cli.test.ts`
