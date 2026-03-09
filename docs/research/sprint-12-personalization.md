# Sprint 12: Personalization and Compounding Memory

This sprint adds explicit user preferences, notebook ingestion, and a personalized ticker snapshot on top of the existing external research graph.

## Added

- Preference storage:
  - `research_user_preferences`
  - CLI:
    - `openclaw research prefs-set --key <text> --value <text>`
    - `openclaw research prefs-list`
- Notebook storage and ingestion:
  - `research_notebook_entries`
  - CLI:
    - `openclaw research notebook-add --title <text> --content <text> --ticker <symbol>`
    - `openclaw research notebook-list [--ticker <symbol>]`
  - notebook entries are ingested into `external_documents`, extracted, and allowed to update reports/theses
- Personalized ticker snapshot:
  - CLI:
    - `openclaw research personalized --ticker <symbol>`
  - merges:
    - current report/thesis state
    - stored preferences
    - recent ticker-linked notebook entries

## Why it matters

This is the compounding loop for a single operator:

- your own notes become first-class evidence
- thesis updates absorb notebook evidence instead of ignoring it
- preference state is durable and queryable
- the system can render a ticker view through your stored lens

## Verification

- `./node_modules/.bin/tsc -p tsconfig.json --noEmit`
- `./node_modules/.bin/vitest run src/research/external-research-personalization.test.ts src/research/external-research-advanced.test.ts src/v2/llm/complete-json.test.ts src/research/external-research-report.test.ts src/research/external-research-thesis.test.ts src/research/external-research.test.ts src/cli/research-cli.test.ts`
