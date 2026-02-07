# Institutional Research Platform v3 Design

## 1) Root Cause Analysis

Quality regressed for three concrete reasons:
- Prompt drift and template overfitting: report writers produced generic prose regardless of source quality.
- Weak pre-render controls: the old flow could pass/fail on scalar metrics while still emitting low-narrative output.
- Output contamination: gate/telemetry diagnostics were appended directly into memo bodies, degrading deliverable quality.

## 2) New Architecture

The updated architecture enforces institutional output quality at the final rendering boundary.

Pipeline:
1. Data collection and retrieval.
2. Synthesis passes (story, market belief, capital implications) using source-backed content.
3. Draft markdown assembly in schema v3.
4. **Institutional Output Gate v3** (`src/research/institutional-output-gate.ts`) before final emission.
5. Deterministic targeted repair passes if gate fails:
   - Narrative repair pass
   - Freshness justification pass
   - Exhibit repair pass
   - Actionability repair pass
   - Risks/falsifiers repair pass
6. Re-score and only return final output if passing threshold and hard-fail checks.

## 3) What Changed in Code

- Added corpus/rubric/prompt assets under `research-corpus/`.
- Added ingest utility: `scripts/research-corpus-ingest.mjs`.
- Added output gate engine: `src/research/institutional-output-gate.ts`.
- Integrated gate into memo rendering (`src/research/memo.ts`) and theme/sector rendering (`src/research/theme-sector-report.ts`).
- Updated CLI sector/theme flow to prefer v3 output-gate evaluation when present (`src/cli/research-cli.ts`).
- Added regression tests around schema/exhibit/takeaway/freshness/debug-leak constraints.

## 4) Regression Protection

Tests now enforce:
- Required schema section coverage.
- Exhibit minimums (theme/sector >= 8, single-name >= 6).
- `Takeaway:` present for each exhibit.
- Debug/telemetry leakage blocked.
- Freshness logic and justification behavior.

## 5) Adding New Exemplars

1. Add URLs to `research-corpus/annotations/seed_urls.txt`.
2. Run:
   - `node scripts/research-corpus-ingest.mjs`
3. Review outputs:
   - `research-corpus/snapshots/<id>/metadata.json`
   - `research-corpus/annotations/gold_standard.jsonl`
   - `research-corpus/annotations/failures.json`
4. Expand prompt patterns and rubric checks if new writing styles expose gaps.

## 6) Operational Notes

- If network is unavailable, ingestion records explicit failures and still writes placeholder annotations.
- Deliverables no longer include runtime telemetry in the memo body.
- Quality remains fail-closed by default when enforced.
