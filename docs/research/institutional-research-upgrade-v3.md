# Institutional Research Upgrade v3

## 1) Root Cause Analysis of Regression

### Primary causes
- Signal-to-noise inversion: the pipeline optimized for metric coverage, not for narrative synthesis.
- Missing explicit market-belief layer: outputs described data but did not separate consensus vs differentiated view.
- Exhibit interpretation gap: charts/metrics were emitted without investor-level interpretation and capital implications.
- Gate misalignment: cross-sectional gate scored quantitative readiness but did not enforce narrative quality, exhibit floor, or actionability.
- Output shape mismatch: command output favored telemetry-like key/value logs, which made reports non-investable and non-memo-like.

### Observable symptoms
- Reports read like API dumps rather than an IC brief.
- Theme/sector conclusions did not answer "so what" for portfolio construction.
- Repeated retries did not improve quality because quality dimensions were not explicitly scored.

## 2) Updated Agent Architecture (Passes + Responsibilities)

### Pass 0: Data Assembly
- Inputs: SEC/XBRL, transcripts, Massive, FRED, curated web/news.
- Output: normalized evidence packs and diagnostics.

### Pass 1: Theme Synthesis Pass
- Job: infer structural change and timing logic from evidence.
- Output: historical setup, inflection point, consensus framing, differentiated thesis.

### Pass 2: Market Belief Pass
- Job: map market-implied belief vs our belief.
- Output: explicit consensus map, underappreciated vectors, early-vs-wrong diagnosis.

### Pass 3: Capital Implications Pass
- Job: translate thesis into deployable exposure.
- Output: core/satellite framework, private/infrastructure/optionality implications, avoid list.

### Pass 4: Institutional Report Builder (Schema v3)
- Job: render A-J sections with a minimum of 8 exhibits and "Takeaway" lines.
- Output: markdown report suitable for IC review.

### Pass 5: Institutional Quality Gate v2
- Required checks:
  - narrative_clarity
  - exhibit_minimums
  - capital_actionability
  - evidence_freshness_180d
- Existing checks retained: evidence coverage, benchmark context, scenarios, calibration, contradiction handling.

### Pass 6: Retry / Regeneration Controller
- If strict mode and required checks fail, regenerate up to configured attempts.
- Retry reasons are explicit and logged for tuning.

## 3) Revised Prompts / Templates

### Theme Synthesis Prompt
"Identify the structural shift, why it is happening now vs 2-3 years ago, and where consensus is likely mis-specified. Focus on investor-relevant causal chains, not cataloguing metrics."

### Market Belief Prompt
"State what the market appears to believe, what is already priced, and where the market is early or wrong. Tie this to observable benchmark-relative behavior and implied expectation diagnostics."

### Capital Implications Prompt
"Convert thesis into deployable exposure today: core/satellite public equities, private implications, infrastructure picks-and-shovels, optionality trades, and explicit avoids. Include sizing discipline and risk asymmetry logic."

### Exhibit Prompt
"Every exhibit must answer one question, show real data, and end with a one-sentence takeaway that changes allocation behavior."

## 4) Sample Report
- See `docs/research/sample-thematic-report-v3.md`.

## 5) Regression Checklist

### Pre-deliverable checklist
- Narrative answers all six core investor questions.
- At least 8 exhibits are present, each with a "Takeaway".
- Consensus vs differentiated view is explicit.
- Capital allocation section is actionable and specific.
- At least 60% of dated evidence is <= 180 days old or confidence is explicitly downgraded.
- No internal logs/telemetry/debug content appears in report body.

### Gate checklist
- narrative_clarity passes.
- exhibit_minimums passes.
- capital_actionability passes.
- evidence_freshness_180d passes.
- Required failures list is empty in strict mode.

### Post-run checklist
- Save report + gate run id.
- Review failed checks, not just aggregate score.
- Use retry reason to tune synthesis and capital implication logic before next run.
