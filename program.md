---
protocol: research-autoresearch-v1
name: drmackenzie-autoresearch
description: Bounded mutate-run-score-keep loop for investor research quality.
taskset: eval/research-harness.example.json
profile: eval/research-improvement-profile.example.json
attempts: 8
min-improvement: 0.005
seed: drmackenzie-autoresearch
write-best: true
require-pass: true
out: eval/runs/drmackenzie-autoresearch-latest.md
---

# DrMackenzie Autoresearch Program

This repo uses an `autoresearch`-style protocol for safe research improvement.

## Objective

Improve investor research quality without letting the agent rewrite arbitrary logic,
change execution policy, or silently degrade trust.

## Allowed Mutation Surface

- `eval/research-improvement-profile*.json`
- bounded retrieval and summarization knobs surfaced through the improvement profile

## Forbidden Mutation Surface

- secret handling
- tool governance and approval rules
- external action policy
- job queue semantics
- citation and provenance requirements
- any code path that can trade safety for score

## Loop

1. Load the task set and the current improvement profile.
2. Mutate only whitelisted numeric knobs.
3. Run the research eval harness on the candidate.
4. Keep the candidate only if it passes the gate and beats the current best
   under the keep rules.
5. Revert otherwise.
6. Write the winning profile back only when the protocol allows it.

## Keep Rules

- Candidate must pass the eval gate.
- Candidate must improve score by at least the configured minimum delta.
- If score is flat, candidate may still win by reducing failed checks.
- If score and failed checks are flat, candidate may still win by increasing passed checks.

## Revert Rules

- Revert any candidate that fails the eval gate.
- Revert any candidate that does not beat the current best candidate.
- If nothing beats baseline, restore the original profile.

## Output Requirements

- Always produce a markdown run report.
- Record baseline vs best.
- Record every mutation, decision, and reason.
- Never claim improvement without an eval-backed score change or better gate result.
