# Research Autoresearch Protocol

This repo now carries a first-class `program.md` modeled on the useful parts of
Karpathy's `autoresearch`: a persistent operating contract that agents can read
and a deterministic runner that executes the contract against the existing
research eval harness.

## Why

The repo already had the hard runtime pieces:

- eval harness
- constrained self-improvement loop
- mutable improvement profile
- keep/revert rules

What was missing was the protocol layer that makes the loop reusable by agents
without relying on ad hoc CLI invocations or prompt memory.

## Files

- `program.md`
  - canonical protocol file for research self-improvement
- `src/research/autoresearch-protocol.ts`
  - frontmatter parser and protocol runner
- `src/research/autoresearch-protocol.test.ts`
  - protocol-level regression coverage

## CLI

Run the program directly:

```bash
pnpm openclaw research autoresearch --program program.md
```

Useful overrides:

```bash
pnpm openclaw research autoresearch \
  --program program.md \
  --attempts 12 \
  --min-improvement 0.003 \
  --db "$HOME/.openclaw/research/research.db"
```

## Protocol Constraints

The protocol is intentionally narrow:

- only mutate whitelisted numeric knobs
- never mutate secrets, governance, or execution safety rules
- require eval-gate pass before keeping a candidate
- restore baseline if no candidate wins

This makes the loop useful for research quality optimization without turning the
investor workflow into an unstable self-modifying system.
