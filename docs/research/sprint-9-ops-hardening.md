# Sprint 9: Ops Hardening

## Scope

Sprint 9 makes the research stack runnable as an unattended Mac mini service instead of a set of manually-invoked commands.

## Deliverables

- Dedicated research worker entrypoint:
  - `openclaw research worker`
- Dedicated research scheduler entrypoint:
  - `openclaw research scheduler`
- One-shot scheduler pass for validation:
  - `openclaw research scheduler-pass`
- Aggregated research health snapshot:
  - `openclaw research health`
- Research runtime backup:
  - `openclaw research backup`
- Failed quickrun replay:
  - `openclaw research replay-failed`

## What changed

- Quickrun jobs now support listing and failed-job replay.
- Quick research worker startup is now exported as a first-class lifecycle entrypoint instead of only being created lazily on message ingress.
- A new research ops layer handles:
  - health snapshots
  - backup packaging
  - failed quickrun replay
  - daily brief generation for default watchlists
  - refresh-queue completion when a matching structured report exists
- The research CLI now exposes Mac mini runtime commands for worker and scheduler loops.

## Operational intent

On the Mac mini, the target runtime is:

- one Telegram/gateway process
- one `openclaw research worker` process
- one `openclaw research scheduler` process

This keeps the heavy research paths out of the ingress process while preserving the current SQLite-backed runtime model.
