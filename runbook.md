# Tri-Lane Workflow Runbook

## Purpose
Repeatable tri-lane workflow with durable memory capture.

## Start a new task (per repo)
1) Templates:
- mkdir -p .openclaw
- cp ~/.openclaw/workflow-packs/tri-lane/* .openclaw/

2) Reset capture:
- cp .openclaw/memory/memory_capture.template.json .openclaw/memory/memory_capture.json

3) Lane A outputs first:
- .openclaw/task_contract.json
- .openclaw/brief.json
- .openclaw/memory/memory_capture.json (at least one entry)

## Lanes
Lane A: scope + assumptions/risks/decisions; no implementation.
Lane B: implement exactly the brief; checkpoint every ~10 minutes.
Lane C: QA + ship/no-ship + promote memory per policy.
