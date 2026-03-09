# Mac Mini Operations

## Required env

- `OPENCLAW_HOST_ROLE=macmini`
- `OPENCLAW_ALLOW_BROWSER=1`
- `OPENCLAW_STATE_DIR=/opt/drmackenziebot/state`
- `OPENCLAW_RESEARCH_DB_PATH=/opt/drmackenziebot/state/research/research.db`
- `OPENCLAW_CHROME_PATH=/Applications/Google Chrome.app/Contents/MacOS/Google Chrome`

Telegram, model, and API secrets should stay in the existing env / 1Password reference flow.

## Processes

Run these three long-lived processes on the Mac mini:

1. Gateway / Telegram ingress
2. `openclaw research worker`
3. `openclaw research scheduler`

The worker handles durable quick research jobs. The scheduler handles daily brief generation and refresh-queue completion.

## Commands

Health:

```bash
openclaw research health --json
```

One-shot scheduler validation:

```bash
openclaw research scheduler-pass
```

Run the worker:

```bash
openclaw research worker
```

Run the scheduler:

```bash
openclaw research scheduler --interval-ms 300000
```

Replay failed quickrun jobs:

```bash
openclaw research replay-failed
```

Create a backup:

```bash
openclaw research backup --dest /opt/drmackenziebot/backups
```

## launchd shape

Use separate launchd units for:

- gateway
- research worker
- research scheduler

Each should:

- restart on crash
- write stdout/stderr to the state log directory
- load the same env file
- start on boot

## Verification

After deployment:

1. Run `openclaw research health --json`
2. Confirm `quickrun.failed=0`
3. Confirm `latest_brief` is current after the scheduler runs
4. Trigger one Telegram quick research request
5. Confirm the worker drains the queue
6. Create one backup and verify the copied `research.db` plus `raw-artifacts`
