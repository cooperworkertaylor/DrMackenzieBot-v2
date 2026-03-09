#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env.1password"
RESOLVED_ENV_FILE="$ROOT_DIR/.env.resolved.sh"

if [[ -n "${OPENCLAW_OP_ENV_FILE:-}" && "${OPENCLAW_OP_ENV_FILE}" != "$ENV_FILE" ]]; then
  echo "Single-secret-source mode: using repo-scoped file: $ENV_FILE"
  echo "Ignoring OPENCLAW_OP_ENV_FILE override: ${OPENCLAW_OP_ENV_FILE}"
fi

if ! command -v op >/dev/null 2>&1; then
  echo "1Password CLI (op) not found. Install it first."
  exit 1
fi

if ! op whoami >/dev/null 2>&1; then
  echo "1Password CLI is not signed in."
  echo "Run: eval \"\$(op signin --account my.1password.com)\""
  exit 1
fi

if [[ "$#" -eq 0 ]]; then
  echo "Usage: scripts/op-run.sh <command> [args...]"
  exit 1
fi

node "$ROOT_DIR/scripts/sync-op-env.mjs" >/dev/null

if [[ ! -f "$RESOLVED_ENV_FILE" ]]; then
  echo "Resolved env file not found: $RESOLVED_ENV_FILE"
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$RESOLVED_ENV_FILE"
set +a

exec "$@"
