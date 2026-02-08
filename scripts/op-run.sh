#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${OPENCLAW_OP_ENV_FILE:-$ROOT_DIR/.env.1password}"

if ! command -v op >/dev/null 2>&1; then
  echo "1Password CLI (op) not found. Install it first."
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Env reference file not found: $ENV_FILE"
  echo "Copy $ROOT_DIR/config/op-env.example to $ENV_FILE and update op:// references."
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

exec op run --env-file "$ENV_FILE" -- "$@"
