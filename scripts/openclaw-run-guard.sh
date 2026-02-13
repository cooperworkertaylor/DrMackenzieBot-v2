#!/usr/bin/env bash
set -euo pipefail

# Stable, repository-local wrapper. Keep this out of /tmp so it travels with the repo.
# Usage:
#   ./scripts/openclaw-run-guard.sh <command>

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ $# -eq 0 ]]; then
  cat <<USAGE
Usage: ./scripts/openclaw-run-guard.sh <command>

Examples:
  ./scripts/openclaw-run-guard.sh env
  ./scripts/openclaw-run-guard.sh status
  ./scripts/openclaw-run-guard.sh secrets
  ./scripts/openclaw-run-guard.sh start-gateway
  ./scripts/openclaw-run-guard.sh stop-gateway
USAGE
  exit 1
fi

"$SCRIPT_DIR/macmini-canonical.sh" "$@"
