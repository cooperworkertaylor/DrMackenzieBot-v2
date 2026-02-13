#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CANONICAL_SECRETS_FILE="$REPO_ROOT/.env.1password"

usage() {
  cat <<USAGE
Usage: ./scripts/macmini-canonical.sh <command>

Commands:
  env              Show canonical paths and required checks
  secrets          Validate required secrets via 1Password-backed env
  status           Show OpenClaw status via repo scoped runtime
  start-gateway    Start gateway via 1Password-backed runtime
  stop-gateway     Stop gateway

This wrapper is pinned to this repo only:
  $REPO_ROOT
USAGE
}

check_repo() {
  if ! cd "$REPO_ROOT"; then
    echo "ERROR: unable to enter repo root: $REPO_ROOT" >&2
    exit 1
  fi

  if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "ERROR: not a git repo: $REPO_ROOT" >&2
    exit 1
  fi
}

check_secrets_file() {
  if [[ ! -f "$CANONICAL_SECRETS_FILE" ]]; then
    echo "Missing: $CANONICAL_SECRETS_FILE" >&2
    echo "Create it with: cp $REPO_ROOT/config/op-env.example .env.1password" >&2
    exit 1
  fi
}

main() {
  check_repo

  case "${1:-}" in
    env)
      echo "REPO_ROOT=$REPO_ROOT"
      echo "SECRETS_FILE=$CANONICAL_SECRETS_FILE"
      echo "SECRETS_EXISTS=$(test -f "$CANONICAL_SECRETS_FILE" && echo yes || echo no)"
      echo "Openclaw binary: $(command -v openclaw || echo none)"
      echo "1Password signed in: $(op whoami >/dev/null 2>&1 && echo yes || echo no)"
      ;;
    secrets)
      check_secrets_file
      ./scripts/repo-secrets-check.sh
      ;;
    status)
      ./scripts/op-run.sh node scripts/run-node.mjs status
      ;;
    start-gateway)
      check_secrets_file
      ./scripts/op-run.sh node scripts/run-node.mjs gateway
      ;;
    stop-gateway)
      openclaw gateway stop
      ;;
    "")
      usage
      exit 1
      ;;
    *)
      usage
      echo "\nERROR: unknown command '$1'" >&2
      exit 1
      ;;
  esac
}

main "$@"
