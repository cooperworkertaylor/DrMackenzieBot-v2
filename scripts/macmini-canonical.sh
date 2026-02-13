#!/usr/bin/env bash
set -euo pipefail

REPO_DEFAULT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="${OPENCLAW_CANONICAL_REPO:-$REPO_DEFAULT_ROOT}"
CANONICAL_SECRETS_FILE="$REPO_ROOT/.env.1password"
ALLOWED_USER="${OPENCLAW_REQUIRED_USER:-agent}"
FORBIDDEN_PATH_TOKEN="${OPENCLAW_FORBIDDEN_PATH_TOKEN:-/cooptaylor1/}"
FORBIDDEN_HOSTNAME="${OPENCLAW_FORBIDDEN_HOSTNAME:-}"
FIND_ROOTS=("/Users/agent" "/Users/agent/clawd" "/Users/cooptaylor1" "/Users/cooptaylor1/Documents")

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

find_candidate_repos() {
  echo "Checking for candidate repo locations..."
  for base in "${FIND_ROOTS[@]}"; do
    if [[ -d "$base" ]]; then
      while IFS= read -r candidate; do
        echo "  $candidate"
      done < <(find "$base" -maxdepth 5 -type d -name "DrMackenzieBot-v2" 2>/dev/null || true)
    fi
  done
}

check_repo() {
  if [[ "${USER}" != "$ALLOWED_USER" ]]; then
    echo "BLOCKED: expected user '$ALLOWED_USER', current user is '$USER'." >&2
    echo "Set OPENCLAW_REQUIRED_USER only if running from a different account." >&2
    exit 1
  fi

  if [[ "$REPO_ROOT" == *"$FORBIDDEN_PATH_TOKEN"* ]]; then
    echo "BLOCKED: repo path contains forbidden segment: $FORBIDDEN_PATH_TOKEN" >&2
    echo "Set OPENCLAW_CANONICAL_REPO to an allowed path for this machine." >&2
    echo "Candidates:"
    find_candidate_repos
    exit 1
  fi

  if [[ ! -d "$REPO_ROOT" ]]; then
    echo "ERROR: repository root not found: $REPO_ROOT" >&2
    echo "Candidates:"
    find_candidate_repos
    exit 1
  fi

  if ! cd "$REPO_ROOT"; then
    echo "ERROR: unable to enter repo root: $REPO_ROOT" >&2
    exit 1
  fi

  if [[ -n "${FORBIDDEN_HOSTNAME}" && "$(hostname)" == "$FORBIDDEN_HOSTNAME" ]]; then
    echo "BLOCKED: forbidden host '$FORBIDDEN_HOSTNAME'." >&2
    exit 1
  fi

  if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "ERROR: not a git repo: $REPO_ROOT" >&2
    exit 1
  fi
}

run_node_cmd() {
  if [[ -f "$REPO_ROOT/scripts/run-node.mjs" ]]; then
    echo "node scripts/run-node.mjs"
    return
  fi

  if [[ -f "$REPO_ROOT/dist/index.js" ]]; then
    echo "node dist/index.js"
    return
  fi

  echo "Could not find scripts/run-node.mjs or dist/index.js under $REPO_ROOT"
  echo "Run: pnpm build"
  exit 1
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
      run_node="$(run_node_cmd)"
      ./scripts/op-run.sh $run_node status
      ;;
    start-gateway)
      check_secrets_file
      run_node="$(run_node_cmd)"
      ./scripts/op-run.sh $run_node gateway
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
