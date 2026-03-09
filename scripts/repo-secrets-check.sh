#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Checking required secrets via repo single source: $ROOT_DIR/.env.1password"

./scripts/op-run.sh node -e '
const required = [
  "OPENAI_API_KEY",
  "ANTHROPIC_API_KEY",
  "GEMINI_API_KEY",
  "BRAVE_API_KEY",
  "MASSIVE_API_KEY",
  "POLYGON_API_KEY",
  "FRED_API_KEY",
  "SEC_USER_AGENT",
  "ALPHAVANTAGE_API_KEY"
];

const optional = [
  "OPENCLAW_RESEARCH_SUBSTACK_COOKIE",
  "OPENCLAW_RESEARCH_STRATECHERY_COOKIE",
  "OPENCLAW_RESEARCH_DIFF_COOKIE",
  "OPENCLAW_RESEARCH_SEMIANALYSIS_COOKIE",
  "TELEGRAM_BOT_TOKEN",
  "GOOGLE_API_KEY",
];

const missingRequired = [];
for (const key of required) {
  const value = (process.env[key] ?? "").trim();
  if (!value) {
    missingRequired.push(key);
  }
}

const missingOptional = [];
for (const key of optional) {
  const value = (process.env[key] ?? "").trim();
  if (!value) {
    missingOptional.push(key);
  }
}

console.log(`checked=${required.length + optional.length}`);
if (missingRequired.length === 0) {
  console.log("status=required:ok");
} else {
  console.log(`status=required:missing ${missingRequired.length}`);
}

for (const key of missingRequired) {
  console.log(`MISSING_REQUIRED ${key}`);
}

if (missingOptional.length > 0) {
  console.log(`status=optional:missing ${missingOptional.length}`);
  for (const key of missingOptional) {
    console.log(`MISSING_OPTIONAL ${key}`);
  }
}

if (missingRequired.length === 0) {
  if (missingOptional.length > 0) {
    console.log("status=ok-with-optional-missing");
  } else {
    console.log("status=ok");
  }
  process.exit(0);
}
process.exit(1);
'
