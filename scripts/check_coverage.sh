#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov is required but not installed" >&2
  exit 1
fi

THRESHOLD="${1:-85}"
PACKAGES=(
  "agentd-store"
  "agentd-core"
  "agentd"
  "agentd-api"
)

for pkg in "${PACKAGES[@]}"; do
  echo "==> coverage for ${pkg} (threshold ${THRESHOLD}%)"
  cargo llvm-cov \
    --package "${pkg}" \
    --fail-under-lines "${THRESHOLD}" \
    --summary-only
done
