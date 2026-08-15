#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
version=$(awk '
  /^\[workspace\.package\]$/ { in_package = 1; next }
  /^\[/ { in_package = 0 }
  in_package && /^version = "/ {
    value = $0
    sub(/^version = "/, "", value)
    sub(/"$/, "", value)
    print value
    exit
  }
' "$root/Cargo.toml")

if [[ -z "$version" ]]; then
  printf 'workspace package version not found\n' >&2
  exit 1
fi

tag=${1:-}
if [[ -n "$tag" && "$tag" != "v$version" ]]; then
  printf 'tag %s does not match Cargo version v%s\n' "$tag" "$version" >&2
  exit 1
fi

printf '%s\n' "$version"
