#!/usr/bin/env bash
set -euo pipefail

revision=761b726dd34fb83930e26aab4e9ac3899aa1fa78
destination=${1:-/opt/agentd/models/multilingual-e5-small}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
license_source=${2:-"$script_dir/../licenses/multilingual-e5-small.LICENSE"}
base="https://huggingface.co/Xenova/multilingual-e5-small/resolve/$revision"

mkdir -p "$destination/onnx"
if [[ ! -f "$license_source" ]]; then
  printf 'model license not found: %s\n' "$license_source" >&2
  exit 1
fi

if command -v shasum >/dev/null 2>&1; then
  hash=(shasum -a 256)
  checksum=(shasum -a 256 -c)
elif command -v sha256sum >/dev/null 2>&1; then
  hash=(sha256sum)
  checksum=(sha256sum -c)
else
  printf 'shasum or sha256sum is required\n' >&2
  exit 1
fi

fetch() {
  relative=$1
  expected=$2
  target="$destination/$relative"
  if [[ -f "$target" ]] && [[ $("${hash[@]}" "$target" | awk '{print $1}') == "$expected" ]]; then
    printf '%s: cached and verified\n' "$relative"
    return
  fi
  curl --fail --location --retry 3 --retry-delay 2 --silent --show-error \
    --output "$target.download" "$base/$relative?download=true"
  actual=$("${hash[@]}" "$target.download" | awk '{print $1}')
  if [[ "$actual" != "$expected" ]]; then
    printf '%s: checksum mismatch\n' "$relative" >&2
    exit 1
  fi
  mv "$target.download" "$target"
}

fetch config.json cb99455288675345e1a4f411438d5d0adbba5fbd3a67ea4fb03c015433b996c1
fetch onnx/model_int8.onnx 4d24e2bc01a447951524466ef533e52944bf48509e6552810bcee1a2711cb02c
fetch special_tokens_map.json d05497f1da52c5e09554c0cd874037a083e1dc1b9cfd48034d1c717f1afc07a7
fetch tokenizer.json 0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39
fetch tokenizer_config.json a1d6bc8734a6f635dc158508bef000f8e2e5a759c7d92f984b2c86e5ff53425b
cp "$license_source" "$destination/LICENSE"

cd "$destination"
"${checksum[@]}" <<'EOF'
cb99455288675345e1a4f411438d5d0adbba5fbd3a67ea4fb03c015433b996c1  config.json
4d24e2bc01a447951524466ef533e52944bf48509e6552810bcee1a2711cb02c  onnx/model_int8.onnx
d05497f1da52c5e09554c0cd874037a083e1dc1b9cfd48034d1c717f1afc07a7  special_tokens_map.json
0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39  tokenizer.json
a1d6bc8734a6f635dc158508bef000f8e2e5a759c7d92f984b2c86e5ff53425b  tokenizer_config.json
b5cb7ff7859e7d282d98fc43ab081b0dda5dc659dc3385cf65740c759a7e6a6c  LICENSE
EOF
