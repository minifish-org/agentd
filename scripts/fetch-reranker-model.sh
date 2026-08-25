#!/usr/bin/env bash
set -euo pipefail

revision=6f5ff65298512715a1e669753bc754d2bc8f367b
destination=${1:-/opt/agentd/models/bge-reranker-v2-m3}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
license_source=${2:-"$script_dir/../LICENSE"}
base="https://huggingface.co/onnx-community/bge-reranker-v2-m3-ONNX/resolve/$revision"

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

fetch config.json 122e922dcfed6503c8721e6fe1daf090340c3d95ca7f3aa3a72730b321a51cfd
fetch onnx/model_int8.onnx 912fc1215c2dbff6499700534bd8d31253af01573861abbfc43afd1fab6cce5d
fetch special_tokens_map.json 8c785abebea9ae3257b61681b4e6fd8365ceafde980c21970d001e834cf10835
fetch tokenizer.json 8bf8afbfd11306bd872018c53bfdf2e160a56f8edbcf49933324404791c148d3
fetch tokenizer_config.json b87c8703482b0300d3da30e201519aa641f6a450f5eb5bf1e624afbf70c74d80
cp "$license_source" "$destination/LICENSE"

cd "$destination"
"${checksum[@]}" <<'EOF'
122e922dcfed6503c8721e6fe1daf090340c3d95ca7f3aa3a72730b321a51cfd  config.json
912fc1215c2dbff6499700534bd8d31253af01573861abbfc43afd1fab6cce5d  onnx/model_int8.onnx
8c785abebea9ae3257b61681b4e6fd8365ceafde980c21970d001e834cf10835  special_tokens_map.json
8bf8afbfd11306bd872018c53bfdf2e160a56f8edbcf49933324404791c148d3  tokenizer.json
b87c8703482b0300d3da30e201519aa641f6a450f5eb5bf1e624afbf70c74d80  tokenizer_config.json
cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30  LICENSE
EOF
