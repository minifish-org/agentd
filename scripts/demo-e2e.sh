#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
for command in cargo curl jq python3; do
  command -v "$command" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command" >&2
    exit 1
  }
done

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

provider_port=${AGENTD_DEMO_PROVIDER_PORT:-$(free_port)}
agentd_port=${AGENTD_DEMO_PORT:-$(free_port)}
model_dir=${AGENTD_EMBEDDING_MODEL_DIR:-"$HOME/.cache/agentd/models/multilingual-e5-small"}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/agentd-demo.XXXXXX")
provider_pid=
agentd_pid=

cleanup() {
  status=$?
  trap - EXIT
  test -z "$agentd_pid" || kill "$agentd_pid" 2>/dev/null || true
  test -z "$provider_pid" || kill "$provider_pid" 2>/dev/null || true
  test -z "$agentd_pid" || wait "$agentd_pid" 2>/dev/null || true
  test -z "$provider_pid" || wait "$provider_pid" 2>/dev/null || true
  if [[ $status -ne 0 ]]; then
    printf '%s\n' '--- demo provider log ---' >&2
    sed -n '1,160p' "$work_dir/provider.log" >&2
    printf '%s\n' '--- agentd log ---' >&2
    sed -n '1,240p' "$work_dir/agentd.log" >&2
  fi
  case "$work_dir" in
    "${TMPDIR:-/tmp}"/agentd-demo.*) rm -r -- "$work_dir" ;;
    *) printf 'refusing to remove unexpected work directory: %s\n' "$work_dir" >&2 ;;
  esac
  exit "$status"
}
trap cleanup EXIT

"$root/scripts/fetch-embedding-model.sh" "$model_dir"

python3 "$root/scripts/demo-openai-provider.py" --port "$provider_port" \
  >"$work_dir/provider.log" 2>&1 &
provider_pid=$!

for _ in $(seq 1 100); do
  if curl --fail --silent --show-error \
    "http://127.0.0.1:$provider_port/healthz" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$provider_pid" 2>/dev/null
  sleep 0.1
done

printf '%s\n' \
  "rest_addr = \"127.0.0.1:$agentd_port\"" \
  "database_path = \"$work_dir/agentd.db\"" \
  'scheduler_tick_ms = 1000' \
  'run_concurrency = 2' \
  'dispatch_poll_interval_ms = 50' \
  'http_timeout_secs = 30' \
  "llm_api_base = \"http://127.0.0.1:$provider_port/v1\"" \
  'llm_api_key = "demo-only"' \
  'llm_model = "demo/chat"' \
  >"$work_dir/agentd.toml"

AGENTD_EMBEDDING_MODEL_DIR="$model_dir" \
  cargo run --quiet --manifest-path "$root/Cargo.toml" -p agentd -- \
  --config "$work_dir/agentd.toml" --reset-data \
  >"$work_dir/agentd.log" 2>&1 &
agentd_pid=$!

for _ in $(seq 1 300); do
  if curl --fail --silent --show-error \
    "http://127.0.0.1:$agentd_port/" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$agentd_pid" 2>/dev/null
  sleep 0.2
done

AGENTD_URL="http://127.0.0.1:$agentd_port" \
AGENTD_TENANT=demo \
AGENTD_AGENT=simple-bot \
  "$root/scripts/api-turn-e2e.sh"

printf 'agentd deterministic demo: ok\n'
