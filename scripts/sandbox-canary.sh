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

provider_port=$(free_port)
agentd_port=$(free_port)
model_dir=${AGENTD_EMBEDDING_MODEL_DIR:-"$HOME/.cache/agentd/models/multilingual-e5-small"}
reranker_dir=${AGENTD_RERANKER_MODEL_DIR:-"$HOME/.cache/agentd/models/bge-reranker-v2-m3"}
image=${AGENTD_SANDBOX_TEST_IMAGE:-"ghcr.io/minifish-org/agentd-sandbox@sha256:5c80ee1cc27e784074cdb4132583e00284355920bf7f03f7562b13e581b1759e"}
work_dir=$(mktemp -d /tmp/adsc.XXXXXX)
provider_pid=
agentd_pid=

cleanup() {
  status=$?
  trap - EXIT
  if [[ -n "$agentd_pid" ]]; then
    kill "$agentd_pid" 2>/dev/null || true
    wait "$agentd_pid" 2>/dev/null || true
  fi
  if [[ -n "$provider_pid" ]]; then
    kill "$provider_pid" 2>/dev/null || true
    wait "$provider_pid" 2>/dev/null || true
  fi
  if [[ $status -ne 0 ]]; then
    printf '%s\n' '--- sandbox canary provider log ---' >&2
    sed -n '1,160p' "$work_dir/provider.log" >&2 || true
    printf '%s\n' '--- sandbox canary agentd log ---' >&2
    sed -n '1,320p' "$work_dir/agentd.log" >&2 || true
  fi
  case "$work_dir" in
    /tmp/adsc.*) rm -r -- "$work_dir" ;;
    *) printf 'refusing to remove unexpected work directory: %s\n' "$work_dir" >&2 ;;
  esac
  exit "$status"
}
trap cleanup EXIT

"$root/scripts/fetch-embedding-model.sh" "$model_dir"
"$root/scripts/fetch-reranker-model.sh" "$reranker_dir"

python3 "$root/scripts/demo-openai-provider.py" --port "$provider_port" \
  >"$work_dir/provider.log" 2>&1 &
provider_pid=$!

for _ in $(seq 1 100); do
  if curl --fail --silent "http://127.0.0.1:$provider_port/healthz" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$provider_pid" 2>/dev/null
  sleep 0.1
done

printf '%s\n' \
  "rest_addr = \"127.0.0.1:$agentd_port\"" \
  "database_path = \"$work_dir/agentd.db\"" \
  'scheduler_tick_ms = 1000' \
  'run_concurrency = 1' \
  'dispatch_poll_interval_ms = 25' \
  'http_timeout_secs = 30' \
  "llm_api_base = \"http://127.0.0.1:$provider_port/v1\"" \
  'llm_api_key = "sandbox-canary-only"' \
  'llm_model = "demo/sandbox-canary"' \
  '' \
  '[sandbox]' \
  'enabled = true' \
  "image = \"$image\"" \
  'cpus = 1' \
  'memory_mib = 512' \
  'default_command_timeout_ms = 30000' \
  'max_command_timeout_ms = 60000' \
  'max_output_bytes_per_stream = 524288' \
  "state_dir = \"$work_dir/msb\"" \
  >"$work_dir/agentd.toml"

AGENTD_EMBEDDING_MODEL_DIR="$model_dir" \
AGENTD_RERANKER_MODEL_DIR="$reranker_dir" \
  cargo run --quiet --manifest-path "$root/Cargo.toml" -p agentd -- \
  --config "$work_dir/agentd.toml" --reset-data \
  >"$work_dir/agentd.log" 2>&1 &
agentd_pid=$!

for _ in $(seq 1 600); do
  if curl --fail --silent "http://127.0.0.1:$agentd_port/" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$agentd_pid" 2>/dev/null
  sleep 0.2
done

base="http://127.0.0.1:$agentd_port"
request() {
  curl --silent --show-error --fail "$@"
}

request -X POST "$base/v1/tenants" \
  -H 'content-type: application/json' \
  -d '{"name":"sandbox-canary"}' >/dev/null
request -X PUT "$base/v1/tenants/sandbox-canary/agents/canary" \
  -H 'content-type: application/toml' \
  --data-binary $'model = "demo/sandbox-canary"\ntimeout_ms = 300000\nmax_steps = 12\nmax_tokens = 1024\ncontext_window = 2\nallowed_families = ["sandbox"]\n' \
  >/dev/null
request "$base/v1/tenants/sandbox-canary/tools?agent=canary" \
  | jq -e 'length == 1 and .[0].name == "sandbox_session"' >/dev/null

submit() {
  scenario=$1
  scope=$2
  body=$(jq -nc --arg scenario "$scenario" --arg scope "$scope" \
    '{agent:"canary",scope:$scope,payload:{scenario:$scenario},request_id:($scenario+"-"+$scope)}')
  request -X POST "$base/v1/tenants/sandbox-canary/turns" \
    -H 'content-type: application/json' -d "$body" | jq -er '.run_id'
}

full_run=$(submit full canary-full)
full_result=$(request "$base/v1/tenants/sandbox-canary/runs/$full_run/wait?timeout_ms=300000")
jq -e '.status == "succeeded" and .output.reply == "sandbox canary full complete"' \
  <<<"$full_result" >/dev/null
full_trace=$(request "$base/v1/tenants/sandbox-canary/runs/$full_run/trace")
jq -e '
  [.[] | select(.kind == "tool" and .payload.phase == "result") | .payload.result] as $r
  | ($r | length) == 8
  and $r[0].ok and $r[0].result.success
  and ($r[1].result.stdout == "env-ok")
  and ($r[2].result.stdout == "persistent")
  and ($r[3].result.stdout == "python-ok\n")
  and ($r[4].result.stdout == "node-ok\n")
  and $r[5].result.success
  and ($r[6].result.success == false and $r[6].result.exit_code == 7 and $r[6].result.stderr == "nonzero")
  and ($r[7].result.timed_out == true and $r[7].result.success == false)
' <<<"$full_trace" >/dev/null

isolation_run=$(submit isolation canary-isolation)
isolation_result=$(request "$base/v1/tenants/sandbox-canary/runs/$isolation_run/wait?timeout_ms=120000")
jq -e '.status == "succeeded" and .output.reply == "sandbox canary isolation complete"' \
  <<<"$isolation_result" >/dev/null
request "$base/v1/tenants/sandbox-canary/runs/$isolation_run/trace" \
  | jq -e '[.[] | select(.kind == "tool" and .payload.phase == "result") | .payload.result.result][0].stdout == "isolated"' \
  >/dev/null

cancel_run=$(submit cancel canary-cancel)
for _ in $(seq 1 600); do
  if request "$base/v1/tenants/sandbox-canary/runs/$cancel_run/trace" \
    | jq -e 'any(.[]; .kind == "tool" and .payload.phase == "call" and .payload.name == "sandbox_session")' \
      >/dev/null; then
    break
  fi
  sleep 0.05
done
request -X POST "$base/v1/tenants/sandbox-canary/runs/$cancel_run/cancel" \
  -H 'content-type: application/json' -d '{"reason":"sandbox canary cancellation"}' \
  | jq -e '.status == "cancelled"' >/dev/null
request "$base/v1/tenants/sandbox-canary/runs/$cancel_run/wait?timeout_ms=30000" \
  | jq -e '.status == "cancelled" and .timed_out == false' >/dev/null

for run_id in "$full_run" "$isolation_run" "$cancel_run"; do
  grep -q "$run_id" "$work_dir/agentd.log"
done
destroyed=$(grep -c 'sandbox session destroyed' "$work_dir/agentd.log" || true)
test "$destroyed" -ge 3

printf 'agentd sandbox canary: ok full=%s isolation=%s cancel=%s\n' \
  "$full_run" "$isolation_run" "$cancel_run"
