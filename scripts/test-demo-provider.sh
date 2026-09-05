#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
log_file=$(mktemp "${TMPDIR:-/tmp}/agentd-demo-provider-test.XXXXXX")
non_loopback_log="$log_file.non-loopback"
provider_pid=

cleanup() {
  status=$?
  trap - EXIT
  test -z "$provider_pid" || kill "$provider_pid" 2>/dev/null || true
  test -z "$provider_pid" || wait "$provider_pid" 2>/dev/null || true
  if [[ $status -ne 0 ]]; then
    sed -n '1,120p' "$log_file" >&2
  fi
  rm -f -- "$log_file" "$non_loopback_log"
  exit "$status"
}
trap cleanup EXIT

python3 "$root/scripts/demo-openai-provider.py" --port "$port" >"$log_file" 2>&1 &
provider_pid=$!

for _ in $(seq 1 50); do
  if curl --fail --silent --show-error \
    "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
    break
  fi
  kill -0 "$provider_pid" 2>/dev/null
  sleep 0.1
done

response=$(curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  -d '{"model":"demo/chat","messages":[{"role":"user","content":"hello"}]}' \
  "http://127.0.0.1:$port/v1/chat/completions")
printf '%s' "$response" | python3 -c '
import json, sys
payload = json.load(sys.stdin)
content = payload["choices"][0]["message"]["content"]
assert json.loads(content)["reply"] == "agentd completed a deterministic demo turn"
'

canary=$(curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  -d '{"model":"demo/sandbox-canary","messages":[{"role":"user","content":"{\"input\":{\"scenario\":\"full\"}}"}]}' \
  "http://127.0.0.1:$port/v1/chat/completions")
printf '%s' "$canary" | python3 -c '
import json, sys
payload = json.load(sys.stdin)
choice = payload["choices"][0]
call = choice["message"]["tool_calls"][0]
assert choice["finish_reason"] == "tool_calls"
assert call["function"]["name"] == "sandbox_session"
assert json.loads(call["function"]["arguments"])["action"] == "shell"
'

status=$(curl --silent --output /dev/null --write-out '%{http_code}' \
  -H 'content-type: application/json' \
  -d '{"messages":[]}' \
  "http://127.0.0.1:$port/v1/chat/completions")
test "$status" = 400

if python3 "$root/scripts/demo-openai-provider.py" --host 0.0.0.0 \
  >"$non_loopback_log" 2>&1; then
  printf 'demo provider accepted a non-loopback bind\n' >&2
  exit 1
fi

printf 'agentd demo provider: ok\n'
