#!/usr/bin/env bash
set -euo pipefail

base=${AGENTD_URL:-http://127.0.0.1:8080}
tenant=${AGENTD_TENANT:-demo}
agent=${AGENTD_AGENT:-simple-bot}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
request() {
  if [[ -n "${AGENTD_TOKEN:-}" ]]; then
    curl --silent --show-error --fail \
      -H "Authorization: Bearer $AGENTD_TOKEN" "$@"
  else
    curl --silent --show-error --fail "$@"
  fi
}

request -X POST "$base/v1/tenants" \
  -H 'content-type: application/json' \
  -d "{\"name\":\"$tenant\"}" >/dev/null
request -X PUT "$base/v1/tenants/$tenant/agents/$agent" \
  -H 'content-type: application/toml' \
  --data-binary "@$root/agents/simple-bot.toml" >/dev/null

request_id="e2e-turn-$(date -u +%Y%m%dT%H%M%SZ)-$$"
scope="e2e/turn/$request_id"
turn_body=$(jq -nc \
  --arg agent "$agent" \
  --arg scope "$scope" \
  --arg request_id "$request_id" \
  '{agent:$agent,scope:$scope,payload:{text:"reply with a short greeting"},request_id:$request_id}')
submitted=$(request -X POST "$base/v1/tenants/$tenant/turns" \
  -H 'content-type: application/json' \
  -d "$turn_body")
run_id=$(jq -er '.run_id' <<<"$submitted")
jq -e '.status == "queued"' <<<"$submitted" >/dev/null
response=$(request "$base/v1/tenants/$tenant/runs/$run_id/wait?timeout_ms=60000")
jq -e '
  .status == "succeeded"
  and (.output.reply | type == "string")
  and (.output.reply | length > 0)
' <<<"$response" >/dev/null

request "$base/v1/tenants/$tenant/runs/$run_id" | jq -e --arg id "$run_id" '.run_id == $id' >/dev/null
request "$base/v1/tenants/$tenant/runs/$run_id/trace" | jq -e 'map(.kind) | index("output") != null' >/dev/null
request "$base/v1/tenants/$tenant/deliveries?run_id=$run_id" | jq -e '.deliveries | length == 0' >/dev/null

health_path="health/e2e-$run_id.txt"
request -X PUT "$base/v1/tenants/$tenant/artifacts/$health_path" \
  -H 'content-type: text/plain' --data-binary 'healthy' >/dev/null
test "$(request "$base/v1/tenants/$tenant/artifacts/$health_path")" = healthy
request -X DELETE "$base/v1/tenants/$tenant/artifacts/$health_path" >/dev/null

printf 'agentd e2e passed run=%s\n' "$run_id"
