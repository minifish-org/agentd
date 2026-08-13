#!/usr/bin/env bash
set -euo pipefail

: "${AGENTD_URL:?set AGENTD_URL to the deployed agentd base URL}"
: "${AGENTD_API_TOKEN:?set AGENTD_API_TOKEN to the deployed API token}"

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
base=${AGENTD_URL%/}
auth=(--header "Authorization: Bearer ${AGENTD_API_TOKEN}")

create_tenant() {
  local tenant=$1
  curl --fail-with-body --silent --show-error \
    "${auth[@]}" \
    --header 'Content-Type: application/json' \
    --data "{\"name\":\"${tenant}\"}" \
    "${base}/v1/tenants" >/dev/null
}

put_agent() {
  local tenant=$1
  local agent=$2
  curl --fail-with-body --silent --show-error \
    "${auth[@]}" \
    --request PUT \
    --header 'Content-Type: application/toml' \
    --data-binary "@${root}/agents/${agent}.toml" \
    "${base}/v1/tenants/${tenant}/agents/${agent}" >/dev/null
}

create_tenant demo
put_agent demo simple-bot

create_tenant werewolf
for agent in werewolf-wolf werewolf-seer werewolf-villager werewolf-judge; do
  put_agent werewolf "$agent"
done

curl --fail-with-body --silent --show-error \
  "${auth[@]}" \
  --request PUT \
  --header 'Content-Type: application/json' \
  --data '{"enabled":true,"transport":{"type":"http","url":"http://canopy:8000/mcp"}}' \
  "${base}/v1/tenants/demo/mcp/canopy" >/dev/null

echo "agentd deployment resources are ready"
