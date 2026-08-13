#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
bash -n "$root/scripts/deploy-macos.sh"
bash -n "$root/scripts/rollback-macos.sh"
bash -n "$root/scripts/fetch-embedding-model.sh"
bash -n "$root/scripts/test-demo-provider.sh"
bash -n "$root/scripts/demo-e2e.sh"
python3 -c 'import sys; compile(open(sys.argv[1], encoding="utf-8").read(), sys.argv[1], "exec")' \
  "$root/scripts/demo-openai-provider.py"
"$root/scripts/test-demo-provider.sh"

grep -q '/v1/tenants/' "$root/scripts/deploy-macos.sh"
! grep -q '^embedding_' "$root/configs/agentd.toml"
grep -q 'multilingual-e5-small@614241f' \
  "$root/crates/agentd-core/src/embedding_provider.rs"
grep -q 'multilingual-e5-small.LICENSE' "$root/scripts/fetch-embedding-model.sh"
grep -q 'LICENSE' "$root/scripts/fetch-embedding-model.sh"
test -f "$root/licenses/multilingual-e5-small.LICENSE"
grep -q 'Copyright (c) Microsoft Corporation' \
  "$root/licenses/multilingual-e5-small.LICENSE"
grep -q 'THIRD_PARTY_NOTICES.md' "$root/Dockerfile"
test -f "$root/SECURITY.md"
test -f "$root/CONTRIBUTING.md"
test -f "$root/CHANGELOG.md"
test -f "$root/docs/threat-model.md"
test -f "$root/docs/reliability.md"
test -f "$root/docs/demo.md"
test -f "$root/deny.toml"
test -f "$root/.github/workflows/security.yml"
grep -q 'Stdio MCP is \*\*not a sandbox\*\*' "$root/docs/threat-model.md"
grep -q 'Tenant names scope stored resources' "$root/docs/threat-model.md"
grep -q 'final_output_trace_and_delivery_commit_together' \
  "$root/docs/reliability.md"
grep -q 'Reliability evidence and known gaps' "$root/README.md"
grep -q 'Demo without LLM credentials' "$root/README.md"
grep -q 'check advisories licenses sources' "$root/.github/workflows/security.yml"
grep -q 'gitleaks/gitleaks:v8.28.0@sha256:' "$root/.github/workflows/security.yml"
grep -q '^rest_addr = "0.0.0.0:8080"' "$root/configs/agentd.docker.toml"
grep -q '^api_token = "local-dev-token"' "$root/configs/agentd.docker.toml"
! grep -Eq '100\.[0-9]+\.[0-9]+\.[0-9]+|minifish|tailgate' \
  "$root/configs/agentd.docker.toml"
! grep -R -q 'agentd-telegram-adapter\|tg-webhook-adapter\|tg-adapter' \
  "$root/Cargo.toml" \
  "$root/crates"

printf 'agentd deployment scripts: ok\n'
