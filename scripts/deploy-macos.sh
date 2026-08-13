#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo=$(CDPATH= cd -- "$script_dir/.." && pwd)
label=dev.agentd.agentd
domain="gui/$(id -u)"
plist="$HOME/Library/LaunchAgents/$label.plist"
runtime="$HOME/Library/Application Support/agentd"
data="$runtime/data"
releases="$runtime/releases"
log="$HOME/Library/Logs/agentd/agentd.log"
health_url=${AGENTD_HEALTH_URL:-http://127.0.0.1:8080}
auth=()
if test -z "${AGENTD_TOKEN:-}"; then
  AGENTD_TOKEN=$(sed -nE 's/^[[:space:]]*api_token[[:space:]]*=[[:space:]]*"([^"\\]+)"[[:space:]]*(#.*)?$/\1/p' "$HOME/.agentd.toml" | head -n 1)
fi
test -z "${AGENTD_TOKEN:-}" || auth=(-H "Authorization: Bearer $AGENTD_TOKEN")

test -f "$plist"
test -f "$HOME/.agentd.toml"
cargo build --locked --release -p agentd --manifest-path "$repo/Cargo.toml"
binary="$repo/build/release/agentd"
sha=$(shasum -a 256 "$binary" | awk '{print $1}')
install_dir="$releases/$sha"
installed="$install_dir/agentd"
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
plist_backup="$plist.$timestamp.backup"
data_backup="$runtime/data.$timestamp.backup"

mkdir -p "$install_dir" "$(dirname "$log")"
install -m 0755 "$binary" "$installed"
cp -p "$plist" "$plist_backup"

rollback() {
  rc=$?
  trap - EXIT
  launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
  cp -p "$plist_backup" "$plist"
  rm -rf "$data"
  test ! -e "$data_backup" || mv "$data_backup" "$data"
  launchctl bootstrap "$domain" "$plist" >/dev/null 2>&1 || true
  exit "$rc"
}
trap rollback EXIT

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
test ! -e "$data" || mv "$data" "$data_backup"

/usr/libexec/PlistBuddy -c "Set :Program $installed" "$plist"
/usr/libexec/PlistBuddy -c 'Delete :ProgramArguments' "$plist" >/dev/null 2>&1 || true
/usr/libexec/PlistBuddy -c 'Add :ProgramArguments array' "$plist"
/usr/libexec/PlistBuddy -c "Add :ProgramArguments:0 string $installed" "$plist"
/usr/libexec/PlistBuddy -c 'Add :ProgramArguments:1 string --config' "$plist"
/usr/libexec/PlistBuddy -c "Add :ProgramArguments:2 string $HOME/.agentd.toml" "$plist"
plutil -lint "$plist" >/dev/null
launchctl bootstrap "$domain" "$plist"

ready=0
for _ in {1..30}; do
  if curl --silent --show-error --fail "${auth[@]}" "$health_url/v1/tenants" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
test "$ready" -eq 1

curl --silent --show-error --fail "${auth[@]}" -X POST "$health_url/v1/tenants" \
  -H 'content-type: application/json' -d '{"name":"demo"}' >/dev/null
curl --silent --show-error --fail "${auth[@]}" -X POST "$health_url/v1/tenants" \
  -H 'content-type: application/json' -d '{"name":"werewolf"}' >/dev/null
curl --silent --show-error --fail "${auth[@]}" -X PUT "$health_url/v1/tenants/demo/agents/simple-bot" \
  -H 'content-type: application/toml' --data-binary "@$repo/agents/simple-bot.toml" >/dev/null
for role in seer villager wolf; do
  curl --silent --show-error --fail "${auth[@]}" -X PUT \
    "$health_url/v1/tenants/werewolf/agents/werewolf-$role" \
    -H 'content-type: application/toml' --data-binary "@$repo/agents/werewolf-$role.toml" >/dev/null
done
curl --silent --show-error --fail "${auth[@]}" -X PUT \
  "$health_url/v1/tenants/werewolf/agents/werewolf-judge" \
  -H 'content-type: application/toml' --data-binary "@$repo/agents/werewolf-judge.toml" >/dev/null

path="health/deploy-$timestamp.txt"
curl --silent --show-error --fail "${auth[@]}" -X PUT \
  "$health_url/v1/tenants/demo/artifacts/$path" -H 'content-type: text/plain' \
  --data-binary 'healthy' >/dev/null
test "$(curl --silent --show-error --fail "${auth[@]}" "$health_url/v1/tenants/demo/artifacts/$path")" = healthy
curl --silent --show-error --fail "${auth[@]}" -X DELETE \
  "$health_url/v1/tenants/demo/artifacts/$path" >/dev/null

trap - EXIT
printf 'agentd deployed sha=%s data_backup=%s plist_backup=%s\n' "$sha" "$data_backup" "$plist_backup"
