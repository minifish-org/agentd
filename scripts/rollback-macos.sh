#!/usr/bin/env bash
set -euo pipefail

test "$#" -eq 2 || {
  printf 'usage: %s DATA_BACKUP PLIST_BACKUP\n' "$0" >&2
  exit 2
}

data_backup=$1
plist_backup=$2
runtime="$HOME/Library/Application Support/agentd"
data="$runtime/data"
plist="$HOME/Library/LaunchAgents/dev.agentd.agentd.plist"
label=dev.agentd.agentd
domain="gui/$(id -u)"

case "$data_backup" in "$runtime"/data.*.backup) ;; *) printf 'invalid data backup\n' >&2; exit 2 ;; esac
case "$plist_backup" in "$plist".*.backup) ;; *) printf 'invalid plist backup\n' >&2; exit 2 ;; esac
test -d "$data_backup" && test ! -L "$data_backup"
test -f "$plist_backup" && test ! -L "$plist_backup"
plutil -lint "$plist_backup" >/dev/null

launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
failed="$runtime/data.failed.$(date -u +%Y%m%dT%H%M%SZ)"
test ! -e "$data" || mv "$data" "$failed"
mv "$data_backup" "$data"
cp -p "$plist_backup" "$plist"
launchctl bootstrap "$domain" "$plist"

for _ in {1..30}; do
  if launchctl print "$domain/$label" >/dev/null 2>&1; then
    printf 'agentd rollback complete; failed data retained at %s\n' "$failed"
    exit 0
  fi
  sleep 1
done

printf 'rollback restored files but launchd did not become ready\n' >&2
exit 1
