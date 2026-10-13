#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

PROGRESS_FILE="${ASKMYDASMA_LAUNCH_PROGRESS_FILE:-$ROOT_DIR/.tmp/askmydasma-launch-progress.log}"
CONFIG_HELPER="$ROOT_DIR/scripts/web/campaign_config.py"
mkdir -p "$(dirname "$PROGRESS_FILE")"

log_progress() {
  local message="$1"
  printf '%s\n' "$message"
  printf '%s\n' "$message" >> "$PROGRESS_FILE"
}

SERVICES=()
while IFS= read -r service; do
  SERVICES+=("$service")
done < <(python3 "$CONFIG_HELPER" all-runtime-services)

log_progress "[launch] stopping campaign containers"
docker compose stop "${SERVICES[@]}" >/dev/null 2>&1 || true
log_progress "[launch] removing campaign containers"
docker compose rm -f "${SERVICES[@]}" >/dev/null 2>&1 || true

printf '{"status":"ok","action":"stop_campaign"}\n'
