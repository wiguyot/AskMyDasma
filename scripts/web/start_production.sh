#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

PROGRESS_FILE="${ASKMYDASMA_LAUNCH_PROGRESS_FILE:-$ROOT_DIR/.tmp/askmydasma-launch-progress.log}"
CONFIG_PATH="${1:-$ROOT_DIR/.tmp/current-campaign.json}"
CONFIG_HELPER="$ROOT_DIR/scripts/web/campaign_config.py"

mkdir -p "$(dirname "$PROGRESS_FILE")"

log_progress() {
  local message="$1"
  printf '%s\n' "$message"
  printf '%s\n' "$message" >> "$PROGRESS_FILE"
}

if [ ! -f "$CONFIG_PATH" ]; then
  printf '{"status":"error","error":"campaign_config_not_found","message":"campaign JSON does not exist"}\n' >&2
  exit 1
fi

eval "$(python3 "$CONFIG_HELPER" export-env "$CONFIG_PATH" --wait-for-start)"

if [ -z "${DATA_SOURCE_FILE:-}" ]; then
  printf '{"status":"error","error":"missing_dataset","message":"dataset path is required"}\n' >&2
  exit 1
fi

log_progress "[production] dataset: $DATA_SOURCE_FILE"
read -r -a COMPOSE_SERVICES <<< "${DASMA_COMPOSE_SERVICES:-consensus-analysis build-metrics telegraf}"

log_progress "[production] starting runtime services: ${COMPOSE_SERVICES[*]}"
docker compose up --pull never -d --force-recreate \
  "${COMPOSE_SERVICES[@]}" >/dev/null

log_progress "[production] creating producer container"
docker compose up --pull never -d --force-recreate producer >/dev/null

log_progress "[launch] producer mode: ${DASMA_PRODUCER_TRANSFORMATION:-batch}"
log_progress "[launch] waiting for producer readiness"
for _ in $(seq 1 30); do
  if docker compose exec -T producer sh -lc 'touch "${DASMA_PRODUCER_START_SIGNAL_FILE:-/tmp/dasma-producer-start.signal}"' >/dev/null 2>&1; then
    python3 "$CONFIG_HELPER" payload "$CONFIG_PATH" --action start_production
    exit 0
  fi
  sleep 1
done

printf '{"status":"error","error":"producer_not_ready","message":"producer container is not ready to receive the start signal"}\n' >&2
exit 1
