#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

PROGRESS_FILE="${ASKMYDASMA_LAUNCH_PROGRESS_FILE:-$ROOT_DIR/.tmp/askmydasma-launch-progress.log}"
CONFIG_PATH="${1:-$ROOT_DIR/.tmp/current-campaign.json}"
CONFIG_HELPER="$ROOT_DIR/scripts/web/campaign_config.py"

mkdir -p "$(dirname "$PROGRESS_FILE")"
: > "$PROGRESS_FILE"

log_progress() {
  printf '%s\n' "$1"
  printf '%s\n' "$1" >> "$PROGRESS_FILE"
}

if [ ! -f "$CONFIG_PATH" ]; then
  printf '{"status":"error","error":"campaign_config_not_found","message":"campaign JSON does not exist"}\n' >&2
  exit 1
fi

eval "$(python3 "$CONFIG_HELPER" export-env "$CONFIG_PATH")"

if [ -z "${DATA_SOURCE_FILE:-}" ]; then
  printf '{"status":"error","error":"missing_dataset","message":"dataset path is required"}\n' >&2
  exit 1
fi

log_progress "[launch] checking prerequisites"
log_progress "[launch] dataset: $DATA_SOURCE_FILE"

if [ ! -f "$ROOT_DIR/$DATA_SOURCE_FILE" ]; then
  printf '{"status":"error","error":"dataset_not_found","message":"dataset does not exist"}\n' >&2
  exit 1
fi

log_progress "[launch] ensuring runtime images"
bash "$ROOT_DIR/scripts/docker/pull_missing_images.sh" \
  init-topic hstree-analysis iforest-analysis kitnet-analysis deepant-analysis mdragstream-point-analysis explainability explainability-v2 repeatability-v1 repeatability-v2 repeatability-diff reproducibility-export explainability-shap-diff consensus-analysis build-metrics telegraf

log_progress "[launch] ensuring kafka topics"
docker compose up --pull never -d --force-recreate init-topic >/dev/null

python3 "$CONFIG_HELPER" payload "$CONFIG_PATH" --action launch_campaign
