#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

CONFIG_PATH="${1:-$ROOT_DIR/.tmp/current-campaign.json}"

if [ ! -f "$CONFIG_PATH" ]; then
  printf '{"status":"error","error":"campaign_config_not_found","message":"campaign JSON does not exist"}\n' >&2
  exit 1
fi

bash "$ROOT_DIR/scripts/web/stop_campaign.sh" >/dev/null
bash "$ROOT_DIR/scripts/web/launch_campaign.sh" "$CONFIG_PATH"
bash "$ROOT_DIR/scripts/web/start_production.sh" "$CONFIG_PATH"
