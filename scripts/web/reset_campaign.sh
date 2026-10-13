#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

PROGRESS_FILE="${ASKMYDASMA_LAUNCH_PROGRESS_FILE:-$ROOT_DIR/.tmp/askmydasma-launch-progress.log}"
CONFIG_HELPER="$ROOT_DIR/scripts/web/campaign_config.py"
PROJECT_NAME="$(docker compose config --format json | python3 -c 'import json, sys; print(json.load(sys.stdin)["name"])')"
mkdir -p "$(dirname "$PROGRESS_FILE")"

log_progress() {
  local message="$1"
  printf '%s\n' "$message"
  printf '%s\n' "$message" >> "$PROGRESS_FILE"
}

log_progress "[launch] ensuring reset prerequisites"
bash "$ROOT_DIR/scripts/docker/pull_missing_images.sh" init-topic influxdb schema-registry kafka1 >/dev/null

log_progress "[launch] ensuring kafka readiness"
docker compose up --pull never --wait --wait-timeout 240 kafka1 kafka2 kafka3 schema-registry >/dev/null

INFLUX_VOLUME="${PROJECT_NAME}_influxdb-data"

TOPICS=()
while IFS= read -r topic; do
  TOPICS+=("$topic")
done < <(python3 "$CONFIG_HELPER" reset-topics)

SCHEMA_SUBJECTS=()
while IFS= read -r subject; do
  SCHEMA_SUBJECTS+=("$subject")
done < <(python3 "$CONFIG_HELPER" reset-schema-subjects)

log_progress "[launch] removing kafka topics"
for topic in "${TOPICS[@]}"; do
  docker compose exec -T kafka1 bash -lc "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --delete --if-exists --topic '$topic'" >/dev/null 2>&1 || true
done

log_progress "[launch] recreating kafka topics"
docker compose up --pull never --no-deps --force-recreate init-topic >/dev/null

log_progress "[launch] removing schema registry subjects"
for subject in "${SCHEMA_SUBJECTS[@]}"; do
  docker compose exec -T schema-registry sh -lc \
    "curl -fsS -X DELETE 'http://localhost:8081/subjects/${subject}?permanent=true'" \
    >/dev/null 2>&1 || true
  docker compose exec -T schema-registry sh -lc \
    "curl -fsS -X DELETE 'http://localhost:8081/subjects/${subject}'" \
    >/dev/null 2>&1 || true
done

log_progress "[launch] resetting influxdb storage"
docker compose stop influxdb influxdb-init-buckets influxdb-apply-template >/dev/null 2>&1 || true
docker compose rm -sfv influxdb influxdb-init-buckets influxdb-apply-template >/dev/null 2>&1 || true
docker volume rm -f "${INFLUX_VOLUME}" >/dev/null 2>&1 || true

log_progress "[launch] recreating influxdb"
docker compose up --pull never --wait --wait-timeout 240 influxdb >/dev/null

log_progress "[launch] initializing influxdb buckets"
docker compose up --pull never --no-deps --force-recreate influxdb-init-buckets >/dev/null

log_progress "[launch] applying influxdb template"
docker compose up --pull never --no-deps --force-recreate influxdb-apply-template >/dev/null

printf '{"status":"ok","action":"reset_campaign","details":"topics recreated, influxdb volume reset, schema registry subjects removed"}\n'
