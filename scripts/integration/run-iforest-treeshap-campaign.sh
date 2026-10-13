#!/usr/bin/env bash
# Run one isolated V-C1/V-C2 runtime witness without touching the regular
# stopped development stack, its named containers, or its Kafka volumes.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

usage() {
  echo "Usage: $0 reference|simple OUTPUT_DIR PROJECT_NAME [iforest|hstree] [tree|kernel]" >&2
  exit 2
}

[[ $# -ge 3 && $# -le 5 ]] || usage
MODE="$1"
OUTPUT_DIR="$2"
PROJECT_NAME="$3"
MODEL="${4:-iforest}"
METHOD="${5:-tree}"
[[ "$MODE" == "reference" || "$MODE" == "simple" ]] || usage
[[ "$MODEL" == "iforest" || "$MODEL" == "hstree" ]] || usage
[[ "$METHOD" == "tree" || "$METHOD" == "kernel" ]] || usage
[[ "$MODEL" == "iforest" || "$METHOD" == "kernel" ]] || usage
[[ "$PROJECT_NAME" =~ ^[a-z0-9][a-z0-9_-]{2,40}$ ]] || usage

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

if [[ "$MODE" == "reference" ]]; then
  SOURCE_TOPIC="${MODEL}-analysis"
  EXPLAINABILITY_MODE="reference"
  if [[ "$MODEL" == "iforest" ]]; then
    DETECTOR_SERVICE=iforest-analysis
    DETECTOR_VIZ_TOPIC=iforest-analysis-viz
    DASMA_IFOREST_ENABLED=1
    DASMA_HSTREE_ENABLED=0
    DETECTOR_GROUP=analyse-iforest-consumer
  else
    DETECTOR_SERVICE=hstree-analysis
    DETECTOR_VIZ_TOPIC=hstree-analysis-viz
    DASMA_IFOREST_ENABLED=0
    DASMA_HSTREE_ENABLED=1
    DETECTOR_GROUP=analyse-hstree-consumer
  fi
  SERVICES=(init-topic producer "$DETECTOR_SERVICE" explainability explainability-v2)
  CONSUMER_GROUPS=("$DETECTOR_GROUP" explainability-consumer explainability-v2-consumer)
else
  SOURCE_TOPIC="source-data"
  EXPLAINABILITY_MODE="simple"
  DASMA_IFOREST_ENABLED=0
  DASMA_HSTREE_ENABLED=0
  SERVICES=(init-topic producer explainability explainability-v2)
  CONSUMER_GROUPS=(explainability-consumer explainability-v2-consumer)
fi

export DATA_SOURCE_FILE="data/source-5000.csv"
export EXPLAINABILITY_MODEL="$MODEL"
export EXPLAINABILITY_MODE
export EXPLAINABILITY_METHOD="$METHOD"
export EXPLAINABILITY_SOURCE_TOPIC="$SOURCE_TOPIC"
export EXPLAINABILITY_V2_MODEL="$MODEL"
export EXPLAINABILITY_V2_MODE="$EXPLAINABILITY_MODE"
export EXPLAINABILITY_V2_METHOD="$METHOD"
export EXPLAINABILITY_V2_SOURCE_TOPIC="$SOURCE_TOPIC"
export EXPLAINABILITY_SHAP_NSAMPLES=100
export EXPLAINABILITY_V2_SHAP_NSAMPLES=100
export SHAP_BACKGROUND_SIZE=100
export HSTREE_INITIAL_WINDOW_SIZE=256
export HSTREE_WINDOW_SIZE=256
export HSTREE_NUM_TREES=25
export HSTREE_MAX_DEPTH=15
export IFOREST_INITIAL_WINDOW_SIZE=512
export IFOREST_WINDOW_SIZE=2048
export IFOREST_N_ESTIMATORS=100
export IFOREST_MAX_SAMPLES=256
export IFOREST_REFIT_EVERY=128
export ANALYSE_SEED=42
export DASMA_IFOREST_ENABLED
export DASMA_HSTREE_ENABLED
export DASMA_EXPLAINABILITY_ENABLED=1
export DASMA_EXPLAINABILITY_V2_ENABLED=1

BASE_COMPOSE=(docker compose -p "$PROJECT_NAME" -f docker-compose.yml -f docker/compose/experiments/docker-compose.iforest-treeshap.yml)
CONFIG_FILE="$(mktemp "${TMPDIR:-/tmp}/iforest-treeshap-compose.XXXXXX")"
cleanup() {
  "${COMPOSE[@]}" down --timeout 30 >/dev/null 2>&1 || true
  rm -f "$CONFIG_FILE"
}

"${BASE_COMPOSE[@]}" config --format json \
  | jq 'del(.services[].container_name)' > "$CONFIG_FILE"
COMPOSE=(docker compose -p "$PROJECT_NAME" -f "$CONFIG_FILE")
"${COMPOSE[@]}" config --quiet
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

EXPECTED_EVENTS="$(PYTHONPATH=src:. .venv/bin/python -c 'import csv; print(sum(1 for _ in csv.DictReader(open("data/source-5000.csv", newline="", encoding="utf-8-sig"))))')"
SOURCE_SHA256="$(shasum -a 256 data/source-5000.csv | awk '{print $1}')"
printf 'mode=%s\nmodel=%s\nmethod=%s\nproject=%s\ndataset=data/source-5000.csv\nrows=%s\nsource_sha256=%s\n' \
  "$MODE" "$MODEL" "$METHOD" "$PROJECT_NAME" "$EXPECTED_EVENTS" "$SOURCE_SHA256" > "$OUTPUT_DIR/campaign.txt"
printf 'started_utc=%s\n' "$(date -u +%FT%TZ)" >> "$OUTPUT_DIR/campaign.txt"

echo "[$MODE] starting isolated Compose project $PROJECT_NAME ($EXPECTED_EVENTS rows)"
"${COMPOSE[@]}" up --pull never -d "${SERVICES[@]}"

producer_id="$("${COMPOSE[@]}" ps --all -q producer)"
if [[ -z "$producer_id" ]]; then
  echo "Producer container did not start" >&2
  exit 1
fi
for ((attempt = 0; attempt < 600; attempt++)); do
  producer_state="$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$producer_id")"
  if [[ "$producer_state" == "exited 0" ]]; then break; fi
  if [[ "$producer_state" == exited* ]]; then
    echo "Producer ended unsuccessfully: $producer_state" >&2
    exit 1
  fi
  sleep 2
done
[[ "$producer_state" == "exited 0" ]] || { echo "Producer timeout" >&2; exit 1; }
echo "[$MODE] producer completed; waiting for consumer offsets and output delivery"

lag_for_group() {
  local group="$1" details
  details="$("${COMPOSE[@]}" exec -T kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka1:9092 --describe --group "$group" 2>/dev/null)" || return 1
  awk 'NF == 0 { next }
       !col { for (i = 1; i <= NF; i++) if ($i == "LAG") col = i; next }
       $col ~ /^[0-9]+$/ { total += $col; rows++ }
       END { if (!rows) exit 1; print total }' <<< "$details"
}

all_lag_zero=0
for ((attempt = 0; attempt < 600; attempt++)); do
  all_lag_zero=1
  for group in "${CONSUMER_GROUPS[@]}"; do
    lag="$(lag_for_group "$group" 2>/dev/null || true)"
    if [[ "$lag" != "0" ]]; then
      all_lag_zero=0
      printf '[%s] waiting: group=%s lag=%s\n' "$MODE" "$group" "${lag:-not-ready}"
      break
    fi
  done
  [[ "$all_lag_zero" == 1 ]] && break
  sleep 2
done
[[ "$all_lag_zero" == 1 ]] || { echo "Consumer lag timeout" >&2; exit 1; }

"${COMPOSE[@]}" stop --timeout 90 "${SERVICES[@]}" >/dev/null
"${COMPOSE[@]}" logs --no-color producer iforest-analysis explainability explainability-v2 \
  > "$OUTPUT_DIR/docker.log" 2>&1 || true

export_topic() {
  local topic="$1" target="$2"
  "${COMPOSE[@]}" exec -T kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka1:9092 --topic "$topic" --from-beginning \
    --max-messages "$EXPECTED_EVENTS" --timeout-ms 120000 > "$target"
  local lines
  lines="$(wc -l < "$target" | tr -d ' ')"
  [[ "$lines" == "$EXPECTED_EVENTS" ]] || {
    echo "Export $topic contains $lines lines; expected $EXPECTED_EVENTS" >&2
    return 1
  }
}

export_topic explainability-viz "$OUTPUT_DIR/v1.jsonl"
export_topic explainability-v2-viz "$OUTPUT_DIR/v2.jsonl"
if [[ "$MODE" == reference ]]; then
  export_topic "$DETECTOR_VIZ_TOPIC" "$OUTPUT_DIR/detector.jsonl"
fi
printf 'completed_utc=%s\n' "$(date -u +%FT%TZ)" >> "$OUTPUT_DIR/campaign.txt"
printf 'v1_sha256=%s\nv2_sha256=%s\n' \
  "$(shasum -a 256 "$OUTPUT_DIR/v1.jsonl" | awk '{print $1}')" \
  "$(shasum -a 256 "$OUTPUT_DIR/v2.jsonl" | awk '{print $1}')" >> "$OUTPUT_DIR/campaign.txt"
echo "[$MODE] exports complete in $OUTPUT_DIR"
