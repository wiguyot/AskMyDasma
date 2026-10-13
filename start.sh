#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="${ROOT_DIR}/.tmp"
BACKEND_HOST="${ASKMYDASMA_HOST:-127.0.0.1}"
BACKEND_PORT="${ASKMYDASMA_PORT:-8000}"
BACKEND_MODE="${ASKMYDASMA_BACKEND_MODE:-container}"
BACKEND_PID_FILE="${TMP_DIR}/askmydasma-backend.pid"
BACKEND_LOG_FILE="${TMP_DIR}/askmydasma-backend.log"
LAUNCH_PROGRESS_FILE="${TMP_DIR}/askmydasma-launch-progress.log"
BACKEND_START_TIMEOUT="${ASKMYDASMA_START_TIMEOUT:-20}"
AUTO_PREPARE_ON_START="${ASKMYDASMA_AUTO_PREPARE_ON_START:-1}"
START_DATASET_PATH="${ASKMYDASMA_START_DATASET_PATH:-data/source.csv}"
START_MODE="${ASKMYDASMA_START_MODE:-realtime-x100}"
BROKER_SERVICES=(
  kafka1
  kafka2
  kafka3
)
CORE_INFRA_SERVICES=(
  schema-registry
  influxdb
  grafana
)
AUX_INFRA_SERVICES=(
  influxdb-init-buckets
  influxdb-apply-template
)
POST_SCHEMA_SERVICES=(
  kafka-ui
)

mkdir -p "${TMP_DIR}"
cd "${ROOT_DIR}"

ensure_web_backend_python() {
  if [ -n "${ASKMYDASMA_PYTHON:-}" ]; then
    if "${ASKMYDASMA_PYTHON}" -c "import fastapi, uvicorn" >/dev/null 2>&1; then
      echo "${ASKMYDASMA_PYTHON}"
      return 0
    fi

    echo "[start] ASKMYDASMA_PYTHON is set but cannot import fastapi and uvicorn: ${ASKMYDASMA_PYTHON}" >&2
    return 1
  fi

  local venv_python="${ROOT_DIR}/.venv/bin/python"

  if [ -x "${venv_python}" ]; then
    if "${venv_python}" -c "import fastapi, uvicorn" >/dev/null 2>&1; then
      echo "${venv_python}"
      return 0
    fi

    "${ROOT_DIR}/.venv/bin/pip" install -e '.[web]' >&2
    echo "${venv_python}"
    return 0
  fi

  if python3 -c "import fastapi, uvicorn" >/dev/null 2>&1; then
    echo "python3"
    return 0
  fi

  python3.12 -m venv .venv
  "${ROOT_DIR}/.venv/bin/pip" install -e '.[web]' >&2
  echo "${venv_python}"
}

backend_running() {
  if [ ! -f "${BACKEND_PID_FILE}" ]; then
    return 1
  fi

  local pid
  pid="$(cat "${BACKEND_PID_FILE}")"
  if [ -z "${pid}" ]; then
    return 1
  fi

  if kill -0 "${pid}" >/dev/null 2>&1; then
    return 0
  fi

  rm -f "${BACKEND_PID_FILE}"
  return 1
}

start_backend() {
  rm -f "${BACKEND_PID_FILE}"

  "${BACKEND_PYTHON}" "${ROOT_DIR}/scripts/web/spawn_backend.py" \
    --root-dir "${ROOT_DIR}" \
    --host "${BACKEND_HOST}" \
    --port "${BACKEND_PORT}" \
    --python "${BACKEND_PYTHON}" \
    --pid-file "${BACKEND_PID_FILE}" \
    --log-file "${BACKEND_LOG_FILE}"
}

backend_process_running() {
  if [ "${BACKEND_MODE}" = "container" ]; then
    container_backend_running
  else
    backend_running
  fi
}

wait_for_backend() {
  local attempt
  for ((attempt=0; attempt<BACKEND_START_TIMEOUT; attempt++)); do
    if backend_process_running \
      && curl -fsS "http://${BACKEND_HOST}:${BACKEND_PORT}/health" >/dev/null 2>&1 \
      && curl -fsS "http://${BACKEND_HOST}:${BACKEND_PORT}/askmydasma/" >/dev/null 2>&1; then
      local stable_check
      for ((stable_check=0; stable_check<3; stable_check++)); do
        sleep 1
        if ! backend_process_running \
          || ! curl -fsS "http://${BACKEND_HOST}:${BACKEND_PORT}/health" >/dev/null 2>&1 \
          || ! curl -fsS "http://${BACKEND_HOST}:${BACKEND_PORT}/askmydasma/" >/dev/null 2>&1; then
          break
        fi
      done
      if [ "${stable_check}" -eq 3 ]; then
        return 0
      fi
    fi
    sleep 1
  done
  return 1
}

wait_for_container_health() {
  local container_name="$1"
  local timeout_s="$2"
  local attempt

  for ((attempt=0; attempt<timeout_s; attempt++)); do
    local state=""
    local health=""

    state="$(docker inspect -f '{{.State.Status}}' "${container_name}" 2>/dev/null || true)"
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "${container_name}" 2>/dev/null || true)"

    if [ "${state}" = "running" ] && [ "${health}" = "healthy" ]; then
      return 0
    fi

    sleep 1
  done

  return 1
}

ensure_compose_images() {
  bash "${ROOT_DIR}/scripts/docker/pull_missing_images.sh" "$@"
}

ensure_askmydasma_image() {
  local image="dasma-kafka-askmydasma:0.1.0"
  if docker image inspect "${image}" >/dev/null 2>&1; then
    echo "[images] present: ${image}"
  else
    echo "[images] building missing local image: ${image} (service: askmydasma-web)"
    docker compose --profile askmydasma-container build askmydasma-web
  fi
}

container_backend_running() {
  local state
  state="$(docker inspect -f '{{.State.Status}}' askmydasma-web 2>/dev/null || true)"
  [ "${state}" = "running" ]
}

LAUNCH_PROGRESS_LAST_LINE=0

flush_launch_progress() {
  local current_line_count=0
  if [ -f "${LAUNCH_PROGRESS_FILE}" ]; then
    current_line_count="$(wc -l < "${LAUNCH_PROGRESS_FILE}" 2>/dev/null || printf '0')"
  fi

  if [ "${current_line_count}" -gt "${LAUNCH_PROGRESS_LAST_LINE}" ]; then
    sed -n "$((LAUNCH_PROGRESS_LAST_LINE + 1)),$current_line_count p" "${LAUNCH_PROGRESS_FILE}" 2>/dev/null || true
    LAUNCH_PROGRESS_LAST_LINE="${current_line_count}"
  fi
}

auto_prepare_campaign() {
  if [ "${AUTO_PREPARE_ON_START}" = "0" ]; then
    echo "[start] Auto-prepare on start is disabled."
    return 0
  fi

  echo "[start] Preparing initial campaign..."
  echo "[start] launching campaign..."
  local payload
  payload="$(python3 -c 'import json, sys; print(json.dumps({"dataset_path": sys.argv[1], "mode": sys.argv[2]}))' "${START_DATASET_PATH}" "${START_MODE}")"
  local launch_response="${TMP_DIR}/askmydasma-launch-response.json"
  local launch_log="${TMP_DIR}/askmydasma-launch.log"
  local launch_pid

  rm -f "${launch_response}" "${launch_log}"
  : > "${LAUNCH_PROGRESS_FILE}"
  LAUNCH_PROGRESS_LAST_LINE=0
  (
    curl -fsS -X POST \
      -H "Content-Type: application/json" \
      -d "${payload}" \
      "http://${BACKEND_HOST}:${BACKEND_PORT}/api/campaigns/launch" \
      >"${launch_response}" 2>"${launch_log}"
  ) &
  launch_pid=$!

  local last_snapshot=""
  local elapsed=0
  while kill -0 "${launch_pid}" >/dev/null 2>&1; do
    flush_launch_progress
    local snapshot
    snapshot="$(
      curl -fsS "http://${BACKEND_HOST}:${BACKEND_PORT}/api/status" 2>/dev/null \
      | python3 -c 'import json, sys; data=json.load(sys.stdin); print(f"platform={data.get(\"platform_status\", \"-\")} campaign={data.get(\"campaign_status\", \"-\")} grafana={\"up\" if data.get(\"grafana_available\") else \"down\"}")' 2>/dev/null || true
    )"
    if [ -n "${snapshot}" ] && [ "${snapshot}" != "${last_snapshot}" ]; then
      echo "[start] ${snapshot}"
      last_snapshot="${snapshot}"
    fi
    if [ $((elapsed % 5)) -eq 0 ]; then
      if [ -n "${snapshot}" ]; then
        echo "[start] still preparing initial campaign... ${elapsed}s elapsed (${snapshot})"
      else
        echo "[start] still preparing initial campaign... ${elapsed}s elapsed"
      fi
    fi
    elapsed=$((elapsed + 1))
    sleep 1
  done
  flush_launch_progress

  if ! wait "${launch_pid}"; then
    echo "[start] Campaign preparation failed." >&2
    if [ -f "${launch_log}" ] && [ -s "${launch_log}" ]; then
      tail -n 40 "${launch_log}" >&2 || true
    fi
    return 1
  fi

  if [ -f "${launch_response}" ] && [ -s "${launch_response}" ]; then
    echo "[start] Initial campaign prepared. The producer will be created on Start production."
    return 0
  fi

  echo "[start] Initial campaign prepared, but the launch response was empty." >&2
  return 1
}

wait_for_platform_ready() {
  local attempt
  for ((attempt=0; attempt<BACKEND_START_TIMEOUT; attempt++)); do
    local platform_status
    platform_status="$(curl -fsS "http://${BACKEND_HOST}:${BACKEND_PORT}/api/status" 2>/dev/null | python3 -c 'import json, sys; print(json.load(sys.stdin).get("platform_status", ""))' 2>/dev/null || true)"
    if [ "${platform_status}" = "ready_for_production" ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "[start] checking prerequisites..."
echo "[start] Backend mode: ${BACKEND_MODE}"
echo "[start] Ensuring Docker images are available..."
# Every build-type image must exist on the HOST before the campaign launches.
# In container mode the askmydasma-web orchestrator can run docker compose
# up/exec/down (the host daemon resolves bind mounts via --project-directory),
# but it CANNOT build images: it has no buildx plugin and the build context
# path is the host path, which does not exist inside the container. So we
# pre-build/pull *all* compose images here, on the host, where buildx and the
# real build context are available. Passing no service filter makes the set
# the single source of truth (docker compose config) and avoids drift with the
# orchestrator's launch image list.
ensure_compose_images
if [ "${BACKEND_MODE}" = "container" ]; then
  ensure_askmydasma_image
fi

echo "[start] starting infra..."
echo "[start] Starting Kafka brokers..."
docker compose up --pull never -d "${BROKER_SERVICES[@]}"

echo "[start] Starting core infrastructure..."
docker compose up --pull never -d "${CORE_INFRA_SERVICES[@]}"

echo "[start] Waiting for schema-registry health..."
if ! wait_for_container_health "schema-registry" 90; then
  echo "[start] schema-registry failed to become healthy." >&2
  docker compose logs --no-color --tail 120 schema-registry >&2 || true
  exit 1
fi

echo "[start] Starting auxiliary services..."
docker compose up --pull never -d "${AUX_INFRA_SERVICES[@]}"

echo "[start] Starting Kafka UI..."
docker compose up --pull never -d "${POST_SCHEMA_SERVICES[@]}"

echo "[start] Preparing AskMyDasma backend..."
if [ "${BACKEND_MODE}" = "container" ]; then
  if container_backend_running; then
    echo "[start] AskMyDasma container already running."
  else
    echo "[start] Starting AskMyDasma container on http://${BACKEND_HOST}:${BACKEND_PORT} ..."
    docker compose --profile askmydasma-container up --pull never -d askmydasma-web
  fi
else
  BACKEND_PYTHON="$(ensure_web_backend_python)"
  if backend_running; then
    echo "[start] AskMyDasma backend already running (pid $(cat "${BACKEND_PID_FILE}"))."
  else
    echo "[start] Starting AskMyDasma backend on http://${BACKEND_HOST}:${BACKEND_PORT} ..."
    start_backend
  fi
fi

echo "[start] waiting for backend..."
if ! wait_for_backend; then
  echo "[start] AskMyDasma backend failed to start." >&2
  if [ "${BACKEND_MODE}" = "container" ]; then
    docker compose logs --no-color --tail 40 askmydasma-web >&2 || true
  else
    echo "[start] Last backend log lines:" >&2
    tail -n 40 "${BACKEND_LOG_FILE}" >&2 || true
  fi
  exit 1
fi

if ! auto_prepare_campaign; then
  echo "[start] Failed to prepare the initial campaign." >&2
  if [ "${BACKEND_MODE}" = "container" ]; then
    docker compose logs --no-color --tail 40 askmydasma-web >&2 || true
  else
    tail -n 40 "${BACKEND_LOG_FILE}" >&2 || true
  fi
  exit 1
fi

if [ "${AUTO_PREPARE_ON_START}" != "0" ]; then
  if ! wait_for_platform_ready; then
    echo "[start] Campaign preparation did not reach the ready_for_production state in time." >&2
    exit 1
  fi
fi

echo "[start] URLs:"
echo "  AskMyDasma:  http://${BACKEND_HOST}:${BACKEND_PORT}/askmydasma/"
echo "  API health:  http://${BACKEND_HOST}:${BACKEND_PORT}/health"
echo "  Grafana:     http://localhost:3000"
echo "  Kafka UI:    http://localhost:8080"
echo "  InfluxDB:    http://localhost:8086"
echo
if [ "${BACKEND_MODE}" = "container" ]; then
  echo "[start] Backend logs: docker compose logs -f askmydasma-web"
else
  echo "[start] Backend log: ${BACKEND_LOG_FILE}"
fi
