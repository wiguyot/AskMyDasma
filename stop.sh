#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="${ROOT_DIR}/.tmp"
BACKEND_PID_FILE="${TMP_DIR}/askmydasma-backend.pid"
COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME:-$(basename "${ROOT_DIR}")}"

compose() {
  docker compose \
    --project-directory "${ROOT_DIR}" \
    --project-name "${COMPOSE_PROJECT}" \
    --file "${ROOT_DIR}/docker-compose.yml" \
    "$@"
}

cd "${ROOT_DIR}"

stop_backend() {
  if [ ! -f "${BACKEND_PID_FILE}" ]; then
    echo "[stop] AskMyDasma backend not tracked."
    return 0
  fi

  local pid
  pid="$(cat "${BACKEND_PID_FILE}")"

  if [ -z "${pid}" ]; then
    rm -f "${BACKEND_PID_FILE}"
    echo "[stop] AskMyDasma backend PID file was empty."
    return 0
  fi

  if kill -0 "${pid}" >/dev/null 2>&1; then
    echo "[stop] Stopping AskMyDasma backend (pid ${pid})..."
    kill "${pid}" >/dev/null 2>&1 || true

    local attempt
    for ((attempt=0; attempt<10; attempt++)); do
      if ! kill -0 "${pid}" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done

    if kill -0 "${pid}" >/dev/null 2>&1; then
      echo "[stop] Backend did not stop gracefully, sending SIGKILL..."
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
  else
    echo "[stop] AskMyDasma backend PID ${pid} is no longer running."
  fi

  rm -f "${BACKEND_PID_FILE}"
}

cleanup_volumes() {
  local project="${COMPOSE_PROJECT_NAME:-$(basename "${ROOT_DIR}")}"
  local keep_project="${project}_pip-cache"
  local keep_global="pip-cache"
  local volumes=()
  local volume
  local label_project

  echo "[stop] Project = ${project}"
  echo "[stop] Removing project volumes except pip-cache..."

  while IFS= read -r volume; do
    [ -z "${volume}" ] && continue

    if [ "${volume}" = "${keep_project}" ] || [ "${volume}" = "${keep_global}" ]; then
      continue
    fi

    label_project="$(docker volume inspect "${volume}" --format '{{ index .Labels "com.docker.compose.project" }}' 2>/dev/null || true)"

    if [ "${label_project}" = "${project}" ] || [[ "${volume}" == "${project}_"* ]]; then
      volumes+=("${volume}")
    fi
  done < <(docker volume ls -q)

  if [ "${#volumes[@]}" -eq 0 ]; then
    echo "[stop] No labeled or prefixed project volume to remove, excluding pip-cache."
  else
    echo "[stop] Volumes to remove:"
    printf ' - %s\n' "${volumes[@]}"
    docker volume rm "${volumes[@]}" >/dev/null
  fi

  local all_volumes=()
  while IFS= read -r volume; do
    [ -z "${volume}" ] && continue
    if [ "${volume}" = "${keep_project}" ] || [ "${volume}" = "${keep_global}" ]; then
      continue
    fi
    all_volumes+=("${volume}")
  done < <(docker volume ls -q)

  if [ "${#all_volumes[@]}" -gt 0 ]; then
    echo "[stop] Cleaning remaining volumes except pip-cache..."

    local container_ids
    for volume in "${all_volumes[@]}"; do
      container_ids="$(docker ps -a -q --filter "volume=${volume}" --filter "status=exited" --filter "status=created" --filter "status=dead" || true)"
      if [ -n "${container_ids}" ]; then
        echo "[stop] Removing stopped containers that use volume ${volume}"
        docker rm -f ${container_ids} >/dev/null 2>&1 || true
      fi

      docker volume rm "${volume}" >/dev/null 2>&1 || true
    done
  fi

  echo "[stop] Removing remaining orphan volumes while keeping pip-cache..."
  local keep_container="keep-pip-cache-${project}"

  if ! docker volume inspect "${keep_project}" >/dev/null 2>&1; then
    docker volume create "${keep_project}" >/dev/null
  fi

  docker rm -f "${keep_container}" >/dev/null 2>&1 || true
  docker create --name "${keep_container}" -v "${keep_project}:/pip-cache" alpine:3.19 true >/dev/null
  docker volume prune -f >/dev/null
  docker rm -f "${keep_container}" >/dev/null 2>&1 || true
}

echo "[stop] Stopping AskMyDasma backend..."
stop_backend
if docker inspect askmydasma-web >/dev/null 2>&1; then
  echo "[stop] Stopping AskMyDasma container..."
  compose --profile askmydasma-container stop askmydasma-web >/dev/null 2>&1 || true
  compose --profile askmydasma-container rm -f askmydasma-web >/dev/null 2>&1 || true
fi

echo "[stop] Stopping Docker stack..."
compose --profile askmydasma-container down --remove-orphans

cleanup_volumes

echo "[stop] Done."
