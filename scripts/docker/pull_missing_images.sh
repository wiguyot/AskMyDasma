#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT_DIR}"

if [ "$#" -gt 0 ]; then
  CONFIG_CMD=(docker compose config --format json "$@")
else
  CONFIG_CMD=(docker compose config --format json)
fi

SERVICE_IMAGE_ROWS="$(
  "${CONFIG_CMD[@]}" | python3 -c '
import json
import sys

data = json.load(sys.stdin)
for service_name, service_config in sorted(data.get("services", {}).items()):
    image = service_config.get("image")
    if not image:
        continue
    has_build = "1" if "build" in service_config else "0"
    print(f"{service_name}\t{image}\t{has_build}")
'
)"

while IFS=$'\t' read -r service_name image has_build; do
  if [ -z "${image}" ]; then
    continue
  fi

  if docker image inspect "${image}" >/dev/null 2>&1; then
    echo "[images] present: ${image}"
    continue
  fi

  if [ "${has_build}" = "1" ]; then
    echo "[images] building missing local image: ${image} (service: ${service_name})"
    docker compose build "${service_name}"
    if ! docker image inspect "${image}" >/dev/null 2>&1; then
      echo "[images] build completed but image is still unavailable: ${image}" >&2
      exit 1
    fi
    continue
  fi

  echo "[images] pulling missing image: ${image}"
  docker pull "${image}"
done <<< "${SERVICE_IMAGE_ROWS}"
