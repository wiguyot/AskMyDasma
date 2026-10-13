#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOST="${ASKMYDASMA_HOST:-0.0.0.0}"
PORT="${ASKMYDASMA_PORT:-8000}"
PYTHON_BIN="${ASKMYDASMA_PYTHON:-}"

cd "${ROOT_DIR}"

if [ -z "${PYTHON_BIN}" ]; then
  if [ -x "${ROOT_DIR}/.venv/bin/python" ]; then
    PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

if ! "${PYTHON_BIN}" -c "import fastapi, uvicorn" >/dev/null 2>&1; then
  cat >&2 <<'EOF'
AskMyDasma backend dependencies are missing.

Install them with:
  python3.12 -m venv .venv
  .venv/bin/pip install -e .[web]

Then rerun:
  bash scripts/web/run_backend.sh
EOF
  exit 1
fi

exec "${PYTHON_BIN}" -m uvicorn \
  --app-dir src \
  --host "${HOST}" \
  --port "${PORT}" \
  dasma_kafka.web.app:app
