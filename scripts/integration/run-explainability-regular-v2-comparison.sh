#!/usr/bin/env bash
# OC-13 controlled regular/v2 comparison (non-destructive runner).
set -euo pipefail
cd "$(dirname "$0")/../.."
PYTHONPATH=src:. .venv/bin/python scripts/integration/run-explainability-regular-v2-comparison.py "$@"
