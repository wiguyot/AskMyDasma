#!/usr/bin/env bash
# OC-14 explainability-v2 capacity campaign (non-destructive runner).
set -euo pipefail
cd "$(dirname "$0")/../.."
PYTHONPATH=src:. .venv/bin/python scripts/integration/run-explainability-v2-capacity.py "$@"
