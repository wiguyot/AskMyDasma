#!/usr/bin/env bash
# OC-15 explainability-v2 minimal validation (non-destructive runner).
set -euo pipefail
cd "$(dirname "$0")/../.."
PYTHONPATH=src:. .venv/bin/python scripts/integration/run-explainability-v2-minimal-validation.py "$@"
