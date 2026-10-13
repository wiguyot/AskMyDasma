#!/usr/bin/env bash
# V1-06 controlled V1/V2 parity witness campaign (non-destructive runner).
set -euo pipefail
cd "$(dirname "$0")/../.."
PYTHONPATH=src:. .venv/bin/python scripts/integration/run-explainability-v1-v2-parity.py "$@"
