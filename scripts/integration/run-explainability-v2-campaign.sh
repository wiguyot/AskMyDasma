#!/usr/bin/env bash
# OC-12 explainability-v2 integration campaign (non-destructive runner).
set -euo pipefail
cd "$(dirname "$0")/../.."
PYTHONPATH=src:. .venv/bin/python scripts/integration/run-explainability-v2-campaign.py "$@"
