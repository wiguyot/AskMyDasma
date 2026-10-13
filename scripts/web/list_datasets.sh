#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
DATA_DIR="$ROOT_DIR/data"

if [ ! -d "$DATA_DIR" ]; then
  printf '{"datasets":[]}\n'
  exit 0
fi

find "$DATA_DIR" -maxdepth 1 -type f -name '*.csv' | sort | while read -r path; do
  rel="${path#"$ROOT_DIR"/}"
  printf '%s\n' "$rel"
done | python3 -c 'import json,sys; print(json.dumps({"datasets":[line.strip() for line in sys.stdin if line.strip()]}, ensure_ascii=False))'
