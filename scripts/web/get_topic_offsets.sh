#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

if [ "$#" -eq 0 ]; then
  printf '{}\n'
  exit 0
fi

docker compose exec -T kafka1 bash -lc '
set -euo pipefail

for topic in "$@"; do
  output="$(/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka1:9092 --topic "$topic" 2>/dev/null || true)"
  printf "%s\t%s\n" "$topic" "$output"
done
' bash "$@" | python3 -c '
import json
import sys

offsets = {}
for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue
    topic, _, payload = line.partition("\t")
    if not topic:
        continue
    offset = None
    if payload:
        parts = payload.split(":")
        if len(parts) >= 3:
            try:
                offset = int(parts[-1])
            except ValueError:
                offset = None
    offsets[topic] = offset

print(json.dumps(offsets, ensure_ascii=False))
'
