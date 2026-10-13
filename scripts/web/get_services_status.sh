#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

docker compose ps --format json | python3 -c '
import json, sys

raw = sys.stdin.read().strip()
if not raw:
    print("[]")
    raise SystemExit(0)

services = []
for line in raw.splitlines():
    item = json.loads(line)
    state = (item.get("State") or "").lower()
    health = (item.get("Health") or "").lower()
    health_view = health or "not_configured"

    if state == "running" and health == "healthy":
        status = "healthy"
    elif state == "running":
        status = "running"
    elif state in {"created", "restarting"}:
        status = "starting"
    elif state in {"exited", "dead", "removing"}:
        status = "stopped"
    elif not state:
        status = "unknown"
    else:
        status = "error"

    services.append(
        {
            "name": item.get("Service"),
            "container_name": item.get("Name"),
            "state": state or "unknown",
            "health": health_view,
            "status": status,
        }
    )

print(json.dumps(services, ensure_ascii=False))
'
