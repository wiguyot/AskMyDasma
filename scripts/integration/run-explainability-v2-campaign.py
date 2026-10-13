"""OC-12C — non-destructive Docker runner for the explainability-v2 campaign.

Chain under test:
CSV fixture -> oc12 producer -> oc12 hstree -> oc12 explainability-v2 ->
oc12 JSON viz topics -> oc12 Telegraf -> oc12-campaign InfluxDB bucket, then
OC-05 reconciliation + OC-12 validation.

Non-destructive by construction (OC-12R):
- every created resource carries the OC-12 run id: topics, consumer groups,
  Avro subjects (TopicNameStrategy derives subjects from the dedicated topic
  names) and the InfluxDB measurement;
- the runner NEVER deletes topics, consumer groups, schema subjects or
  InfluxDB data;
- the runner REFUSES to start when the standard compute services are running,
  instead of stopping them;
- only the oc12-* services are started and stopped.

This script is never executed by pytest. Completeness never depends on a log
line: it requires the producer to have exited, the expected message counts on
both viz topics, a zero oc12-v2 consumer lag, and the InfluxDB proof holding
both simultaneous source_event_id values plus the `explanation_status` (string)
and `hstree_anomaly` (bool) fields.

Usage:
    PYTHONPATH=src:. .venv/bin/python \
        scripts/integration/run-explainability-v2-campaign.py \
        [--artifacts-dir DIR] [--keep-up] [--global-timeout S]

Artifacts are always preserved: they are the campaign proof.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2"
VALIDATOR = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "validate_oc12_campaign.py"
DASHBOARD = PROJECT_ROOT / "observability" / "grafana" / "dashboards" / "explainability_v2_influxdb.json"
STATUS_PANEL_ID = 4
VERDICT_PANEL_ID = 5

BASE_COMPOSE = ["-f", "docker-compose.yml"]
OC12_COMPOSE = BASE_COMPOSE + ["-f", "docker/compose/experiments/docker-compose.oc12.yml"]

# Standard compute services: if any of them is running, the runner refuses to
# start (it never stops or restarts shared resources).
STANDARD_COMPUTE_SERVICES = ["producer", "hstree-analysis", "explainability-v2", "telegraf"]

# Infrastructure services: started if missing, never stopped by the runner.
INFRA_SERVICES = ["kafka1", "kafka2", "kafka3", "schema-registry", "influxdb"]

OC12_SERVICES = ["oc12-producer", "oc12-hstree", "oc12-v2", "oc12-telegraf"]

OC12_TOPICS = [
    "oc12-source-{run_id}",
    "oc12-hstree-{run_id}",
    "oc12-hstree-viz-{run_id}",
    "oc12-v2-{run_id}",
    "oc12-v2-viz-{run_id}",
]

INFLUX_URL = "http://localhost:8086"
INFLUX_ORG = "dasma"
INFLUX_TOKEN = "dasma-super-token"
INFLUX_BUCKET = "oc12-campaign"


class RunnerError(RuntimeError):
    """Fatal campaign error; artifacts are preserved for diagnosis."""


def log(message: str) -> None:
    print(f"[oc12 {dt.datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def generate_run_id(now: Optional[dt.datetime] = None) -> str:
    """Build a unique OC-12 run id: oc12-YYYYMMDD-HHMMSS-<8 hex chars>.

    The timestamp keeps artifacts sortable; the random suffix makes
    concurrent or same-second runs unique. Only lowercase letters, digits
    and dashes: valid for Kafka topics/groups, Docker container names and
    InfluxDB measurements.
    """
    stamp = (now or dt.datetime.now()).strftime("%Y%m%d-%H%M%S")
    return f"oc12-{stamp}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Subprocess helpers (bounded timeouts everywhere)
# ---------------------------------------------------------------------------


def run_command(
    command: List[str],
    *,
    timeout: float,
    env_extra: Optional[Dict[str, str]] = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    merged_env = os.environ.copy()
    if env_extra:
        merged_env.update(env_extra)
    try:
        result = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            env=merged_env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RunnerError(f"command timed out after {timeout}s: {' '.join(command)}") from exc
    if check and result.returncode != 0:
        raise RunnerError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stderr: {result.stderr[-2000:]}"
        )
    return result


def compose(
    *args: str,
    timeout: float,
    files: Optional[List[str]] = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    return run_command(
        ["docker", "compose", *(files or BASE_COMPOSE), *args],
        timeout=timeout,
        env_extra=campaign_env(),
        check=check,
    )


def wait_until(
    predicate: Callable[[], bool],
    timeout: float,
    description: str,
    interval: float = 2.0,
) -> None:
    deadline = time.monotonic() + timeout
    last_error: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except RunnerError:
            raise
        except Exception as exc:  # transient docker/kafka errors are retried
            last_error = str(exc)
        time.sleep(interval)
    detail = f" (last error: {last_error})" if last_error else ""
    raise RunnerError(f"timeout after {timeout:.0f}s waiting for {description}{detail}")


# ---------------------------------------------------------------------------
# Campaign naming (everything derives from the run id)
# ---------------------------------------------------------------------------

_RUN_ID: str = ""


def campaign_env() -> Dict[str, str]:
    run_id = _RUN_ID or "unset"
    return {
        "OC12_RUN_ID": run_id,
        "OC12_V2_VIZ_TOPIC": f"oc12-v2-viz-{run_id}",
        "OC12_TELEGRAF_GROUP": f"oc12-telegraf-consumer-{run_id}",
        "OC12_MEASUREMENT": f"oc12-v2-{run_id}",
        "INFLUX_TOKEN": INFLUX_TOKEN,
    }


# ---------------------------------------------------------------------------
# Kafka state helpers (CLI inside the kafka1 container)
# ---------------------------------------------------------------------------


def topic_message_count(topic: str) -> int:
    result = compose(
        "exec", "-T", "kafka1",
        "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "kafka1:9092",
        "--topic", topic,
        timeout=30.0,
    )
    total = 0
    for line in result.stdout.splitlines():
        parts = line.strip().split(":")
        if len(parts) >= 3:
            try:
                total += max(int(parts[-1]), 0)
            except ValueError:
                continue
    return total


def consumer_group_lag(group: str, topic: str) -> Optional[int]:
    """Sum the LAG column for `topic`; Kafka 3.9 refuses --topic with
    --describe, so the full group description is read and rows are filtered on
    the TOPIC column. None when the output is ambiguous."""
    result = compose(
        "exec", "-T", "kafka1",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "kafka1:9092",
        "--describe", "--group", group,
        timeout=30.0,
        check=False,
    )
    lines = result.stdout.splitlines()
    header_index = None
    topic_index = None
    lag_index = None
    for index, line in enumerate(lines):
        tokens = line.split()
        upper_tokens = [token.upper() for token in tokens]
        if "LAG" in upper_tokens:
            header_index = index
            lag_index = upper_tokens.index("LAG")
            topic_index = upper_tokens.index("TOPIC")
            break
    if header_index is None:
        return None
    total = 0
    topic_seen = False
    for line in lines[header_index + 1 :]:
        tokens = line.split()
        if not tokens:
            continue
        if len(tokens) <= lag_index or len(tokens) <= topic_index:
            return None
        if tokens[topic_index] != topic:
            continue
        topic_seen = True
        if not tokens[lag_index].isdigit():
            return None
        total += int(tokens[lag_index])
    return total if topic_seen else None


def producer_finished() -> bool:
    result = compose(
        "ps", "--all", "--format", "json", "oc12-producer",
        timeout=30.0, check=False, files=OC12_COMPOSE,
    )
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("Service") == "oc12-producer":
            return payload.get("State") == "exited" and int(payload.get("ExitCode") or 0) == 0
    return False


def create_oc12_topics() -> None:
    for template in OC12_TOPICS:
        topic = template.format(run_id=_RUN_ID)
        compose(
            "exec", "-T", "kafka1",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "kafka1:9092",
            "--create", "--if-not-exists",
            "--topic", topic,
            "--partitions", "1",
            "--replication-factor", "3",
            timeout=30.0,
        )
        log(f"topic ready: {topic}")


# ---------------------------------------------------------------------------
# Docker guards and lifecycle (never destructive)
# ---------------------------------------------------------------------------


def services_running(services: List[str], files: Optional[List[str]] = None) -> List[str]:
    result = compose("ps", "--all", "--format", "json", *services, timeout=30.0, check=False, files=files)
    running = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("State") == "running":
            running.append(payload.get("Service"))
    return running


def oc12_containers_running() -> bool:
    return bool(services_running(OC12_SERVICES, files=OC12_COMPOSE))


def stop_oc12_services() -> None:
    """Stop and remove ONLY the oc12-* campaign services.

    Shared resources are never touched: topics, consumer groups, Avro
    subjects and the oc12-campaign InfluxDB data stay in place for manual
    inspection.
    """
    compose("stop", *OC12_SERVICES, timeout=120.0, check=False, files=OC12_COMPOSE)
    compose("rm", "-f", *OC12_SERVICES, timeout=60.0, check=False, files=OC12_COMPOSE)
    try:
        wait_until(
            lambda: not oc12_containers_running(),
            timeout=60.0,
            description="all oc12 containers stopped and removed",
            interval=1.0,
        )
    except RunnerError as exc:
        # A cleanup problem must never mask the campaign result.
        print(f"[oc12] WARNING: cleanup incomplete: {exc}", file=sys.stderr)


def assert_standard_services_stopped() -> None:
    running = services_running(STANDARD_COMPUTE_SERVICES)
    if running:
        raise RunnerError(
            "standard compute services are still running: "
            + ", ".join(running)
            + ". Stop them first; the OC-12 runner never stops shared services."
        )


# ---------------------------------------------------------------------------
# InfluxDB helpers (HTTP API from the host; the port is published)
# ---------------------------------------------------------------------------


def influx_request(method: str, path: str, body: Optional[bytes] = None, accept: str = "application/json") -> str:
    request = urllib.request.Request(
        INFLUX_URL + path,
        data=body,
        method=method,
        headers={
            "Authorization": f"Token {INFLUX_TOKEN}",
            "Content-Type": "application/json",
            "Accept": accept,
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read().decode("utf-8")


def ensure_oc12_bucket() -> None:
    """Create the dedicated oc12-campaign bucket if missing (create-only)."""
    orgs = json.loads(influx_request("GET", "/api/v2/orgs?org=" + INFLUX_ORG))
    org_id = orgs["orgs"][0]["id"]
    body = json.dumps({"name": INFLUX_BUCKET, "orgID": org_id, "retentionRules": []}).encode("utf-8")
    request = urllib.request.Request(
        INFLUX_URL + "/api/v2/buckets",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Token {INFLUX_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        urllib.request.urlopen(request, timeout=30)
        log(f"bucket created: {INFLUX_BUCKET}")
    except urllib.error.HTTPError as exc:
        if exc.code in (409, 422):
            log(f"bucket already present: {INFLUX_BUCKET}")
        else:
            raise


def query_influx_identity(
    measurement: str,
    group: str,
    shared_timestamp: str,
    simultaneous_ids: List[str],
    artifacts: Path,
) -> Tuple[List[Dict[str, str]], Dict[str, List[str]]]:
    """Fetch the identity proof points and the per-event field inventory."""
    moment = dt.datetime.strptime(shared_timestamp, "%Y-%m-%d %H:%M:%S")
    start = (moment - dt.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    stop = (moment + dt.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ids_filter = " or ".join(f'r.source_event_id == "{event_id}"' for event_id in simultaneous_ids)
    query = (
        f'from(bucket: "{INFLUX_BUCKET}")\n'
        f"  |> range(start: {start}, stop: {stop})\n"
        f'  |> filter(fn: (r) => r._measurement == "{measurement}")\n'
        f"  |> filter(fn: (r) => {ids_filter})\n"
        '  |> keep(columns: ["_time", "Group", "source_event_id", "_field", "_value"])\n'
    )
    csv_text = influx_request(
        "POST",
        f"/api/v2/query?org={INFLUX_ORG}",
        body=json.dumps({"query": query, "type": "flux"}).encode("utf-8"),
        accept="application/csv",
    )
    (artifacts / "influxdb-proof-query.flux").write_text(query, encoding="utf-8")
    (artifacts / "influxdb-proof-raw.csv").write_text(csv_text, encoding="utf-8")

    points: List[Dict[str, str]] = []
    fields_by_event: Dict[str, List[str]] = {}
    header: Optional[List[str]] = None
    for line in csv_text.splitlines():
        if not line or line.startswith("#"):
            continue
        cells = line.split(",")
        # Annotated CSV: every table repeats its header (which contains the
        # time column marker, _time or _start depending on the kept columns).
        if "_time" in cells or "_start" in cells:
            header = cells
            continue
        if header is None or len(cells) != len(header):
            continue
        row = dict(zip(header, cells))
        event_id = row.get("source_event_id", "")
        fields_by_event.setdefault(event_id, []).append(row.get("_field", ""))
        points.append(
            {
                "timestamp": str(row.get("_time", "")).replace("T", " ")[:19],
                "Group": row.get("Group", ""),
                "source_event_id": event_id,
                "_field": row.get("_field", ""),
                "_value": row.get("_value", ""),
            }
        )
    (artifacts / "influxdb-proof.json").write_text(
        json.dumps(
            {"query": query, "points": points, "fields_by_event": fields_by_event},
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return points, fields_by_event


# ---------------------------------------------------------------------------
# Exports and validation
# ---------------------------------------------------------------------------


def export_jsonl_topic(topic: str, output_path: Path, expected: int, timeout: float) -> int:
    result = compose(
        "exec", "-T", "kafka1",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "kafka1:9092",
        "--from-beginning",
        "--timeout-ms", "5000",
        "--topic", topic,
        timeout=timeout,
    )
    raw_path = output_path.with_suffix(".raw")
    raw_path.write_text(result.stdout, encoding="utf-8")

    valid_lines = 0
    invalid_lines = 0
    with open(output_path, "w", encoding="utf-8") as handle:
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                json.loads(stripped)
            except json.JSONDecodeError:
                invalid_lines += 1
                continue
            handle.write(stripped + "\n")
            valid_lines += 1
    if invalid_lines:
        raise RunnerError(
            f"export of {topic} contains {invalid_lines} invalid JSON lines "
            f"(raw output kept at {raw_path})"
        )
    if valid_lines != expected:
        raise RunnerError(
            f"export of {topic} holds {valid_lines} events, expected {expected}"
        )
    return valid_lines


def dump_service_logs(artifacts: Path) -> None:
    try:
        result = compose(
            "logs", "--tail", "60", *OC12_SERVICES,
            timeout=60.0, check=False, files=OC12_COMPOSE,
        )
        (artifacts / "services-failure-logs.txt").write_text(result.stdout, encoding="utf-8")
        print(f"[oc12] failure logs saved to {artifacts / 'services-failure-logs.txt'}")
    except Exception as exc:
        print(f"[oc12] could not dump service logs: {exc}")


# ---------------------------------------------------------------------------
# Grafana panel proof (real Flux execution on campaign data)
# ---------------------------------------------------------------------------


def _panel_queries(panel: Dict) -> List[str]:
    return [target["query"] for target in panel.get("targets", []) if "query" in target]


def load_dashboard_panels() -> Dict[int, Dict]:
    data = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    return {panel["id"]: panel for panel in data["panels"]}


def adapt_dashboard_query(query: str, oc12_bucket: str, oc12_measurement: str, group: str, range_start: str, range_stop: str) -> str:
    """Adapt one dashboard query to the OC-12 context without touching the
    dashboard source, then refuse any unresolved Grafana placeholder."""
    adapted = query.replace('from(bucket: "explainability-v2")', f'from(bucket: "{oc12_bucket}")')
    adapted = adapted.replace('r._measurement == "explainability-v2"', f'r._measurement == "{oc12_measurement}"')
    adapted = adapted.replace('"${group}"', f'"{group}"')
    adapted = adapted.replace("v.timeRangeStart", range_start)
    adapted = adapted.replace("v.timeRangeStop", range_stop)
    if "${" in adapted or "v.timeRange" in adapted:
        raise RunnerError(
            "refusing to execute a dashboard query with unresolved Grafana "
            f"placeholders: {adapted[:200]}"
        )
    return adapted


def _parse_flux_csv(csv_text: str) -> Tuple[Optional[List[str]], List[List[str]]]:
    """Parse a uniform Flux CSV: first non-comment line is the header,
    repeated identical header lines (one per table) are skipped."""
    lines = [line for line in csv_text.splitlines() if line.strip() and not line.startswith("#")]
    if not lines:
        return None, []
    header = lines[0].split(",")
    rows = []
    for line in lines[1:]:
        if line == lines[0]:
            continue  # repeated table header
        cells = line.split(",")
        if len(cells) == len(header):
            rows.append(cells)
    return header, rows


def run_grafana_panel_proof(
    measurement: str,
    group: str,
    shared_timestamp: str,
    artifacts: Path,
) -> Dict:
    """Execute the REAL dashboard panel 4 and 5 Flux queries against the
    OC-12 campaign data and check the exact fixture expectations."""
    panels = load_dashboard_panels()
    moment = dt.datetime.strptime(shared_timestamp, "%Y-%m-%d %H:%M:%S")
    range_start = (moment - dt.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    range_stop = (moment + dt.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

    expected_panel4 = {"warmup": 99, "computed": 11}
    expected_panel5 = {
        "normal": {"published": 6, "computed_count": 6, "rate": 1.0},
        "anomaly": {"published": 3, "computed_count": 3, "rate": 1.0},
    }

    summary: Dict = {"panels": {}, "mismatches": []}
    expectations = {
        STATUS_PANEL_ID: ("grafana-panel-4", "status_counts", expected_panel4),
        VERDICT_PANEL_ID: ("grafana-panel-5", "verdicts", expected_panel5),
    }
    for panel_id, (artifact_stem, result_name, expected) in expectations.items():
        panel = panels.get(panel_id)
        if panel is None:
            raise RunnerError(f"dashboard panel {panel_id} not found in {DASHBOARD.name}")
        queries = _panel_queries(panel)
        if len(queries) != 1:
            raise RunnerError(f"dashboard panel {panel_id} must hold exactly one Flux query")
        adapted = adapt_dashboard_query(
            queries[0], INFLUX_BUCKET, measurement, group, range_start, range_stop
        )
        (artifacts / f"{artifact_stem}.flux").write_text(adapted, encoding="utf-8")
        csv_text = influx_request(
            "POST",
            f"/api/v2/query?org={INFLUX_ORG}",
            body=json.dumps({"query": adapted, "type": "flux"}).encode("utf-8"),
            accept="application/csv",
        )
        (artifacts / f"{artifact_stem}.csv").write_text(csv_text, encoding="utf-8")

        header, rows = _parse_flux_csv(csv_text)
        if header is None:
            summary["panels"][result_name] = {}
            summary["mismatches"].append(f"panel {panel_id} returned no rows")
            continue

        if panel_id == STATUS_PANEL_ID:
            got = {}
            for cells in rows:
                row = dict(zip(header, cells))
                got[row["status"]] = int(float(row["_value"]))
            summary["panels"][result_name] = got
            if got != expected:
                summary["mismatches"].append(
                    f"panel {panel_id} status counts {got} != expected {expected}"
                )
        else:
            got = {}
            for cells in rows:
                row = dict(zip(header, cells))
                got[row["verdict"]] = {
                    "published": int(row["published"]),
                    "computed_count": int(row["computed_count"]),
                    "rate": round(float(row["computed_rate"]), 6),
                }
            summary["panels"][result_name] = got
            if got != expected:
                summary["mismatches"].append(
                    f"panel {panel_id} verdicts {got} != expected {expected}"
                )

    (artifacts / "grafana-panels-summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return summary


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    global _RUN_ID
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--artifacts-dir", default=None, help="artifacts directory (default: /tmp/oc12-campaign-<run-id>)")
    parser.add_argument("--keep-up", action="store_true", help="keep the oc12 services running after the run (success AND failure) for manual inspection")
    parser.add_argument("--global-timeout", type=float, default=900.0, help="global campaign deadline in seconds")
    args = parser.parse_args(argv)

    _RUN_ID = generate_run_id()
    manifest_path = FIXTURE_DIR / "oc12_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = int(manifest["expected_total_events"])
    simultaneous_ids = list(manifest["simultaneous_ids"])
    shared_group = manifest["simultaneous_group"]
    shared_timestamp = manifest["simultaneous_timestamp"]
    measurement = campaign_env()["OC12_MEASUREMENT"]
    v2_group = f"oc12-v2-consumer-{_RUN_ID}"
    hstree_viz_topic = f"oc12-hstree-viz-{_RUN_ID}"
    v2_viz_topic = f"oc12-v2-viz-{_RUN_ID}"
    hstree_topic = f"oc12-hstree-{_RUN_ID}"

    artifacts = Path(args.artifacts_dir) if args.artifacts_dir else Path("/tmp") / f"oc12-campaign-{_RUN_ID}"
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "campaign-env.json").write_text(
        json.dumps(campaign_env(), indent=2) + "\n", encoding="utf-8"
    )
    log(f"run id: {_RUN_ID} | artifacts: {artifacts}")

    deadline = time.monotonic() + args.global_timeout

    def remaining(cap: float = 240.0) -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise RunnerError(f"global timeout of {args.global_timeout}s exceeded")
        return min(left, cap)

    error_message: Optional[str] = None
    oc12_started = False
    try:
        log("checking that the standard compute services are stopped")
        assert_standard_services_stopped()

        log("ensuring infrastructure is up (never stopped by this runner)")
        compose("up", "-d", *INFRA_SERVICES, timeout=remaining())
        ensure_oc12_bucket()

        create_oc12_topics()

        log("starting the oc12 campaign services")
        # Tracked so the finally below can clean up even when this very
        # command fails partway through.
        oc12_started = True
        compose("up", "-d", *OC12_SERVICES, timeout=remaining(), files=OC12_COMPOSE)

        log("waiting for the oc12 producer to finish")
        wait_until(producer_finished, remaining(), "producer completion")
        log(f"waiting for {expected} events in {hstree_viz_topic}")
        wait_until(
            lambda: topic_message_count(hstree_viz_topic) >= expected,
            remaining(),
            f"{expected} events in {hstree_viz_topic}",
        )
        log(f"waiting for {expected} events in {v2_viz_topic}")
        wait_until(
            lambda: topic_message_count(v2_viz_topic) >= expected,
            remaining(),
            f"{expected} events in {v2_viz_topic}",
        )
        log(f"waiting for a zero {v2_group} lag")
        wait_until(
            lambda: consumer_group_lag(v2_group, hstree_topic) == 0,
            remaining(),
            "zero v2 consumer lag",
        )

        log("exporting the two oc12 JSON topics")
        hstree_count = export_jsonl_topic(
            hstree_viz_topic, artifacts / "hstree-analysis-viz.jsonl", expected, remaining(120.0)
        )
        v2_count = export_jsonl_topic(
            v2_viz_topic, artifacts / "explainability-v2-viz.jsonl", expected, remaining(120.0)
        )
        log(f"exported hstree={hstree_count} v2={v2_count}")

        log("waiting for the InfluxDB proof (identity + field types)")
        def proof_ready() -> bool:
            points, fields_by_event = query_influx_identity(
                measurement, shared_group, shared_timestamp, simultaneous_ids, artifacts
            )
            ids_with_points = {point["source_event_id"] for point in points}
            both_ids = all(event_id in ids_with_points for event_id in simultaneous_ids)
            fields_ok = all(
                set(fields_by_event.get(event_id, []))
                >= {"explanation_status", "hstree_anomaly"}
                for event_id in simultaneous_ids
            )
            return both_ids and fields_ok

        wait_until(proof_ready, remaining(180.0), "the InfluxDB identity and type proof", interval=5.0)

        log("executing the real Grafana panel 4 and 5 Flux queries")
        grafana_summary = run_grafana_panel_proof(measurement, shared_group, shared_timestamp, artifacts)
        if grafana_summary["mismatches"]:
            raise RunnerError(
                "Grafana panel proof mismatched the fixture: "
                + "; ".join(grafana_summary["mismatches"])
            )

        log("running the OC-12 validator")
        validator_result = run_command(
            [
                sys.executable,
                str(VALIDATOR),
                "--manifest", str(manifest_path),
                "--hstree-export", str(artifacts / "hstree-analysis-viz.jsonl"),
                "--v2-export", str(artifacts / "explainability-v2-viz.jsonl"),
                "--influx-proof", str(artifacts / "influxdb-proof.json"),
                "--output", str(artifacts / "oc12-validation-report.json"),
            ],
            timeout=remaining(120.0),
            check=False,
        )
        if validator_result.returncode != 0:
            raise RunnerError("OC-12 validator failed (see oc12-validation-report.json)")

        log(f"artifacts: {artifacts}")
    except RunnerError as exc:
        error_message = str(exc)
    except Exception as exc:  # the runner must never hang or fail silently
        error_message = f"unexpected failure: {exc}"
    finally:
        if error_message is not None:
            print(f"[oc12] FAILED: {error_message}", file=sys.stderr)
            # Diagnostics are written BEFORE the cleanup so a stopped service
            # can no longer report its state.
            try:
                dump_service_logs(artifacts)
            except Exception as log_exc:
                print(f"[oc12] could not dump service logs: {log_exc}", file=sys.stderr)
        if oc12_started and not args.keep_up:
            # Default behavior: the oc12-* containers are cleaned up on
            # success AND on failure. Only --keep-up keeps them running.
            # A cleanup failure never masks the campaign result above.
            try:
                log("stopping the oc12 campaign services (only oc12-* services)")
                stop_oc12_services()
            except Exception as cleanup_exc:
                print(f"[oc12] WARNING: cleanup failed: {cleanup_exc}", file=sys.stderr)
        if error_message is not None:
            print(f"[oc12] artifacts kept for diagnosis: {artifacts}", file=sys.stderr)
            return 1

    log(f"OC-12 campaign PASSED — artifacts: {artifacts}")
    print((artifacts / "oc12-validation-report.json").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
