#!/usr/bin/env python3
"""V1-06 — non-destructive Docker runner for the V1/V2 parity witness campaign.

Chain under test:
CSV fixture (OC-12 collision campaign) -> v1v2 producer -> v1v2 hstree ->
{v1v2 regular (V1), v1v2 v2} -> dedicated JSON viz topics -> raw exports ->
V1-05c parity CLI (experiments/explainability/v1_v2/parity_comparator.py).

Scientific scope: exact V1/V2 parity on the exclusive identity
``(Group, source_event_id)`` — NEVER on the timestamp — including the OC-12
deliberate collision pair (same group, same timestamp, distinct
source_event_id). The comparison decision NEVER depends on a log line: it
comes from the deterministic parity report and verdict written by the V1-05c
CLI on the raw Kafka JSON exports.

Difference with the frozen OC-13 runner
(scripts/integration/run-explainability-regular-v2-comparison.py): OC-13
compares on (Group, timestamp) with its own comparator and MUST stay
unchanged for its historical witness value; this runner shares its Docker
orchestration pattern but not its comparison step.

Non-destructive by construction (OC-12R.1 guarantees):
- every created resource carries the V1-06 run id: topics, consumer groups
  and Avro subjects (TopicNameStrategy derives subjects from the dedicated
  topic names);
- the runner NEVER deletes topics, consumer groups, schema subjects or data;
- the runner REFUSES to start when the standard compute services are running;
- only the v1v2-* services are started and stopped;
- artifacts are always preserved: they are the campaign proof.

Exit codes: the parity verdict is propagated verbatim —
0 parity_ok, 1 parity_ko, 2 incomplete_inputs — and every runner-level
failure (Docker, export, comparator input error) exits with 3 so a ko
verdict can never be confused with a broken campaign.

This script is never executed by pytest.

Usage:
    PYTHONPATH=src:. .venv/bin/python \\
        scripts/integration/run-explainability-v1-v2-parity.py \\
        [--source-csv PATH] [--manifest-template PATH]
        [--skip-timestamp-collision-check] [--max-events N] [--artifacts-dir DIR]
        [--keep-up] [--global-timeout S]
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v1_v2"
MANIFEST_TEMPLATE = FIXTURE_DIR / "v1v2_parity_manifest.json"
SOURCE_CSV = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2" / "oc12_source.csv"
PARITY_MODULE = "experiments.explainability.v1_v2.parity_comparator"

BASE_COMPOSE = ["-f", "docker-compose.yml"]
V1V2_COMPOSE = BASE_COMPOSE + ["-f", "docker/compose/experiments/docker-compose.v1v2.yml"]

# Standard compute services: if any of them is running, the runner refuses to
# start (it never stops or restarts shared resources).
STANDARD_COMPUTE_SERVICES = [
    "producer",
    "hstree-analysis",
    "explainability",
    "explainability-v2",
]

# Infrastructure services: started if missing, never stopped by the runner.
INFRA_SERVICES = ["kafka1", "kafka2", "kafka3", "schema-registry"]

V1V2_SERVICES = ["v1v2-producer", "v1v2-hstree", "v1v2-regular", "v1v2-v2"]

V1V2_TOPICS = [
    "v1v2-source-{run_id}",
    "v1v2-source-viz-{run_id}",
    "v1v2-hstree-{run_id}",
    "v1v2-hstree-viz-{run_id}",
    "v1v2-regular-{run_id}",
    "v1v2-regular-viz-{run_id}",
    "v1v2-v2-{run_id}",
    "v1v2-v2-viz-{run_id}",
]

# V1-05c verdict exit codes, propagated verbatim by this runner.
VERDICT_EXIT_CODES = (0, 1, 2)
CAMPAIGN_FAILURE_EXIT = 3


class RunnerError(RuntimeError):
    """Fatal campaign error; artifacts are preserved for diagnosis."""


def log(message: str) -> None:
    print(f"[v1v2 {dt.datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def generate_run_id(now: Optional[dt.datetime] = None) -> str:
    """Build a unique V1-06 run id: v1v2-YYYYMMDD-HHMMSS-<8 hex chars>.

    The timestamp keeps artifacts sortable; the random suffix makes
    concurrent or same-second runs unique. Only lowercase letters, digits
    and dashes: valid for Kafka topics/groups and Docker container names.
    """
    stamp = (now or dt.datetime.now()).strftime("%Y%m%d-%H%M%S")
    return f"v1v2-{stamp}-{uuid.uuid4().hex[:8]}"


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
_SOURCE_CSV_CONTAINER_PATH = "tests/fixtures/explainability_v2/oc12_source.csv"
_TOPIC_PARTITIONS = 1
_PRODUCER_DELAY = "0.0001"
_Q3_TRACE_HOST_DIR = ""
_Q3_TRACE_ENABLED = False


def campaign_env() -> Dict[str, str]:
    run_id = _RUN_ID or "unset"
    return {
        "V1V2_RUN_ID": run_id,
        "V1V2_SOURCE_CSV": _SOURCE_CSV_CONTAINER_PATH,
        "V1V2_TOPIC_PARTITIONS": str(_TOPIC_PARTITIONS),
        "V1V2_TIME_DELAY": _PRODUCER_DELAY,
        "V1V2_HSTREE_NUM_TREES": os.environ.get("V1V2_HSTREE_NUM_TREES", "25"),
        "V1V2_HSTREE_VARIANT": os.environ.get("V1V2_HSTREE_VARIANT", "paper"),
        "V1V2_Q3_TRACE_HOST_DIR": _Q3_TRACE_HOST_DIR,
        "V1V2_Q3_TRACE_CONTAINER_DIR": "/q3-traces" if _Q3_TRACE_ENABLED else "",
    }


def v1v2_topic(template: str) -> str:
    return template.format(run_id=_RUN_ID)


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
    the TOPIC column. The header must carry BOTH the TOPIC and LAG columns;
    None when the output is ambiguous."""
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
        if "LAG" in upper_tokens and "TOPIC" in upper_tokens:
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
        "ps", "--all", "--format", "json", "v1v2-producer",
        timeout=30.0, check=False, files=V1V2_COMPOSE,
    )
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("Service") == "v1v2-producer":
            return payload.get("State") == "exited" and int(payload.get("ExitCode") or 0) == 0
    return False


def create_v1v2_topics() -> None:
    for template in V1V2_TOPICS:
        topic = v1v2_topic(template)
        compose(
            "exec", "-T", "kafka1",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "kafka1:9092",
            "--create", "--if-not-exists",
            "--topic", topic,
            "--partitions", str(_TOPIC_PARTITIONS),
            "--replication-factor", "3",
            timeout=30.0,
        )
        log(f"topic ready: {topic}")
    # Kafka may acknowledge topic creation before every broker has refreshed
    # metadata.  Let the producer see all dedicated topics before it starts,
    # especially when a fast campaign immediately publishes at high rate.
    time.sleep(5.0)


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


def v1v2_containers_running() -> bool:
    return bool(services_running(V1V2_SERVICES, files=V1V2_COMPOSE))


def stop_v1v2_services(artifacts: Optional[Path] = None) -> None:
    """Stop and remove ONLY the v1v2-* campaign services.

    Shared resources are never touched: topics, consumer groups and Avro
    subjects stay in place for manual inspection.
    """
    compose("stop", *V1V2_SERVICES, timeout=120.0, check=False, files=V1V2_COMPOSE)
    if artifacts is not None:
        # Workers emit their final queue/status counters during shutdown. Read
        # the stopped containers before `rm` removes their logs.
        try:
            capture_worker_metrics(artifacts)
        except Exception as exc:
            print(f"[v1v2] WARNING: worker metrics capture failed: {exc}", file=sys.stderr)
    compose("rm", "-f", *V1V2_SERVICES, timeout=60.0, check=False, files=V1V2_COMPOSE)
    try:
        wait_until(
            lambda: not v1v2_containers_running(),
            timeout=60.0,
            description="all v1v2 containers stopped and removed",
            interval=1.0,
        )
    except RunnerError as exc:
        # A cleanup problem must never mask the campaign result.
        print(f"[v1v2] WARNING: cleanup incomplete: {exc}", file=sys.stderr)


def assert_standard_services_stopped() -> None:
    running = services_running(STANDARD_COMPUTE_SERVICES)
    if running:
        raise RunnerError(
            "standard compute services are still running: "
            + ", ".join(running)
            + ". Stop them first; the V1-06 runner never stops shared services."
        )


# ---------------------------------------------------------------------------
# Scenario pre-flight: the collision fixture must match the manifest
# ---------------------------------------------------------------------------


def load_source_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def expected_keys_for_source(csv_path: Path, limit: Optional[int] = None) -> List[str]:
    """Derive identities using the producer's existing/generative rule."""
    rows = load_source_rows(csv_path)
    has_source_ids = "source_event_id" in (rows[0] if rows else {})
    keys: List[str] = []
    for index, row in enumerate(rows[:limit], start=1):
        source_event_id = row.get("source_event_id") if has_source_ids else None
        if not isinstance(source_event_id, str) or not source_event_id:
            source_event_id = f"event-{index:012d}"
        keys.append(f"{row.get('Group')}|{source_event_id}")
    return keys


def manifest_for_source(source_csv: Path, template: Dict, limit: Optional[int] = None) -> Dict:
    """Create a dataset-specific V1-05c manifest without editing the dataset."""
    manifest = json.loads(json.dumps(template))
    keys = expected_keys_for_source(source_csv, limit)
    if len(set(keys)) != len(keys):
        raise RunnerError(
            f"dataset {source_csv.name} generates duplicate (Group, source_event_id) identities"
        )
    manifest["expected_keys"] = keys
    manifest["source_csv"] = str(source_csv.relative_to(PROJECT_ROOT))
    notes = manifest.setdefault("design_notes", [])
    notes.append("expected_keys derived from existing source_event_id values or producer emission-order IDs.")
    return manifest


def assert_fixture_scenario(
    csv_path: Path, manifest: Dict, *, require_timestamp_collision: bool = True,
    limit: Optional[int] = None,
) -> None:
    """Fail fast when the campaign cannot exercise the V1-06 scenario.

    Required: every manifest expected key is a real (Group, source_event_id)
    of the fixture, identities are unique, and at least one collision pair
    exists (same group, same timestamp, distinct source_event_id). The
    timestamp is never a comparison key: the collision is the proof that the
    exclusive identity is required.
    """
    rows = load_source_rows(csv_path)
    keys = expected_keys_for_source(csv_path, limit)
    expected_keys = manifest["expected_keys"]
    if sorted(keys) != sorted(expected_keys):
        raise RunnerError(
            f"fixture {csv_path.name} holds {len(keys)} identities, manifest expects "
            f"{len(expected_keys)} and both sets differ"
        )
    if len(set(keys)) != len(keys):
        raise RunnerError(f"fixture {csv_path.name} has duplicate (Group, source_event_id) identities")
    collisions = {}
    for row in rows:
        collisions.setdefault((row.get("Group"), row.get("timestamp")), []).append(
            row.get("source_event_id")
        )
    collision_pairs = [ids for ids in collisions.values() if len(ids) > 1]
    if require_timestamp_collision and not collision_pairs:
        raise RunnerError(
            f"fixture {csv_path.name} has no (Group, timestamp) collision with distinct "
            "source_event_id: the V1-06 scenario is not exercised"
        )
    log(
        "fixture scenario verified: "
        f"{len(keys)} identities, {len(collision_pairs)} timestamp collision pair(s)"
        + (f" (first: {collision_pairs[0]})" if collision_pairs else "")
    )


# ---------------------------------------------------------------------------
# Exports, manifest and parity comparison (V1-05c)
# ---------------------------------------------------------------------------


def export_jsonl_topic(topic: str, output_path: Path, expected: int, timeout: float) -> int:
    result = compose(
        "exec", "-T", "kafka1",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "kafka1:9092",
        "--from-beginning",
        "--max-messages", str(expected),
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
            if valid_lines >= expected:
                break
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


def _load_jsonl_records(path: Path, export_name: str) -> List[Dict[str, object]]:
    """Load JSONL records and reject malformed or non-object entries."""
    records: List[Dict[str, object]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RunnerError(f"{export_name}:{line_number}: invalid JSON: {exc.msg}") from exc
        if not isinstance(record, dict):
            raise RunnerError(f"{export_name}:{line_number}: record must be a JSON object")
        records.append(record)
    return records


def _identity_set(records: List[Dict[str, object]], export_name: str) -> set[tuple[str, str]]:
    """Return exact raw ``(Group, source_event_id)`` identities."""
    identities: set[tuple[str, str]] = set()
    for line_number, record in enumerate(records, start=1):
        group = record.get("Group")
        source_event_id = record.get("source_event_id")
        if not isinstance(group, str) or not group.strip():
            raise RunnerError(f"{export_name}:{line_number}: Group must be a non-empty string")
        if not isinstance(source_event_id, str) or not source_event_id.strip():
            raise RunnerError(
                f"{export_name}:{line_number}: source_event_id must be a non-empty string"
            )
        identity = (group, source_event_id)
        if identity in identities:
            raise RunnerError(f"{export_name}:{line_number}: duplicate identity {identity!r}")
        identities.add(identity)
    return identities


def validate_three_export_coverage(
    hstree_path: Path,
    v1_path: Path,
    v2_path: Path,
    expected_keys: List[str],
    output_path: Optional[Path] = None,
) -> Dict[str, object]:
    """Validate the common identity coverage of upstream, V1 and V2 exports.

    The upstream export is checked with the Q1.1 contract.  V1/V2 records are
    checked for exact root identities and duplicates.  No score comparison is
    performed here; that remains the responsibility of Q1.2/Q1.5.
    """
    from experiments.explainability.v1_v2.hstree_amont_contract import (
        HSTreeExportError,
        load_hstree_jsonl,
    )

    try:
        upstream_validation = load_hstree_jsonl(hstree_path, export_name="upstream")
    except HSTreeExportError as exc:
        raise RunnerError(f"upstream HSTree export is invalid: {exc}") from exc
    upstream = set(upstream_validation.identities)
    v1 = _identity_set(_load_jsonl_records(v1_path, "v1"), "v1")
    v2 = _identity_set(_load_jsonl_records(v2_path, "v2"), "v2")
    expected = set()
    for key in expected_keys:
        if not isinstance(key, str) or "|" not in key:
            raise RunnerError(f"invalid expected identity key: {key!r}")
        group, source_event_id = key.split("|", 1)
        expected.add((group, source_event_id))

    sets = {"expected": expected, "upstream": upstream, "v1": v1, "v2": v2}
    coverage: Dict[str, object] = {
        "comparison_key": "(Group, source_event_id)",
        "expected_count": len(expected),
        "exports": {name: len(values) for name, values in sets.items() if name != "expected"},
        "all_equal": len({frozenset(values) for values in sets.values()}) == 1,
        "missing_by_export": {},
        "unexpected_by_export": {},
    }
    for name, values in sets.items():
        if name == "expected":
            continue
        coverage["missing_by_export"][name] = [list(key) for key in sorted(expected - values)]
        coverage["unexpected_by_export"][name] = [list(key) for key in sorted(values - expected)]
    if not coverage["all_equal"]:
        raise RunnerError(
            "three-export identity coverage mismatch: "
            + json.dumps(coverage, sort_keys=True)
        )
    if output_path is not None:
        output_path.write_text(
            json.dumps(coverage, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return coverage


def select_common_identity_sample(
    v1_path: Path, v2_path: Path, limit: Optional[int]
) -> List[str]:
    """Keep a deterministic common identity set and filter both exports to it."""
    def load(path: Path) -> Dict[str, List[str]]:
        records: Dict[str, List[str]] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            group, event_id = record.get("Group"), record.get("source_event_id")
            if isinstance(group, str) and group and isinstance(event_id, str) and event_id:
                records.setdefault(f"{group}|{event_id}", []).append(line)
        return records

    v1, v2 = load(v1_path), load(v2_path)
    common = sorted(set(v1) & set(v2))
    if limit is not None:
        common = common[:limit]
    for path, records in ((v1_path, v1), (v2_path, v2)):
        lines = [line for key in common for line in records.get(key, [])]
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return common


def dump_service_logs(artifacts: Path) -> None:
    try:
        result = compose(
            "logs", "--tail", "60", *V1V2_SERVICES,
            timeout=60.0, check=False, files=V1V2_COMPOSE,
        )
        (artifacts / "services-failure-logs.txt").write_text(result.stdout, encoding="utf-8")
        print(f"[v1v2] failure logs saved to {artifacts / 'services-failure-logs.txt'}")
    except Exception as exc:
        print(f"[v1v2] could not dump service logs: {exc}")


def capture_worker_metrics(artifacts: Path) -> None:
    """Capture final worker counters before the campaign containers stop."""
    import re

    pattern = re.compile(
        r"Group '(?P<group>[^']+)': enqueued=(?P<enqueued>\d+) "
        r"started=(?P<started>\d+) completed=(?P<completed>\d+) "
        r"queue_depth=(?P<queue_depth>-?\d+) in_flight=(?P<in_flight>\d+) "
        r"statuses=(?P<statuses>.*)"
    )
    payload: Dict[str, object] = {"services": {}}
    for service in ("v1v2-regular", "v1v2-v2"):
        container = f"{service}-{_RUN_ID}"
        result = run_command(
            ["docker", "logs", "--tail", "5000", container],
            timeout=60.0,
            check=False,
        )
        rows = []
        for line in result.stdout.splitlines():
            match = pattern.search(line)
            if not match:
                continue
            row = {key: int(match.group(key)) for key in (
                "enqueued", "started", "completed", "queue_depth", "in_flight"
            )}
            row["group"] = match.group("group")
            row["statuses"] = match.group("statuses")
            rows.append(row)
        payload["services"][service] = {"final_group_counters": rows}
    (artifacts / "worker-metrics.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


class LagSampler:
    """Background sampler for consumer lag and HSTree high-water marks."""

    def __init__(self, artifacts: Path, regular_group: str, v2_group: str, topic: str, interval: float) -> None:
        self.artifacts = artifacts
        self.regular_group = regular_group
        self.v2_group = v2_group
        self.topic = topic
        self.interval = interval
        self.started = time.monotonic()
        self.stop_event = threading.Event()
        self.samples: List[Dict[str, object]] = []
        self.thread = threading.Thread(target=self._run, name="v1v2-lag-sampler", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=max(2.0, self.interval + 2.0))
        (self.artifacts / "lag-samples.json").write_text(
            json.dumps({"interval_seconds": self.interval, "samples": self.samples}, indent=2) + "\n",
            encoding="utf-8",
        )

    def _run(self) -> None:
        while not self.stop_event.is_set():
            sample: Dict[str, object] = {
                "elapsed_seconds": round(time.monotonic() - self.started, 3)
            }
            try:
                sample.update({
                    "regular_lag": consumer_group_lag(self.regular_group, self.topic),
                    "v2_lag": consumer_group_lag(self.v2_group, self.topic),
                    "hstree_records": topic_message_count(self.topic),
                })
            except Exception as exc:
                sample["error"] = str(exc)
            self.samples.append(sample)
            self.stop_event.wait(self.interval)


def build_parity_manifest(
    artifacts: Path, regular_export: Path, v2_export: Path,
    manifest_template: Optional[Dict] = None,
) -> Path:
    """Complete the V1-05c manifest template with the per-run export paths.

    The template carries the campaign, the exclusive comparison key, the
    expected keys and the tolerances; only ``exports.v1``/``exports.v2`` are
    rewritten to the run artifacts. The result is validated with the V1-05c
    loader so a manifest error is explicit before the CLI runs.
    """
    from experiments.explainability.v1_v2.parity_comparator import load_manifest

    manifest = (
        json.loads(json.dumps(manifest_template))
        if manifest_template is not None
        else json.loads(MANIFEST_TEMPLATE.read_text(encoding="utf-8"))
    )
    manifest["exports"] = {"v1": str(regular_export), "v2": str(v2_export)}
    target = artifacts / "parity-manifest.json"
    target.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    load_manifest(json.loads(target.read_text(encoding="utf-8")))
    log(f"parity manifest written and validated: {target}")
    return target


def run_parity_comparator(artifacts: Path, timeout: float) -> int:
    """Run the V1-05c parity CLI; return its verdict exit code.

    0 = parity_ok, 1 = parity_ko, 2 = incomplete_inputs: propagated verbatim.
    Any other code (3 manifest, 4 file, 5 JSONL, 6 usage) or a crash is a
    campaign failure; the CLI summary is printed as the human-readable proof.
    """
    result = run_command(
        [
            sys.executable, "-m", PARITY_MODULE,
            "--manifest", str(artifacts / "parity-manifest.json"),
            "--output", str(artifacts / "parity-report.json"),
        ],
        timeout=timeout,
        check=False,
        env_extra={"PYTHONPATH": "src:."},
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.returncode not in VERDICT_EXIT_CODES:
        raise RunnerError(
            f"parity comparator failed with exit code {result.returncode} "
            f"(verdicts are 0/1/2; 3=manifest, 4=file, 5=JSONL, 6=usage)\n"
            f"stderr: {result.stderr[-2000:]}"
        )
    if result.stderr.strip():
        print(result.stderr, file=sys.stderr, end="")
    return result.returncode


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    global _RUN_ID, _SOURCE_CSV_CONTAINER_PATH, _TOPIC_PARTITIONS, _PRODUCER_DELAY
    global _Q3_TRACE_HOST_DIR, _Q3_TRACE_ENABLED, V1V2_COMPOSE
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--artifacts-dir", default=None, help="artifacts directory (default: /tmp/v1v2-parity-<run-id>)")
    parser.add_argument("--source-csv", default=str(SOURCE_CSV), help="dataset path inside the repository")
    parser.add_argument("--manifest-template", default=str(MANIFEST_TEMPLATE), help="V1-05c manifest template")
    parser.add_argument("--skip-timestamp-collision-check", action="store_true", help="allow datasets without a deliberate timestamp collision")
    parser.add_argument("--max-events", type=int, default=None, help="compare only the first N dataset identities (the producer still streams the source)")
    parser.add_argument("--keep-up", action="store_true", help="keep the v1v2 services running after the run (success AND failure) for manual inspection")
    parser.add_argument("--global-timeout", type=float, default=900.0, help="global campaign deadline in seconds")
    parser.add_argument("--producer-delay", default="0.0001", help="producer delay passed to the dedicated Compose campaign")
    parser.add_argument("--partitions", type=int, default=1, help="number of partitions for dedicated campaign topics")
    parser.add_argument("--lag-sample-interval", type=float, default=5.0, help="seconds between lag samples; zero disables sampling")
    parser.add_argument("--q3-trace", action="store_true", help="enable buffered Q3 side-channel traces (adds trace files under artifacts-dir/traces)")
    args = parser.parse_args(argv)

    if args.partitions <= 0:
        raise SystemExit("--partitions must be positive")
    if args.lag_sample_interval < 0:
        raise SystemExit("--lag-sample-interval must be non-negative")
    _TOPIC_PARTITIONS = args.partitions
    _PRODUCER_DELAY = str(args.producer_delay)

    _RUN_ID = generate_run_id()
    source_csv = Path(args.source_csv)
    if not source_csv.is_absolute():
        source_csv = PROJECT_ROOT / source_csv
    source_csv = source_csv.resolve()
    manifest_template_path = Path(args.manifest_template)
    if not manifest_template_path.is_absolute():
        manifest_template_path = PROJECT_ROOT / manifest_template_path
    manifest_template_path = manifest_template_path.resolve()
    try:
        source_csv.relative_to(PROJECT_ROOT)
    except ValueError as exc:
        raise SystemExit(f"--source-csv must be inside the repository: {source_csv}") from exc
    if not source_csv.is_file():
        raise SystemExit(f"--source-csv does not exist: {source_csv}")
    if not manifest_template_path.is_file():
        raise SystemExit(f"--manifest-template does not exist: {manifest_template_path}")
    if args.max_events is not None and args.max_events <= 0:
        raise SystemExit("--max-events must be positive")
    manifest_template = manifest_for_source(
        source_csv, json.loads(manifest_template_path.read_text(encoding="utf-8")), args.max_events
    )
    _SOURCE_CSV_CONTAINER_PATH = str(source_csv.relative_to(PROJECT_ROOT))
    expected = len(manifest_template["expected_keys"])
    hstree_viz_topic = v1v2_topic("v1v2-hstree-viz-{run_id}")
    regular_viz_topic = v1v2_topic("v1v2-regular-viz-{run_id}")
    v2_viz_topic = v1v2_topic("v1v2-v2-viz-{run_id}")
    hstree_topic = v1v2_topic("v1v2-hstree-{run_id}")
    regular_group = f"v1v2-regular-consumer-{_RUN_ID}"
    v2_group = f"v1v2-v2-consumer-{_RUN_ID}"

    artifacts = Path(args.artifacts_dir) if args.artifacts_dir else Path("/tmp") / f"v1v2-parity-{_RUN_ID}"
    artifacts.mkdir(parents=True, exist_ok=True)
    _Q3_TRACE_ENABLED = args.q3_trace
    if _Q3_TRACE_ENABLED:
        trace_dir = artifacts / "traces"
        trace_dir.mkdir(parents=True, exist_ok=True)
        _Q3_TRACE_HOST_DIR = str(trace_dir.resolve())
        V1V2_COMPOSE = BASE_COMPOSE + ["-f", "docker/compose/experiments/docker-compose.v1v2.yml", "-f", "docker/compose/experiments/docker-compose.q3-trace.yml"]
    else:
        _Q3_TRACE_HOST_DIR = ""
        V1V2_COMPOSE = BASE_COMPOSE + ["-f", "docker/compose/experiments/docker-compose.v1v2.yml"]
    (artifacts / "campaign-env.json").write_text(
        json.dumps(
            {
                **campaign_env(),
                "manifest_template": str(manifest_template_path),
                "source_csv": str(source_csv),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    log(f"run id: {_RUN_ID} | artifacts: {artifacts}")

    campaign_started = time.monotonic()
    campaign_started_utc = dt.datetime.now(dt.timezone.utc)
    deadline = campaign_started + args.global_timeout

    # A full source-5000 run performs one deterministic SHAP calculation per
    # event.  The per-stage cap must therefore exceed the small OC12 witness
    # default; otherwise the runner aborts while the services are still
    # making steady progress.
    def remaining(cap: float = 2400.0) -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise RunnerError(f"global timeout of {args.global_timeout}s exceeded")
        return min(left, cap)

    error_message: Optional[str] = None
    v1v2_started = False
    verdict_code: Optional[int] = None
    lag_sampler: Optional[LagSampler] = None
    try:
        log("checking that the standard compute services are stopped")
        assert_standard_services_stopped()

        log("checking the collision fixture against the manifest template")
        assert_fixture_scenario(
            source_csv,
            manifest_template,
            require_timestamp_collision=not args.skip_timestamp_collision_check,
            limit=args.max_events,
        )

        log("ensuring infrastructure is up (never stopped by this runner)")
        compose("up", "-d", *INFRA_SERVICES, timeout=remaining())
        create_v1v2_topics()

        log("starting the v1v2 parity services")
        # Tracked so the finally below can clean up even when this very
        # command fails partway through.
        v1v2_started = True
        compose("up", "-d", *V1V2_SERVICES, timeout=remaining(), files=V1V2_COMPOSE)
        if args.lag_sample_interval > 0:
            lag_sampler = LagSampler(
                artifacts, regular_group, v2_group, hstree_topic, args.lag_sample_interval
            )
            lag_sampler.start()

        log("waiting for the v1v2 producer to finish")
        wait_until(producer_finished, remaining(), "producer completion")
        for label, topic in (
            ("hstree", hstree_viz_topic),
            ("regular", regular_viz_topic),
            ("v2", v2_viz_topic),
        ):
            log(f"waiting for {expected} events in {topic}")
            wait_until(
                lambda topic=topic: topic_message_count(topic) >= expected,
                remaining(),
                f"{expected} events in {topic}",
            )

        if args.max_events is None:
            log(f"waiting for zero lags ({regular_group}, {v2_group})")
            def lags_drained() -> bool:
                regular_lag = consumer_group_lag(regular_group, hstree_topic)
                v2_lag = consumer_group_lag(v2_group, hstree_topic)
                return regular_lag == 0 and v2_lag == 0

            wait_until(lags_drained, remaining(), "zero regular and v2 consumer lags")
        else:
            log(f"sample limit reached; skipping zero-lag wait ({args.max_events} events)")

        log("exporting the HSTree, regular (V1) and v2 JSON topics")
        hstree_count = export_jsonl_topic(
            hstree_viz_topic, artifacts / "v1v2-hstree-viz.jsonl", expected, remaining(120.0)
        )
        regular_count = export_jsonl_topic(
            regular_viz_topic, artifacts / "v1v2-regular-viz.jsonl", expected, remaining(120.0)
        )
        v2_count = export_jsonl_topic(
            v2_viz_topic, artifacts / "v1v2-v2-viz.jsonl", expected, remaining(120.0)
        )
        log(f"exported hstree={hstree_count} regular={regular_count} v2={v2_count}")

        log("validating common identity coverage across upstream, V1 and V2")
        coverage = validate_three_export_coverage(
            artifacts / "v1v2-hstree-viz.jsonl",
            artifacts / "v1v2-regular-viz.jsonl",
            artifacts / "v1v2-v2-viz.jsonl",
            manifest_template["expected_keys"],
            artifacts / "three-export-coverage.json",
        )
        log(f"three-export coverage verified: {coverage['expected_count']} identities")

        common_keys = select_common_identity_sample(
            artifacts / "v1v2-regular-viz.jsonl",
            artifacts / "v1v2-v2-viz.jsonl",
            args.max_events,
        )
        manifest_template["expected_keys"] = common_keys
        log(f"selected deterministic common identity set: {len(common_keys)}")

        final_regular_lag = consumer_group_lag(regular_group, hstree_topic)
        final_v2_lag = consumer_group_lag(v2_group, hstree_topic)
        if final_regular_lag is None or final_v2_lag is None:
            raise RunnerError("final consumer lag could not be read (ambiguous Kafka output)")
        (artifacts / "lag-metrics.json").write_text(
            json.dumps(
                {
                    "regular_group": regular_group,
                    "v2_group": v2_group,
                    "final_lags": {"regular": final_regular_lag, "v2": final_v2_lag},
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (artifacts / "runner-metrics.json").write_text(
            json.dumps(
                {
                    "run_id": _RUN_ID,
                    "global_timeout": args.global_timeout,
                    "elapsed_seconds": round(time.monotonic() - campaign_started, 3),
                    "final_lags": {"regular": final_regular_lag, "v2": final_v2_lag},
                    "comparison_key": manifest_template["comparison_key"],
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        log("building the V1-05c parity manifest from the run exports")
        build_parity_manifest(
            artifacts,
            artifacts / "v1v2-regular-viz.jsonl",
            artifacts / "v1v2-v2-viz.jsonl",
            manifest_template,
        )

        log("running the V1-05c parity comparator on the raw exports")
        verdict_code = run_parity_comparator(artifacts, remaining(120.0))
        log(f"parity verdict code: {verdict_code}")
        log(f"artifacts: {artifacts}")
    except RunnerError as exc:
        error_message = str(exc)
    except Exception as exc:  # the runner must never hang or fail silently
        error_message = f"unexpected failure: {exc}"
    finally:
        if lag_sampler is not None:
            lag_sampler.stop()
        if error_message is not None:
            print(f"[v1v2] FAILED: {error_message}", file=sys.stderr)
            # Diagnostics are written BEFORE the cleanup so a stopped service
            # can no longer report its state.
            try:
                dump_service_logs(artifacts)
            except Exception as log_exc:
                print(f"[v1v2] could not dump service logs: {log_exc}", file=sys.stderr)
        if v1v2_started and not args.keep_up:
            # Default behavior: the v1v2-* containers are cleaned up on
            # success AND on failure. Only --keep-up keeps them running.
            # A cleanup failure never masks the campaign result above.
            try:
                log("stopping the v1v2 parity services (only v1v2-* services)")
                stop_v1v2_services(artifacts)
            except Exception as cleanup_exc:
                print(f"[v1v2] WARNING: cleanup failed: {cleanup_exc}", file=sys.stderr)
        if args.q3_trace:
            compose_paths = [PROJECT_ROOT / "docker-compose.yml", PROJECT_ROOT / "docker/compose/experiments/docker-compose.v1v2.yml"]
            if _Q3_TRACE_ENABLED:
                compose_paths.append(PROJECT_ROOT / "docker/compose/experiments/docker-compose.q3-trace.yml")
            try:
                revision = run_command(["git", "rev-parse", "HEAD"], timeout=10.0).stdout.strip()
                working_tree_status = run_command(
                    ["git", "status", "--short", "--untracked-files=all"], timeout=10.0
                ).stdout.splitlines()
                traces = sorted((artifacts / "traces").glob("*.jsonl"))
                trace_count = sum(sum(1 for _ in path.open(encoding="utf-8")) for path in traces)
                runtime_source_paths = [
                    PROJECT_ROOT / relative for relative in (
                        "src/dasma_kafka/explainability/q3_trace.py",
                        "src/dasma_kafka/explainability/runtime.py",
                        "src/dasma_kafka/explainability/model.py",
                        "src/dasma_kafka/explainability_v2/runtime.py",
                        "src/dasma_kafka/explainability_v2/model.py",
                        "runtime/detectors/hstree/hstree_analysis.py",
                        "runtime/detectors/hstree/models/hstree.py",
                        "runtime/explainability/regular/explainability-multi-thread-by-group.py",
                        "runtime/explainability/v2/explainability_v2.py",
                        "scripts/integration/run-explainability-v1-v2-parity.py",
                    )
                ]
                manifest = {
                    "schema_version": "q3-run-manifest-v1",
                    "run_id": _RUN_ID,
                    "git_revision": revision,
                    "working_tree_dirty": bool(working_tree_status),
                    "working_tree_status": working_tree_status,
                    "runtime_source_sha256": {str(path.relative_to(PROJECT_ROOT)): sha256_file(path) for path in runtime_source_paths},
                    "compose_sha256": {path.name: sha256_file(path) for path in compose_paths},
                    "source_csv": str(source_csv),
                    "source_csv_sha256": sha256_file(source_csv),
                    "partitions": args.partitions,
                    "producer_delay_seconds": float(args.producer_delay),
                    "max_events": args.max_events,
                    "hstree": {"initial_window_size": 100, "window_size": 2048, "num_trees": 50, "max_depth": 15, "seed": 42},
                    "explainability": {"initial_window_size": 100, "background_size": 100, "nsamples": 100, "seed": 42},
                    "image_tags": {
                        "producer": "dasma-kafka-producer:0.1.0",
                        "hstree": "dasma-kafka-anomaly-light:0.1.0",
                        "explainability": "dasma-kafka-explainability:0.1.0",
                    },
                    "image_ids": {
                        image: run_command(
                            ["docker", "image", "inspect", "--format={{.Id}}", image],
                            timeout=15.0, check=False,
                        ).stdout.strip()
                        for image in (
                            "dasma-kafka-producer:0.1.0",
                            "dasma-kafka-anomaly-light:0.1.0",
                            "dasma-kafka-explainability:0.1.0",
                        )
                    },
                    "trace_files": {path.name: {"sha256": sha256_file(path), "records": sum(1 for _ in path.open(encoding="utf-8"))} for path in traces},
                    "trace_record_count": trace_count,
                    "trace_enabled": True,
                    "started_utc": campaign_started_utc.isoformat(),
                    "finished_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "campaign_exit": verdict_code if error_message is None else CAMPAIGN_FAILURE_EXIT,
                    "campaign_error": error_message,
                }
                (artifacts / "q3-run-manifest.json").write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
                )
            except Exception as manifest_exc:
                print(f"[v1v2] WARNING: could not write Q3 run manifest: {manifest_exc}", file=sys.stderr)
        if error_message is not None:
            print(f"[v1v2] artifacts kept for diagnosis: {artifacts}", file=sys.stderr)
            return CAMPAIGN_FAILURE_EXIT

    assert verdict_code is not None
    log(
        f"V1-06 parity campaign finished with verdict code {verdict_code} "
        f"(0=parity_ok, 1=parity_ko, 2=incomplete_inputs) — artifacts: {artifacts}"
    )
    return verdict_code


if __name__ == "__main__":
    sys.exit(main())
