#!/usr/bin/env python3
"""OC-14 — non-destructive Docker runner for the explainability-v2 capacity.

For EACH SHAP budget (default 100, 1000, 1500, 2000) the runner starts a
fresh isolated campaign on the OC-12R.1/OC-13 model:

CSV fixture -> oc14 producer -> oc14 hstree -> oc14 v2 -> dedicated JSON viz
topic, with a 1 Hz lag sampler on the v2 consumer group.

Non-destructive by construction:
- every created resource carries the per-budget OC14_RUN_ID and the budget
  suffix: topics, consumer groups and Avro subjects (TopicNameStrategy);
- the runner NEVER deletes topics, consumer groups, schema subjects or data
  (no deletion primitive of any kind: Kafka topics/groups, Avro subjects,
  InfluxDB data; created resources stay for audit);
- the runner REFUSES to start when the standard compute services are running;
- only the oc14-* services are started and stopped;
- artifacts are always preserved: they are the measurement proof.

The lag sampler uses the Kafka 3.9-safe parser validated in OC-12R.1 (header
must carry BOTH the TOPIC and LAG columns; rows filtered on the TOPIC
column). The dashboard is never used as a measurement source.

This script is never executed by pytest.

Usage:
    PYTHONPATH=src:. .venv/bin/python \\
        scripts/integration/run-explainability-v2-capacity.py \\
        [--budgets 100,1000,1500,2000] [--skip-offline] [--keep-up]
        [--global-timeout S] [--artifacts-dir DIR]
        [--target-events-per-second-per-group R --max-drain-seconds S]
        [--assumed-input-rates R1,R2,...]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2"
MANIFEST = FIXTURE_DIR / "oc14_capacity_manifest.json"
CAPACITY_BENCHMARK = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "capacity_benchmark.py"
CAPACITY_REPORT = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "capacity_report.py"
REPORTS_DIR = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "reports"

BASE_COMPOSE = ["-f", "docker-compose.yml"]
OC14_COMPOSE = BASE_COMPOSE + ["-f", "docker/compose/experiments/docker-compose.oc14.yml"]

# Standard compute services: if any of them is running, the runner refuses to
# start (it never stops or restarts shared resources).
STANDARD_COMPUTE_SERVICES = [
    "producer",
    "hstree-analysis",
    "explainability-v2",
    "explainability",
    "telegraf",
]

# Infrastructure services: started if missing, never stopped by the runner.
INFRA_SERVICES = ["kafka1", "kafka2", "kafka3", "schema-registry"]

OC14_SERVICES = ["oc14-producer", "oc14-hstree", "oc14-v2"]

OC14_TOPICS = [
    "oc14-ns{nsamples}-source-{run_id}",
    "oc14-ns{nsamples}-hstree-{run_id}",
    "oc14-ns{nsamples}-hstree-viz-{run_id}",
    "oc14-ns{nsamples}-v2-{run_id}",
    "oc14-ns{nsamples}-v2-viz-{run_id}",
]

DEFAULT_BUDGETS = [100, 1000, 1500, 2000]


class RunnerError(RuntimeError):
    """Fatal campaign error; artifacts are preserved for diagnosis."""


def log(message: str) -> None:
    print(f"[oc14 {dt.datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def generate_run_id(now: Optional[dt.datetime] = None) -> str:
    """Build a unique OC-14 run id: oc14-YYYYMMDD-HHMMSS-<8 hex chars>.

    The timestamp keeps artifacts sortable; the random suffix makes
    concurrent or same-second runs unique. Only lowercase letters, digits
    and dashes: valid for Kafka topics/groups and Docker container names.
    """
    stamp = (now or dt.datetime.now()).strftime("%Y%m%d-%H%M%S")
    return f"oc14-{stamp}-{uuid.uuid4().hex[:8]}"


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
    env_extra: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    return run_command(
        ["docker", "compose", *(files or BASE_COMPOSE), *args],
        timeout=timeout,
        env_extra=env_extra,
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
# Kafka state helpers (CLI inside the kafka1 container)
# ---------------------------------------------------------------------------


def topic_message_count(topic: str, env_extra: Dict[str, str]) -> int:
    result = compose(
        "exec", "-T", "kafka1",
        "/opt/kafka/bin/kafka-get-offsets.sh",
        "--bootstrap-server", "kafka1:9092",
        "--topic", topic,
        timeout=30.0,
        env_extra=env_extra,
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


def consumer_group_lag(group: str, topic: str, env_extra: Dict[str, str]) -> Optional[int]:
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
        env_extra=env_extra,
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


def services_running(services: List[str], files: Optional[List[str]] = None, env_extra: Optional[Dict[str, str]] = None) -> List[str]:
    result = compose("ps", "--all", "--format", "json", *services, timeout=30.0, check=False, files=files, env_extra=env_extra)
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


def producer_status(env_extra: Dict[str, str]) -> str:
    running = services_running(["oc14-producer"], files=OC14_COMPOSE, env_extra=env_extra)
    if "oc14-producer" in running:
        return "running"
    result = compose("ps", "--all", "--format", "json", "oc14-producer", timeout=30.0, check=False, files=OC14_COMPOSE, env_extra=env_extra)
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("Service") == "oc14-producer":
            return "exited" if payload.get("State") == "exited" else str(payload.get("State"))
    return "missing"


def producer_finished(env_extra: Dict[str, str]) -> bool:
    result = compose(
        "ps", "--all", "--format", "json", "oc14-producer",
        timeout=30.0, check=False, files=OC14_COMPOSE, env_extra=env_extra,
    )
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("Service") == "oc14-producer":
            return payload.get("State") == "exited" and int(payload.get("ExitCode") or 0) == 0
    return False


def create_oc14_topics(nsamples: int, run_id: str, env_extra: Dict[str, str]) -> None:
    for template in OC14_TOPICS:
        topic = template.format(nsamples=nsamples, run_id=run_id)
        compose(
            "exec", "-T", "kafka1",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "kafka1:9092",
            "--create", "--if-not-exists",
            "--topic", topic,
            "--partitions", "1",
            "--replication-factor", "3",
            timeout=30.0,
            env_extra=env_extra,
        )
        log(f"topic ready: {topic}")


# ---------------------------------------------------------------------------
# Docker guards and lifecycle (never destructive)
# ---------------------------------------------------------------------------


def oc14_containers_running(env_extra: Dict[str, str]) -> bool:
    return bool(services_running(OC14_SERVICES, files=OC14_COMPOSE, env_extra=env_extra))


def stop_oc14_services(env_extra: Dict[str, str]) -> None:
    """Stop and remove ONLY the oc14-* campaign services of this budget.

    Created topics, consumer groups and Avro subjects are never deleted:
    they stay available for audit.
    """
    compose("stop", *OC14_SERVICES, timeout=120.0, check=False, files=OC14_COMPOSE, env_extra=env_extra)
    compose("rm", "-f", *OC14_SERVICES, timeout=60.0, check=False, files=OC14_COMPOSE, env_extra=env_extra)
    try:
        wait_until(
            lambda: not oc14_containers_running(env_extra),
            timeout=60.0,
            description="all oc14 containers stopped and removed",
            interval=1.0,
        )
    except RunnerError as exc:
        # A cleanup problem must never mask the campaign result.
        print(f"[oc14] WARNING: cleanup incomplete: {exc}", file=sys.stderr)


def assert_standard_services_stopped() -> None:
    running = services_running(STANDARD_COMPUTE_SERVICES)
    if running:
        raise RunnerError(
            "standard compute services are still running: "
            + ", ".join(running)
            + ". Stop them first; the OC-14 runner never stops shared services."
        )


def dump_service_logs(env_extra: Dict[str, str], artifacts: Path) -> None:
    try:
        result = compose(
            "logs", "--tail", "60", *OC14_SERVICES,
            timeout=60.0, check=False, files=OC14_COMPOSE, env_extra=env_extra,
        )
        (artifacts / "services-failure-logs.txt").write_text(result.stdout, encoding="utf-8")
        print(f"[oc14] failure logs saved to {artifacts / 'services-failure-logs.txt'}")
    except Exception as exc:
        print(f"[oc14] could not dump service logs: {exc}")


# ---------------------------------------------------------------------------
# Lag sampler (1 Hz) and exports
# ---------------------------------------------------------------------------


class LagSampler:
    """Sample the v2 consumer lag and pipeline state every second.

    Runs in a background thread so the main wait loop is never blocked. Each
    sample records: monotonic t (seconds since campaign start), wall clock,
    v2 lag, v2 JSON output count, producer status and v2 status.
    """

    def __init__(
        self,
        v2_group: str,
        hstree_topic: str,
        v2_json_topic: str,
        env_extra: Dict[str, str],
        interval: float = 1.0,
    ) -> None:
        self.v2_group = v2_group
        self.hstree_topic = hstree_topic
        self.v2_json_topic = v2_json_topic
        self.env_extra = env_extra
        self.interval = interval
        self.samples: List[Dict] = []
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> List[Dict]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30.0)
        return self.samples

    def _run(self) -> None:
        start = time.monotonic()
        while not self._stop.is_set():
            sample_t = time.monotonic() - start
            sample = {
                "t": round(sample_t, 3),
                "wall_clock": dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "lag": consumer_group_lag(self.v2_group, self.hstree_topic, self.env_extra),
                "v2_json_count": topic_message_count(self.v2_json_topic, self.env_extra),
                "producer_status": producer_status(self.env_extra),
                "v2_status": (
                    "running"
                    if "oc14-v2" in services_running(["oc14-v2"], files=OC14_COMPOSE, env_extra=self.env_extra)
                    else "stopped"
                ),
            }
            self.samples.append(sample)
            self._stop.wait(self.interval)


def export_jsonl_topic(topic: str, output_path: Path, expected: int, env_extra: Dict[str, str], timeout: float) -> int:
    result = compose(
        "exec", "-T", "kafka1",
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server", "kafka1:9092",
        "--from-beginning",
        "--timeout-ms", "5000",
        "--topic", topic,
        timeout=timeout,
        env_extra=env_extra,
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


# ---------------------------------------------------------------------------
# Campaign analysis (uses the pure capacity_report helpers)
# ---------------------------------------------------------------------------

sys.path.insert(0, str(PROJECT_ROOT / "experiments" / "explainability" / "v2"))
from capacity_report import analyze_lag_samples, summarize_shap_times, validate_campaign  # noqa: E402


def analyze_campaign_export(
    budget: int,
    run_id: str,
    manifest: Dict,
    v2_export: Path,
    samples: List[Dict],
    producer_exit_time: Optional[float],
    completion_time: Optional[float],
    producer_started_time: float,
) -> Dict:
    """Build the per-budget campaign JSON from the raw export and samples."""
    totals_seen = set()
    status_counts: Dict[str, int] = {}
    shap_times: List[Optional[float]] = []
    seen_ids: List[str] = []
    duplicate_ids: List[str] = []
    missing = 0
    with open(v2_export, encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            record = json.loads(stripped)
            event_id = record.get("source_event_id")
            if event_id is None:
                missing += 1
                continue
            if event_id in totals_seen:
                duplicate_ids.append(event_id)
            totals_seen.add(event_id)
            seen_ids.append(event_id)
            status = record.get("explanation_status")
            if status is None:
                status = "missing_status"
            status_counts[status] = status_counts.get(status, 0) + 1
            shap_times.append(record.get("shap_computation_time_ms"))

    expected_total = int(manifest["expected_total_events"])
    missing_keys = expected_total - len(totals_seen)
    eligible = status_counts.get("computed", 0) + status_counts.get("failed", 0)

    lag_analysis = analyze_lag_samples(samples, producer_exit_time, completion_time)
    drain_seconds = lag_analysis.get("drain_seconds")

    return {
        "budget": budget,
        "run_id": run_id,
        "totals": {
            "expected": expected_total,
            "hstree": None,  # filled by the runner from get-offsets
            "v2": len(totals_seen),
            "v2_json": len(seen_ids),
        },
        "status_distribution": status_counts,
        "shap": {
            "eligible": eligible,
            "computed": status_counts.get("computed", 0),
            "failed": status_counts.get("failed", 0),
        },
        "duplicates": {"source_event_id": sorted(duplicate_ids)},
        "missing_keys_count": missing_keys + missing,
        "producer": {
            "started_at": round(producer_started_time, 3),
            "exit_at": round(producer_exit_time, 3) if producer_exit_time is not None else None,
            "duration_seconds": (
                round(producer_exit_time - producer_started_time, 3)
                if producer_exit_time is not None
                else None
            ),
        },
        "completion_time": round(completion_time, 3) if completion_time is not None else None,
        "total_duration_seconds": (
            round(completion_time - producer_started_time, 3)
            if completion_time is not None
            else None
        ),
        "lag_analysis": lag_analysis,
        "delivery_throughput_events_per_second": (
            round(eligible / drain_seconds, 6)
            if drain_seconds is not None and drain_seconds > 0
            else None
        ),
        "shap_time_stats": summarize_shap_times(shap_times),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--budgets", default=",".join(str(b) for b in DEFAULT_BUDGETS))
    parser.add_argument("--skip-offline", action="store_true", help="skip the offline KernelSHAP benchmark")
    parser.add_argument("--artifacts-dir", default=None, help="artifacts directory (default: /tmp/oc14-capacity-<run-id>)")
    parser.add_argument("--keep-up", action="store_true", help="keep the oc14 services running after the run (success AND failure) for manual inspection")
    parser.add_argument("--global-timeout", type=float, default=3600.0, help="global deadline in seconds across all budgets")
    parser.add_argument("--target-events-per-second-per-group", type=float, default=None, help="optional SLO: target input rate per group")
    parser.add_argument("--max-drain-seconds", type=float, default=None, help="optional SLO: maximal acceptable drain duration")
    parser.add_argument("--assumed-input-rates", default=None, help="comma-separated assumed input rates (events/s per group)")
    args = parser.parse_args(argv)

    budgets = sorted({int(b) for b in args.budgets.split(",") if b.strip()})
    campaign_run_id = generate_run_id()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    expected_total = int(manifest["expected_total_events"])

    artifacts = Path(args.artifacts_dir) if args.artifacts_dir else Path("/tmp") / f"oc14-capacity-{campaign_run_id}"
    artifacts.mkdir(parents=True, exist_ok=True)
    host_info = {
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "python_version": sys.version.split()[0],
        "docker_compose": "v2",
    }
    (artifacts / "host-info.json").write_text(json.dumps(host_info, indent=2) + "\n", encoding="utf-8")
    (artifacts / "campaign-env.json").write_text(
        json.dumps({"campaign_run_id": campaign_run_id, "budgets": budgets, "manifest": str(MANIFEST)}, indent=2) + "\n",
        encoding="utf-8",
    )
    log(f"campaign run id: {campaign_run_id} | budgets: {budgets} | artifacts: {artifacts}")

    deadline = time.monotonic() + args.global_timeout

    def remaining(cap: float = 300.0) -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise RunnerError(f"global timeout of {args.global_timeout}s exceeded")
        return min(left, cap)

    error_message: Optional[str] = None
    oc14_started = False
    budget_envs_started: List[Dict[str, str]] = []
    try:
        log("checking that the standard compute services are stopped")
        assert_standard_services_stopped()

        log("ensuring infrastructure is up (never stopped by this runner)")
        compose("up", "-d", *INFRA_SERVICES, timeout=remaining())

        offline_path: Optional[Path] = None
        if not args.skip_offline:
            log("running the offline KernelSHAP benchmark (background 100, 81 features)")
            offline_path = artifacts / "offline-benchmark.json"
            result = run_command(
                [
                    sys.executable,
                    str(CAPACITY_BENCHMARK),
                    "--csv", str(PROJECT_ROOT / manifest["source_csv"]),
                    "--manifest", str(MANIFEST),
                    "--budgets", ",".join(str(b) for b in budgets),
                    "--calls-per-budget", "3",
                    "--output", str(offline_path),
                ],
                timeout=remaining(1200.0),
                check=False,
                env_extra={"PYTHONPATH": "src:."},
            )
            if result.returncode != 0:
                raise RunnerError("offline capacity benchmark failed")
            for line in result.stdout.splitlines():
                if line.startswith("[oc14-offline]"):
                    log(line.split("] ", 1)[1])

        campaign_files: List[Path] = []
        for budget in budgets:
            budget_run_id = generate_run_id()
            env_extra = {
                "OC14_RUN_ID": budget_run_id,
                "OC14_NSAMPLES": str(budget),
            }
            budget_dir = artifacts / f"ns{budget}"
            budget_dir.mkdir(parents=True, exist_ok=True)
            (budget_dir / "campaign-env.json").write_text(
                json.dumps({**env_extra, "manifest": str(MANIFEST)}, indent=2) + "\n",
                encoding="utf-8",
            )
            log(f"=== budget {budget} (run id {budget_run_id}) ===")

            create_oc14_topics(budget, budget_run_id, env_extra)

            # Tracked so the finally below can clean up even when this very
            # command fails partway through.
            oc14_started = True
            budget_envs_started.append(env_extra)
            compose("up", "-d", *OC14_SERVICES, timeout=remaining(), files=OC14_COMPOSE, env_extra=env_extra)

            hstree_topic = f"oc14-ns{budget}-hstree-{budget_run_id}"
            v2_json_topic = f"oc14-ns{budget}-v2-viz-{budget_run_id}"
            v2_group = f"oc14-ns{budget}-v2-consumer-{budget_run_id}"

            sampler = LagSampler(v2_group, hstree_topic, v2_json_topic, env_extra)
            sampler.start()
            campaign_start = time.monotonic()
            producer_exit_time: Optional[float] = None
            completion_time: Optional[float] = None
            try:
                log("waiting for the oc14 producer to finish")
                while time.monotonic() - campaign_start < remaining():
                    if producer_finished(env_extra):
                        producer_exit_time = time.monotonic() - campaign_start
                        break
                    time.sleep(1.0)
                if producer_exit_time is None:
                    raise RunnerError("producer did not exit cleanly")

                log(f"waiting for {expected_total} events and a zero v2 lag (drain)")
                def drained() -> bool:
                    lag = consumer_group_lag(v2_group, hstree_topic, env_extra)
                    return lag == 0 and topic_message_count(v2_json_topic, env_extra) >= expected_total

                # The drain is dominated by SHAP: allow roughly twice the
                # worst-case offline estimate (eligible events x max budget
                # mean) but never exceed the global deadline.
                drain_cap = max(
                    600.0,
                    2.5 * int(manifest["expected_shap_events"]) * (max(budgets) / 1000.0) * 30.0,
                )
                wait_until(drained, remaining(drain_cap), "drain completion (zero lag and full output)", interval=2.0)
                completion_time = time.monotonic() - campaign_start
            finally:
                samples = sampler.stop()
                (budget_dir / "lag-samples.json").write_text(
                    json.dumps(samples, indent=2) + "\n", encoding="utf-8"
                )

            log("exporting the v2 JSON topic")
            export_jsonl_topic(
                v2_json_topic, budget_dir / "oc14-v2-viz.jsonl", expected_total, env_extra, remaining(120.0)
            )
            hstree_count = topic_message_count(hstree_topic, env_extra)

            campaign = analyze_campaign_export(
                budget,
                budget_run_id,
                manifest,
                budget_dir / "oc14-v2-viz.jsonl",
                samples,
                producer_exit_time,
                completion_time,
                0.0,
            )
            campaign["totals"]["hstree"] = hstree_count
            reasons = validate_campaign(campaign, manifest)
            campaign["invalid_reasons"] = reasons
            campaign["valid"] = not reasons
            (budget_dir / "campaign.json").write_text(
                json.dumps(campaign, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            campaign_files.append(budget_dir / "campaign.json")
            log(
                f"budget {budget}: drain={campaign['lag_analysis'].get('drain_seconds')}s | "
                f"max_lag={campaign['lag_analysis'].get('max_lag')} | "
                f"final_lag={campaign['lag_analysis'].get('final_lag')} | "
                f"lag_area={campaign['lag_analysis'].get('lag_area_event_seconds')} ev·s | "
                f"throughput={campaign['delivery_throughput_events_per_second']} ev/s"
            )
            if not campaign["valid"]:
                raise RunnerError(
                    f"budget {budget} measurement is INVALID (no performance conclusion): "
                    + "; ".join(reasons)
                )

            # Per-budget cleanup: only oc14-* containers; topics/groups/subjects stay.
            if not args.keep_up:
                log(f"stopping the oc14 services of budget {budget} (only oc14-* services)")
                stop_oc14_services(env_extra)
                oc14_started = False
            else:
                log(f"keeping the oc14 services of budget {budget} up (--keep-up)")

        log("aggregating the four budget campaigns into the capacity report")
        json_out = artifacts / "oc14-capacity-report.json"
        markdown_out = artifacts / "oc14-capacity-report.md"
        report_command = [
            sys.executable,
            str(CAPACITY_REPORT),
            "--manifest", str(MANIFEST),
            "--host-info", str(artifacts / "host-info.json"),
            "--json-out", str(json_out),
            "--markdown-out", str(markdown_out),
        ]
        if offline_path is not None:
            report_command += ["--offline", str(offline_path)]
        for campaign_file in campaign_files:
            report_command += ["--campaign", str(campaign_file)]
        if args.target_events_per_second_per_group is not None:
            report_command += ["--target-events-per-second-per-group", str(args.target_events_per_second_per_group)]
        if args.max_drain_seconds is not None:
            report_command += ["--max-drain-seconds", str(args.max_drain_seconds)]
        if args.assumed_input_rates:
            report_command += ["--assumed-input-rates", args.assumed_input_rates]
        run_command(report_command, timeout=remaining(120.0), check=True, env_extra={"PYTHONPATH": "src:."})

        versioned = REPORTS_DIR / f"oc14-capacity-{campaign_run_id}.md"
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        versioned.write_text(markdown_out.read_text(encoding="utf-8"), encoding="utf-8")

        (artifacts / "runner-metrics.json").write_text(
            json.dumps(
                {
                    "campaign_run_id": campaign_run_id,
                    "budgets": budgets,
                    "global_timeout": args.global_timeout,
                    "slo": {
                        "target_events_per_second_per_group": args.target_events_per_second_per_group,
                        "max_drain_seconds": args.max_drain_seconds,
                    },
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        log(f"artifacts: {artifacts}")
        log(f"versioned report: {versioned}")
    except RunnerError as exc:
        error_message = str(exc)
    except Exception as exc:  # the runner must never hang or fail silently
        error_message = f"unexpected failure: {exc}"
    finally:
        if error_message is not None:
            print(f"[oc14] FAILED: {error_message}", file=sys.stderr)
            # Diagnostics are written BEFORE the cleanup so a stopped service
            # can no longer report its state. Only the budget envs that
            # actually started their services are logged.
            for budget_env in budget_envs_started:
                try:
                    dump_service_logs(budget_env)
                except Exception as log_exc:
                    print(f"[oc14] could not dump service logs: {log_exc}", file=sys.stderr)
        if oc14_started and not args.keep_up:
            # Default behavior: the oc14-* containers are cleaned up on
            # success AND on failure. Only --keep-up keeps them running.
            # A cleanup failure never masks the campaign result above.
            for budget_env in budget_envs_started:
                try:
                    log("stopping the remaining oc14 services (only oc14-* services)")
                    stop_oc14_services(budget_env)
                except Exception as cleanup_exc:
                    print(f"[oc14] WARNING: cleanup failed: {cleanup_exc}", file=sys.stderr)
        if error_message is not None:
            print(f"[oc14] artifacts kept for diagnosis: {artifacts}", file=sys.stderr)
            return 1

    log(f"OC-14 capacity campaign PASSED — artifacts: {artifacts}")
    print((artifacts / "oc14-capacity-report.md").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
