#!/usr/bin/env python3
"""OC-13 — non-destructive Docker runner for the controlled regular/v2 comparison.

Chain under test:
CSV fixture -> oc13 producer -> oc13 hstree -> {oc13 regular, oc13 v2} ->
dedicated JSON viz topics -> raw exports -> pure comparator.

Scientific scope: check that v2 preserves the regular SHAP baseline on a
single-group fixture with unique (Group, timestamp) couples. The comparator
(OC-13 B) holds the decision logic; this runner only orchestrates Docker,
waits for completion signals, exports the raw JSON topics and runs the
comparator.

Non-destructive by construction (OC-12R.1 guarantees):
- every created resource carries the OC-13 run id: topics, consumer groups
  and Avro subjects (TopicNameStrategy derives subjects from the dedicated
  topic names);
- the runner NEVER deletes topics, consumer groups, schema subjects or data;
- the runner REFUSES to start when the standard compute services are running;
- only the oc13-* services are started and stopped;
- artifacts are always preserved: they are the campaign proof.

This script is never executed by pytest. The comparison decision NEVER
depends on a log line: it comes from the comparator report on the raw
Kafka JSON exports.

Usage:
    PYTHONPATH=src:. .venv/bin/python \\
        scripts/integration/run-explainability-regular-v2-comparison.py \\
        [--artifacts-dir DIR] [--keep-up] [--global-timeout S]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "explainability_v2"
MANIFEST = FIXTURE_DIR / "oc13_manifest.json"
COMPARATOR = PROJECT_ROOT / "experiments" / "explainability" / "v2" / "compare_regular_v2.py"

BASE_COMPOSE = ["-f", "docker-compose.yml"]
OC13_COMPOSE = BASE_COMPOSE + ["-f", "docker/compose/experiments/docker-compose.oc13.yml"]

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

OC13_SERVICES = ["oc13-producer", "oc13-hstree", "oc13-regular", "oc13-v2"]

OC13_TOPICS = [
    "oc13-source-{run_id}",
    "oc13-hstree-{run_id}",
    "oc13-hstree-viz-{run_id}",
    "oc13-regular-{run_id}",
    "oc13-regular-viz-{run_id}",
    "oc13-v2-{run_id}",
    "oc13-v2-viz-{run_id}",
]


class RunnerError(RuntimeError):
    """Fatal campaign error; artifacts are preserved for diagnosis."""


def log(message: str) -> None:
    print(f"[oc13 {dt.datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def generate_run_id(now: Optional[dt.datetime] = None) -> str:
    """Build a unique OC-13 run id: oc13-YYYYMMDD-HHMMSS-<8 hex chars>.

    The timestamp keeps artifacts sortable; the random suffix makes
    concurrent or same-second runs unique. Only lowercase letters, digits
    and dashes: valid for Kafka topics/groups and Docker container names.
    """
    stamp = (now or dt.datetime.now()).strftime("%Y%m%d-%H%M%S")
    return f"oc13-{stamp}-{uuid.uuid4().hex[:8]}"


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
    return {"OC13_RUN_ID": run_id}


def oc13_topic(template: str) -> str:
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
        "ps", "--all", "--format", "json", "oc13-producer",
        timeout=30.0, check=False, files=OC13_COMPOSE,
    )
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("Service") == "oc13-producer":
            return payload.get("State") == "exited" and int(payload.get("ExitCode") or 0) == 0
    return False


def create_oc13_topics() -> None:
    for template in OC13_TOPICS:
        topic = oc13_topic(template)
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


def oc13_containers_running() -> bool:
    return bool(services_running(OC13_SERVICES, files=OC13_COMPOSE))


def stop_oc13_services() -> None:
    """Stop and remove ONLY the oc13-* campaign services.

    Shared resources are never touched: topics, consumer groups and Avro
    subjects stay in place for manual inspection.
    """
    compose("stop", *OC13_SERVICES, timeout=120.0, check=False, files=OC13_COMPOSE)
    compose("rm", "-f", *OC13_SERVICES, timeout=60.0, check=False, files=OC13_COMPOSE)
    try:
        wait_until(
            lambda: not oc13_containers_running(),
            timeout=60.0,
            description="all oc13 containers stopped and removed",
            interval=1.0,
        )
    except RunnerError as exc:
        # A cleanup problem must never mask the campaign result.
        print(f"[oc13] WARNING: cleanup incomplete: {exc}", file=sys.stderr)


def assert_standard_services_stopped() -> None:
    running = services_running(STANDARD_COMPUTE_SERVICES)
    if running:
        raise RunnerError(
            "standard compute services are still running: "
            + ", ".join(running)
            + ". Stop them first; the OC-13 runner never stops shared services."
        )


# ---------------------------------------------------------------------------
# Exports and comparison
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
            "logs", "--tail", "60", *OC13_SERVICES,
            timeout=60.0, check=False, files=OC13_COMPOSE,
        )
        (artifacts / "services-failure-logs.txt").write_text(result.stdout, encoding="utf-8")
        print(f"[oc13] failure logs saved to {artifacts / 'services-failure-logs.txt'}")
    except Exception as exc:
        print(f"[oc13] could not dump service logs: {exc}")


def run_comparator(artifacts: Path, timeout: float) -> Dict:
    """Run the pure OC-13 comparator on the raw exports; return its report."""
    metrics = json.loads((artifacts / "runner-metrics.json").read_text(encoding="utf-8"))
    result = run_command(
        [
            sys.executable,
            str(COMPARATOR),
            "--manifest", str(MANIFEST),
            "--regular-export", str(artifacts / "oc13-regular-viz.jsonl"),
            "--v2-export", str(artifacts / "oc13-v2-viz.jsonl"),
            "--runner-metrics", str(artifacts / "runner-metrics.json"),
            "--output", str(artifacts / "oc13-comparison-report.json"),
        ],
        timeout=timeout,
        check=False,
        env_extra={"PYTHONPATH": "src:."},
    )
    if result.returncode != 0:
        raise RunnerError(
            "OC-13 comparator failed (see oc13-comparison-report.json)\n"
            f"stderr: {result.stderr[-2000:]}"
        )
    report = json.loads(
        (artifacts / "oc13-comparison-report.json").read_text(encoding="utf-8")
    )
    if not report.get("passed"):
        raise RunnerError(
            "OC-13 comparator verdict is FAILED (see oc13-comparison-report.json)"
        )
    return report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    global _RUN_ID
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--artifacts-dir", default=None, help="artifacts directory (default: /tmp/oc13-comparison-<run-id>)")
    parser.add_argument("--keep-up", action="store_true", help="keep the oc13 services running after the run (success AND failure) for manual inspection")
    parser.add_argument("--global-timeout", type=float, default=900.0, help="global campaign deadline in seconds")
    args = parser.parse_args(argv)

    _RUN_ID = generate_run_id()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    expected = int(manifest["expected_total_events"])
    hstree_viz_topic = oc13_topic("oc13-hstree-viz-{run_id}")
    regular_viz_topic = oc13_topic("oc13-regular-viz-{run_id}")
    v2_viz_topic = oc13_topic("oc13-v2-viz-{run_id}")
    hstree_topic = oc13_topic("oc13-hstree-{run_id}")
    regular_group = f"oc13-regular-consumer-{_RUN_ID}"
    v2_group = f"oc13-v2-consumer-{_RUN_ID}"

    artifacts = Path(args.artifacts_dir) if args.artifacts_dir else Path("/tmp") / f"oc13-comparison-{_RUN_ID}"
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "campaign-env.json").write_text(
        json.dumps({**campaign_env(), "manifest": str(MANIFEST)}, indent=2) + "\n",
        encoding="utf-8",
    )
    log(f"run id: {_RUN_ID} | artifacts: {artifacts}")

    campaign_started = time.monotonic()
    deadline = campaign_started + args.global_timeout

    def remaining(cap: float = 240.0) -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise RunnerError(f"global timeout of {args.global_timeout}s exceeded")
        return min(left, cap)

    error_message: Optional[str] = None
    oc13_started = False
    try:
        log("checking that the standard compute services are stopped")
        assert_standard_services_stopped()

        log("ensuring infrastructure is up (never stopped by this runner)")
        compose("up", "-d", *INFRA_SERVICES, timeout=remaining())
        create_oc13_topics()

        log("starting the oc13 comparison services")
        # Tracked so the finally below can clean up even when this very
        # command fails partway through.
        oc13_started = True
        compose("up", "-d", *OC13_SERVICES, timeout=remaining(), files=OC13_COMPOSE)

        log("waiting for the oc13 producer to finish")
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

        log(f"waiting for zero lags ({regular_group}, {v2_group})")
        def lags_drained() -> bool:
            regular_lag = consumer_group_lag(regular_group, hstree_topic)
            v2_lag = consumer_group_lag(v2_group, hstree_topic)
            return regular_lag == 0 and v2_lag == 0

        wait_until(lags_drained, remaining(), "zero regular and v2 consumer lags")

        log("exporting the regular and v2 JSON topics")
        regular_count = export_jsonl_topic(
            regular_viz_topic, artifacts / "oc13-regular-viz.jsonl", expected, remaining(120.0)
        )
        v2_count = export_jsonl_topic(
            v2_viz_topic, artifacts / "oc13-v2-viz.jsonl", expected, remaining(120.0)
        )
        log(f"exported regular={regular_count} v2={v2_count}")

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
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        log("running the OC-13 pure comparator on the raw exports")
        report = run_comparator(artifacts, remaining(120.0))
        log(
            "comparator verdict: PASSED — "
            f"equal={len(report['categories']['both_computed_equal'])} "
            f"both_null={len(report['categories']['both_null'])} "
            f"divergent={len(report['categories']['both_computed_different'])}"
        )

        log(f"artifacts: {artifacts}")
    except RunnerError as exc:
        error_message = str(exc)
    except Exception as exc:  # the runner must never hang or fail silently
        error_message = f"unexpected failure: {exc}"
    finally:
        if error_message is not None:
            print(f"[oc13] FAILED: {error_message}", file=sys.stderr)
            # Diagnostics are written BEFORE the cleanup so a stopped service
            # can no longer report its state.
            try:
                dump_service_logs(artifacts)
            except Exception as log_exc:
                print(f"[oc13] could not dump service logs: {log_exc}", file=sys.stderr)
        if oc13_started and not args.keep_up:
            # Default behavior: the oc13-* containers are cleaned up on
            # success AND on failure. Only --keep-up keeps them running.
            # A cleanup failure never masks the campaign result above.
            try:
                log("stopping the oc13 comparison services (only oc13-* services)")
                stop_oc13_services()
            except Exception as cleanup_exc:
                print(f"[oc13] WARNING: cleanup failed: {cleanup_exc}", file=sys.stderr)
        if error_message is not None:
            print(f"[oc13] artifacts kept for diagnosis: {artifacts}", file=sys.stderr)
            return 1

    log(f"OC-13 comparison PASSED — artifacts: {artifacts}")
    print((artifacts / "oc13-comparison-report.json").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
