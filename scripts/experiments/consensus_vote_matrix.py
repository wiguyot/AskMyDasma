#!/usr/bin/env python3
"""
consensus_vote_matrix.py

Automated experiment that proves the consensus service reports model
anomalies correctly for every implemented model, first individually, then in
selected combinations.

Run from the Docker network:
    docker compose run --rm --entrypoint python consensus-analysis \
      scripts/experiments/consensus_vote_matrix.py

Or locally (requires confluent-kafka + influxdb-client):
    python scripts/experiments/consensus_vote_matrix.py
"""

import json
import os
import argparse
import csv
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime
from io import StringIO
from typing import Optional
from urllib import request, error

try:
    from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
except ImportError:
    sys.stderr.write("ERROR: confluent-kafka is required. Install via: pip install confluent-kafka\n")
    sys.exit(1)

IN_DOCKER = os.path.exists("/.dockerenv")
BOOTSTRAP_SERVERS = os.getenv(
    "BOOTSTRAP_SERVERS",
    "kafka1:9092,kafka2:9092,kafka3:9092" if IN_DOCKER else "localhost:9092",
)
if not IN_DOCKER and any(name in BOOTSTRAP_SERVERS for name in ("kafka1", "kafka2", "kafka3")):
    BOOTSTRAP_SERVERS = "localhost:9092"
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086" if IN_DOCKER else "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "dasma-super-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "dasma")
INFLUX_BUCKET = "consensus-analysis"
DEFAULT_TIMEOUT = int(os.getenv("CONSENSUS_TIMEOUT_SEC", "5"))
DEFAULT_RETENTION = int(os.getenv("CONSENSUS_RETENTION_SEC", "10"))
TIMEOUT_WAIT = 2.0
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

MODEL_TOPICS = {
    "hstree": "hstree-analysis-viz",
    "iforest": "iforest-analysis-viz",
    "kitnet": "kitnet-analysis-viz",
    "deepant": "deepant-analysis-viz",
    "mdragpoint": "mdragstream-point-analysis-viz",
}

CONSENSUS_TOPIC = "consensus-analysis"
EXP_GROUP = "EXP-CONSENSUS"


@dataclass
class TestCase:
    case_id: str
    timestamp: str
    group: str = EXP_GROUP
    selected_models: list[str] = field(default_factory=list)
    verdicts: dict[str, bool] = field(default_factory=dict)
    timeout_sec: int = DEFAULT_TIMEOUT
    retention_sec: int = DEFAULT_RETENTION
    expect_complete: bool = True
    inject_delay: float = 0.1


def _run_command(args: list[str], env: Optional[dict[str, str]] = None) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            args,
            cwd=ROOT_DIR,
            env={**os.environ, **(env or {})},
            text=True,
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        details = "\n".join(
            part
            for part in (
                f"command failed: {' '.join(args)}",
                f"returncode={exc.returncode}",
                f"stdout={exc.stdout.strip()}" if exc.stdout else "",
                f"stderr={exc.stderr.strip()}" if exc.stderr else "",
            )
            if part
        )
        raise RuntimeError(details) from exc


def _restart_consensus(case: TestCase) -> None:
    if IN_DOCKER:
        raise RuntimeError(
            "Cannot restart docker compose services from inside a container. "
            "Run this script from the host, or start consensus-analysis manually "
            f"with DASMA_SELECTED_MODELS={','.join(case.selected_models)}."
        )
    if shutil.which("docker") is None:
        raise RuntimeError("docker command not found; cannot manage consensus-analysis")

    env = {
        "DASMA_SELECTED_MODELS": ",".join(case.selected_models),
        "CONSENSUS_TIMEOUT_SEC": str(case.timeout_sec),
        "CONSENSUS_RETENTION_SEC": str(case.retention_sec),
    }
    _run_command(
        ["docker", "compose", "up", "--pull", "never", "-d", "--force-recreate", "consensus-analysis"],
        env=env,
    )
    time.sleep(2.0)


def _produce(producer: Producer, topic: str, key: str, value: dict) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
        callback=_delivery_callback,
    )
    producer.flush(timeout=5.0)


def _delivery_callback(err, msg):
    if err:
        raise KafkaException(f"Delivery failed for {msg.topic()}: {err}")


def _build_model_message(case: TestCase, model_id: str) -> dict:
    anomaly_val = case.verdicts.get(model_id, False)
    return {
        "timestamp": case.timestamp,
        "source_event_id": case.case_id,
        "Group": case.group,
        f"{model_id}_anomaly": anomaly_val,
        f"{model_id}_score": 0.999 if anomaly_val else 0.001,
        f"{model_id}_status": "anomaly" if anomaly_val else "normal",
    }


def _publish_verdicts(producer: Producer, case: TestCase) -> None:
    for model_id in case.verdicts:
        msg = _build_model_message(case, model_id)
        _produce(producer, MODEL_TOPICS[model_id], case.group, msg)
        if case.inject_delay > 0:
            time.sleep(case.inject_delay)


def _consume_consensus(case_id: str, timeout: int) -> Optional[dict]:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"consensus-exp-reader-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([CONSENSUS_TOPIC])
    deadline = time.time() + timeout
    record = None
    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue
            if payload.get("source_event_id") == case_id:
                record = payload
                break
    finally:
        consumer.close()
    return record


def _validate_consensus(case: TestCase, record: dict) -> tuple[bool, list[str]]:
    errors: list[str] = []
    expected_count = sum(1 for m in case.selected_models if case.verdicts.get(m, False))

    if record.get("Group") != case.group:
        errors.append(f"Group mismatch: got {record.get('Group')}")
    if record.get("source_event_id") != case.case_id:
        errors.append(f"source_event_id mismatch: got {record.get('source_event_id')}")
    if record.get("timestamp") != case.timestamp:
        errors.append(f"timestamp mismatch: got {record.get('timestamp')}")

    active = len(case.selected_models)
    if record.get("active_models") != active:
        errors.append(f"active_models: expected {active}, got {record.get('active_models')}")
    if record.get("reported_models") != active:
        errors.append(f"reported_models: expected {active}, got {record.get('reported_models')}")

    if case.expect_complete:
        if not record.get("complete"):
            errors.append(f"complete: expected True, got {record.get('complete')}")

    if record.get("anomaly_count") != expected_count:
        errors.append(f"anomaly_count: expected {expected_count}, got {record.get('anomaly_count')}")

    for model_id in case.selected_models:
        expected_anomaly = case.verdicts.get(model_id, False)
        if record.get(f"{model_id}_anomaly") != expected_anomaly:
            errors.append(f"{model_id}_anomaly: expected {expected_anomaly}")
        expected_vote = 1 if expected_anomaly else 0
        if record.get(f"{model_id}_vote") != expected_vote:
            errors.append(f"{model_id}_vote: expected {expected_vote}")

    all_models = {"hstree", "iforest", "kitnet", "deepant", "mdragpoint"}
    for model_id in all_models - set(case.selected_models):
        if record.get(f"{model_id}_vote") is not None:
            errors.append(f"{model_id}_vote: should be null for non-selected model")

    return len(errors) == 0, errors


def _query_influx(flux: str) -> list[dict[str, str]]:
    payload = json.dumps({"query": flux, "type": "flux"}).encode("utf-8")
    req = request.Request(
        f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}",
        data=payload,
        headers={
            "Authorization": f"Token {INFLUX_TOKEN}",
            "Accept": "application/csv",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=5) as response:
        text = response.read().decode("utf-8")
    data_lines = [line for line in text.splitlines() if line and not line.startswith("#")]
    if not data_lines:
        return []
    return list(csv.DictReader(StringIO("\n".join(data_lines))))


def _validate_influxdb(case: TestCase, kafka_record: dict, wait_sec: float = 10.0) -> tuple[bool, str]:
    expected_fields = {"anomaly_count", *(f"{model}_vote" for model in case.selected_models)}
    expected_values = {
        "anomaly_count": str(sum(1 for m in case.selected_models if case.verdicts.get(m, False))),
        **{
            f"{model}_vote": str(1 if case.verdicts.get(model, False) else 0)
            for model in case.selected_models
        },
    }
    fields_filter = " or ".join(f'r["_field"] == "{field}"' for field in sorted(expected_fields))
    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -15m)
  |> filter(fn: (r) => r["_measurement"] == "consensus-analysis")
  |> filter(fn: (r) => r["Group"] == "{case.group}")
  |> filter(fn: (r) => r["source_event_id"] == "{case.case_id}")
  |> filter(fn: (r) => {fields_filter})
  |> last()
'''
    deadline = time.time() + wait_sec
    last_error = ""
    while time.time() < deadline:
        try:
            rows = _query_influx(flux)
            values = {row.get("_field", ""): row.get("_value", "") for row in rows}
            if expected_fields.issubset(values):
                mismatches = [
                    f"{field}: expected {expected}, got {values.get(field)}"
                    for field, expected in expected_values.items()
                    if values.get(field) != expected
                ]
                if mismatches:
                    return False, "; ".join(mismatches)
                return True, "InfluxDB numeric fields match"
            last_error = f"missing fields: {sorted(expected_fields - set(values))}; seen={values}"
        except error.URLError as exc:
            last_error = f"InfluxDB HTTP error: {exc}"
        except Exception as exc:
            last_error = f"InfluxDB error: {exc}"
        time.sleep(1.0)
    return False, last_error or "No InfluxDB record found"


def _validate_influxdb_available() -> tuple[bool, str]:
    try:
        _query_influx(f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -1m) |> limit(n: 1)')
        return True, "InfluxDB query API reachable"
    except Exception as exc:
        return False, f"InfluxDB unavailable: {exc}"


def _run_case(producer: Producer, case: TestCase, *, manage_consensus: bool, validate_influx: bool) -> dict:
    result: dict = {
        "case_id": case.case_id,
        "selected_models": case.selected_models,
        "verdicts": case.verdicts,
        "expected_anomaly_count": sum(1 for m in case.selected_models if case.verdicts.get(m, False)),
        "kafka_consensus": None,
        "influx_validation": None,
        "passed": False,
        "errors": [],
    }
    try:
        if manage_consensus:
            _restart_consensus(case)
        _publish_verdicts(producer, case)
        timeout = case.timeout_sec + TIMEOUT_WAIT
        record = _consume_consensus(case.case_id, timeout)
        if record is None:
            result["errors"].append("No consensus record found")
            return result
        result["kafka_consensus"] = record
        passed, errors = _validate_consensus(case, record)
        result["errors"].extend(errors)
        influx_ok = True
        if validate_influx:
            influx_ok, influx_msg = _validate_influxdb(case, record)
            result["influx_validation"] = influx_msg
            if not influx_ok:
                result["errors"].append(f"InfluxDB: {influx_msg}")
        else:
            result["influx_validation"] = "skipped"
        result["passed"] = passed and influx_ok
    except Exception as e:
        result["errors"].append(f"Exception: {e}")
    return result


def get_test_cases() -> list[TestCase]:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cases = []

    # Phase 1: Individual Models
    for model_id in ["hstree", "iforest", "kitnet", "deepant", "mdragpoint"]:
        cases.append(TestCase(
            case_id=f"exp-single-{model_id}-{uuid.uuid4().hex[:8]}",
            timestamp=ts,
            selected_models=[model_id],
            verdicts={model_id: True},
            expect_complete=True,
        ))

    # Negative control
    cases.append(TestCase(
        case_id=f"exp-single-kitnet-normal-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["kitnet"],
        verdicts={"kitnet": False},
        expect_complete=True,
    ))

    # Phase 2: Pairwise
    cases.append(TestCase(
        case_id=f"exp-pair-hstree-kitnet-both-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["hstree", "kitnet"],
        verdicts={"hstree": True, "kitnet": True},
    ))
    cases.append(TestCase(
        case_id=f"exp-pair-hstree-kitnet-one-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["hstree", "kitnet"],
        verdicts={"hstree": True, "kitnet": False},
    ))
    cases.append(TestCase(
        case_id=f"exp-pair-iforest-mdragpoint-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["iforest", "mdragpoint"],
        verdicts={"iforest": True, "mdragpoint": True},
    ))
    cases.append(TestCase(
        case_id=f"exp-pair-deepant-hstree-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["deepant", "hstree"],
        verdicts={"deepant": True, "hstree": True},
    ))
    cases.append(TestCase(
        case_id=f"exp-pair-kitnet-mdragpoint-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["kitnet", "mdragpoint"],
        verdicts={"kitnet": False, "mdragpoint": True},
    ))

    # Phase 3: Higher-order
    cases.append(TestCase(
        case_id=f"exp-triple-core-all-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["hstree", "iforest", "kitnet"],
        verdicts={"hstree": True, "iforest": True, "kitnet": True},
    ))
    cases.append(TestCase(
        case_id=f"exp-triple-core-two-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["hstree", "iforest", "kitnet"],
        verdicts={"hstree": True, "iforest": False, "kitnet": True},
    ))
    cases.append(TestCase(
        case_id=f"exp-triple-heavy-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["deepant", "kitnet", "mdragpoint"],
        verdicts={"deepant": True, "kitnet": True, "mdragpoint": True},
    ))
    cases.append(TestCase(
        case_id=f"exp-quad-no-deepant-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["hstree", "iforest", "kitnet", "mdragpoint"],
        verdicts={"hstree": True, "iforest": True, "kitnet": True, "mdragpoint": True},
    ))
    cases.append(TestCase(
        case_id=f"exp-all-five-all-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["hstree", "iforest", "kitnet", "deepant", "mdragpoint"],
        verdicts={"hstree": True, "iforest": True, "kitnet": True, "deepant": True, "mdragpoint": True},
    ))
    cases.append(TestCase(
        case_id=f"exp-all-five-mixed-{uuid.uuid4().hex[:8]}",
        timestamp=ts,
        selected_models=["hstree", "iforest", "kitnet", "deepant", "mdragpoint"],
        verdicts={"hstree": True, "iforest": False, "kitnet": True, "deepant": False, "mdragpoint": True},
    ))

    # Phase 4: Synchronization - Out-of-order
    oo_case_id = f"exp-oo-arrival-{uuid.uuid4().hex[:8]}"
    cases.append(TestCase(
        case_id=oo_case_id,
        timestamp=ts,
        selected_models=["hstree", "iforest", "kitnet", "mdragpoint"],
        verdicts={"kitnet": True, "mdragpoint": True, "hstree": False, "iforest": True},
        inject_delay=0.5,
    ))

    # Phase 4: Same second, different events
    same_sec_id_a = f"exp-same-sec-a-{uuid.uuid4().hex[:8]}"
    same_sec_id_b = f"exp-same-sec-b-{uuid.uuid4().hex[:8]}"
    cases.append(TestCase(
        case_id=same_sec_id_a,
        timestamp=ts,
        selected_models=["kitnet", "mdragpoint"],
        verdicts={"kitnet": True, "mdragpoint": False},
    ))
    cases.append(TestCase(
        case_id=same_sec_id_b,
        timestamp=ts,
        selected_models=["kitnet", "mdragpoint"],
        verdicts={"kitnet": False, "mdragpoint": True},
    ))

    # Phase 4: Timeout then late completion (uses shorter timeout)
    late_case_id = f"exp-late-complete-{uuid.uuid4().hex[:8]}"
    cases.append(TestCase(
        case_id=late_case_id,
        timestamp=ts,
        selected_models=["hstree", "iforest", "kitnet"],
        verdicts={"kitnet": True},
        timeout_sec=3,
        retention_sec=10,
        inject_delay=0.1,
    ))

    return cases


def get_simple_cases() -> list[TestCase]:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return [
        TestCase(
            case_id=f"exp-simple-kitnet-{uuid.uuid4().hex[:8]}",
            timestamp=ts,
            selected_models=["kitnet"],
            verdicts={"kitnet": True},
        ),
        TestCase(
            case_id=f"exp-simple-hstree-kitnet-both-{uuid.uuid4().hex[:8]}",
            timestamp=ts,
            selected_models=["hstree", "kitnet"],
            verdicts={"hstree": True, "kitnet": True},
        ),
        TestCase(
            case_id=f"exp-simple-hstree-kitnet-one-{uuid.uuid4().hex[:8]}",
            timestamp=ts,
            selected_models=["hstree", "kitnet"],
            verdicts={"hstree": True, "kitnet": False},
        ),
        TestCase(
            case_id=f"exp-simple-kitnet-mdrag-a-{uuid.uuid4().hex[:8]}",
            timestamp=ts,
            selected_models=["kitnet", "mdragpoint"],
            verdicts={"kitnet": True, "mdragpoint": False},
        ),
        TestCase(
            case_id=f"exp-simple-kitnet-mdrag-b-{uuid.uuid4().hex[:8]}",
            timestamp=ts,
            selected_models=["kitnet", "mdragpoint"],
            verdicts={"kitnet": False, "mdragpoint": True},
        ),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate consensus model votes with controlled Kafka messages.")
    parser.add_argument("--suite", choices=("simple", "full"), default="simple")
    parser.add_argument(
        "--no-manage-consensus",
        action="store_true",
        help="Do not restart consensus-analysis between cases; caller must configure SELECTED_MODELS manually.",
    )
    parser.add_argument(
        "--skip-influx",
        action="store_true",
        help="Only validate Kafka consensus output.",
    )
    parser.add_argument(
        "--only",
        default="",
        help="Run only cases whose generated case_id contains this substring.",
    )
    parser.add_argument(
        "--selected-models",
        default="",
        help="Run only cases whose selected model list exactly matches this comma-separated list.",
    )
    parser.add_argument(
        "--report-path",
        default=os.getenv(
            "REPORT_PATH",
            "/tmp/consensus-model-vote-experiment-report.json" if IN_DOCKER else ".tmp/consensus-model-vote-experiment-report.json",
        ),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    report_path = args.report_path
    report_dir = os.path.dirname(report_path)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
    validate_influx = not args.skip_influx
    if validate_influx:
        influx_ok, influx_msg = _validate_influxdb_available()
        if not influx_ok:
            print(f"[warn] {influx_msg}; continuing with Kafka-only validation")
            validate_influx = False

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    results = []
    cases = get_simple_cases() if args.suite == "simple" else get_test_cases()
    if args.only:
        cases = [case for case in cases if args.only in case.case_id]
    if args.selected_models:
        selected = [model.strip() for model in args.selected_models.split(",") if model.strip()]
        cases = [case for case in cases if case.selected_models == selected]
    if not cases:
        print(
            f"No cases matched --only={args.only!r} --selected-models={args.selected_models!r}",
            file=sys.stderr,
        )
        sys.exit(2)
    for case in cases:
        print(f"Running case: {case.case_id}")
        result = _run_case(
            producer,
            case,
            manage_consensus=not args.no_manage_consensus,
            validate_influx=validate_influx,
        )
        results.append(result)
        status = "PASS" if result["passed"] else "FAIL"
        record = result.get("kafka_consensus")
        print(f"  [{status}] anomaly_count={record.get('anomaly_count') if record else 'N/A'}")
        if result["errors"]:
            for err in result["errors"]:
                print(f"    ERROR: {err}")

    total = len(results)
    passed = sum(1 for r in results if r["passed"])
    failed = total - passed

    summary = {
        "start_timestamp": datetime.now().isoformat(),
        "total_cases": total,
        "passed": passed,
        "failed": failed,
        "cases": results,
    }

    with open(report_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\nResults: {passed}/{total} passed")
    if failed > 0:
        print("FAILED cases:")
        for r in results:
            if not r["passed"]:
                print(f"  - {r['case_id']}: {r['errors']}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
