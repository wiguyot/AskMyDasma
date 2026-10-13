"""Contract tests for Q3's opt-in JSONL side-channel trace."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from dasma_kafka.explainability.q3_trace import Q3TraceRecorder, initialization_fields


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "experiments/explainability/v1_v2/q3_partition_trace.schema.json"
RUNNER_PATH = ROOT / "scripts/integration/run-explainability-v1-v2-parity.py"


def _check_schema_contract(record: dict, schema: dict) -> None:
    """Check the schema's record discriminator, required fields and key allowlist."""
    assert record["schema_version"] == schema["properties"]["schema_version"]["const"]
    assert record["service"] in schema["properties"]["service"]["enum"]
    branch = next(
        branch for branch in schema["oneOf"]
        if branch["properties"]["record_type"]["const"] == record["record_type"]
    )
    assert set(schema["required"]) | set(branch["required"]) <= record.keys()
    assert record.keys() <= schema["properties"].keys()
    if record["record_type"] == "message_lifecycle":
        assert record["consumer"].keys() == schema["properties"]["consumer"]["properties"].keys()
        assert record["worker"].keys() == schema["properties"]["worker"]["properties"].keys()
        assert all(
            output.keys() == schema["properties"]["published_outputs"]["items"]["properties"].keys()
            for output in record["published_outputs"]
        )
    elif record["record_type"] == "offset_commit":
        assert record["commit_outcome"] in schema["properties"]["commit_outcome"]["enum"]
        assert all(
            offset.keys() == schema["properties"]["offsets"]["items"]["properties"].keys()
            for offset in record["offsets"]
        )
    else:
        assert record["numpy_state"]["algorithm"] == "MT19937"
        assert len(record["numpy_state"]["keys"]) == 624
        assert len(record["warmup_values_sha256"]) == 64


def test_trace_is_strictly_opt_in(tmp_path, monkeypatch):
    monkeypatch.delenv("DASMA_Q3_TRACE_DIR", raising=False)
    recorder = Q3TraceRecorder("explainability-v1")
    recorder.message_lifecycle(
        topic="input", partition=0, offset=4, group="G", source_event_id="event-4",
        receive_seq=1, dispatch_seq=1, worker_id="worker", group_seq=1,
        start_seq=1, complete_seq=1, published_outputs=[], output_status="warmup",
    )
    recorder.close()
    assert list(tmp_path.iterdir()) == []


def test_trace_emits_all_contract_record_types_without_advancing_rng(tmp_path):
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    before = np.random.get_state()
    recorder = Q3TraceRecorder("explainability-v1", str(tmp_path))
    recorder.message_lifecycle(
        topic="input", partition=1, offset=9, group="G", source_event_id="event-9",
        receive_seq=3, dispatch_seq=2, worker_id="worker-1", group_seq=2,
        start_seq=2, complete_seq=2,
        published_outputs=[{"topic": "output", "process_instance": "worker-1", "publish_seq": 1}],
        output_status="computed",
    )
    recorder.message_lifecycle(
        topic="input", partition=1, offset=10, group="G", source_event_id="event-10",
        receive_seq=4, dispatch_seq=3, worker_id="worker-1", group_seq=3,
        start_seq=3, complete_seq=3, published_outputs=[], output_status="warmup",
        consumer_process_instance="explainability-v1:main",
    )
    recorder.offset_commit("input", 1, 10, "requested")
    state = initialization_fields(
        topic="input", partition=1, offset=9, group="G", source_event_id="event-9",
        initialization_order_seq=1, warmup_event_ids=["event-1", "event-9"],
        warmup_values=[[1.0, 2.0], [2.0, 3.0]], feature_names=["a", "b"],
        feature_mins=np.array([1.0, 2.0]), feature_maxes=np.array([2.0, 3.0]),
        hstree_parameters={"initial_window_size": 2, "window_size": 20, "num_trees": 3,
                           "max_depth": 4, "configured_seed": 42}, pysad_version="test",
    )
    recorder.model_initialization(**state)
    after = np.random.get_state()
    assert before[0] == after[0]
    assert np.array_equal(before[1], after[1])
    assert before[2:] == after[2:]
    recorder.close()

    path, = tmp_path.glob("explainability-v1-*.jsonl")
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [record["record_type"] for record in records] == [
        "message_lifecycle", "message_lifecycle", "offset_commit", "model_initialization"
    ]
    assert [record["record_seq"] for record in records] == [1, 2, 3, 4]
    for record in records:
        _check_schema_contract(record, schema)
    lifecycle = records[0]
    assert (lifecycle["source_partition"], lifecycle["source_offset"], lifecycle["source_event_id"]) == (
        1, 9, "event-9"
    )
    assert lifecycle["worker"]["group_seq"] == 2
    lifecycles = [record for record in records if record["record_type"] == "message_lifecycle"]
    assert [record["consumer"]["receive_seq"] for record in lifecycles] == [3, 4]
    assert [record["consumer"]["dispatch_seq"] for record in lifecycles] == [2, 3]
    assert [record["worker"]["group_seq"] for record in lifecycles] == [2, 3]
    assert [record["source_event_id"] for record in lifecycles] == ["event-9", "event-10"]
    assert lifecycles[1]["consumer"]["process_instance"] == "explainability-v1:main"


def test_campaign_runner_keeps_trace_opt_in_and_writes_a_run_manifest():
    runner = RUNNER_PATH.read_text(encoding="utf-8")
    base_compose = (ROOT / "docker/compose/experiments/docker-compose.v1v2.yml").read_text(encoding="utf-8")
    trace_override = (ROOT / "docker/compose/experiments/docker-compose.q3-trace.yml").read_text(encoding="utf-8")
    assert '"--q3-trace"' in runner
    assert '"q3-run-manifest.json"' in runner
    assert '"runtime_source_sha256"' in runner
    assert '"V1V2_Q3_TRACE_CONTAINER_DIR"' in runner
    assert '"${V1V2_Q3_TRACE_CONTAINER_DIR:-}"' in base_compose
    assert "DASMA_Q3_TRACE_DIR" in base_compose
    assert "DASMA_Q3_TRACE_DIR" not in trace_override
    assert "${V1V2_Q3_TRACE_HOST_DIR}" in trace_override
