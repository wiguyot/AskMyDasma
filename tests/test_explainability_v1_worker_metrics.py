"""Worker metrics tests for explainability V1 (V1-02).

Three families are covered:

1. The pure ``WorkerMetricsAggregator``: per-group counters, derived
   ``queue_depth``/``in_flight``, the nominal relation
   ``completed`` is the sum of all declared explanation statuses,
   and the explicit rejection policy — duplicate, out-of-order or gapped
   events raise ``ValueError`` (programming error), never silently absorbed.

2. The worker emission contract: with the optional ``metrics_q``, the worker
   emits exactly one ``WorkerStarted`` per item taken (before the identity
   gate) and one ``WorkerCompleted`` after the existing publication attempt
   for every item that reached status attribution (``computed`` and
   ``failed`` alike). Extraction failures and the fatal identity gate stay
   outside the status accounting, and the metrics queue remains optional.

3. The entrypoint wiring: ``enqueued`` is counted only after a successful
   ``put`` and the final bilan uses the aggregated ``computed`` total, not a
   local variable left at zero.

The tests are pure: queues, producers, model and SHAP are mocked; neither
Kafka nor Docker is started.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import queue
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import numpy as np

from dasma_kafka.explainability import runtime as runtime_module
from dasma_kafka.explainability.config import ExplainabilityConfig
from dasma_kafka.explainability.runtime import (
    GroupWorkItem,
    WorkerCompleted,
    WorkerDeliveryAck,
    WorkerMetricsAggregator,
    WorkerStarted,
    drain_worker_metrics,
    group_worker_main,
)


WORKER_CONFIG: Dict[str, Any] = {
    "bootstrap_servers": "kafka:9092",
    "source_topic": "hstree-analysis",
    "explained_model": "hstree",
    "explainability_mode": "reference",
    "explanation_method": "kernel",
    "dest_topic": "explainability",
    "dest_topic_json": "explainability-viz",
    "schema_registry_url": "http://schema-registry:8081",
    "group_id": "explainability-consumer",
    "log_level": "CRITICAL",
    "hstree_window_size": 2048,
    "hstree_num_trees": 25,
    "hstree_max_depth": 15,
    "hstree_initial_window_size": 512,
    "shap_background_size": 100,
    "shap_nsamples": 100,
    "shap_enabled": True,
    "explainability_seed": 42,
}

COMPUTED_SHAP_RESULT = {
    "shap_values": {"A": 0.1, "B": -0.2},
    "shap_base_value": 0.05,
    "shap_computation_time_ms": 1.5,
}


def _record(source_event_id: str = "row-000001") -> Dict[str, Any]:
    return {
        "timestamp": "2026-01-01 00:00:01",
        "Group": "g1",
        "source_event_id": source_event_id,
        "hstree_score": 0.5,
        "hstree_status": "anomaly",
        "hstree_anomaly": True,
        "hstree_threshold": 0.7,
        "A": 1.5,
        "B": 2.5,
    }


def _item(record: Dict[str, Any], offset: int) -> GroupWorkItem:
    return GroupWorkItem(
        key_str=None,
        record=record,
        topic="hstree-analysis",
        partition=0,
        offset=offset,
    )


def _drain_all(metrics_q: Any) -> List[Any]:
    events = []
    while True:
        try:
            events.append(metrics_q.get_nowait())
        except queue.Empty:
            return events


class WorkerMetricsAggregatorTest(unittest.TestCase):
    def test_aggregates_multiple_statuses_for_one_group(self) -> None:
        aggregator = WorkerMetricsAggregator()
        statuses = ["disabled", "warmup", "background_not_ready", "computed", "failed", "model_mismatch"]
        for _ in statuses:
            aggregator.record_enqueued("g1")
        for seq in range(1, len(statuses) + 1):
            aggregator.record_started("g1", seq=seq)
        for seq, status in enumerate(statuses, start=1):
            aggregator.record_completed("g1", seq=seq, status=status)

        snapshot = aggregator.snapshot()["g1"]
        self.assertEqual(snapshot["enqueued"], len(statuses))
        self.assertEqual(snapshot["started"], len(statuses))
        self.assertEqual(snapshot["completed"], len(statuses))
        self.assertEqual(snapshot["queue_depth"], 0)
        self.assertEqual(snapshot["in_flight"], 0)
        self.assertEqual(
            snapshot["status_counts"],
            {
                "disabled": 1,
                "warmup": 1,
                "background_not_ready": 1,
                "computed": 1,
                "failed": 1,
                "model_mismatch": 1,
            },
        )
        # Nominal relation: completed is the sum of the five statuses.
        self.assertEqual(
            snapshot["completed"], sum(snapshot["status_counts"].values())
        )

    def test_two_groups_are_aggregated_without_mixing(self) -> None:
        aggregator = WorkerMetricsAggregator()
        for _ in range(3):
            aggregator.record_enqueued("A")
        aggregator.record_started("A", seq=1)
        aggregator.record_started("A", seq=2)
        aggregator.record_started("A", seq=3)
        aggregator.record_completed("A", seq=1, status="computed")
        aggregator.record_completed("A", seq=2, status="computed")
        aggregator.record_completed("A", seq=3, status="warmup")

        for _ in range(2):
            aggregator.record_enqueued("B")
        aggregator.record_started("B", seq=1)
        aggregator.record_completed("B", seq=1, status="failed")

        snapshot = aggregator.snapshot()
        self.assertEqual(snapshot["A"]["completed"], 3)
        self.assertEqual(
            snapshot["A"]["status_counts"], {"computed": 2, "warmup": 1}
        )
        self.assertEqual(snapshot["B"]["completed"], 1)
        self.assertEqual(snapshot["B"]["status_counts"], {"failed": 1})
        self.assertEqual(snapshot["B"]["status_counts"].get("computed", 0), 0)
        self.assertEqual(aggregator.total_completed_for_status("computed"), 2)
        self.assertEqual(aggregator.total_completed_for_status("failed"), 1)

    def test_queue_depth_and_in_flight_at_each_step(self) -> None:
        aggregator = WorkerMetricsAggregator()
        for _ in range(3):
            aggregator.record_enqueued("g1")
        snapshot = aggregator.snapshot()["g1"]
        self.assertEqual(snapshot["queue_depth"], 3)
        self.assertEqual(snapshot["in_flight"], 0)

        aggregator.record_started("g1", seq=1)
        snapshot = aggregator.snapshot()["g1"]
        self.assertEqual(snapshot["queue_depth"], 2)
        self.assertEqual(snapshot["in_flight"], 1)

        aggregator.record_completed("g1", seq=1, status="computed")
        snapshot = aggregator.snapshot()["g1"]
        self.assertEqual(snapshot["queue_depth"], 2)
        self.assertEqual(snapshot["in_flight"], 0)

        aggregator.record_started("g1", seq=2)
        aggregator.record_started("g1", seq=3)
        snapshot = aggregator.snapshot()["g1"]
        self.assertEqual(snapshot["queue_depth"], 0)
        self.assertEqual(snapshot["in_flight"], 2)
        self.assertGreaterEqual(snapshot["queue_depth"], 0)
        self.assertGreaterEqual(snapshot["in_flight"], 0)

        aggregator.record_completed("g1", seq=2, status="failed")
        snapshot = aggregator.snapshot()["g1"]
        self.assertEqual(snapshot["in_flight"], 1)

    def test_duplicate_started_is_rejected_as_programming_error(self) -> None:
        aggregator = WorkerMetricsAggregator()
        aggregator.record_enqueued("g1")
        aggregator.record_enqueued("g1")
        aggregator.record_started("g1", seq=1)

        with self.assertRaises(ValueError):
            aggregator.record_started("g1", seq=1)

    def test_out_of_order_started_is_rejected(self) -> None:
        aggregator = WorkerMetricsAggregator()
        for _ in range(3):
            aggregator.record_enqueued("g1")
        aggregator.record_started("g1", seq=1)
        aggregator.record_started("g1", seq=2)

        # A replay of an older seq (out-of-order arrival) is rejected.
        with self.assertRaises(ValueError):
            aggregator.record_started("g1", seq=1)

        # The stream continues densely after the rejection.
        aggregator.record_started("g1", seq=3)
        self.assertEqual(aggregator.snapshot()["g1"]["started"], 3)

    def test_started_sequence_gap_is_rejected(self) -> None:
        aggregator = WorkerMetricsAggregator()
        for _ in range(3):
            aggregator.record_enqueued("g1")
        aggregator.record_started("g1", seq=1)

        # A gap (seq=3 while seq=2 was never seen) is a lost or corrupted
        # event: rejected, not absorbed.
        with self.assertRaises(ValueError):
            aggregator.record_started("g1", seq=3)

        # The aggregator state is unchanged: the expected seq is still 2.
        aggregator.record_started("g1", seq=2)
        self.assertEqual(aggregator.snapshot()["g1"]["started"], 2)

    def test_duplicate_completed_is_rejected_as_programming_error(self) -> None:
        aggregator = WorkerMetricsAggregator()
        aggregator.record_enqueued("g1")
        aggregator.record_enqueued("g1")
        aggregator.record_started("g1", seq=1)
        aggregator.record_completed("g1", seq=1, status="computed")

        with self.assertRaises(ValueError):
            aggregator.record_completed("g1", seq=1, status="computed")

        # Out-of-order completions are rejected as well.
        aggregator.record_started("g1", seq=2)
        with self.assertRaises(ValueError):
            aggregator.record_completed("g1", seq=0, status="failed")

    def test_completed_for_an_item_never_started_is_rejected(self) -> None:
        aggregator = WorkerMetricsAggregator()
        aggregator.record_enqueued("g1")
        aggregator.record_enqueued("g1")
        aggregator.record_started("g1", seq=1)

        # seq=2 was never started: no completion can be accepted for it.
        with self.assertRaises(ValueError):
            aggregator.record_completed("g1", seq=2, status="computed")

        # The aggregator state is unchanged.
        self.assertEqual(aggregator.snapshot()["g1"]["completed"], 0)

    def test_completed_gap_from_a_skipped_item_is_tolerated(self) -> None:
        # Documented policy: an item started then abandoned before status
        # attribution (feature extraction failure) never emits a completed
        # event, so completed sequences may have gaps. The gap must not crash
        # the aggregation; in_flight keeps tracking the skipped item.
        aggregator = WorkerMetricsAggregator()
        for _ in range(3):
            aggregator.record_enqueued("g1")
        aggregator.record_started("g1", seq=1)
        aggregator.record_started("g1", seq=2)
        aggregator.record_started("g1", seq=3)

        aggregator.record_completed("g1", seq=1, status="computed")
        # seq=2 was skipped by the worker (no completed event for it).
        aggregator.record_completed("g1", seq=3, status="computed")

        snapshot = aggregator.snapshot()["g1"]
        self.assertEqual(snapshot["started"], 3)
        self.assertEqual(snapshot["completed"], 2)
        self.assertEqual(snapshot["in_flight"], 1)
        self.assertEqual(snapshot["status_counts"], {"computed": 2})

    def test_started_without_enqueued_violates_the_invariant(self) -> None:
        aggregator = WorkerMetricsAggregator()
        with self.assertRaises(ValueError):
            aggregator.record_started("g1", seq=1)

    def test_completed_without_started_violates_the_invariant(self) -> None:
        aggregator = WorkerMetricsAggregator()
        aggregator.record_enqueued("g1")
        with self.assertRaises(ValueError):
            aggregator.record_completed("g1", seq=1, status="computed")

    def test_unknown_status_is_rejected(self) -> None:
        aggregator = WorkerMetricsAggregator()
        aggregator.record_enqueued("g1")
        aggregator.record_started("g1", seq=1)

        with self.assertRaises(ValueError):
            aggregator.record_completed("g1", seq=1, status="bogus")

    def test_total_global_computed_across_groups(self) -> None:
        aggregator = WorkerMetricsAggregator()
        for group, computed in (("A", 2), ("B", 1)):
            for seq in range(1, computed + 1):
                aggregator.record_enqueued(group)
                aggregator.record_started(group, seq=seq)
                aggregator.record_completed(group, seq=seq, status="computed")

        self.assertEqual(aggregator.total_completed_for_status("computed"), 3)
        self.assertEqual(aggregator.total_completed_for_status("failed"), 0)

    def test_drain_ignores_unknown_events(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        metrics_q.put("not-an-event")
        aggregator = WorkerMetricsAggregator()
        drain_worker_metrics(metrics_q, aggregator, MagicMock())

        self.assertEqual(aggregator.snapshot(), {})

    def test_drain_returns_on_empty_queue(self) -> None:
        aggregator = WorkerMetricsAggregator()
        drain_worker_metrics(queue.Queue(), aggregator, MagicMock())
        self.assertEqual(aggregator.snapshot(), {})


class WorkerMetricsEmissionTest(unittest.TestCase):
    def _run_worker(
        self,
        records: List[Dict[str, Any]],
        metrics_q: Any = "__default__",
        *,
        extract_error: Optional[BaseException] = None,
        shap_result: Optional[Dict[str, Any]] = COMPUTED_SHAP_RESULT,
        shap_side_effect: Optional[BaseException] = None,
        config_overrides: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[BaseException], Any, MagicMock]:
        """Run the worker; ``"__default__"`` exercises the optional path."""
        metrics_queue = None if metrics_q == "__default__" else metrics_q

        model_manager = MagicMock(name="model-manager")
        model_manager.reference_score_field = "hstree_score"
        model_manager.mismatch_code = "hstree_score_mismatch"
        model_manager.fit_score.return_value = 0.5
        model_manager.get_background_data.return_value = np.zeros((100, 2))
        model_manager.get_model.return_value = MagicMock(name="hstree-model")

        shap_explainer = MagicMock(name="shap-explainer")
        if shap_side_effect is not None:
            shap_explainer.compute_shap_values.side_effect = shap_side_effect
        else:
            shap_explainer.compute_shap_values.return_value = shap_result

        feature_extractor = MagicMock(name="feature-extractor")
        if extract_error is not None:
            feature_extractor.extract.side_effect = extract_error
        else:
            feature_extractor.extract.return_value = np.array([1.5, 2.5])

        # V1-03: the worker waits for the delivery callbacks; the mocks fire
        # them immediately so the run stays instantaneous.
        def _firing_producer():
            producer = MagicMock(name="serializing-producer")

            def _fire(topic, key=None, value=None, on_delivery=None, **kwargs):
                if on_delivery is not None:
                    on_delivery(None, MagicMock())
                return 0

            producer.produce.side_effect = _fire
            return producer

        avro_producer = _firing_producer()
        json_producer = _firing_producer()

        work_q: queue.Queue = queue.Queue()
        for offset, record in enumerate(records):
            work_q.put(_item(record, offset))
        work_q.put(None)

        error: Optional[BaseException] = None
        worker_config = dict(WORKER_CONFIG)
        if config_overrides:
            worker_config.update(config_overrides)
        with patch.object(
            runtime_module, "create_schema_registry_client", return_value=MagicMock()
        ), patch.object(
            runtime_module, "AvroSerializer", MagicMock()
        ), patch.object(
            runtime_module,
            "SerializingProducer",
            side_effect=[avro_producer, json_producer],
        ), patch.object(
            runtime_module, "HSTreeModelManager", return_value=model_manager
        ), patch.object(
            runtime_module, "SHAPExplainer", return_value=shap_explainer
        ), patch.object(
            runtime_module, "FeatureExtractor", return_value=feature_extractor
        ), patch(
            "builtins.print", side_effect=lambda *a, **k: None
        ):
            try:
                group_worker_main(
                    "g1", worker_config, ["A", "B"], work_q, metrics_queue
                )
            except Exception as exc:  # noqa: BLE001 - asserted by the caller
                error = exc

        return error, metrics_q, avro_producer

    def test_worker_emits_exactly_one_started_and_one_completed_per_message(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        error, _, _ = self._run_worker([_record()], metrics_q=metrics_q)

        self.assertIsNone(error)
        events = _drain_all(metrics_q)
        self.assertEqual(len(events), 2)
        started, completed = events
        self.assertIsInstance(started, WorkerStarted)
        self.assertEqual(started.group, "g1")
        self.assertEqual(started.seq, 1)
        self.assertIsInstance(completed, WorkerCompleted)
        self.assertEqual(completed.seq, 1)
        self.assertEqual(completed.status, "computed")
        # started strictly before completed, same identity.
        self.assertEqual((started.group, started.seq), (completed.group, completed.seq))

    def test_started_sequences_are_dense_across_items(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        error, _, _ = self._run_worker(
            [_record("row-000001"), _record("row-000002"), _record("row-000003")],
            metrics_q=metrics_q,
        )

        self.assertIsNone(error)
        events = _drain_all(metrics_q)
        seqs = [event.seq for event in events if isinstance(event, WorkerStarted)]
        self.assertEqual(seqs, [1, 2, 3])
        completed = [event for event in events if isinstance(event, WorkerCompleted)]
        self.assertEqual([event.seq for event in completed], [1, 2, 3])

    def test_failed_status_emits_completed_after_publication(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        failed_result = {
            "shap_values": None,
            "shap_base_value": None,
            "shap_computation_time_ms": None,
        }
        error, _, avro_producer = self._run_worker(
            [_record()], metrics_q=metrics_q, shap_result=failed_result
        )

        self.assertIsNone(error)
        events = _drain_all(metrics_q)
        self.assertEqual(len(events), 2)
        started, completed = events
        self.assertIsInstance(started, WorkerStarted)
        self.assertEqual(completed.status, "failed")
        # The publication attempt still happened for the failed status: the
        # completed event follows it.
        self.assertEqual(avro_producer.produce.call_count, 1)

    def test_extraction_failure_emits_started_without_completed(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        error, _, _ = self._run_worker(
            [_record()], metrics_q=metrics_q, extract_error=ValueError("bad features")
        )

        self.assertIsNone(error)
        events = _drain_all(metrics_q)
        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], WorkerStarted)
        self.assertEqual(events[0].seq, 1)

    def test_identity_gate_stops_before_any_completed(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        record = _record()
        del record["source_event_id"]
        error, _, avro_producer = self._run_worker([record], metrics_q=metrics_q)

        self.assertIsInstance(error, ValueError)
        events = _drain_all(metrics_q)
        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], WorkerStarted)
        self.assertEqual(events[0].seq, 1)
        # Nothing was published: the gate fires before the business processing.
        self.assertEqual(avro_producer.produce.call_count, 0)

    def test_metrics_queue_is_optional(self) -> None:
        # No metrics queue passed: the worker runs without emitting anything
        # and without error (preserves the V1-01 call shape).
        error, _, _ = self._run_worker([_record()], metrics_q=None)
        self.assertIsNone(error)

    def test_metrics_emission_failure_is_logged_without_stopping_the_worker(self) -> None:
        class FailingMetricsQueue:
            def put_nowait(self, event: Any) -> None:
                raise RuntimeError("metrics transport unavailable")

        with self.assertLogs("explainability.worker.g1", level="ERROR") as logs:
            error, _, avro_producer = self._run_worker(
                [_record()],
                metrics_q=FailingMetricsQueue(),
                config_overrides={"log_level": "ERROR"},
            )

        self.assertIsNone(error)
        self.assertEqual(avro_producer.produce.call_count, 1)
        self.assertTrue(
            any("Unable to emit metrics event" in message for message in logs.output)
        )

    def test_drain_feeds_the_aggregator_from_the_real_worker(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        error, _, _ = self._run_worker(
            [_record("row-000001"), _record("row-000002")], metrics_q=metrics_q
        )
        self.assertIsNone(error)

        aggregator = WorkerMetricsAggregator()
        # The entrypoint enqueued both items.
        for _ in range(2):
            aggregator.record_enqueued("g1")
        drain_worker_metrics(metrics_q, aggregator, MagicMock())

        snapshot = aggregator.snapshot()["g1"]
        self.assertEqual(snapshot["started"], 2)
        self.assertEqual(snapshot["completed"], 2)
        self.assertEqual(snapshot["queue_depth"], 0)
        self.assertEqual(snapshot["in_flight"], 0)
        self.assertEqual(aggregator.total_completed_for_status("computed"), 2)


class _CapturingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class _FakeWorkQueue:
    """Work queue refusing the first non-sentinel put, then delegating."""

    def __init__(self) -> None:
        self._real: queue.Queue = queue.Queue()
        self._full_once = True
        self.put_attempts = 0

    def put(self, item: Any, timeout: Optional[float] = None) -> None:
        self.put_attempts += 1
        if self._full_once and item is not None:
            self._full_once = False
            raise queue.Full()
        self._real.put(item, timeout=0)

    def get(self, timeout: Optional[float] = None) -> Any:
        return self._real.get(timeout=timeout)


class _FakeProcess:
    def __init__(self, target: Any, args: Tuple[Any, ...], **kwargs: Any) -> None:
        self.target = target
        self.args = args
        self.exitcode = 0
        self.daemon = None
        self._joined = False

    def start(self) -> None:
        # The real worker consumes its queue asynchronously; the fake emits
        # its metrics and its delivery ack right away and lets the main loop
        # drain them (V1-03: the ack unblocks the contiguous commit).
        group_name, _worker_config, _features, _work_q, metrics_q, delivery_q = self.args
        metrics_q.put(WorkerStarted(group=group_name, seq=1))
        metrics_q.put(WorkerCompleted(group=group_name, seq=1, status="computed"))
        delivery_q.put(
            WorkerDeliveryAck(
                topic="hstree-analysis", partition=0, offset=0, success=True
            )
        )

    def join(self, timeout: Optional[float] = None) -> None:
        self._joined = True

    def is_alive(self) -> bool:
        # Alive during consumption (V1-03 supervision); a clean exit is only
        # observed after the main process joined it.
        return not self._joined


class EntrypointMetricsTest(unittest.TestCase):
    """The V1 entrypoint wiring, without Kafka, Docker or real workers."""

    def _load_entrypoint(self) -> Any:
        entrypoint = (
            Path(__file__).resolve().parents[1]
            / "runtime"
            / "explainability"
            / "regular"
            / "explainability-multi-thread-by-group.py"
        )
        spec = importlib.util.spec_from_file_location(
            "explainability_regular_entrypoint_v102", entrypoint
        )
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module

    def test_enqueued_counted_once_and_bilan_uses_aggregated_total(self) -> None:
        mod = self._load_entrypoint()

        handler = _CapturingHandler()
        logger = logging.getLogger("v102-entrypoint-test")
        logger.setLevel(logging.INFO)
        logger.handlers = [handler]
        logger.propagate = False

        record = {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "A",
            "source_event_id": "row-000001",
            "A": 1.5,
            "B": 2.5,
        }

        message = MagicMock(name="kafka-message")
        message.error.return_value = None
        message.key.return_value = None
        message.value.return_value = b"avro-bytes"
        message.topic.return_value = "hstree-analysis"
        message.partition.return_value = 0
        message.offset.return_value = 0

        consumer = MagicMock(name="consumer")
        consumer.poll.side_effect = [message, KeyboardInterrupt()]

        fake_config = ExplainabilityConfig(
            bootstrap_servers="kafka:9092",
            schema_registry_url="http://schema-registry:8081",
            group_id="explainability-consumer",
            dest_topic_json="explainability-viz",
            source_topic="hstree-analysis",
            explained_model="hstree",
            explainability_mode="reference",
            explanation_method="kernel",
        )
        fake_config_cls = SimpleNamespace(from_environment=lambda: fake_config)

        fake_mp = SimpleNamespace(
            set_start_method=lambda *a, **k: None,
            Queue=lambda maxsize=0: _FakeWorkQueue() if maxsize else queue.Queue(),
            Process=lambda target, args, **kw: _FakeProcess(target, args, **kw),
        )

        with patch.object(mod, "ExplainabilityConfig", fake_config_cls), patch.object(
            mod, "mp", fake_mp
        ), patch.object(
            mod, "make_logger", lambda: logger
        ), patch.object(
            mod,
            "create_schema_registry_client",
            lambda *a, **k: MagicMock(name="sr-client"),
        ), patch.object(
            mod, "create_consumer", lambda *a, **k: consumer
        ), patch.object(
            mod, "AvroDeserializer", lambda **kw: (lambda value, ctx: dict(record))
        ), patch.object(
            mod, "FeatureExtractor", lambda *a, **k: MagicMock(
                is_initialized=True,
                initialize_from_record=MagicMock(),
                get_feature_names=lambda: ["A", "B"],
            )
        ):
            mod.main()

        # enqueued is counted once: the refused put (Full) is not counted.
        group_lines = [m for m in handler.messages if "Group 'A'" in m]
        self.assertEqual(len(group_lines), 1)
        self.assertIn("enqueued=1", group_lines[0])
        self.assertIn("started=1", group_lines[0])
        self.assertIn("completed=1", group_lines[0])
        self.assertIn("queue_depth=0", group_lines[0])
        self.assertIn("in_flight=0", group_lines[0])
        self.assertIn("statuses={'computed': 1}", group_lines[0])

        # The bilan reports the aggregated computed total (1), not the
        # historical counter left at zero.
        bilan_lines = [
            m for m in handler.messages if "Computed SHAP explanations:" in m
        ]
        self.assertEqual(len(bilan_lines), 1)
        self.assertIn("Computed SHAP explanations: 1", bilan_lines[0])


if __name__ == "__main__":
    unittest.main()
