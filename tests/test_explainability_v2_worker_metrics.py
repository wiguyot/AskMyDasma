"""Worker metrics aggregation tests for explainability-v2 (OC-04).

Two families are covered:

1. The pure ``WorkerMetricsAggregator``: per-group counters, derived
   ``queue_depth``/``in_flight``, the nominal relation
   ``completed`` is the sum of all declared explanation statuses,
   and the explicit duplicate policy — a duplicate or out-of-order event is
   rejected with ``ValueError`` (programming error), never silently absorbed.

2. The worker emission contract: with an optional metrics queue, the worker
   emits exactly one ``WorkerStarted`` per item taken and one
   ``WorkerCompleted`` per item that reached status attribution. Extraction
   failures stay outside the status accounting (started without completed),
   and the metrics queue remains optional.
"""

from __future__ import annotations

import queue
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import numpy as np

from dasma_kafka.explainability_v2 import runtime as runtime_module
from dasma_kafka.explainability_v2.runtime import (
    GroupWorkItem,
    WorkerCompleted,
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
    "dest_topic": "explainability-v2",
    "dest_topic_json": "explainability-v2-viz",
    "schema_registry_url": "http://schema-registry:8081",
    "group_id": "explainability-v2-consumer",
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


class WorkerMetricsAggregatorTest(unittest.TestCase):
    def test_aggregates_multiple_statuses_for_one_group(self) -> None:
        aggregator = WorkerMetricsAggregator()
        statuses = ["disabled", "warmup", "background_not_ready", "computed", "failed", "model_mismatch"]
        for seq in range(1, len(statuses) + 1):
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
        aggregator.record_completed("A", seq=1, status="computed")
        aggregator.record_completed("A", seq=2, status="computed")

        for _ in range(2):
            aggregator.record_enqueued("B")
        aggregator.record_started("B", seq=1)
        aggregator.record_completed("B", seq=1, status="failed")

        snapshot = aggregator.snapshot()
        self.assertEqual(snapshot["A"]["completed"], 2)
        self.assertEqual(snapshot["A"]["status_counts"], {"computed": 2})
        self.assertEqual(snapshot["B"]["completed"], 1)
        self.assertEqual(snapshot["B"]["status_counts"], {"failed": 1})
        self.assertEqual(snapshot["B"]["status_counts"].get("computed", 0), 0)
        self.assertEqual(aggregator.total_completed_for_status("computed"), 2)

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

        aggregator.record_completed("g1", seq=2, status="computed")
        snapshot = aggregator.snapshot()["g1"]
        self.assertEqual(snapshot["in_flight"], 1)

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

    def test_duplicate_started_is_rejected_as_programming_error(self) -> None:
        aggregator = WorkerMetricsAggregator()
        aggregator.record_enqueued("g1")
        aggregator.record_enqueued("g1")
        aggregator.record_started("g1", seq=1)

        with self.assertRaises(ValueError):
            aggregator.record_started("g1", seq=1)

    def test_started_sequence_gap_is_rejected(self) -> None:
        aggregator = WorkerMetricsAggregator()
        aggregator.record_enqueued("g1")
        aggregator.record_enqueued("g1")
        aggregator.record_enqueued("g1")
        aggregator.record_started("g1", seq=1)

        # A gap (seq=3 while seq=2 was never seen) is a lost or corrupted
        # event: rejected, not absorbed.
        with self.assertRaises(ValueError):
            aggregator.record_started("g1", seq=3)

        # The aggregator state is unchanged: the expected seq is still 2.
        aggregator.record_started("g1", seq=2)
        self.assertEqual(aggregator.snapshot()["g1"]["started"], 2)

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
        # Documented policy: an item started then skipped before status
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


class WorkerMetricsEmissionTest(unittest.TestCase):
    def _run_worker(
        self,
        records: List[Dict[str, Any]],
        metrics_q: Any = "__default__",
        *,
        extract_error: Optional[BaseException] = None,
    ) -> Any:
        """Run the worker; ``"__default__"`` exercises the optional path."""
        metrics_queue = None if metrics_q == "__default__" else metrics_q

        model_manager = MagicMock(name="model-manager")
        model_manager.reference_score_field = "hstree_score"
        model_manager.mismatch_code = "hstree_score_mismatch"
        model_manager.fit_score.return_value = 0.5
        model_manager.get_background_data.return_value = np.zeros((100, 2))
        model_manager.get_model.return_value = MagicMock(name="hstree-model")

        shap_explainer = MagicMock(name="shap-explainer")
        shap_explainer.compute_shap_values.return_value = {
            "shap_values": {"A": 0.1, "B": -0.2},
            "shap_base_value": 0.05,
            "shap_computation_time_ms": 1.0,
        }

        feature_extractor = MagicMock(name="feature-extractor")
        if extract_error is not None:
            feature_extractor.extract.side_effect = extract_error
        else:
            feature_extractor.extract.return_value = np.array([1.5, 2.5])

        def _fire_delivery(topic, key=None, value=None, on_delivery=None, **kwargs):
            # OC-07 mocks: fire the delivery callback immediately so the
            # worker never waits for the bounded delivery timeout.
            if on_delivery is not None:
                on_delivery(None, MagicMock())
            return 0

        def _firing_producer():
            producer = MagicMock(name="serializing-producer")
            producer.produce.side_effect = _fire_delivery
            return producer

        work_q: queue.Queue = queue.Queue()
        for offset, record in enumerate(records):
            work_q.put(_item(record, offset))
        work_q.put(None)

        with patch.object(
            runtime_module, "create_schema_registry_client", return_value=MagicMock()
        ), patch.object(
            runtime_module, "AvroSerializer", MagicMock()
        ), patch.object(
            runtime_module,
            "SerializingProducer",
            side_effect=[_firing_producer(), _firing_producer()],
        ), patch.object(
            runtime_module, "HSTreeModelManager", return_value=model_manager
        ), patch.object(
            runtime_module, "SHAPExplainer", return_value=shap_explainer
        ), patch.object(
            runtime_module, "FeatureExtractor", return_value=feature_extractor
        ):
            group_worker_main("g1", dict(WORKER_CONFIG), ["A", "B"], work_q, metrics_queue)

        return metrics_q

    def test_worker_emits_exactly_one_started_and_one_completed_per_message(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        self._run_worker([_record()], metrics_q=metrics_q)

        events = []
        while True:
            try:
                events.append(metrics_q.get_nowait())
            except queue.Empty:
                break

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

    def test_drain_feeds_the_aggregator_from_the_real_worker(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record("row-000001"), _record("row-000002")], metrics_q=metrics_q
        )
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

    def test_extraction_failure_emits_started_without_completed(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record()], metrics_q=metrics_q, extract_error=ValueError("bad features")
        )

        events = []
        while True:
            try:
                events.append(metrics_q.get_nowait())
            except queue.Empty:
                break

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], WorkerStarted)
        self.assertEqual(events[0].seq, 1)

    def test_metrics_queue_is_optional(self) -> None:
        # No metrics queue passed: the worker runs without emitting anything
        # and without error (preserves the OC-02 call shape).
        self._run_worker([_record()], metrics_q=None)

    def test_drain_ignores_unknown_events(self) -> None:
        metrics_q: queue.Queue = queue.Queue()
        metrics_q.put("not-an-event")
        aggregator = WorkerMetricsAggregator()
        drain_worker_metrics(metrics_q, aggregator, MagicMock())

        self.assertEqual(aggregator.snapshot(), {})


def _item(record: Dict[str, Any], offset: int) -> GroupWorkItem:
    return GroupWorkItem(
        key_str=None,
        record=record,
        topic="hstree-analysis",
        partition=0,
        offset=offset,
    )


if __name__ == "__main__":
    unittest.main()
