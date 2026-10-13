"""OC-08 backpressure primitives for explainability-v2.

The broker-facing pause/resume calls stay in the entrypoint.  These tests lock
the part that must be deterministic without Kafka: once a prefetched record is
accepted into the deferred buffer, it is retained FIFO per partition until a
worker can accept it.
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from queue import Queue
from unittest.mock import MagicMock, patch

from dasma_kafka.explainability_v2.runtime import (
    DeferredDispatchBuffer,
    DeferredDispatchError,
    DeliveryFailureError,
    GroupWorkItem,
    WorkerDeliveryAck,
    ensure_workers_alive,
    incomplete_offset_partitions,
)
from dasma_kafka.explainability_v2.offset_tracker import ContiguousOffsetTracker


ROOT = Path(__file__).resolve().parents[1]
ENTRY_PATH = ROOT / "runtime" / "explainability" / "v2" / "explainability_v2.py"


def _load_entry_module():
    spec = importlib.util.spec_from_file_location("explainability_v2_oc08_entry", ENTRY_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _item(partition: int, offset: int) -> GroupWorkItem:
    return GroupWorkItem(
        key_str=None,
        record={},
        topic="hstree-analysis",
        partition=partition,
        offset=offset,
    )


class DeferredDispatchBufferTest(unittest.TestCase):
    def test_retains_records_fifo_until_they_are_admitted(self) -> None:
        buffer = DeferredDispatchBuffer(per_partition_limit=3)
        buffer.add("A", _item(0, 10))
        buffer.add("B", _item(0, 11))

        self.assertEqual(buffer.snapshot(), {0: [10, 11]})
        first = buffer.pop(0)
        self.assertEqual((first.group, first.work_item.offset), ("A", 10))
        self.assertEqual(buffer.snapshot(), {0: [11]})
        second = buffer.pop(0)
        self.assertEqual((second.group, second.work_item.offset), ("B", 11))
        self.assertTrue(buffer.is_empty())

    def test_partitions_are_independent_but_each_partition_is_fifo(self) -> None:
        buffer = DeferredDispatchBuffer(per_partition_limit=3)
        buffer.add("A", _item(1, 20))
        buffer.add("B", _item(0, 7))
        buffer.add("C", _item(1, 21))

        self.assertEqual(buffer.partitions(), [0, 1])
        self.assertEqual(buffer.snapshot(), {0: [7], 1: [20, 21]})
        self.assertEqual(buffer.pop(1).work_item.offset, 20)
        self.assertEqual(buffer.peek(0).work_item.offset, 7)

    def test_rejects_gap_without_losing_already_retained_record(self) -> None:
        buffer = DeferredDispatchBuffer(per_partition_limit=3)
        buffer.add("A", _item(0, 10))

        with self.assertRaises(DeferredDispatchError):
            buffer.add("A", _item(0, 12))

        self.assertEqual(buffer.snapshot(), {0: [10]})

    def test_rejects_overflow_without_losing_already_retained_records(self) -> None:
        buffer = DeferredDispatchBuffer(per_partition_limit=2)
        buffer.add("A", _item(0, 10))
        buffer.add("A", _item(0, 11))

        with self.assertRaises(DeferredDispatchError):
            buffer.add("A", _item(0, 12))

        self.assertEqual(buffer.snapshot(), {0: [10, 11]})

    def test_rejects_invalid_limit(self) -> None:
        for limit in (0, -1, True, False, "2"):
            with self.subTest(limit=limit):
                with self.assertRaises(ValueError):
                    DeferredDispatchBuffer(limit)


class _Process:
    def __init__(self, *, alive: bool, exitcode: int | None) -> None:
        self._alive = alive
        self.exitcode = exitcode

    def is_alive(self) -> bool:
        return self._alive


class WorkerLivenessTest(unittest.TestCase):
    def test_worker_exit_is_fatal_while_consuming_even_with_exitcode_zero(self) -> None:
        with self.assertRaisesRegex(DeliveryFailureError, "group 'A'.*exitcode=0"):
            ensure_workers_alive({"A": _Process(alive=False, exitcode=0)})

    def test_clean_worker_exit_is_accepted_only_during_normal_shutdown(self) -> None:
        ensure_workers_alive(
            {"A": _Process(alive=False, exitcode=0)}, allow_clean_exit=True
        )

    def test_crashed_worker_is_fatal_during_normal_shutdown(self) -> None:
        with self.assertRaisesRegex(DeliveryFailureError, "exitcode=3"):
            ensure_workers_alive(
                {"A": _Process(alive=False, exitcode=3)}, allow_clean_exit=True
            )


class DrainCompletenessTest(unittest.TestCase):
    def test_no_incomplete_offsets_after_contiguous_delivery(self) -> None:
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 10)
        tracker.record_outcome(0, 10, True)

        self.assertEqual(incomplete_offset_partitions(tracker), {})

    def test_pending_blocked_and_failed_offsets_prevent_normal_shutdown(self) -> None:
        tracker = ContiguousOffsetTracker()
        for offset in (10, 11, 12):
            tracker.register_received(0, offset)
        tracker.record_outcome(0, 12, True)
        tracker.record_outcome(0, 11, False)

        self.assertEqual(
            incomplete_offset_partitions(tracker),
            {
                0: {
                    "first_offset": 10,
                    "next_committable": None,
                    "pending": [10],
                    "succeeded_blocked": [12],
                    "failed": [11],
                }
            },
        )


class _StoppingProcess:
    def __init__(self) -> None:
        self._alive = True
        self.exitcode = None

    def start(self) -> None:
        pass

    def join(self, timeout: float = 0) -> None:
        pass

    def is_alive(self) -> bool:
        return self._alive

    def stop_cleanly(self) -> None:
        self._alive = False
        self.exitcode = 0


class _AckingWorkQueue:
    """Worker-queue double that confirms delivery before normal shutdown."""

    def __init__(self, delivery_queue: Queue, process: _StoppingProcess) -> None:
        self._delivery_queue = delivery_queue
        self._process = process

    def put_nowait(self, item: GroupWorkItem) -> None:
        self._delivery_queue.put(
            WorkerDeliveryAck(item.topic, item.partition, item.offset, True)
        )

    def put(self, item, timeout: float = 0) -> None:
        if item is None:
            self._process.stop_cleanly()


class EntrypointDrainTest(unittest.TestCase):
    def test_normal_shutdown_drains_delivery_and_commits_before_closing(self) -> None:
        entry = _load_entry_module()
        process = _StoppingProcess()
        metrics_queue: Queue = Queue()
        delivery_queue: Queue = Queue()
        work_queue = _AckingWorkQueue(delivery_queue, process)
        consumer = MagicMock(name="consumer")
        consumer.assignment.return_value = []
        message = MagicMock(name="message")
        message.error.return_value = None
        message.topic.return_value = "hstree-analysis"
        message.partition.return_value = 0
        message.offset.return_value = 10
        message.key.return_value = None
        message.value.return_value = b"payload"
        consumer.poll.side_effect = [message, KeyboardInterrupt()]
        extractor = MagicMock(name="feature-extractor")
        extractor.is_initialized = True
        extractor.get_feature_names.return_value = ["A"]
        deserializer = MagicMock(
            return_value={"Group": "A", "source_event_id": "row-10"}
        )
        env = {
            "BOOTSTRAP_SERVERS": "kafka:9092",
            "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
            "KAFKA_DEST_TOPIC_JSON": "explainability-v2-viz",
            "DASMA_ENABLED": "1",
            "LOG_LEVEL": "CRITICAL",
            "EXPLAINABILITY_MODEL": "hstree",
            "EXPLAINABILITY_MODE": "reference",
            "EXPLAINABILITY_METHOD": "kernel",
            "KAFKA_SOURCE_TOPIC": "hstree-analysis",
        }

        with patch.dict("os.environ", env, clear=True), patch.object(
            entry, "create_schema_registry_client", return_value=MagicMock()
        ), patch.object(
            entry, "AvroDeserializer", return_value=deserializer
        ), patch.object(
            entry, "create_consumer", return_value=consumer
        ), patch.object(
            entry, "FeatureExtractor", return_value=extractor
        ), patch.object(
            entry.mp, "Queue", side_effect=[metrics_queue, delivery_queue, work_queue]
        ), patch.object(
            entry.mp, "Process", return_value=process
        ):
            entry.main()

        self.assertEqual(consumer.commit.call_count, 1)
        self.assertEqual(consumer.commit.call_args.kwargs["offsets"][0].offset, 11)
        self.assertFalse(process.is_alive())
        consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
