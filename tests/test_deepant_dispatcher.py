"""Integration tests for the DeepAnT parallel-by-group dispatcher.

``mp.Process`` / ``mp.Queue`` are replaced with thread-backed fakes so the
dispatcher logic (ack -> commit watermark, publish-before-commit, backpressure,
group->worker assignment over a bounded pool) is exercised deterministically
in-process, without torch, real Kafka, or multiprocessing spawn semantics.
"""

from __future__ import annotations

import queue
import threading
import time
import unittest
from unittest.mock import patch

from src.dasma_kafka.deepant_analysis import runtime as rt
from src.dasma_kafka.deepant_analysis.runtime import (
    DeepAntAck,
    DeepAntWorkerConfig,
    run_parallel_by_group,
)


# --- thread-backed multiprocessing fakes ------------------------------------


class _FakeQueue:
    def __init__(self, maxsize: int = 0) -> None:
        self._q: queue.Queue = queue.Queue(maxsize=maxsize)

    def put(self, item, timeout=None):
        self._q.put(item, timeout=timeout)

    def get(self, timeout=None):
        return self._q.get(timeout=timeout)

    def get_nowait(self):
        return self._q.get_nowait()


class _FakeProcess:
    def __init__(self, target, args=(), daemon=False) -> None:
        self._thread = threading.Thread(target=target, args=args, daemon=True)

    def start(self):
        self._thread.start()

    def join(self, timeout=None):
        self._thread.join(timeout=timeout)

    def is_alive(self):
        return self._thread.is_alive()

    def terminate(self):
        # Threads cannot be force-killed; workers exit on the None sentinel.
        pass


class _FakeMp:
    Queue = _FakeQueue
    Process = _FakeProcess


# --- stub workers (run in a thread) -----------------------------------------


def stub_worker_ok(worker_id, config, feature_names, work_q, ack_q):
    """Ack every item as a successful publish."""
    while True:
        item = work_q.get()
        if item is None:
            break
        ack_q.put(DeepAntAck(item.topic, item.partition, item.offset, True))


def stub_worker_slow(worker_id, config, feature_names, work_q, ack_q):
    """Ack successfully but slowly, to force the bounded queue to fill."""
    while True:
        item = work_q.get()
        if item is None:
            break
        time.sleep(0.02)
        ack_q.put(DeepAntAck(item.topic, item.partition, item.offset, True))


def stub_worker_fail_offset_1(worker_id, config, feature_names, work_q, ack_q):
    """Ack all offsets as success except offset 1, which fails to publish."""
    while True:
        item = work_q.get()
        if item is None:
            break
        ack_q.put(DeepAntAck(item.topic, item.partition, item.offset, item.offset != 1))


# --- fakes for the consumer --------------------------------------------------


class _FakeMessage:
    def __init__(self, offset, group, topic="source-data", partition=0):
        self._offset = offset
        self._record = {"Group": group, "f1": float(offset)}
        self._topic = topic
        self._partition = partition

    def value(self):
        return self._record

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._record["Group"]

    def timestamp(self):
        return (1, 1000 + self._offset)

    def error(self):
        return None


class _FakeConsumer:
    """Yields the given messages once, then raises KeyboardInterrupt to stop."""

    def __init__(self, messages):
        self._messages = list(messages)
        self.commits: list = []
        self.subscribed = None
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        if self._messages:
            return self._messages.pop(0)
        # Let the loop drain remaining acks before stopping.
        raise KeyboardInterrupt

    def commit(self, offsets, asynchronous=False):
        # offsets is a list of TopicPartition(topic, partition, offset)
        for tp in offsets:
            self.commits.append((tp.topic, tp.partition, tp.offset))

    def close(self):
        self.closed = True


def _config() -> DeepAntWorkerConfig:
    return DeepAntWorkerConfig(
        bootstrap_servers="kafka1:9092",
        schema_registry_url="http://schema-registry:8081",
        schema_registry_basic_auth_user_info=None,
        schema_registry_basic_auth_source="USER_INFO",
        dest_topic="deepant-analysis",
        dest_viz_topic="deepant-analysis-viz",
        dest_avro_schema="{}",
        seed=42,
        log_level="INFO",
        window_size=0,
        exclude_fields=["timestamp", "Group", "group", "Cycle", "LabviewTime", "time"],
        threshold_mode="percentile",
        threshold_percentile=99.0,
        threshold_window=500,
        welford_window=2048,
        anomaly_k_sigma=3.0,
        anomaly_warmup_window=100,
        exclude_anomalies_from_refit=False,
        cpu_threads=1,
        lookback_size=100,
        initial_window_size=512,
        refit_every=64,
        deepant_window_size=2048,
        filters=32,
        kernel_size=2,
        dropout=0.25,
        lr=0.001,
        epochs_initial=10,
        epochs_refit=5,
        batch_size=32,
    )


EXCLUDE = {"timestamp", "Group", "group", "Cycle", "LabviewTime", "time"}


class DispatcherTest(unittest.TestCase):
    def setUp(self) -> None:
        # Replace multiprocessing with thread-backed fakes and neutralize signal
        # registration (only valid in the main thread, interferes with pytest).
        self._mp_patch = patch.object(rt, "mp", _FakeMp)
        self._sig_patch = patch.object(rt.signal, "signal", lambda *a, **k: None)
        self._mp_patch.start()
        self._sig_patch.start()

    def tearDown(self) -> None:
        self._mp_patch.stop()
        self._sig_patch.stop()

    def _run(self, consumer, worker_target, max_workers=32, queue_maxsize=1000):
        run_parallel_by_group(
            consumer=consumer,
            source_topic="source-data",
            worker_config=_config(),
            exclude_fields=EXCLUDE,
            max_workers=max_workers,
            queue_maxsize=queue_maxsize,
            idle_exit_sec=0,
            log_every=1000,
            worker_target=worker_target,
        )

    def test_commits_contiguous_prefix_after_acks(self) -> None:
        msgs = [_FakeMessage(off, group="A") for off in range(5)]
        consumer = _FakeConsumer(msgs)

        self._run(consumer, stub_worker_ok)

        # All five offsets acked -> final commit position is 5 (next to read).
        self.assertTrue(consumer.commits)
        self.assertEqual(consumer.commits[-1], ("source-data", 0, 5))
        self.assertTrue(consumer.closed)

    def test_groups_share_bounded_pool(self) -> None:
        # Four distinct groups, but the pool is capped at 2 workers: groups are
        # multiplexed onto at most 2 worker processes and nothing is dropped.
        msgs = [
            _FakeMessage(0, "A"),
            _FakeMessage(1, "B"),
            _FakeMessage(2, "C"),
            _FakeMessage(3, "D"),
            _FakeMessage(4, "A"),
        ]
        consumer = _FakeConsumer(msgs)

        started: list = []

        class _CountingProcess(_FakeProcess):
            def __init__(self, target, args=(), daemon=False):
                started.append(args[0])  # worker_id (int)
                super().__init__(target, args=args, daemon=daemon)

        with patch.object(_FakeMp, "Process", _CountingProcess):
            self._run(consumer, stub_worker_ok, max_workers=2)

        # No more workers than the pool size, and slots are valid indices.
        self.assertLessEqual(len(set(started)), 2)
        self.assertTrue(set(started).issubset({0, 1}))
        # Every message is still processed and committed (no group dropped).
        self.assertEqual(consumer.commits[-1], ("source-data", 0, 5))

    def test_worker_failure_stalls_commit_past_offset(self) -> None:
        msgs = [_FakeMessage(off, group="A") for off in range(4)]
        consumer = _FakeConsumer(msgs)

        self._run(consumer, stub_worker_fail_offset_1)

        # Offset 1 fails -> watermark cannot pass it -> highest commit is 1.
        self.assertTrue(consumer.commits)
        self.assertEqual(max(off for _, _, off in consumer.commits), 1)

    def test_more_groups_than_workers_still_commits_all(self) -> None:
        # Three groups onto a single-worker pool: all groups are multiplexed onto
        # worker 0 and every offset is committed -> no fatal shutdown, no loss.
        msgs = [_FakeMessage(0, "A"), _FakeMessage(1, "B"), _FakeMessage(2, "C")]
        consumer = _FakeConsumer(msgs)

        started: list = []

        class _CountingProcess(_FakeProcess):
            def __init__(self, target, args=(), daemon=False):
                started.append(args[0])
                super().__init__(target, args=args, daemon=daemon)

        with patch.object(_FakeMp, "Process", _CountingProcess):
            self._run(consumer, stub_worker_ok, max_workers=1)

        # Exactly one worker spawned; all three offsets committed.
        self.assertEqual(set(started), {0})
        self.assertEqual(consumer.commits[-1], ("source-data", 0, 3))
        self.assertTrue(consumer.closed)

    def test_backpressure_does_not_drop_under_full_queue(self) -> None:
        # Bounded queue of size 1 with a slow worker forces backpressure waits,
        # but every message must still be processed and committed (no drop).
        msgs = [_FakeMessage(off, group="A") for off in range(6)]
        consumer = _FakeConsumer(msgs)

        self._run(consumer, stub_worker_slow, queue_maxsize=1)

        self.assertEqual(consumer.commits[-1], ("source-data", 0, 6))


if __name__ == "__main__":
    unittest.main()
