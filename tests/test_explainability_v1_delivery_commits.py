"""Delivery at-least-once tests for explainability V1 (V1-03).

The tested cycle is:

    produce -> Avro callback + JSON callback -> WorkerDeliveryAck (worker ->
    main, separate channel from the V1-02 metrics) -> ContiguousOffsetTracker
    -> synchronous Kafka commit of the exclusive contiguous offset.

Covered policy points: ``enable.auto.commit=False``; both outputs mandatory
(explicit failure on an empty JSON topic, before consuming); a bare
``produce()`` is never a confirmation (callbacks or bounded timeout only); a
SHAP ``failed`` status remains committable when both result messages were
delivered; a feature-extraction failure emits a negative ack without a
completion metric; a negative ack or a failed commit fails the service
explicitly; no local manual retry (the idempotent producers keep their
internal retries, the replay is guaranteed by the absence of commit).
"""

from __future__ import annotations

import importlib.util
import queue
import signal
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from dasma_kafka.explainability import runtime as runtime_module
from dasma_kafka.explainability.offset_tracker import ContiguousOffsetTracker
from dasma_kafka.explainability.runtime import (
    DeliveryFailureError,
    GroupWorkItem,
    WorkerCompleted,
    WorkerDeliveryAck,
    WorkerStarted,
    create_consumer,
    drain_delivery_acks,
    ensure_workers_alive,
    group_worker_main,
)

ROOT = Path(__file__).resolve().parents[1]
ENTRY_PATH = ROOT / "runtime" / "explainability" / "regular" / "explainability-multi-thread-by-group.py"

WORKER_CONFIG = {
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


def _record(source_event_id: str = "row-000001") -> dict:
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


def _item(record, offset: int, partition: int = 0) -> GroupWorkItem:
    return GroupWorkItem(
        key_str=None, record=record, topic="hstree-analysis", partition=partition, offset=offset
    )


def _drain_queue(q: queue.Queue) -> list:
    events = []
    while True:
        try:
            events.append(q.get_nowait())
        except queue.Empty:
            return events


class ConsumerConfigurationTest(unittest.TestCase):
    def test_auto_commit_is_disabled(self) -> None:
        with patch.object(runtime_module, "Consumer") as mock_consumer:
            runtime_module.create_consumer(MagicMock(), "kafka:9092", "explainability-consumer")

        config = mock_consumer.call_args[0][0]
        self.assertIs(config["enable.auto.commit"], False)


class WorkerDeliveryTest(unittest.TestCase):
    def _run_worker(
        self,
        records,
        *,
        delivery_q=None,
        metrics_q=None,
        avro_callback_error=None,
        json_callback_error=None,
        avro_produce_error=None,
        json_produce_error=None,
        fire_callbacks=True,
        shap_return=None,
        fit_score_return=0.5,
        extract_error=None,
        config_overrides=None,
    ):
        config_dict = dict(WORKER_CONFIG, **(config_overrides or {}))

        model_manager = MagicMock(name="model-manager")
        model_manager.reference_score_field = "hstree_score"
        model_manager.mismatch_code = "hstree_score_mismatch"
        model_manager.fit_score.return_value = fit_score_return
        model_manager.get_background_data.return_value = np.zeros((100, 2))
        model_manager.get_model.return_value = MagicMock(name="hstree-model")

        shap_explainer = MagicMock(name="shap-explainer")
        shap_explainer.compute_shap_values.return_value = shap_return or {
            "shap_values": {"A": 0.1, "B": -0.2},
            "shap_base_value": 0.05,
            "shap_computation_time_ms": 1.0,
        }

        feature_extractor = MagicMock(name="feature-extractor")
        if extract_error is not None:
            feature_extractor.extract.side_effect = extract_error
        else:
            feature_extractor.extract.return_value = np.array([1.5, 2.5])

        def produce_avro(topic, key=None, value=None, on_delivery=None, **kwargs):
            if avro_produce_error is not None:
                raise avro_produce_error
            if on_delivery is not None and fire_callbacks:
                on_delivery(avro_callback_error, MagicMock())
            return 0

        def produce_json(topic, key=None, value=None, on_delivery=None, **kwargs):
            if json_produce_error is not None:
                raise json_produce_error
            if on_delivery is not None and fire_callbacks:
                on_delivery(json_callback_error, MagicMock())
            return 0

        avro_producer = MagicMock(name="avro-producer")
        avro_producer.produce.side_effect = produce_avro
        json_producer = MagicMock(name="json-producer")
        json_producer.produce.side_effect = produce_json

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
            if fire_callbacks:
                group_worker_main(
                    "g1", config_dict, ["A", "B"], work_q, metrics_q, delivery_q
                )
            else:
                # No callback ever fires: the bounded timeout applies.
                with patch.object(runtime_module, "DELIVERY_TIMEOUT_SECONDS", 0.05):
                    group_worker_main(
                        "g1", config_dict, ["A", "B"], work_q, metrics_q, delivery_q
                    )

        return avro_producer, json_producer

    def test_double_success_sends_positive_acks_and_commits_exclusive_offsets(self) -> None:
        delivery_q: queue.Queue = queue.Queue()
        metrics_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record("row-000001"), _record("row-000002"), _record("row-000003")],
            delivery_q=delivery_q,
            metrics_q=metrics_q,
        )

        acks = list(delivery_q.queue)  # peek, keep them for the drain
        self.assertEqual(len(acks), 3)
        for ack in acks:
            self.assertIsInstance(ack, WorkerDeliveryAck)
            self.assertTrue(ack.success)
            self.assertIsNone(ack.error_code)

        tracker = ContiguousOffsetTracker()
        for offset in (0, 1, 2):
            tracker.register_received(0, offset)
        consumer = MagicMock()
        drain_delivery_acks(delivery_q, tracker, consumer, "hstree-analysis", MagicMock())

        commits = [call.kwargs["offsets"] for call in consumer.commit.call_args_list]
        self.assertEqual(
            commits,
            [
                [TopicPartition("hstree-analysis", 0, 1)],
                [TopicPartition("hstree-analysis", 0, 2)],
                [TopicPartition("hstree-analysis", 0, 3)],
            ],
        )
        self.assertTrue(all(call.kwargs["asynchronous"] is False for call in consumer.commit.call_args_list))

    def test_out_of_order_delivery_commits_only_after_the_hole_is_filled(self) -> None:
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 11)  # still pending
        tracker.register_received(0, 12)
        delivery_q: queue.Queue = queue.Queue()
        delivery_q.put(WorkerDeliveryAck("hstree-analysis", 0, 12, True))
        consumer = MagicMock()

        drain_delivery_acks(delivery_q, tracker, consumer, "hstree-analysis", MagicMock())
        consumer.commit.assert_not_called()

        delivery_q.put(WorkerDeliveryAck("hstree-analysis", 0, 11, True))
        drain_delivery_acks(delivery_q, tracker, consumer, "hstree-analysis", MagicMock())
        self.assertEqual(
            consumer.commit.call_args.kwargs["offsets"],
            [TopicPartition("hstree-analysis", 0, 13)],
        )

    def test_avro_callback_failure_sends_negative_ack_and_fails_the_service(self) -> None:
        delivery_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record()],
            delivery_q=delivery_q,
            avro_callback_error=KafkaException("avro broker error"),
        )

        ack = list(delivery_q.queue)[0]  # peek
        self.assertFalse(ack.success)
        self.assertEqual(ack.error_code, "avro_delivery_failed")

        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 0)
        consumer = MagicMock()
        with self.assertRaises(DeliveryFailureError):
            drain_delivery_acks(delivery_q, tracker, consumer, "hstree-analysis", MagicMock())
        consumer.commit.assert_not_called()

    def test_json_callback_failure_sends_negative_ack_and_fails_the_service(self) -> None:
        delivery_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record()],
            delivery_q=delivery_q,
            json_callback_error=KafkaException("json broker error"),
        )

        ack = list(delivery_q.queue)[0]  # peek
        self.assertFalse(ack.success)
        self.assertEqual(ack.error_code, "json_delivery_failed")

        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 0)
        consumer = MagicMock()
        with self.assertRaises(DeliveryFailureError):
            drain_delivery_acks(delivery_q, tracker, consumer, "hstree-analysis", MagicMock())
        consumer.commit.assert_not_called()

    def test_avro_produce_exception_sends_negative_ack(self) -> None:
        delivery_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record()], delivery_q=delivery_q, avro_produce_error=BufferError("queue full")
        )

        ack = _drain_queue(delivery_q)[0]
        self.assertFalse(ack.success)
        self.assertEqual(ack.error_code, "avro_produce_error")

    def test_json_produce_exception_sends_negative_ack(self) -> None:
        delivery_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record()], delivery_q=delivery_q, json_produce_error=BufferError("queue full")
        )

        ack = _drain_queue(delivery_q)[0]
        self.assertFalse(ack.success)
        self.assertEqual(ack.error_code, "json_produce_error")

    def test_delivery_timeout_sends_negative_ack_without_commit(self) -> None:
        delivery_q: queue.Queue = queue.Queue()
        self._run_worker([_record()], delivery_q=delivery_q, fire_callbacks=False)

        ack = list(delivery_q.queue)[0]  # peek
        self.assertFalse(ack.success)
        self.assertEqual(ack.error_code, "delivery_timeout")

        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 0)
        consumer = MagicMock()
        with self.assertRaises(DeliveryFailureError):
            drain_delivery_acks(delivery_q, tracker, consumer, "hstree-analysis", MagicMock())
        consumer.commit.assert_not_called()

    def test_shap_failed_status_remains_committable_when_both_outputs_delivered(self) -> None:
        delivery_q: queue.Queue = queue.Queue()
        metrics_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record()],
            delivery_q=delivery_q,
            metrics_q=metrics_q,
            shap_return={
                "shap_values": None,
                "shap_base_value": None,
                "shap_computation_time_ms": 2.0,
            },
        )

        completed = _drain_queue(metrics_q)[-1]
        self.assertEqual(completed.status, "failed")  # computation status kept
        ack = list(delivery_q.queue)[0]  # peek, keep it for the drain
        self.assertTrue(ack.success)  # both failed-status messages delivered

        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 0)
        consumer = MagicMock()
        drain_delivery_acks(delivery_q, tracker, consumer, "hstree-analysis", MagicMock())
        self.assertEqual(
            consumer.commit.call_args.kwargs["offsets"],
            [TopicPartition("hstree-analysis", 0, 1)],
        )

    def test_extraction_failure_sends_negative_ack_without_completion(self) -> None:
        delivery_q: queue.Queue = queue.Queue()
        metrics_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record()],
            delivery_q=delivery_q,
            metrics_q=metrics_q,
            extract_error=ValueError("bad features"),
        )

        # No completion metric (V1-02 semantics preserved)...
        metrics_events = _drain_queue(metrics_q)
        self.assertEqual(len(metrics_events), 1)
        self.assertIsInstance(metrics_events[0], WorkerStarted)

        # ...but a negative ack with the stable code, so the campaign stops.
        ack = list(delivery_q.queue)[0]  # peek, keep it for the drain
        self.assertFalse(ack.success)
        self.assertEqual(ack.error_code, "feature_extraction_error")

        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 0)
        consumer = MagicMock()
        with self.assertRaises(DeliveryFailureError):
            drain_delivery_acks(delivery_q, tracker, consumer, "hstree-analysis", MagicMock())
        consumer.commit.assert_not_called()

    def test_metrics_and_delivery_are_two_separate_channels(self) -> None:
        delivery_q: queue.Queue = queue.Queue()
        metrics_q: queue.Queue = queue.Queue()
        self._run_worker(
            [_record()], delivery_q=delivery_q, metrics_q=metrics_q
        )

        metrics_events = _drain_queue(metrics_q)
        delivery_events = _drain_queue(delivery_q)
        self.assertTrue(
            all(isinstance(event, (WorkerStarted, WorkerCompleted)) for event in metrics_events)
        )
        self.assertTrue(
            all(isinstance(event, WorkerDeliveryAck) for event in delivery_events)
        )
        self.assertEqual(len(delivery_events), 1)
        self.assertTrue(delivery_events[0].success)
        # V1-02 non-regression: completed means "status attributed and
        # publication attempted", not a Kafka delivery confirmation.
        completed = [e for e in metrics_events if isinstance(e, WorkerCompleted)]
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0].status, "computed")


class CommitFailureTest(unittest.TestCase):
    def test_commit_failure_fails_the_service_without_further_commits(self) -> None:
        tracker = ContiguousOffsetTracker()
        for offset in (0, 1, 2):
            tracker.register_received(0, offset)
        delivery_q: queue.Queue = queue.Queue()
        for offset in (0, 1, 2):
            delivery_q.put(WorkerDeliveryAck("hstree-analysis", 0, offset, True))

        consumer = MagicMock()
        consumer.commit.side_effect = KafkaException("commit boom")
        with self.assertRaises(DeliveryFailureError):
            drain_delivery_acks(delivery_q, tracker, consumer, "hstree-analysis", MagicMock())

        self.assertEqual(consumer.commit.call_count, 1)


class JsonTopicMandatoryTest(unittest.TestCase):
    def test_worker_fails_fast_without_json_topic(self) -> None:
        with self.assertRaises(RuntimeError) as ctx:
            WorkerDeliveryTest()._run_worker(
                [_record()], config_overrides={"dest_topic_json": ""}
            )
        self.assertIn("JSON", str(ctx.exception))

    def test_entrypoint_guard_requires_json_before_consuming(self) -> None:
        source = ENTRY_PATH.read_text(encoding="utf-8")
        guard_position = source.index("KAFKA_DEST_TOPIC_JSON is empty")
        subscribe_position = source.index("consumer.subscribe")
        self.assertLess(guard_position, subscribe_position)


class WorkerSupervisionTest(unittest.TestCase):
    def _process(self, *, alive: bool, exitcode: int) -> MagicMock:
        process = MagicMock(name="process")
        process.is_alive.return_value = alive
        process.exitcode = exitcode
        return process

    def test_dead_worker_during_consumption_is_fatal_even_with_exit_zero(self) -> None:
        group_workers = {"g1": self._process(alive=False, exitcode=0)}
        with self.assertRaises(DeliveryFailureError):
            ensure_workers_alive(group_workers)

    def test_dead_worker_with_nonzero_exit_is_fatal_after_sentinels(self) -> None:
        group_workers = {"g1": self._process(alive=False, exitcode=1)}
        with self.assertRaises(DeliveryFailureError):
            ensure_workers_alive(group_workers, allow_clean_exit=True)

    def test_clean_exit_is_tolerated_only_after_sentinels(self) -> None:
        group_workers = {"g1": self._process(alive=False, exitcode=0)}
        ensure_workers_alive(group_workers, allow_clean_exit=True)  # no raise

    def test_alive_workers_are_tolerated(self) -> None:
        group_workers = {"g1": self._process(alive=True, exitcode=None)}
        ensure_workers_alive(group_workers)
        ensure_workers_alive(group_workers, allow_clean_exit=True)


def _load_entry_module():
    spec = importlib.util.spec_from_file_location("explainability_regular_entry_v103", ENTRY_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["explainability_regular_entry_v103"] = module
    spec.loader.exec_module(module)
    return module


def _data_message(offset: int, *, value=b"payload"):
    message = MagicMock(name=f"msg-{offset}")
    message.error.return_value = None
    message.topic.return_value = "hstree-analysis"
    message.partition.return_value = 0
    message.offset.return_value = offset
    message.key.return_value = None
    message.value.return_value = value
    return message


ENTRYPOINT_ENV = {
    "BOOTSTRAP_SERVERS": "kafka:9092",
    "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
    "KAFKA_DEST_TOPIC_JSON": "explainability-viz",
    "DASMA_ENABLED": "1",
    "LOG_LEVEL": "CRITICAL",
    "EXPLAINABILITY_MODEL": "hstree",
    "EXPLAINABILITY_MODE": "reference",
    "EXPLAINABILITY_METHOD": "kernel",
    "KAFKA_SOURCE_TOPIC": "hstree-analysis",
}


class EntrypointRejectionTest(unittest.TestCase):
    """Light integration tests of the entrypoint with a mocked consumer.

    V1-03 hardening: a data message that cannot be turned into a tracked work
    item fails the service BEFORE the next offset is consumed, so the tracker
    never starts at a later offset and Kafka replays the unprocessed one.
    """

    def setUp(self) -> None:
        self.entry = _load_entry_module()

    def _run_main(self, poll_sequence, *, deserializer=None, extractor=None):
        consumer = MagicMock(name="consumer")
        consumer.poll.side_effect = list(poll_sequence)
        tracker = MagicMock(name="offset-tracker")

        with patch.dict("os.environ", ENTRYPOINT_ENV, clear=True), patch.object(
            self.entry, "create_schema_registry_client", return_value=MagicMock()
        ), patch.object(
            self.entry, "AvroDeserializer", return_value=deserializer or MagicMock()
        ), patch.object(
            self.entry, "create_consumer", return_value=consumer
        ), patch.object(
            self.entry, "ContiguousOffsetTracker", return_value=tracker
        ), patch.object(
            self.entry, "drain_delivery_acks"
        ), patch.object(
            self.entry, "drain_worker_metrics"
        ):
            if extractor is not None:
                with patch.object(self.entry, "FeatureExtractor", return_value=extractor):
                    with self.assertRaises(DeliveryFailureError) as ctx:
                        self.entry.main()
            else:
                with self.assertRaises(DeliveryFailureError) as ctx:
                    self.entry.main()
        return ctx, tracker, consumer

    def test_invalid_value_at_offset_10_fails_service_before_offset_11(self) -> None:
        msg10 = _data_message(10, value=None)
        msg11 = _data_message(11)  # theoretically valid, never reached

        ctx, tracker, consumer = self._run_main([msg10, msg11, KeyboardInterrupt()])

        # The service failed on offset 10: offset 11 is neither consumed, nor
        # enqueued, nor registered; nothing is committed; the consumer is
        # closed in the finally.
        self.assertEqual(consumer.poll.call_count, 1)
        tracker.register_received.assert_not_called()
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()
        self.assertIn("partition=0", str(ctx.exception))
        self.assertIn("offset=10", str(ctx.exception))

    def test_deserialization_failure_fails_the_service(self) -> None:
        broken_deserializer = MagicMock(name="deserializer")
        broken_deserializer.side_effect = ValueError("bad avro payload")
        msg10 = _data_message(10)

        ctx, tracker, consumer = self._run_main(
            [msg10, KeyboardInterrupt()], deserializer=broken_deserializer
        )

        self.assertEqual(consumer.poll.call_count, 1)
        tracker.register_received.assert_not_called()
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()
        self.assertIn("partition=0", str(ctx.exception))
        self.assertIn("offset=10", str(ctx.exception))

    def test_message_without_ready_worker_fails_the_service(self) -> None:
        # A deserializable record whose group never gets a worker queue:
        # rejected explicitly, never silently skipped.
        record = {"Group": "G1", "source_event_id": "row-10", "label": "text-only"}
        extractor = MagicMock(name="feature-extractor")
        extractor.is_initialized = False

        msg10 = _data_message(10)
        ctx, tracker, consumer = self._run_main(
            [msg10, KeyboardInterrupt()],
            deserializer=MagicMock(return_value=record),
            extractor=extractor,
        )

        self.assertIn("no worker queue available", str(ctx.exception))
        self.assertIn("partition=0", str(ctx.exception))
        self.assertIn("offset=10", str(ctx.exception))
        tracker.register_received.assert_not_called()
        consumer.commit.assert_not_called()

    def test_kafka_poll_failure_is_fatal(self) -> None:
        consumer = MagicMock(name="consumer")
        consumer.poll.side_effect = KafkaException("transport lost")
        tracker = MagicMock(name="offset-tracker")

        with patch.dict("os.environ", ENTRYPOINT_ENV, clear=True), patch.object(
            self.entry, "create_schema_registry_client", return_value=MagicMock()
        ), patch.object(self.entry, "AvroDeserializer", return_value=MagicMock()), patch.object(
            self.entry, "create_consumer", return_value=consumer
        ), patch.object(
            self.entry, "ContiguousOffsetTracker", return_value=tracker
        ), patch.object(
            self.entry, "drain_delivery_acks"
        ), patch.object(
            self.entry, "drain_worker_metrics"
        ):
            with self.assertRaises(DeliveryFailureError):
                self.entry.main()

        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()

    def test_partition_eof_continues_without_registration_or_commit(self) -> None:
        eof_error = MagicMock(name="eof-error")
        eof_error.code.return_value = KafkaError._PARTITION_EOF
        eof_message = MagicMock(name="eof-message")
        eof_message.error.return_value = eof_error

        consumer = MagicMock(name="consumer")
        consumer.poll.side_effect = [eof_message, KeyboardInterrupt()]
        tracker = MagicMock(name="offset-tracker")

        with patch.dict("os.environ", ENTRYPOINT_ENV, clear=True), patch.object(
            self.entry, "create_schema_registry_client", return_value=MagicMock()
        ), patch.object(self.entry, "AvroDeserializer", return_value=MagicMock()), patch.object(
            self.entry, "create_consumer", return_value=consumer
        ), patch.object(
            self.entry, "ContiguousOffsetTracker", return_value=tracker
        ), patch.object(
            self.entry, "drain_delivery_acks"
        ), patch.object(
            self.entry, "drain_worker_metrics"
        ):
            self.entry.main()  # graceful stop on KeyboardInterrupt

        # EOF is explicitly not a record: no registration, no commit,
        # consumption continued (poll called twice), consumer closed.
        tracker.register_received.assert_not_called()
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()
        self.assertEqual(consumer.poll.call_count, 2)


class SigtermShutdownTest(unittest.TestCase):
    """SIGTERM borrows the existing normal-stop path (V1-03, pure test).

    Docker sends SIGTERM: the entrypoint must install a handler that raises
    KeyboardInterrupt (so the drain of metrics, delivery acks and contiguous
    commits runs), then restore the previous handler in the finally.
    """

    def setUp(self) -> None:
        self.entry = _load_entry_module()

    def _run_main_with_signals(self, consumer: MagicMock, captured: dict) -> tuple:
        """Run main() with mocked signal APIs; return the mock call data.

        ``captured`` is filled by the fake ``signal.signal`` during main(),
        so the caller can wire the installed handler into its own mocks.
        """
        tracker = MagicMock(name="offset-tracker")
        previous_handler = MagicMock(name="previous-handler")
        installed: list = []

        def _fake_signal(signum, handler):
            installed.append((signum, handler))
            # Keep the first handler installed per signal: the restore call
            # in the finally overwrites it otherwise.
            captured.setdefault(signum, handler)

        with patch.dict("os.environ", ENTRYPOINT_ENV, clear=True), patch.object(
            self.entry.signal, "getsignal", return_value=previous_handler
        ) as mock_getsignal, patch.object(
            self.entry.signal, "signal", side_effect=_fake_signal
        ) as mock_signal, patch.object(
            self.entry, "create_schema_registry_client", return_value=MagicMock()
        ), patch.object(
            self.entry, "AvroDeserializer", return_value=MagicMock()
        ), patch.object(
            self.entry, "create_consumer", return_value=consumer
        ), patch.object(
            self.entry, "ContiguousOffsetTracker", return_value=tracker
        ), patch.object(
            self.entry, "drain_delivery_acks"
        ) as mock_drain_acks, patch.object(
            self.entry, "drain_worker_metrics"
        ):
            self.entry.main()

        return mock_getsignal, mock_signal, mock_drain_acks, previous_handler

    def test_sigterm_handler_is_installed_then_restored(self) -> None:
        consumer = MagicMock(name="consumer")
        consumer.poll.side_effect = KeyboardInterrupt()  # loop ends immediately
        captured: dict = {}

        mock_getsignal, mock_signal, _, previous_handler = (
            self._run_main_with_signals(consumer, captured)
        )

        # The previous handler was read and the normal-shutdown handler was
        # installed exactly once.
        mock_getsignal.assert_called_once_with(signal.SIGTERM)
        installs = [call for call in mock_signal.call_args_list if call.args[1] is not previous_handler]
        self.assertEqual(len(installs), 1)
        self.assertEqual(installs[0].args[0], signal.SIGTERM)
        self.assertIn(signal.SIGTERM, captured)

        # The previous handler was restored in the finally, exactly once.
        restores = [call for call in mock_signal.call_args_list if call.args[1] is previous_handler]
        self.assertEqual(len(restores), 1)
        self.assertEqual(restores[0].args[0], signal.SIGTERM)

        # The installed handler raises KeyboardInterrupt: it borrows the
        # existing normal-stop path.
        with self.assertRaises(KeyboardInterrupt):
            captured[signal.SIGTERM](signal.SIGTERM, None)

    def test_sigterm_during_poll_triggers_the_normal_drain(self) -> None:
        captured: dict = {}
        consumer = MagicMock(name="consumer")

        def _poll(timeout=None):
            # Simulate SIGTERM arriving while the main loop is polling.
            captured[signal.SIGTERM](signal.SIGTERM, None)

        consumer.poll.side_effect = _poll

        _, _, mock_drain_acks, _ = self._run_main_with_signals(consumer, captured)

        # The SIGTERM handler raised KeyboardInterrupt: the finally executed
        # the normal stop (sentinels, bounded drain of metrics and delivery
        # acks) and closed the consumer without any commit.
        self.assertGreaterEqual(mock_drain_acks.call_count, 1)
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
