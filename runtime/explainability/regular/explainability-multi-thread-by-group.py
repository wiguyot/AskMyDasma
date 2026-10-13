#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Entry point for the explainability service.

This file is intentionally kept easy to read:
- it loads the configuration
- it consumes `hstree-analysis`
- it routes messages to one worker per group

The more technical logic lives in `src/dasma_kafka/explainability/`.
"""

import logging
import multiprocessing as mp
import os
import random
import signal
import sys
import time
from pathlib import Path
from queue import Full
from typing import Any, Dict

import numpy as np
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

ROOT_DIR = Path(__file__).resolve().parents[3]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.explainability.config import ExplainabilityConfig, validate_explainability_selection  # noqa: E402
from dasma_kafka.explainability.features import (  # noqa: E402
    FeatureExtractor,
    IForestFeatureExtractor,
    KitNetFeatureExtractor,
)
from dasma_kafka.common.grouping import resolve_group_key  # noqa: E402
from dasma_kafka.explainability.offset_tracker import ContiguousOffsetTracker  # noqa: E402
from dasma_kafka.explainability.q3_trace import Q3TraceRecorder  # noqa: E402
from dasma_kafka.explainability.runtime import (  # noqa: E402
    DeliveryFailureError,
    GroupWorkItem,
    WorkerMetricsAggregator,
    build_worker_config_dict,
    create_consumer,
    create_schema_registry_client,
    drain_delivery_acks,
    drain_worker_metrics,
    ensure_workers_alive,
    group_worker_main,
    incomplete_offset_partitions,
    make_logger,
)

WORKER_DRAIN_TIMEOUT_SECONDS = 30.0
WORKER_DRAIN_POLL_SECONDS = 0.05


def _reject_unprocessable_message(logger: logging.Logger, message: Any, reason: str) -> None:
    """Fail the service explicitly for a data message that cannot be tracked.

    The offset is neither registered in the contiguous offset tracker nor
    committed: Kafka replays it on restart (at-least-once). Never continue to
    the next offset, which would silently skip this one forever.
    """
    logger.critical(
        "Unprocessable Kafka message on topic=%s partition=%s offset=%s: %s; "
        "failing the service without commit (Kafka will replay it on restart)",
        message.topic(),
        message.partition(),
        message.offset(),
        reason,
    )
    raise DeliveryFailureError(
        f"unprocessable message partition={message.partition()} "
        f"offset={message.offset()}: {reason}"
    )


def main() -> None:
    """Orchestrate the service without holding the calculation details."""
    if os.getenv("DASMA_ENABLED", "1") == "0":
        logging.info("DASMA_ENABLED=0 — service disabled for this campaign. Idling.")
        signal.pause()
        return
    logger = make_logger()
    logging.getLogger("shap").setLevel(logging.WARNING)

    logger.info("=" * 60)
    logger.info("Starting the Explainability service with KernelSHAP")
    logger.info("=" * 60)

    config = ExplainabilityConfig.from_environment()
    validate_explainability_selection(config)
    trace = Q3TraceRecorder(os.getenv("DASMA_Q3_TRACE_SERVICE", "explainability-v1"))

    if not config.dest_topic_json:
        # Both outputs are mandatory (V1-03): never degrade silently to
        # Avro-only. Fail explicitly before starting to consume.
        logger.critical(
            "KAFKA_DEST_TOPIC_JSON is empty: explainability requires both "
            "Avro and JSON outputs. Refusing to start."
        )
        raise RuntimeError(
            "KAFKA_DEST_TOPIC_JSON is empty: both Avro and JSON outputs are mandatory"
        )

    logger.info("Configuration:")
    logger.info("  BOOTSTRAP_SERVERS     = %s", config.bootstrap_servers)
    logger.info("  KAFKA_SOURCE_TOPIC    = %s", config.source_topic)
    logger.info("  KAFKA_DEST_TOPIC      = %s", config.dest_topic)
    logger.info("  KAFKA_DEST_TOPIC_JSON = %s", config.dest_topic_json)
    logger.info("  SCHEMA_REGISTRY_URL   = %s", config.schema_registry_url)
    logger.info("  GROUP_ID              = %s", config.group_id)
    logger.info("  HSTREE_WINDOW_SIZE    = %s", config.hstree_window_size)
    logger.info("  HSTREE_NUM_TREES      = %s", config.hstree_num_trees)
    logger.info("  HSTREE_MAX_DEPTH      = %s", config.hstree_max_depth)
    logger.info("  HSTREE_INITIAL_WINDOW = %s", config.hstree_initial_window_size)
    logger.info("  SHAP_BACKGROUND_SIZE  = %s", config.shap_background_size)
    logger.info("  SHAP_NSAMPLES         = %s", config.shap_nsamples)
    logger.info("  SHAP_ENABLED          = %s", config.shap_enabled)
    logger.info("  EXPLAINABILITY_SEED   = %s", config.explainability_seed)

    np.random.seed(config.explainability_seed)
    random.seed(config.explainability_seed)
    logger.info(
        "Reproducibility enabled (EXPLAINABILITY_SEED=%s)",
        config.explainability_seed,
    )

    schema_registry_client = create_schema_registry_client(
        logger,
        config.schema_registry_url,
    )
    value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client)
    consumer = create_consumer(logger, config.bootstrap_servers, config.group_id)
    if config.explained_model == "iforest":
        feature_extractor = IForestFeatureExtractor(logger)
    elif config.explained_model == "kitnet":
        feature_extractor = KitNetFeatureExtractor(logger)
    elif config.explained_model == "hstree":
        feature_extractor = FeatureExtractor(logger)
    else:
        raise ValueError(f"Unsupported explainability model: {config.explained_model!r}")

    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        pass

    group_workers: Dict[str, mp.Process] = {}
    group_queues: Dict[str, mp.Queue] = {}
    worker_config = build_worker_config_dict(config)

    # Worker -> main process metrics (V1-02): started/completed events drained
    # without blocking in the consumption loop.
    metrics_queue: mp.Queue = mp.Queue()
    metrics = WorkerMetricsAggregator()

    # Worker -> main process delivery acks (V1-03): separate channel from the
    # metrics; feeds the contiguous offset tracker driving manual commits.
    delivery_ack_queue: mp.Queue = mp.Queue()
    offset_tracker = ContiguousOffsetTracker()

    def ensure_group_worker(group_name: str) -> None:
        if group_name in group_workers or not feature_extractor.is_initialized:
            return

        queue: mp.Queue = mp.Queue(maxsize=1000)
        process = mp.Process(
            target=group_worker_main,
            args=(
                group_name,
                worker_config,
                feature_extractor.get_feature_names(),
                queue,
                metrics_queue,
                delivery_ack_queue,
            ),
            daemon=True,
        )
        process.start()
        group_workers[group_name] = process
        group_queues[group_name] = queue
        logger.info("Worker started for group '%s'", group_name)

    consumer.subscribe([config.source_topic])
    logger.info("Subscribed to topic '%s'", config.source_topic)
    logger.info("Waiting for messages...")

    message_count = 0
    no_msg_count = 0
    normal_shutdown = False
    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def request_normal_shutdown(signum: int, _frame: Any) -> None:
        logger.info("Stop requested (signal %s)", signum)
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, request_normal_shutdown)

    try:
        while True:
            # Non-blocking drains (V1-02 metrics, V1-03 delivery acks) and
            # worker supervision before each new admission.
            ensure_workers_alive(group_workers)
            drain_worker_metrics(metrics_queue, metrics, logger)
            drain_delivery_acks(
                delivery_ack_queue, offset_tracker, consumer, config.source_topic, logger, trace
            )

            try:
                message = consumer.poll(timeout=1.0)
            except KafkaException as exc:
                # Ambiguous transport-level failure: no record can be trusted,
                # never continue silently (V1-03).
                logger.critical("Kafka poll failed: %s; failing the service", exc)
                raise DeliveryFailureError(f"Kafka poll failed: {exc}") from exc

            if message is None:
                no_msg_count += 1
                if no_msg_count % 10 == 0:
                    logger.info("Waiting for messages... (processed total: %s)", message_count)
                continue

            kafka_error = message.error()
            if kafka_error is not None:
                if kafka_error.code() == KafkaError._PARTITION_EOF:
                    # Explicitly not a record: no registration, no commit.
                    logger.info("Partition EOF reached (%s); continuing", kafka_error)
                    continue
                # Any other Kafka error may carry a record: treat as fatal.
                _reject_unprocessable_message(
                    logger, message, f"kafka consumption error: {kafka_error}"
                )

            no_msg_count = 0
            message_count += 1

            key_bytes = message.key()
            value_bytes = message.value()
            if value_bytes is None:
                _reject_unprocessable_message(logger, message, "message without a value")

            try:
                context = SerializationContext(config.source_topic, MessageField.VALUE)
                record = value_deserializer(value_bytes, context)
            except Exception as exc:
                _reject_unprocessable_message(
                    logger, message, f"Avro deserialization failed: {exc}"
                )

            if not feature_extractor.is_initialized:
                feature_extractor.initialize_from_record(record)

            group = resolve_group_key(record)
            ensure_group_worker(group)

            if group not in group_queues:
                _reject_unprocessable_message(
                    logger, message, f"no worker queue available for group '{group}'"
                )

            try:
                key_str = key_bytes.decode("utf-8") if key_bytes else None
                work_item = GroupWorkItem(
                    key_str=key_str,
                    record=record,
                    topic=message.topic(),
                    partition=int(message.partition()),
                    offset=int(message.offset()),
                    receive_seq=message_count,
                    dispatch_seq=message_count,
                    consumer_process_instance=trace.process_instance,
                )
            except Exception as exc:
                _reject_unprocessable_message(
                    logger, message, f"work item construction failed: {exc}"
                )

            enqueue_attempts = 0
            while True:
                try:
                    group_queues[group].put(work_item, timeout=1.0)
                    # Counted only after a successful put (V1-02): a refused
                    # item is not in the worker queue.
                    metrics.record_enqueued(group)
                    # V1-03: registered only once the worker queue accepted
                    # the item, in partition order; the tracker refuses gaps.
                    offset_tracker.register_received(
                        partition=int(message.partition()),
                        offset=int(message.offset()),
                    )
                    if enqueue_attempts:
                        logger.info(
                            "Enqueued delayed message for group '%s' after %s backpressure wait(s)",
                            group,
                            enqueue_attempts,
                        )
                    break
                except Full:
                    enqueue_attempts += 1
                    if enqueue_attempts == 1 or enqueue_attempts % 30 == 0:
                        logger.warning(
                            "Backpressure for group '%s': worker queue is full; waiting instead of dropping message "
                            "(partition=%s, offset=%s, waits=%s)",
                            group,
                            int(message.partition()),
                            int(message.offset()),
                            enqueue_attempts,
                        )
                except Exception as exc:
                    logger.critical(
                        "Unable to enqueue message for group '%s' (partition=%s, offset=%s): %s; "
                        "failing the service without commit (Kafka will replay it on restart)",
                        group,
                        int(message.partition()),
                        int(message.offset()),
                        exc,
                    )
                    raise DeliveryFailureError(
                        f"unable to enqueue message for group '{group}' "
                        f"(partition={int(message.partition())}, offset={int(message.offset())})"
                    ) from exc

    except KeyboardInterrupt:
        normal_shutdown = True
        logger.info("Stop requested (SIGINT)")

    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)
        logger.info("=" * 60)
        logger.info("Final statistics:")
        logger.info("  Processed messages: %s", message_count)
        logger.info("=" * 60)

        try:
            # Metrics and acks emitted before the workers stop must reach the
            # aggregator and the tracker before the final bilan (V1-02/V1-03).
            drain_worker_metrics(metrics_queue, metrics, logger)
            drain_delivery_acks(
                delivery_ack_queue, offset_tracker, consumer, config.source_topic, logger, trace
            )

            logger.info("Stopping group workers...")
            for queue in group_queues.values():
                try:
                    queue.put(None, timeout=1.0)
                except Exception:
                    pass

            # Bounded drain: join the workers while continuing to collect the
            # available metrics and acks, until nothing is alive or the
            # deadline expires (V1-03).
            drain_deadline = time.monotonic() + WORKER_DRAIN_TIMEOUT_SECONDS
            while True:
                drain_worker_metrics(metrics_queue, metrics, logger)
                drain_delivery_acks(
                    delivery_ack_queue,
                    offset_tracker,
                    consumer,
                    config.source_topic,
                    logger,
                    trace,
                )
                alive = []
                for group_name, process in group_workers.items():
                    try:
                        process.join(timeout=0)
                    except Exception:
                        pass
                    if process.is_alive():
                        alive.append(group_name)
                    elif process.exitcode not in (0, None):
                        raise DeliveryFailureError(
                            f"worker died during drain for group '{group_name}' "
                            f"(exitcode={process.exitcode})"
                        )
                if not alive:
                    break
                if time.monotonic() >= drain_deadline:
                    raise DeliveryFailureError(
                        f"worker drain timed out; still alive: {sorted(alive)}"
                    )
                time.sleep(WORKER_DRAIN_POLL_SECONDS)

            # Final drains after the workers stopped and joined (V1-02/V1-03).
            drain_worker_metrics(metrics_queue, metrics, logger)
            drain_delivery_acks(
                delivery_ack_queue, offset_tracker, consumer, config.source_topic, logger, trace
            )

            # A normal end is claimed only when the tracker holds no pending
            # offset and every necessary commit succeeded (V1-03).
            if normal_shutdown:
                incomplete = incomplete_offset_partitions(offset_tracker)
                if incomplete:
                    raise DeliveryFailureError(
                        f"normal drain left incomplete offsets: {incomplete}"
                    )

            computed_total = metrics.total_completed_for_status("computed")
            logger.info("  Computed SHAP explanations: %s", computed_total)
            for group_name, counters in sorted(metrics.snapshot().items()):
                logger.info(
                    "  Group '%s': enqueued=%s started=%s completed=%s queue_depth=%s in_flight=%s statuses=%s",
                    group_name,
                    counters["enqueued"],
                    counters["started"],
                    counters["completed"],
                    counters["queue_depth"],
                    counters["in_flight"],
                    counters["status_counts"],
                )
        finally:
            trace.close()
            logger.info("Closing consumer...")
            consumer.close()
            logger.info("Service stopped")


if __name__ == "__main__":
    main()
