#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Entry point for the explainability-v2 service.

This is the isolated second explainability model. It is a fine copy of the regular
explainability entry point, wired to the `explainability_v2` package so that the
regular service is never changed. v2 has its own Kafka topics
(`explainability-v2` / `explainability-v2-viz`), consumer group
(`explainability-v2-consumer`), and dashboard.

This file is intentionally kept easy to read:
- it loads the configuration
- it consumes `hstree-analysis`
- it routes messages to one worker per group

The more technical logic lives in `src/dasma_kafka/explainability_v2/`.
"""

import logging
import multiprocessing as mp
import os
import random
import signal
import sys
import time
from pathlib import Path
from queue import Empty
from queue import Full
from typing import Any, Dict

import numpy as np
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

ROOT_DIR = Path(__file__).resolve().parents[3]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.explainability_v2.config import ExplainabilityV2Config, validate_explainability_selection  # noqa: E402
from dasma_kafka.explainability_v2.features import (  # noqa: E402
    FeatureExtractor,
    IForestFeatureExtractor,
    KitNetFeatureExtractor,
)
from dasma_kafka.common.grouping import resolve_group_key  # noqa: E402
from dasma_kafka.explainability.q3_trace import Q3TraceRecorder  # noqa: E402
from dasma_kafka.explainability_v2.runtime import (  # noqa: E402
    ContiguousOffsetTracker,
    DeferredDispatchBuffer,
    DeferredDispatchError,
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


# The worker queue itself is bounded to 1000 items.  This separate bound also
# limits records already returned by Kafka but not yet accepted by a worker.
DEFERRED_BUFFER_PER_PARTITION_LIMIT = 1000
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
    logger.info("Starting the Explainability-v2 service with KernelSHAP")
    logger.info("=" * 60)

    config = ExplainabilityV2Config.from_environment()
    validate_explainability_selection(config)
    trace = Q3TraceRecorder(os.getenv("DASMA_Q3_TRACE_SERVICE", "explainability-v2"))

    if not config.dest_topic_json:
        # Both outputs are mandatory in v2 (OC-07): never degrade silently to
        # Avro-only. Fail explicitly before starting to consume.
        logger.critical(
            "KAFKA_DEST_TOPIC_JSON is empty: explainability-v2 requires both "
            "Avro and JSON outputs. Refusing to start."
        )
        sys.exit(1)

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

    # Worker -> main process metrics (OC-04): started/completed events drained
    # without blocking in the consumption loop.
    metrics_queue: mp.Queue = mp.Queue()
    metrics = WorkerMetricsAggregator()

    # Worker -> main process delivery acks (OC-07): separate channel from the
    # metrics; feeds the contiguous offset tracker driving manual commits.
    delivery_ack_queue: mp.Queue = mp.Queue()
    offset_tracker = ContiguousOffsetTracker()
    deferred = DeferredDispatchBuffer(DEFERRED_BUFFER_PER_PARTITION_LIMIT)
    dispatch_seq = 0
    paused_partitions: set[int] = set()

    def _topic_partition(partition: int) -> TopicPartition:
        return TopicPartition(config.source_topic, partition)

    def pause_partition(partition: int) -> None:
        if partition in paused_partitions:
            return
        try:
            consumer.pause([_topic_partition(partition)])
        except Exception as exc:
            raise DeliveryFailureError(
                f"unable to pause partition {partition}: {exc}"
            ) from exc
        paused_partitions.add(partition)
        logger.warning("Paused partition %s due to worker backpressure", partition)

    def resume_partition(partition: int) -> None:
        if partition not in paused_partitions:
            return
        try:
            consumer.resume([_topic_partition(partition)])
        except Exception as exc:
            raise DeliveryFailureError(
                f"unable to resume partition {partition}: {exc}"
            ) from exc
        paused_partitions.remove(partition)
        logger.info("Resumed partition %s after backpressure", partition)

    def accept_for_worker(group: str, work_item: GroupWorkItem) -> bool:
        """Try a non-blocking admission; track only after worker acceptance."""
        nonlocal dispatch_seq
        work_item.dispatch_seq = dispatch_seq + 1
        try:
            group_queues[group].put_nowait(work_item)
        except Full:
            work_item.dispatch_seq = None
            return False
        except Exception as exc:
            raise DeliveryFailureError(
                f"unable to enqueue partition={work_item.partition} offset={work_item.offset}: {exc}"
            ) from exc
        dispatch_seq += 1
        metrics.record_enqueued(group)
        offset_tracker.register_received(
            partition=int(work_item.partition), offset=int(work_item.offset)
        )
        return True

    def pump_deferred(*, resume_when_empty: bool) -> bool:
        """Move FIFO deferred records into workers whenever capacity returns."""
        progressed = False
        for partition in deferred.partitions():
            pending = deferred.peek(partition)
            if pending is None:
                continue
            if accept_for_worker(pending.group, pending.work_item):
                deferred.pop(partition)
                progressed = True
                if resume_when_empty and deferred.peek(partition) is None:
                    resume_partition(partition)
        return progressed

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
            # Regular non-blocking drains (OC-04 metrics, OC-07 delivery acks).
            drain_worker_metrics(metrics_queue, metrics, logger)
            drain_delivery_acks(
                delivery_ack_queue, offset_tracker, consumer, config.source_topic, logger, trace
            )
            ensure_workers_alive(group_workers)
            pump_deferred(resume_when_empty=True)

            try:
                # Keep polling under backpressure so the consumer group remains
                # healthy. Paused partitions stop fetching; any prefetched
                # records are retained in ``deferred`` below, never discarded.
                message = consumer.poll(timeout=WORKER_DRAIN_POLL_SECONDS)
            except KafkaException as exc:
                # Ambiguous transport-level failure: no record can be trusted,
                # never continue silently (OC-07 hardening).
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
                    consumer_process_instance=trace.process_instance,
                )
            except Exception as exc:
                _reject_unprocessable_message(
                    logger, message, f"work item construction failed: {exc}"
                )

            partition = int(message.partition())
            # Do not overtake a record already deferred from the same partition:
            # Kafka offsets must enter the tracker in partition order.
            if deferred.peek(partition) is not None or not accept_for_worker(group, work_item):
                # Backpressure: waiting instead of dropping message. Keep the
                # accepted record in the per-partition FIFO until a worker can
                # take it, while the Kafka partition is paused.
                try:
                    deferred.add(group, work_item)
                except DeferredDispatchError as exc:
                    raise DeliveryFailureError(
                        f"unable to defer partition={partition} offset={work_item.offset}: {exc}"
                    ) from exc
                pause_partition(partition)

    except KeyboardInterrupt:
        normal_shutdown = True
        logger.info("Stop requested (SIGINT)")

    finally:
        signal.signal(signal.SIGTERM, previous_sigterm_handler)
        drain_deadline = time.monotonic() + WORKER_DRAIN_TIMEOUT_SECONDS

        if normal_shutdown:
            # Stop new fetches before draining records already accepted by the
            # main process. A deferred record is not abandoned: it must first
            # enter its worker queue and therefore the offset tracker.
            try:
                assignments = consumer.assignment()
                if assignments:
                    consumer.pause(assignments)
            except Exception as exc:
                raise DeliveryFailureError(f"unable to pause consumer for drain: {exc}") from exc

            while not deferred.is_empty():
                drain_worker_metrics(metrics_queue, metrics, logger)
                drain_delivery_acks(
                    delivery_ack_queue, offset_tracker, consumer, config.source_topic, logger, trace
                )
                ensure_workers_alive(group_workers)
                pump_deferred(resume_when_empty=False)
                if deferred.is_empty():
                    break
                if time.monotonic() >= drain_deadline:
                    raise DeliveryFailureError(
                        f"drain timed out with deferred records: {deferred.snapshot()}"
                    )
                time.sleep(WORKER_DRAIN_POLL_SECONDS)

        logger.info("Stopping group workers...")
        for queue in group_queues.values():
            try:
                queue.put(None, timeout=1.0)
            except Exception:
                pass

        while True:
            drain_worker_metrics(metrics_queue, metrics, logger)
            drain_delivery_acks(
                delivery_ack_queue, offset_tracker, consumer, config.source_topic, logger, trace
            )
            alive = []
            for group_name, process in group_workers.items():
                try:
                    process.join(timeout=0)
                except Exception:
                    pass
                if process.is_alive():
                    alive.append(group_name)
                elif normal_shutdown and process.exitcode not in (0, None):
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

        # Final drains after the workers stopped and joined: metrics (OC-04),
        # then delivery acks (OC-07) so already-confirmed offsets are committed
        # before the consumer closes.
        drain_worker_metrics(metrics_queue, metrics, logger)
        drain_delivery_acks(
            delivery_ack_queue, offset_tracker, consumer, config.source_topic, logger, trace
        )
        trace.close()

        if normal_shutdown:
            incomplete = incomplete_offset_partitions(offset_tracker)
            if incomplete:
                raise DeliveryFailureError(
                    f"normal drain left incomplete offsets: {incomplete}"
                )

        computed_total = metrics.total_completed_for_status("computed")
        logger.info("=" * 60)
        logger.info("Final statistics:")
        logger.info("  Processed messages: %s", message_count)
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
        logger.info("=" * 60)

        logger.info("Closing consumer...")
        consumer.close()
        logger.info("Service stopped")


if __name__ == "__main__":
    main()
