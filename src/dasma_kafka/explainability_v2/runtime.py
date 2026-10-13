"""Kafka orchestration and workers for the explainability-v2 service.

Fine copy of the regular explainability runtime. Identical behavior; kept in the
v2 package so v2 stays isolated from regular. Only the v2 identity differs (config
type, Avro schema constant, and logger name).
"""

from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import random
import sys
import time
from contextlib import nullcontext
from collections import deque
from dataclasses import asdict, dataclass, field
from queue import Empty
from typing import Any, Dict, List, Optional

import numpy as np
from confluent_kafka import Consumer, SerializingProducer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from dasma_kafka.common import isolated_rng, local_score_error, score_fidelity_error

from .config import (
    EXPLAINABILITY_V2_VALUE_SCHEMA_STR,
    EXPLANATION_PROFILE,
    EXPLANATION_STATUSES,
    HSTREE_MODEL_VERSION,
    ExplainabilityV2Config,
    validate_explainability_selection,
)
from .features import (
    FeatureExtractor,
    IForestFeatureExtractor,
    KitNetFeatureExtractor,
    build_output_record,
    build_viz_json_payload,
    compute_feature_fingerprint,
    compute_hstree_model_fingerprint,
)
from .model import HSTreeModelManager
from dasma_kafka.common.explained_models import IForestModelManager, KitNetModelManager
from .offset_tracker import ContiguousOffsetTracker
from .shap_engine import SHAPExplainer, seed_for_message
from dasma_kafka.explainability.q3_trace import Q3TraceRecorder, initialization_fields

# Bounded deadline for waiting the two delivery callbacks of one message
# (Avro + JSON). The idempotent producers keep their internal retries; past
# this deadline the message is reported as undelivered (no commit) and the
# campaign fails via a negative ack.
DELIVERY_TIMEOUT_SECONDS = 30.0
DELIVERY_POLL_INTERVAL_SECONDS = 0.01


@dataclass
class GroupWorkItem:
    """Message to be processed by a group's worker."""

    key_str: Optional[str]
    record: Dict[str, Any]
    topic: str
    partition: int
    offset: int
    receive_seq: int = 0
    dispatch_seq: Optional[int] = None
    consumer_process_instance: Optional[str] = None


class DeferredDispatchError(RuntimeError):
    """A main-process backpressure buffer cannot safely accept more work."""


@dataclass(frozen=True)
class DeferredWorkItem:
    """One source record retained by the main process during backpressure."""

    group: str
    work_item: GroupWorkItem


class DeferredDispatchBuffer:
    """Bounded FIFO deferred-dispatch buffer, independently ordered per partition.

    An item only enters the offset tracker after it leaves this buffer and is
    accepted by its group's worker queue.  This preserves the invariant that a
    Kafka offset is never considered in-flight before a worker owns it.
    """

    def __init__(self, per_partition_limit: int = 1000) -> None:
        if (
            isinstance(per_partition_limit, bool)
            or not isinstance(per_partition_limit, int)
            or per_partition_limit <= 0
        ):
            raise ValueError("per_partition_limit must be a positive integer")
        self._limit = per_partition_limit
        self._items: Dict[int, deque[DeferredWorkItem]] = {}

    def add(self, group: str, work_item: GroupWorkItem) -> None:
        partition = int(work_item.partition)
        entries = self._items.setdefault(partition, deque())
        if len(entries) >= self._limit:
            raise DeferredDispatchError(
                f"deferred buffer limit reached for partition {partition} "
                f"(limit={self._limit})"
            )
        if entries and work_item.offset != entries[-1].work_item.offset + 1:
            raise DeferredDispatchError(
                f"non-contiguous deferred offset for partition {partition}: "
                f"offset={work_item.offset}, expected={entries[-1].work_item.offset + 1}"
            )
        entries.append(DeferredWorkItem(group=group, work_item=work_item))

    def partitions(self) -> List[int]:
        return sorted(self._items)

    def peek(self, partition: int) -> Optional[DeferredWorkItem]:
        entries = self._items.get(partition)
        return entries[0] if entries else None

    def pop(self, partition: int) -> DeferredWorkItem:
        entries = self._items.get(partition)
        if not entries:
            raise DeferredDispatchError(f"no deferred item for partition {partition}")
        item = entries.popleft()
        if not entries:
            del self._items[partition]
        return item

    def is_empty(self) -> bool:
        return not self._items

    def snapshot(self) -> Dict[int, List[int]]:
        """Read-only view: deferred offsets in FIFO order per partition."""
        return {
            partition: [entry.work_item.offset for entry in entries]
            for partition, entries in sorted(self._items.items())
        }


@dataclass
class WorkerStarted:
    """Worker metric event: an item was taken out of the work queue.

    ``seq`` is the worker-side sequence of started items (1-based); the
    matching completed event, if any, carries the same ``seq``.
    """

    group: str
    seq: int


@dataclass
class WorkerCompleted:
    """Worker metric event: an item finished with an OC-02 status.

    Items skipped before status attribution (feature extraction failure) or
    killed by the identity gate never produce this event. This event measures
    the computation status only: it never means a Kafka delivery is confirmed
    (see ``WorkerDeliveryAck``).
    """

    group: str
    seq: int
    status: str


@dataclass
class WorkerDeliveryAck:
    """Delivery confirmation event (worker -> main process), OC-07.

    Separate channel from the OC-04 metrics: it confirms ONLY that both
    outputs (Avro and JSON) of one source message received a positive delivery
    callback. ``error_code`` is a stable, low-cardinality code (no technical
    detail; the detail stays in the worker logs).
    """

    topic: str
    partition: int
    offset: int
    success: bool
    error_code: Optional[str] = None


class DeliveryFailureError(RuntimeError):
    """A v2 delivery or commit failed: the service must fail explicitly."""


def ensure_workers_alive(
    group_workers: Dict[str, Any], *, allow_clean_exit: bool = False
) -> None:
    """Raise when a group worker has disappeared unexpectedly.

    A successful exit is tolerated only after the main process sent the normal
    shutdown sentinel.  During consumption, even exit code zero means queued
    work could no longer receive delivery acknowledgements.
    """
    for group_name, process in group_workers.items():
        if process.is_alive():
            continue
        exitcode = process.exitcode
        if allow_clean_exit and exitcode == 0:
            continue
        raise DeliveryFailureError(
            f"worker died for group '{group_name}' (exitcode={exitcode})"
        )


def incomplete_offset_partitions(
    offset_tracker: ContiguousOffsetTracker,
) -> Dict[int, Dict[str, object]]:
    """Return accepted offsets that prevent a normal drain from succeeding."""
    return {
        partition: state
        for partition, state in offset_tracker.snapshot().items()
        if state["pending"] or state["succeeded_blocked"] or state["failed"]
    }


@dataclass
class GroupWorkerCounters:
    """Counters of one group; never negative thanks to aggregator guards."""

    enqueued: int = 0
    started: int = 0
    completed: int = 0
    status_counts: Dict[str, int] = field(default_factory=dict)

    @property
    def queue_depth(self) -> int:
        """Items accepted but not yet taken by the worker (backlog)."""
        return self.enqueued - self.started

    @property
    def in_flight(self) -> int:
        """Items taken by the worker but not yet completed."""
        return self.started - self.completed


class WorkerMetricsAggregator:
    """Aggregate worker metric events into per-group counters.

    Pure and pickling-free: the entrypoint feeds it with the drained events,
    tests feed it directly.

    Invariants (a violation is a programming error and raises ``ValueError``,
    it is never silently absorbed):

    - ``started`` seq must be exactly ``last_started_seq + 1``: the started
      stream is dense, a duplicate, an out-of-order event or a gap is rejected;
    - ``started`` never exceeds ``enqueued``;
    - ``completed`` never exceeds ``started``;
    - a ``completed`` seq must have been started (``seq <= last_started_seq``)
      and must be strictly increasing: a duplicate, a replay, an out-of-order
      event or a completion for an item never started is rejected. Gaps in the
      completed sequences are legitimate: items started then skipped before
      status attribution (feature extraction failure) never emit a completed
      event (OC-02 semantics preserved, no crash of the main process);
    - ``status`` must belong to ``EXPLANATION_STATUSES``.

    Under these guards ``queue_depth >= 0`` and ``in_flight >= 0`` always hold,
    and in the nominal scenario
    ``completed == disabled + warmup + background_not_ready + computed + failed``.
    """

    def __init__(self) -> None:
        self.groups: Dict[str, GroupWorkerCounters] = {}
        self._last_started_seq: Dict[str, int] = {}
        self._last_completed_seq: Dict[str, int] = {}

    def _counters_for(self, group: str) -> GroupWorkerCounters:
        if group not in self.groups:
            self.groups[group] = GroupWorkerCounters()
        return self.groups[group]

    def record_enqueued(self, group: str) -> None:
        """Count an item accepted into a worker queue (after a successful put)."""
        self._counters_for(group).enqueued += 1

    def record_started(self, group: str, seq: int) -> None:
        counters = self._counters_for(group)
        expected_seq = self._last_started_seq.get(group, 0) + 1
        if seq != expected_seq:
            raise ValueError(
                f"duplicate, out-of-order or gapped started event for group "
                f"'{group}' (seq={seq}, expected={expected_seq})"
            )
        if counters.started >= counters.enqueued:
            raise ValueError(
                f"started event exceeds enqueued for group '{group}' "
                f"(started={counters.started}, enqueued={counters.enqueued})"
            )
        counters.started += 1
        self._last_started_seq[group] = seq

    def record_completed(self, group: str, seq: int, status: str) -> None:
        if status not in EXPLANATION_STATUSES:
            raise ValueError(f"unknown explanation status: {status!r}")
        counters = self._counters_for(group)
        last_completed_seq = self._last_completed_seq.get(group, 0)
        if seq <= last_completed_seq:
            raise ValueError(
                f"duplicate, out-of-order or replayed completed event for group "
                f"'{group}' (seq={seq}, last_completed={last_completed_seq})"
            )
        last_started_seq = self._last_started_seq.get(group, 0)
        if seq > last_started_seq:
            raise ValueError(
                f"completed event for an item never started for group "
                f"'{group}' (seq={seq}, last_started={last_started_seq})"
            )
        counters.completed += 1
        counters.status_counts[status] = counters.status_counts.get(status, 0) + 1
        self._last_completed_seq[group] = seq

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Return a copy of the per-group counters, with derived depths."""
        return {
            group: {
                "enqueued": counters.enqueued,
                "started": counters.started,
                "completed": counters.completed,
                "queue_depth": counters.queue_depth,
                "in_flight": counters.in_flight,
                "status_counts": dict(counters.status_counts),
            }
            for group, counters in self.groups.items()
        }

    def total_completed_for_status(self, status: str) -> int:
        """Global number of completed items carrying this status."""
        return sum(
            counters.status_counts.get(status, 0)
            for counters in self.groups.values()
        )


def drain_worker_metrics(
    metrics_q: Any,
    aggregator: WorkerMetricsAggregator,
    logger: logging.Logger,
) -> None:
    """Drain the worker metrics queue without blocking the caller.

    Unknown event types are logged and dropped; duplicate or out-of-order
    events raise ``ValueError`` (programming error), by design.
    """
    while True:
        try:
            event = metrics_q.get_nowait()
        except Empty:
            return
        except Exception as exc:
            logger.error("Worker metrics queue error: %s", exc)
            return
        if isinstance(event, WorkerStarted):
            aggregator.record_started(event.group, event.seq)
        elif isinstance(event, WorkerCompleted):
            aggregator.record_completed(event.group, event.seq, event.status)
        else:
            logger.warning("Unknown worker metrics event: %r", event)


def make_logger() -> logging.Logger:
    """Create the main logger for the service."""
    level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        stream=sys.stdout,
    )
    return logging.getLogger("explainability-v2")


def create_schema_registry_client(
    logger: logging.Logger,
    url: str,
) -> SchemaRegistryClient:
    """Build a Schema Registry client."""
    if not url:
        logger.error("SCHEMA_REGISTRY_URL not set")
        sys.exit(1)

    logger.info("Connecting to Schema Registry: %s", url)
    return SchemaRegistryClient({"url": url})


def create_consumer(
    logger: logging.Logger,
    bootstrap_servers: str,
    group_id: str,
) -> Consumer:
    """Build the main Kafka consumer (manual commits: at-least-once, OC-07)."""
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    logger.info("Creating Kafka consumer (group.id=%s, enable.auto.commit=False)", group_id)
    return Consumer(config)


def build_worker_config_dict(config: ExplainabilityV2Config) -> Dict[str, Any]:
    """Convert the dataclass into a plain dict passed to the worker."""
    return asdict(config)


@dataclass
class _DeliveryWaiter:
    """Collect the two delivery callbacks of one message (Avro + JSON)."""

    avro_done: bool = False
    avro_ok: bool = False
    json_done: bool = False
    json_ok: bool = False
    error_code: Optional[str] = None

    def confirm(self, channel: str, ok: bool, error_code: Optional[str]) -> None:
        if channel == "avro":
            self.avro_done, self.avro_ok = True, ok
        else:
            self.json_done, self.json_ok = True, ok
        if not ok and self.error_code is None:
            self.error_code = error_code

    @property
    def complete(self) -> bool:
        return self.avro_done and self.json_done

    @property
    def success(self) -> bool:
        return self.avro_done and self.avro_ok and self.json_done and self.json_ok


def _make_delivery_callback(waiter: _DeliveryWaiter, channel: str):
    def _on_delivery(err, msg) -> None:
        if err is not None:
            waiter.confirm(channel, False, f"{channel}_delivery_failed")
        else:
            waiter.confirm(channel, True, None)

    return _on_delivery


def _wait_for_delivery_confirmations(
    waiter: _DeliveryWaiter,
    producer: Any,
    json_producer: Any,
) -> None:
    """Poll both producers until both callbacks are done or the deadline hits.

    A bare ``produce()`` call is never a confirmation: only callbacks count.
    On timeout the missing channels are marked failed with the stable code
    ``delivery_timeout``.
    """
    deadline = time.monotonic() + DELIVERY_TIMEOUT_SECONDS
    while not waiter.complete:
        producer.poll(0)
        if json_producer is not None:
            json_producer.poll(0)
        if waiter.complete:
            return
        if time.monotonic() >= deadline:
            if not waiter.avro_done:
                waiter.confirm("avro", False, "delivery_timeout")
            if not waiter.json_done:
                waiter.confirm("json", False, "delivery_timeout")
            return
        time.sleep(DELIVERY_POLL_INTERVAL_SECONDS)


def _emit_delivery_ack(
    group: str,
    logger: logging.Logger,
    delivery_q: Any,
    item: GroupWorkItem,
    success: bool,
    error_code: Optional[str],
) -> None:
    """Emit one delivery ack; losing the ack is fatal to the worker."""
    if delivery_q is None:
        return
    try:
        delivery_q.put_nowait(
            WorkerDeliveryAck(
                topic=item.topic,
                partition=int(item.partition),
                offset=int(item.offset),
                success=success,
                error_code=error_code,
            )
        )
    except Exception as exc:
        logger.critical(
            "[%s] Unable to emit delivery ack (partition=%s, offset=%s): %s",
            group,
            item.partition,
            item.offset,
            exc,
        )
        raise RuntimeError(
            f"[{group}] delivery ack emission failed for "
            f"partition={item.partition} offset={item.offset}"
        ) from exc


def drain_delivery_acks(
    delivery_q: Any,
    offset_tracker: ContiguousOffsetTracker,
    consumer: Any,
    source_topic: str,
    logger: logging.Logger,
    trace: Optional[Q3TraceRecorder] = None,
) -> None:
    """Drain worker delivery acks (non-blocking) and commit contiguous offsets.

    - positive ack: ``record_outcome(success=True)``; when the tracker returns
      a new commit value (exclusive), a synchronous Kafka commit is performed
      with ``TopicPartition(source_topic, partition, next_offset)``;
    - negative ack: ``record_outcome(success=False)``, critical log, and the
      service fails explicitly (``DeliveryFailureError``) — never a commit for
      this offset or beyond, never a silent restart;
    - a failed ``consumer.commit`` also fails the service explicitly: no offset
      is considered durably committed without a successful commit call.
    """
    while True:
        try:
            ack = delivery_q.get_nowait()
        except Empty:
            return
        except Exception as exc:
            logger.error("Delivery ack queue error: %s", exc)
            return
        if not isinstance(ack, WorkerDeliveryAck):
            logger.warning("Unknown delivery ack event: %r", ack)
            continue
        if ack.success:
            next_offset = offset_tracker.record_outcome(ack.partition, ack.offset, True)
            if next_offset is not None:
                logger.info(
                    "Committing offset %s (exclusive) for partition %s",
                    next_offset,
                    ack.partition,
                )
                try:
                    if trace is not None and trace.enabled:
                        trace.offset_commit(source_topic, ack.partition, next_offset, "requested")
                    consumer.commit(
                        offsets=[TopicPartition(source_topic, ack.partition, next_offset)],
                        asynchronous=False,
                    )
                    if trace is not None and trace.enabled:
                        trace.offset_commit(source_topic, ack.partition, next_offset, "acknowledged")
                except Exception as exc:
                    if trace is not None and trace.enabled:
                        trace.offset_commit(source_topic, ack.partition, next_offset, "failed")
                    logger.critical(
                        "Kafka commit failed for partition=%s next_offset=%s: %s",
                        ack.partition,
                        next_offset,
                        exc,
                    )
                    raise DeliveryFailureError(
                        f"Kafka commit failed for partition={ack.partition} "
                        f"next_offset={next_offset}"
                    ) from exc
        else:
            offset_tracker.record_outcome(ack.partition, ack.offset, False)
            logger.critical(
                "Delivery failed for partition=%s offset=%s (code=%s); "
                "failing the service without commit",
                ack.partition,
                ack.offset,
                ack.error_code,
            )
            raise DeliveryFailureError(
                f"delivery failed for partition={ack.partition} "
                f"offset={ack.offset} (code={ack.error_code})"
            )


def group_worker_main(
    group: str,
    config_dict: Dict[str, Any],
    feature_names: List[str],
    work_q: "mp.Queue[Optional[GroupWorkItem]]",
    metrics_q: Any = None,
    delivery_q: Any = None,
) -> None:
    """Process a group's messages sequentially in a dedicated process.

    ``metrics_q`` and ``delivery_q`` are optional (preserves existing callers
    and tests). When provided:

    - ``metrics_q`` receives one ``WorkerStarted`` per item taken and one
      ``WorkerCompleted`` per item that received an OC-02 status (computation
      status only, OC-04);
    - ``delivery_q`` receives one ``WorkerDeliveryAck`` per item that reached
      status attribution, confirming the delivery of BOTH outputs (Avro and
      JSON) or reporting the failure (OC-07). A lost ack emission fails the
      worker instead of silently continuing.
    """
    logger = logging.getLogger(f"explainability-v2.worker.{group}")
    trace = Q3TraceRecorder(os.getenv("DASMA_Q3_TRACE_SERVICE", "explainability-v2"))
    level = getattr(logging, str(config_dict.get("log_level", "INFO")).upper(), logging.INFO)
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    if not logger.handlers:
        logger.addHandler(handler)

    config = ExplainabilityV2Config(**config_dict)
    try:
        validate_explainability_selection(config)
    except ValueError as exc:
        logger.critical("[%s] Invalid explainability configuration: %s", group, exc)
        raise ValueError(f"[{group}] Invalid explainability configuration: {exc}") from exc
    if not config.dest_topic_json:
        # Both outputs are mandatory in v2; never degrade silently to Avro-only.
        logger.critical(
            "[%s] KAFKA_DEST_TOPIC_JSON is empty: explainability-v2 requires both "
            "Avro and JSON outputs. Refusing to start.",
            group,
        )
        raise RuntimeError(
            f"[{group}] KAFKA_DEST_TOPIC_JSON is empty: both Avro and JSON "
            f"outputs are mandatory"
        )

    np.random.seed(config.explainability_seed)
    random.seed(config.explainability_seed)

    sr_client = create_schema_registry_client(logger, config.schema_registry_url)
    value_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=EXPLAINABILITY_V2_VALUE_SCHEMA_STR,
    )
    producer = SerializingProducer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": value_serializer,
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    json_producer = SerializingProducer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": StringSerializer("utf_8"),
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    logger.info("[%s] JSON duplication enabled -> topic '%s'", group, config.dest_topic_json)

    initialization_order = 0

    def observe_local_initialization(**state: Any) -> None:
        nonlocal initialization_order
        import importlib.metadata

        initialization_order += 1
        context = state["trace_context"]
        trace.model_initialization(
            **initialization_fields(
                topic=context["topic"], partition=context["partition"], offset=context["offset"],
                group=state["group"], source_event_id=state["source_event_id"],
                initialization_order_seq=initialization_order,
                warmup_event_ids=state["warmup_event_ids"], warmup_values=state["warmup_values"],
                feature_names=feature_names, feature_mins=state["feature_mins"],
                feature_maxes=state["feature_maxes"],
                hstree_parameters={
                    "initial_window_size": config.hstree_initial_window_size,
                    "window_size": config.hstree_window_size,
                    "num_trees": config.hstree_num_trees,
                    "max_depth": config.hstree_max_depth,
                    "configured_seed": config.explainability_seed,
                }, pysad_version=importlib.metadata.version("pysad"),
            )
        )

    if config.explained_model == "iforest":
        model_manager = IForestModelManager(config, logger)
    elif config.explained_model == "kitnet":
        model_manager = KitNetModelManager(config, logger)
    elif config.explained_model == "hstree":
        model_manager = HSTreeModelManager(
            config, logger, initialization_observer=observe_local_initialization if trace.enabled else None
        )
    else:
        raise ValueError(f"Unsupported explainability model: {config.explained_model!r}")
    shap_explainer = SHAPExplainer(config, logger)
    shap_explainer.set_feature_names(feature_names)

    if config.explained_model == "iforest":
        feature_extractor = IForestFeatureExtractor(logger)
    elif config.explained_model == "kitnet":
        feature_extractor = KitNetFeatureExtractor(logger)
    elif config.explained_model == "hstree":
        feature_extractor = FeatureExtractor(logger)
    else:
        raise ValueError(f"Unsupported explainability model: {config.explained_model!r}")
    feature_extractor.feature_names = feature_names
    feature_extractor.is_initialized = True

    # Computation metadata (OC-03): loop-invariant, built once from the
    # configuration and the ordered feature list so every published message
    # (whatever its OC-02 status) can be compared by configuration.
    is_hstree_model = config.explained_model == "hstree"
    hstree_model_version = getattr(model_manager, "model_version", HSTREE_MODEL_VERSION)
    if not isinstance(hstree_model_version, str):
        hstree_model_version = HSTREE_MODEL_VERSION
    hstree_model_fingerprint = (
        compute_hstree_model_fingerprint(
            hstree_model_version=hstree_model_version,
            hstree_window_size=config.hstree_window_size,
            hstree_num_trees=config.hstree_num_trees,
            hstree_max_depth=config.hstree_max_depth,
            hstree_initial_window_size=config.hstree_initial_window_size,
            hstree_seed=config.hstree_seed,
        )
        if is_hstree_model
        else None
    )
    computation_metadata: Dict[str, Any] = {
        "explanation_profile": EXPLANATION_PROFILE,
        "shap_nsamples": config.shap_nsamples if config.explanation_method == "kernel" else None,
        "shap_background_size": config.shap_background_size,
        "explainability_seed": config.explainability_seed if config.explanation_method == "kernel" else None,
        "hstree_model_version": hstree_model_version if is_hstree_model else None,
        "hstree_model_fingerprint": hstree_model_fingerprint,
        "feature_fingerprint": compute_feature_fingerprint(feature_names),
        "explained_model": config.explained_model,
        "explainability_mode": config.explainability_mode,
        "explanation_method": config.explanation_method,
        "model_version": hstree_model_version if is_hstree_model else model_manager.model_version,
        "model_fingerprint": model_manager.model_fingerprint(feature_names),
    }

    processed = 0
    shap_computed = 0
    item_seq = 0
    publish_seq = 0

    def _emit_metrics(event: Any) -> None:
        # Metrics must never block or kill the worker; a lost event leaves the
        # item out of the main-process counters and is logged.
        if metrics_q is None:
            return
        try:
            metrics_q.put_nowait(event)
        except Exception as exc:
            logger.error("[%s] Unable to emit metrics event: %s", group, exc)

    try:
        while True:
            item = work_q.get()
            if item is None:
                break

            processed += 1
            record = item.record
            item_seq += 1
            _emit_metrics(WorkerStarted(group=group, seq=item_seq))

            # Identity gate (OC-02): a v2 message without a valid
            # source_event_id is invalid. Fail fast with an explicit log and a
            # fatal worker error; no timestamp fallback is allowed.
            source_event_id = (
                record.get("source_event_id") if isinstance(record, dict) else None
            )
            if not isinstance(source_event_id, str) or not source_event_id.strip():
                logger.critical(
                    "[%s] Missing or empty source_event_id (partition=%s, offset=%s): %r. "
                    "No timestamp fallback is allowed; stopping worker.",
                    group,
                    item.partition,
                    item.offset,
                    source_event_id,
                )
                raise ValueError(
                    f"[{group}] missing or empty source_event_id "
                    f"(partition={item.partition}, offset={item.offset})"
                )

            try:
                features = feature_extractor.extract(record)
            except Exception as exc:
                logger.error("[%s] Error while extracting features: %s", group, exc)
                # The main process has already registered this source offset.
                # Report the terminal failure so it records the barrier and
                # fails explicitly instead of waiting forever for an ack.
                _emit_delivery_ack(
                    group,
                    logger,
                    delivery_q,
                    item,
                    False,
                    "feature_extraction_error",
                )
                continue

            if trace.enabled:
                local_score = model_manager.fit_score(
                    group, features, source_event_id=source_event_id,
                    trace_context={"topic": item.topic, "partition": item.partition, "offset": item.offset},
                )
            else:
                local_score = model_manager.fit_score(group, features)
            shap_result = {
                "shap_values": None,
                "shap_base_value": None,
                "shap_computation_time_ms": None,
            }
            explanation_status: Optional[str] = None
            explanation_error_code: Optional[str] = None

            # Status attribution (OC-02). SHAP stays exhaustive: no branch
            # filters on hstree_anomaly. The eligibility chain below preserves
            # the historical condition: shap_enabled and local score available
            # and background and model available.
            input_error_method = getattr(model_manager, "input_error", None)
            input_error_result = (
                input_error_method(group) if callable(input_error_method) else None
            )
            input_error_code = (
                input_error_result.strip()
                if isinstance(input_error_result, str) and input_error_result.strip()
                else None
            )
            if not config.shap_enabled:
                explanation_status = "disabled"
            elif input_error_code:
                explanation_status = "failed"
                explanation_error_code = str(input_error_code)
            elif local_score is None:
                explanation_status = "warmup"
            else:
                reference_score_field = model_manager.reference_score_field
                mismatch_code = model_manager.mismatch_code
                if config.explainability_mode == "reference":
                    fidelity_error = score_fidelity_error(
                        record.get(reference_score_field),
                        local_score,
                        mismatch_code=mismatch_code,
                    )
                else:
                    fidelity_error = local_score_error(local_score)
                if fidelity_error is not None:
                    explanation_status = (
                        "model_mismatch"
                        if fidelity_error == mismatch_code
                        else "failed"
                    )
                    explanation_error_code = fidelity_error
                    logger.error(
                        "[%s] HSTree score fidelity check failed: upstream=%r local=%r code=%s",
                        group,
                        record.get("hstree_score"),
                        local_score,
                        fidelity_error,
                    )
                else:
                    background_data = model_manager.get_background_data(group)
                    model = model_manager.get_model(group)
                    predict_fn = None
                    if model is not None:
                        try:
                            predict_fn = model_manager.predict_fn(group)
                        except ValueError:
                            predict_fn = None
                    if background_data is None or predict_fn is None:
                        explanation_status = "background_not_ready"
                    else:
                        message_seed = seed_for_message(
                            config.explainability_seed,
                            group,
                            int(item.partition),
                            int(item.offset),
                        ) if config.explanation_method == "kernel" else None
                        with (isolated_rng(message_seed) if message_seed is not None else nullcontext()):
                            try:
                                if config.explanation_method == "tree":
                                    shap_result = shap_explainer.compute_tree_values(
                                        model_manager, group, features, background_data,
                                    )
                                else:
                                    shap_result = shap_explainer.compute_shap_values(
                                        predict_fn=predict_fn,
                                        features=features,
                                        background_data=background_data,
                                    )
                            except Exception as exc:
                                # The technical detail stays in the logs; the
                                # published message only carries the generic error
                                # code. The RNG context restores both global states.
                                logger.error(
                                    "[%s] SHAP computation failed unexpectedly: %s",
                                    group,
                                    exc,
                                )
                                shap_result = {
                                    "shap_values": None,
                                    "shap_base_value": None,
                                    "shap_computation_time_ms": None,
                                }

                        if shap_result.get("shap_values") is not None:
                            shap_computed += 1
                            explanation_status = "computed"
                        else:
                            explanation_status = "failed"
                            explanation_error_code = "shap_exception"

            enriched_record = build_output_record(
                record=record,
                group=group,
                feature_names=feature_names,
                shap_result=shap_result,
                local_score=local_score,
                explanation_status=explanation_status,
                explanation_error_code=explanation_error_code,
                **computation_metadata,
            )

            out_display = {
                "Group": group,
                "hstree_score": record.get("hstree_score"),
                "local_score": local_score,
                "explanation_status": explanation_status,
                "shap_computed": shap_result.get("shap_values") is not None,
                "shap_time_ms": shap_result.get("shap_computation_time_ms"),
            }
            print(json.dumps(out_display, ensure_ascii=False), flush=True)

            if shap_result.get("shap_values"):
                shap_values = shap_result["shap_values"]
                sorted_shap = sorted(
                    shap_values.items(),
                    key=lambda item: abs(item[1]),
                    reverse=True,
                )
                top_5 = sorted_shap[:5]
                top_5_display = {key: round(value, 2) for key, value in top_5 if value != 0}
                if top_5_display:
                    print(
                        f"  -> Top contributors: {json.dumps(top_5_display)}",
                        flush=True,
                    )

            # --- Publish both outputs and wait for their callbacks (OC-07).
            # A bare produce() is never a confirmation: only the Avro and JSON
            # delivery callbacks count. The idempotent producers keep their
            # internal retries; no local manual retry is attempted (OC-07
            # policy). A failed produce, a failed callback or a timeout yields
            # a negative ack; the detail stays in the logs.
            waiter = _DeliveryWaiter()
            try:
                producer.produce(
                    topic=config.dest_topic,
                    key=item.key_str,
                    value=enriched_record,
                    on_delivery=_make_delivery_callback(waiter, "avro"),
                )
            except Exception as exc:
                waiter.confirm("avro", False, "avro_produce_error")
                logger.error(
                    "[%s] Avro produce error (partition=%s, offset=%s): %s",
                    group,
                    item.partition,
                    item.offset,
                    exc,
                )
            try:
                json_payload = build_viz_json_payload(enriched_record)
                json_producer.produce(
                    topic=config.dest_topic_json,
                    key=item.key_str,
                    value=json_payload,
                    on_delivery=_make_delivery_callback(waiter, "json"),
                )
            except Exception as exc:
                waiter.confirm("json", False, "json_produce_error")
                logger.error(
                    "[%s] JSON produce error (partition=%s, offset=%s): %s",
                    group,
                    item.partition,
                    item.offset,
                    exc,
                )

            _wait_for_delivery_confirmations(waiter, producer, json_producer)

            if waiter.success:
                ack_error_code = None
            else:
                ack_error_code = waiter.error_code or "delivery_failed"
                logger.error(
                    "[%s] Delivery failed for partition=%s offset=%s: %s",
                    group,
                    item.partition,
                    item.offset,
                    ack_error_code,
                )

            _emit_delivery_ack(
                group,
                logger,
                delivery_q,
                item,
                waiter.success,
                ack_error_code,
            )

            # One completed event per item that reached status attribution,
            # emitted after the publication attempt (OC-04). It measures the
            # computation status; the delivery confirmation travels on the
            # separate WorkerDeliveryAck channel (OC-07).
            _emit_metrics(
                WorkerCompleted(group=group, seq=item_seq, status=explanation_status)
            )
            if trace.enabled:
                source_event_id = record.get("source_event_id")
            else:
                source_event_id = None
            if trace.enabled and isinstance(source_event_id, str) and source_event_id.strip():
                published = []
                if waiter.avro_done and waiter.avro_ok:
                    publish_seq += 1
                    published.append({"topic": config.dest_topic, "process_instance": trace.process_instance, "publish_seq": publish_seq})
                if waiter.json_done and waiter.json_ok:
                    publish_seq += 1
                    published.append({"topic": config.dest_topic_json, "process_instance": trace.process_instance, "publish_seq": publish_seq})
                trace.message_lifecycle(
                    topic=item.topic, partition=item.partition, offset=item.offset,
                    group=group, source_event_id=source_event_id,
                    consumer_process_instance=item.consumer_process_instance,
                    receive_seq=item.receive_seq or item_seq, dispatch_seq=item.dispatch_seq,
                    worker_id=trace.process_instance, group_seq=item_seq,
                    start_seq=item_seq, complete_seq=item_seq,
                    published_outputs=published, output_status=explanation_status,
                )

    finally:
        logger.info(
            "[%s] Worker stopped. Processed messages=%s, SHAP computed=%s",
            group,
            processed,
            shap_computed,
        )
        try:
            producer.flush(10)
        except Exception:
            pass

        if json_producer is not None:
            try:
                json_producer.flush(10)
            except Exception:
                pass
        trace.close()
