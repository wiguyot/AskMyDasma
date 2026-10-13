"""Per-group worker runtime for the parallel-by-group DeepAnT service.

Mirrors the structure of ``src/dasma_kafka/explainability/runtime.py``: the main
process owns Kafka consumption, feature inference and offset commit, while each
logical group is processed sequentially in its own worker process. Unlike the
explainability service, DeepAnT preserves a strict publish-before-commit
guarantee: each worker waits for its Avro + JSON deliveries and reports the
outcome on an ack queue, so the main process only commits offsets whose workers
confirmed a successful publish (see ``offset_tracker.CommitOffsetTracker``).

torch, the DeepAnT model and confluent_kafka are imported lazily inside the
worker so this module stays importable (and unit-testable) without torch.
"""

from __future__ import annotations

import hashlib
import json
import logging
import multiprocessing as mp
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from queue import Empty as QueueEmpty, Full as QueueFull
from typing import Any, Dict, List, Optional

from confluent_kafka import TopicPartition
from confluent_kafka.error import ConsumeError

# torch-free helpers: safe to import at module load.
from dasma_kafka.common.timestamps import resolve_timestamp_value
from dasma_kafka.deepant_analysis.helpers import (
    build_feature_vector,
    create_viz_state,
    ensure_deepant_analysis_fields,
    enrich_viz_record,
    infer_feature_names,
    resolve_group_key,
    strip_excluded_fields,
)
from dasma_kafka.deepant_analysis.offset_tracker import CommitOffsetTracker


@dataclass
class DeepAntWorkItem:
    """A single Kafka message dispatched to a pool worker."""

    key_str: Optional[str]
    record: Dict[str, Any]
    topic: str
    partition: int
    offset: int
    timestamp_ms: Optional[int]
    group: str = ""


@dataclass
class _GroupState:
    """Per-group model and threshold state held inside a pool worker.

    A pool worker may own several groups (assigned by stable hash), so the
    DeepAnT model, threshold estimator and viz state cannot be plain locals —
    they are kept here, one instance per group the worker has seen.
    """

    model: Optional[Any] = None
    welford: Optional[Any] = None
    percentile_estimator: Optional[Any] = None
    viz_state: Optional[Dict[str, Any]] = None


@dataclass
class DeepAntAck:
    """Result reported by a worker after attempting to publish a message.

    ``ok=True`` means both the Avro and JSON deliveries succeeded and the main
    process may advance the commit watermark past this offset. ``ok=False`` means
    the message must not be committed (it will be reprocessed on restart).
    """

    topic: str
    partition: int
    offset: int
    ok: bool


@dataclass
class DeepAntWorkerConfig:
    """Picklable configuration handed to each worker process."""

    bootstrap_servers: str
    schema_registry_url: str
    schema_registry_basic_auth_user_info: Optional[str]
    schema_registry_basic_auth_source: str
    dest_topic: str
    dest_viz_topic: str
    dest_avro_schema: str
    seed: int
    log_level: str
    window_size: int
    exclude_fields: List[str]
    threshold_mode: str
    threshold_percentile: float
    threshold_window: int
    welford_window: int
    anomaly_k_sigma: float
    anomaly_warmup_window: int
    exclude_anomalies_from_refit: bool
    cpu_threads: int
    # DeepAnT model hyperparameters.
    lookback_size: int
    initial_window_size: int
    refit_every: int
    deepant_window_size: int
    filters: int
    kernel_size: int
    dropout: float
    lr: float
    epochs_initial: int
    epochs_refit: int
    batch_size: int


def _worker_index(group: str, pool_size: int) -> int:
    """Map a group to a worker slot with a stable, process-independent hash.

    Python's built-in ``hash`` is salted per process, so it cannot be used to
    route a group to the same worker across restarts. A fixed digest keeps the
    assignment deterministic and evenly spread over ``pool_size`` slots.
    """
    digest = hashlib.md5(group.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % max(1, pool_size)


def _apply_cpu_thread_limits(cpu_threads: int) -> None:
    """Cap CPU threads so N group workers do not spawn N x N threads."""
    value = str(max(1, cpu_threads))
    for var in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "TORCH_NUM_THREADS",
    ):
        os.environ.setdefault(var, value)


def deepant_pool_worker_main(
    worker_id: int,
    config: DeepAntWorkerConfig,
    feature_names: List[str],
    work_q: Any,
    ack_q: Any,
) -> None:
    """Process messages for the groups assigned to one pool worker.

    A bounded pool of these workers replaces the former one-process-per-group
    model: the main process assigns each group to a worker by stable hash, so a
    single worker may own several groups and keeps per-group model + threshold
    state in a ``{group: _GroupState}`` dict. Pulls ``DeepAntWorkItem`` objects
    from ``work_q`` (``None`` is the stop sentinel), scores each with its group's
    DeepAnT model and threshold estimator, publishes the enriched Avro record and
    the JSON viz copy, waits for both deliveries, and reports a ``DeepAntAck`` on
    ``ack_q``.
    """
    logger = logging.getLogger(f"analyse-deepant.worker.{worker_id}")
    level = getattr(logging, str(config.log_level).upper(), logging.INFO)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)

    _apply_cpu_thread_limits(config.cpu_threads)

    # Lazy imports: torch / model / kafka are only needed inside the worker.
    import numpy as np
    import torch

    torch.set_num_threads(max(1, config.cpu_threads))

    try:
        from models.deepant import (
            DeepAntStreamingModel,
            classify_deepant_score,
            classify_deepant_score_percentile,
        )
    except ModuleNotFoundError:
        from runtime.detectors.deepant.models.deepant import (
            DeepAntStreamingModel,
            classify_deepant_score,
            classify_deepant_score_percentile,
        )

    from dasma_kafka.anomaly.thresholds import (
        SlidingPercentileEstimator,
        WelfordEstimator,
        WindowedWelfordEstimator,
    )
    from confluent_kafka import SerializingProducer
    from confluent_kafka.serialization import StringSerializer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer

    np.random.seed(config.seed)
    random.seed(config.seed)
    torch.manual_seed(config.seed)

    exclude_fields = set(config.exclude_fields)

    sr_conf: Dict[str, str] = {"url": config.schema_registry_url}
    if config.schema_registry_basic_auth_user_info:
        sr_conf["basic.auth.user.info"] = config.schema_registry_basic_auth_user_info
        sr_conf["basic.auth.credentials.source"] = config.schema_registry_basic_auth_source
    sr_client = SchemaRegistryClient(sr_conf)
    value_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=config.dest_avro_schema,
    )

    producer_conf = {
        "bootstrap.servers": config.bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 1,
        "retries": 2_147_483_647,
        "delivery.timeout.ms": 300000,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": value_serializer,
    }
    producer = SerializingProducer(producer_conf)
    json_producer = SerializingProducer(
        {**producer_conf, "value.serializer": StringSerializer("utf_8")}
    )

    states: Dict[str, _GroupState] = {}

    delivery_state: Dict[str, Any] = {"avro_ok": False, "avro_err": None, "json_ok": False, "json_err": None}

    def on_delivery_avro(err, _msg):
        delivery_state["avro_ok"] = err is None
        delivery_state["avro_err"] = err

    def on_delivery_json(err, _msg):
        delivery_state["json_ok"] = err is None
        delivery_state["json_err"] = err

    def wait_for_both_deliveries(timeout_sec: float = 5.0) -> bool:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            producer.poll(0.1)
            json_producer.poll(0.1)
            if delivery_state["avro_err"] is not None or delivery_state["json_err"] is not None:
                return False
            if delivery_state["avro_ok"] and delivery_state["json_ok"]:
                return True
        return delivery_state["avro_ok"] and delivery_state["json_ok"]

    logger.info("Pool worker %d started (%d features)", worker_id, len(feature_names))
    processed = 0

    try:
        while True:
            item: Optional[DeepAntWorkItem] = work_q.get()
            if item is None:
                break

            group = item.group
            state = states.get(group)
            if state is None:
                state = _GroupState()
                states[group] = state
                logger.info("Worker %d now owns group '%s'", worker_id, group)

            record = item.record
            ok = False
            try:
                deepant_score: Optional[float] = None
                pending_refit_x: Optional[List[float]] = None

                try:
                    x = build_feature_vector(record, feature_names)
                except Exception as exc:
                    logger.error("[%s] feature vector error: %s", group, exc)
                    x = None

                if x is not None:
                    if state.model is None:
                        state.model = DeepAntStreamingModel(
                            n_features=len(feature_names),
                            lookback_size=config.lookback_size,
                            initial_window_size=config.initial_window_size,
                            refit_every=config.refit_every,
                            window_size=config.deepant_window_size,
                            filters=config.filters,
                            kernel_size=config.kernel_size,
                            dropout=config.dropout,
                            lr=config.lr,
                            epochs_initial=config.epochs_initial,
                            epochs_refit=config.epochs_refit,
                            batch_size=config.batch_size,
                            seed=config.seed,
                        )
                        logger.info("[%s] created DeepAntStreamingModel", group)

                    defer_refit_update = (
                        config.exclude_anomalies_from_refit and not state.model.is_warming_up
                    )
                    try:
                        deepant_score = float(
                            state.model.partial_fit_score(x, learn=not defer_refit_update)
                        )
                        if defer_refit_update:
                            pending_refit_x = x
                    except Exception as exc:
                        logger.error("[%s] scoring error: %s", group, exc)
                        deepant_score = None

                deepant_status = "warming_up"
                deepant_anomaly: Optional[bool] = None
                deepant_threshold: Optional[float] = None
                if state.model is not None:
                    if config.threshold_mode == "percentile":
                        if state.percentile_estimator is None:
                            state.percentile_estimator = SlidingPercentileEstimator(
                                config.threshold_window, config.threshold_percentile
                            )
                        deepant_status, deepant_anomaly, deepant_threshold = (
                            classify_deepant_score_percentile(
                                deepant_score,
                                state.percentile_estimator,
                                is_warming_up=state.model.is_warming_up,
                                min_samples=config.anomaly_warmup_window,
                            )
                        )
                    else:
                        if state.welford is None:
                            state.welford = (
                                WindowedWelfordEstimator(config.welford_window)
                                if config.threshold_mode == "welford-windowed"
                                else WelfordEstimator()
                            )
                        deepant_status, deepant_anomaly, deepant_threshold = classify_deepant_score(
                            deepant_score,
                            state.welford,
                            is_warming_up=state.model.is_warming_up,
                            min_samples=config.anomaly_warmup_window,
                            k_sigma=config.anomaly_k_sigma,
                        )

                if (
                    pending_refit_x is not None
                    and state.model is not None
                    and deepant_anomaly is not True
                ):
                    try:
                        state.model.accept_sample(pending_refit_x)
                    except Exception as exc:
                        logger.error("[%s] refit history error: %s", group, exc)

                record, deepant_score = ensure_deepant_analysis_fields(
                    record, deepant_score, deepant_status, deepant_anomaly, deepant_threshold
                )

                viz_record = (
                    strip_excluded_fields(
                        record, exclude_fields, preserve_fields={"Group", "group", "timestamp", "source_event_id"}
                    )
                    if isinstance(record, dict)
                    else {"deepant_score": deepant_score}
                )
                if state.viz_state is None:
                    state.viz_state = create_viz_state()
                viz_record = enrich_viz_record(viz_record, state.viz_state, config.window_size)
                if viz_record.get("timestamp") in (None, ""):
                    resolved_ts = resolve_timestamp_value(record)
                    if resolved_ts not in (None, ""):
                        viz_record["timestamp"] = str(resolved_ts)

                try:
                    json_payload = json.dumps(viz_record, ensure_ascii=False)
                except Exception:
                    json_payload = "{}"

                delivery_state.update({"avro_ok": False, "avro_err": None, "json_ok": False, "json_err": None})
                produce_kwargs = {}
                if item.timestamp_ms is not None:
                    produce_kwargs["timestamp"] = item.timestamp_ms
                producer.produce(
                    topic=config.dest_topic,
                    key=item.key_str,
                    value=record,
                    on_delivery=on_delivery_avro,
                    **produce_kwargs,
                )
                json_producer.produce(
                    topic=config.dest_viz_topic,
                    key=item.key_str,
                    value=json_payload,
                    on_delivery=on_delivery_json,
                    **produce_kwargs,
                )

                ok = wait_for_both_deliveries(timeout_sec=5.0)
                if not ok:
                    logger.error(
                        "[%s] delivery failed (avro_err=%s, json_err=%s) offset=%s",
                        group,
                        delivery_state["avro_err"],
                        delivery_state["json_err"],
                        item.offset,
                    )
                else:
                    processed += 1
            except Exception as exc:
                logger.error("[%s] unexpected processing error at offset %s: %s", group, item.offset, exc)
                ok = False
            finally:
                ack_q.put(DeepAntAck(item.topic, item.partition, item.offset, ok))
    finally:
        logger.info(
            "Pool worker %d stopping, processed=%s, groups=%s",
            worker_id,
            processed,
            sorted(states),
        )
        try:
            producer.flush(10)
        except Exception:
            pass
        try:
            json_producer.flush(10)
        except Exception:
            pass


def run_parallel_by_group(
    consumer: Any,
    source_topic: str,
    worker_config: DeepAntWorkerConfig,
    exclude_fields: set,
    max_workers: int,
    queue_maxsize: int,
    idle_exit_sec: int,
    log_every: int,
    worker_target: Any = deepant_pool_worker_main,
) -> None:
    """Dispatch messages to a bounded worker pool, preserving publish-before-commit.

    The main process owns Kafka consumption, feature inference and offset commit.
    Groups are spread over a fixed pool of at most ``max_workers`` processes by
    stable hash (``_worker_index``), so a worker may own several groups and the
    number of distinct groups never causes a shutdown. Workers are spawned lazily
    the first time a group hashes to their slot. Workers ack each publish on
    ``ack_q`` and the main commits only the contiguous prefix of acked offsets via
    ``CommitOffsetTracker``. Clean shutdown (no commit of the current message)
    happens on a feature-set change, which would invalidate DeepAnT scores.
    """
    tracker = CommitOffsetTracker()
    pool_size = max(1, max_workers)
    workers: Dict[int, Any] = {}
    queues: Dict[int, Any] = {}
    ack_q: Any = mp.Queue()
    feature_names: Optional[List[str]] = None
    no_numeric_skipped = 0
    dispatched = 0

    running = {"on": True}

    def handle_sigint(_sig, _frame):
        running["on"] = False

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    def drain_acks() -> None:
        while True:
            try:
                ack = ack_q.get_nowait()
            except QueueEmpty:
                break
            if ack.ok:
                tracker.mark_done(ack.topic, ack.partition, ack.offset)
            # Worker failure: do NOT mark done -> the watermark stalls at this
            # offset and nothing past it is committed (reprocessed on restart).

    def commit_ready() -> None:
        for topic, partition, offset in tracker.drain_commits():
            try:
                consumer.commit(
                    offsets=[TopicPartition(topic, partition, offset)],
                    asynchronous=False,
                )
            except Exception as exc:
                print(f"[analyse-deepant] commit error: {exc}", file=sys.stderr, flush=True)

    def shutdown_and_drain() -> None:
        # Ask each worker to stop after it has drained its own queue.
        for q in queues.values():
            try:
                q.put(None, timeout=1.0)
            except Exception:
                pass
        deadline = time.time() + 60
        pending = dict(workers)
        while pending and time.time() < deadline:
            drain_acks()
            commit_ready()
            for wid, proc in list(pending.items()):
                proc.join(timeout=0.1)
                if not proc.is_alive():
                    del pending[wid]
        drain_acks()
        commit_ready()
        for proc in pending.values():
            if proc.is_alive():
                proc.terminate()

    print(
        f"[analyse-deepant] Parallel-by-group pool enabled "
        f"(pool_size={pool_size}, queue_maxsize={queue_maxsize}, "
        f"cpu_threads_per_worker={worker_config.cpu_threads}). "
        f"Groups are spread over the pool by stable hash; the number of groups "
        f"is not capped.",
        flush=True,
    )
    consumer.subscribe([source_topic])
    last_msg_ts = time.monotonic()

    try:
        while running["on"]:
            try:
                msg = consumer.poll(1.0)
            except ConsumeError as exc:
                print(f"[consumer-error] {exc}", file=sys.stderr, flush=True)
                continue

            drain_acks()
            commit_ready()

            if msg is None:
                if idle_exit_sec > 0 and (time.monotonic() - last_msg_ts) > idle_exit_sec:
                    print(
                        f"[analyse-deepant] Idle for >{idle_exit_sec}s, exiting.",
                        flush=True,
                    )
                    break
                continue

            if msg.error() is not None:
                print(f"[consumer-error] {msg.error()}", file=sys.stderr, flush=True)
                continue

            last_msg_ts = time.monotonic()
            record = msg.value()
            topic = msg.topic()
            partition = int(msg.partition())
            offset = int(msg.offset())
            tracker.register(topic, partition, offset)

            inferred = (
                sorted(infer_feature_names(record, exclude_fields))
                if isinstance(record, dict)
                else []
            )

            if feature_names is None:
                if inferred:
                    feature_names = inferred
                    print(
                        f"[analyse-deepant] Using {len(feature_names)} features "
                        f"for DeepAnT: {feature_names}",
                        flush=True,
                    )
                else:
                    no_numeric_skipped += 1
                    if no_numeric_skipped == 1 or no_numeric_skipped % 100 == 0:
                        print(
                            f"[analyse-deepant] Record without numeric fields, "
                            f"skipping (count={no_numeric_skipped}).",
                            file=sys.stderr,
                            flush=True,
                        )
                    tracker.mark_done(topic, partition, offset)
                    continue
            elif inferred and inferred != feature_names:
                print(
                    f"[analyse-deepant] FATAL: feature set changed from "
                    f"{feature_names} to {inferred}. Shutting down to avoid "
                    f"invalid DeepAnT scores (current message not committed).",
                    file=sys.stderr,
                    flush=True,
                )
                running["on"] = False
                break

            group = resolve_group_key(record)
            worker_id = _worker_index(group, pool_size)

            if worker_id not in workers:
                queue: Any = mp.Queue(maxsize=queue_maxsize)
                proc = mp.Process(
                    target=worker_target,
                    args=(worker_id, worker_config, feature_names, queue, ack_q),
                    daemon=False,
                )
                proc.start()
                workers[worker_id] = proc
                queues[worker_id] = queue
                print(
                    f"[analyse-deepant] Pool worker {worker_id} started "
                    f"(active workers={len(workers)}/{pool_size}).",
                    flush=True,
                )

            key_str = msg.key()
            ts = msg.timestamp()
            ts_ms = ts[1] if ts else None
            item = DeepAntWorkItem(
                key_str=key_str,
                record=record,
                topic=topic,
                partition=partition,
                offset=offset,
                timestamp_ms=ts_ms,
                group=group,
            )

            enqueue_attempts = 0
            while running["on"]:
                try:
                    queues[worker_id].put(item, timeout=1.0)
                    dispatched += 1
                    if dispatched % log_every == 0:
                        print(
                            f"[analyse-deepant] Dispatched={dispatched} "
                            f"pending_commit={tracker.pending_count()} "
                            f"workers={len(workers)}",
                            flush=True,
                        )
                    break
                except QueueFull:
                    enqueue_attempts += 1
                    # Relieve pressure and make commit progress while blocked.
                    drain_acks()
                    commit_ready()
                    if enqueue_attempts == 1 or enqueue_attempts % 30 == 0:
                        print(
                            f"[analyse-deepant] Backpressure for group '{group}': "
                            f"queue full, waiting instead of dropping "
                            f"(offset={offset}, waits={enqueue_attempts}).",
                            flush=True,
                        )
    except KeyboardInterrupt:
        pass
    finally:
        print(
            f"[analyse-deepant] Stopping workers (dispatched={dispatched}, "
            f"skipped_no_numeric={no_numeric_skipped}).",
            flush=True,
        )
        shutdown_and_drain()
        try:
            consumer.close()
        except Exception:
            pass
