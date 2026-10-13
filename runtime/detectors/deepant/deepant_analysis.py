#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
deepant_analysis.py

- Consumes Avro messages from a source topic (default: `source-data`).
- Deserializes them through Schema Registry.
- Builds one DeepAntStreamingModel per logical group over numeric fields, excluding
  `timestamp`, `Group`, and similar metadata.
- Computes a continuous anomaly score, `deepant_score`, for each processed record
  (L2 distance between predicted and observed next value, on Min-Max normalized
  data), `0.0` during warmup or on error.
- Adds an explicit DeepAnT trigger verdict:
  - `deepant_status`   : `warming_up`, `normal`, or `anomaly`
  - `deepant_anomaly`  : boolean verdict, `null` while warming up
  - `deepant_threshold`: current per-group threshold, `null` while warming up
- Extends the source Avro schema with nullable DeepAnT analysis fields.
- Republishes enriched records to `deepant-analysis` (Avro) and a derived JSON
  payload to `deepant-analysis-viz` (for Telegraf/Grafana).
- Commits offsets only after successful production, at-least-once.

PyTorch (CPU-only) is required. The script aborts at startup if `torch` is not
importable. The image build installs the CPU-only wheel via the
`download.pytorch.org/whl/cpu` index.

Campaign parameters are documented in docs/architecture/askmydasma-campaign-runtime.md.
"""
import os
import sys
import signal
import json
import argparse
import time
import random
from pathlib import Path

from typing import List, Optional, Dict, Any

if os.getenv("DASMA_ENABLED", "1") == "0":
    print(
        "[analyse-deepant] DASMA_ENABLED=0 - service disabled. Idling.",
        flush=True,
    )
    signal.pause()
    sys.exit(0)

import numpy as np

ROOT_DIR = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src" / "dasma_kafka").is_dir()
)
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.deepant_analysis.helpers import (
    build_feature_vector,
    create_viz_state,
    ensure_deepant_analysis_fields,
    enrich_viz_record,
    infer_feature_names,
    resolve_group_key,
    strip_excluded_fields,
)
from dasma_kafka.common.timestamps import resolve_timestamp_value
from dasma_kafka.common.threshold_modes import (
    DEEPANT_ANALYSIS_SCHEMA_FIELDS,
    extend_deepant_avro_schema,
    normalize_deepant_threshold_mode,
    parse_deepant_exclude_anomalies_from_refit,
)

try:
    import torch  # noqa: F401
except ImportError:
    print(
        "[analyse-deepant] FATAL: torch not available. "
        "Install with: pip install torch==2.6.0 "
        "--index-url https://download.pytorch.org/whl/cpu",
        file=sys.stderr,
        flush=True,
    )
    sys.exit(1)

try:
    # When running with working_dir=/app/deepant_analysis, `models` is a top-level package.
    from models.deepant import (
        DeepAntStreamingModel,
        classify_deepant_score,
        classify_deepant_score_percentile,
    )
    from dasma_kafka.anomaly.thresholds import (
        SlidingPercentileEstimator,
        WelfordEstimator,
        WindowedWelfordEstimator,
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

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.error import ConsumeError
from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient

from dasma_kafka.deepant_analysis.runtime import (
    DeepAntWorkerConfig,
    run_parallel_by_group,
)


# DEEPANT_ANALYSIS_SCHEMA_FIELDS, extend_deepant_avro_schema,
# normalize_deepant_threshold_mode, parse_deepant_exclude_anomalies_from_refit
# are now imported from dasma_kafka.common.threshold_modes above.


def _get_bootstrap_servers() -> str:
    bs = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
    bs = ",".join([p.strip() for p in bs.split(",") if p.strip()])
    return bs or "kafka1:9092,kafka2:9092,kafka3:9092"


def _topic_partitions_and_rf(admin: AdminClient, topic: str) -> tuple[int, int]:
    md = admin.list_topics(topic=topic, timeout=10)
    t = md.topics.get(topic)
    if t is None or t.error is not None:
        raise RuntimeError(
            str(t.error) if t and t.error else f"topic '{topic}' not found"
        )
    partitions = t.partitions or {}
    pcount = len(partitions)
    rf = 0
    if partitions:
        first_pid = sorted(partitions.keys())[0]
        rf = len(partitions[first_pid].replicas or [])
    return pcount, rf


def _check_topics_layout(
    bootstrap_servers: str,
    topics: list[str],
    expected_partitions: int | None,
    expected_rf: int | None,
) -> None:
    if expected_partitions is None and expected_rf is None:
        return
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    for t in topics:
        try:
            pcount, rf = _topic_partitions_and_rf(admin, t)
            if expected_partitions is not None and pcount != expected_partitions:
                print(
                    f"[analyse-deepant] WARN topic '{t}' has partitions={pcount} "
                    f"but TOPIC_PARTITIONS={expected_partitions}",
                    flush=True,
                )
            if expected_rf is not None and rf != expected_rf:
                print(
                    f"[analyse-deepant] WARN topic '{t}' has replication_factor={rf} "
                    f"but TOPIC_RF={expected_rf}",
                    flush=True,
                )
        except Exception as e:
            print(
                f"[analyse-deepant] WARN cannot inspect topic '{t}': {e}",
                flush=True,
            )


def make_sr_client() -> SchemaRegistryClient:
    url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    conf = {"url": url}
    user_info = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")
    if user_info:
        conf["basic.auth.user.info"] = user_info
        conf["basic.auth.credentials.source"] = os.getenv(
            "SCHEMA_REGISTRY_BASIC_AUTH_SOURCE", "USER_INFO"
        )
    return SchemaRegistryClient(conf)


def make_consumer(
    group_id: str, value_deserializer: AvroDeserializer
) -> DeserializingConsumer:
    conf = {
        "bootstrap.servers": _get_bootstrap_servers(),
        "group.id": group_id,
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": value_deserializer,
        "enable.partition.eof": False,
    }
    return DeserializingConsumer(conf)


def make_producer(value_serializer: AvroSerializer) -> SerializingProducer:
    conf = {
        "bootstrap.servers": _get_bootstrap_servers(),
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 1,
        "retries": 2_147_483_647,
        "delivery.timeout.ms": 300000,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": value_serializer,
    }
    return SerializingProducer(conf)


def make_json_producer() -> SerializingProducer:
    conf = {
        "bootstrap.servers": _get_bootstrap_servers(),
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 1,
        "retries": 2_147_483_647,
        "delivery.timeout.ms": 300000,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": StringSerializer("utf_8"),
    }
    return SerializingProducer(conf)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Consume from `source-data` and republish to `deepant-analysis` "
            "(Avro / Schema Registry)."
        )
    )
    parser.add_argument(
        "-s",
        "--source-topic",
        default=os.getenv("KAFKA_SOURCE_TOPIC", "source-data"),
        help="Source topic, default `source-data`",
    )
    parser.add_argument(
        "-d",
        "--dest-topic",
        default=os.getenv("KAFKA_DEST_TOPIC", "deepant-analysis"),
        help="Destination topic, default `deepant-analysis`",
    )
    parser.add_argument(
        "--dest-viz-topic",
        default=os.getenv("KAFKA_DEST_VIZ_TOPIC", "deepant-analysis-viz"),
        help="Destination viz topic for JSON, default `deepant-analysis-viz`",
    )
    parser.add_argument(
        "-g",
        "--group-id",
        default=os.getenv("ANALYSE_DEEPANT_GROUP_ID", "analyse-deepant-consumer"),
        help="Consumer `group.id`, default `analyse-deepant-consumer`",
    )
    args = parser.parse_args()

    bootstrap_servers = _get_bootstrap_servers()
    print(
        f"[analyse-deepant] Using BOOTSTRAP_SERVERS={bootstrap_servers}",
        flush=True,
    )

    expected_partitions = os.getenv("TOPIC_PARTITIONS")
    expected_rf = os.getenv("TOPIC_RF")
    exp_p = (
        int(expected_partitions)
        if expected_partitions and expected_partitions.strip().isdigit()
        else None
    )
    exp_rf = (
        int(expected_rf)
        if expected_rf and expected_rf.strip().isdigit()
        else None
    )
    if exp_p is not None or exp_rf is not None:
        _check_topics_layout(
            bootstrap_servers,
            topics=[args.source_topic, args.dest_topic, args.dest_viz_topic],
            expected_partitions=exp_p,
            expected_rf=exp_rf,
        )

    seed = int(os.getenv("ANALYSE_SEED", "42"))
    np.random.seed(seed)
    random.seed(seed)
    torch.manual_seed(seed)
    print(
        f"[analyse-deepant] Reproducibility enabled (ANALYSE_SEED={seed})",
        flush=True,
    )

    legacy_group = os.getenv("ANALYSE_GROUP")
    if legacy_group:
        print(
            f"[analyse-deepant] ANALYSE_GROUP is set to '{legacy_group}' but is "
            f"ignored: multi-group DeepAnT is active.",
            flush=True,
        )

    idle_exit_sec = int(os.getenv("ANALYSE_IDLE_EXIT_SEC", "0"))

    sr = make_sr_client()
    value_deserializer = AvroDeserializer(schema_registry_client=sr)

    source_subject = f"{args.source_topic}-value"
    wait_seconds = int(os.getenv("ANALYSE_SCHEMA_WAIT_SEC", "0"))
    schema_poll_interval = int(os.getenv("ANALYSE_SCHEMA_POLL_INTERVAL_SEC", "5"))

    latest_schema = None
    waited = 0
    while latest_schema is None:
        try:
            latest_schema = sr.get_latest_version(source_subject)
        except Exception as e:
            msg = str(e)
            if "40401" in msg or "Subject not found" in msg:
                if wait_seconds > 0 and waited >= wait_seconds:
                    print(
                        f"[analyse-deepant] Schema Registry subject "
                        f"'{source_subject}' not found after waiting "
                        f"{wait_seconds}s; aborting.",
                        file=sys.stderr,
                        flush=True,
                    )
                    sys.exit(1)
                print(
                    f"[analyse-deepant] Schema Registry subject "
                    f"'{source_subject}' not found yet, waiting "
                    f"{schema_poll_interval}s... (error: {e})",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(schema_poll_interval)
                waited += schema_poll_interval
                continue
            print(
                f"[analyse-deepant] Error while fetching schema for subject "
                f"'{source_subject}' from Schema Registry: {e}",
                file=sys.stderr,
                flush=True,
            )
            sys.exit(1)

    source_avro_schema = latest_schema.schema.schema_str

    try:
        dest_avro_schema = extend_deepant_avro_schema(source_avro_schema)
    except Exception as e:
        print(
            f"[analyse-deepant] Failed to parse source Avro schema as JSON: {e}",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    value_serializer = AvroSerializer(
        schema_registry_client=sr, schema_str=dest_avro_schema
    )

    consumer = make_consumer(args.group_id, value_deserializer)
    producer = make_producer(value_serializer)
    json_producer = make_json_producer()

    last_msg_ts = time.monotonic()

    window_str = os.getenv("WINDOWS_SIZE", "0")
    try:
        window_size = int(window_str)
    except Exception:
        window_size = 0

    log_every = int(os.getenv("LOG_EVERY", "10"))
    if log_every <= 0:
        log_every = 10

    viz_state_by_group: Dict[str, Dict[str, Any]] = {}
    processed = 0

    exclude_fields_env = os.getenv(
        "DEEPANT_EXCLUDE_FIELDS", "source_event_id,timestamp,Group,group,Cycle,LabviewTime,time"
    )
    DEEPANT_EXCLUDE_FIELDS = {
        name.strip() for name in exclude_fields_env.split(",") if name.strip()
    }

    DEEPANT_LOOKBACK_SIZE = int(os.getenv("DEEPANT_LOOKBACK_SIZE", "100"))
    DEEPANT_INITIAL_WINDOW_SIZE = int(os.getenv("DEEPANT_INITIAL_WINDOW_SIZE", "512"))
    DEEPANT_REFIT_EVERY = int(os.getenv("DEEPANT_REFIT_EVERY", "64"))
    DEEPANT_WINDOW_SIZE = int(os.getenv("DEEPANT_WINDOW_SIZE", "2048"))
    DEEPANT_FILTERS = int(os.getenv("DEEPANT_FILTERS", "32"))
    DEEPANT_KERNEL_SIZE = int(os.getenv("DEEPANT_KERNEL_SIZE", "2"))
    DEEPANT_DROPOUT = float(os.getenv("DEEPANT_DROPOUT", "0.25"))
    DEEPANT_LR = float(os.getenv("DEEPANT_LR", "0.001"))
    DEEPANT_EPOCHS_INITIAL = int(os.getenv("DEEPANT_EPOCHS_INITIAL", "10"))
    DEEPANT_EPOCHS_REFIT = int(os.getenv("DEEPANT_EPOCHS_REFIT", "5"))
    DEEPANT_BATCH_SIZE = int(os.getenv("DEEPANT_BATCH_SIZE", "32"))
    DEEPANT_ANOMALY_K_SIGMA = float(os.getenv("DEEPANT_ANOMALY_K_SIGMA", "3.0"))
    DEEPANT_ANOMALY_WARMUP_WINDOW = max(
        2,
        int(
            os.getenv(
                "DEEPANT_ANOMALY_WARMUP_WINDOW",
                os.getenv("DEEPANT_ANOMALY_MIN_SAMPLES", "100"),
            )
        ),
    )
    DEEPANT_THRESHOLD_MODE = normalize_deepant_threshold_mode(
        os.getenv("DEEPANT_THRESHOLD_MODE", "welford-windowed")
    )
    DEEPANT_THRESHOLD_PERCENTILE = float(
        os.getenv("DEEPANT_THRESHOLD_PERCENTILE", "99.0")
    )
    DEEPANT_THRESHOLD_WINDOW = int(os.getenv("DEEPANT_THRESHOLD_WINDOW", "500"))
    DEEPANT_WELFORD_WINDOW = int(os.getenv("DEEPANT_WELFORD_WINDOW", str(DEEPANT_WINDOW_SIZE)))
    DEEPANT_EXCLUDE_ANOMALIES_FROM_REFIT = (
        parse_deepant_exclude_anomalies_from_refit(
            os.getenv("DEEPANT_EXCLUDE_ANOMALIES_FROM_REFIT", "0")
        )
    )

    if DEEPANT_LOOKBACK_SIZE < 8:
        print(
            f"[analyse-deepant] FATAL: DEEPANT_LOOKBACK_SIZE must be >= 8 "
            f"(got {DEEPANT_LOOKBACK_SIZE})",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
    if DEEPANT_INITIAL_WINDOW_SIZE < DEEPANT_LOOKBACK_SIZE:
        print(
            f"[analyse-deepant] FATAL: DEEPANT_INITIAL_WINDOW_SIZE "
            f"({DEEPANT_INITIAL_WINDOW_SIZE}) must be >= "
            f"DEEPANT_LOOKBACK_SIZE ({DEEPANT_LOOKBACK_SIZE})",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)

    print(
        f"[analyse-deepant] DeepAnT config: lookback={DEEPANT_LOOKBACK_SIZE}, "
        f"initial_window={DEEPANT_INITIAL_WINDOW_SIZE}, "
        f"refit_every={DEEPANT_REFIT_EVERY}, window={DEEPANT_WINDOW_SIZE}, "
        f"filters={DEEPANT_FILTERS}, kernel={DEEPANT_KERNEL_SIZE}, "
        f"dropout={DEEPANT_DROPOUT}, lr={DEEPANT_LR}, "
        f"epochs_initial={DEEPANT_EPOCHS_INITIAL}, "
        f"epochs_refit={DEEPANT_EPOCHS_REFIT}, batch={DEEPANT_BATCH_SIZE}.",
        flush=True,
    )
    print(
        f"[analyse-deepant] DeepAnT anomaly trigger: "
        f"mode={DEEPANT_THRESHOLD_MODE}, "
        f"warmup_window={DEEPANT_ANOMALY_WARMUP_WINDOW}, "
        f"exclude_anomalies_from_refit={int(DEEPANT_EXCLUDE_ANOMALIES_FROM_REFIT)}.",
        flush=True,
    )
    if DEEPANT_THRESHOLD_MODE == "percentile":
        print(
            f"[analyse-deepant] Percentile threshold: "
            f"P{DEEPANT_THRESHOLD_PERCENTILE}, "
            f"window={DEEPANT_THRESHOLD_WINDOW}.",
            flush=True,
        )
    elif DEEPANT_THRESHOLD_MODE == "welford-windowed":
        print(
            f"[analyse-deepant] Windowed Welford threshold: "
            f"k_sigma={DEEPANT_ANOMALY_K_SIGMA}, window={DEEPANT_WELFORD_WINDOW}.",
            flush=True,
        )
    else:
        print(
            f"[analyse-deepant] Welford threshold: k_sigma={DEEPANT_ANOMALY_K_SIGMA}.",
            flush=True,
        )

    parallel_by_group = os.getenv("DEEPANT_PARALLEL_BY_GROUP", "0").strip() in {"1", "true", "True"}
    if parallel_by_group:
        max_group_workers = int(os.getenv("DEEPANT_MAX_GROUP_WORKERS", "2"))
        group_queue_maxsize = int(os.getenv("DEEPANT_GROUP_QUEUE_MAXSIZE", "1000"))
        cpu_threads_per_worker = int(os.getenv("DEEPANT_CPU_THREADS_PER_WORKER", "1"))
        worker_config = DeepAntWorkerConfig(
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            schema_registry_basic_auth_user_info=os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"),
            schema_registry_basic_auth_source=os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_SOURCE", "USER_INFO"),
            dest_topic=args.dest_topic,
            dest_viz_topic=args.dest_viz_topic,
            dest_avro_schema=dest_avro_schema,
            seed=seed,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            window_size=window_size,
            exclude_fields=list(DEEPANT_EXCLUDE_FIELDS),
            threshold_mode=DEEPANT_THRESHOLD_MODE,
            threshold_percentile=DEEPANT_THRESHOLD_PERCENTILE,
            threshold_window=DEEPANT_THRESHOLD_WINDOW,
            welford_window=DEEPANT_WELFORD_WINDOW,
            anomaly_k_sigma=DEEPANT_ANOMALY_K_SIGMA,
            anomaly_warmup_window=DEEPANT_ANOMALY_WARMUP_WINDOW,
            exclude_anomalies_from_refit=DEEPANT_EXCLUDE_ANOMALIES_FROM_REFIT,
            cpu_threads=cpu_threads_per_worker,
            lookback_size=DEEPANT_LOOKBACK_SIZE,
            initial_window_size=DEEPANT_INITIAL_WINDOW_SIZE,
            refit_every=DEEPANT_REFIT_EVERY,
            deepant_window_size=DEEPANT_WINDOW_SIZE,
            filters=DEEPANT_FILTERS,
            kernel_size=DEEPANT_KERNEL_SIZE,
            dropout=DEEPANT_DROPOUT,
            lr=DEEPANT_LR,
            epochs_initial=DEEPANT_EPOCHS_INITIAL,
            epochs_refit=DEEPANT_EPOCHS_REFIT,
            batch_size=DEEPANT_BATCH_SIZE,
        )
        run_parallel_by_group(
            consumer=consumer,
            source_topic=args.source_topic,
            worker_config=worker_config,
            exclude_fields=DEEPANT_EXCLUDE_FIELDS,
            max_workers=max_group_workers,
            queue_maxsize=group_queue_maxsize,
            idle_exit_sec=idle_exit_sec,
            log_every=log_every,
        )
        print("--------------", flush=True)
        print("End of analyse-deepant & Kafka forwarding (parallel-by-group)", flush=True)
        print("--------------", flush=True)
        return

    models_by_group: dict[str, DeepAntStreamingModel] = {}
    welford_by_group: dict[str, WelfordEstimator | WindowedWelfordEstimator] = {}
    percentile_by_group: dict[str, SlidingPercentileEstimator] = {}
    feature_names: Optional[List[str]] = None

    running = True

    def handle_sigint(_sig, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    consumer.subscribe([args.source_topic])
    print(
        f"[analyse-deepant] Waiting for new messages on topic "
        f"'{args.source_topic}' (infinite wait)...",
        flush=True,
    )

    delivery_state: Dict[str, Any] = {
        "avro_ok": False,
        "avro_err": None,
        "json_ok": False,
        "json_err": None,
    }

    def on_delivery_avro(err, _msg):
        delivery_state["avro_ok"] = err is None
        delivery_state["avro_err"] = err

    def on_delivery_json(err, _msg):
        delivery_state["json_ok"] = err is None
        delivery_state["json_err"] = err

    def reset_delivery_state() -> None:
        delivery_state["avro_ok"] = False
        delivery_state["avro_err"] = None
        delivery_state["json_ok"] = False
        delivery_state["json_err"] = None

    def wait_for_both_deliveries(timeout_sec: float = 5.0) -> bool:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            producer.poll(0.1)
            json_producer.poll(0.1)
            if (
                delivery_state["avro_err"] is not None
                or delivery_state["json_err"] is not None
            ):
                return False
            if delivery_state["avro_ok"] and delivery_state["json_ok"]:
                return True
        return delivery_state["avro_ok"] and delivery_state["json_ok"]

    try:
        while running:
            try:
                msg = consumer.poll(1.0)
            except ConsumeError as e:
                err = e.args[0] if e.args else e
                code = (
                    err.code()
                    if hasattr(err, "code") and callable(getattr(err, "code"))
                    else None
                )
                if (
                    code is not None and code == KafkaError._PARTITION_EOF
                ) or "_PARTITION_EOF" in str(err):
                    if (
                        idle_exit_sec > 0
                        and (time.monotonic() - last_msg_ts) >= idle_exit_sec
                    ):
                        print(
                            f"[analyse-deepant] Idle for {idle_exit_sec}s after "
                            f"EOF (exception), exiting.",
                            flush=True,
                        )
                        break
                    continue
                print(f"[consumer-error] {err}", file=sys.stderr, flush=True)
                continue
            if msg is None:
                print("[analyse-deepant] Waiting for new messages...", flush=True)
                if (
                    idle_exit_sec > 0
                    and (time.monotonic() - last_msg_ts) >= idle_exit_sec
                ):
                    print(
                        f"[analyse-deepant] No messages for {idle_exit_sec}s, exiting.",
                        flush=True,
                    )
                    break
                continue
            if msg.error():
                err = msg.error()
                code = (
                    err.code()
                    if hasattr(err, "code") and callable(getattr(err, "code"))
                    else None
                )
                if (
                    code is not None and code == KafkaError._PARTITION_EOF
                ) or "_PARTITION_EOF" in str(err):
                    if (
                        idle_exit_sec > 0
                        and (time.monotonic() - last_msg_ts) >= idle_exit_sec
                    ):
                        print(
                            f"[analyse-deepant] Idle for {idle_exit_sec}s after EOF, exiting.",
                            flush=True,
                        )
                        break
                    continue
                print(f"[consumer-error] {err}", file=sys.stderr, flush=True)
                continue

            last_msg_ts = time.monotonic()

            key = msg.key()
            record = msg.value()

            group_key = resolve_group_key(record)

            deepant_score: Optional[float] = None
            deepant_model: Optional[DeepAntStreamingModel] = None
            pending_deepant_refit_x: Optional[List[float]] = None

            if feature_names is None and isinstance(record, dict):
                numeric_fields = infer_feature_names(record, DEEPANT_EXCLUDE_FIELDS)
                if not numeric_fields:
                    print(
                        "[analyse-deepant] No numeric fields found to build "
                        "DeepAnT feature vector; records will be forwarded with "
                        "deepant_score=0.0.",
                        file=sys.stderr,
                        flush=True,
                    )
                else:
                    feature_names = sorted(numeric_fields)
                    print(
                        f"[analyse-deepant] Using {len(feature_names)} features "
                        f"for DeepAnT: {feature_names}",
                        flush=True,
                    )
            elif feature_names is not None and isinstance(record, dict):
                # Detect schema evolution: numeric field set changed.
                current_fields = sorted(
                    infer_feature_names(record, DEEPANT_EXCLUDE_FIELDS)
                )
                if current_fields and current_fields != feature_names:
                    print(
                        f"[analyse-deepant] WARN feature set changed from "
                        f"{feature_names} to {current_fields}. Resetting all "
                        f"DeepAnT models and restarting warmup.",
                        flush=True,
                    )
                    feature_names = current_fields
                    models_by_group.clear()
                    welford_by_group.clear()
                    percentile_by_group.clear()

            if feature_names:
                try:
                    x = build_feature_vector(record, feature_names)
                except Exception as e:
                    print(
                        f"[analyse-deepant] Error while building feature vector: {e}",
                        file=sys.stderr,
                        flush=True,
                    )
                    x = None

                if x is not None:
                    deepant_model = models_by_group.get(group_key)
                    if deepant_model is None:
                        try:
                            deepant_model = DeepAntStreamingModel(
                                n_features=len(feature_names),
                                lookback_size=DEEPANT_LOOKBACK_SIZE,
                                initial_window_size=DEEPANT_INITIAL_WINDOW_SIZE,
                                refit_every=DEEPANT_REFIT_EVERY,
                                window_size=DEEPANT_WINDOW_SIZE,
                                filters=DEEPANT_FILTERS,
                                kernel_size=DEEPANT_KERNEL_SIZE,
                                dropout=DEEPANT_DROPOUT,
                                lr=DEEPANT_LR,
                                epochs_initial=DEEPANT_EPOCHS_INITIAL,
                                epochs_refit=DEEPANT_EPOCHS_REFIT,
                                batch_size=DEEPANT_BATCH_SIZE,
                                seed=seed,
                            )
                            models_by_group[group_key] = deepant_model
                            print(
                                f"[analyse-deepant] Created DeepAntStreamingModel "
                                f"for Group='{group_key}' with "
                                f"{len(feature_names)} features.",
                                flush=True,
                            )
                        except Exception as e:
                            print(
                                f"[analyse-deepant] Failed to instantiate "
                                f"DeepAntStreamingModel for Group='{group_key}': {e}",
                                file=sys.stderr,
                                flush=True,
                            )
                            deepant_model = None

                    if deepant_model is not None:
                        try:
                            defer_refit_update = (
                                DEEPANT_EXCLUDE_ANOMALIES_FROM_REFIT
                                and not deepant_model.is_warming_up
                            )
                            deepant_score = float(
                                deepant_model.partial_fit_score(
                                    x, learn=not defer_refit_update
                                )
                            )
                            if defer_refit_update:
                                pending_deepant_refit_x = x
                        except Exception as e:
                            print(
                                f"[analyse-deepant] Error during DeepAnT scoring "
                                f"for Group='{group_key}': {e}",
                                file=sys.stderr,
                                flush=True,
                            )
                            deepant_score = None

            deepant_status = "warming_up"
            deepant_anomaly: Optional[bool] = None
            deepant_threshold: Optional[float] = None
            if deepant_model is not None:
                if DEEPANT_THRESHOLD_MODE == "percentile":
                    estimator_p = percentile_by_group.setdefault(
                        group_key,
                        SlidingPercentileEstimator(
                            DEEPANT_THRESHOLD_WINDOW, DEEPANT_THRESHOLD_PERCENTILE
                        ),
                    )
                    (
                        deepant_status,
                        deepant_anomaly,
                        deepant_threshold,
                    ) = classify_deepant_score_percentile(
                        deepant_score,
                        estimator_p,
                        is_warming_up=deepant_model.is_warming_up,
                        min_samples=DEEPANT_ANOMALY_WARMUP_WINDOW,
                    )
                elif DEEPANT_THRESHOLD_MODE == "welford-windowed":
                    welford = welford_by_group.setdefault(
                        group_key, WindowedWelfordEstimator(DEEPANT_WELFORD_WINDOW)
                    )
                    (
                        deepant_status,
                        deepant_anomaly,
                        deepant_threshold,
                    ) = classify_deepant_score(
                        deepant_score,
                        welford,
                        is_warming_up=deepant_model.is_warming_up,
                        min_samples=DEEPANT_ANOMALY_WARMUP_WINDOW,
                        k_sigma=DEEPANT_ANOMALY_K_SIGMA,
                    )
                else:
                    welford = welford_by_group.setdefault(group_key, WelfordEstimator())
                    (
                        deepant_status,
                        deepant_anomaly,
                        deepant_threshold,
                    ) = classify_deepant_score(
                        deepant_score,
                        welford,
                        is_warming_up=deepant_model.is_warming_up,
                        min_samples=DEEPANT_ANOMALY_WARMUP_WINDOW,
                        k_sigma=DEEPANT_ANOMALY_K_SIGMA,
                    )

            if (
                pending_deepant_refit_x is not None
                and deepant_model is not None
                and deepant_anomaly is not True
            ):
                try:
                    deepant_model.accept_sample(pending_deepant_refit_x)
                except Exception as e:
                    print(
                        f"[analyse-deepant] Error while updating DeepAnT refit "
                        f"history for Group='{group_key}': {e}",
                        file=sys.stderr,
                        flush=True,
                    )

            record, deepant_score = ensure_deepant_analysis_fields(
                record,
                deepant_score,
                deepant_status,
                deepant_anomaly,
                deepant_threshold,
            )

            try:
                print(json.dumps(record, ensure_ascii=False), flush=True)
            except Exception:
                print(str(record), flush=True)

            reset_delivery_state()

            viz_record = (
                strip_excluded_fields(
                    record,
                    DEEPANT_EXCLUDE_FIELDS,
                    preserve_fields={"Group", "group", "timestamp", "source_event_id"},
                )
                if isinstance(record, dict)
                else {"deepant_score": deepant_score}
            )

            st = viz_state_by_group.get(group_key)
            if st is None:
                st = create_viz_state()
                viz_state_by_group[group_key] = st

            viz_record = enrich_viz_record(viz_record, st, window_size)
            if viz_record.get("timestamp") in (None, ""):
                resolved_ts = resolve_timestamp_value(record)
                if resolved_ts not in (None, ""):
                    viz_record["timestamp"] = str(resolved_ts)

            try:
                json_payload = json.dumps(viz_record, ensure_ascii=False)
            except Exception as e:
                print(
                    f"[analyse-deepant] Error while serializing JSON for viz topic: {e}",
                    file=sys.stderr,
                    flush=True,
                )
                json_payload = "{}"

            _ts_type, ts_ms = msg.timestamp() or (None, None)
            if ts_ms is not None:
                producer.produce(
                    topic=args.dest_topic,
                    key=key,
                    value=record,
                    timestamp=ts_ms,
                    on_delivery=on_delivery_avro,
                )
                json_producer.produce(
                    topic=args.dest_viz_topic,
                    key=key,
                    value=json_payload,
                    timestamp=ts_ms,
                    on_delivery=on_delivery_json,
                )
            else:
                producer.produce(
                    topic=args.dest_topic,
                    key=key,
                    value=record,
                    on_delivery=on_delivery_avro,
                )
                json_producer.produce(
                    topic=args.dest_viz_topic,
                    key=key,
                    value=json_payload,
                    on_delivery=on_delivery_json,
                )

            ok = wait_for_both_deliveries(timeout_sec=5.0)
            if not ok:
                if delivery_state["avro_err"] is not None:
                    print(
                        f"[analyse-deepant] delivery-error avro: "
                        f"{delivery_state['avro_err']}",
                        file=sys.stderr,
                        flush=True,
                    )
                if delivery_state["json_err"] is not None:
                    print(
                        f"[analyse-deepant] delivery-error json: "
                        f"{delivery_state['json_err']}",
                        file=sys.stderr,
                        flush=True,
                    )
                continue

            processed += 1
            if processed % log_every == 0:
                print(
                    f"[analyse-deepant] Processed={processed} topic={msg.topic()} "
                    f"partition={msg.partition()} offset={msg.offset()}",
                    flush=True,
                )

            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            producer.flush(10)
            json_producer.flush(10)
        finally:
            consumer.close()

    print("--------------", flush=True)
    print("End of analyse-deepant & Kafka forwarding", flush=True)
    print("--------------", flush=True)


if __name__ == "__main__":
    main()
