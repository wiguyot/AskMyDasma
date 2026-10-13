#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""mDragStream Point-based Kafka analysis service."""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

ROOT_DIR = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src" / "dasma_kafka").is_dir()
)
SRC_DIR = ROOT_DIR / "src"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.common.timestamps import resolve_timestamp_value
from dasma_kafka.common.threshold_modes import (
    MDRAGPOINT_ANALYSIS_SCHEMA_FIELDS,
    extend_mdragpoint_avro_schema,
    normalize_mdragpoint_threshold_mode,
    normalize_mdragpoint_verdict_mode,
)
from dasma_kafka.mdragpoint_analysis.helpers import (
    build_feature_vector,
    create_viz_state,
    ensure_mdragpoint_analysis_fields,
    enrich_viz_record,
    infer_feature_names,
    resolve_group_key,
    strip_excluded_fields,
)

try:
    from models.mdragpoint import (
        MDragPointGlobalModel,
        classify_mdragpoint_score_internal,
        classify_mdragpoint_score,
        classify_mdragpoint_score_percentile,
    )
    from dasma_kafka.anomaly.thresholds import (
        SlidingPercentileEstimator,
        WelfordEstimator,
        WindowedWelfordEstimator,
    )
except ModuleNotFoundError:
    from runtime.detectors.mdragpoint.models.mdragpoint import (
        MDragPointGlobalModel,
        classify_mdragpoint_score_internal,
        classify_mdragpoint_score,
        classify_mdragpoint_score_percentile,
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

from confluent_kafka import DeserializingConsumer, KafkaError, SerializingProducer
from confluent_kafka.admin import AdminClient
from confluent_kafka.error import ConsumeError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer


# MDRAGPOINT_ANALYSIS_SCHEMA_FIELDS, extend_mdragpoint_avro_schema,
# normalize_mdragpoint_threshold_mode, normalize_mdragpoint_verdict_mode
# are now imported from dasma_kafka.common.threshold_modes above.


def parse_bool(raw_value: Optional[str], *, default: bool = False) -> bool:
    """Parse common env-var boolean values."""
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _get_bootstrap_servers() -> str:
    bs = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
    bs = ",".join([part.strip() for part in bs.split(",") if part.strip()])
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
    for topic in topics:
        try:
            pcount, rf = _topic_partitions_and_rf(admin, topic)
            if expected_partitions is not None and pcount != expected_partitions:
                print(
                    f"[analyse-mdragpoint] WARN topic '{topic}' has "
                    f"partitions={pcount} but TOPIC_PARTITIONS={expected_partitions}",
                    flush=True,
                )
            if expected_rf is not None and rf != expected_rf:
                print(
                    f"[analyse-mdragpoint] WARN topic '{topic}' has "
                    f"replication_factor={rf} but TOPIC_RF={expected_rf}",
                    flush=True,
                )
        except Exception as exc:
            print(
                f"[analyse-mdragpoint] WARN cannot inspect topic '{topic}': {exc}",
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


def main() -> None:
    if os.getenv("DASMA_ENABLED", "1") == "0":
        print(
            "[analyse-mdragpoint] DASMA_ENABLED=0 - service disabled. Idling.",
            flush=True,
        )
        signal.pause()
        return

    parser = argparse.ArgumentParser(
        description=(
            "Consume from `source-data` and republish to "
            "`mdragstream-point-analysis` (Avro / Schema Registry)."
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
        default=os.getenv("KAFKA_DEST_TOPIC", "mdragstream-point-analysis"),
        help="Destination topic, default `mdragstream-point-analysis`",
    )
    parser.add_argument(
        "--dest-viz-topic",
        default=os.getenv(
            "KAFKA_DEST_VIZ_TOPIC", "mdragstream-point-analysis-viz"
        ),
        help="Destination viz topic for JSON, default `mdragstream-point-analysis-viz`",
    )
    parser.add_argument(
        "-g",
        "--group-id",
        default=os.getenv(
            "ANALYSE_MDRAGPOINT_GROUP_ID", "analyse-mdragpoint-consumer"
        ),
        help="Consumer `group.id`, default `analyse-mdragpoint-consumer`",
    )
    args = parser.parse_args()

    bootstrap_servers = _get_bootstrap_servers()
    print(
        f"[analyse-mdragpoint] Using BOOTSTRAP_SERVERS={bootstrap_servers}",
        flush=True,
    )

    expected_partitions = os.getenv("TOPIC_PARTITIONS")
    expected_rf = os.getenv("TOPIC_RF")
    exp_p = (
        int(expected_partitions)
        if expected_partitions and expected_partitions.strip().isdigit()
        else None
    )
    exp_rf = int(expected_rf) if expected_rf and expected_rf.strip().isdigit() else None
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

    idle_exit_sec = int(os.getenv("ANALYSE_IDLE_EXIT_SEC", "0"))
    window_size = int(os.getenv("WINDOWS_SIZE", "0"))
    log_every = max(1, int(os.getenv("LOG_EVERY", "10")))

    exclude_fields_env = os.getenv(
        "MDRAGPOINT_EXCLUDE_FIELDS", "source_event_id,timestamp,Group,group,Cycle,LabviewTime,time"
    )
    exclude_fields = {
        name.strip() for name in exclude_fields_env.split(",") if name.strip()
    }

    dynamic_threshold = parse_bool(os.getenv("MDRAGPOINT_DYNAMIC_THRESHOLD", "1"))
    lof_init = parse_bool(os.getenv("MDRAGPOINT_LOF_INIT", "0"))
    n_init = int(os.getenv("MDRAGPOINT_N_INIT", "500"))
    max_nb_clusters = int(os.getenv("MDRAGPOINT_MAX_NB_CLUSTERS", "30"))
    max_cluster_size = int(os.getenv("MDRAGPOINT_MAX_CLUSTER_SIZE", "30"))
    metric = os.getenv("MDRAGPOINT_METRIC", "mahalanobis").strip().lower()
    threshold_method = os.getenv("MDRAGPOINT_THRESHOLD_METHOD", "std").strip().lower()
    threshold_t = float(os.getenv("MDRAGPOINT_THRESHOLD_T", "2.0"))
    lof_neighbors = int(os.getenv("MDRAGPOINT_LOF_NEIGHBORS", "10"))

    anomaly_k_sigma = float(os.getenv("MDRAGPOINT_ANOMALY_K_SIGMA", "3.0"))
    verdict_mode = normalize_mdragpoint_verdict_mode(
        os.getenv("MDRAGPOINT_VERDICT_MODE", "internal")
    )
    anomaly_warmup_window = max(
        2,
        int(
            os.getenv(
                "MDRAGPOINT_ANOMALY_WARMUP_WINDOW",
                os.getenv("MDRAGPOINT_ANOMALY_MIN_SAMPLES", "100"),
            )
        ),
    )
    threshold_mode = normalize_mdragpoint_threshold_mode(
        os.getenv("MDRAGPOINT_THRESHOLD_MODE", "percentile")
    )
    threshold_percentile = float(os.getenv("MDRAGPOINT_THRESHOLD_PERCENTILE", "99.0"))
    threshold_window = int(os.getenv("MDRAGPOINT_THRESHOLD_WINDOW", "500"))
    welford_window = int(os.getenv("MDRAGPOINT_WELFORD_WINDOW", str(max_nb_clusters * max_cluster_size)))

    print(
        "[analyse-mdragpoint] mDragPoint config: "
        f"dynamic_threshold={int(dynamic_threshold)}, lof_init={int(lof_init)}, "
        f"n_init={n_init}, max_nb_clusters={max_nb_clusters}, "
        f"max_cluster_size={max_cluster_size}, metric={metric}, "
        f"internal_threshold={threshold_method}, threshold_t={threshold_t}, "
        f"lof_neighbors={lof_neighbors}.",
        flush=True,
    )
    print(
        "[analyse-mdragpoint] Verdict config: "
        f"mode={verdict_mode}, external_threshold_mode={threshold_mode}, "
        f"warmup_window={anomaly_warmup_window}.",
        flush=True,
    )
    if verdict_mode == "internal":
        print(
            "[analyse-mdragpoint] Internal verdict mode: "
            "mdragpoint_threshold mirrors mdragpoint_internal_r.",
            flush=True,
        )
    elif threshold_mode == "percentile":
        print(
            f"[analyse-mdragpoint] Percentile threshold: P{threshold_percentile}, "
            f"window={threshold_window}.",
            flush=True,
        )
    elif threshold_mode == "welford-windowed":
        print(
            f"[analyse-mdragpoint] Windowed Welford threshold: "
            f"k_sigma={anomaly_k_sigma}, window={welford_window}.",
            flush=True,
        )
    else:
        print(
            f"[analyse-mdragpoint] Welford threshold: k_sigma={anomaly_k_sigma}.",
            flush=True,
        )

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
        except Exception as exc:
            msg = str(exc)
            if "40401" in msg or "Subject not found" in msg:
                if wait_seconds > 0 and waited >= wait_seconds:
                    print(
                        f"[analyse-mdragpoint] Schema Registry subject "
                        f"'{source_subject}' not found after waiting "
                        f"{wait_seconds}s; aborting.",
                        file=sys.stderr,
                        flush=True,
                    )
                    sys.exit(1)
                print(
                    f"[analyse-mdragpoint] Schema Registry subject "
                    f"'{source_subject}' not found yet, waiting "
                    f"{schema_poll_interval}s... (error: {exc})",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(schema_poll_interval)
                waited += schema_poll_interval
                continue
            print(
                f"[analyse-mdragpoint] Error while fetching schema for subject "
                f"'{source_subject}' from Schema Registry: {exc}",
                file=sys.stderr,
                flush=True,
            )
            sys.exit(1)

    source_avro_schema = latest_schema.schema.schema_str
    try:
        dest_avro_schema = extend_mdragpoint_avro_schema(source_avro_schema)
    except Exception as exc:
        print(
            f"[analyse-mdragpoint] Failed to parse source Avro schema as JSON: {exc}",
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

    models_by_group: dict[str, MDragPointGlobalModel] = {}
    welford_by_group: dict[str, WelfordEstimator | WindowedWelfordEstimator] = {}
    percentile_by_group: dict[str, SlidingPercentileEstimator] = {}
    viz_state_by_group: Dict[str, Dict[str, Any]] = {}
    feature_names: Optional[List[str]] = None

    consumer.subscribe([args.source_topic])
    print(
        f"[analyse-mdragpoint] Waiting for new messages on topic "
        f"'{args.source_topic}'...",
        flush=True,
    )

    running = True
    last_msg_ts = time.monotonic()
    processed = 0

    def handle_sigint(_sig, _frame):  # noqa: ANN001
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

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
            except ConsumeError as exc:
                err = exc.args[0] if exc.args else exc
                code = (
                    err.code()
                    if hasattr(err, "code") and callable(getattr(err, "code"))
                    else None
                )
                if (code is not None and code == KafkaError._PARTITION_EOF) or (
                    "_PARTITION_EOF" in str(err)
                ):
                    if idle_exit_sec > 0 and time.monotonic() - last_msg_ts >= idle_exit_sec:
                        print(
                            f"[analyse-mdragpoint] Idle for {idle_exit_sec}s after EOF, exiting.",
                            flush=True,
                        )
                        break
                    continue
                print(f"[analyse-mdragpoint] consumer error: {err}", file=sys.stderr, flush=True)
                continue

            if msg is None:
                if idle_exit_sec > 0 and time.monotonic() - last_msg_ts >= idle_exit_sec:
                    print(
                        f"[analyse-mdragpoint] No messages for {idle_exit_sec}s, exiting.",
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
                if (code is not None and code == KafkaError._PARTITION_EOF) or (
                    "_PARTITION_EOF" in str(err)
                ):
                    if idle_exit_sec > 0 and time.monotonic() - last_msg_ts >= idle_exit_sec:
                        print(
                            f"[analyse-mdragpoint] Idle for {idle_exit_sec}s after EOF, exiting.",
                            flush=True,
                        )
                        break
                    continue
                print(f"[analyse-mdragpoint] consumer error: {err}", file=sys.stderr, flush=True)
                continue

            last_msg_ts = time.monotonic()
            key = msg.key()
            record = msg.value()
            group_key = resolve_group_key(record)
            mdragpoint_score: Optional[float] = None
            model: Optional[MDragPointGlobalModel] = None

            if feature_names is None and isinstance(record, dict):
                numeric_fields = infer_feature_names(record, exclude_fields)
                if not numeric_fields:
                    print(
                        "[analyse-mdragpoint] No numeric fields found; records "
                        "will be forwarded with mdragpoint_score=0.0.",
                        file=sys.stderr,
                        flush=True,
                    )
                else:
                    feature_names = sorted(numeric_fields)
                    print(
                        f"[analyse-mdragpoint] Using {len(feature_names)} features: "
                        f"{feature_names}",
                        flush=True,
                    )
            elif feature_names is not None and isinstance(record, dict):
                current_fields = sorted(infer_feature_names(record, exclude_fields))
                if current_fields and current_fields != feature_names:
                    print(
                        f"[analyse-mdragpoint] WARN feature set changed from "
                        f"{feature_names} to {current_fields}. Resetting models.",
                        flush=True,
                    )
                    feature_names = current_fields
                    models_by_group.clear()
                    welford_by_group.clear()
                    percentile_by_group.clear()

            if feature_names:
                try:
                    x = build_feature_vector(record, feature_names)
                except Exception as exc:
                    print(
                        f"[analyse-mdragpoint] Error while building feature vector: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                    x = None

                if x is not None:
                    model = models_by_group.get(group_key)
                    if model is None:
                        try:
                            model = MDragPointGlobalModel(
                                num_features=len(feature_names),
                                dynamic_threshold=dynamic_threshold,
                                lof_init=lof_init,
                                n_init=n_init,
                                max_nb_clusters=max_nb_clusters,
                                max_cluster_size=max_cluster_size,
                                metric=metric,
                                threshold_method=threshold_method,
                                threshold_t=threshold_t,
                                lof_neighbors=lof_neighbors,
                                seed=seed,
                            )
                            models_by_group[group_key] = model
                            print(
                                f"[analyse-mdragpoint] Created MDragPointGlobalModel "
                                f"for Group='{group_key}' with {len(feature_names)} features.",
                                flush=True,
                            )
                        except Exception as exc:
                            print(
                                f"[analyse-mdragpoint] Failed to instantiate model "
                                f"for Group='{group_key}': {exc}",
                                file=sys.stderr,
                                flush=True,
                            )
                            model = None

                    if model is not None:
                        try:
                            mdragpoint_score = float(model.partial_fit_score(x))
                        except Exception as exc:
                            print(
                                f"[analyse-mdragpoint] Error during scoring for "
                                f"Group='{group_key}': {exc}",
                                file=sys.stderr,
                                flush=True,
                            )
                            mdragpoint_score = None

            status = "warming_up"
            anomaly: Optional[bool] = None
            threshold: Optional[float] = None
            internal_r = model.get_internal_threshold() if model is not None else None

            if model is not None:
                if verdict_mode == "internal":
                    status, anomaly, threshold = classify_mdragpoint_score_internal(
                        mdragpoint_score,
                        model.last_is_anomaly,
                        internal_r,
                        is_warming_up=model.is_warming_up,
                    )
                elif threshold_mode == "percentile":
                    estimator_p = percentile_by_group.setdefault(
                        group_key,
                        SlidingPercentileEstimator(
                            threshold_window, threshold_percentile
                        ),
                    )
                    status, anomaly, threshold = classify_mdragpoint_score_percentile(
                        mdragpoint_score,
                        estimator_p,
                        is_warming_up=model.is_warming_up,
                        min_samples=anomaly_warmup_window,
                    )
                elif threshold_mode == "welford-windowed":
                    welford = welford_by_group.setdefault(
                        group_key, WindowedWelfordEstimator(welford_window)
                    )
                    status, anomaly, threshold = classify_mdragpoint_score(
                        mdragpoint_score,
                        welford,
                        is_warming_up=model.is_warming_up,
                        min_samples=anomaly_warmup_window,
                        k_sigma=anomaly_k_sigma,
                    )
                else:
                    welford = welford_by_group.setdefault(group_key, WelfordEstimator())
                    status, anomaly, threshold = classify_mdragpoint_score(
                        mdragpoint_score,
                        welford,
                        is_warming_up=model.is_warming_up,
                        min_samples=anomaly_warmup_window,
                        k_sigma=anomaly_k_sigma,
                    )

            record, mdragpoint_score = ensure_mdragpoint_analysis_fields(
                record,
                mdragpoint_score,
                status,
                anomaly,
                threshold,
                internal_r,
            )

            reset_delivery_state()
            viz_record = (
                strip_excluded_fields(
                    record,
                    exclude_fields,
                    preserve_fields={"Group", "group", "timestamp", "source_event_id"},
                )
                if isinstance(record, dict)
                else {"mdragpoint_score": mdragpoint_score}
            )

            state = viz_state_by_group.get(group_key)
            if state is None:
                state = create_viz_state()
                viz_state_by_group[group_key] = state
            viz_record = enrich_viz_record(viz_record, state, window_size)
            if viz_record.get("timestamp") in (None, ""):
                resolved_ts = resolve_timestamp_value(record)
                if resolved_ts not in (None, ""):
                    viz_record["timestamp"] = str(resolved_ts)

            try:
                json_payload = json.dumps(viz_record, ensure_ascii=False)
            except Exception as exc:
                print(
                    f"[analyse-mdragpoint] Error while serializing JSON: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                json_payload = "{}"

            _ts_type, ts_ms = msg.timestamp() or (None, None)
            produce_kwargs = {"timestamp": ts_ms} if ts_ms is not None else {}
            producer.produce(
                topic=args.dest_topic,
                key=key,
                value=record,
                on_delivery=on_delivery_avro,
                **produce_kwargs,
            )
            json_producer.produce(
                topic=args.dest_viz_topic,
                key=key,
                value=json_payload,
                on_delivery=on_delivery_json,
                **produce_kwargs,
            )

            ok = wait_for_both_deliveries(timeout_sec=5.0)
            if not ok:
                if delivery_state["avro_err"] is not None:
                    print(
                        f"[analyse-mdragpoint] delivery-error avro: "
                        f"{delivery_state['avro_err']}",
                        file=sys.stderr,
                        flush=True,
                    )
                if delivery_state["json_err"] is not None:
                    print(
                        f"[analyse-mdragpoint] delivery-error json: "
                        f"{delivery_state['json_err']}",
                        file=sys.stderr,
                        flush=True,
                    )
                continue

            processed += 1
            if processed % log_every == 0:
                print(
                    f"[analyse-mdragpoint] Processed={processed} topic={msg.topic()} "
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

    print("[analyse-mdragpoint] done.", flush=True)


if __name__ == "__main__":
    main()
