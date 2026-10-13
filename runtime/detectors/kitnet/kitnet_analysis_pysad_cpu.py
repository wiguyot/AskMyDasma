

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""kitnet_analysis_pysad_cpu.py

DASMA-Kafka — Streaming anomaly detection with **KitNET** (via PySAD, CPU / NumPy).

Important scope note (Kitsune vs KitNET)
---------------------------------------
The original research system **Kitsune** is a pipeline:
  Feature Extractor (AfterImage) -> Feature Mapper (FM) -> Anomaly Detector (AD = KitNET).

This script implements **KitNET only** (FM + AD as provided by the underlying KitNET-py code
wrapped by `pysad.models.KitNet`). It **does not implement AfterImage**.

Therefore, this consumer expects that the Kafka records already contain a suitable numerical
feature vector (i.e., features engineered upstream).

What this script does
---------------------
- Consumes Avro records from a source topic (default: `source-data`).
- Deserializes via Confluent Schema Registry.
- Builds and maintains **one KitNET model per group** (record['Group'] preferred,
  fallback record['group'], else "__NO_GROUP__").
- Computes a continuous anomaly score `kitnet_score` for each record.
- Extends the source Avro schema with a nullable double field `kitnet_score`.
- Republishes enriched records to a destination topic (default: `kitnet-analysis`).
- Optionally publishes the same enriched records as JSON to a viz topic
  (default: `kitnet-analysis-viz`) when `KAFKA_DEST_VIZ_TOPIC` is non-empty.
- Commits offsets only after successful production (Avro + JSON if enabled).
- Preserves the original Kafka timestamp when present.

How the feature vector is built (very important)
-----------------------------------------------
The feature vector `x` is built by iterating over all fields in the record and keeping
those that can be converted to float, excluding fields listed in `KITNET_EXCLUDE_FIELDS`.

**KitNET assumes a stable feature ordering and meaning across messages.**
If the order/identity of fields changes (schema evolution, different insertion order, etc.),
then the model will see different semantics for the same index and quality will degrade.

This script also resets a per-group model if the feature count changes.

NaN handling
------------
If any selected feature value is NaN, this script returns a score of 0.0 for that message.
(Instance is effectively ignored by the underlying model call.)

Grace periods
-------------
KitNET has two training phases:
- Feature Mapper (FM) grace period: learns how to cluster/correlate features.
- Anomaly Detector (AD) grace period: trains the autoencoders before scores become meaningful.

During these grace periods the underlying KitNET implementation typically returns 0.0
(as designed).

Environment variables
---------------------
Kafka / Schema Registry
- BOOTSTRAP_SERVERS                     Kafka bootstrap servers (default: 'kafka1:9092')
- SCHEMA_REGISTRY_URL                   Schema Registry URL (default: 'http://schema-registry:8081')
- SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO  Optional 'user:password' for Schema Registry
- SCHEMA_REGISTRY_BASIC_AUTH_CREDENTIALS_SOURCE Optional (default: 'USER_INFO')

Topics
- KAFKA_SOURCE_TOPIC                    Source topic (default: source-data)
- KAFKA_DEST_TOPIC                      Destination topic (default: kitnet-analysis)
- KAFKA_DEST_VIZ_TOPIC                 JSON viz topic (default: kitnet-analysis-viz)
                                        Set to empty string to disable JSON output.

Consumer
- ANALYSE_KITNET_GROUP_ID               Consumer group id (default: analyse-kitnet-consumer)
- AUTO_OFFSET_RESET                     'earliest' or 'latest' (default: 'earliest')

Reproducibility / control
- ANALYSE_SEED                          Global seed (default: 42)
- ANALYSE_SCHEMA_WAIT_SEC               Wait for source schema (0 = wait indefinitely)
- ANALYSE_SCHEMA_POLL_INTERVAL_SEC      Poll interval while waiting for schema (default: 5)
- ANALYSE_IDLE_EXIT_SEC                 Exit if idle for this many seconds (0 disables)

Feature selection
- KITNET_EXCLUDE_FIELDS                 Comma-separated list of fields excluded from `x`
                                        (default: 'source_event_id,timestamp,Group,group,name,time')

KitNET parameters (PySAD -> KitNET-py)
- KITNET_MAX_SIZE_AE                    max_size_ae (default: 10)
- KITNET_GRACE_FEATURE_MAPPING          FM grace period (int) or 'None' to disable (default: None)
- KITNET_GRACE_ANOMALY_DETECTOR         AD grace period (default: 50000)
- KITNET_LEARNING_RATE                  learning_rate (default: 0.1)
- KITNET_HIDDEN_RATIO                   hidden_ratio (default: 0.75)

Backward-compatible aliases
- KITNET_FM_GRACE_PERIOD                alias for KITNET_GRACE_FEATURE_MAPPING
- KITNET_AD_GRACE_PERIOD                alias for KITNET_GRACE_ANOMALY_DETECTOR

Sanity checks (non-blocking)
- TOPIC_PARTITIONS                      Expected partitions (default: 1)
- TOPIC_RF                              Expected replication factor (default: 3)

Dependencies
------------
Python packages:
  confluent-kafka, fastavro, pysad, numpy, pandas, attrs, certifi, httpx, cachetools, authlib

"""

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
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.kitnet_analysis.helpers import (
    ensure_kitnet_analysis_fields,
    parse_exclude_fields,
    parse_grace_anomaly_detector,
    parse_grace_feature_mapping,
    resolve_group_key,
    select_features,
    strip_excluded_fields,
)
from dasma_kafka.anomaly.thresholds import (
    WelfordEstimator,
    WindowedWelfordEstimator,
    SlidingPercentileEstimator,
    classify_score,
    classify_score_percentile,
)
from dasma_kafka.common.timestamps import resolve_timestamp_value
from dasma_kafka.common.threshold_modes import (
    KITNET_ANALYSIS_SCHEMA_FIELDS,
    extend_kitnet_avro_schema,
    normalize_kitnet_threshold_mode,
)
from dasma_kafka.kitnet_analysis.model import KitNetGroupModels, KitNetParams

from confluent_kafka import DeserializingConsumer, KafkaError, KafkaException, Message
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serializing_producer import SerializingProducer

# KITNET_ANALYSIS_SCHEMA_FIELDS, extend_kitnet_avro_schema,
# normalize_kitnet_threshold_mode are now imported from
# dasma_kafka.common.threshold_modes above.


# --------------------------------------------------------------------------------------
# 1) Helpers: environment, Schema Registry, and small utilities
#
# Goal for students:
# - Keep all configuration in env vars (12-factor style)
# - Hide Kafka/SR boilerplate behind small functions
# - Make the main loop readable
# --------------------------------------------------------------------------------------

# Read an environment variable.
# - If unset or empty, return the provided default.
# - We treat empty string as "unset" to simplify docker-compose overrides.
def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    return default if (v is None or v == "") else v


# Same as _env(), but converts to int. If conversion fails, fall back to default.
def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except Exception:
        return default


# Same as _env(), but converts to float. If conversion fails, fall back to default.
def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except Exception:
        return default


def _get_bootstrap_servers() -> str:
    return _env("BOOTSTRAP_SERVERS", "kafka1:9092")


def _get_schema_registry_url() -> str:
    return _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


# Build a Schema Registry client.
# In most labs we use an open SR, but basic-auth is supported for completeness.
def _sr_client() -> SchemaRegistryClient:
    conf: Dict[str, Any] = {"url": _get_schema_registry_url()}
    # Optional basic auth
    user_info = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")
    if user_info:
        conf["basic.auth.user.info"] = user_info
        conf["basic.auth.credentials.source"] = _env("SCHEMA_REGISTRY_BASIC_AUTH_CREDENTIALS_SOURCE", "USER_INFO")
    return SchemaRegistryClient(conf)


def _wait_for_latest_schema(
    sr: SchemaRegistryClient,
    subject: str,
    wait_sec: int,
    poll_interval_sec: int,
) -> str:
    """Wait for Schema Registry to expose a latest version for `subject`.

    Why we wait:
    - In docker-compose labs, producers/Schema Registry may start slightly after this consumer.
    - We prefer "wait + retry" instead of failing immediately.

    Returns:
      The Avro schema string (JSON text) for the latest version of the subject.
    """
    start = time.monotonic()
    while True:
        try:
            latest = sr.get_latest_version(subject)
            return latest.schema.schema_str
        except Exception:
            if wait_sec != 0 and (time.monotonic() - start) >= wait_sec:
                raise
            time.sleep(poll_interval_sec)


# Create a Kafka consumer configured for:
# - manual commits (we commit only after the enriched message is produced)
# - Avro value deserialization (Schema Registry)
# - string keys
def _make_consumer(group_id: str, value_deserializer: AvroDeserializer) -> DeserializingConsumer:
    # Kafka Consumer configuration notes (teaching):
    # - group.id: identifies the consumer group (parallelism + offset tracking).
    # - auto.offset.reset: what to do when there is no committed offset yet:
    #     * earliest: start from the beginning (good for labs / replay)
    #     * latest: start from "now" (good for real-time)
    # - enable.auto.commit=False: we commit manually after successful production.
    #   This avoids "losing" messages when the output produce fails.
    # - max.poll.interval.ms / session.timeout.ms: consumer liveness settings.
    #   (If processing is too slow, Kafka may consider the consumer dead and rebalance.)
    conf = {
        "bootstrap.servers": _get_bootstrap_servers(),
        "group.id": group_id,  # consumer group name (offsets are tracked per group)
        "auto.offset.reset": _env("AUTO_OFFSET_RESET", "earliest"),  # earliest/latest when no offset is committed yet
        "enable.auto.commit": False,  # manual commit (commit only after produce succeeds)
        "key.deserializer": StringDeserializer("utf_8"),  # keys are UTF-8 strings
        "value.deserializer": value_deserializer,  # Avro -> Python dict (Schema Registry)
        "max.poll.interval.ms": 300000,  # max processing time between polls before rebalance
        "session.timeout.ms": 45000,  # heartbeat/session timeout for group membership
    }
    return DeserializingConsumer(conf)


# Create an idempotent Kafka producer for Avro values.
# Idempotence + max.in.flight=1 makes "produce then commit" safer in teaching labs.
def _make_producer(value_serializer: AvroSerializer) -> SerializingProducer:
    # Kafka Producer configuration notes (teaching):
    # - enable.idempotence=True + acks=all: stronger delivery guarantees.
    #   The broker will de-duplicate retries for the same producer session.
    # - max.in.flight.requests.per.connection=1: keeps message order on retries.
    #   (Without this, retries can be re-ordered when multiple requests are in flight.)
    # - retries set to a very large number: keep retrying transient errors.
    # - delivery.timeout.ms: overall time limit for delivering a message (including retries).
    #
    # Important:
    # This does NOT magically make the pipeline "exactly-once" end-to-end.
    # We approximate reliable processing with the pattern:
    #   consume -> produce -> commit
    # If produce fails, we do not commit, so the message can be retried.
    conf = {
        "bootstrap.servers": _get_bootstrap_servers(),
        "enable.idempotence": True,  # broker-side de-duplication on retries
        "acks": "all",  # wait for all in-sync replicas
        "max.in.flight.requests.per.connection": 1,  # preserve order when retries happen
        "retries": 2_147_483_647,  # practically infinite retries
        "delivery.timeout.ms": 300000,  # total delivery time budget per message
        "key.serializer": StringSerializer("utf_8"),  # keys are UTF-8 strings
        "value.serializer": value_serializer,  # Avro serialization using extended schema
    }
    return SerializingProducer(conf)


# Optional second producer for JSON output (viz/debug topic).
# Kept separate so we can disable it without affecting the Avro pipeline.
def _make_json_producer() -> SerializingProducer:
    # JSON producer uses the same reliability settings as the Avro producer.
    # This topic is typically used for dashboards or quick debugging (human-readable payload).
    conf = {
        "bootstrap.servers": _get_bootstrap_servers(),
        "enable.idempotence": True,  # same reliability guarantees
        "acks": "all",  # wait for all in-sync replicas
        "max.in.flight.requests.per.connection": 1,  # preserve order when retries happen
        "retries": 2_147_483_647,  # practically infinite retries
        "delivery.timeout.ms": 300000,  # total delivery time budget per message
        "key.serializer": StringSerializer("utf_8"),  # keys are UTF-8 strings
        "value.serializer": StringSerializer("utf_8"),  # JSON payload as UTF-8 string
    }
    return SerializingProducer(conf)


# --------------------------------------------------------------------------------------
# 2) KitNET per-group wrapper
#
# Why one model per group?
# - In DASMA we often have "contexts" (e.g., different chemical groups / stations / lines).
# - A single global model can be less stable if distributions differ strongly.
#
# Trade-off:
# - Each group needs enough samples to learn its Feature Mapper (FM) and Autoencoders (AD).
# --------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------
# 3) Main: wiring Kafka + Schema Registry + online scoring loop
#
# Teaching objectives:
# - Read -> Score -> Produce -> Commit (exactly once-like behavior at the application level)
# - Keep schema evolution simple: we extend the source schema with one nullable field
# --------------------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="DASMA-Kafka: KitNET anomaly detection (PySAD / CPU)")
    p.add_argument("--source-topic", default=_env("KAFKA_SOURCE_TOPIC", "source-data"))
    p.add_argument("--dest-topic", default=_env("KAFKA_DEST_TOPIC", "kitnet-analysis"))
    p.add_argument("--dest-viz-topic", default=_env("KAFKA_DEST_VIZ_TOPIC", "kitnet-analysis-viz"))
    p.add_argument("--group-id", default=_env("ANALYSE_KITNET_GROUP_ID", "analyse-kitnet-consumer"))
    return p


def main() -> int:
    if os.getenv("DASMA_ENABLED", "1") == "0":
        print("DASMA_ENABLED=0 — service disabled for this campaign. Idling.", flush=True)
        signal.pause()
        return 0
    # Reproducibility (best-effort): seed Python's RNG and NumPy.
    # Note: KitNET internal randomness is limited, but seeding is good practice in labs.
    seed = _env_int("ANALYSE_SEED", 42)
    random.seed(seed)
    np.random.seed(seed)

    args = _build_arg_parser().parse_args()

    schema_wait_sec = _env_int("ANALYSE_SCHEMA_WAIT_SEC", 0)
    schema_poll_sec = _env_int("ANALYSE_SCHEMA_POLL_INTERVAL_SEC", 5)
    idle_exit_sec = _env_int("ANALYSE_IDLE_EXIT_SEC", 0)

    # Sanity checks (non-blocking): kept for parity with other analyzers.
    # In this script we read the env vars but do not enforce them.
    _ = _env_int("TOPIC_PARTITIONS", 1)
    _ = _env_int("TOPIC_RF", 3)

    # Decide which fields are NOT part of the feature vector (timestamps, labels, identifiers, etc.).
    exclude_fields = parse_exclude_fields(_env("KITNET_EXCLUDE_FIELDS", "source_event_id,timestamp,Group,group,name,time"))

    # KitNET parameters (PySAD wrapper). These are usually tuned per dataset/lab.
    max_size_ae = _env_int("KITNET_MAX_SIZE_AE", 10)

    # Prefer explicit PySAD names, then aliases
    # Grace periods:
    # - FM grace period can be "None" (disable FM learning window limit)
    # - AD grace period is the number of instances used to train autoencoders before scores become meaningful
    gfm_raw = os.getenv("KITNET_GRACE_FEATURE_MAPPING")
    if gfm_raw is None or gfm_raw == "":
        gfm_raw = os.getenv("KITNET_FM_GRACE_PERIOD")
    grace_feature_mapping = parse_grace_feature_mapping(gfm_raw)

    gad_raw = os.getenv("KITNET_GRACE_ANOMALY_DETECTOR")
    if gad_raw is None or gad_raw == "":
        gad_raw = os.getenv("KITNET_AD_GRACE_PERIOD")
    grace_anomaly_detector = parse_grace_anomaly_detector(gad_raw, 50000)

    learning_rate = _env_float("KITNET_LEARNING_RATE", 0.1)
    hidden_ratio = _env_float("KITNET_HIDDEN_RATIO", 0.75)
    kitnet_threshold_mode = normalize_kitnet_threshold_mode(
        os.getenv("KITNET_THRESHOLD_MODE", "welford-windowed")
    )
    kitnet_threshold_window = _env_int("KITNET_THRESHOLD_WINDOW", 500)
    kitnet_threshold_percentile = _env_float("KITNET_THRESHOLD_PERCENTILE", 99.0)
    kitnet_anomaly_k_sigma = _env_float("KITNET_ANOMALY_K_SIGMA", 3.0)
    kitnet_anomaly_warmup_window = max(2, _env_int("KITNET_ANOMALY_WARMUP_WINDOW",
        _env_int("KITNET_ANOMALY_MIN_SAMPLES", 50)))
    kitnet_welford_window = _env_int("KITNET_WELFORD_WINDOW", kitnet_threshold_window)

    params = KitNetParams(
        max_size_ae=max_size_ae,
        grace_feature_mapping=grace_feature_mapping,
        grace_anomaly_detector=grace_anomaly_detector,
        learning_rate=learning_rate,
        hidden_ratio=hidden_ratio,
    )
    kitnet_models = KitNetGroupModels(params)
    welford_by_group: Dict[str, WelfordEstimator | WindowedWelfordEstimator] = {}
    percentile_by_group: Dict[str, SlidingPercentileEstimator] = {}

    # Connect to Schema Registry to fetch the source Avro schema (subject: <topic>-value).
    sr = _sr_client()

    # Schema Registry subject convention for Avro values: "<topic>-value".
    source_subject = f"{args.source_topic}-value"

    try:
        source_avro_schema = _wait_for_latest_schema(sr, source_subject, schema_wait_sec, schema_poll_sec)
    except Exception as e:
        print(f"[analyse-kitnet] Failed to get latest schema for subject={source_subject}: {e}", file=sys.stderr, flush=True)
        return 2

    value_deserializer = AvroDeserializer(schema_registry_client=sr, schema_str=source_avro_schema)

    # Extend the source Avro schema with nullable KitNET analysis fields.
    try:
        dest_avro_schema = extend_kitnet_avro_schema(source_avro_schema)
    except Exception as e:
        print(f"[analyse-kitnet] Failed to parse source Avro schema as JSON: {e}", file=sys.stderr, flush=True)
        return 2

    value_serializer = AvroSerializer(schema_registry_client=sr, schema_str=dest_avro_schema)

    # Create consumer + producer(s). We use manual commit to avoid losing messages when produce fails.
    consumer = _make_consumer(args.group_id, value_deserializer)
    producer = _make_producer(value_serializer)

    # JSON output is optional (useful for dashboards / quick inspection). Set dest-viz-topic to empty to disable.
    json_enabled = bool(args.dest_viz_topic and args.dest_viz_topic.strip())
    json_producer = _make_json_producer() if json_enabled else None

    # Start consuming from the source topic.
    consumer.subscribe([args.source_topic])

    last_msg_ts = time.monotonic()

    # Delivery coordination (Avro + JSON):
    # We commit offsets only if BOTH deliveries succeeded.
    # This is a simple way to teach reliable streaming patterns.
    delivery_state: Dict[str, Any] = {"avro_ok": False, "json_ok": False, "avro_err": None, "json_err": None}

    def reset_delivery_state() -> None:
        delivery_state["avro_ok"] = False
        delivery_state["json_ok"] = (not json_enabled)
        delivery_state["avro_err"] = None
        delivery_state["json_err"] = None

    def on_delivery_avro(err, msg):
        if err is not None:
            delivery_state["avro_err"] = err
        else:
            delivery_state["avro_ok"] = True

    def on_delivery_json(err, msg):
        if err is not None:
            delivery_state["json_err"] = err
        else:
            delivery_state["json_ok"] = True

    # Poll producers until both deliveries are acknowledged or an error occurs.
    def wait_for_both(timeout_sec: float) -> bool:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            producer.poll(0)
            if json_enabled and json_producer is not None:
                json_producer.poll(0)
            if delivery_state["avro_ok"] and delivery_state["json_ok"]:
                return True
            if delivery_state["avro_err"] is not None or delivery_state["json_err"] is not None:
                return False
            time.sleep(0.01)
        return False

    print(
        "[analyse-kitnet] started "
        f"source={args.source_topic} dest={args.dest_topic} viz={args.dest_viz_topic if json_enabled else '(disabled)'} "
        f"bootstrap={_get_bootstrap_servers()} schema_registry={_get_schema_registry_url()} "
        f"exclude_fields={exclude_fields} kitnet(max_size_ae={max_size_ae}, grace_feature_mapping={grace_feature_mapping}, "
        f"grace_anomaly_detector={grace_anomaly_detector}, learning_rate={learning_rate}, hidden_ratio={hidden_ratio}) "
        f"trigger(mode={kitnet_threshold_mode}, k_sigma={kitnet_anomaly_k_sigma}, warmup_window={kitnet_anomaly_warmup_window}"
        + (f", welford_window={kitnet_welford_window}" if kitnet_threshold_mode == "welford-windowed" else "")
        + ")",
        flush=True,
    )

    try:
        while True:
            # Poll for one message. Timeout keeps the loop responsive (idle exit + Ctrl-C).
            msg: Message = consumer.poll(1.0)

            # Optional idle exit: useful in CI / automated lab runs when topics are finite.
            if msg is None:
                if idle_exit_sec and (time.monotonic() - last_msg_ts) > idle_exit_sec:
                    print(f"[analyse-kitnet] idle for >{idle_exit_sec}s -> exiting", flush=True)
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            last_msg_ts = time.monotonic()

            key = msg.key()  # already deserialized (StringDeserializer)
            record = msg.value()  # already deserialized (AvroDeserializer)

            if record is None:
                continue
            if not isinstance(record, dict):
                # AvroDeserializer should return dict, but be defensive.
                try:
                    record = dict(record)  # type: ignore
                except Exception:
                    continue

            # Determine the group/context for this record.
            # We support both 'Group' and 'group' field names for compatibility.
            group = resolve_group_key(record)

            # Build feature vector x from the record (see _select_features for the exact policy).
            _feature_names, x = select_features(record, exclude_fields)

            # Online scoring (and training) for this group.
            score = kitnet_models.score(group, x)
            if kitnet_threshold_mode == "p99":
                estimator_p = percentile_by_group.setdefault(
                    group,
                    SlidingPercentileEstimator(kitnet_threshold_window, kitnet_threshold_percentile),
                )
                kitnet_status, kitnet_anomaly, kitnet_threshold = classify_score_percentile(
                    score,
                    estimator_p,
                    is_warming_up=kitnet_models.is_warming_up(group),
                    min_samples=kitnet_anomaly_warmup_window,
                )
            elif kitnet_threshold_mode == "welford-windowed":
                welford = welford_by_group.setdefault(
                    group, WindowedWelfordEstimator(kitnet_welford_window)
                )
                kitnet_status, kitnet_anomaly, kitnet_threshold = classify_score(
                    score,
                    welford,
                    is_warming_up=kitnet_models.is_warming_up(group),
                    min_samples=kitnet_anomaly_warmup_window,
                    k_sigma=kitnet_anomaly_k_sigma,
                )
            else:
                welford = welford_by_group.setdefault(group, WelfordEstimator())
                kitnet_status, kitnet_anomaly, kitnet_threshold = classify_score(
                    score,
                    welford,
                    is_warming_up=kitnet_models.is_warming_up(group),
                    min_samples=kitnet_anomaly_warmup_window,
                    k_sigma=kitnet_anomaly_k_sigma,
                )
            record, score = ensure_kitnet_analysis_fields(
                record,
                score,
                kitnet_status,
                kitnet_anomaly,
                kitnet_threshold,
            )

            # Print to stdout (handy in labs to see what is produced without Kafka tooling).
            try:
                print(json.dumps(record, ensure_ascii=False), flush=True)
            except Exception:
                print(str(record), flush=True)

            # Produce to Kafka:
            # 1) Avro enriched record to dest-topic
            # 2) optional JSON copy to viz topic
            # Then commit the source offset ONLY if both deliveries succeeded.
            reset_delivery_state()

            json_payload = "{}"
            if json_enabled and json_producer is not None:
                viz_record = (
                    strip_excluded_fields(
                        record,
                        exclude_fields,
                        preserve_fields={"Group", "group", "timestamp", "source_event_id"},
                    )
                    if isinstance(record, dict)
                    else {}
                )
                if viz_record.get("timestamp") in (None, ""):
                    resolved_ts = resolve_timestamp_value(record)
                    if resolved_ts not in (None, ""):
                        viz_record["timestamp"] = str(resolved_ts)
                try:
                    json_payload = json.dumps(viz_record, ensure_ascii=False)
                except Exception as e:
                    print(f"[analyse-kitnet] Error while serializing JSON for viz topic: {e}", file=sys.stderr, flush=True)
                    json_payload = "{}"

            # Preserve the original Kafka message timestamp when present (so Grafana time-series stay aligned).
            _ts_type, ts_ms = msg.timestamp() or (None, None)
            if ts_ms is not None:
                producer.produce(topic=args.dest_topic, key=key, value=record, timestamp=ts_ms, on_delivery=on_delivery_avro)
                if json_enabled and json_producer is not None:
                    json_producer.produce(topic=args.dest_viz_topic, key=key, value=json_payload, timestamp=ts_ms, on_delivery=on_delivery_json)
            else:
                producer.produce(topic=args.dest_topic, key=key, value=record, on_delivery=on_delivery_avro)
                if json_enabled and json_producer is not None:
                    json_producer.produce(topic=args.dest_viz_topic, key=key, value=json_payload, on_delivery=on_delivery_json)

            # We wait for delivery reports (acks) from Kafka before committing the offset.
            # This is the key reliability pattern in this script.
            # - If delivery succeeds: commit offset => message is considered processed.
            # - If delivery fails: do NOT commit => message will be re-processed (at-least-once).
            ok = wait_for_both(timeout_sec=5.0)
            if not ok:
                if delivery_state["avro_err"] is not None:
                    print(f"[analyse-kitnet] delivery error (avro): {delivery_state['avro_err']}", file=sys.stderr, flush=True)
                if delivery_state["json_err"] is not None:
                    print(f"[analyse-kitnet] delivery error (json): {delivery_state['json_err']}", file=sys.stderr, flush=True)
                # Delivery failed => do NOT commit. The message will be retried on next poll.
                continue

            # Synchronous commit: easier to reason about in labs (commit has completed when the call returns).
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print("[analyse-kitnet] interrupted", flush=True)
    except Exception as e:
        print(f"[analyse-kitnet] fatal error: {e}", file=sys.stderr, flush=True)
        return 1
    finally:
        # Flush producers and close consumer cleanly (best-effort).
        try:
            producer.flush(10)
        except Exception:
            pass
        if json_enabled and json_producer is not None:
            try:
                json_producer.flush(10)
            except Exception:
                pass
        try:
            consumer.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
