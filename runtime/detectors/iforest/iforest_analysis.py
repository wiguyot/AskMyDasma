

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""iforest_analysis.py

- Consumes Avro messages from a source topic, default `source-data`.
- Deserializes them through Schema Registry.
- Builds a per-group scikit-learn IsolationForest model, with a sliding reference window, over numeric fields excluding `timestamp`, `Group`, and similar metadata, and uses warmup through an initial window.
- Computes the Isolation Forest anomaly score for each event using the already-fitted forest, then learns from the event.
- Extends the source Avro schema with a nullable double field, `iforest_score`.
- Republishes enriched records to a destination topic, default `iforest-analysis`.
- Also publishes the same enriched records as JSON to a viz topic, default `iforest-analysis-viz`.
- Commits offsets only after successful production, at-least-once.
- Preserves the original Kafka timestamp when present.

This script implements a streaming anomaly detection pipeline for component records.
Each incoming record is scored for anomaly using an Isolation Forest for streaming data,
with scikit-learn `IsolationForest` and a sliding reference window, trained and updated on numeric fields excluding metadata.

Dependencies:
  `pip install confluent-kafka fastavro numpy scikit-learn`

Useful environment variables:
  `BOOTSTRAP_SERVERS`                     Kafka bootstrap servers for the consumer and producers, whitespace trimmed, default `kafka1:9092,kafka2:9092,kafka3:9092`
  `SCHEMA_REGISTRY_URL`                   Schema Registry URL, for example `http://schema-registry:8081`
  `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO`  Optional Basic Auth credentials for Schema Registry, for example `user:password`
  `SCHEMA_REGISTRY_BASIC_AUTH_SOURCE`     Optional auth source for Schema Registry, for example `USER_INFO`

  `ANALYSE_IFOREST_GROUP_ID`              Consumer `group.id`, default `analyse-iforest-consumer`
  `KAFKA_SOURCE_TOPIC`                    Source Kafka topic, default `source-data`
  `KAFKA_DEST_TOPIC`                      Destination Kafka topic, default `iforest-analysis`
  `KAFKA_DEST_VIZ_TOPIC`                 Destination viz topic for JSON, default `iforest-analysis-viz`

  `ANALYSE_SEED`                          Global seed for reproducibility, default `42`
  `ANALYSE_SCHEMA_WAIT_SEC`               Seconds to wait for the source schema, `0` means wait indefinitely
  `ANALYSE_SCHEMA_POLL_INTERVAL_SEC`      Polling interval while waiting for the schema, default `5`
  `ANALYSE_IDLE_EXIT_SEC`                 Exit if idle for this many seconds, `0` disables idle exit

  `IFOREST_EXCLUDE_FIELDS`                Comma-separated list of fields to exclude from the feature vector
                                        default `source_event_id,timestamp,Group,group,Cycle,LabviewTime,time`
  `IFOREST_INITIAL_WINDOW_SIZE`           Number of records to buffer for iForest initialization, default `512`
  `IFOREST_WINDOW_SIZE`                   Sliding reference window size for IsolationForest, default `2048`

  Optional parameters forwarded to the underlying Isolation Forest implementation, scikit-learn
  `IFOREST_N_ESTIMATORS`                  Number of trees, default `100`
  `IFOREST_MAX_SAMPLES`                   Fixed integer subsample size ψ, default `256`
  `IFOREST_CONTAMINATION`                 Native cutoff: `auto` (default) or 0.01–0.50 in steps of 0.01
  `IFOREST_THRESHOLD_MODE`                `native` (default), `p99`, `welford`, or `welford-windowed`
  `IFOREST_REFIT_EVERY`                   Refit cadence, number of scored samples between refits, default `128`

  Optional non-fatal topic layout sanity check
  `TOPIC_PARTITIONS`                      Expected number of partitions for topics, optional, logs a warning if mismatched
  `TOPIC_RF`                              Expected replication factor for topics, optional, logs a warning if mismatched

Notes on reproducibility
------------------------
- NumPy and Python RNG seeds are set through `ANALYSE_SEED`.
- `random_state=ANALYSE_SEED` is passed to scikit-learn `IsolationForest`.

"""

import os
import sys
import signal
import json
import argparse
import time
import random
from pathlib import Path

from typing import Dict, Any

import numpy as np

ROOT_DIR = next(
  parent
  for parent in Path(__file__).resolve().parents
  if (parent / "src" / "dasma_kafka").is_dir()
)
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
  sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.iforest_analysis.helpers import (
  build_feature_vector,
  classify_iforest_native,
  ensure_iforest_analysis_fields,
  infer_feature_names,
  parse_iforest_contamination,
  parse_iforest_max_samples,
  resolve_group_key,
  strip_excluded_fields,
)
from dasma_kafka.iforest_analysis.model import IForestGlobalModel, validate_iforest_params
from dasma_kafka.anomaly.thresholds import (
    WelfordEstimator,
    WindowedWelfordEstimator,
    SlidingPercentileEstimator,
    classify_score,
    classify_score_percentile,
)
from dasma_kafka.common.timestamps import resolve_timestamp_value
from dasma_kafka.common.threshold_modes import (
    IFOREST_ANALYSIS_SCHEMA_FIELDS,
    extend_iforest_avro_schema,
    normalize_iforest_threshold_mode,
)

# Force stdout to be line-buffered to avoid Docker stdout buffering
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

# --- Robust `BOOTSTRAP_SERVERS` handling and topic layout check --------------
from confluent_kafka.admin import AdminClient

# IFOREST_ANALYSIS_SCHEMA_FIELDS, extend_iforest_avro_schema,
# normalize_iforest_threshold_mode are now imported from
# dasma_kafka.common.threshold_modes above.


def _get_bootstrap_servers() -> str:
  """Return `BOOTSTRAP_SERVERS` with whitespace trimmed; fall back to the three-broker default."""
  bs = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
  bs = ",".join([p.strip() for p in bs.split(",") if p.strip()])
  return bs or "kafka1:9092,kafka2:9092,kafka3:9092"

def _topic_partitions_and_rf(admin: AdminClient, topic: str) -> tuple[int, int]:
  """Best-effort: return `(partitions_count, replication_factor)` for a topic."""
  md = admin.list_topics(topic=topic, timeout=10)
  t = md.topics.get(topic)
  if t is None or t.error is not None:
    raise RuntimeError(str(t.error) if t and t.error else f"topic '{topic}' not found")
  partitions = t.partitions or {}
  pcount = len(partitions)
  rf = 0
  if partitions:
    first_pid = sorted(partitions.keys())[0]
    rf = len(partitions[first_pid].replicas or [])
  return pcount, rf

def _check_topics_layout(bootstrap_servers: str, topics: list[str], expected_partitions: int | None, expected_rf: int | None) -> None:
  """Log warnings if existing topics do not match expected `TOPIC_PARTITIONS` / `TOPIC_RF`."""
  if expected_partitions is None and expected_rf is None:
    return
  admin = AdminClient({"bootstrap.servers": bootstrap_servers})
  for t in topics:
    try:
      pcount, rf = _topic_partitions_and_rf(admin, t)
      if expected_partitions is not None and pcount != expected_partitions:
        print(f"[analyse-iforest] WARN topic '{t}' has partitions={pcount} but `TOPIC_PARTITIONS`={expected_partitions}", flush=True)
      if expected_rf is not None and rf != expected_rf:
        print(f"[analyse-iforest] WARN topic '{t}' has replication_factor={rf} but `TOPIC_RF`={expected_rf}", flush=True)
    except Exception as e:
      print(f"[analyse-iforest] WARN cannot inspect topic '{t}': {e}", flush=True)


# --- Schema Registry helpers -------------------------------------------------
def make_sr_client() -> SchemaRegistryClient:
  url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
  conf = {"url": url}
  user_info = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")
  if user_info:
    conf["basic.auth.user.info"] = user_info
    conf["basic.auth.credentials.source"] = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_SOURCE", "USER_INFO")
  return SchemaRegistryClient(conf)


# --- Consumer / Producer builders -------------------------------------------
def make_consumer(group_id: str, value_deserializer: AvroDeserializer) -> DeserializingConsumer:
  conf = {
    "bootstrap.servers": _get_bootstrap_servers(),
    "group.id": group_id,
    "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
    "enable.auto.commit": False,  # manual commit after successful production
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


# JSON producer for viz topic
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


# --- Main --------------------------------------------------------------------
def main():
  if os.getenv("DASMA_ENABLED", "1") == "0":
    print("DASMA_ENABLED=0 — service disabled for this campaign. Idling.", flush=True)
    signal.pause()
    return
  parser = argparse.ArgumentParser(
    description="Consume from 'source-data' and republish enriched records to 'iforest-analysis' (Avro/Schema Registry)."
  )
  parser.add_argument(
    "-s",
    "--source-topic",
    default=os.getenv("KAFKA_SOURCE_TOPIC", "source-data"),
    help="Source topic (default: source-data)",
  )
  parser.add_argument(
    "-d",
    "--dest-topic",
    default=os.getenv("KAFKA_DEST_TOPIC", "iforest-analysis"),
    help="Destination topic (default: iforest-analysis)",
  )
  parser.add_argument(
    "--dest-viz-topic",
    default=os.getenv("KAFKA_DEST_VIZ_TOPIC", "iforest-analysis-viz"),
    help="Destination viz topic for JSON (default: iforest-analysis-viz)",
  )
  parser.add_argument(
    "-g",
    "--group-id",
    default=os.getenv("ANALYSE_IFOREST_GROUP_ID", "analyse-iforest-consumer"),
    help="Consumer group.id (default: analyse-iforest-consumer)",
  )
  args = parser.parse_args()

  # Validate model and native cutoff parameters before touching Kafka.
  try:
    IFOREST_INITIAL_WINDOW_SIZE = int(os.getenv("IFOREST_INITIAL_WINDOW_SIZE", "512"))
    IFOREST_WINDOW_SIZE = int(os.getenv("IFOREST_WINDOW_SIZE", "2048"))
    IFOREST_N_ESTIMATORS = int(os.getenv("IFOREST_N_ESTIMATORS", "100"))
    IFOREST_MAX_SAMPLES = parse_iforest_max_samples(os.getenv("IFOREST_MAX_SAMPLES", "256"))
    IFOREST_CONTAMINATION = parse_iforest_contamination(os.getenv("IFOREST_CONTAMINATION", "auto"))
    IFOREST_REFIT_EVERY = int(os.getenv("IFOREST_REFIT_EVERY", "128"))
    validate_iforest_params(
      IFOREST_INITIAL_WINDOW_SIZE,
      IFOREST_WINDOW_SIZE,
      IFOREST_MAX_SAMPLES,
      IFOREST_N_ESTIMATORS,
      IFOREST_REFIT_EVERY,
    )
  except (TypeError, ValueError) as exc:
    print(str(exc), file=sys.stderr, flush=True)
    sys.exit(1)

  # --- Bootstrap servers and topic layout check ---
  bootstrap_servers = _get_bootstrap_servers()
  print(f"[analyse-iforest] Using BOOTSTRAP_SERVERS={bootstrap_servers}", flush=True)

  expected_partitions = os.getenv("TOPIC_PARTITIONS")
  expected_rf = os.getenv("TOPIC_RF")
  exp_p = int(expected_partitions) if expected_partitions and expected_partitions.strip().isdigit() else None
  exp_rf = int(expected_rf) if expected_rf and expected_rf.strip().isdigit() else None
  if exp_p is not None or exp_rf is not None:
    _check_topics_layout(
      bootstrap_servers,
      topics=[args.source_topic, args.dest_topic, args.dest_viz_topic],
      expected_partitions=exp_p,
      expected_rf=exp_rf,
    )

  # Reproducibility, default 42.
  seed = int(os.getenv("ANALYSE_SEED", "42"))
  np.random.seed(seed)
  random.seed(seed)
  print(f"[analyse-iforest] Reproducibility enabled (ANALYSE_SEED={seed})", flush=True)

  # Optional idle exit
  idle_exit_sec = int(os.getenv("ANALYSE_IDLE_EXIT_SEC", "0"))

  # Schema Registry
  sr = make_sr_client()
  value_deserializer = AvroDeserializer(schema_registry_client=sr)

  # Wait for source schema (topic-value)
  source_subject = f"{args.source_topic}-value"
  wait_seconds = int(os.getenv("ANALYSE_SCHEMA_WAIT_SEC", "0"))  # 0 = wait indefinitely
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
            f"[analyse-iforest] Schema Registry subject '{source_subject}' not found after waiting {wait_seconds}s; aborting.",
            file=sys.stderr,
            flush=True,
          )
          sys.exit(1)
        print(
          f"[analyse-iforest] Schema Registry subject '{source_subject}' not found yet, waiting {schema_poll_interval}s... (error: {e})",
          file=sys.stderr,
          flush=True,
        )
        time.sleep(schema_poll_interval)
        waited += schema_poll_interval
        continue

      print(
        f"[analyse-iforest] Error while fetching schema for subject '{source_subject}' from Schema Registry: {e}",
        file=sys.stderr,
        flush=True,
      )
      sys.exit(1)

  source_avro_schema = latest_schema.schema.schema_str

  # Extend schema with nullable iForest analysis fields.
  try:
    dest_avro_schema = extend_iforest_avro_schema(source_avro_schema)
  except Exception as e:
    print(f"[analyse-iforest] Failed to parse source Avro schema as JSON: {e}", file=sys.stderr, flush=True)
    sys.exit(1)

  value_serializer = AvroSerializer(schema_registry_client=sr, schema_str=dest_avro_schema)

  consumer = make_consumer(args.group_id, value_deserializer)
  producer = make_producer(value_serializer)
  json_producer = make_json_producer()

  last_msg_ts = time.monotonic()

  # Feature selection
  exclude_fields_env = os.getenv("IFOREST_EXCLUDE_FIELDS", "source_event_id,timestamp,Group,group,Cycle,LabviewTime,time")
  IFOREST_EXCLUDE_FIELDS = {name.strip() for name in exclude_fields_env.split(",") if name.strip()}

  # Hyper-parameters
  IFOREST_THRESHOLD_MODE = normalize_iforest_threshold_mode(
    os.getenv("IFOREST_THRESHOLD_MODE", "native")
  )
  IFOREST_THRESHOLD_WINDOW = int(os.getenv("IFOREST_THRESHOLD_WINDOW", "500"))
  IFOREST_THRESHOLD_PERCENTILE = float(os.getenv("IFOREST_THRESHOLD_PERCENTILE", "99.0"))
  IFOREST_ANOMALY_K_SIGMA = float(os.getenv("IFOREST_ANOMALY_K_SIGMA", "3.0"))
  IFOREST_ANOMALY_WARMUP_WINDOW = max(2, int(os.getenv("IFOREST_ANOMALY_WARMUP_WINDOW",
      os.getenv("IFOREST_ANOMALY_MIN_SAMPLES", "50"))))
  IFOREST_WELFORD_WINDOW = int(os.getenv("IFOREST_WELFORD_WINDOW", str(IFOREST_WINDOW_SIZE)))
  print(
    f"[analyse-iforest] iForest anomaly trigger: mode={IFOREST_THRESHOLD_MODE}, "
    f"contamination={IFOREST_CONTAMINATION}.",
    flush=True,
  )
  if IFOREST_THRESHOLD_MODE == "native":
    print(
      f"[analyse-iforest] Native threshold: contamination={IFOREST_CONTAMINATION}; "
      "verdict available after model warmup, without estimator calibration.",
      flush=True,
    )
  elif IFOREST_THRESHOLD_MODE == "p99":
    print(
      f"[analyse-iforest] Percentile threshold: P{IFOREST_THRESHOLD_PERCENTILE}, "
      f"window={IFOREST_THRESHOLD_WINDOW}, warmup_window={IFOREST_ANOMALY_WARMUP_WINDOW}.",
      flush=True,
    )
  elif IFOREST_THRESHOLD_MODE == "welford-windowed":
    print(
      f"[analyse-iforest] Windowed Welford threshold: k_sigma={IFOREST_ANOMALY_K_SIGMA}, "
      f"window={IFOREST_WELFORD_WINDOW}, warmup_window={IFOREST_ANOMALY_WARMUP_WINDOW}.",
      flush=True,
    )
  else:
    print(
      f"[analyse-iforest] Welford threshold: k_sigma={IFOREST_ANOMALY_K_SIGMA}, "
      f"warmup_window={IFOREST_ANOMALY_WARMUP_WINDOW}.",
      flush=True,
    )

  models_by_group: Dict[str, IForestGlobalModel] = {}
  welford_by_group: Dict[str, WelfordEstimator | WindowedWelfordEstimator] = {}
  percentile_by_group: Dict[str, SlidingPercentileEstimator] = {}
  feature_names: Optional[List[str]] = None

  # Graceful shutdown
  running = True

  def handle_sigint(sig, frame):
    nonlocal running
    running = False

  signal.signal(signal.SIGINT, handle_sigint)
  signal.signal(signal.SIGTERM, handle_sigint)

  consumer.subscribe([args.source_topic])
  print(f"[analyse-iforest] Waiting for new messages on topic '{args.source_topic}' (infinite wait)...", flush=True)

  delivery_state: Dict[str, Any] = {"avro_ok": False, "avro_err": None, "json_ok": False, "json_err": None}

  def on_delivery_avro(err, _msg):
    if err is not None:
      delivery_state["avro_ok"] = False
      delivery_state["avro_err"] = err
    else:
      delivery_state["avro_ok"] = True
      delivery_state["avro_err"] = None

  def on_delivery_json(err, _msg):
    if err is not None:
      delivery_state["json_ok"] = False
      delivery_state["json_err"] = err
    else:
      delivery_state["json_ok"] = True
      delivery_state["json_err"] = None

  def reset_delivery_state() -> None:
    delivery_state["avro_ok"] = False
    delivery_state["avro_err"] = None
    delivery_state["json_ok"] = False
    delivery_state["json_err"] = None

  def wait_for_both_deliveries(timeout_sec: float = 5.0) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
      # Drive callbacks for both producers
      producer.poll(0.1)
      json_producer.poll(0.1)

      if delivery_state["avro_err"] is not None or delivery_state["json_err"] is not None:
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
        code = err.code() if hasattr(err, "code") and callable(getattr(err, "code")) else None
        if (code is not None and code == KafkaError._PARTITION_EOF) or "_PARTITION_EOF" in str(err):
          if idle_exit_sec > 0 and (time.monotonic() - last_msg_ts) >= idle_exit_sec:
            print(f"[analyse-iforest] Idle for {idle_exit_sec}s after EOF (exception), exiting.", flush=True)
            break
          continue
        print(f"[consumer-error] {err}", file=sys.stderr, flush=True)
        continue

      if msg is None:
        print("[analyse-iforest] Waiting for new messages...", flush=True)
        if idle_exit_sec > 0 and (time.monotonic() - last_msg_ts) >= idle_exit_sec:
          print(f"[analyse-iforest] No messages for {idle_exit_sec}s, exiting.", flush=True)
          break
        continue

      if msg.error():
        err = msg.error()
        code = err.code() if hasattr(err, "code") and callable(getattr(err, "code")) else None
        if (code is not None and code == KafkaError._PARTITION_EOF) or "_PARTITION_EOF" in str(err):
          if idle_exit_sec > 0 and (time.monotonic() - last_msg_ts) >= idle_exit_sec:
            print(f"[analyse-iforest] Idle for {idle_exit_sec}s after EOF, exiting.", flush=True)
            break
          continue
        print(f"[consumer-error] {err}", file=sys.stderr, flush=True)
        continue

      last_msg_ts = time.monotonic()

      key = msg.key()
      record = msg.value()

      # Determine group key (fallback '__NO_GROUP__')
      group_key = resolve_group_key(record)

      # Infer feature names once from first record
      if feature_names is None and isinstance(record, dict):
        numeric_fields = infer_feature_names(record, IFOREST_EXCLUDE_FIELDS)
        if not numeric_fields:
          print(
            "[analyse-iforest] No numeric fields found to build iForest feature vector; records will be forwarded with iforest_score=0.0.",
            file=sys.stderr,
            flush=True,
          )
        else:
          feature_names = sorted(numeric_fields)
          print(f"[analyse-iforest] Using {len(feature_names)} features for iForest: {feature_names}", flush=True)

      iforest_score: Optional[float] = None
      iforest_model: Optional[IForestGlobalModel] = None

      if feature_names:
        try:
          x = build_feature_vector(record, feature_names)
        except Exception as e:
          print(f"[analyse-iforest] Error while building feature vector: {e}", file=sys.stderr, flush=True)
          x = None

        if x is not None:
          iforest_model = models_by_group.get(group_key)
          if iforest_model is None:
            try:
              iforest_model = IForestGlobalModel(
                num_features=len(feature_names),
                initial_window_size=IFOREST_INITIAL_WINDOW_SIZE,
                window_size=IFOREST_WINDOW_SIZE,
                random_state=seed,
                n_estimators=IFOREST_N_ESTIMATORS,
                max_samples=IFOREST_MAX_SAMPLES,
                refit_every=IFOREST_REFIT_EVERY,
                contamination=IFOREST_CONTAMINATION,
              )
              models_by_group[group_key] = iforest_model
              print(
                f"[analyse-iforest] Created IForestGlobalModel for Group='{group_key}' with "
                f"{len(feature_names)} features, window_size={IFOREST_WINDOW_SIZE}, "
                f"initial_window_size={IFOREST_INITIAL_WINDOW_SIZE}, n_estimators={IFOREST_N_ESTIMATORS}, "
                f"max_samples={IFOREST_MAX_SAMPLES}, refit_every={IFOREST_REFIT_EVERY}, "
                f"contamination={IFOREST_CONTAMINATION}.",
                flush=True,
              )
            except Exception as e:
              print(
                f"[analyse-iforest] Failed to instantiate IForestGlobalModel for Group='{group_key}': {e}",
                file=sys.stderr,
                flush=True,
              )
              iforest_model = None

          if iforest_model is not None:
            try:
              iforest_score = float(iforest_model.partial_fit_score(x))
            except Exception as e:
              print(
                f"[analyse-iforest] Error during iForest scoring for Group='{group_key}': {e}",
                file=sys.stderr,
                flush=True,
              )
              iforest_score = None

      iforest_status = "warming_up"
      iforest_anomaly: Optional[bool] = None
      iforest_threshold: Optional[float] = None
      if iforest_model is not None:
        if IFOREST_THRESHOLD_MODE == "native":
          iforest_status, iforest_anomaly, iforest_threshold = classify_iforest_native(
            iforest_score,
            iforest_model.last_scoring_threshold,
            is_warming_up=iforest_model.is_warming_up,
          )
        elif IFOREST_THRESHOLD_MODE == "p99":
          estimator_p = percentile_by_group.setdefault(
            group_key,
            SlidingPercentileEstimator(IFOREST_THRESHOLD_WINDOW, IFOREST_THRESHOLD_PERCENTILE),
          )
          iforest_status, iforest_anomaly, iforest_threshold = classify_score_percentile(
            iforest_score,
            estimator_p,
            is_warming_up=iforest_model.is_warming_up,
            min_samples=IFOREST_ANOMALY_WARMUP_WINDOW,
          )
        elif IFOREST_THRESHOLD_MODE == "welford-windowed":
          welford = welford_by_group.setdefault(
            group_key, WindowedWelfordEstimator(IFOREST_WELFORD_WINDOW)
          )
          iforest_status, iforest_anomaly, iforest_threshold = classify_score(
            iforest_score,
            welford,
            is_warming_up=iforest_model.is_warming_up,
            min_samples=IFOREST_ANOMALY_WARMUP_WINDOW,
            k_sigma=IFOREST_ANOMALY_K_SIGMA,
          )
        else:
          welford = welford_by_group.setdefault(group_key, WelfordEstimator())
          iforest_status, iforest_anomaly, iforest_threshold = classify_score(
            iforest_score,
            welford,
            is_warming_up=iforest_model.is_warming_up,
            min_samples=IFOREST_ANOMALY_WARMUP_WINDOW,
            k_sigma=IFOREST_ANOMALY_K_SIGMA,
          )

      record, iforest_score = ensure_iforest_analysis_fields(
        record,
        iforest_score,
        iforest_status,
        iforest_anomaly,
        iforest_threshold,
      )

      # Print processed row (useful in teaching)
      try:
        print(json.dumps(record, ensure_ascii=False), flush=True)
      except Exception:
        print(str(record), flush=True)

      # Produce to both Avro topic and JSON viz topic, then commit the input offset only if both were delivered.
      reset_delivery_state()

      # Build JSON payload (ensure_ascii=False to keep labels readable)
      viz_record = (
        strip_excluded_fields(
          record,
          IFOREST_EXCLUDE_FIELDS,
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
        print(f"[analyse-iforest] Error while serializing JSON for viz topic: {e}", file=sys.stderr, flush=True)
        json_payload = "{}"

      # Preserve Kafka timestamp
      _ts_type, ts_ms = msg.timestamp() or (None, None)
      if ts_ms is not None:
        producer.produce(topic=args.dest_topic, key=key, value=record, timestamp=ts_ms, on_delivery=on_delivery_avro)
        json_producer.produce(
          topic=args.dest_viz_topic,
          key=key,
          value=json_payload,
          timestamp=ts_ms,
          on_delivery=on_delivery_json,
        )
      else:
        producer.produce(topic=args.dest_topic, key=key, value=record, on_delivery=on_delivery_avro)
        json_producer.produce(topic=args.dest_viz_topic, key=key, value=json_payload, on_delivery=on_delivery_json)

      ok = wait_for_both_deliveries(timeout_sec=5.0)
      if not ok:
        if delivery_state["avro_err"] is not None:
          print(f"[analyse-iforest] delivery-error avro: {delivery_state['avro_err']}", file=sys.stderr, flush=True)
        if delivery_state["json_err"] is not None:
          print(f"[analyse-iforest] delivery-error json: {delivery_state['json_err']}", file=sys.stderr, flush=True)
        # Do not commit: message will be reprocessed (at-least-once)
        continue

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
  print("End of analyse-iforest & Kafka forwarding", flush=True)
  print("--------------", flush=True)


if __name__ == "__main__":
  # Print sklearn version for diagnostics
  try:
    import sklearn
    print(f"[analyse-iforest] sklearn={sklearn.__version__}", flush=True)
  except Exception:
    pass
  main()
