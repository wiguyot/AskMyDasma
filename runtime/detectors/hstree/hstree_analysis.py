#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hstree_analysis.py

- Consumes Avro messages from a source topic (default: `source-data`).
- Deserializes them through Schema Registry.
- Builds one PySAD HalfSpaceTrees model per logical group over numeric fields, excluding `timestamp`, `Group`, and similar metadata, with warmup through an initial window.
- Supports the legacy `ANALYSE_GROUP` variable for compatibility, but the current implementation runs in multi-group mode and does not filter on it.
- Computes a continuous anomaly score, `hstree_score`, for each processed record, `0.0` during warmup or on error.
- Adds an explicit HSTree trigger verdict:
  - `hstree_status`: `warming_up`, `normal`, or `anomaly`
  - `hstree_anomaly`: boolean verdict, `null` while warming up
  - `hstree_threshold`: current per-group threshold, `null` while warming up
- Extends the source Avro schema with nullable HSTree analysis fields.
- Republishes enriched records to a destination topic, default `hstree-analysis`.
- Also publishes a JSON version, with derived fields, to a viz topic, default `hstree-analysis-viz`.
- The JSON published to the viz topic reproduces the same derived fields as the legacy visualization conversion script:
  - `hstree_score_log10`
  - `hstree_score_log10_diff`
  - If `WINDOWS_SIZE > 0`: moving average fields
    - `hstree_score_ma_<N>`
    - `hstree_score_ma_<N>_log10`
    - `hstree_score_ma_<N>_log10_diff`
- Commits offsets only after successful production, at-least-once.
- Preserves the original Kafka timestamp when present.
- Logs the effective `BOOTSTRAP_SERVERS` and, optionally, warns if topic partitions or replication differ from `TOPIC_PARTITIONS` / `TOPIC_RF`, non-fatal.

This script implements a streaming anomaly detection pipeline for component records.
Each incoming record is scored by the HalfSpaceTrees model associated with its group.
The anomaly score and trigger fields are added to the output Avro record.
The legacy `ANALYSE_GROUP` environment variable is accepted for compatibility, but the current implementation processes all groups.
Offsets are committed only after successful production to the destination topic.

Dependencies:
  `pip install confluent-kafka fastavro pysad numpy`

Useful environment variables:
  `BOOTSTRAP_SERVERS`                Kafka bootstrap servers used by the consumer and producers. Whitespace is trimmed, so trailing spaces are tolerated. Default: `kafka1:9092,kafka2:9092,kafka3:9092`
  `TOPIC_PARTITIONS`                Optional expected partition count for the involved topics. Used for a non-fatal sanity check at startup.
  `TOPIC_RF`                        Optional expected replication factor for the involved topics. Used for a non-fatal sanity check at startup.
  `SCHEMA_REGISTRY_URL`              Schema Registry URL, for example `http://schema-registry:8081`
  `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO` Optional Basic Auth credentials for Schema Registry, for example `user:password`
  `SCHEMA_REGISTRY_BASIC_AUTH_SOURCE`    Optional auth source for Schema Registry, for example `USER_INFO`
  `ANALYSE_HSTREE_GROUP_ID`          Consumer `group.id`, for example `analyse-hstree-consumer-1`
  `HSTREE_VARIANT`                   HSTree implementation: `paper` (default) or `pysad`
  `KAFKA_SOURCE_TOPIC`               Source Kafka topic, default `source-data`
  `KAFKA_DEST_TOPIC`                 Destination Kafka topic, default `hstree-analysis`
  `KAFKA_DEST_VIZ_TOPIC`            Destination JSON viz topic, default `hstree-analysis-viz`
  `WINDOWS_SIZE`                     Window size for moving average in viz enrichment, default `0`
  `LOG_EVERY`                        Log progress every `LOG_EVERY` processed messages, default `10`
  `ANALYSE_GROUP`                    Legacy compatibility variable. If set, it is logged and ignored because multi-group processing is active.
  `ANALYSE_IDLE_EXIT_SEC`            Exit if idle for this many seconds, `0` disables idle exit
  `HSTREE_EXCLUDE_FIELDS`            Comma-separated list of fields to exclude from the feature vector, default `source_event_id,timestamp,Group,group,Cycle,LabviewTime,time`
  `HSTREE_INITIAL_WINDOW_SIZE`       Number of records to buffer for HSTree initialization, default `256`
  `HSTREE_WINDOW_SIZE`               Sliding window size for HSTree, default `256`
  `HSTREE_NUM_TREES`                 Number of trees in HSTree, default `25`
  `HSTREE_MAX_DEPTH`                 Maximum tree depth in HSTree, default `15`
  `HSTREE_ANOMALY_K_SIGMA`           Threshold sensitivity, default `3.0`
  `HSTREE_ANOMALY_MIN_SAMPLES`       Number of real scores per group before activating the verdict, default `50`

Documentation
-------------

**Overview, pipeline**
- Read: *at-least-once*. Offsets are committed synchronously only **after** successful production to the destination topic; `auto.offset.reset=earliest`; 1-second `poll()` timeout, with partition EOF ignored so the service waits cleanly for new messages.
- Write: *idempotent, non-transactional* (`enable.idempotence=True`, `acks="all"`, `max.in.flight.requests.per.connection=1`, very high `retries`). No end-to-end Exactly-Once, because there is no `transactional.id`.
- Startup checks: if `TOPIC_PARTITIONS` and or `TOPIC_RF` are set, the script inspects live Kafka metadata for source, destination, and viz topics and logs a warning on mismatch. It does not exit.

**Hardening, to tighten guarantees**
1. *At-least-once with commit-on-ack*
   - Commit the consumer offset **only if** delivery actually succeeded.
   - Strategies:
     - Check that `producer.flush()` returns success, for example `rc == 0`, before committing.
     - Or maintain a success flag through `on_delivery` and commit only when all deliveries in the batch have succeeded.
2. *End-to-end Exactly-Once, EOS*
   - Set a `transactional.id` and use `init_transactions()`, `begin_transaction()`, `produce(...)`, `commit_transaction()`.
   - Consumers should use `isolation.level="read_committed"`.
   - Cluster prerequisites: transactional internal topics healthy and sufficiently replicated, with stable brokers.

Notes
- The current implementation performs HSTree-based anomaly scoring and enrichment directly in this file.
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

import numpy as np
ROOT_DIR = next(
  parent
  for parent in Path(__file__).resolve().parents
  if (parent / "src" / "dasma_kafka").is_dir()
)
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
  sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.hstree_analysis.helpers import (
  build_feature_vector,
  create_viz_state,
  ensure_hstree_analysis_fields,
  enrich_viz_record,
  infer_feature_names,
  resolve_group_key,
  strip_excluded_fields,
)
from dasma_kafka.common.timestamps import resolve_timestamp_value
from dasma_kafka.common.threshold_modes import (
    HSTREE_ANALYSIS_SCHEMA_FIELDS,
    extend_hstree_avro_schema,
    normalize_hstree_threshold_mode,
)
from dasma_kafka.explainability.q3_trace import Q3TraceRecorder, initialization_fields

try:
  # When running with working_dir=/app/hstree_analysis, `models` is a top-level package.
  from models.hstree import (
    HSTreeGlobalModel,
    WelfordEstimator,
    WindowedWelfordEstimator,
    SlidingPercentileEstimator,
    classify_hstree_score,
    classify_hstree_score_percentile,
  )
except ModuleNotFoundError:
  from runtime.detectors.hstree.models.hstree import (
    HSTreeGlobalModel,
    WelfordEstimator,
    WindowedWelfordEstimator,
    SlidingPercentileEstimator,
    classify_hstree_score,
    classify_hstree_score_percentile,
  )

# Force stdout to be line-buffered to avoid Docker stdout buffering
try:
  sys.stdout.reconfigure(line_buffering=True)
except Exception:
  # Fallback: set `PYTHONUNBUFFERED=1` at runtime or rely on explicit `flush=True` in prints
  pass

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.error import ConsumeError
from confluent_kafka import KafkaError

# --- Kafka Admin helpers ----------------------------------------------------
from confluent_kafka.admin import AdminClient

# HSTREE_ANALYSIS_SCHEMA_FIELDS, extend_hstree_avro_schema,
# normalize_hstree_threshold_mode are now imported from
# dasma_kafka.common.threshold_modes above.


def _get_bootstrap_servers() -> str:
  """Return `BOOTSTRAP_SERVERS` with whitespace trimmed; fall back to the three-broker default."""
  bs = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
  # Be robust to accidental trailing spaces in docker-compose
  bs = ",".join([p.strip() for p in bs.split(",") if p.strip()])
  return bs or "kafka1:9092,kafka2:9092,kafka3:9092"


def _warn_if_hstree_windows_differ(initial_window_size: int, window_size: int) -> None:
  """Warn direct runtime users when the reference and model windows differ."""
  if initial_window_size != window_size:
    print(
      "[analyse-hstree] WARNING: HSTREE_INITIAL_WINDOW_SIZE="
      f"{initial_window_size} differs from HSTREE_WINDOW_SIZE={window_size}; "
      "the paper-style reference-window behavior requires equal values.",
      flush=True,
    )


def _hstree_variant_from_environment() -> str:
  """Return a supported HSTree variant or fail explicitly before Kafka setup."""
  variant = os.getenv("HSTREE_VARIANT", "paper").strip()
  if variant not in {"pysad", "paper"}:
    print(
      f"[analyse-hstree] ERROR: unknown HSTREE_VARIANT '{variant}' (values: pysad, paper)",
      file=sys.stderr,
      flush=True,
    )
    raise SystemExit(1)
  return variant


def _topic_partitions_and_rf(admin: AdminClient, topic: str) -> tuple[int, int]:
  """Best-effort: return `(partitions_count, replication_factor)` for a topic."""
  md = admin.list_topics(topic=topic, timeout=10)
  t = md.topics.get(topic)
  if t is None or t.error is not None:
    raise RuntimeError(str(t.error) if t and t.error else f"topic '{topic}' not found")
  partitions = t.partitions or {}
  pcount = len(partitions)
  rf = 0
  # Replication factor is the number of replicas in a partition. Assume uniform and use partition 0 if present.
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
        print(f"[analyse-hstree] WARN topic '{t}' has partitions={pcount} but `TOPIC_PARTITIONS`={expected_partitions}", flush=True)
      if expected_rf is not None and rf != expected_rf:
        print(f"[analyse-hstree] WARN topic '{t}' has replication_factor={rf} but `TOPIC_RF`={expected_rf}", flush=True)
    except Exception as e:
      print(f"[analyse-hstree] WARN cannot inspect topic '{t}': {e}", flush=True)


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
    "enable.auto.commit": False,  # we commit manually after production
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": value_deserializer,
    # Disable EOF notifications to avoid noisy "errors" and simply wait for new messages
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


# JSON producer for viz topic (string key + string value)
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
  parser = argparse.ArgumentParser(description="Consume from `source-data` and republish to `hstree-analysis` (Avro / Schema Registry).")
  parser.add_argument("-s", "--source-topic", default=os.getenv("KAFKA_SOURCE_TOPIC", "source-data"),
                      help="Source topic, default `source-data`")
  parser.add_argument("-d", "--dest-topic", default=os.getenv("KAFKA_DEST_TOPIC", "hstree-analysis"),
                      help="Destination topic, default `hstree-analysis`")
  parser.add_argument(
    "--dest-viz-topic",
    default=os.getenv("KAFKA_DEST_VIZ_TOPIC", "hstree-analysis-viz"),
    help="Destination viz topic for JSON, default `hstree-analysis-viz`",
  )
  parser.add_argument("-g", "--group-id", default=os.getenv("ANALYSE_HSTREE_GROUP_ID", "analyse-hstree-consumer"),
                      help="Consumer `group.id`, default `analyse-hstree-consumer`")
  args = parser.parse_args()
  hstree_variant = _hstree_variant_from_environment()
  trace = Q3TraceRecorder(os.getenv("DASMA_Q3_TRACE_SERVICE", "hstree-upstream"))
  trace_receive_seq = 0
  trace_init_seq = 0
  trace_publish_seq = 0

  # --- Kafka topic layout validation, optional and non-fatal ---
  bootstrap_servers = _get_bootstrap_servers()
  print(f"[analyse-hstree] Using BOOTSTRAP_SERVERS={bootstrap_servers}", flush=True)

  # Optional sanity checks: ensure the topics match the expected layout from docker-compose
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

  # Reproducibility, default 42
  seed = int(os.getenv("ANALYSE_SEED", "42"))
  np.random.seed(seed)
  random.seed(seed)
  print(f"[analyse-hstree] Reproducibility enabled (ANALYSE_SEED={seed})", flush=True)

  # --- ANALYSE_GROUP environment variable (ignored in multi-group mode) ---
  ANALYSE_GROUP = os.getenv("ANALYSE_GROUP")
  if ANALYSE_GROUP:
    print(
      f"[analyse-hstree] ANALYSE_GROUP is set to '{ANALYSE_GROUP}' "
      f"but is ignored: multi-group HSTree is active and all Groups will be processed.",
      flush=True,
    )
  else:
    print("[analyse-hstree] ANALYSE_GROUP is not set. All Groups will be processed (multi-group HSTree).", flush=True)

  # Optional idle exit: stop the service if no messages arrive for N seconds
  idle_exit_sec = int(os.getenv("ANALYSE_IDLE_EXIT_SEC", "0"))  # 0 disables idle exit

  # Shared Schema Registry client
  sr = make_sr_client()

  # Consumer-side deserialization: schema_str can be omitted; the schema ID is read from the message
  value_deserializer = AvroDeserializer(schema_registry_client=sr)

  # Resolve the Avro schema for the source topic from Schema Registry.
  # We may start before the producer has registered the schema, so we optionally
  # wait and retry instead of failing immediately on "subject not found".
  source_subject = f"{args.source_topic}-value"
  wait_seconds = int(os.getenv("ANALYSE_SCHEMA_WAIT_SEC", "0"))  # 0 = wait indefinitely
  schema_poll_interval = int(os.getenv("ANALYSE_SCHEMA_POLL_INTERVAL_SEC", "5"))

  latest_schema = None
  waited = 0
  while latest_schema is None:
    try:
      # Fetch the latest schema metadata for the source topic's value subject
      latest_schema = sr.get_latest_version(source_subject)
    except Exception as e:
      msg = str(e)
      # 40401 = Subject not found in Schema Registry
      if "40401" in msg or "Subject not found" in msg:
        if wait_seconds > 0 and waited >= wait_seconds:
          print(
            f"[analyse-hstree] Schema Registry subject '{source_subject}' not found after waiting {wait_seconds}s; aborting.",
            file=sys.stderr,
            flush=True,
          )
          sys.exit(1)
        print(
          f"[analyse-hstree] Schema Registry subject '{source_subject}' not found yet, waiting {schema_poll_interval}s... (error: {e})",
          file=sys.stderr,
          flush=True,
        )
        time.sleep(schema_poll_interval)
        waited += schema_poll_interval
        continue
      # Any other error is considered fatal
      print(
        f"[analyse-hstree] Error while fetching schema for subject '{source_subject}' from Schema Registry: {e}",
        file=sys.stderr,
        flush=True,
      )
      sys.exit(1)

  # Extract the raw Avro schema string from the Schema Registry response
  source_avro_schema = latest_schema.schema.schema_str

  # Extend the source schema with nullable HSTree analysis fields for the
  # destination topic, with default null for Avro compatibility.
  try:
    dest_avro_schema = extend_hstree_avro_schema(source_avro_schema)
  except Exception as e:
    print(f"[analyse-hstree] Failed to parse source Avro schema as JSON: {e}", file=sys.stderr, flush=True)
    sys.exit(1)

  # Build an AvroSerializer with the extended schema for the destination topic.
  value_serializer = AvroSerializer(schema_registry_client=sr, schema_str=dest_avro_schema)

  consumer = make_consumer(args.group_id, value_deserializer)
  producer = make_producer(value_serializer)
  json_producer = make_json_producer()

  last_msg_ts = time.monotonic()

  # --- Viz enrichment config (same logic as the legacy visualization conversion script) ---
  window_str = os.getenv("WINDOWS_SIZE", "0")
  try:
    window_size = int(window_str)
  except Exception:
    window_size = 0

  log_every = int(os.getenv("LOG_EVERY", "10"))
  if log_every <= 0:
    log_every = 10

  # Per-Group derived fields state (avoid mixing Groups for diffs/MA)
  viz_state_by_group: Dict[str, Dict[str, Any]] = {}
  processed = 0

  # --- HStree feature selection & model configuration -----------------------
  # Comma-separated list of fields to exclude from the feature vector.
  # Defaults cover common meta fields; can be overridden via HSTREE_EXCLUDE_FIELDS.
  exclude_fields_env = os.getenv("HSTREE_EXCLUDE_FIELDS", "source_event_id,timestamp,Group,group,Cycle,LabviewTime,time")
  HSTREE_EXCLUDE_FIELDS = {
    name.strip() for name in exclude_fields_env.split(",") if name.strip()
  }

  # Hyper-parameters for the global HStree model (can be overridden via env).
  HSTREE_INITIAL_WINDOW_SIZE = int(os.getenv("HSTREE_INITIAL_WINDOW_SIZE", "256"))
  HSTREE_WINDOW_SIZE = int(os.getenv("HSTREE_WINDOW_SIZE", "256"))
  _warn_if_hstree_windows_differ(HSTREE_INITIAL_WINDOW_SIZE, HSTREE_WINDOW_SIZE)
  HSTREE_NUM_TREES = int(os.getenv("HSTREE_NUM_TREES", "25"))
  HSTREE_MAX_DEPTH = int(os.getenv("HSTREE_MAX_DEPTH", "15"))
  HSTREE_ANOMALY_K_SIGMA = float(os.getenv("HSTREE_ANOMALY_K_SIGMA", "3.0"))
  HSTREE_ANOMALY_WARMUP_WINDOW = max(2, int(os.getenv("HSTREE_ANOMALY_WARMUP_WINDOW",
      os.getenv("HSTREE_ANOMALY_MIN_SAMPLES", "100"))))
  HSTREE_THRESHOLD_MODE = normalize_hstree_threshold_mode(
    os.getenv("HSTREE_THRESHOLD_MODE", "p99")
  )
  HSTREE_THRESHOLD_PERCENTILE = float(os.getenv("HSTREE_THRESHOLD_PERCENTILE", "99.0"))
  HSTREE_THRESHOLD_WINDOW = int(os.getenv("HSTREE_THRESHOLD_WINDOW", "500"))
  HSTREE_WELFORD_WINDOW = int(os.getenv("HSTREE_WELFORD_WINDOW", "2048"))
  print(
    f"[analyse-hstree] HSTree anomaly trigger: mode={HSTREE_THRESHOLD_MODE}, "
    f"warmup_window={HSTREE_ANOMALY_WARMUP_WINDOW}.",
    flush=True,
  )
  if HSTREE_THRESHOLD_MODE == "percentile":
    print(
      f"[analyse-hstree] Percentile threshold: P{HSTREE_THRESHOLD_PERCENTILE}, "
      f"window={HSTREE_THRESHOLD_WINDOW}.",
      flush=True,
    )
  elif HSTREE_THRESHOLD_MODE == "welford-windowed":
    print(
      f"[analyse-hstree] Windowed Welford threshold: "
      f"k_sigma={HSTREE_ANOMALY_K_SIGMA}, window={HSTREE_WELFORD_WINDOW}.",
      flush=True,
    )
  else:
    print(
      f"[analyse-hstree] Welford threshold: k_sigma={HSTREE_ANOMALY_K_SIGMA}.",
      flush=True,
    )

  # Model + feature state, initialised lazily on first messages.
  # One HSTreeGlobalModel is maintained per Group (multi-group mode).
  models_by_group: dict[str, HSTreeGlobalModel] = {}
  welford_by_group: dict[str, WelfordEstimator | WindowedWelfordEstimator] = {}
  percentile_by_group: dict[str, SlidingPercentileEstimator] = {}
  feature_names: Optional[List[str]] = None

  # Graceful shutdown
  running = True
  def handle_sigint(sig, frame):
    nonlocal running
    running = False
  signal.signal(signal.SIGINT, handle_sigint)
  signal.signal(signal.SIGTERM, handle_sigint)

  consumer.subscribe([args.source_topic])
  print(f"[analyse-hstree] Waiting for new messages on topic '{args.source_topic}' (infinite wait)...", flush=True)

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
        # Wait with timeout to emit a heartbeat message every second
        msg = consumer.poll(1.0)
      except ConsumeError as e:
        err = e.args[0] if e.args else e
        # Ignore benign end-of-partition notifications (robust even if code() is unavailable)
        code = err.code() if hasattr(err, "code") and callable(getattr(err, "code")) else None
        if (code is not None and code == KafkaError._PARTITION_EOF) or "_PARTITION_EOF" in str(err):
          # Wait for new messages without logging an error
          if idle_exit_sec > 0 and (time.monotonic() - last_msg_ts) >= idle_exit_sec:
            print(f"[analyse-hstree] Idle for {idle_exit_sec}s after EOF (exception), exiting.", flush=True)
            break
          continue
        # Other errors only
        print(f"[consumer-error] {err}", file=sys.stderr, flush=True)
        continue
      if msg is None:
        # Heartbeat: print a message every second while no message arrives
        print("[analyse-hstree] Waiting for new messages...", flush=True)
        if idle_exit_sec > 0 and (time.monotonic() - last_msg_ts) >= idle_exit_sec:
          print(f"[analyse-hstree] No messages for {idle_exit_sec}s, exiting.", flush=True)
          break
        continue
      if msg.error():
        err = msg.error()
        # Silently ignore partition EOF (infinite wait for new messages)
        code = err.code() if hasattr(err, "code") and callable(getattr(err, "code")) else None
        if (code is not None and code == KafkaError._PARTITION_EOF) or "_PARTITION_EOF" in str(err):
          if idle_exit_sec > 0 and (time.monotonic() - last_msg_ts) >= idle_exit_sec:
            print(f"[analyse-hstree] Idle for {idle_exit_sec}s after EOF, exiting.", flush=True)
            break
          continue
        # Other errors only
        print(f"[consumer-error] {err}", file=sys.stderr, flush=True)
        continue

      last_msg_ts = time.monotonic()
      trace_receive_seq += 1

      key = msg.key()  # already deserialized to str thanks to StringDeserializer
      record = msg.value()  # dict (Avro-deserialized)

      # Determine group key for multi-group HSTree (fall back to '__NO_GROUP__')
      group_key = resolve_group_key(record)

      # --- HStree feature vector & scoring ----------------------------------
      hstree_score: Optional[float] = None
      hstree_model: Optional[HSTreeGlobalModel] = None

      # Infer feature names once, from the first record, by keeping only
      # numeric fields and excluding known meta fields.
      if feature_names is None and isinstance(record, dict):
        numeric_fields = infer_feature_names(record, HSTREE_EXCLUDE_FIELDS)
        if not numeric_fields:
          print(
            "[analyse-hstree] No numeric fields found to build HStree feature vector; "
            "records will be forwarded with hstree_score=0.0.",
            file=sys.stderr,
            flush=True,
          )
        else:
          feature_names = sorted(numeric_fields)
          print(
            f"[analyse-hstree] Using {len(feature_names)} features for HStree: {feature_names}",
            flush=True,
          )

      # If we have a defined feature set, attempt to build the feature vector
      # and score the instance.
      if feature_names:
        try:
          x = build_feature_vector(record, feature_names)
        except Exception as e:
          print(f"[analyse-hstree] Error while building feature vector: {e}", file=sys.stderr, flush=True)
          x = None

        if x is not None:
          # Lazily instantiate or retrieve the HSTree model for this group
          hstree_model = models_by_group.get(group_key)
          if hstree_model is None:
            try:
              def observe_upstream_initialization(**state):
                nonlocal trace_init_seq
                import importlib.metadata
                trace_init_seq += 1
                context = state["context"]
                trace.model_initialization(
                  **initialization_fields(
                    topic=context["topic"], partition=context["partition"], offset=context["offset"],
                    group=group_key, source_event_id=context["source_event_id"],
                    initialization_order_seq=trace_init_seq,
                    warmup_event_ids=state["warmup_event_ids"], warmup_values=state["warmup_values"],
                    feature_names=feature_names, feature_mins=state["feature_mins"],
                    feature_maxes=state["feature_maxes"],
                    hstree_parameters={
                      "initial_window_size": HSTREE_INITIAL_WINDOW_SIZE,
                      "window_size": HSTREE_WINDOW_SIZE,
                      "num_trees": HSTREE_NUM_TREES,
                      "max_depth": HSTREE_MAX_DEPTH,
                      "configured_seed": seed,
                      "variant": hstree_variant,
                    }, pysad_version=importlib.metadata.version("pysad"),
                  )
                )
              hstree_model = HSTreeGlobalModel(
                num_features=len(feature_names),
                window_size=HSTREE_WINDOW_SIZE,
                num_trees=HSTREE_NUM_TREES,
                max_depth=HSTREE_MAX_DEPTH,
                initial_window_size=HSTREE_INITIAL_WINDOW_SIZE,
                seed=seed,
                variant=hstree_variant,
                initialization_observer=observe_upstream_initialization if trace.enabled else None,
                feature_names=feature_names,
              )
              models_by_group[group_key] = hstree_model
              print(
                f"[analyse-hstree] Created HSTreeGlobalModel for Group='{group_key}' with "
                f"{len(feature_names)} features, window_size={HSTREE_WINDOW_SIZE}, "
                f"num_trees={HSTREE_NUM_TREES}, max_depth={HSTREE_MAX_DEPTH}, "
                f"initial_window_size={HSTREE_INITIAL_WINDOW_SIZE}, variant={hstree_variant}.",
                flush=True,
              )
            except Exception as e:
              print(f"[analyse-hstree] Failed to instantiate HSTreeGlobalModel for Group='{group_key}': {e}", file=sys.stderr, flush=True)
              hstree_model = None

          if hstree_model is not None:
            try:
              source_event_id = record.get("source_event_id")
              if trace.enabled:
                hstree_score = float(hstree_model.partial_fit_score(
                  x,
                  source_event_id=source_event_id if isinstance(source_event_id, str) else None,
                  trace_context={
                    "topic": msg.topic(), "partition": int(msg.partition()),
                    "offset": int(msg.offset()), "source_event_id": source_event_id,
                  },
                ))
              else:
                hstree_score = float(hstree_model.partial_fit_score(x))
            except Exception as e:
              print(f"[analyse-hstree] Error during HStree scoring for Group='{group_key}': {e}", file=sys.stderr, flush=True)
              hstree_score = None

      hstree_status = "warming_up"
      hstree_anomaly: Optional[bool] = None
      hstree_threshold: Optional[float] = None
      if hstree_model is not None:
        if HSTREE_THRESHOLD_MODE == "percentile":
          estimator_p = percentile_by_group.setdefault(
            group_key,
            SlidingPercentileEstimator(HSTREE_THRESHOLD_WINDOW, HSTREE_THRESHOLD_PERCENTILE),
          )
          hstree_status, hstree_anomaly, hstree_threshold = classify_hstree_score_percentile(
            hstree_score,
            estimator_p,
            is_warming_up=hstree_model.is_warming_up,
            min_samples=HSTREE_ANOMALY_WARMUP_WINDOW,
          )
        elif HSTREE_THRESHOLD_MODE == "welford-windowed":
          welford = welford_by_group.setdefault(
            group_key, WindowedWelfordEstimator(HSTREE_WELFORD_WINDOW)
          )
          hstree_status, hstree_anomaly, hstree_threshold = classify_hstree_score(
            hstree_score,
            welford,
            is_warming_up=hstree_model.is_warming_up,
            min_samples=HSTREE_ANOMALY_WARMUP_WINDOW,
            k_sigma=HSTREE_ANOMALY_K_SIGMA,
          )
        else:
          welford = welford_by_group.setdefault(group_key, WelfordEstimator())
          hstree_status, hstree_anomaly, hstree_threshold = classify_hstree_score(
            hstree_score,
            welford,
            is_warming_up=hstree_model.is_warming_up,
            min_samples=HSTREE_ANOMALY_WARMUP_WINDOW,
            k_sigma=HSTREE_ANOMALY_K_SIGMA,
          )

      # If scoring failed or no features are available, fall back to 0.0.
      record, hstree_score = ensure_hstree_analysis_fields(
        record,
        hstree_score,
        hstree_status,
        hstree_anomaly,
        hstree_threshold,
        hstree_variant=hstree_variant,
      )

      # Also print the processed row (optional)
      try:
        print(json.dumps(record, ensure_ascii=False), flush=True)
      except Exception:
        print(str(record), flush=True)

      # --- Publish to Avro topic + JSON viz topic (with derived fields) -----
      reset_delivery_state()

      # Build a viz record (copy) and compute derived fields.
      viz_record = (
        strip_excluded_fields(
          record,
          HSTREE_EXCLUDE_FIELDS,
          preserve_fields={"Group", "group", "timestamp", "source_event_id"},
        )
        if isinstance(record, dict)
        else {"hstree_score": hstree_score}
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

      # Build JSON payload (ensure_ascii=False to keep labels readable)
      try:
        json_payload = json.dumps(viz_record, ensure_ascii=False)
      except Exception as e:
        print(f"[analyse-hstree] Error while serializing JSON for viz topic: {e}", file=sys.stderr, flush=True)
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
          print(f"[analyse-hstree] delivery-error avro: {delivery_state['avro_err']}", file=sys.stderr, flush=True)
        if delivery_state["json_err"] is not None:
          print(f"[analyse-hstree] delivery-error json: {delivery_state['json_err']}", file=sys.stderr, flush=True)
        # Do not commit: message will be reprocessed (at-least-once)
        continue

      processed += 1
      if processed % log_every == 0:
        print(
          f"[analyse-hstree] Processed={processed} topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}",
          flush=True,
        )

      if trace.enabled:
        trace.offset_commit(msg.topic(), int(msg.partition()), int(msg.offset()) + 1, "requested")
      try:
        consumer.commit(message=msg, asynchronous=False)
      except Exception:
        if trace.enabled:
          trace.offset_commit(msg.topic(), int(msg.partition()), int(msg.offset()) + 1, "failed")
        raise
      if trace.enabled:
        trace.offset_commit(msg.topic(), int(msg.partition()), int(msg.offset()) + 1, "acknowledged")
        source_event_id = record.get("source_event_id")
        if isinstance(source_event_id, str) and source_event_id.strip():
          trace_publish_seq += 1
          published = [{"topic": args.dest_topic, "process_instance": trace.process_instance, "publish_seq": trace_publish_seq}]
          trace_publish_seq += 1
          published.append({"topic": args.dest_viz_topic, "process_instance": trace.process_instance, "publish_seq": trace_publish_seq})
          trace.message_lifecycle(
            topic=msg.topic(), partition=int(msg.partition()), offset=int(msg.offset()),
            group=group_key, source_event_id=source_event_id,
            receive_seq=trace_receive_seq, dispatch_seq=None, worker_id=None,
            group_seq=None, start_seq=None, complete_seq=None,
            published_outputs=published, output_status=hstree_status,
          )

  except KeyboardInterrupt:
    pass
  finally:
    try:
      producer.flush(10)
      json_producer.flush(10)
    finally:
      trace.close()
      consumer.close()

  print("--------------", flush=True)
  print("End of analyse-hstree & Kafka forwarding", flush=True)
  print("--------------", flush=True)
  
if __name__ == "__main__":
  main()
