#!/usr/bin/env python3
"""
repeatability_diff.py

Compares the two outputs of a repeatability test.  It consumes the JSON viz
topics of the two identical model instances (``repeatability-v1-viz`` and
``repeatability-v2-viz``), joins them on the *strict* key
``(Group, source_event_id)`` and publishes the comparison to the
``repeatability-diff`` topic (JSON only, keyed by ``Group``).

Join semantics
--------------
* The join key is strictly ``(str(record["Group"]), str(record["source_event_id"]))``.
  Records missing either field are rejected and logged — there is **no**
  timestamp fallback, so two distinct source events sharing one display second
  are never merged.
* ``timestamp`` is display-only: taken from A when present, otherwise from the
  record that is available.
* A bucket publishes when:

  * **completion** — both A and B have arrived: ``emit_reason="complete"``;
  * **timeout / watermark** — ``REPEATABILITY_TIMEOUT_SEC`` wall-clock seconds
    elapsed with only one side present: a single incomplete record is published
    with ``emit_reason="timeout"``, and the bucket is *kept*;
  * **progressive update** — a late arrival then re-publishes the bucket
    (``emit_reason="progress"`` while still incomplete, ``"complete"`` once
    both sides are present, and ``"progress"`` again if an already-complete
    bucket is modified);
  * **shutdown** — on SIGINT/SIGTERM every unsealed bucket is drained with
    ``emit_reason="shutdown"``.

* Past ``REPEATABILITY_RETENTION_SEC`` a bucket is sealed: late arrivals are
  ignored (``dropped_late``) and the tombstone is removed at ``2 * retention``.

Re-arrival policy (last wins with signature dedup)
--------------------------------------------------
An arrival for a slot that already holds an *identical* record is a no-op.  An
arrival carrying *different* content replaces the slot and the signature change
triggers a re-publish — no real progression is ever masked, and no duplicate is
published at identical state.

Comparison semantics
--------------------
``equal_values`` is exact: absent/None on both sides are equal, two NaN values
are equal, floats compare with ``==`` without any epsilon (``-0.0 == 0.0`` is
accepted), any other values compare with ``==``, and a null on one side only
is a divergence.  ``identical`` is 1 when a complete bucket is fully equal on
every configured field, 0 on any divergence, and null while incomplete.

Output message (JSON).  Booleans are never published for the numeric twins
because Telegraf's JSON parser drops booleans: ``anomaly_a``, ``anomaly_b``,
``identical`` are ``0``/``1`` or null, and ``complete`` (boolean, for JSON
consumers) is accompanied by ``complete_int`` (``1``/``0``) for InfluxDB/Grafana.

  {
    "timestamp":      "2026-01-01 12:34:56",
    "Group":          "G1", "source_event_id": "event-000000000001",
    "model":          "hstree",
    "score_a": 0.0123, "score_b": 0.0123, "score_diff": 0.0, "abs_diff": 0.0,
    "status_a": "normal", "status_b": "normal",
    "threshold_a": 1.5,   "threshold_b": 1.5,
    "anomaly_a": 0,       "anomaly_b": 0,
    "identical": 1,
    "complete": true,     "complete_int": 1,
    "emit_reason": "complete"
  }

Each extra field ``x`` of ``REPEATABILITY_EXTRA_FIELDS`` adds ``x_a`` and
``x_b``, plus ``x_diff`` and ``x_abs_diff`` when both values are numeric.  No
nested JSON structure is ever produced.  NaN values never reach the output:
the verdict keeps the ``NaN == NaN`` semantics on compared fields, but every
published raw value and diff is normalized to ``null`` so the stream stays
standard JSON (``json.dumps(..., allow_nan=False)`` safe).

Useful environment variables:
  BOOTSTRAP_SERVERS              Kafka brokers, default kafka1:9092,kafka2:9092,kafka3:9092
  REPEATABILITY_VIZ_TOPIC_A      Input topic of instance 1 (default repeatability-v1-viz)
  REPEATABILITY_VIZ_TOPIC_B      Input topic of instance 2 (default repeatability-v2-viz)
  REPEATABILITY_DEST_TOPIC       Output topic (default repeatability-diff)
  REPEATABILITY_GROUP_ID         Kafka consumer group id (default repeatability-diff-consumer)
  REPEATABILITY_MODEL_ID         Model label in the output records (default hstree)
  REPEATABILITY_SCORE_FIELD      Compared score field (default hstree_score)
  REPEATABILITY_STATUS_FIELD     Compared status field (default hstree_status)
  REPEATABILITY_THRESHOLD_FIELD  Compared threshold field (default hstree_threshold)
  REPEATABILITY_ANOMALY_FIELD    Compared anomaly field (default hstree_anomaly)
  REPEATABILITY_EXTRA_FIELDS     Extra compared fields, comma-separated (default none)
  REPEATABILITY_TIMEOUT_SEC      Watermark before a partial publish (default 30)
  REPEATABILITY_RETENTION_SEC    Seconds before a bucket is sealed (default 300, >= timeout)
  LOG_EVERY                      Log every N emitted records (default 100)
  DASMA_ENABLED                  Set to 0 to disable this service (idle loop)
"""

from __future__ import annotations

import json
import math
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, Producer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"


@dataclass(frozen=True)
class DiffConfig:
    """Immutable configuration of the repeatability diff service."""

    bootstrap_servers: str
    viz_topic_a: str
    viz_topic_b: str
    dest_topic: str
    group_id: str
    model_id: str
    score_field: str
    status_field: str
    threshold_field: str
    anomaly_field: str
    extra_fields: Tuple[str, ...]
    timeout_sec: float
    retention_sec: float

    @property
    def compared_fields(self) -> Tuple[str, ...]:
        """Every record field compared by the identical verdict, in order."""
        return (
            self.score_field,
            self.status_field,
            self.threshold_field,
            self.anomaly_field,
            *self.extra_fields,
        )


def parse_extra_fields(raw: Optional[str]) -> Tuple[str, ...]:
    """Parse the CSV ``REPEATABILITY_EXTRA_FIELDS`` value.

    Entries are trimmed, empties are dropped and duplicates removed, keeping
    the first-seen order: ``"a, b ,a,c,"`` becomes ``("a", "b", "c")``.
    """
    if raw is None:
        return ()
    seen: Dict[str, None] = {}
    for part in str(raw).split(","):
        name = part.strip()
        if name:
            seen.setdefault(name, None)
    return tuple(seen)


def _env_str(env: Dict[str, str], key: str, default: str) -> str:
    value = env.get(key)
    if value is None or str(value).strip() == "":
        return default
    return str(value)


def _env_float(env: Dict[str, str], key: str, default: float) -> float:
    try:
        return float(env[key])
    except (KeyError, TypeError, ValueError):
        return default


def load_config(env: Dict[str, str] = os.environ) -> DiffConfig:
    """Build the configuration from an environment mapping (pure, testable)."""
    timeout_sec = max(0.0, _env_float(env, "REPEATABILITY_TIMEOUT_SEC", 30.0))
    retention_sec = max(timeout_sec, _env_float(env, "REPEATABILITY_RETENTION_SEC", 300.0))
    return DiffConfig(
        bootstrap_servers=_env_str(env, "BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        viz_topic_a=_env_str(env, "REPEATABILITY_VIZ_TOPIC_A", "repeatability-v1-viz"),
        viz_topic_b=_env_str(env, "REPEATABILITY_VIZ_TOPIC_B", "repeatability-v2-viz"),
        dest_topic=_env_str(env, "REPEATABILITY_DEST_TOPIC", "repeatability-diff"),
        group_id=_env_str(env, "REPEATABILITY_GROUP_ID", "repeatability-diff-consumer"),
        model_id=_env_str(env, "REPEATABILITY_MODEL_ID", "hstree"),
        score_field=_env_str(env, "REPEATABILITY_SCORE_FIELD", "hstree_score"),
        status_field=_env_str(env, "REPEATABILITY_STATUS_FIELD", "hstree_status"),
        threshold_field=_env_str(env, "REPEATABILITY_THRESHOLD_FIELD", "hstree_threshold"),
        anomaly_field=_env_str(env, "REPEATABILITY_ANOMALY_FIELD", "hstree_anomaly"),
        extra_fields=parse_extra_fields(env.get("REPEATABILITY_EXTRA_FIELDS")),
        timeout_sec=timeout_sec,
        retention_sec=retention_sec,
    )


# ---------------------------------------------------------------------------
# Pure, testable state helpers
# ---------------------------------------------------------------------------

# A bucket holds the two records received so far for one (Group, source_event_id).
#   bucket = {
#       "timestamp": str,                # display time (A's when present)
#       "Group": str, "source_event_id": str,
#       "rec_a": Optional[dict],         # instance 1 record, None until it arrives
#       "rec_b": Optional[dict],         # instance 2 record, None until it arrives
#       "first_seen": float,             # wall-clock of the first arrival
#       "last_emit_signature": Optional[tuple],  # dedup of re-publishes
#       "sealed": bool,                  # past retention: ignores late arrivals
#   }
Bucket = Dict[str, Any]
GroupBuckets = Dict[str, Bucket]
Pending = Dict[str, GroupBuckets]

SIDES: Tuple[str, str] = ("a", "b")

_NAN_SENTINEL = "__NaN__"


def equal_values(a: Any, b: Any) -> bool:
    """Exact equality of two compared values (no epsilon).

    Absent/``None`` equals absent/``None``; two NaN values are equal; floats
    compare with ``==`` (``-0.0 == 0.0`` accepted); every other pair compares
    with ``==``; a null on one side only is a divergence.
    """
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    both_nan = (
        isinstance(a, float)
        and isinstance(b, float)
        and math.isnan(a)
        and math.isnan(b)
    )
    if both_nan:
        return True
    return bool(a == b)


def numeric_or_none(value: Any) -> Optional[float]:
    """Return the value as a float when numeric, else ``None``.

    Booleans are not numeric for diff purposes, and NaN counts as numeric
    (``nan - nan`` stays NaN) but a NaN diff is meaningless: it is reported
    as ``None`` by :func:`build_diff_record` through the NaN check below.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _published_value(value: Any) -> Any:
    """Normalise a raw value for JSON output: NaN becomes ``null``.

    Telegraf consumes standard JSON; ``json.dumps`` would otherwise emit a bare
    ``NaN`` literal, which is non-standard and fragile for downstream parsers.
    The identical verdict still treats NaN as NaN via :func:`equal_values`.
    """
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _int_twin(value: Any) -> Optional[int]:
    """Numeric twin for InfluxDB/Telegraf: ``1``/``0``, or ``None`` when absent.

    Telegraf's JSON parser drops booleans, so anomaly fields and the identical
    verdict are published as integers or null, never as booleans.
    """
    if value is True:
        return 1
    if value is False:
        return 0
    return None


def _warming_up_int(record: Optional[Dict[str, Any]], status_field: str) -> Optional[int]:
    """Numeric warmup state for observability, or null when a side is absent."""
    if record is None:
        return None
    return 1 if record.get(status_field) == "warming_up" else 0


def _canonical(value: Any) -> Any:
    """Canonical form used for record equality and emit signatures.

    NaN values are replaced by a sentinel so a redelivered record containing
    NaN compares equal to the stored one (``nan != nan`` otherwise).
    """
    if isinstance(value, dict):
        return {str(k): _canonical(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    if isinstance(value, float) and math.isnan(value):
        return _NAN_SENTINEL
    return value


def resolve_join_key(record: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """Strict join key ``(Group, source_event_id)`` or ``None`` when invalid.

    There is no timestamp fallback: a record missing either field is rejected
    and logged by the caller.
    """
    group = record.get("Group")
    event_id = record.get("source_event_id")
    if group in (None, "") or event_id in (None, ""):
        return None
    return str(group), str(event_id)


def bucket_is_complete(bucket: Bucket) -> bool:
    """True once both instance records are present in the bucket."""
    return bucket["rec_a"] is not None and bucket["rec_b"] is not None


def process_arrival(
    pending: Pending,
    side: str,
    group: str,
    event_key: str,
    record: Dict[str, Any],
    now: float,
    timestamp: Optional[str] = None,
) -> Tuple[Optional[Bucket], Optional[str], bool]:
    """Pure state transition for one viz record arrival.

    Returns ``(bucket, reason, dropped)``:

    * ``bucket``  the touched bucket, or ``None`` when it was sealed (the
      arrival came past retention and is dropped);
    * ``reason``  ``"complete"`` when this arrival made an incomplete bucket
      complete, ``"progress"`` when a timed-out bucket gains content or an
      already-complete bucket is modified, ``None`` when nothing must be
      published now (identical re-arrival, or first partial arrival still
      waiting for the watermark);
    * ``dropped`` ``True`` when the arrival was ignored (sealed bucket).
    """
    group_buckets = pending.setdefault(group, {})
    bucket = group_buckets.get(event_key)
    if bucket is None:
        bucket = {
            "timestamp": timestamp if timestamp is not None else "",
            "Group": group,
            "source_event_id": event_key,
            "rec_a": None,
            "rec_b": None,
            "first_seen": now,
            "last_emit_signature": None,
            "sealed": False,
        }
        group_buckets[event_key] = bucket
    if bucket["sealed"]:
        return None, None, True

    was_complete = bucket_is_complete(bucket)
    slot = f"rec_{side}"
    if bucket[slot] is not None and _canonical(bucket[slot]) == _canonical(record):
        return bucket, None, False  # identical re-arrival: no-op
    bucket[slot] = record  # last-wins replacement

    # Display timestamp: A wins once it arrives with a non-empty value; B only
    # fills it when the bucket has none yet (contract: "A si présent, sinon B").
    if side == "a":
        if timestamp:
            bucket["timestamp"] = timestamp
    elif not bucket["timestamp"] and timestamp:
        bucket["timestamp"] = timestamp

    now_complete = bucket_is_complete(bucket)
    if now_complete:
        reason = "complete" if not was_complete else "progress"
    elif bucket["last_emit_signature"] is not None:
        reason = "progress"
    else:
        reason = None
    return bucket, reason, False


def compute_identical(
    rec_a: Optional[Dict[str, Any]],
    rec_b: Optional[Dict[str, Any]],
    fields: Tuple[str, ...],
) -> Optional[int]:
    """``1`` when both records are present and equal on every field, else ``0``.

    Returns ``None`` while the bucket is incomplete.
    """
    if rec_a is None or rec_b is None:
        return None
    for field in fields:
        if not equal_values(rec_a.get(field), rec_b.get(field)):
            return 0
    return 1


def build_diff_record(
    bucket: Bucket,
    config: DiffConfig,
    reason: str,
) -> Dict[str, Any]:
    """Build the JSON comparison record for one bucket.

    ``score_diff``/``abs_diff`` are null when a score is missing, null or
    non-numeric.  Extra fields publish ``x_a``/``x_b`` always and
    ``x_diff``/``x_abs_diff`` only when both values are numeric.  No nested
    JSON structure is produced.
    """
    rec_a_raw = bucket["rec_a"]
    rec_b_raw = bucket["rec_b"]
    rec_a = rec_a_raw or {}
    rec_b = rec_b_raw or {}
    complete = bucket_is_complete(bucket)
    identical = compute_identical(rec_a_raw, rec_b_raw, config.compared_fields)

    score_a = rec_a.get(config.score_field)
    score_b = rec_b.get(config.score_field)
    score_a_num = numeric_or_none(score_a)
    score_b_num = numeric_or_none(score_b)
    if score_a_num is None or score_b_num is None or (
        math.isnan(score_a_num) or math.isnan(score_b_num)
    ):
        score_diff: Optional[float] = None
        abs_diff: Optional[float] = None
    else:
        score_diff = score_a_num - score_b_num
        abs_diff = abs(score_diff)

    record: Dict[str, Any] = {
        "timestamp": str(bucket.get("timestamp") or ""),
        "Group": bucket["Group"],
        "source_event_id": bucket["source_event_id"],
        "model": config.model_id,
    }
    record.update({
        "score_a": _published_value(score_a),
        "score_b": _published_value(score_b),
        "score_diff": score_diff,
        "abs_diff": abs_diff,
        "status_a": _published_value(rec_a.get(config.status_field)),
        "status_b": _published_value(rec_b.get(config.status_field)),
        "warming_up_a": _warming_up_int(rec_a_raw, config.status_field),
        "warming_up_b": _warming_up_int(rec_b_raw, config.status_field),
        "threshold_a": _published_value(rec_a.get(config.threshold_field)),
        "threshold_b": _published_value(rec_b.get(config.threshold_field)),
        "anomaly_a": _int_twin(rec_a.get(config.anomaly_field)),
        "anomaly_b": _int_twin(rec_b.get(config.anomaly_field)),
        "identical": identical,
        "complete": complete,
        "complete_int": 1 if complete else 0,
        "emit_reason": reason,
    })
    for extra in config.extra_fields:
        extra_a = rec_a.get(extra)
        extra_b = rec_b.get(extra)
        record[f"{extra}_a"] = _published_value(extra_a)
        record[f"{extra}_b"] = _published_value(extra_b)
        extra_a_num = numeric_or_none(extra_a)
        extra_b_num = numeric_or_none(extra_b)
        if extra_a_num is not None and extra_b_num is not None:
            if math.isnan(extra_a_num) or math.isnan(extra_b_num):
                record[f"{extra}_diff"] = None
                record[f"{extra}_abs_diff"] = None
            else:
                record[f"{extra}_diff"] = extra_a_num - extra_b_num
                record[f"{extra}_abs_diff"] = abs(extra_a_num - extra_b_num)
    return record


def build_and_maybe_emit(
    group: str,
    event_key: str,
    bucket: Bucket,
    config: DiffConfig,
    reason: str,
    emit: Callable[[Dict[str, Any]], None],
) -> bool:
    """Publish the bucket unless its signature is unchanged.

    The signature covers both records, completeness, the verdict and the emit
    reason, so an identical re-publish is skipped while any real progression
    (new record, replacement, reason change) is published.
    """
    record = build_diff_record(bucket, config, reason)
    signature = (
        _canonical(bucket["rec_a"]),
        _canonical(bucket["rec_b"]),
        record["complete"],
        record["identical"],
        record["emit_reason"],
    )
    if bucket["last_emit_signature"] == signature:
        return False
    bucket["last_emit_signature"] = signature
    emit(record)
    return True


def sweep_action(
    bucket: Bucket,
    now: float,
    timeout_sec: float,
    retention_sec: float,
) -> Optional[str]:
    """Decide what the periodic sweep should do with a bucket.

    Returns one of:

    * ``"timeout"`` — publish the single incomplete state (watermark reached,
      nothing published yet);
    * ``"seal"``   — stop accepting late arrivals (retention reached);
    * ``"remove"`` — drop the sealed tombstone entirely (``2 * retention``);
    * ``None``     — leave the bucket untouched.
    """
    age = now - bucket["first_seen"]
    if bucket["sealed"]:
        return "remove" if age >= 2 * retention_sec else None
    if age >= retention_sec:
        return "seal"
    if (
        age >= timeout_sec
        and bucket["last_emit_signature"] is None
        and not bucket_is_complete(bucket)
    ):
        return "timeout"
    return None


def drain_shutdown(pending: Pending) -> List[Tuple[str, str, Bucket]]:
    """Pop every unsealed bucket for the shutdown drain (sealed ones are dropped)."""
    drained: List[Tuple[str, str, Bucket]] = []
    for group in list(pending.keys()):
        for event_key in list(pending[group].keys()):
            bucket = pending[group].pop(event_key)
            if not bucket["sealed"]:
                drained.append((group, event_key, bucket))
        if not pending[group]:
            del pending[group]
    return drained


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------


def build_consumer_config(config: DiffConfig) -> Dict[str, Any]:
    """Consumer configuration for the two viz topics (pure, testable)."""
    return {
        "bootstrap.servers": config.bootstrap_servers,
        "group.id": config.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        # librdkafka emits ``_PARTITION_EOF`` only when this supported option
        # is enabled.  ``enable.eof.notify`` is not a librdkafka property.
        "enable.partition.eof": True,
    }


def build_producer_config(config: DiffConfig) -> Dict[str, Any]:
    """Idempotent producer configuration (pure, testable)."""
    return {
        "bootstrap.servers": config.bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 1,
        "retries": 2_147_483_647,
        "delivery.timeout.ms": 300_000,
    }


def _make_consumer(config: DiffConfig) -> Consumer:
    consumer = Consumer(build_consumer_config(config))
    consumer.subscribe([config.viz_topic_a, config.viz_topic_b])
    return consumer


def _make_producer(config: DiffConfig) -> Producer:
    return Producer(build_producer_config(config))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    if os.getenv("DASMA_ENABLED", "1") == "0":
        print("[repeatability-diff] DASMA_ENABLED=0 — service disabled. Idling.", flush=True)
        signal.pause()
        return

    config = load_config()
    log_every = max(1, int(os.getenv("LOG_EVERY", "100")))

    print(f"[repeatability-diff] model={config.model_id}", flush=True)
    print(
        f"[repeatability-diff] subscribing to [{config.viz_topic_a}, {config.viz_topic_b}]",
        flush=True,
    )
    print(f"[repeatability-diff] publishing to '{config.dest_topic}'", flush=True)
    print(
        f"[repeatability-diff] join: strict (Group, source_event_id), "
        f"timeout={config.timeout_sec}s retention={config.retention_sec}s",
        flush=True,
    )

    consumer = _make_consumer(config)
    producer = _make_producer(config)

    pending: Pending = {}
    running = True
    last_flush = time.monotonic()
    emitted_total = 0
    dropped_late = 0
    rejected_key = 0

    def _handle_stop(sig, frame):  # noqa: ANN001
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    def _emit(record: Dict[str, Any]) -> None:
        nonlocal emitted_total
        try:
            payload = json.dumps(record, ensure_ascii=False)

            def _on_delivery(err, _msg, _r=record):
                if err is not None:
                    print(
                        f"[repeatability-diff] delivery error for "
                        f"Group={_r['Group']} event={_r['source_event_id']}: {err}",
                        file=sys.stderr,
                        flush=True,
                    )

            producer.produce(
                topic=config.dest_topic,
                key=record["Group"],
                value=payload,
                on_delivery=_on_delivery,
            )
            producer.poll(0)
            emitted_total += 1
            if emitted_total % log_every == 0:
                print(
                    f"[repeatability-diff] emitted={emitted_total} "
                    f"last: Group={record['Group']} event={record['source_event_id']} "
                    f"identical={record['identical']} "
                    f"abs_diff={record['abs_diff']} reason={record['emit_reason']}",
                    flush=True,
                )
        except Exception as exc:
            print(f"[repeatability-diff] produce error: {exc}", file=sys.stderr, flush=True)

    def _sweep(now: float) -> None:
        """Apply timeout / retention transitions to every pending bucket."""
        for group in list(pending.keys()):
            for event_key in list(pending[group].keys()):
                bucket = pending[group][event_key]
                action = sweep_action(bucket, now, config.timeout_sec, config.retention_sec)
                if action == "timeout":
                    build_and_maybe_emit(group, event_key, bucket, config, "timeout", _emit)
                elif action == "seal":
                    # Past retention: keep a tombstone that ignores late arrivals
                    # (they cannot alter the published point) but free memory.
                    bucket["sealed"] = True
                    bucket["rec_a"] = None
                    bucket["rec_b"] = None
                elif action == "remove":
                    pending[group].pop(event_key)
                    if not pending[group]:
                        del pending[group]

    try:
        while running:
            msg = consumer.poll(1.0)
            now = time.monotonic()

            # Watermark/retention sweep, throttled to once a second.
            if now - last_flush >= 1.0:
                _sweep(now)
                last_flush = now

            if msg is None:
                continue

            if msg.error():
                err = msg.error()
                code = err.code() if hasattr(err, "code") and callable(err.code) else None
                if (code is not None and code == KafkaError._PARTITION_EOF) or "_PARTITION_EOF" in str(err):
                    continue
                print(f"[repeatability-diff] consumer error: {err}", file=sys.stderr, flush=True)
                consumer.commit(message=msg, asynchronous=False)
                continue

            topic = msg.topic()
            if topic not in (config.viz_topic_a, config.viz_topic_b):
                # Unexpected topic: defensive, should not happen with subscribe().
                print(f"[repeatability-diff] unexpected topic {topic!r}", file=sys.stderr, flush=True)
                consumer.commit(message=msg, asynchronous=False)
                continue
            side = "a" if topic == config.viz_topic_a else "b"

            raw = msg.value()
            try:
                record_in: Dict[str, Any] = json.loads(raw)
            except Exception as exc:
                print(f"[repeatability-diff] JSON parse error on {topic}: {exc}", file=sys.stderr, flush=True)
                consumer.commit(message=msg, asynchronous=False)
                continue

            key = resolve_join_key(record_in)
            if key is None:
                rejected_key += 1
                print(
                    f"[repeatability-diff] rejected record without strict join key "
                    f"(Group/source_event_id missing) on {topic}",
                    file=sys.stderr,
                    flush=True,
                )
                consumer.commit(message=msg, asynchronous=False)
                continue
            group, event_key = key
            timestamp = str(record_in.get("timestamp") or "")

            bucket, reason, dropped = process_arrival(
                pending, side, group, event_key, record_in, now, timestamp=timestamp,
            )
            if dropped:
                dropped_late += 1
                if dropped_late % log_every == 0:
                    print(
                        f"[repeatability-diff] dropped {dropped_late} record(s) past "
                        f"retention (last: Group={group} event={event_key})",
                        file=sys.stderr,
                        flush=True,
                    )
            elif bucket is not None and reason is not None:
                build_and_maybe_emit(group, event_key, bucket, config, reason, _emit)
            # else: still partial and not yet timed out, or identical re-arrival.

            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        pass
    finally:
        # Drain whatever is still pending so no comparison is silently dropped.
        for group, event_key, bucket in drain_shutdown(pending):
            build_and_maybe_emit(group, event_key, bucket, config, "shutdown", _emit)
        producer.flush(10)
        consumer.close()

    print(
        f"[repeatability-diff] done. total emitted={emitted_total} "
        f"dropped_late={dropped_late} rejected_key={rejected_key}",
        flush=True,
    )


if __name__ == "__main__":
    main()
