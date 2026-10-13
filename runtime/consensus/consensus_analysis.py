#!/usr/bin/env python3
"""
consensus_analysis.py

Consumes from the model viz topics (hstree-analysis-viz, iforest-analysis-viz,
kitnet-analysis-viz, deepant-analysis-viz, mdragstream-point-analysis-viz) and
builds a **strict event-time consensus**: for every business instant it joins
the verdicts of the active models that analysed the *same* source data point.

Consensus semantics
-------------------
The consensus is a *vote of the models for one source event*, not an event
counter.  For a given ``source_event_id`` ``E`` and group ``G``:

    HSTree   -> normal / anomaly
    KitNET   -> normal / anomaly
    IForest  -> normal / anomaly
    ...
    anomaly_count = number of active models reporting anomaly for E in G

The models do not produce their scores at the same rate: their compute cost
differs (IsolationForest refits, KitNET autoencoder training, DeepAnT inference).
The verdicts for the same ``(G, E)`` therefore arrive asynchronously and are
accumulated in a per-``(Group, source_event_id)`` bucket.  A bucket is
published when:

  * **completion** — every active model has reported a verdict for ``(G, E)``;
    the consensus is published immediately with ``emit_reason="complete"``;
  * **timeout / watermark** — the bucket has been pending for more than
    ``CONSENSUS_TIMEOUT_SEC`` wall-clock seconds without completing; a partial
    consensus is published (``emit_reason="timeout"``) with the late models
    handled by ``CONSENSUS_LATE_POLICY``.

**Progressive completion.**  After a timeout publish, the bucket is *kept*: as
the slow models finally report, an updated consensus for the same ``(G, E)`` is
re-published (``emit_reason="progress"`` while still partial, then ``"complete"``).
Because InfluxDB/Telegraf key a point by measurement + tags + timestamp, the
consensus bucket stores ``source_event_id`` as a tag: re-publishes for the same
event overwrite each other, but distinct events sharing the same display second
do not overwrite each other.

**Monotone (sticky) verdicts.**  Within a bucket a verdict can only strengthen:
the precedence is ``None`` (warming up) < ``False`` < ``True``.  This makes
``anomaly_count`` and ``reported_models`` non-decreasing over the life of a
bucket, so a re-publish can never *lower* a consensus and erase an anomaly.  At
one source event this is identical to a strict join (each model reports once).

This replaces the former *latest-state* design, which kept only the last known
verdict per ``(Group, model)`` and could mix a KitNET verdict at ``T`` with an
older HSTree verdict at ``T-5s``.  The event-time join guarantees that the
``Y`` scores combined into one consensus point describe the *same* instant.

Known limitation — join-key granularity
---------------------------------------
The realtime producer restamps records at *second* granularity
(``%Y-%m-%d %H:%M:%S``).  Under the accelerated modes (``realtime-x5/x20/x100``)
several distinct source points of the same group can share one display second.
Exact joins at those rates rely on the producer-generated ``source_event_id``
propagated through every model.  If legacy messages do not carry that key, the
service falls back to the old ``(Group, timestamp)`` bucket and the result is
only exact at <= 1 source point per group-second.

Late-model policy (``CONSENSUS_LATE_POLICY``)
--------------------------------------------
  missing        (default) late models stay ``null``: not counted, not reported.
                 ``anomaly_count`` reflects only the verdicts that actually
                 arrived for this instant — the strict, honest interpretation.
  carry_forward  (experimental) late models are filled with their last known
                 verdict for the group; carried anomalies *do* increment
                 ``anomaly_count`` and are reported through ``carried_models``.
                 ``last_known`` is global per ``(Group, model)`` with no
                 source-time guard, so with out-of-order delivery a future
                 verdict can be carried onto an older instant.  Use only when
                 per-partition ordering is trusted; ``missing`` stays the
                 default until a TTL / source-time guard lands.

Output message (JSON).  Each model carries a boolean ``*_anomaly`` (for JSON
consumers) and a numeric ``*_vote`` (1 anomaly / 0 normal / 0 missing) that
InfluxDB/Telegraf can persist — Grafana decomposes the consensus from the votes,
which sum to ``anomaly_count``:
  {
    "timestamp":         "2026-01-01 12:34:56",
    "source_event_id":   "event-000000123456",
    "Group":             "Sortie CTA",
    "anomaly_count":     2,
    "hstree_anomaly":    true,  "hstree_vote":     1,
    "iforest_anomaly":   true,  "iforest_vote":    1,
    "kitnet_anomaly":    false, "kitnet_vote":     0,
    "deepant_anomaly":   null,  "deepant_vote":    0,
    "mdragpoint_anomaly": null, "mdragpoint_vote": 0,
    "active_models":     3,
    "reported_models":   3,
    "carried_models":    0,
    "complete":          true,
    "emit_reason":       "complete"
  }

``emit_reason`` is one of ``complete`` (all active models voted), ``timeout``
(first partial publish at the watermark), ``progress`` (a later, more complete
re-publish of a timed-out instant), or ``shutdown`` (drained on exit).

Useful environment variables:
  BOOTSTRAP_SERVERS           Kafka brokers, default kafka1:9092,kafka2:9092,kafka3:9092
  CONSENSUS_GROUP_ID          Kafka consumer group id, default consensus-analysis-consumer
  KAFKA_DEST_TOPIC            Output topic, default consensus-analysis
  SELECTED_MODELS             Comma-separated active models, default hstree,iforest,kitnet
  CONSENSUS_TIMEOUT_SEC       Wall-clock watermark before a partial bucket is published (default 30)
  CONSENSUS_RETENTION_SEC     Seconds a bucket accepts late verdicts before it is sealed/evicted (default 300)
  CONSENSUS_LATE_POLICY       missing | carry_forward (default missing)
  ANALYSE_IDLE_EXIT_SEC       Exit after N idle seconds, 0 disables (default 0)
  LOG_EVERY                   Log every N emitted records (default 100)
  DASMA_ENABLED               Set to 0 to disable this service (idle loop)
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, Producer

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODELS: Tuple[str, ...] = ("hstree", "iforest", "kitnet", "deepant", "mdragpoint")

TOPIC_MAP: Dict[str, str] = {
    "hstree-analysis-viz": "hstree",
    "iforest-analysis-viz": "iforest",
    "kitnet-analysis-viz": "kitnet",
    "deepant-analysis-viz": "deepant",
    "mdragstream-point-analysis-viz": "mdragpoint",
}

LATE_POLICIES: Tuple[str, ...] = ("missing", "carry_forward")

# ---------------------------------------------------------------------------
# Pure, testable state helpers
# ---------------------------------------------------------------------------

# A bucket holds every verdict received so far for one (Group, source_event_id).
#   bucket = {
#       "timestamp": str,                         # display/event timestamp
#       "source_event_id": Optional[str],         # exact join key when available
#       "verdicts":  {model: Optional[bool]},   # sticky/monotone, see _stronger
#       "first_seen": float,                     # wall-clock of the first verdict
#       "last_emit":  Optional[Tuple[int, int]], # last published (anomaly, reported)
#       "sealed":     bool,                       # past retention: ignores late verdicts
#   }
Bucket      = Dict[str, Any]
GroupBuckets = Dict[str, Bucket]          # event key -> bucket
Pending      = Dict[str, GroupBuckets]    # group     -> {event key -> bucket}

# Side table feeding the carry_forward policy.
#   last_known[group][model] = Optional[bool]
LastKnown = Dict[str, Dict[str, Optional[bool]]]

# Verdict strength order for the sticky merge: warming-up < normal < anomaly.
_VERDICT_RANK: Dict[Optional[bool], int] = {None: 0, False: 1, True: 2}


def parse_anomaly_value(raw: Any) -> Optional[bool]:
    """Normalise a raw anomaly field to True / False / None (warming-up)."""
    if raw is True:
        return True
    if raw is False:
        return False
    return None


def _vote_value(verdict: Optional[bool]) -> int:
    """Numeric twin of a verdict for InfluxDB: 1 anomaly, 0 normal, 0 missing.

    Telegraf's JSON parser persists numbers but drops booleans, so the boolean
    ``*_anomaly`` fields never reach InfluxDB.  These ``*_vote`` integers are what
    Grafana reads to decompose the consensus per model.  Missing votes are
    encoded as 0 so progressive re-publishes overwrite previous numeric values
    in InfluxDB instead of leaving stale model votes behind.
    """
    if verdict is True:
        return 1
    if verdict is False:
        return 0
    return 0


def _stronger(current: Optional[bool], incoming: Optional[bool]) -> Optional[bool]:
    """Return the stronger of two verdicts (None < False < True).

    Keeping the strongest verdict seen for a model within one instant makes the
    consensus monotone, so a re-published point can never erase an anomaly.
    """
    return current if _VERDICT_RANK[current] >= _VERDICT_RANK[incoming] else incoming


def resolve_event_key(record: Dict[str, Any]) -> Tuple[str, Optional[str], str]:
    """Return ``(event_key, source_event_id, timestamp)`` for an input record.

    ``source_event_id`` is the exact join key.  ``timestamp`` remains the display
    time and is used as a backwards-compatible fallback for legacy messages.
    """
    timestamp = str(record.get("timestamp") or "")
    raw_source_event_id = record.get("source_event_id")
    source_event_id = (
        str(raw_source_event_id)
        if raw_source_event_id not in (None, "")
        else None
    )
    return source_event_id or timestamp, source_event_id, timestamp


def record_verdict(
    pending: Pending,
    group: str,
    event_key: str,
    model: str,
    anomaly_val: Optional[bool],
    now: float,
    timestamp: Optional[str] = None,
    source_event_id: Optional[str] = None,
) -> Optional[Bucket]:
    """Store *model*'s verdict in the ``(group, event_key)`` bucket.

    A fresh bucket records the wall-clock ``first_seen`` so the watermark can be
    measured from the first verdict received for that instant.  An existing
    model verdict is only ever strengthened (sticky merge).  Returns the bucket,
    or ``None`` when the bucket is sealed (the verdict arrived past retention and
    is dropped).
    """
    group_buckets = pending.setdefault(group, {})
    bucket = group_buckets.get(event_key)
    if bucket is None:
        bucket = {
            "timestamp": timestamp if timestamp is not None else event_key,
            "source_event_id": source_event_id,
            "verdicts": {},
            "first_seen": now,
            "last_emit": None,
            "sealed": False,
        }
        group_buckets[event_key] = bucket
    else:
        if bucket.get("timestamp") in (None, "") and timestamp not in (None, ""):
            bucket["timestamp"] = timestamp
        if bucket.get("source_event_id") in (None, "") and source_event_id not in (None, ""):
            bucket["source_event_id"] = source_event_id
    if bucket["sealed"]:
        return None
    verdicts = bucket["verdicts"]
    if model in verdicts:
        verdicts[model] = _stronger(verdicts[model], anomaly_val)
    else:
        verdicts[model] = anomaly_val
    return bucket


def update_last_known(
    last_known: LastKnown,
    group: str,
    model: str,
    anomaly_val: Optional[bool],
) -> None:
    """Remember the most recent verdict of *model* for *group* (carry_forward)."""
    last_known.setdefault(group, {})[model] = anomaly_val


def bucket_is_complete(bucket: Bucket, active_models: List[str]) -> bool:
    """True once every active model has reported a verdict for this instant."""
    verdicts = bucket["verdicts"]
    return all(model in verdicts for model in active_models)


def bucket_age(bucket: Bucket, now: float) -> float:
    """Wall-clock seconds since the first verdict landed in this bucket."""
    return now - bucket["first_seen"]


def pop_bucket(pending: Pending, group: str, event_key: str) -> Bucket:
    """Remove and return the ``(group, event_key)`` bucket, pruning empty groups."""
    bucket = pending[group].pop(event_key)
    if not pending[group]:
        del pending[group]
    return bucket


def sweep_action(
    bucket: Bucket,
    now: float,
    timeout_sec: float,
    retention_sec: float,
    active_models: List[str],
) -> Optional[str]:
    """Decide what the periodic sweep should do with a bucket.

    Returns one of:
      "timeout"  publish the first partial consensus (watermark reached and the
                 instant has neither completed nor been published yet);
      "seal"     stop accepting late verdicts (retention reached);
      "remove"   drop the sealed tombstone entirely (well past retention);
      None       leave the bucket untouched.
    """
    age = bucket_age(bucket, now)
    if bucket["sealed"]:
        return "remove" if age >= 2 * retention_sec else None
    if age >= retention_sec:
        return "seal"
    if (
        age >= timeout_sec
        and bucket["last_emit"] is None
        and not bucket_is_complete(bucket, active_models)
    ):
        return "timeout"
    return None


def finalize_bucket(
    group: str,
    event_key: str,
    bucket: Bucket,
    active_models: List[str],
    last_known: LastKnown,
    policy: str,
    reason: str,
) -> Dict[str, Any]:
    """Build the consensus record for one ``(group, event_key)`` bucket.

    ``reported_models`` counts the active models that produced a *fresh* verdict
    for this exact instant.  Under ``carry_forward`` the still-missing active
    models are filled from ``last_known`` and counted in ``carried_models``;
    those carried anomalies contribute to ``anomaly_count``.  Under ``missing``
    the late models stay ``null`` and do not contribute.

    Each model carries both ``*_anomaly`` (boolean, for JSON consumers) and
    ``*_vote`` (1/0, the InfluxDB-persistable twin Grafana decomposes); the
    votes of the active models sum to ``anomaly_count``.
    """
    fresh = bucket["verdicts"]
    reported_models = sum(1 for model in active_models if model in fresh)

    effective: Dict[str, Optional[bool]] = dict(fresh)
    carried_models = 0
    if policy == "carry_forward":
        group_last = last_known.get(group, {})
        for model in active_models:
            if model not in effective and model in group_last:
                effective[model] = group_last[model]
                carried_models += 1

    anomaly_count = sum(1 for model in active_models if effective.get(model) is True)
    timestamp = str(bucket.get("timestamp") or event_key)
    source_event_id = bucket.get("source_event_id")

    record: Dict[str, Any] = {
        "timestamp":       timestamp,
        "Group":           group,
        "anomaly_count":   anomaly_count,
    }
    if source_event_id not in (None, ""):
        record["source_event_id"] = str(source_event_id)
    for model in MODELS:
        verdict = effective.get(model)
        record[f"{model}_anomaly"] = verdict
        record[f"{model}_vote"] = _vote_value(verdict)
    record.update({
        "active_models":   len(active_models),
        "reported_models": reported_models,
        "carried_models":  carried_models,
        "complete":        reported_models == len(active_models),
        "emit_reason":     reason,
    })
    return record


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _get_bootstrap_servers() -> str:
    bs = os.getenv("BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
    return ",".join(p.strip() for p in bs.split(",") if p.strip()) or "kafka1:9092"


def _make_consumer(topics: List[str]) -> Consumer:
    conf = {
        "bootstrap.servers": _get_bootstrap_servers(),
        "group.id": os.getenv("CONSENSUS_GROUP_ID", "consensus-analysis-consumer"),
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,
    }
    c = Consumer(conf)
    c.subscribe(topics)
    return c


def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": _get_bootstrap_servers(),
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 1,
        "retries": 2_147_483_647,
        "delivery.timeout.ms": 300_000,
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if os.getenv("DASMA_ENABLED", "1") == "0":
        print("[consensus] DASMA_ENABLED=0 — service disabled. Idling.", flush=True)
        signal.pause()
        return

    selected_raw = os.getenv("SELECTED_MODELS", "hstree,iforest,kitnet")
    active_models: List[str] = [m for m in MODELS if m in selected_raw.split(",")]

    if not active_models:
        print("[consensus] No active models in SELECTED_MODELS — idling.", flush=True)
        signal.pause()
        return

    source_topics = [t for t, m in TOPIC_MAP.items() if m in active_models]
    dest_topic    = os.getenv("KAFKA_DEST_TOPIC", "consensus-analysis")
    idle_exit_sec = int(os.getenv("ANALYSE_IDLE_EXIT_SEC", "0"))
    log_every     = max(1, int(os.getenv("LOG_EVERY", "100")))
    timeout_sec   = max(0.0, float(os.getenv("CONSENSUS_TIMEOUT_SEC", "30")))
    retention_sec = max(timeout_sec, float(os.getenv("CONSENSUS_RETENTION_SEC", "300")))
    late_policy   = os.getenv("CONSENSUS_LATE_POLICY", "missing").strip().lower()
    if late_policy not in LATE_POLICIES:
        print(
            f"[consensus] unknown CONSENSUS_LATE_POLICY={late_policy!r}, "
            f"falling back to 'missing'", file=sys.stderr, flush=True,
        )
        late_policy = "missing"

    print(f"[consensus] active_models={active_models}", flush=True)
    print(f"[consensus] subscribing to {source_topics}", flush=True)
    print(f"[consensus] publishing to '{dest_topic}'", flush=True)
    print(
        f"[consensus] mode: event-time join by (Group, source_event_id), progressive "
        f"completion timeout={timeout_sec}s retention={retention_sec}s policy={late_policy}",
        flush=True,
    )

    consumer = _make_consumer(source_topics)
    producer  = _make_producer()

    pending: Pending       = {}
    last_known: LastKnown  = {}
    running       = True
    last_msg_ts   = time.monotonic()
    last_flush    = time.monotonic()
    emitted_total = 0
    dropped_late  = 0

    def _handle_stop(sig, frame):  # noqa: ANN001
        nonlocal running
        running = False

    signal.signal(signal.SIGINT,  _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    def _emit(record: Dict[str, Any]) -> None:
        nonlocal emitted_total
        try:
            payload = json.dumps(record, ensure_ascii=False)

            def _on_delivery(err, _msg, _r=record):
                if err is not None:
                    print(
                        f"[consensus] delivery error for "
                        f"Group={_r['Group']} ts={_r['timestamp']} "
                        f"event={_r.get('source_event_id', '<timestamp-fallback>')}: {err}",
                        file=sys.stderr,
                        flush=True,
                    )

            producer.produce(
                topic=dest_topic,
                key=record["Group"],
                value=payload,
                on_delivery=_on_delivery,
            )
            producer.poll(0)
            emitted_total += 1
            if emitted_total % log_every == 0:
                print(
                    f"[consensus] emitted={emitted_total} "
                    f"last: Group={record['Group']} ts={record['timestamp']} "
                    f"event={record.get('source_event_id', '<timestamp-fallback>')} "
                    f"anomaly_count={record['anomaly_count']}/{record['active_models']} "
                    f"reported={record['reported_models']}/{record['active_models']} "
                    f"reason={record['emit_reason']}",
                    flush=True,
                )
        except Exception as exc:
            print(f"[consensus] produce error: {exc}", file=sys.stderr, flush=True)

    def _maybe_emit(group: str, event_key: str, bucket: Bucket, reason: str) -> None:
        """Publish the bucket's consensus, skipping unchanged re-publishes.

        ``last_emit`` stores the published ``(anomaly_count, reported_models)``.
        Because the verdicts are monotone, the signature only ever grows, so a
        re-publish always carries a more complete consensus.
        """
        record = finalize_bucket(
            group, event_key, bucket, active_models, last_known, late_policy, reason,
        )
        signature = (record["anomaly_count"], record["reported_models"])
        if bucket["last_emit"] == signature:
            return
        bucket["last_emit"] = signature
        _emit(record)

    def _sweep(now: float) -> None:
        """Apply timeout / retention transitions to every pending bucket."""
        for group in list(pending.keys()):
            for event_key in list(pending[group].keys()):
                bucket = pending[group][event_key]
                action = sweep_action(
                    bucket, now, timeout_sec, retention_sec, active_models,
                )
                if action == "timeout":
                    _maybe_emit(group, event_key, bucket, "timeout")
                elif action == "seal":
                    # Past retention: keep a tombstone that ignores late verdicts
                    # (so they cannot erase the published point) but free memory.
                    bucket["sealed"] = True
                    bucket["verdicts"] = {}
                elif action == "remove":
                    pop_bucket(pending, group, event_key)

    try:
        while running:
            msg = consumer.poll(1.0)
            now = time.monotonic()

            # Watermark/retention sweep: publish timed-out buckets and evict old
            # ones, even under continuous load (throttled to once a second).
            if now - last_flush >= 1.0:
                _sweep(now)
                last_flush = now

            if msg is None:
                if idle_exit_sec > 0 and now - last_msg_ts >= idle_exit_sec:
                    print(f"[consensus] Idle for {idle_exit_sec}s, exiting.", flush=True)
                    break
                continue

            if msg.error():
                err  = msg.error()
                code = err.code() if hasattr(err, "code") and callable(err.code) else None
                if (code is not None and code == KafkaError._PARTITION_EOF) or "_PARTITION_EOF" in str(err):
                    if idle_exit_sec > 0 and now - last_msg_ts >= idle_exit_sec:
                        print(f"[consensus] Idle for {idle_exit_sec}s after EOF, exiting.", flush=True)
                        break
                    continue
                print(f"[consensus] consumer error: {err}", file=sys.stderr, flush=True)
                continue

            last_msg_ts = now

            topic = msg.topic()
            model = TOPIC_MAP.get(topic)
            if model is None or model not in active_models:
                consumer.commit(message=msg, asynchronous=False)
                continue

            raw = msg.value()
            try:
                record_in: Dict[str, Any] = json.loads(raw)
            except Exception as exc:
                print(f"[consensus] JSON parse error on {topic}: {exc}", file=sys.stderr, flush=True)
                consumer.commit(message=msg, asynchronous=False)
                continue

            group     = str(record_in.get("Group") or record_in.get("group") or "__NO_GROUP__")
            event_key, source_event_id, timestamp = resolve_event_key(record_in)
            anomaly_field = f"{model}_anomaly"
            anomaly_val   = parse_anomaly_value(record_in.get(anomaly_field))

            update_last_known(last_known, group, model, anomaly_val)
            bucket = record_verdict(
                pending,
                group,
                event_key,
                model,
                anomaly_val,
                now,
                timestamp=timestamp,
                source_event_id=source_event_id,
            )

            if bucket is None:
                # Verdict arrived after the instant was sealed (past retention).
                dropped_late += 1
                if dropped_late % log_every == 0:
                    print(
                        f"[consensus] dropped {dropped_late} verdict(s) past "
                        f"retention (last: Group={group} ts={timestamp} event={event_key})",
                        file=sys.stderr, flush=True,
                    )
            elif bucket_is_complete(bucket, active_models):
                # Every active model has voted: publish the complete consensus.
                _maybe_emit(group, event_key, bucket, "complete")
            elif bucket["last_emit"] is not None:
                # Already published a timed-out partial: progressively complete it
                # as the slow models report (re-publish overwrites in InfluxDB).
                _maybe_emit(group, event_key, bucket, "progress")
            # else: still partial and not yet timed out — wait for completion or
            # the watermark sweep.

            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        pass
    finally:
        # Drain whatever is still pending so no instant is silently dropped.
        for group in list(pending.keys()):
            for event_key in list(pending[group].keys()):
                bucket = pop_bucket(pending, group, event_key)
                if not bucket["sealed"]:
                    _maybe_emit(group, event_key, bucket, "shutdown")
        producer.flush(10)
        consumer.close()

    print(
        f"[consensus] done. total emitted={emitted_total} dropped_late={dropped_late}",
        flush=True,
    )


if __name__ == "__main__":
    main()
