"""Unit tests for the repeatability diff service helpers.

The diff service joins the two repeatability instance viz topics on the strict
key ``(Group, source_event_id)``, compares the configured fields with exact
equality (no epsilon) and publishes the score difference.  Like the consensus
tests, these exercise the pure state helpers and the Kafka config builders —
no broker is ever started.
"""

from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional, Tuple
from unittest import TestCase

from validation.repeatability.repeatability_diff import (
    DiffConfig,
    build_and_maybe_emit,
    build_consumer_config,
    build_diff_record,
    build_producer_config,
    compute_identical,
    drain_shutdown,
    equal_values,
    load_config,
    numeric_or_none,
    parse_extra_fields,
    process_arrival,
    resolve_join_key,
    sweep_action,
)

_NOW = 1000.0
_TS = "2026-01-01 12:34:56"
_EVENT = "event-000000000001"
_GROUP = "G1"

_CONFIG = DiffConfig(
    bootstrap_servers="kafka1:9092",
    viz_topic_a="repeatability-v1-viz",
    viz_topic_b="repeatability-v2-viz",
    dest_topic="repeatability-diff",
    group_id="repeatability-diff-consumer",
    model_id="hstree",
    score_field="hstree_score",
    status_field="hstree_status",
    threshold_field="hstree_threshold",
    anomaly_field="hstree_anomaly",
    extra_fields=("feat_x", "label_y"),
    timeout_sec=30.0,
    retention_sec=300.0,
)


def _record(**overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "timestamp": _TS,
        "Group": _GROUP,
        "source_event_id": _EVENT,
        "hstree_score": 0.0123,
        "hstree_status": "normal",
        "hstree_threshold": 1.5,
        "hstree_anomaly": False,
        "feat_x": 2.0,
        "label_y": "abc",
    }
    base.update(overrides)
    return base


_UNSET = object()


def _bucket(
    rec_a: Any = _UNSET,
    rec_b: Any = _UNSET,
) -> Dict[str, Any]:
    """Bucket factory; pass ``None`` explicitly to simulate a missing side."""
    return {
        "timestamp": _TS,
        "Group": _GROUP,
        "source_event_id": _EVENT,
        "rec_a": _record() if rec_a is _UNSET else rec_a,
        "rec_b": _record() if rec_b is _UNSET else rec_b,
        "first_seen": _NOW,
        "last_emit_signature": None,
        "sealed": False,
    }


def _arrive(
    pending: Dict[str, Any],
    side: str,
    record: Dict[str, Any],
    now: float = _NOW,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], bool]:
    key = resolve_join_key(record)
    assert key is not None
    group, event_key = key
    return process_arrival(
        pending, side, group, event_key, record, now,
        timestamp=str(record.get("timestamp") or ""),
    )


class EqualValuesTest(TestCase):
    def test_none_and_absent_are_equal(self) -> None:
        self.assertTrue(equal_values(None, None))
        self.assertTrue(equal_values(None, None))

    def test_one_side_none_is_divergent(self) -> None:
        self.assertFalse(equal_values(None, 0.0))
        self.assertFalse(equal_values(0.0, None))
        self.assertFalse(equal_values(None, "normal"))
        self.assertFalse(equal_values("normal", None))

    def test_two_nan_are_equal(self) -> None:
        self.assertTrue(equal_values(float("nan"), float("nan")))
        self.assertFalse(equal_values(float("nan"), 1.0))
        self.assertFalse(equal_values(1.0, float("nan")))

    def test_exact_float_equality_without_epsilon(self) -> None:
        self.assertTrue(equal_values(0.0123, 0.0123))
        self.assertTrue(equal_values(-0.0, 0.0))  # -0.0 == 0.0 accepted
        self.assertTrue(equal_values(1, 1.0))     # int == float exact match
        self.assertFalse(equal_values(0.0123, 0.0123 + 1e-12))  # no epsilon
        self.assertFalse(equal_values(0.1 + 0.2, 0.3))

    def test_other_values_use_plain_equality(self) -> None:
        self.assertTrue(equal_values("normal", "normal"))
        self.assertFalse(equal_values("normal", "anomaly"))
        self.assertTrue(equal_values(True, True))
        self.assertFalse(equal_values(True, False))
        self.assertFalse(equal_values("1", 1))


class NumericAndIntTwinTest(TestCase):
    def test_numeric_or_none(self) -> None:
        self.assertEqual(numeric_or_none(1.5), 1.5)
        self.assertEqual(numeric_or_none(3), 3.0)
        self.assertIsNone(numeric_or_none(None))
        self.assertIsNone(numeric_or_none("1.5"))
        self.assertIsNone(numeric_or_none(True))  # booleans are not numeric
        self.assertTrue(math.isnan(numeric_or_none(float("nan"))))

    def test_int_twin_never_boolean(self) -> None:
        from validation.repeatability.repeatability_diff import _int_twin

        self.assertEqual(_int_twin(True), 1)
        self.assertEqual(_int_twin(False), 0)
        self.assertIsNone(_int_twin(None))
        self.assertIsNone(_int_twin("yes"))


class ParseExtraFieldsTest(TestCase):
    def test_trim_drop_empties_and_dedup_in_order(self) -> None:
        self.assertEqual(parse_extra_fields("a, b ,a,c,"), ("a", "b", "c"))
        self.assertEqual(parse_extra_fields(""), ())
        self.assertEqual(parse_extra_fields(None), ())
        self.assertEqual(parse_extra_fields("  x  "), ("x",))


class StrictJoinKeyTest(TestCase):
    def test_valid_key_is_string_pair(self) -> None:
        self.assertEqual(resolve_join_key(_record()), (_GROUP, _EVENT))
        self.assertEqual(
            resolve_join_key(_record(Group=123, source_event_id=45)),
            ("123", "45"),
        )

    def test_missing_or_empty_key_is_rejected(self) -> None:
        self.assertIsNone(resolve_join_key(_record(Group=None)))
        self.assertIsNone(resolve_join_key(_record(Group="")))
        self.assertIsNone(resolve_join_key(_record(source_event_id=None)))
        self.assertIsNone(resolve_join_key(_record(source_event_id="")))
        self.assertIsNone(resolve_join_key({"timestamp": _TS}))

    def test_two_events_sharing_one_timestamp_stay_distinct(self) -> None:
        pending: Dict[str, Any] = {}
        first = _record(source_event_id="event-1")
        second = _record(source_event_id="event-2")  # same display second
        for record in (first, second):
            _arrive(pending, "a", record)
        self.assertEqual(len(pending[_GROUP]), 2)
        self.assertIn("event-1", pending[_GROUP])
        self.assertIn("event-2", pending[_GROUP])


class ComparisonTest(TestCase):
    def test_complete_identical_bucket(self) -> None:
        record = build_diff_record(_bucket(), _CONFIG, "complete")

        self.assertEqual(record["identical"], 1)
        self.assertEqual(record["score_diff"], 0.0)
        self.assertEqual(record["abs_diff"], 0.0)
        self.assertTrue(record["complete"])
        self.assertEqual(record["complete_int"], 1)
        self.assertEqual(record["emit_reason"], "complete")
        self.assertEqual(record["warming_up_a"], 0)
        self.assertEqual(record["warming_up_b"], 0)
        self.assertEqual(record["model"], "hstree")
        self.assertEqual(record["timestamp"], _TS)

    def test_score_divergence(self) -> None:
        record = build_diff_record(
            _bucket(rec_b=_record(hstree_score=0.02)), _CONFIG, "complete",
        )
        self.assertEqual(record["identical"], 0)
        self.assertAlmostEqual(record["score_diff"], 0.0123 - 0.02)
        self.assertAlmostEqual(record["abs_diff"], abs(0.0123 - 0.02))

    def test_status_threshold_anomaly_divergences(self) -> None:
        for field, value in (
            ("hstree_status", "anomaly"),
            ("hstree_threshold", 1.6),
            ("hstree_anomaly", True),
        ):
            record = build_diff_record(
                _bucket(rec_b=_record(**{field: value})), _CONFIG, "complete",
            )
            self.assertEqual(record["identical"], 0, f"divergence on {field}")

    def test_warmup_state_is_numeric_and_keeps_an_absent_side_null(self) -> None:
        record = build_diff_record(
            _bucket(rec_a=_record(hstree_status="warming_up")), _CONFIG, "complete",
        )
        self.assertEqual(record["warming_up_a"], 1)
        self.assertEqual(record["warming_up_b"], 0)

        incomplete = build_diff_record(_bucket(rec_b=None), _CONFIG, "timeout")
        self.assertEqual(incomplete["warming_up_a"], 0)
        self.assertIsNone(incomplete["warming_up_b"])

    def test_extra_field_divergences(self) -> None:
        record = build_diff_record(
            _bucket(rec_b=_record(feat_x=2.5, label_y="zzz")), _CONFIG, "complete",
        )
        self.assertEqual(record["identical"], 0)
        self.assertAlmostEqual(record["feat_x_diff"], 2.0 - 2.5)
        self.assertAlmostEqual(record["feat_x_abs_diff"], 0.5)
        self.assertNotIn("label_y_diff", record)  # non-numeric: no diff keys
        self.assertEqual(record["label_y_a"], "abc")
        self.assertEqual(record["label_y_b"], "zzz")

    def test_incomplete_bucket_identical_is_none(self) -> None:
        bucket = _bucket(rec_b=None)
        record = build_diff_record(bucket, _CONFIG, "timeout")
        self.assertIsNone(record["identical"])
        self.assertFalse(record["complete"])
        self.assertEqual(record["complete_int"], 0)

    def test_missing_or_non_numeric_score_gives_null_diff(self) -> None:
        record = build_diff_record(
            _bucket(rec_b=_record(hstree_score=None)), _CONFIG, "complete",
        )
        self.assertIsNone(record["score_diff"])
        self.assertIsNone(record["abs_diff"])
        self.assertEqual(record["score_b"], None)

        record = build_diff_record(
            _bucket(rec_a=_record(hstree_score="oops")), _CONFIG, "complete",
        )
        self.assertIsNone(record["score_diff"])

    def test_timestamp_takes_a_then_b(self) -> None:
        bucket = _bucket(rec_b=_record(timestamp="other-time"))
        self.assertEqual(build_diff_record(bucket, _CONFIG, "complete")["timestamp"], _TS)
        bucket = _bucket(rec_a=None, rec_b=_record(timestamp="other-time"))
        bucket["timestamp"] = "other-time"
        self.assertEqual(
            build_diff_record(bucket, _CONFIG, "timeout")["timestamp"], "other-time",
        )

    def test_timestamp_b_then_a_uses_a_timestamp(self) -> None:
        """The display timestamp must be A's once A arrives (contract: A, sinon B)."""
        pending: Dict[str, Any] = {}
        b_record = _record(timestamp="2026-01-01 00:00:01")
        a_record = _record(timestamp="2026-01-01 00:00:02")

        bucket, _, _ = _arrive(pending, "b", b_record, now=_NOW)
        self.assertEqual(bucket["timestamp"], "2026-01-01 00:00:01")

        bucket, reason, _ = _arrive(pending, "a", a_record, now=_NOW + 1.0)
        self.assertEqual(reason, "complete")
        self.assertEqual(bucket["timestamp"], "2026-01-01 00:00:02")

        record = build_diff_record(bucket, _CONFIG, "complete")
        self.assertEqual(record["timestamp"], "2026-01-01 00:00:02")

    def test_timestamp_filled_by_b_when_a_has_none(self) -> None:
        pending: Dict[str, Any] = {}
        _arrive(pending, "a", _record(timestamp=""), now=_NOW)
        bucket = pending[_GROUP][_EVENT]
        self.assertEqual(bucket["timestamp"], "")

        bucket, _, _ = _arrive(pending, "b", _record(), now=_NOW + 1.0)
        self.assertEqual(bucket["timestamp"], _TS)

    def test_compute_identical_on_all_fields(self) -> None:
        fields = _CONFIG.compared_fields
        self.assertEqual(compute_identical(_record(), _record(), fields), 1)
        self.assertEqual(compute_identical(_record(), _record(hstree_score=9.0), fields), 0)
        self.assertIsNone(compute_identical(_record(), None, fields))


class OutputContractTest(TestCase):
    def test_flat_json_with_expected_types(self) -> None:
        record = build_diff_record(
            _bucket(rec_b=_record(hstree_anomaly=True)), _CONFIG, "complete",
        )
        payload = json.loads(json.dumps(record))

        # anomaly twins and identical are integers, never booleans.
        self.assertIs(payload["anomaly_a"], 0)
        self.assertIs(payload["anomaly_b"], 1)
        self.assertIs(payload["identical"], 0)
        self.assertNotIsInstance(payload["anomaly_a"], bool)
        self.assertNotIsInstance(payload["identical"], bool)
        # complete stays boolean and complete_int is its numeric twin.
        self.assertIsInstance(payload["complete"], bool)
        self.assertIs(payload["complete_int"], 1)
        self.assertIn(payload["emit_reason"], {"complete", "timeout", "progress", "shutdown"})

        # No nested JSON structure anywhere.
        for key, value in payload.items():
            self.assertNotIsInstance(value, (dict, list), f"nested value at {key!r}")

    def test_expected_keys_present(self) -> None:
        record = build_diff_record(_bucket(), _CONFIG, "complete")
        expected = {
            "timestamp", "Group", "source_event_id", "model",
            "score_a", "score_b", "score_diff", "abs_diff",
            "status_a", "status_b", "warming_up_a", "warming_up_b", "threshold_a", "threshold_b",
            "anomaly_a", "anomaly_b", "identical",
            "complete", "complete_int", "emit_reason",
            "feat_x_a", "feat_x_b", "feat_x_diff", "feat_x_abs_diff",
            "label_y_a", "label_y_b",
        }
        self.assertEqual(set(record), expected)

    def test_null_schema_for_incomplete(self) -> None:
        record = build_diff_record(_bucket(rec_b=None), _CONFIG, "timeout")
        self.assertIsNone(record["anomaly_b"])
        self.assertIsNone(record["identical"])
        self.assertIsNone(record["score_diff"])
        self.assertEqual(record["complete_int"], 0)

    def test_nan_values_are_normalized_to_null_in_output(self) -> None:
        """NaN stays NaN for the verdict but is published as null, never as a
        bare NaN literal: json.dumps(allow_nan=False) must succeed."""
        nan = float("nan")
        record = build_diff_record(
            _bucket(
                rec_a=_record(hstree_score=nan, feat_x=nan),
                rec_b=_record(hstree_score=nan, feat_x=nan),
            ),
            _CONFIG,
            "complete",
        )

        # Verdict keeps NaN == NaN semantics on the compared fields.
        self.assertEqual(record["identical"], 1)

        # Published raw values are null, diffs are null.
        self.assertIsNone(record["score_a"])
        self.assertIsNone(record["score_b"])
        self.assertIsNone(record["score_diff"])
        self.assertIsNone(record["abs_diff"])
        self.assertIsNone(record["feat_x_a"])
        self.assertIsNone(record["feat_x_b"])
        self.assertIsNone(record["feat_x_diff"])
        self.assertIsNone(record["feat_x_abs_diff"])

        # Standard JSON only: no bare NaN literal survives serialization.
        json.dumps(record, allow_nan=False)

    def test_nan_on_one_side_only_is_a_divergence(self) -> None:
        nan = float("nan")
        record = build_diff_record(
            _bucket(rec_a=_record(feat_x=nan), rec_b=_record(feat_x=2.0)),
            _CONFIG,
            "complete",
        )
        self.assertEqual(record["identical"], 0)
        self.assertIsNone(record["feat_x_a"])
        self.assertIsNone(record["feat_x_diff"])


class LifecycleTest(TestCase):
    def setUp(self) -> None:
        self.pending: Dict[str, Any] = {}

    def test_first_partial_arrival_waits_for_watermark(self) -> None:
        bucket, reason, dropped = _arrive(self.pending, "a", _record())
        self.assertFalse(dropped)
        self.assertIsNone(reason)
        self.assertFalse(bucket["sealed"])

    def test_second_side_completes_with_complete_reason(self) -> None:
        _arrive(self.pending, "a", _record())
        bucket, reason, _ = _arrive(self.pending, "b", _record())
        self.assertEqual(reason, "complete")
        self.assertTrue(bucket["rec_a"] is not None and bucket["rec_b"] is not None)

    def test_timeout_publishes_once_then_stays_quiet(self) -> None:
        _arrive(self.pending, "a", _record(), now=_NOW)
        bucket = self.pending[_GROUP][_EVENT]
        self.assertEqual(sweep_action(bucket, _NOW + 31.0, 30.0, 300.0), "timeout")
        bucket["last_emit_signature"] = ("sig",)  # as if published
        self.assertIsNone(sweep_action(bucket, _NOW + 62.0, 30.0, 300.0))

    def test_no_timeout_before_watermark_and_after_completion(self) -> None:
        _arrive(self.pending, "a", _record())
        bucket = self.pending[_GROUP][_EVENT]
        self.assertIsNone(sweep_action(bucket, _NOW + 5.0, 30.0, 300.0))
        _arrive(self.pending, "b", _record(), now=_NOW + 6.0)
        self.assertIsNone(sweep_action(bucket, _NOW + 62.0, 30.0, 300.0))

    def test_seal_then_removal_and_dropped_late(self) -> None:
        _arrive(self.pending, "a", _record())
        bucket = self.pending[_GROUP][_EVENT]

        self.assertEqual(sweep_action(bucket, _NOW + 301.0, 30.0, 300.0), "seal")
        bucket["sealed"] = True
        self.assertIsNone(sweep_action(bucket, _NOW + 400.0, 30.0, 300.0))
        self.assertEqual(sweep_action(bucket, _NOW + 601.0, 30.0, 300.0), "remove")

        # Arrivals past the seal are dropped and counted by the caller.
        _, reason, dropped = _arrive(self.pending, "b", _record(), now=_NOW + 310.0)
        self.assertTrue(dropped)
        self.assertIsNone(reason)

    def test_retention_normalized_to_at_least_timeout(self) -> None:
        config = load_config({
            "REPEATABILITY_TIMEOUT_SEC": "120",
            "REPEATABILITY_RETENTION_SEC": "30",
        })
        self.assertEqual(config.timeout_sec, 120.0)
        self.assertEqual(config.retention_sec, 120.0)

    def test_late_arrival_completes_timed_out_bucket_with_complete_reason(self) -> None:
        _arrive(self.pending, "a", _record(), now=_NOW)
        bucket = self.pending[_GROUP][_EVENT]
        bucket["last_emit_signature"] = ("sig",)  # timeout already published

        _, reason, _ = _arrive(self.pending, "b", _record(), now=_NOW + 40.0)
        self.assertEqual(reason, "complete")

    def test_modification_of_complete_bucket_uses_progress_reason(self) -> None:
        _arrive(self.pending, "a", _record())
        _arrive(self.pending, "b", _record())
        _, reason, _ = _arrive(self.pending, "a", _record(hstree_score=9.9), now=_NOW + 1.0)
        self.assertEqual(reason, "progress")
        bucket = self.pending[_GROUP][_EVENT]
        self.assertEqual(bucket["rec_a"]["hstree_score"], 9.9)

    def test_incomplete_progress_after_timeout_emit(self) -> None:
        _arrive(self.pending, "a", _record())
        bucket = self.pending[_GROUP][_EVENT]
        bucket["last_emit_signature"] = ("sig",)
        # A *different* a-record replaces the slot while still incomplete.
        _, reason, _ = _arrive(self.pending, "a", _record(hstree_score=4.2), now=_NOW + 1.0)
        self.assertEqual(reason, "progress")


class SignatureDedupTest(TestCase):
    def setUp(self) -> None:
        self.pending: Dict[str, Any] = {}
        self.emitted: List[Dict[str, Any]] = []

    def _emit(self, record: Dict[str, Any]) -> None:
        self.emitted.append(record)

    def test_identical_re_arrival_does_not_republish(self) -> None:
        _arrive(self.pending, "a", _record())
        _arrive(self.pending, "b", _record())
        bucket = self.pending[_GROUP][_EVENT]
        self.assertTrue(build_and_maybe_emit(_GROUP, _EVENT, bucket, _CONFIG, "complete", self._emit))
        self.assertEqual(len(self.emitted), 1)

        # Identical re-arrival: process_arrival returns reason=None.
        _, reason, _ = _arrive(self.pending, "b", _record(), now=_NOW + 2.0)
        self.assertIsNone(reason)
        self.assertEqual(len(self.emitted), 1)

    def test_signature_change_republishes(self) -> None:
        _arrive(self.pending, "a", _record())
        _arrive(self.pending, "b", _record())
        bucket = self.pending[_GROUP][_EVENT]
        build_and_maybe_emit(_GROUP, _EVENT, bucket, _CONFIG, "complete", self._emit)
        self.assertEqual(len(self.emitted), 1)

        # Different b-record: slot replaced, signature changes -> re-publish.
        _, reason, _ = _arrive(self.pending, "b", _record(hstree_score=7.7), now=_NOW + 1.0)
        self.assertEqual(reason, "progress")
        self.assertTrue(
            build_and_maybe_emit(_GROUP, _EVENT, bucket, _CONFIG, reason, self._emit)
        )
        self.assertEqual(len(self.emitted), 2)
        self.assertEqual(self.emitted[1]["score_b"], 7.7)

    def test_same_state_with_new_reason_republishes(self) -> None:
        _arrive(self.pending, "a", _record())
        bucket = self.pending[_GROUP][_EVENT]
        build_and_maybe_emit(_GROUP, _EVENT, bucket, _CONFIG, "timeout", self._emit)
        self.assertEqual(len(self.emitted), 1)

        # Drain at shutdown uses another reason: the signature must differ.
        self.assertTrue(
            build_and_maybe_emit(_GROUP, _EVENT, bucket, _CONFIG, "shutdown", self._emit)
        )
        self.assertEqual(self.emitted[1]["emit_reason"], "shutdown")

    def test_nan_redelivery_is_not_a_change(self) -> None:
        record = _record(hstree_score=float("nan"))
        _arrive(self.pending, "a", record)
        _arrive(self.pending, "b", record)
        bucket = self.pending[_GROUP][_EVENT]
        build_and_maybe_emit(_GROUP, _EVENT, bucket, _CONFIG, "complete", self._emit)

        # Same record redelivered (still NaN): canonical form matches -> no-op.
        _, reason, _ = _arrive(self.pending, "a", _record(hstree_score=float("nan")), now=_NOW + 1.0)
        self.assertIsNone(reason)
        self.assertEqual(len(self.emitted), 1)


class ShutdownDrainTest(TestCase):
    def test_drain_pops_unsealed_and_skips_sealed(self) -> None:
        pending: Dict[str, Any] = {}
        _arrive(pending, "a", _record())
        _arrive(pending, "b", _record(), now=_NOW)  # complete
        other_event = "event-2"
        _arrive(pending, "a", _record(source_event_id=other_event))
        sealed = pending[_GROUP][other_event]
        sealed["sealed"] = True

        drained = drain_shutdown(pending)

        self.assertEqual(len(drained), 1)
        group, event_key, bucket = drained[0]
        self.assertEqual((group, event_key), (_GROUP, _EVENT))
        self.assertFalse(bucket["sealed"])
        self.assertEqual(pending, {})

    def test_drain_then_publish_shutdown(self) -> None:
        pending: Dict[str, Any] = {}
        _arrive(pending, "a", _record())
        _arrive(pending, "b", _record())
        emitted: List[Dict[str, Any]] = []
        for group, event_key, bucket in drain_shutdown(pending):
            build_and_maybe_emit(group, event_key, bucket, _CONFIG, "shutdown", emitted.append)
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["emit_reason"], "shutdown")
        self.assertTrue(emitted[0]["complete"])
        self.assertEqual(emitted[0]["complete_int"], 1)


class KafkaConfigTest(TestCase):
    def test_consumer_config(self) -> None:
        conf = build_consumer_config(_CONFIG)
        self.assertEqual(conf["bootstrap.servers"], "kafka1:9092")
        self.assertEqual(conf["group.id"], "repeatability-diff-consumer")
        self.assertEqual(conf["auto.offset.reset"], "earliest")
        self.assertFalse(conf["enable.auto.commit"])
        self.assertTrue(conf["enable.partition.eof"])
        self.assertNotIn("enable.eof.notify", conf)

    def test_producer_config_is_idempotent(self) -> None:
        conf = build_producer_config(_CONFIG)
        self.assertTrue(conf["enable.idempotence"])
        self.assertEqual(conf["acks"], "all")
        self.assertEqual(conf["max.in.flight.requests.per.connection"], 1)

    def test_load_config_defaults(self) -> None:
        config = load_config({})
        self.assertEqual(config.viz_topic_a, "repeatability-v1-viz")
        self.assertEqual(config.viz_topic_b, "repeatability-v2-viz")
        self.assertEqual(config.dest_topic, "repeatability-diff")
        self.assertEqual(config.group_id, "repeatability-diff-consumer")
        self.assertEqual(config.model_id, "hstree")
        self.assertEqual(config.score_field, "hstree_score")
        self.assertEqual(config.status_field, "hstree_status")
        self.assertEqual(config.threshold_field, "hstree_threshold")
        self.assertEqual(config.anomaly_field, "hstree_anomaly")
        self.assertEqual(config.extra_fields, ())
        self.assertEqual(config.timeout_sec, 30.0)
        self.assertEqual(config.retention_sec, 300.0)
        self.assertEqual(config.compared_fields, (
            "hstree_score", "hstree_status", "hstree_threshold", "hstree_anomaly",
        ))


class DisabledModeTest(TestCase):
    def test_idle_env_is_detected_before_kafka_clients(self) -> None:
        """DASMA_ENABLED=0 must idle: main() calls signal.pause() before clients.

        The contract forbids starting any Kafka client when disabled, so we
        verify the guard order by patching signal.pause to raise and asserting
        the Kafka factories are never constructed.
        """
        import os
        from unittest import mock

        import validation.repeatability.repeatability_diff as module

        env = dict(os.environ)
        env["DASMA_ENABLED"] = "0"
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.object(module.signal, "pause", side_effect=KeyboardInterrupt):
                with mock.patch.object(
                    module, "_make_consumer", side_effect=AssertionError("consumer created"),
                ) as make_consumer, mock.patch.object(
                    module, "_make_producer", side_effect=AssertionError("producer created"),
                ) as make_producer:
                    with self.assertRaises(KeyboardInterrupt):
                        module.main()
        make_consumer.assert_not_called()
        make_producer.assert_not_called()
