"""Unit tests for the event-time (Group, source_event_id) consensus helpers.

The consensus joins asynchronous model verdicts per ``(Group, source_event_id)``
and completes a bucket progressively: it publishes on completion or
at a wall-clock watermark, then re-publishes (monotone) as slow models catch up.
"""

from __future__ import annotations

from unittest import TestCase

from runtime.consensus.consensus_analysis import (
    LATE_POLICIES,
    MODELS,
    TOPIC_MAP,
    _stronger,
    _vote_value,
    bucket_age,
    bucket_is_complete,
    finalize_bucket,
    parse_anomaly_value,
    pop_bucket,
    record_verdict,
    resolve_event_key,
    sweep_action,
    update_last_known,
)

_NOW = 1000.0
_TS  = "2026-01-01 00:00:00"
_MODELS = ["hstree", "iforest", "kitnet"]
_TIMEOUT = 30.0
_RETENTION = 300.0


def _build_bucket(pending: dict, group: str, timestamp: str, verdicts: dict, now: float = _NOW):
    """Record several models' verdicts for one legacy (group, timestamp) bucket."""
    bucket = None
    for model, val in verdicts.items():
        bucket = record_verdict(pending, group, timestamp, model, val, now)
    return bucket


# ---------------------------------------------------------------------------
# parse_anomaly_value
# ---------------------------------------------------------------------------

class TestParseAnomalyValue(TestCase):
    def test_true(self):
        self.assertIs(parse_anomaly_value(True), True)

    def test_false(self):
        self.assertIs(parse_anomaly_value(False), False)

    def test_none_is_warming_up(self):
        self.assertIsNone(parse_anomaly_value(None))

    def test_string_is_warming_up(self):
        self.assertIsNone(parse_anomaly_value("true"))

    def test_integer_is_warming_up(self):
        self.assertIsNone(parse_anomaly_value(1))


# ---------------------------------------------------------------------------
# _stronger — verdict precedence None < False < True
# ---------------------------------------------------------------------------

class TestStronger(TestCase):
    def test_true_beats_everything(self):
        self.assertIs(_stronger(True, False), True)
        self.assertIs(_stronger(False, True), True)
        self.assertIs(_stronger(True, None), True)
        self.assertIs(_stronger(None, True), True)

    def test_false_beats_none(self):
        self.assertIs(_stronger(False, None), False)
        self.assertIs(_stronger(None, False), False)

    def test_none_only_when_both_none(self):
        self.assertIsNone(_stronger(None, None))

    def test_equal_keeps_current(self):
        self.assertIs(_stronger(False, False), False)
        self.assertIs(_stronger(True, True), True)


class TestVoteValue(TestCase):
    def test_true_is_one(self):
        self.assertEqual(_vote_value(True), 1)

    def test_false_is_zero(self):
        self.assertEqual(_vote_value(False), 0)

    def test_none_is_zero_for_influx(self):
        self.assertEqual(_vote_value(None), 0)


# ---------------------------------------------------------------------------
# resolve_event_key
# ---------------------------------------------------------------------------

class TestResolveEventKey(TestCase):
    def test_uses_source_event_id_when_present(self):
        event_key, source_event_id, timestamp = resolve_event_key(
            {"timestamp": _TS, "source_event_id": "event-000000000042"}
        )

        self.assertEqual(event_key, "event-000000000042")
        self.assertEqual(source_event_id, "event-000000000042")
        self.assertEqual(timestamp, _TS)

    def test_falls_back_to_timestamp_for_legacy_messages(self):
        event_key, source_event_id, timestamp = resolve_event_key({"timestamp": _TS})

        self.assertEqual(event_key, _TS)
        self.assertIsNone(source_event_id)
        self.assertEqual(timestamp, _TS)


# ---------------------------------------------------------------------------
# record_verdict
# ---------------------------------------------------------------------------

class TestRecordVerdict(TestCase):
    def setUp(self):
        self.pending = {}

    def test_creates_group_and_bucket(self):
        record_verdict(self.pending, "G1", _TS, "hstree", True, _NOW)
        self.assertIn("G1", self.pending)
        self.assertIn(_TS, self.pending["G1"])
        self.assertIs(self.pending["G1"][_TS]["verdicts"]["hstree"], True)

    def test_bucket_metadata_initialised(self):
        bucket = record_verdict(self.pending, "G1", _TS, "hstree", True, _NOW)
        self.assertEqual(bucket["timestamp"], _TS)
        self.assertIsNone(bucket["source_event_id"])
        self.assertEqual(bucket["first_seen"], _NOW)
        self.assertIsNone(bucket["last_emit"])
        self.assertFalse(bucket["sealed"])

    def test_source_event_id_metadata_initialised(self):
        bucket = record_verdict(
            self.pending,
            "G1",
            "event-000000000001",
            "hstree",
            True,
            _NOW,
            timestamp=_TS,
            source_event_id="event-000000000001",
        )

        self.assertEqual(bucket["timestamp"], _TS)
        self.assertEqual(bucket["source_event_id"], "event-000000000001")
        self.assertIn("event-000000000001", self.pending["G1"])

    def test_first_seen_kept_on_later_verdicts(self):
        record_verdict(self.pending, "G1", _TS, "hstree", True, _NOW)
        record_verdict(self.pending, "G1", _TS, "iforest", False, _NOW + 7)
        self.assertEqual(self.pending["G1"][_TS]["first_seen"], _NOW)

    def test_separate_timestamps_are_independent_buckets(self):
        ts2 = "2026-01-01 00:00:05"
        record_verdict(self.pending, "G1", _TS, "hstree", True, _NOW)
        record_verdict(self.pending, "G1", ts2, "hstree", False, _NOW + 5)
        self.assertIs(self.pending["G1"][_TS]["verdicts"]["hstree"], True)
        self.assertIs(self.pending["G1"][ts2]["verdicts"]["hstree"], False)

    def test_same_timestamp_with_different_source_event_ids_is_independent(self):
        record_verdict(
            self.pending,
            "G1",
            "event-1",
            "kitnet",
            True,
            _NOW,
            timestamp=_TS,
            source_event_id="event-1",
        )
        record_verdict(
            self.pending,
            "G1",
            "event-2",
            "kitnet",
            False,
            _NOW,
            timestamp=_TS,
            source_event_id="event-2",
        )

        self.assertIs(self.pending["G1"]["event-1"]["verdicts"]["kitnet"], True)
        self.assertIs(self.pending["G1"]["event-2"]["verdicts"]["kitnet"], False)

    def test_different_groups_are_independent(self):
        record_verdict(self.pending, "G1", _TS, "hstree", True, _NOW)
        record_verdict(self.pending, "G2", _TS, "hstree", False, _NOW)
        self.assertIs(self.pending["G1"][_TS]["verdicts"]["hstree"], True)
        self.assertIs(self.pending["G2"][_TS]["verdicts"]["hstree"], False)

    def test_warming_up_stored_as_none(self):
        record_verdict(self.pending, "G1", _TS, "kitnet", None, _NOW)
        self.assertIsNone(self.pending["G1"][_TS]["verdicts"]["kitnet"])

    # --- sticky / monotone merge ---

    def test_true_is_sticky_against_later_false(self):
        record_verdict(self.pending, "G1", _TS, "kitnet", True, _NOW)
        record_verdict(self.pending, "G1", _TS, "kitnet", False, _NOW + 1)
        self.assertIs(self.pending["G1"][_TS]["verdicts"]["kitnet"], True)

    def test_false_not_reverted_by_later_none(self):
        record_verdict(self.pending, "G1", _TS, "kitnet", False, _NOW)
        record_verdict(self.pending, "G1", _TS, "kitnet", None, _NOW + 1)
        self.assertIs(self.pending["G1"][_TS]["verdicts"]["kitnet"], False)

    def test_none_then_true_becomes_true(self):
        record_verdict(self.pending, "G1", _TS, "kitnet", None, _NOW)
        record_verdict(self.pending, "G1", _TS, "kitnet", True, _NOW + 1)
        self.assertIs(self.pending["G1"][_TS]["verdicts"]["kitnet"], True)

    def test_sealed_bucket_drops_verdict(self):
        record_verdict(self.pending, "G1", _TS, "hstree", True, _NOW)
        self.pending["G1"][_TS]["sealed"] = True
        self.pending["G1"][_TS]["verdicts"] = {}
        result = record_verdict(self.pending, "G1", _TS, "iforest", True, _NOW + 1)
        self.assertIsNone(result)
        self.assertEqual(self.pending["G1"][_TS]["verdicts"], {})


# ---------------------------------------------------------------------------
# bucket_is_complete / bucket_age
# ---------------------------------------------------------------------------

class TestBucketIsComplete(TestCase):
    def test_complete_when_all_active_reported(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": True, "iforest": False, "kitnet": False})
        self.assertTrue(bucket_is_complete(bucket, _MODELS))

    def test_incomplete_when_a_model_missing(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": True, "iforest": False})
        self.assertFalse(bucket_is_complete(bucket, _MODELS))

    def test_warming_up_none_still_counts_as_reported(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": None, "iforest": None, "kitnet": None})
        self.assertTrue(bucket_is_complete(bucket, _MODELS))

    def test_inactive_model_does_not_block_completion(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": True, "iforest": False})
        self.assertTrue(bucket_is_complete(bucket, ["hstree", "iforest"]))


class TestBucketAge(TestCase):
    def test_age_from_first_seen(self):
        pending = {}
        bucket = record_verdict(pending, "G1", _TS, "hstree", True, _NOW)
        self.assertEqual(bucket_age(bucket, _NOW + 12), 12)


# ---------------------------------------------------------------------------
# pop_bucket
# ---------------------------------------------------------------------------

class TestPopBucket(TestCase):
    def test_removes_bucket_and_prunes_empty_group(self):
        pending = {}
        record_verdict(pending, "G1", _TS, "hstree", True, _NOW)
        bucket = pop_bucket(pending, "G1", _TS)
        self.assertIs(bucket["verdicts"]["hstree"], True)
        self.assertNotIn("G1", pending)

    def test_keeps_group_with_other_buckets(self):
        pending = {}
        ts2 = "2026-01-01 00:00:05"
        record_verdict(pending, "G1", _TS, "hstree", True, _NOW)
        record_verdict(pending, "G1", ts2, "hstree", False, _NOW)
        pop_bucket(pending, "G1", _TS)
        self.assertIn("G1", pending)
        self.assertIn(ts2, pending["G1"])


# ---------------------------------------------------------------------------
# sweep_action — timeout / seal / remove transitions
# ---------------------------------------------------------------------------

class TestSweepAction(TestCase):
    def _bucket(self, verdicts, first_seen=_NOW, last_emit=None, sealed=False):
        return {
            "verdicts": dict(verdicts),
            "first_seen": first_seen,
            "last_emit": last_emit,
            "sealed": sealed,
        }

    def test_none_before_timeout(self):
        bucket = self._bucket({"hstree": True})
        self.assertIsNone(sweep_action(bucket, _NOW + 5, _TIMEOUT, _RETENTION, _MODELS))

    def test_timeout_when_partial_and_unpublished(self):
        bucket = self._bucket({"hstree": True})
        self.assertEqual(
            sweep_action(bucket, _NOW + 31, _TIMEOUT, _RETENTION, _MODELS), "timeout"
        )

    def test_no_timeout_when_already_complete(self):
        bucket = self._bucket({"hstree": True, "iforest": False, "kitnet": False})
        self.assertIsNone(sweep_action(bucket, _NOW + 31, _TIMEOUT, _RETENTION, _MODELS))

    def test_no_second_timeout_after_publish(self):
        bucket = self._bucket({"hstree": True}, last_emit=(1, 1))
        self.assertIsNone(sweep_action(bucket, _NOW + 31, _TIMEOUT, _RETENTION, _MODELS))

    def test_seal_at_retention(self):
        bucket = self._bucket({"hstree": True}, last_emit=(1, 1))
        self.assertEqual(
            sweep_action(bucket, _NOW + _RETENTION + 1, _TIMEOUT, _RETENTION, _MODELS), "seal"
        )

    def test_sealed_kept_until_double_retention(self):
        bucket = self._bucket({}, sealed=True)
        self.assertIsNone(
            sweep_action(bucket, _NOW + _RETENTION + 1, _TIMEOUT, _RETENTION, _MODELS)
        )

    def test_sealed_removed_after_double_retention(self):
        bucket = self._bucket({}, sealed=True)
        self.assertEqual(
            sweep_action(bucket, _NOW + 2 * _RETENTION + 1, _TIMEOUT, _RETENTION, _MODELS),
            "remove",
        )


# ---------------------------------------------------------------------------
# finalize_bucket — anomaly_count / reported_models
# ---------------------------------------------------------------------------

class TestFinalizeBucket(TestCase):
    def _finalize(self, verdicts, active=_MODELS, policy="missing", reason="complete", last_known=None):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, verdicts)
        return finalize_bucket("G1", _TS, bucket, active, last_known or {}, policy, reason)

    # --- anomaly_count ---

    def test_all_three_anomaly(self):
        c = self._finalize({"hstree": True, "iforest": True, "kitnet": True})
        self.assertEqual(c["anomaly_count"], 3)

    def test_two_anomaly_one_normal(self):
        c = self._finalize({"hstree": True, "iforest": True, "kitnet": False})
        self.assertEqual(c["anomaly_count"], 2)

    def test_one_anomaly(self):
        c = self._finalize({"hstree": False, "iforest": True, "kitnet": False})
        self.assertEqual(c["anomaly_count"], 1)

    def test_all_normal(self):
        c = self._finalize({"hstree": False, "iforest": False, "kitnet": False})
        self.assertEqual(c["anomaly_count"], 0)

    def test_warming_up_counts_zero(self):
        c = self._finalize({"hstree": None, "iforest": None, "kitnet": None})
        self.assertEqual(c["anomaly_count"], 0)

    def test_mixed_warming_up_and_anomaly(self):
        c = self._finalize({"hstree": True, "iforest": None, "kitnet": False})
        self.assertEqual(c["anomaly_count"], 1)

    # --- reported_models / complete ---

    def test_reported_models_all_three(self):
        c = self._finalize({"hstree": True, "iforest": False, "kitnet": False})
        self.assertEqual(c["reported_models"], 3)
        self.assertTrue(c["complete"])

    def test_reported_models_partial_marks_incomplete(self):
        c = self._finalize({"hstree": True})
        self.assertEqual(c["reported_models"], 1)
        self.assertEqual(c["anomaly_count"], 1)
        self.assertFalse(c["complete"])

    def test_carried_models_zero_under_missing(self):
        c = self._finalize({"hstree": True})
        self.assertEqual(c["carried_models"], 0)

    # --- metadata ---

    def test_active_models_reflects_selection(self):
        c = self._finalize({"hstree": True, "iforest": True}, active=["hstree", "iforest"])
        self.assertEqual(c["active_models"], 2)

    def test_timestamp_and_group_in_output(self):
        c = self._finalize({"hstree": True, "iforest": False, "kitnet": False})
        self.assertEqual(c["timestamp"], _TS)
        self.assertEqual(c["Group"], "G1")

    def test_source_event_id_in_output_when_available(self):
        pending = {}
        bucket = record_verdict(
            pending,
            "G1",
            "event-000000000001",
            "hstree",
            True,
            _NOW,
            timestamp=_TS,
            source_event_id="event-000000000001",
        )

        c = finalize_bucket(
            "G1",
            "event-000000000001",
            bucket,
            ["hstree"],
            {},
            "missing",
            "complete",
        )

        self.assertEqual(c["timestamp"], _TS)
        self.assertEqual(c["source_event_id"], "event-000000000001")

    def test_emit_reason_propagated(self):
        c = self._finalize({"hstree": True, "iforest": False, "kitnet": False}, reason="progress")
        self.assertEqual(c["emit_reason"], "progress")

    # --- per-model anomaly fields ---

    def test_per_model_anomaly_fields(self):
        c = self._finalize({"hstree": True, "iforest": False, "kitnet": None})
        self.assertIs(c["hstree_anomaly"],  True)
        self.assertIs(c["iforest_anomaly"], False)
        self.assertIsNone(c["kitnet_anomaly"])
        self.assertIsNone(c["deepant_anomaly"])
        self.assertIsNone(c["mdragpoint_anomaly"])

    def test_missing_model_field_is_none(self):
        c = self._finalize({"hstree": True, "iforest": False})
        self.assertIsNone(c["kitnet_anomaly"])

    # --- numeric *_vote twins (InfluxDB-persistable) ---

    def test_vote_fields_present_for_all_models(self):
        c = self._finalize({"hstree": True, "iforest": False, "kitnet": None})
        for model in MODELS:
            self.assertIn(f"{model}_vote", c)

    def test_vote_encoding_one_zero_zero(self):
        c = self._finalize({"hstree": True, "iforest": False, "kitnet": None})
        self.assertEqual(c["hstree_vote"], 1)
        self.assertEqual(c["iforest_vote"], 0)
        self.assertEqual(c["kitnet_vote"], 0)

    def test_unreported_model_vote_is_zero(self):
        c = self._finalize({"hstree": True, "iforest": False})
        self.assertEqual(c["kitnet_vote"], 0)
        self.assertEqual(c["deepant_vote"], 0)
        self.assertEqual(c["mdragpoint_vote"], 0)

    def test_votes_sum_to_anomaly_count(self):
        c = self._finalize(
            {"hstree": True, "iforest": False, "kitnet": True, "mdragpoint": True},
            active=[*_MODELS, "mdragpoint"],
        )
        votes = sum(c[f"{m}_vote"] for m in [*_MODELS, "mdragpoint"])
        self.assertEqual(votes, c["anomaly_count"])

    def test_mdragpoint_vote_visible_when_active(self):
        c = self._finalize(
            {"hstree": False, "iforest": False, "kitnet": False, "mdragpoint": True},
            active=[*_MODELS, "mdragpoint"],
        )
        self.assertEqual(c["mdragpoint_vote"], 1)

    def test_carry_forward_vote_matches_carried_verdict(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": True})
        last_known = {"G1": {"iforest": True, "kitnet": False}}
        c = finalize_bucket("G1", _TS, bucket, _MODELS, last_known, "carry_forward", "timeout")
        self.assertEqual(c["iforest_vote"], 1)
        self.assertEqual(c["kitnet_vote"], 0)
        votes = sum(c[f"{m}_vote"] for m in _MODELS)
        self.assertEqual(votes, c["anomaly_count"])

    def test_mdragpoint_participates_when_active(self):
        c = self._finalize(
            {"hstree": False, "iforest": False, "kitnet": False, "mdragpoint": True},
            active=[*_MODELS, "mdragpoint"],
        )
        self.assertEqual(c["anomaly_count"], 1)
        self.assertEqual(c["active_models"], 4)
        self.assertEqual(c["reported_models"], 4)
        self.assertIs(c["mdragpoint_anomaly"], True)

    def test_inactive_model_ignored(self):
        c = self._finalize(
            {"hstree": True, "iforest": True, "kitnet": True},
            active=["hstree", "iforest"],
        )
        self.assertEqual(c["anomaly_count"], 2)
        self.assertEqual(c["active_models"], 2)
        self.assertEqual(c["reported_models"], 2)
        # The field is still emitted for downstream consumers.
        self.assertIs(c["kitnet_anomaly"], True)


# ---------------------------------------------------------------------------
# finalize_bucket — late-model policy
# ---------------------------------------------------------------------------

class TestLatePolicy(TestCase):
    def test_policies_constant(self):
        self.assertEqual(LATE_POLICIES, ("missing", "carry_forward"))

    def test_missing_policy_leaves_late_models_null(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": True})
        last_known = {"G1": {"iforest": True, "kitnet": True}}
        c = finalize_bucket("G1", _TS, bucket, _MODELS, last_known, "missing", "timeout")
        self.assertEqual(c["anomaly_count"], 1)          # only the fresh hstree
        self.assertEqual(c["reported_models"], 1)
        self.assertEqual(c["carried_models"], 0)
        self.assertIsNone(c["iforest_anomaly"])
        self.assertIsNone(c["kitnet_anomaly"])

    def test_carry_forward_fills_late_models(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": True})
        last_known = {"G1": {"iforest": True, "kitnet": False}}
        c = finalize_bucket("G1", _TS, bucket, _MODELS, last_known, "carry_forward", "timeout")
        # hstree fresh True + iforest carried True = 2 anomalies.
        self.assertEqual(c["anomaly_count"], 2)
        self.assertEqual(c["reported_models"], 1)
        self.assertEqual(c["carried_models"], 2)
        self.assertIs(c["iforest_anomaly"], True)
        self.assertIs(c["kitnet_anomaly"], False)

    def test_carry_forward_without_history_stays_null(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": True})
        c = finalize_bucket("G1", _TS, bucket, _MODELS, {}, "carry_forward", "timeout")
        self.assertEqual(c["anomaly_count"], 1)
        self.assertEqual(c["carried_models"], 0)
        self.assertIsNone(c["iforest_anomaly"])

    def test_fresh_verdict_not_overwritten_by_carry_forward(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": False})
        last_known = {"G1": {"hstree": True}}  # stale True must not win over fresh False
        c = finalize_bucket("G1", _TS, bucket, ["hstree"], last_known, "carry_forward", "timeout")
        self.assertIs(c["hstree_anomaly"], False)
        self.assertEqual(c["anomaly_count"], 0)
        self.assertEqual(c["carried_models"], 0)


# ---------------------------------------------------------------------------
# update_last_known
# ---------------------------------------------------------------------------

class TestUpdateLastKnown(TestCase):
    def test_records_latest_verdict_per_group_model(self):
        last_known = {}
        update_last_known(last_known, "G1", "hstree", True)
        update_last_known(last_known, "G1", "hstree", False)
        self.assertIs(last_known["G1"]["hstree"], False)

    def test_groups_are_independent(self):
        last_known = {}
        update_last_known(last_known, "G1", "hstree", True)
        update_last_known(last_known, "G2", "hstree", False)
        self.assertIs(last_known["G1"]["hstree"], True)
        self.assertIs(last_known["G2"]["hstree"], False)


# ---------------------------------------------------------------------------
# Event-time scenarios
# ---------------------------------------------------------------------------

class TestEventTimeScenario(TestCase):
    def test_async_models_join_on_same_timestamp(self):
        """KitNET at T must not be mixed with an older HSTree verdict at T-5s."""
        pending = {}
        t0 = "2026-01-01 00:00:00"
        t1 = "2026-01-01 00:00:05"

        _build_bucket(pending, "G1", t0, {"hstree": False, "iforest": False, "kitnet": False})
        b0 = pending["G1"][t0]
        self.assertTrue(bucket_is_complete(b0, _MODELS))
        c0 = finalize_bucket("G1", t0, pop_bucket(pending, "G1", t0), _MODELS, {}, "missing", "complete")
        self.assertEqual(c0["anomaly_count"], 0)

        record_verdict(pending, "G1", t1, "kitnet", True, _NOW + 5)
        b1 = pending["G1"][t1]
        self.assertFalse(bucket_is_complete(b1, _MODELS))

        record_verdict(pending, "G1", t1, "hstree", False, _NOW + 7)
        record_verdict(pending, "G1", t1, "iforest", False, _NOW + 8)
        self.assertTrue(bucket_is_complete(b1, _MODELS))
        c1 = finalize_bucket("G1", t1, pop_bucket(pending, "G1", t1), _MODELS, {}, "missing", "complete")
        self.assertEqual(c1["anomaly_count"], 1)
        self.assertEqual(c1["reported_models"], 3)
        self.assertIs(c1["kitnet_anomaly"], True)
        self.assertIs(c1["hstree_anomaly"], False)

    def test_progressive_completion_after_timeout_is_monotone(self):
        """A timed-out partial must only grow as the slow model reports late.

        Reproduces the erasure bug: a late ``iforest=false`` must not lower the
        published ``anomaly_count`` for the same ``(Group, timestamp)``.
        """
        pending = {}
        # KitNET flags anomaly; HSTree/IForest are slow.
        record_verdict(pending, "G1", _TS, "kitnet", True, _NOW)
        bucket = pending["G1"][_TS]

        # Watermark fires: publish a partial (1 anomaly, 1 reported).
        first = finalize_bucket("G1", _TS, bucket, _MODELS, {}, "missing", "timeout")
        bucket["last_emit"] = (first["anomaly_count"], first["reported_models"])
        self.assertEqual((first["anomaly_count"], first["reported_models"]), (1, 1))

        # HSTree then IForest arrive late, both normal — bucket is the SAME one.
        record_verdict(pending, "G1", _TS, "hstree", False, _NOW + 35)
        record_verdict(pending, "G1", _TS, "iforest", False, _NOW + 36)
        final = finalize_bucket("G1", _TS, bucket, _MODELS, {}, "missing", "complete")

        # KitNET's anomaly is preserved (sticky); count never went back to 0.
        self.assertEqual(final["anomaly_count"], 1)
        self.assertEqual(final["reported_models"], 3)
        self.assertTrue(final["complete"])
        self.assertIs(final["kitnet_anomaly"], True)
        # The re-published signature strictly grew (1,1) -> (1,3): no erasure.
        self.assertGreater(
            (final["anomaly_count"], final["reported_models"]),
            (first["anomaly_count"], first["reported_models"]),
        )

    def test_unique_source_event_id_prevents_subsecond_collision(self):
        """Two source points sharing one second stay separate with source_event_id."""
        pending = {}
        record_verdict(
            pending,
            "G1",
            "event-1",
            "kitnet",
            True,
            _NOW,
            timestamp=_TS,
            source_event_id="event-1",
        )
        record_verdict(
            pending,
            "G1",
            "event-2",
            "kitnet",
            False,
            _NOW,
            timestamp=_TS,
            source_event_id="event-2",
        )

        c1 = finalize_bucket("G1", "event-1", pending["G1"]["event-1"], _MODELS, {}, "missing", "progress")
        c2 = finalize_bucket("G1", "event-2", pending["G1"]["event-2"], _MODELS, {}, "missing", "progress")

        self.assertEqual(c1["source_event_id"], "event-1")
        self.assertIs(c1["kitnet_anomaly"], True)
        self.assertEqual(c1["anomaly_count"], 1)
        self.assertEqual(c2["source_event_id"], "event-2")
        self.assertIs(c2["kitnet_anomaly"], False)
        self.assertEqual(c2["anomaly_count"], 0)

    def test_legacy_subsecond_collision_keeps_anomaly(self):
        """Legacy messages without source_event_id still degrade safely.

        Distinct points colliding on the same ``(Group, second)`` merge into one
        bucket; the sticky merge keeps the anomaly visible.
        """
        pending = {}
        # First point: kitnet anomaly. Second point (same second): kitnet normal.
        record_verdict(pending, "G1", _TS, "kitnet", True, _NOW)
        record_verdict(pending, "G1", _TS, "kitnet", False, _NOW)
        bucket = pending["G1"][_TS]
        c = finalize_bucket("G1", _TS, bucket, _MODELS, {}, "missing", "progress")
        self.assertIs(c["kitnet_anomaly"], True)
        self.assertEqual(c["anomaly_count"], 1)


# ---------------------------------------------------------------------------
# Model registry
# ---------------------------------------------------------------------------

class TestModelRegistry(TestCase):
    def test_deepant_is_in_models(self):
        self.assertIn("deepant", MODELS)
        self.assertEqual(TOPIC_MAP.get("deepant-analysis-viz"), "deepant")

    def test_mdragpoint_is_in_models(self):
        self.assertIn("mdragpoint", MODELS)
        self.assertNotIn("mdragsubseq", MODELS)
        self.assertEqual(TOPIC_MAP.get("mdragstream-point-analysis-viz"), "mdragpoint")
        self.assertNotIn("mdragstream-subseq-analysis-viz", TOPIC_MAP)

    def test_consensus_always_exposes_all_model_fields(self):
        pending = {}
        bucket = _build_bucket(pending, "G1", _TS, {"hstree": True, "iforest": True, "kitnet": False})
        c = finalize_bucket("G1", _TS, bucket, _MODELS, {}, "missing", "complete")
        for model in MODELS:
            self.assertIn(f"{model}_anomaly", c)
        self.assertIsNone(c["deepant_anomaly"])
        self.assertIsNone(c["mdragpoint_anomaly"])

    def test_deepant_active_and_reporting(self):
        pending = {}
        bucket = _build_bucket(
            pending, "G1", _TS,
            {"hstree": False, "iforest": False, "kitnet": False, "deepant": True},
        )
        c = finalize_bucket(
            "G1", _TS, bucket, ["hstree", "iforest", "kitnet", "deepant"], {}, "missing", "complete",
        )
        self.assertEqual(c["anomaly_count"], 1)
        self.assertEqual(c["active_models"], 4)
        self.assertEqual(c["reported_models"], 4)
        self.assertIs(c["deepant_anomaly"], True)
