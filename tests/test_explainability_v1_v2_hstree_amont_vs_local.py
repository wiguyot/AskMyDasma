import math
import unittest

from experiments.explainability.v1_v2.hstree_amont_vs_local import (
    HSTreeComparisonError,
    compare_pair,
    compare_three_exports,
)


def _upstream(**overrides):
    record = {
        "timestamp": "2026-01-01 00:00:00",
        "Group": "G1",
        "source_event_id": "event-1",
        "hstree_score": -1.0,
        "hstree_status": "normal",
        "hstree_anomaly": False,
        "hstree_threshold": -0.5,
    }
    record.update(overrides)
    return record


def _local(**overrides):
    record = {
        "timestamp": "2026-01-01 00:00:00",
        "Group": "G1",
        "source_event_id": "event-1",
        "hstree_score": -1.0,
        "local_hstree_score": -1.0,
        "hstree_status": "normal",
        "hstree_anomaly": False,
        "hstree_threshold": -0.5,
    }
    record.update(overrides)
    return record


class HSTreeAmontVsLocalTest(unittest.TestCase):
    def test_iforest_fields_can_be_compared_with_same_comparator(self):
        upstream = {
            "timestamp": "2026-01-01 00:00:00",
            "Group": "G1",
            "source_event_id": "event-1",
            "iforest_score": 0.42,
            "iforest_status": "normal",
            "iforest_anomaly": False,
            "iforest_threshold": 0.9,
        }
        local = {
            **upstream,
            "local_score": 0.42,
        }
        report = compare_pair(
            [upstream],
            [local],
            upstream_score_field="iforest_score",
            local_score_field="local_score",
            upstream_status_field="iforest_status",
            local_status_field="iforest_status",
            upstream_anomaly_field="iforest_anomaly",
            local_anomaly_field="iforest_anomaly",
            upstream_threshold_field="iforest_threshold",
            local_threshold_field="iforest_threshold",
        )
        self.assertEqual(report["verdict"], "ok")
        self.assertEqual(report["categories"]["equal"], 1)

    def test_exact_equal_active_scores(self):
        report = compare_pair([_upstream()], [_local()])
        self.assertEqual(report["verdict"], "ok")
        self.assertEqual(report["categories"]["equal"], 1)
        self.assertEqual(report["numeric_outcomes"]["equal"], 2)

    def test_warmup_does_not_compare_upstream_fallback_score_to_null_local_score(self):
        report = compare_pair(
            [_upstream(hstree_score=0.0, hstree_status="warming_up", hstree_anomaly=None, hstree_threshold=None)],
            [_local(local_hstree_score=None, hstree_status="warming_up", hstree_anomaly=None, hstree_threshold=None)],
        )
        self.assertEqual(report["verdict"], "ok")
        self.assertEqual(report["categories"]["equal"], 1)
        self.assertEqual(report["numeric_outcomes"]["invalid_numeric"], 0)

    def test_threshold_warmup_still_compares_real_hstree_scores(self):
        upstream = _upstream(
            hstree_status="warming_up", hstree_anomaly=None,
            hstree_threshold=None, hstree_score=-8.0,
        )
        local = _local(
            hstree_status="warming_up", hstree_anomaly=None,
            hstree_threshold=None, local_hstree_score=-9.0,
        )
        report = compare_pair([upstream], [local])
        self.assertEqual(report["categories"]["divergent"], 1)
        self.assertEqual(report["numeric_outcomes"]["divergent"], 1)
        self.assertEqual(
            [detail["path"] for detail in report["identities"][0]["details"]],
            ["local_hstree_score"],
        )

        local["local_hstree_score"] = -8.0
        equal_report = compare_pair([upstream], [local])
        self.assertEqual(equal_report["categories"]["equal"], 1)
        self.assertEqual(equal_report["numeric_outcomes"]["equal"], 1)

        upstream["hstree_score"] = 0.0
        local["local_hstree_score"] = 0.0
        zero_report = compare_pair([upstream], [local])
        self.assertEqual(zero_report["numeric_outcomes"]["equal"], 1)

    def test_threshold_warmup_nonzero_score_with_missing_local_is_invalid(self):
        report = compare_pair(
            [_upstream(hstree_status="warming_up", hstree_anomaly=None,
                       hstree_threshold=None, hstree_score=-8.0)],
            [_local(hstree_status="warming_up", hstree_anomaly=None,
                    hstree_threshold=None, local_hstree_score=None)],
        )
        self.assertEqual(report["categories"]["invalid_numeric"], 1)

    def test_v1_v2_scores_during_threshold_warmup_are_compared(self):
        v1 = _local(hstree_status="warming_up", hstree_anomaly=None,
                    hstree_threshold=None, local_hstree_score=-8.0)
        v2 = _local(hstree_status="warming_up", hstree_anomaly=None,
                    hstree_threshold=None, local_hstree_score=-9.0)
        report = compare_pair(
            [v1], [v2], upstream_name="v1", local_name="v2",
            upstream_score_field="local_hstree_score",
        )
        self.assertEqual(report["categories"]["divergent"], 1)
        self.assertEqual(report["numeric_outcomes"]["divergent"], 1)

    def test_tolerance_is_distinguished_from_exact_equality(self):
        report = compare_pair([_upstream(hstree_score=-1.0)], [_local(local_hstree_score=-1.0 + 1e-9)], atol=1e-8)
        self.assertEqual(report["verdict"], "ok")
        self.assertEqual(report["categories"]["tolerated_equal"], 1)

    def test_score_divergence_is_reported(self):
        report = compare_pair([_upstream(hstree_score=-1.0)], [_local(local_hstree_score=-2.0)])
        self.assertEqual(report["verdict"], "ko")
        self.assertEqual(report["categories"]["divergent"], 1)
        self.assertEqual(report["identities"][0]["details"][0]["path"], "local_hstree_score")
        self.assertEqual(report["first_divergence"]["key"], ["G1", "event-1"])
        self.assertEqual(report["by_group"]["G1"]["divergent"], 1)

    def test_status_verdict_and_threshold_divergences_are_reported(self):
        report = compare_pair(
            [_upstream(hstree_status="anomaly", hstree_anomaly=True, hstree_threshold=-0.5)],
            [_local(hstree_status="normal", hstree_anomaly=False, hstree_threshold=-0.7)],
        )
        self.assertEqual(report["verdict"], "ko")
        paths = {detail["path"] for detail in report["identities"][0]["details"]}
        self.assertEqual(
            paths,
            {"hstree_status", "hstree_anomaly", "local_hstree_score", "hstree_threshold"},
        )

    def test_missing_identity_is_reported(self):
        report = compare_pair([_upstream()], [_local(source_event_id="event-2")])
        self.assertEqual(report["categories"]["missing"], 2)
        self.assertEqual(report["only_upstream"], [["G1", "event-1"]])
        self.assertEqual(report["only_local"], [["G1", "event-2"]])

    def test_duplicate_identity_is_reported_and_not_compared(self):
        report = compare_pair([_upstream(), _upstream(hstree_score=-2.0)], [_local()])
        self.assertEqual(report["verdict"], "ko")
        self.assertEqual(report["categories"]["duplicate"], 1)
        self.assertEqual(report["numeric_outcomes"]["equal"], 0)

    def test_invalid_numeric_is_never_equal(self):
        for value in ("bad", True, math.nan, math.inf):
            with self.subTest(value=value):
                report = compare_pair([_upstream(hstree_score=value)], [_local()])
                self.assertEqual(report["verdict"], "ko")
                self.assertEqual(report["categories"]["invalid_numeric"], 1)

    def test_group_and_event_id_are_not_trimmed(self):
        report = compare_pair(
            [_upstream(Group=" G1 ", source_event_id=" event-1 ")],
            [_local(Group="G1", source_event_id="event-1")],
        )
        self.assertEqual(report["categories"]["missing"], 2)

    def test_three_way_report_distinguishes_local_parity_from_upstream_drift(self):
        upstream = [_upstream(hstree_score=-1.0)]
        v1 = [_local(local_hstree_score=-2.0)]
        v2 = [_local(local_hstree_score=-2.0)]
        report = compare_three_exports(upstream, v1, v2)
        self.assertEqual(report["comparisons"]["upstream_vs_v1"]["verdict"], "ko")
        self.assertEqual(report["comparisons"]["upstream_vs_v2"]["verdict"], "ko")
        self.assertEqual(report["comparisons"]["v1_vs_v2"]["verdict"], "ok")

    def test_tolerances_must_be_finite_and_non_negative(self):
        for value in (-1, math.nan, math.inf, True):
            with self.subTest(value=value):
                with self.assertRaises(HSTreeComparisonError):
                    compare_pair([_upstream()], [_local()], atol=value)

    def test_identity_order_does_not_change_report(self):
        upstream = [_upstream(source_event_id="event-2"), _upstream(source_event_id="event-1")]
        local = [_local(source_event_id="event-1"), _local(source_event_id="event-2")]
        first = compare_pair(upstream, local)
        second = compare_pair(list(reversed(upstream)), list(reversed(local)))
        self.assertEqual(first, second)
