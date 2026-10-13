"""Pure tests for the OC-13 regular/v2 comparator (no Docker, Kafka, files)."""

import copy
import json
import random
import unittest

from experiments.explainability.v2.compare_regular_v2 import (
    BOTH_COMPUTED_DIFFERENT,
    BOTH_COMPUTED_EQUAL,
    BOTH_NULL,
    ComparisonError,
    FEATURE_SET_MISMATCH,
    REGULAR_ONLY,
    REGULAR_ONLY_NULL,
    UNEXPECTED_BOTH_NULL,
    V2_ONLY,
    V2_ONLY_NULL,
    canonical_key,
    compare_campaign,
    index_export,
    timing_statistics,
    values_equal,
)

GROUP = "OC13-Campaign"


def make_manifest():
    keys = [f"{GROUP}|2026-09-18 12:00:{second:02d}" for second in range(4)]
    return {
        "campaign": "oc13-regular-v2-comparison",
        "comparison_key": "(Group, timestamp)",
        "group": GROUP,
        "expected_total_events": 4,
        "expected_keys": keys,
        "expected_warmup_count": 2,
        "warmup_keys": keys[:2],
        "campaign_env": {"HSTREE_INITIAL_WINDOW_SIZE": "100", "SHAP_NSAMPLES": "100"},
        "numeric_tolerances": {
            "absolute_tolerance": 1e-12,
            "relative_tolerance": 1e-12,
        },
    }


def make_record(key, shap=None, base=0.5, score=-1.25, time_ms=12.0):
    group, timestamp = key.split("|", 1)
    return {
        "timestamp": timestamp,
        "Group": group,
        "shap_values": shap,
        "shap_base_value": base,
        "local_hstree_score": score,
        "shap_computation_time_ms": time_ms,
    }


def make_campaign():
    manifest = make_manifest()
    computed_shap = {"feat_a": 1.0, "feat_b": -2.0}
    regular = [
        make_record(manifest["expected_keys"][0]),
        make_record(manifest["expected_keys"][1]),
        make_record(manifest["expected_keys"][2], shap=computed_shap, base=0.5, score=-1.25),
        make_record(manifest["expected_keys"][3], shap={"feat_a": 0.25, "feat_b": -0.5}, base=0.1, score=-0.75),
    ]
    v2 = copy.deepcopy(regular)
    return manifest, regular, v2


class HelperTest(unittest.TestCase):
    def test_canonical_key_requires_group_and_timestamp(self) -> None:
        self.assertEqual(canonical_key({"Group": "g", "timestamp": "t"}), "g|t")
        self.assertIsNone(canonical_key({"Group": "g"}))
        self.assertIsNone(canonical_key({"timestamp": "t"}))
        self.assertIsNone(canonical_key({"Group": "", "timestamp": "t"}))

    def test_values_equal_none_semantics(self) -> None:
        self.assertTrue(values_equal(None, None, 1e-12, 1e-12))
        self.assertFalse(values_equal(None, 1.0, 1e-12, 1e-12))
        self.assertFalse(values_equal(1.0, None, 1e-12, 1e-12))

    def test_timing_statistics_are_order_independent_and_reported(self) -> None:
        values = [20.0, 10.0, 30.0, 40.0, None]
        first = timing_statistics(values)
        shuffled = values[:]
        random.Random(7).shuffle(shuffled)
        second = timing_statistics(shuffled)
        self.assertEqual(first, second)
        self.assertEqual(first["count"], 4)
        self.assertEqual(first["min"], 10.0)
        self.assertEqual(first["max"], 40.0)
        self.assertEqual(first["mean"], 25.0)
        self.assertEqual(first["p50"], 25.0)

    def test_index_export_rejects_invalid_shap_structure(self) -> None:
        with self.assertRaises(ComparisonError):
            index_export([make_record("g|t", shap="not-a-dict")], "regular")
        with self.assertRaises(ComparisonError):
            index_export([make_record("g|t", shap={"feat_a": "x"})], "regular")
        # A missing shap_values key is equivalent to a null SHAP (valid).
        index_export([{"Group": "g", "timestamp": "t"}], "regular")
        with self.assertRaises(ComparisonError):
            index_export(
                [make_record("g|t"), make_record("g|t")], "regular"
            )


class IdenticalCampaignTest(unittest.TestCase):
    def test_identical_campaign_passes(self) -> None:
        manifest, regular, v2 = make_campaign()
        report = compare_campaign(
            manifest,
            regular,
            v2,
            {"final_lags": {"regular": 0, "v2": 0}},
        )
        self.assertTrue(report["passed"], json.dumps(report["checks"]))
        self.assertEqual(report["categories"][BOTH_NULL], manifest["warmup_keys"])
        self.assertEqual(
            report["categories"][BOTH_COMPUTED_EQUAL],
            manifest["expected_keys"][2:],
        )
        self.assertEqual(report["divergences"], [])
        self.assertEqual(report["final_lags"], {"regular": 0, "v2": 0})

    def test_computation_time_difference_is_not_a_divergence(self) -> None:
        manifest, regular, v2 = make_campaign()
        for record in v2:
            if record["shap_computation_time_ms"] is not None:
                record["shap_computation_time_ms"] *= 10
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertTrue(report["passed"])
        self.assertEqual(report["divergences"], [])
        self.assertEqual(report["timing"]["ratio_mean_v2_over_regular"], 10.0)
        self.assertIn("NOT a scientific divergence", report["timing"]["note"])


class DivergenceTest(unittest.TestCase):
    def test_feature_divergence_is_reported_with_details(self) -> None:
        manifest, regular, v2 = make_campaign()
        v2[2]["shap_values"]["feat_a"] = 1.0 + 1e-6
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        self.assertEqual(report["categories"][BOTH_COMPUTED_DIFFERENT], [manifest["expected_keys"][2]])
        divergence = report["divergences"][0]
        self.assertEqual(divergence["feature"], "feat_a")
        self.assertEqual(divergence["kind"], "shap_value")
        self.assertEqual(divergence["v1"], 1.0)
        self.assertEqual(divergence["v2"], 1.0 + 1e-6)
        self.assertAlmostEqual(divergence["abs_diff"], 1e-6)

    def test_shap_base_value_divergence(self) -> None:
        manifest, regular, v2 = make_campaign()
        v2[2]["shap_base_value"] = 0.5 + 1e-9
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        kinds = {divergence["kind"] for divergence in report["divergences"]}
        self.assertIn("shap_base_value", kinds)

    def test_local_hstree_score_divergence(self) -> None:
        manifest, regular, v2 = make_campaign()
        v2[3]["local_hstree_score"] = -0.75 + 1e-9
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        kinds = {divergence["kind"] for divergence in report["divergences"]}
        self.assertIn("local_hstree_score", kinds)

    def test_tolerance_just_below_and_just_above(self) -> None:
        manifest, regular, v2 = make_campaign()
        atol = manifest["numeric_tolerances"]["absolute_tolerance"]
        # Exactly at atol: within tolerance.
        v2[2]["shap_values"]["feat_b"] = -2.0 + atol
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertTrue(report["passed"])
        # Just above atol (and above rtol): divergence.
        v2[2]["shap_values"]["feat_b"] = -2.0 + atol * 10
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])

    def test_feature_set_mismatch(self) -> None:
        manifest, regular, v2 = make_campaign()
        del v2[2]["shap_values"]["feat_b"]
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        self.assertEqual(
            report["categories"][FEATURE_SET_MISMATCH], [manifest["expected_keys"][2]]
        )


class CoverageTest(unittest.TestCase):
    def test_missing_event_is_reported_and_fails(self) -> None:
        manifest, regular, v2 = make_campaign()
        removed = v2.pop(3)
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        self.assertEqual(report["categories"][V2_ONLY], [removed["Group"] + "|" + removed["timestamp"]])
        self.assertTrue(report["checks"]["exports_cover_exactly_expected_keys"] is False)

    def test_regular_only_and_v2_only_are_symmetric(self) -> None:
        manifest, regular, v2 = make_campaign()
        del regular[3]
        del v2[2]
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        self.assertEqual(report["categories"][REGULAR_ONLY], [manifest["expected_keys"][3]])
        self.assertEqual(report["categories"][V2_ONLY], [manifest["expected_keys"][2]])

    def test_unexpected_key_outside_manifest(self) -> None:
        manifest, regular, v2 = make_campaign()
        v2.append(make_record(f"{GROUP}|2026-09-18 12:00:99", shap={"feat_a": 0.0}))
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        self.assertEqual(
            report["coverage"]["unexpected_keys"], [f"{GROUP}|2026-09-18 12:00:99"]
        )

    def test_non_zero_final_lag_fails(self) -> None:
        manifest, regular, v2 = make_campaign()
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 3}}
        )
        self.assertFalse(report["passed"])
        self.assertFalse(report["checks"]["final_lags_are_zero"])

    def test_invalid_runner_metrics_are_rejected(self) -> None:
        manifest, regular, v2 = make_campaign()
        with self.assertRaises(ComparisonError):
            compare_campaign(manifest, regular, v2, {})
        with self.assertRaises(ComparisonError):
            compare_campaign(manifest, regular, v2, {"final_lags": {"regular": -1, "v2": 0}})


class NullShapPolicyTest(unittest.TestCase):
    def test_both_null_during_warmup_is_admitted(self) -> None:
        manifest, regular, v2 = make_campaign()
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertTrue(report["passed"])
        self.assertEqual(report["categories"][BOTH_NULL], manifest["warmup_keys"])

    def test_unexpected_both_null_after_warmup_fails(self) -> None:
        manifest, regular, v2 = make_campaign()
        v2[2]["shap_values"] = None
        v2[2]["shap_base_value"] = None
        regular[2]["shap_values"] = None
        regular[2]["shap_base_value"] = None
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        self.assertEqual(
            report["categories"][UNEXPECTED_BOTH_NULL], [manifest["expected_keys"][2]]
        )

    def test_one_sided_null_fails(self) -> None:
        manifest, regular, v2 = make_campaign()
        regular[2]["shap_values"] = None
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        self.assertEqual(
            report["categories"][REGULAR_ONLY_NULL], [manifest["expected_keys"][2]]
        )

        manifest, regular, v2 = make_campaign()
        v2[2]["shap_values"] = None
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        self.assertFalse(report["passed"])
        self.assertEqual(
            report["categories"][V2_ONLY_NULL], [manifest["expected_keys"][2]]
        )


class InvalidManifestTest(unittest.TestCase):
    def test_wrong_campaign_name_is_rejected(self) -> None:
        manifest, regular, v2 = make_campaign()
        manifest["campaign"] = "oc12-explainability-v2"
        with self.assertRaises(ComparisonError):
            compare_campaign(manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}})

    def test_warmup_keys_outside_expected_are_rejected(self) -> None:
        manifest, regular, v2 = make_campaign()
        manifest["warmup_keys"] = ["OC13-Campaign|2025-01-01 00:00:00"]
        with self.assertRaises(ComparisonError):
            compare_campaign(manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}})

    def test_missing_tolerances_are_rejected(self) -> None:
        manifest, regular, v2 = make_campaign()
        del manifest["numeric_tolerances"]
        with self.assertRaises(ComparisonError):
            compare_campaign(manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}})


class DeterminismTest(unittest.TestCase):
    def test_report_is_identical_regardless_of_row_order(self) -> None:
        manifest, regular, v2 = make_campaign()
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )

        shuffled_regular = regular[:]
        shuffled_v2 = v2[:]
        random.Random(11).shuffle(shuffled_regular)
        random.Random(13).shuffle(shuffled_v2)
        shuffled = compare_campaign(
            manifest,
            shuffled_regular,
            shuffled_v2,
            {"final_lags": {"regular": 0, "v2": 0}},
        )
        self.assertEqual(
            json.dumps(report, sort_keys=True), json.dumps(shuffled, sort_keys=True)
        )

    def test_divergence_list_is_sorted(self) -> None:
        manifest, regular, v2 = make_campaign()
        # Diverge on two features of two different events.
        v2[2]["shap_values"]["feat_b"] = -2.0 + 1e-6
        v2[3]["shap_values"]["feat_a"] = 0.25 + 1e-6
        report = compare_campaign(
            manifest, regular, v2, {"final_lags": {"regular": 0, "v2": 0}}
        )
        keys = [(item["key"], item["feature"]) for item in report["divergences"]]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(len(keys), 2)


if __name__ == "__main__":
    unittest.main()
