"""Pure tests for the OC-15 minimal validator (no Docker, Kafka, InfluxDB)."""

import copy
import json
import random
import unittest

from experiments.explainability.v2.validate_minimal_explainability import (
    CAMPAIGN_NAME,
    ValidationError,
    compare_minimal_validation,
    event_key,
    shap_sum,
    validate_single_export,
)


def make_manifest():
    keys = [f"OC15-Alpha|oc15-alpha-{i:03d}" for i in range(1, 7)]
    keys += [f"OC15-Beta|oc15-beta-{i:03d}" for i in range(1, 7)]
    return {
        "campaign": CAMPAIGN_NAME,
        "reference_seed": 42,
        "secondary_seeds": [43, 44],
        "numeric_tolerances": {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12},
        "additivity_tolerance": {"absolute_tolerance": 1e-6, "relative_tolerance": 1e-9},
        "expected_keys": keys,
        "warmup_keys": keys[0:2] + keys[6:8],
        "shap_eligible_keys": keys[2:6] + keys[8:12],
        "expected_verdicts": {
            "anomaly": [keys[3], keys[5], keys[9], keys[11]],
            "normal": [keys[4], keys[10]],
            "threshold_warmup": [keys[2], keys[8]],
        },
    }


def make_record(key, status="computed", shap=None, base=0.5, score=None, anomaly=None, time_ms=12.0, timestamp="t"):
    group, event_id = key.split("|", 1)
    if shap is None and status == "computed":
        shap = {"feat_a": 1.0, "feat_b": -2.0}
    if score is None:
        score = base + sum(shap.values()) if shap is not None else -0.5
    return {
        "timestamp": timestamp,
        "Group": group,
        "source_event_id": event_id,
        "shap_values": shap,
        "shap_base_value": base,
        "local_hstree_score": score,
        "shap_computation_time_ms": time_ms,
        "explanation_status": status,
        "hstree_anomaly": anomaly,
    }


def make_campaign():
    manifest = make_manifest()
    runs = []
    for _ in range(2):
        records = []
        for key in manifest["expected_keys"]:
            if key in manifest["warmup_keys"]:
                records.append(make_record(key, status="warmup", anomaly=None))
            elif key in manifest["expected_verdicts"]["threshold_warmup"]:
                records.append(make_record(key, anomaly=None))
            elif key in manifest["expected_verdicts"]["anomaly"]:
                records.append(make_record(key, anomaly=True))
            else:
                records.append(make_record(key, anomaly=False))
        runs.append(copy.deepcopy(records))
    return manifest, runs[0], runs[1]


class IdenticalCampaignTest(unittest.TestCase):
    def test_identical_reference_runs_pass(self) -> None:
        manifest, run_a, run_b = make_campaign()
        report = compare_minimal_validation(manifest, run_a, run_b)
        self.assertEqual(report["verdict"], "minimal_validation_passed")
        self.assertEqual(report["error_count"], 0)
        self.assertTrue(report["passed"])

    def test_report_counts_statuses_and_verdicts(self) -> None:
        manifest, run_a, run_b = make_campaign()
        report = compare_minimal_validation(manifest, run_a, run_b)
        self.assertEqual(report["status_counts"]["run_a"]["warmup"], 4)
        self.assertEqual(report["status_counts"]["run_a"]["computed"], 8)
        # Every null hstree_anomaly is counted as threshold_warmup: the 4
        # warmup events plus the 2 computed threshold events.
        self.assertEqual(report["verdict_counts"]["run_a"]["threshold_warmup"], 6)
        self.assertEqual(report["verdict_counts"]["run_a"]["anomaly"], 4)
        self.assertEqual(report["verdict_counts"]["run_a"]["normal"], 2)


class DivergenceTest(unittest.TestCase):
    def test_different_shap_value_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_b[3]["shap_values"]["feat_a"] = 1.0 + 1e-6
        report = compare_minimal_validation(manifest, run_a, run_b)
        self.assertEqual(report["verdict"], "minimal_validation_failed")
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("shap_value_reproducibility_mismatch", codes)

    def test_missing_feature_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        del run_b[3]["shap_values"]["feat_b"]
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("feature_set_mismatch", codes)

    def test_base_value_divergence_and_additivity_contract(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_b[3]["shap_base_value"] = 0.5 + 1e-9
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("shap_base_value_reproducibility_mismatch", codes)
        self.assertIn("shap_sum_reproducibility_mismatch", codes)

    def test_shap_sum_contract(self) -> None:
        record = make_record("g|e", shap={"feat_a": 1.0, "feat_b": -2.0}, base=0.5)
        self.assertAlmostEqual(shap_sum(record), -0.5)
        self.assertIsNone(shap_sum({"shap_base_value": None, "shap_values": None}))


class CoverageTest(unittest.TestCase):
    def test_missing_key_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        removed = run_b.pop(3)
        report = compare_minimal_validation(manifest, run_a, run_b)
        self.assertEqual(report["verdict"], "minimal_validation_failed")
        self.assertIn(
            removed["Group"] + "|" + removed["source_event_id"],
            report["structural_validation"]["run_b"]["coverage"]["missing"],
        )

    def test_duplicate_key_is_reported(self) -> None:
        manifest, run_a, run_b = make_campaign()
        report = compare_minimal_validation(manifest, run_a, run_b + [run_b[0]])
        self.assertEqual(report["verdict"], "minimal_validation_failed")
        self.assertIn(
            manifest["expected_keys"][0], report["structural_validation"]["run_b"]["duplicates"]
        )

    def test_unexpected_key_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_b.append(make_record("OC15-Alpha|oc15-alpha-999", status="warmup", anomaly=False))
        report = compare_minimal_validation(manifest, run_a, run_b)
        self.assertIn(
            "OC15-Alpha|oc15-alpha-999",
            report["structural_validation"]["run_b"]["coverage"]["unexpected"],
        )
        self.assertEqual(report["verdict"], "minimal_validation_failed")


class StatusAndStructureTest(unittest.TestCase):
    def test_invalid_status_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_a[0]["explanation_status"] = "surprise"
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][0]]]
        self.assertIn("invalid_status_run_a", codes)

    def test_model_mismatch_is_declared_but_fails_fidelity_validation(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_a[3]["explanation_status"] = "model_mismatch"
        run_a[3]["shap_values"] = None
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("model_mismatch_not_acceptable_run_a", codes)
        self.assertEqual(report["verdict"], "minimal_validation_failed")

    def test_status_reproducibility_mismatch_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_b[3]["explanation_status"] = "warmup"
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("status_reproducibility_mismatch", codes)

    def test_nan_and_infinity_fail(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_a[3]["shap_values"]["feat_a"] = float("nan")
        run_a[4]["shap_values"]["feat_b"] = float("inf")
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes_3 = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        codes_4 = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][4]]]
        self.assertIn("invalid_shap_values_run_a", codes_3)
        self.assertIn("invalid_shap_values_run_a", codes_4)

    def test_missing_computed_fields_fail(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_a[3]["shap_base_value"] = None
        run_a[4]["local_hstree_score"] = None
        run_a[5]["shap_computation_time_ms"] = None
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes_3 = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        codes_4 = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][4]]]
        codes_5 = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][5]]]
        self.assertIn("invalid_shap_base_value_run_a", codes_3)
        self.assertIn("invalid_local_hstree_score_run_a", codes_4)
        self.assertIn("invalid_shap_computation_time_run_a", codes_5)

    def test_non_computed_with_shap_values_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_a[0]["shap_values"] = {"feat_a": 0.0}
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][0]]]
        self.assertIn("shap_values_should_be_absent_run_a", codes)


class VerdictCoherenceTest(unittest.TestCase):
    def test_verdict_mismatch_between_runs_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_b[10]["hstree_anomaly"] = not run_a[10]["hstree_anomaly"]
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][10]]]
        self.assertIn("hstree_verdict_mismatch", codes)

    def test_timestamp_mismatch_between_runs_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_b[3]["timestamp"] = "other"
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("timestamp_mismatch", codes)

    def test_expected_anomaly_verdict_missing_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_a[5]["hstree_anomaly"] = False
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][5]]]
        self.assertIn("expected_anomaly_verdict_missing_run_a", codes)
        self.assertIn("hstree_verdict_mismatch", codes)


class DeterminismTest(unittest.TestCase):
    def test_report_is_identical_regardless_of_row_order(self) -> None:
        manifest, run_a, run_b = make_campaign()
        report = compare_minimal_validation(manifest, run_a, run_b)
        shuffled_a = run_a[:]
        shuffled_b = run_b[:]
        random.Random(3).shuffle(shuffled_a)
        random.Random(7).shuffle(shuffled_b)
        shuffled = compare_minimal_validation(manifest, shuffled_a, shuffled_b)
        self.assertEqual(
            json.dumps(report, sort_keys=True), json.dumps(shuffled, sort_keys=True)
        )


class InvalidInputsTest(unittest.TestCase):
    def test_wrong_campaign_name_rejected(self) -> None:
        manifest, run_a, run_b = make_campaign()
        manifest["campaign"] = "oc12-explainability-v2"
        with self.assertRaises(ValidationError):
            compare_minimal_validation(manifest, run_a, run_b)

    def test_unusable_records_are_reported_as_invalid(self) -> None:
        manifest, run_a, run_b = make_campaign()
        structure = validate_single_export(manifest, [{"Group": "g"}], "run_a")
        self.assertFalse(structure["valid"])
        self.assertTrue(structure["invalid_identity_records"])
        structure = validate_single_export(manifest, [42], "run_a")
        self.assertFalse(structure["valid"])

    def test_event_key_helper(self) -> None:
        self.assertEqual(event_key({"Group": "g", "source_event_id": "e"}), "g|e")
        self.assertIsNone(event_key({"Group": "g"}))
        structure = validate_single_export(make_manifest(), [make_record("g|e")], "run")
        self.assertIn("g|e", structure["errors_by_key"])  # unexpected key


if __name__ == "__main__":
    unittest.main()


class AdditivityVsScoreTest(unittest.TestCase):
    """OC-15R1: base + sum(shap) ~= local_hstree_score, per run."""

    def test_sum_differs_from_score_in_run_a(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_a[3]["local_hstree_score"] = run_a[3]["local_hstree_score"] + 0.001
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("shap_additivity_vs_score_mismatch_run_a", codes)
        self.assertEqual(report["verdict"], "minimal_validation_failed")

    def test_sum_differs_from_score_in_run_b(self) -> None:
        manifest, run_a, run_b = make_campaign()
        run_b[4]["local_hstree_score"] = run_b[4]["local_hstree_score"] + 0.001
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][4]]]
        self.assertIn("shap_additivity_vs_score_mismatch_run_b", codes)
        self.assertEqual(report["verdict"], "minimal_validation_failed")

    def test_equal_sums_between_runs_but_both_wrong_versus_score_fails(self) -> None:
        manifest, run_a, run_b = make_campaign()
        # Both runs share the same (wrong) score: sums are equal between runs
        # but the additivity against the score fails on BOTH sides.
        for run in (run_a, run_b):
            run[5]["local_hstree_score"] = run[5]["local_hstree_score"] + 1.0
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][5]]]
        self.assertIn("shap_additivity_vs_score_mismatch_run_a", codes)
        self.assertIn("shap_additivity_vs_score_mismatch_run_b", codes)
        # The sums are still reproducible between the runs.
        self.assertNotIn("shap_sum_reproducibility_mismatch", codes)
        self.assertEqual(report["verdict"], "minimal_validation_failed")

    def test_additivity_within_the_dedicated_tolerance_passes(self) -> None:
        manifest, run_a, run_b = make_campaign()
        # A tiny drift within the dedicated additivity tolerance (1e-6 abs)
        # but above the reproducibility tolerance (1e-12) stays acceptable.
        run_a[3]["local_hstree_score"] = run_a[3]["local_hstree_score"] + 1e-9
        report = compare_minimal_validation(manifest, run_a, run_b)
        # 1e-9 drift on the score of run A: additivity OK (dedicated
        # tolerance), but score reproducibility between runs fails (1e-12).
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertNotIn("shap_additivity_vs_score_mismatch_run_a", codes)
        self.assertIn("local_hstree_score_reproducibility_mismatch", codes)

    def test_manifest_without_dedicated_tolerance_falls_back(self) -> None:
        manifest, run_a, run_b = make_campaign()
        del manifest["additivity_tolerance"]
        # Drift of 1e-9: outside the fallback reproducibility tolerance.
        run_a[3]["local_hstree_score"] = run_a[3]["local_hstree_score"] + 1e-9
        report = compare_minimal_validation(manifest, run_a, run_b)
        codes = [error["code"] for error in report["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("shap_additivity_vs_score_mismatch_run_a", codes)


class SingleExportValidationTest(unittest.TestCase):
    """OC-15R2: structural validation of ONE export, reusable per seed."""

    def test_valid_export_passes(self) -> None:
        manifest, run_a, _ = make_campaign()
        structure = validate_single_export(manifest, run_a, "seed43")
        self.assertTrue(structure["valid"])
        self.assertEqual(structure["error_count"], 0)
        self.assertEqual(structure["coverage"]["observed_events"], len(manifest["expected_keys"]))
        self.assertEqual(structure["duplicates"], [])

    def test_secondary_export_is_never_compared_to_the_reference(self) -> None:
        # The single-export validator signature has no reference input at all:
        # a secondary export cannot be silently compared to the reference.
        import inspect

        parameters = inspect.signature(validate_single_export).parameters
        self.assertEqual(list(parameters), ["manifest", "records", "export_name"])

    def test_timestamp_is_checked_against_the_manifest(self) -> None:
        manifest, run_a, _ = make_campaign()
        manifest["expected_timestamps"] = {
            key: "2026-09-19 09:00:00" for key in manifest["expected_keys"]
        }
        structure = validate_single_export(manifest, run_a, "seed43")
        self.assertFalse(structure["valid"])
        codes = [error["code"] for error in structure["errors_by_key"][manifest["expected_keys"][0]]]
        self.assertIn("timestamp_mismatch_vs_manifest", codes)

    def test_additivity_is_checked_in_a_single_export(self) -> None:
        manifest, run_a, _ = make_campaign()
        run_a[3]["local_hstree_score"] = run_a[3]["local_hstree_score"] + 1.0
        structure = validate_single_export(manifest, run_a, "seed43")
        self.assertFalse(structure["valid"])
        codes = [error["code"] for error in structure["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("shap_additivity_vs_score_mismatch", codes)

    def test_verdict_cases_are_checked_against_the_manifest(self) -> None:
        manifest, run_a, _ = make_campaign()
        run_a[5]["hstree_anomaly"] = False  # expected anomaly, observed normal
        structure = validate_single_export(manifest, run_a, "seed43")
        codes = [error["code"] for error in structure["errors_by_key"][manifest["expected_keys"][5]]]
        self.assertIn("expected_anomaly_verdict_missing", codes)
        self.assertIn("unexpected_normal_verdict", codes)


class JsonVizShapIdentityTest(unittest.TestCase):
    """The Telegraf-oriented JSON copy carries identity inside shap_values."""

    @staticmethod
    def _inject_json_viz_identity(record):
        record["explained_model"] = "kitnet"
        record["shap_values"] = {
            **record["shap_values"],
            "Group": record["Group"],
            "source_event_id": record["source_event_id"],
            "explained_model": record["explained_model"],
        }

    def test_matching_json_viz_identity_is_not_a_shap_contribution(self) -> None:
        manifest, run_a, run_b = make_campaign()
        for run in (run_a, run_b):
            self._inject_json_viz_identity(run[3])
        report = compare_minimal_validation(manifest, run_a, run_b)
        self.assertTrue(report["passed"])
        self.assertEqual(report["error_count"], 0)

    def test_mismatched_json_viz_identity_fails(self) -> None:
        manifest, run_a, _ = make_campaign()
        self._inject_json_viz_identity(run_a[3])
        run_a[3]["shap_values"]["Group"] = "other-group"
        structure = validate_single_export(manifest, run_a, "seed43")
        codes = [error["code"] for error in structure["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("shap_values_identity_metadata_mismatch", codes)

    def test_partial_json_viz_identity_fails(self) -> None:
        manifest, run_a, _ = make_campaign()
        run_a[3]["shap_values"]["Group"] = run_a[3]["Group"]
        structure = validate_single_export(manifest, run_a, "seed43")
        codes = [error["code"] for error in structure["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("shap_values_identity_metadata_incomplete", codes)

    def test_unrelated_non_numeric_shap_value_is_still_rejected(self) -> None:
        manifest, run_a, _ = make_campaign()
        run_a[3]["shap_values"]["not_transport_metadata"] = "not-a-number"
        structure = validate_single_export(manifest, run_a, "seed43")
        codes = [error["code"] for error in structure["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("invalid_shap_values", codes)

    def test_model_tag_alone_is_optional_transport_metadata(self) -> None:
        manifest, run_a, _ = make_campaign()
        run_a[3]["explained_model"] = "kitnet"
        run_a[3]["shap_values"]["explained_model"] = "kitnet"

        structure = validate_single_export(manifest, run_a, "seed43")

        self.assertTrue(structure["valid"])

    def test_mismatched_json_viz_model_tag_fails(self) -> None:
        manifest, run_a, _ = make_campaign()
        self._inject_json_viz_identity(run_a[3])
        run_a[3]["shap_values"]["explained_model"] = "iforest"

        structure = validate_single_export(manifest, run_a, "seed43")
        codes = [error["code"] for error in structure["errors_by_key"][manifest["expected_keys"][3]]]
        self.assertIn("shap_values_identity_metadata_mismatch", codes)
