"""Pure tests for the OC-15 exploratory seed-stability measurement."""

import copy
import json
import unittest

from experiments.explainability.v2.measure_shap_seed_stability import (
    StabilityError,
    measure_seed_stability,
)

CAMPAIGN = "oc15-explainability-v2-minimal-validation"
DISCLAIMER_FRAGMENT = "requiert ultérieurement des critères métier ou scientifiques"


def make_manifest():
    return {
        "campaign": CAMPAIGN,
        "reference_seed": 42,
        "secondary_seeds": [43, 44],
        "feature_count": 3,
        "numeric_tolerances": {"absolute_tolerance": 1e-12, "relative_tolerance": 1e-12},
    }


def make_record(key, shap, anomaly=True, status="computed"):
    group, event_id = key.split("|", 1)
    return {
        "Group": group,
        "source_event_id": event_id,
        "explanation_status": status,
        "shap_values": dict(shap),
        "hstree_anomaly": anomaly,
    }


def make_campaign():
    reference = [
        make_record("OC15-Alpha|oc15-alpha-001", {"feat_a": 1.0, "feat_b": -2.0, "feat_c": 0.5}),
        make_record("OC15-Alpha|oc15-alpha-002", {"feat_a": 0.1, "feat_b": -0.2, "feat_c": 3.0}),
        make_record("OC15-Alpha|oc15-alpha-003", {"feat_a": 0.0, "feat_b": 0.0, "feat_c": 0.0}),
    ]
    # Seed 43: same signs and same ranking, slightly different magnitudes.
    seed43 = [
        make_record("OC15-Alpha|oc15-alpha-001", {"feat_a": 1.2, "feat_b": -1.8, "feat_c": 0.4}),
        make_record("OC15-Alpha|oc15-alpha-002", {"feat_a": 0.1, "feat_b": -0.2, "feat_c": 2.8}),
        make_record("OC15-Alpha|oc15-alpha-003", {"feat_a": 0.0, "feat_b": 0.0, "feat_c": 0.0}),
    ]
    # Seed 44: rank inverted and sign flipped on the top feature.
    seed44 = [
        make_record("OC15-Alpha|oc15-alpha-001", {"feat_a": -1.0, "feat_b": 2.0, "feat_c": 0.5}),
        make_record("OC15-Alpha|oc15-alpha-002", {"feat_a": 0.1, "feat_b": -0.2, "feat_c": 3.0}),
        make_record("OC15-Alpha|oc15-alpha-003", {"feat_a": 0.0, "feat_b": 0.0, "feat_c": 0.0}),
    ]
    return reference, {"43": seed43, "44": seed44}


class SeedStabilityTest(unittest.TestCase):
    @staticmethod
    def _as_json_viz_record(record):
        """Mirror the identity wrapper emitted by the real *-v2-viz topic."""
        wrapped = copy.deepcopy(record)
        wrapped["explained_model"] = "kitnet"
        wrapped["shap_values"] = {
            **wrapped["shap_values"],
            "Group": wrapped["Group"],
            "source_event_id": wrapped["source_event_id"],
            "explained_model": wrapped["explained_model"],
        }
        return wrapped

    def test_real_json_viz_shap_identity_fields_are_not_features(self) -> None:
        reference, secondary = make_campaign()
        reference = [self._as_json_viz_record(record) for record in reference]
        secondary = {
            seed: [self._as_json_viz_record(record) for record in records]
            for seed, records in secondary.items()
        }

        report = measure_seed_stability(make_manifest(), reference, {"43": secondary["43"]})

        event = report["seeds"]["43"]["per_event"][0]
        self.assertNotIn("error", event)
        self.assertEqual(report["seeds"]["43"]["aggregate"]["computed_events_compared"], 3)

    def test_incoherent_json_viz_identity_is_rejected(self) -> None:
        reference, secondary = make_campaign()
        broken = self._as_json_viz_record(secondary["43"][0])
        broken["shap_values"]["Group"] = "wrong-group"
        secondary["43"][0] = broken

        with self.assertRaisesRegex(StabilityError, "shap_values_identity_metadata_mismatch"):
            measure_seed_stability(make_manifest(), reference, {"43": secondary["43"]})

    def test_unrelated_non_numeric_shap_value_is_rejected(self) -> None:
        reference, secondary = make_campaign()
        secondary["43"][0]["shap_values"]["not_a_feature"] = "not-a-number"

        with self.assertRaisesRegex(StabilityError, "non-numeric shap_values"):
            measure_seed_stability(make_manifest(), reference, {"43": secondary["43"]})

    def test_stable_seed_has_high_correlation_and_no_flips(self) -> None:
        reference, secondary = make_campaign()
        report = measure_seed_stability(make_manifest(), reference, {"43": secondary["43"]})
        data = report["seeds"]["43"]
        self.assertEqual(data["aggregate"]["computed_events_compared"], 3)
        self.assertEqual(data["aggregate"]["spearman_mean"], 1.0)
        self.assertEqual(data["aggregate"]["sign_flip_rate_top1_mean"], 0.0)
        self.assertEqual(data["aggregate"]["jaccard_at_1_mean"], 1.0)
        self.assertLess(data["aggregate"]["relative_mean_abs_diff_mean"], 0.2)
        self.assertTrue(data["aggregate"]["identity_status_verdict_all_conserved"])

    def test_unstable_seed_is_measured_without_verdict(self) -> None:
        reference, secondary = make_campaign()
        report = measure_seed_stability(make_manifest(), reference, secondary)
        data = report["seeds"]["44"]
        # Event alpha-001 has its top feature sign flipped; the other events
        # are identical, so the MEAN flip rate is 1/3 — reported, never judged.
        self.assertGreater(data["aggregate"]["sign_flip_rate_top1_mean"], 0.0)
        self.assertLess(data["aggregate"]["spearman_mean"], 1.0)
        self.assertIn("disclaimer", report)
        self.assertIn(DISCLAIMER_FRAGMENT, report["disclaimer"])
        # No pass/fail decision field anywhere in the report tree.
        def collect_keys(node):
            keys = set()
            if isinstance(node, dict):
                for key, value in node.items():
                    keys.add(key)
                    keys |= collect_keys(value)
            elif isinstance(node, list):
                for item in node:
                    keys |= collect_keys(item)
            return keys

        all_keys = collect_keys(report)
        self.assertFalse(
            {"passed", "verdict", "compliant", "threshold"} & all_keys,
            f"decision-like keys found: {sorted(all_keys)}",
        )

    def test_event_level_details_are_reported(self) -> None:
        reference, secondary = make_campaign()
        report = measure_seed_stability(make_manifest(), reference, secondary)
        event = report["seeds"]["44"]["per_event"][0]
        self.assertEqual(event["key"], "OC15-Alpha|oc15-alpha-001")
        self.assertEqual(event["sign_flip_rate_top1"], 1.0)
        # Jaccard@k uses absolute values: a sign flip does not change the
        # top-k membership, so the overlap stays 1.0 — the flip is captured
        # by sign_flip_rate and the rank inversion by Spearman instead.
        self.assertEqual(event["jaccard_at_1"], 1.0)

    def test_identity_and_verdict_conservation(self) -> None:
        reference, secondary = make_campaign()
        broken = copy.deepcopy(secondary["43"])
        broken[0]["hstree_anomaly"] = not broken[0]["hstree_anomaly"]
        report = measure_seed_stability(make_manifest(), reference, {"43": broken})
        self.assertFalse(report["seeds"]["43"]["aggregate"]["identity_status_verdict_all_conserved"])

    def test_feature_set_mismatch_is_flagged_per_event(self) -> None:
        reference, secondary = make_campaign()
        broken = copy.deepcopy(secondary["43"])
        del broken[1]["shap_values"]["feat_b"]
        report = measure_seed_stability(make_manifest(), reference, {"43": broken})
        event = report["seeds"]["43"]["per_event"][1]
        self.assertIn("error", event)
        self.assertEqual(event["error"], "feature_set_mismatch")

    def test_missing_secondary_seed_export_rejected(self) -> None:
        reference, _ = make_campaign()
        with self.assertRaises(StabilityError):
            measure_seed_stability(make_manifest(), reference, {})

    def test_event_not_computed_on_both_sides_is_flagged(self) -> None:
        reference, secondary = make_campaign()
        broken = copy.deepcopy(secondary["43"])
        broken[2]["explanation_status"] = "warmup"
        report = measure_seed_stability(make_manifest(), reference, {"43": broken})
        # alpha-003 is computed in the reference but warmup in seed 43.
        flagged = [event for event in report["seeds"]["43"]["per_event"] if "error" in event]
        self.assertEqual(len(flagged), 1)
        self.assertEqual(flagged[0]["error"], "event_not_computed_on_both_sides")

    def test_wrong_campaign_name_rejected(self) -> None:
        manifest = make_manifest()
        manifest["campaign"] = "other"
        reference, secondary = make_campaign()
        with self.assertRaises(StabilityError):
            measure_seed_stability(manifest, reference, secondary)


if __name__ == "__main__":
    unittest.main()


class ConservationStrictnessTest(unittest.TestCase):
    """OC-15R3: all_conserved is false as soon as ONE event diverges."""

    def test_reference_computed_secondary_warmup_is_flagged(self) -> None:
        reference, secondary = make_campaign()
        broken = copy.deepcopy(secondary["43"])
        broken[0]["explanation_status"] = "warmup"  # no longer computed
        report = measure_seed_stability(make_manifest(), reference, {"43": broken})
        aggregate = report["seeds"]["43"]["aggregate"]
        self.assertFalse(aggregate["identity_status_verdict_all_conserved"])
        self.assertGreater(aggregate["events_missing_on_one_side"], 0)
        flagged = [event for event in report["seeds"]["43"]["per_event"] if "error" in event]
        self.assertEqual(len(flagged), 1)
        self.assertEqual(flagged[0]["error"], "event_not_computed_on_both_sides")

    def test_key_absent_from_secondary_is_flagged(self) -> None:
        reference, secondary = make_campaign()
        broken = copy.deepcopy(secondary["43"])
        del broken[1]
        report = measure_seed_stability(make_manifest(), reference, {"43": broken})
        aggregate = report["seeds"]["43"]["aggregate"]
        self.assertFalse(aggregate["identity_status_verdict_all_conserved"])
        self.assertEqual(aggregate["events_missing_on_one_side"], 1)
        flagged = [event for event in report["seeds"]["43"]["per_event"] if "error" in event]
        self.assertEqual(flagged[0]["key"], "OC15-Alpha|oc15-alpha-002")

    def test_missing_events_never_coexist_with_all_conserved_true(self) -> None:
        reference, secondary = make_campaign()
        for seed in ("43", "44"):
            broken = copy.deepcopy(secondary[seed])
            del broken[2]
            report = measure_seed_stability(make_manifest(), reference, {seed: broken})
            aggregate = report["seeds"][seed]["aggregate"]
            self.assertGreater(aggregate["events_missing_on_one_side"], 0)
            self.assertFalse(aggregate["identity_status_verdict_all_conserved"])

    def test_duplicate_computed_key_raises(self) -> None:
        reference, secondary = make_campaign()
        broken = copy.deepcopy(secondary["43"])
        broken.append(copy.deepcopy(broken[0]))
        with self.assertRaises(StabilityError):
            measure_seed_stability(make_manifest(), reference, {"43": broken})

    def test_feature_set_divergence_fails_conservation(self) -> None:
        reference, secondary = make_campaign()
        broken = copy.deepcopy(secondary["43"])
        broken[0]["shap_values"]["feat_extra"] = 0.1
        report = measure_seed_stability(make_manifest(), reference, {"43": broken})
        aggregate = report["seeds"]["43"]["aggregate"]
        self.assertFalse(aggregate["identity_status_verdict_all_conserved"])
        flagged = [event for event in report["seeds"]["43"]["per_event"] if "error" in event]
        self.assertEqual(flagged[0]["error"], "feature_set_mismatch")

    def test_nominal_campaign_keeps_all_conserved_true(self) -> None:
        reference, secondary = make_campaign()
        report = measure_seed_stability(make_manifest(), reference, secondary)
        for seed, data in report["seeds"].items():
            self.assertTrue(
                data["aggregate"]["identity_status_verdict_all_conserved"],
                f"seed {seed}",
            )
            self.assertEqual(data["aggregate"]["events_missing_on_one_side"], 0)

    def test_no_duplicate_definitions_remain(self) -> None:
        import inspect
        from experiments.explainability.v2 import measure_shap_seed_stability as module

        source = inspect.getsource(module)
        self.assertEqual(source.count("def _computed_index"), 1)
        self.assertEqual(source.count("def _vector"), 1)
        self.assertEqual(source.count("if top_k is None"), 1)
