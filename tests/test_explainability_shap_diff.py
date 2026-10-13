import unittest

from validation.explainability.shap_diff import (
    build_emission_records,
    ComparisonBucket,
    process_arrival,
    ShapDiffError,
    compare_shap_records,
    normalized_shap_values,
    records_signature,
    strict_identity,
)


def record(**overrides):
    base = {
        "timestamp": "2026-09-25 12:00:00",
        "Group": "G1",
        "source_event_id": "row-1",
        "explanation_status": "computed",
        "shap_values": {"A": 1.5, "B": -2.0},
    }
    base.update(overrides)
    return base


class ShapDiffContractTest(unittest.TestCase):
    def test_exact_identity_preserves_spaces(self):
        self.assertEqual(strict_identity(record(Group=" G1 ")), (" G1 ", "row-1"))
        with self.assertRaises(ShapDiffError):
            strict_identity(record(Group="   "))

    def test_injected_identity_is_removed_before_numeric_conversion(self):
        values = normalized_shap_values(
            record(
                shap_values={
                    "Group": "G1",
                    "source_event_id": "row-1",
                    "explained_model": "kitnet",
                    "A": 1,
                }
            ),
            "v1",
        )
        self.assertEqual(values, {"A": 1.0})

    def test_transport_metadata_does_not_change_computed_comparison(self):
        shap_values = {
            "Group": "G1",
            "source_event_id": "row-1",
            "explained_model": "kitnet",
            "A": 1.5,
            "B": -2.0,
        }
        rows = build_emission_records(
            record(shap_values=shap_values), record(shap_values=shap_values)
        )

        self.assertTrue(rows)
        self.assertTrue(all(row.comparison_status == "complete" for row in rows))
        self.assertTrue(all(row.difference == 0.0 for row in rows))
        self.assertEqual({row.feature for row in rows}, {"A", "B"})

    def test_identity_injection_is_not_allowed_to_hide_bad_feature_values(self):
        with self.assertRaisesRegex(ShapDiffError, "not numeric"):
            normalized_shap_values(record(shap_values={"Group": "G1", "A": "bad"}), "v1")

    def test_all_features_are_preserved_and_sorted(self):
        rows = compare_shap_records(record(shap_values={"B": 2.0, "A": 1.0}), record(shap_values={"B": 2.5, "A": 1.0}))
        self.assertEqual([r.feature for r in rows], ["A", "B"])
        self.assertEqual([r.difference for r in rows], [0.0, 0.5])

    def test_values_and_difference_are_flat_records(self):
        row = compare_shap_records(record(), record(shap_values={"A": 2.0, "B": -2.0}))[0].as_record()
        self.assertEqual(row["value_v1"], 1.5)
        self.assertEqual(row["value_v2"], 2.0)
        self.assertEqual(row["difference"], 0.5)
        self.assertEqual(row["comparison_status"], "complete")

    def test_noncomputed_events_do_not_create_zero_shap(self):
        self.assertEqual(compare_shap_records(record(explanation_status="warmup", shap_values=None), record(explanation_status="warmup", shap_values=None)), ())

    def test_missing_feature_is_explicit_and_no_difference_is_invented(self):
        rows = compare_shap_records(record(shap_values={"A": 1.0}), record(shap_values={"B": 2.0}))
        self.assertEqual({r.comparison_status for r in rows}, {"feature_missing_v1", "feature_missing_v2"})
        self.assertTrue(all(r.difference is None for r in rows))

    def test_timestamp_mismatch_is_explicit(self):
        rows = compare_shap_records(record(), record(timestamp="2026-09-25 12:00:01"))
        self.assertTrue(all(r.comparison_status == "timestamp_mismatch" for r in rows))
        self.assertTrue(all(r.difference is None for r in rows))

    def test_identity_mismatch_is_rejected(self):
        with self.assertRaisesRegex(ShapDiffError, "identity mismatch"):
            compare_shap_records(record(), record(source_event_id="row-2"))

    def test_invalid_numbers_are_rejected(self):
        with self.assertRaisesRegex(ShapDiffError, "not finite"):
            compare_shap_records(record(shap_values={"A": float("nan")}), record())
        with self.assertRaisesRegex(ShapDiffError, "not numeric"):
            compare_shap_records(record(shap_values={"A": True}), record())

    def test_empty_computed_shap_is_rejected(self):
        with self.assertRaisesRegex(ShapDiffError, "shap_values is empty"):
            compare_shap_records(record(shap_values={}), record())

    def test_signature_is_order_independent(self):
        rows = compare_shap_records(record(), record())
        self.assertEqual(records_signature(rows), records_signature(tuple(reversed(rows))))

    def test_noncomputed_emits_metadata_without_fake_feature(self):
        rows = build_emission_records(
            record(explanation_status="warmup", shap_values=None),
            record(explanation_status="warmup", shap_values=None),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].feature, "")
        self.assertEqual(rows[0].comparison_status, "non_computed")
        self.assertIsNone(rows[0].difference)

    def test_status_mismatch_emits_metadata(self):
        rows = build_emission_records(record(), record(explanation_status="failed", shap_values=None))
        self.assertEqual(rows[0].comparison_status, "status_mismatch")

    def test_arrivals_are_order_independent(self):
        pending = {}
        first, reason = process_arrival(pending, "v2", record(shap_values={"A": 2.0}), now=1.0)
        self.assertIsNone(reason)
        second, reason = process_arrival(pending, "v1", record(), now=2.0)
        self.assertEqual((first.group, first.source_event_id), (second.group, second.source_event_id))
        self.assertEqual(reason, "complete")

    def test_identical_redelivery_is_suppressed(self):
        pending = {}
        process_arrival(pending, "v1", record(), now=1.0)
        process_arrival(pending, "v2", record(), now=1.0)
        _, reason = process_arrival(pending, "v2", record(), now=2.0)
        self.assertIsNone(reason)

    def test_changed_redelivery_is_progress(self):
        pending = {}
        process_arrival(pending, "v1", record(), now=1.0)
        process_arrival(pending, "v2", record(), now=1.0)
        _, reason = process_arrival(pending, "v2", record(shap_values={"A": 3.0, "B": -2.0}), now=2.0)
        self.assertEqual(reason, "progress")


if __name__ == "__main__":
    unittest.main()
