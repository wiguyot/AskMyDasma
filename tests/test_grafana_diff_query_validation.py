"""Regression checks for the decoder used to compare real Grafana results."""

import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "experiments/explainability/v1_v2/validate_grafana_diff_query.py"
spec = importlib.util.spec_from_file_location("validate_grafana_diff_query", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def response(order, identities=("row-1", "row-2")):
    labels = {"Group": "G1", "_field": "diff A"}
    values = {"_time": [1000, 1000], "source_event_id": list(identities), "_value": [0.0, 0.0]}
    fields = [{"name": name, "labels": labels} for name in order]
    return {"results": {"A": {"frames": [{"schema": {"fields": fields},
            "data": {"values": [values[name] for name in order]}}]}}}


class ComparisonRowsTest(unittest.TestCase):
    def test_field_order_does_not_change_comparison(self):
        original = response(["_time", "source_event_id", "_value"])
        reordered = response(["_time", "_value", "source_event_id"])
        self.assertEqual(module.comparison_rows(original), module.comparison_rows(reordered))
        self.assertEqual(len(module.comparison_rows(original)), 2)

    def test_repeated_rows_are_counted_not_silently_deduplicated(self):
        rows = module.comparison_rows(response(["_time", "_value", "source_event_id"], ("row-1", "row-1")))
        self.assertEqual(list(rows.values()), [2])

    def test_error_rejects_even_partial_frames(self):
        partial = response(["_time", "source_event_id", "_value"])
        partial["results"]["A"]["error"] = "SHAP comparison: duplicate v1"
        with self.assertRaisesRegex(ValueError, "duplicate v1"):
            module.comparison_rows(partial)
