import json
from pathlib import Path
from unittest import TestCase


DASHBOARD_PATH = (
    Path(__file__).resolve().parents[1]
    / "observability" / "grafana"
    / "dashboards" / "Explainability"
    / "explainability_compare_influxdb.json"
)


class GrafanaExplainabilityCompareDashboardContractsTest(TestCase):
    def setUp(self) -> None:
        with DASHBOARD_PATH.open(encoding="utf-8") as fh:
            self.dashboard = json.load(fh)

    def _panel(self, title: str) -> dict:
        for panel in self.dashboard["panels"]:
            if panel.get("title") == title:
                return panel
        raise AssertionError(f"Panel not found: {title}")

    def test_dashboard_metadata(self) -> None:
        self.assertEqual(self.dashboard["uid"], "explainability-compare-influxdb")
        self.assertEqual(self.dashboard["title"], "Explainability v1 vs v2")
        self.assertIn("explainability-v2", self.dashboard.get("tags", []))

    def test_dashboard_has_dedicated_v1_and_v2_score_panels(self) -> None:
        v1_panel = self._panel("shap_base_value and local_model_score (v1)")
        v2_panel = self._panel("shap_base_value and local_model_score (v2)")

        v1_query = v1_panel["targets"][0]["query"]
        v2_query = v2_panel["targets"][0]["query"]

        self.assertIn('from(bucket: "explainability")', v1_query)
        self.assertIn('r._measurement == "explainability"', v1_query)
        self.assertIn('yield(name: "v1")', v1_query)
        self.assertNotIn("explainability-v2", v1_query)

        self.assertIn('from(bucket: "explainability-v2")', v2_query)
        self.assertIn('r._measurement == "explainability-v2"', v2_query)
        self.assertIn('yield(name: "v2")', v2_query)
        self.assertGreater(v2_panel["gridPos"]["y"], v1_panel["gridPos"]["y"])
        self.assertIn('r._field == "local_score"', v1_query)
        self.assertIn('r._field == "local_score"', v2_query)
        self.assertIn('then "local_model_score"', v1_query)
        self.assertIn('then "local_model_score"', v2_query)

    def test_v1_score_panel_groups_events_into_curves(self) -> None:
        panel = self._panel("shap_base_value and local_model_score (v1)")
        query = panel["targets"][0]["query"]
        self.assertIn('group(columns: ["_field", "Group"])', query)
        self.assertIn("aggregateWindow(every: 1s, fn: last, createEmpty: false)", query)
        self.assertEqual(panel["fieldConfig"]["defaults"]["custom"]["showPoints"], "always")

    def test_dashboard_omits_runtime_diagnostic_panels(self) -> None:
        titles = {panel["title"] for panel in self.dashboard["panels"]}

        self.assertNotIn("Pipeline throughput - source vs SHAP completions", titles)
        self.assertNotIn("SHAP computation time (ms) - v1 vs v2", titles)

    def test_removed_contribution_panels_are_not_reintroduced(self) -> None:
        titles = {panel["title"] for panel in self.dashboard["panels"]}
        self.assertNotIn("SHAP contribution of a feature — v1 vs v2 (overlay)", titles)
        self.assertNotIn("SHAP contribution difference (v2 - v1)", titles)

    def test_first_panel_hides_legend(self) -> None:
        panel = self._panel("shap_base_value and local_model_score (v1)")
        self.assertFalse(panel["options"]["legend"]["showLegend"])

    def test_source_panel_hides_legend(self) -> None:
        panel = self._panel("Source elements")
        self.assertFalse(panel["options"]["legend"]["showLegend"])

    def test_source_panel_stays_on_first_row(self) -> None:
        source_panel = self._panel("Source elements")
        v1_panel = self._panel("shap_base_value and local_model_score (v1)")
        v2_panel = self._panel("shap_base_value and local_model_score (v2)")
        values_panel = self._panel("SHAP values V1/V2 (precomputed)")
        diff_panel = self._panel("SHAP difference V2 - V1 (precomputed)")

        self.assertEqual(source_panel["gridPos"]["y"], 0)
        self.assertGreater(v1_panel["gridPos"]["y"], source_panel["gridPos"]["y"])
        self.assertGreater(v2_panel["gridPos"]["y"], v1_panel["gridPos"]["y"])
        self.assertGreater(values_panel["gridPos"]["y"], v2_panel["gridPos"]["y"])
        self.assertGreater(diff_panel["gridPos"]["y"], values_panel["gridPos"]["y"])


if __name__ == "__main__":
    import unittest
    unittest.main()
