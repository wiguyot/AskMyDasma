import json
from pathlib import Path
from unittest import TestCase


DASHBOARD_PATH = (
    Path(__file__).resolve().parents[1]
    / "observability" / "grafana"
    / "dashboards" / "By model"
    / "source-data_iforest_influxdb.json"
)


class GrafanaIForestDashboardContractsTest(TestCase):
    def setUp(self) -> None:
        with DASHBOARD_PATH.open(encoding="utf-8") as fh:
            self.dashboard = json.load(fh)

    def _panel(self, title: str) -> dict:
        for panel in self.dashboard["panels"]:
            if panel.get("title") == title:
                return panel
        raise AssertionError(f"Panel not found: {title}")

    def test_iforest_panel_plots_score_threshold_and_anomaly_markers(self) -> None:
        panel = self._panel("IForest Analysis")
        query = panel["targets"][0]["query"]

        self.assertIn('r._field == "iforest_score"', query)
        self.assertIn('r._field == "iforest_threshold"', query)
        self.assertNotIn('r._field == "iforest_anomaly"', query)
        self.assertIn('r.iforest_status == "anomaly"', query)
        self.assertIn('"iforest_status"', query)
        self.assertIn("iforest_anomaly_marker", query)
        self.assertIn("union(tables: [score, threshold, anomalies])", query)
        self.assertIn(
            "aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)",
            query,
        )
        self.assertIn(
            "aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)",
            query,
        )
        self.assertNotIn(
            "iforest_status",
            panel["fieldConfig"]["defaults"]["displayName"],
        )

    def test_iforest_panel_styles_score_and_threshold_as_distinct_curves(self) -> None:
        panel = self._panel("IForest Analysis")
        overrides = panel["fieldConfig"]["overrides"]
        by_name = {
            override.get("matcher", {}).get("options"): {
                prop["id"]: prop["value"]
                for prop in override.get("properties", [])
            }
            for override in overrides
        }

        score = by_name["iforest_score"]
        self.assertEqual(score["color"]["mode"], "fixed")
        self.assertEqual(score["color"]["fixedColor"], "yellow")
        self.assertEqual(score["custom.lineWidth"], 1)

        threshold = by_name["iforest_threshold"]
        self.assertEqual(threshold["color"]["mode"], "fixed")
        self.assertEqual(threshold["color"]["fixedColor"], "blue")
        self.assertEqual(threshold["custom.lineWidth"], 3)

    def test_iforest_panel_styles_anomaly_marker_as_red_points(self) -> None:
        panel = self._panel("IForest Analysis")
        overrides = panel["fieldConfig"]["overrides"]
        marker_overrides = [
            override
            for override in overrides
            if override.get("matcher", {}).get("options") == "iforest_anomaly_marker"
        ]

        self.assertEqual(len(marker_overrides), 1)
        properties = {
            prop["id"]: prop["value"]
            for prop in marker_overrides[0]["properties"]
        }
        self.assertEqual(properties["custom.drawStyle"], "points")
        self.assertEqual(properties["custom.showPoints"], "always")
        self.assertEqual(properties["custom.lineWidth"], 0)
        self.assertEqual(properties["custom.pointSize"], 8)
        self.assertEqual(properties["color"]["mode"], "fixed")
        self.assertEqual(properties["color"]["fixedColor"], "red")

    def test_features_panel_and_variable_exclude_iforest_derived_fields(self) -> None:
        panel = self._panel("Features")
        panel_query = panel["targets"][0]["query"]
        self.assertIn('not strings.hasPrefix(v: r._field, prefix: "iforest_")', panel_query)
        self.assertNotIn('group(columns: ["_field", "Group"])', panel_query)
        self.assertIn('"iforest_status"', panel_query)
        self.assertIn(
            "aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)",
            panel_query,
        )
        self.assertNotIn("iforest_status", panel["fieldConfig"]["defaults"]["displayName"])

        variables = self.dashboard["templating"]["list"]
        component_variable = next(
            variable for variable in variables if variable["name"] == "feature"
        )
        variable_query = component_variable["query"]
        self.assertIn('not strings.hasPrefix(v: r._value, prefix: "iforest_")', variable_query)
