import json
from pathlib import Path
from unittest import TestCase


DASHBOARD_PATH = (
    Path(__file__).resolve().parents[1]
    / "observability" / "grafana"
    / "dashboards" / "By model"
    / "source-data_kitnet_influxdb.json"
)


class GrafanaKitNETDashboardContractsTest(TestCase):
    def setUp(self) -> None:
        with DASHBOARD_PATH.open(encoding="utf-8") as fh:
            self.dashboard = json.load(fh)

    def _panel(self, title: str) -> dict:
        for panel in self.dashboard["panels"]:
            if panel.get("title") == title:
                return panel
        raise AssertionError(f"Panel not found: {title}")

    def test_kitnet_panel_plots_score_threshold_and_anomaly_markers(self) -> None:
        panel = self._panel("KitNET Analysis")
        query = panel["targets"][0]["query"]

        self.assertIn('import "math"', query)
        self.assertIn('r._field == "kitnet_score"', query)
        self.assertIn('r._field == "kitnet_threshold"', query)
        self.assertIn("math.log10(x: float(v: r._value) + 1.0)", query)
        self.assertIn("kitnet_score_log10", query)
        self.assertIn("kitnet_threshold_log10", query)
        self.assertNotIn('r._field == "kitnet_anomaly"', query)
        self.assertIn('r.kitnet_status == "anomaly"', query)
        self.assertIn('"kitnet_status"', query)
        self.assertIn("kitnet_anomaly_marker_log10", query)
        self.assertIn("union(tables: [score, threshold, anomalies])", query)
        self.assertIn(
            "aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)",
            query,
        )
        self.assertIn(
            "aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)",
            query,
        )
        self.assertNotIn("if float(v: r._value) > 5.0 then 5.0", query)
        self.assertNotIn(
            "kitnet_status",
            panel["fieldConfig"]["defaults"]["displayName"],
        )

    def test_raw_score_panel_keeps_untransformed_kitnet_scores(self) -> None:
        panel = self._panel("KitNET Raw Score")
        query = panel["targets"][0]["query"]

        self.assertIn('r._field == "kitnet_score"', query)
        self.assertIn('r._field == "kitnet_threshold"', query)
        self.assertIn("kitnet_anomaly_marker", query)
        self.assertIn("float(v: r._value)", query)
        self.assertNotIn("math.log10", query)
        self.assertNotIn("if float(v: r._value) > 5.0 then 5.0", query)

    def test_all_timeseries_panels_share_y_axis_width(self) -> None:
        timeseries_panels = [
            panel for panel in self.dashboard["panels"] if panel["type"] == "timeseries"
        ]

        self.assertGreaterEqual(len(timeseries_panels), 3)
        for panel in timeseries_panels:
            with self.subTest(panel=panel["title"]):
                self.assertEqual(
                    panel["fieldConfig"]["defaults"]["custom"]["axisWidth"],
                    96,
                )

    def test_kitnet_panel_styles_score_and_threshold_as_distinct_curves(self) -> None:
        panel = self._panel("KitNET Analysis")
        overrides = panel["fieldConfig"]["overrides"]
        by_name = {
            override.get("matcher", {}).get("options"): {
                prop["id"]: prop["value"]
                for prop in override.get("properties", [])
            }
            for override in overrides
        }

        score = by_name["kitnet_score_log10"]
        self.assertEqual(score["color"]["mode"], "fixed")
        self.assertEqual(score["color"]["fixedColor"], "yellow")
        self.assertEqual(score["custom.lineWidth"], 1)
        self.assertIn("log10(score + 1)", score["displayName"])

        threshold = by_name["kitnet_threshold_log10"]
        self.assertEqual(threshold["color"]["mode"], "fixed")
        self.assertEqual(threshold["color"]["fixedColor"], "blue")
        self.assertEqual(threshold["custom.lineWidth"], 3)
        self.assertIn("log10(threshold + 1)", threshold["displayName"])

    def test_kitnet_panel_styles_anomaly_marker_as_red_points(self) -> None:
        panel = self._panel("KitNET Analysis")
        overrides = panel["fieldConfig"]["overrides"]
        marker_overrides = [
            override
            for override in overrides
            if override.get("matcher", {}).get("options")
            == "kitnet_anomaly_marker_log10"
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

    def test_features_panel_and_variable_exclude_kitnet_derived_fields(self) -> None:
        panel = self._panel("Features")
        panel_query = panel["targets"][0]["query"]
        self.assertIn('not strings.hasPrefix(v: r._field, prefix: "kitnet_")', panel_query)
        self.assertNotIn('group(columns: ["_field", "Group"])', panel_query)
        self.assertIn('"kitnet_status"', panel_query)
        self.assertIn(
            "aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)",
            panel_query,
        )
        self.assertNotIn("kitnet_status", panel["fieldConfig"]["defaults"]["displayName"])

        variables = self.dashboard["templating"]["list"]
        component_variable = next(
            variable for variable in variables if variable["name"] == "feature"
        )
        variable_query = component_variable["query"]
        self.assertIn('not strings.hasPrefix(v: r._value, prefix: "kitnet_")', variable_query)
