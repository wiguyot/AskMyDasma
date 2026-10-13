import json
from pathlib import Path
from unittest import TestCase


DASHBOARD_PATH = (
    Path(__file__).resolve().parents[1]
    / "observability" / "grafana"
    / "dashboards" / "By model"
    / "source-data_deepant_influxdb.json"
)


class GrafanaDeepAntDashboardContractsTest(TestCase):
    def setUp(self) -> None:
        with DASHBOARD_PATH.open(encoding="utf-8") as fh:
            self.dashboard = json.load(fh)

    def _panel(self, title: str) -> dict:
        for panel in self.dashboard["panels"]:
            if panel.get("title") == title:
                return panel
        raise AssertionError(f"Panel not found: {title}")

    def test_dashboard_metadata(self) -> None:
        self.assertEqual(self.dashboard["uid"], "source-data-deepant-influxdb")
        self.assertEqual(self.dashboard["title"], "DeepAnT")
        self.assertIn("deepant", self.dashboard.get("tags", []))

    def test_deepant_panel_plots_score_threshold_and_anomaly_markers(self) -> None:
        panel = self._panel("DeepAnT Analysis")
        query = panel["targets"][0]["query"]

        self.assertIn('import "math"', query)
        self.assertIn('r._field == "deepant_score"', query)
        self.assertIn('r._field == "deepant_threshold"', query)
        self.assertIn("math.log10(x: float(v: r._value) + 1.0)", query)
        self.assertIn("deepant_score_log10", query)
        self.assertIn("deepant_threshold_log10", query)
        self.assertNotIn('r._field == "deepant_anomaly"', query)
        self.assertIn('r.deepant_status == "anomaly"', query)
        self.assertIn('"deepant_status"', query)
        self.assertIn("deepant_anomaly_marker_log10", query)
        self.assertIn("union(tables: [score, threshold, anomalies])", query)
        self.assertIn(
            "aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)",
            query,
        )
        self.assertIn(
            "aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)",
            query,
        )
        self.assertNotIn(
            "deepant_status",
            panel["fieldConfig"]["defaults"]["displayName"],
        )

    def test_raw_score_panel_keeps_untransformed_deepant_scores(self) -> None:
        panel = self._panel("DeepAnT Raw Score")
        query = panel["targets"][0]["query"]

        self.assertIn('r._field == "deepant_score"', query)
        self.assertIn('r._field == "deepant_threshold"', query)
        self.assertIn("deepant_anomaly_marker", query)
        self.assertIn("float(v: r._value)", query)
        self.assertNotIn("math.log10", query)

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

    def test_deepant_panel_reads_from_deepant_bucket(self) -> None:
        panel = self._panel("DeepAnT Analysis")
        query = panel["targets"][0]["query"]
        self.assertIn('from(bucket: "deepant-analysis")', query)
        self.assertIn('r._measurement == "deepant-analysis"', query)

    def test_deepant_panel_styles_score_and_threshold_as_distinct_curves(self) -> None:
        panel = self._panel("DeepAnT Analysis")
        overrides = panel["fieldConfig"]["overrides"]
        by_name = {
            override.get("matcher", {}).get("options"): {
                prop["id"]: prop["value"]
                for prop in override.get("properties", [])
            }
            for override in overrides
        }

        score = by_name["deepant_score_log10"]
        self.assertEqual(score["color"]["mode"], "fixed")
        self.assertEqual(score["color"]["fixedColor"], "yellow")
        self.assertEqual(score["custom.lineWidth"], 1)

        threshold = by_name["deepant_threshold_log10"]
        self.assertEqual(threshold["color"]["mode"], "fixed")
        self.assertEqual(threshold["color"]["fixedColor"], "blue")
        self.assertEqual(threshold["custom.lineWidth"], 3)

    def test_deepant_panel_styles_anomaly_marker_as_red_points(self) -> None:
        panel = self._panel("DeepAnT Analysis")
        overrides = panel["fieldConfig"]["overrides"]
        marker_overrides = [
            override
            for override in overrides
            if override.get("matcher", {}).get("options")
            == "deepant_anomaly_marker_log10"
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

    def test_features_panel_and_variable_exclude_deepant_derived_fields(self) -> None:
        panel = self._panel("Features")
        panel_query = panel["targets"][0]["query"]
        self.assertIn(
            'not strings.hasPrefix(v: r._field, prefix: "deepant_")', panel_query
        )
        self.assertIn('"deepant_status"', panel_query)
        self.assertIn(
            "aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)",
            panel_query,
        )
        self.assertNotIn(
            "deepant_status", panel["fieldConfig"]["defaults"]["displayName"]
        )

        variables = self.dashboard["templating"]["list"]
        feature_variable = next(
            variable for variable in variables if variable["name"] == "feature"
        )
        variable_query = feature_variable["query"]
        self.assertIn(
            'not strings.hasPrefix(v: r._value, prefix: "deepant_")',
            variable_query,
        )
