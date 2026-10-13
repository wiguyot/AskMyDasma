import json
from pathlib import Path
from unittest import TestCase


DASHBOARD_PATH = (
    Path(__file__).resolve().parents[1]
    / "observability" / "grafana"
    / "dashboards" / "By model"
    / "source-data_mdragpoint_influxdb.json"
)


class GrafanaMDragPointDashboardContractsTest(TestCase):
    def setUp(self) -> None:
        with DASHBOARD_PATH.open(encoding="utf-8") as fh:
            self.dashboard = json.load(fh)

    def _panel(self, title: str) -> dict:
        for panel in self.dashboard["panels"]:
            if panel.get("title") == title:
                return panel
        raise AssertionError(f"Panel not found: {title}")

    def test_dashboard_metadata(self) -> None:
        self.assertEqual(self.dashboard["uid"], "source-data-mdragpoint-influxdb")
        self.assertEqual(self.dashboard["title"], "mDragStream Point-based")
        self.assertIn("mdragpoint", self.dashboard.get("tags", []))

    def test_mdragpoint_panel_plots_score_and_anomaly_markers(self) -> None:
        panel = self._panel("mDragStream Point-based Analysis")
        query = panel["targets"][0]["query"]

        self.assertIn('import "math"', query)
        self.assertIn('r._field == "mdragpoint_score"', query)
        self.assertIn("math.log10(x: float(v: r._value) + 1.0)", query)
        self.assertIn("mdragpoint_score_log10", query)
        self.assertNotIn('r._field == "mdragpoint_internal_r"', query)
        self.assertNotIn('r._field == "mdragpoint_threshold"', query)
        self.assertNotIn('r._field == "mdragpoint_anomaly"', query)
        self.assertIn('r.mdragpoint_status == "anomaly"', query)
        self.assertIn('"mdragpoint_status"', query)
        self.assertIn("mdragpoint_anomaly_marker_log10", query)
        self.assertIn("union(tables: [score, anomalies])", query)

    def test_raw_score_panel_keeps_untransformed_score_optional(self) -> None:
        panel = self._panel("mDragStream Point-based Raw Score")
        query = panel["targets"][0]["query"]

        self.assertIn('r._field == "mdragpoint_score"', query)
        self.assertIn("mdragpoint_anomaly_marker", query)
        self.assertIn("float(v: r._value)", query)
        self.assertNotIn("math.log10", query)

    def test_timeseries_panels_share_y_axis_width_for_time_alignment(self) -> None:
        features = self._panel("Features")
        analysis = self._panel("mDragStream Point-based Analysis")
        raw_score = self._panel("mDragStream Point-based Raw Score")

        features_width = features["fieldConfig"]["defaults"]["custom"]["axisWidth"]
        analysis_width = analysis["fieldConfig"]["defaults"]["custom"]["axisWidth"]
        raw_score_width = raw_score["fieldConfig"]["defaults"]["custom"]["axisWidth"]

        self.assertEqual(features_width, 96)
        self.assertEqual(analysis_width, 96)
        self.assertEqual(raw_score_width, analysis_width)

    def test_mdragpoint_panel_reads_from_mdragstream_point_bucket(self) -> None:
        panel = self._panel("mDragStream Point-based Analysis")
        query = panel["targets"][0]["query"]
        self.assertIn('from(bucket: "mdragstream-point-analysis")', query)
        self.assertIn('r._measurement == "mdragstream-point-analysis"', query)

    def test_mdragpoint_panel_styles_score_and_markers(self) -> None:
        panel = self._panel("mDragStream Point-based Analysis")
        overrides = panel["fieldConfig"]["overrides"]
        by_name = {
            override.get("matcher", {}).get("options"): {
                prop["id"]: prop["value"]
                for prop in override.get("properties", [])
            }
            for override in overrides
        }

        self.assertEqual(by_name["mdragpoint_score_log10"]["color"]["fixedColor"], "yellow")
        self.assertNotIn("mdragpoint_threshold", by_name)
        self.assertNotIn("mdragpoint_internal_r", by_name)
        marker = by_name["mdragpoint_anomaly_marker_log10"]
        self.assertEqual(
            marker["displayName"],
            'mDragStream Point-based native anomaly log10(score + 1) group="${__field.labels.Group}"',
        )
        self.assertEqual(marker["color"]["fixedColor"], "red")
        self.assertEqual(marker["custom.drawStyle"], "points")
        self.assertEqual(marker["custom.showPoints"], "always")

    def test_features_panel_and_variable_exclude_mdragpoint_derived_fields(self) -> None:
        panel = self._panel("Features")
        panel_query = panel["targets"][0]["query"]
        self.assertIn(
            'not strings.hasPrefix(v: r._field, prefix: "mdragpoint_")',
            panel_query,
        )
        self.assertIn('"mdragpoint_status"', panel_query)
        self.assertNotIn(
            "mdragpoint_status", panel["fieldConfig"]["defaults"]["displayName"]
        )

        variables = self.dashboard["templating"]["list"]
        feature_variable = next(
            variable for variable in variables if variable["name"] == "feature"
        )
        self.assertIn(
            'not strings.hasPrefix(v: r._value, prefix: "mdragpoint_")',
            feature_variable["query"],
        )
