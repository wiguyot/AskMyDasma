import json
from pathlib import Path
from unittest import TestCase


DASHBOARD_PATH = (
    Path(__file__).resolve().parents[1]
    / "observability" / "grafana"
    / "dashboards"
    / "anomaly_analysis_overview_influxdb.json"
)


class GrafanaAnomalyOverviewDashboardContractsTest(TestCase):
    def setUp(self) -> None:
        with DASHBOARD_PATH.open(encoding="utf-8") as fh:
            self.dashboard = json.load(fh)

    def _panel(self, title: str) -> dict:
        for panel in self.dashboard["panels"]:
            if panel.get("title") == title:
                return panel
        raise AssertionError(f"Panel not found: {title}")

    def test_dashboard_has_one_group_variable_and_five_stacked_panels_plus_consensus_panels(self) -> None:
        self.assertEqual(self.dashboard["uid"], "anomaly-analysis-overview-influxdb")
        self.assertEqual(self.dashboard["title"], "Anomaly Analysis Overview")
        self.assertEqual(self.dashboard["time"], {"from": "now-30m", "to": "now"})
        self.assertEqual(self.dashboard["refresh"], "5m")

        variables = self.dashboard["templating"]["list"]
        self.assertEqual([variable["name"] for variable in variables], ["group"])
        self.assertEqual(variables[0]["label"], "Group")
        self.assertIn('tag: "Group"', variables[0]["query"])
        self.assertIn('bucket: "source-data"', variables[0]["query"])

        panels = self.dashboard["panels"]
        self.assertEqual(
            [(panel["title"], panel["gridPos"]) for panel in panels],
            [
                ("HSTree Analysis", {"h": 8, "w": 24, "x": 0, "y": 0}),
                ("IForest Analysis", {"h": 8, "w": 24, "x": 0, "y": 8}),
                ("KitNET Analysis", {"h": 8, "w": 24, "x": 0, "y": 16}),
                ("DeepAnT Analysis", {"h": 8, "w": 24, "x": 0, "y": 24}),
                ("mDragStream Point-based Analysis", {"h": 8, "w": 24, "x": 0, "y": 32}),
                ("Consensus Strength (max models/event)", {"h": 8, "w": 24, "x": 0, "y": 40}),
                ("Consensus by Model (1/model/window)", {"h": 8, "w": 24, "x": 0, "y": 48}),
            ],
        )

    def test_hstree_panel_uses_score_threshold_and_anomaly_markers(self) -> None:
        panel = self._panel("HSTree Analysis")
        query = panel["targets"][0]["query"]

        self.assertIn('from(bucket: "hstree-analysis")', query)
        self.assertIn('r._field == "hstree_score"', query)
        self.assertIn('r._field == "hstree_threshold"', query)
        self.assertIn('r.hstree_status == "anomaly"', query)
        self.assertIn("hstree_anomaly_marker", query)
        self.assertIn('r.Group == groupSel', query)

    def test_iforest_panel_uses_score_threshold_and_anomaly_markers(self) -> None:
        panel = self._panel("IForest Analysis")
        query = panel["targets"][0]["query"]

        self.assertIn('from(bucket: "iforest-analysis")', query)
        self.assertIn('r._field == "iforest_score"', query)
        self.assertIn('r._field == "iforest_threshold"', query)
        self.assertIn('r.iforest_status == "anomaly"', query)
        self.assertIn("iforest_anomaly_marker", query)
        self.assertIn('r.Group == groupSel', query)

    def test_kitnet_panel_uses_score_threshold_and_anomaly_markers(self) -> None:
        panel = self._panel("KitNET Analysis")
        query = panel["targets"][0]["query"]

        self.assertIn('from(bucket: "kitnet-analysis")', query)
        self.assertIn('r._field == "kitnet_score"', query)
        self.assertIn('r._field == "kitnet_threshold"', query)
        self.assertIn('r.kitnet_status == "anomaly"', query)
        self.assertIn("kitnet_anomaly_marker", query)
        self.assertIn("if float(v: r._value) > 5.0 then 5.0", query)
        self.assertIn('r.Group == groupSel', query)
        self.assertFalse(panel["options"]["legend"]["showLegend"])
        self.assertEqual(panel["options"]["tooltip"]["mode"], "single")

    def test_deepant_panel_uses_score_threshold_and_anomaly_markers(self) -> None:
        panel = self._panel("DeepAnT Analysis")
        query = panel["targets"][0]["query"]

        self.assertIn('from(bucket: "deepant-analysis")', query)
        self.assertIn('r._field == "deepant_score"', query)
        self.assertIn('r._field == "deepant_threshold"', query)
        self.assertIn('r.deepant_status == "anomaly"', query)
        self.assertIn("deepant_anomaly_marker", query)
        self.assertIn('r.Group == groupSel', query)

    def test_mdragpoint_panel_uses_score_and_anomaly_markers(self) -> None:
        panel = self._panel("mDragStream Point-based Analysis")
        query = panel["targets"][0]["query"]

        self.assertIn('from(bucket: "mdragstream-point-analysis")', query)
        self.assertIn('r._field == "mdragpoint_score"', query)
        self.assertIn('import "math"', query)
        self.assertIn("math.log10(x: float(v: r._value) + 1.0)", query)
        self.assertIn("mdragpoint_score_log10", query)
        self.assertNotIn('r._field == "mdragpoint_internal_r"', query)
        self.assertNotIn('r._field == "mdragpoint_threshold"', query)
        self.assertIn('r.mdragpoint_status == "anomaly"', query)
        self.assertIn("mdragpoint_anomaly_marker_log10", query)
        self.assertIn('r.Group == groupSel', query)

    def test_consensus_strength_panel_reads_canonical_anomaly_count_as_window_max(self) -> None:
        panel = self._panel("Consensus Strength (max models/event)")
        query = panel["targets"][0]["query"]

        self.assertIn('from(bucket: "consensus-analysis")', query)
        self.assertIn('r._measurement == "consensus-analysis"', query)
        self.assertIn('r._field == "anomaly_count"', query)
        self.assertIn('r.Group == groupSel', query)
        self.assertIn('group(columns: ["Group"])', query)
        self.assertIn("aggregateWindow(every: v.windowPeriod, fn: max", query)
        self.assertIn("Consensus strength", query)
        self.assertIn("same source_event_id", query)
        self.assertIn("not a volume counter", query)
        self.assertIn("no implicit global aggregation", query)
        self.assertNotIn('from(bucket: "hstree-analysis")', query)
        self.assertNotIn('from(bucket: "iforest-analysis")', query)
        self.assertNotIn('from(bucket: "kitnet-analysis")', query)
        self.assertNotIn('from(bucket: "deepant-analysis")', query)
        self.assertNotIn('from(bucket: "mdragstream-point-analysis")', query)
        self.assertNotIn('r.hstree_status == "anomaly"', query)
        self.assertNotIn('r.iforest_status == "anomaly"', query)
        self.assertNotIn('r.kitnet_status == "anomaly"', query)
        self.assertNotIn('r.deepant_status == "anomaly"', query)
        self.assertNotIn('r.mdragpoint_status == "anomaly"', query)
        self.assertNotIn("union(tables:", query)
        self.assertNotIn("pivot(", query)
        self.assertEqual(panel["fieldConfig"]["defaults"]["max"], 5)

    def test_consensus_by_model_panel_stacks_per_model_window_presence(self) -> None:
        panel = self._panel("Consensus by Model (1/model/window)")
        query = panel["targets"][0]["query"]

        # Reads the numeric *_vote fields from the consensus bucket (booleans are
        # not persisted by Telegraf), not the per-model analysis buckets.
        self.assertIn('from(bucket: "consensus-analysis")', query)
        self.assertIn('r._measurement == "consensus-analysis"', query)
        self.assertIn('r._field =~ /^(hstree|iforest|kitnet|deepant|mdragpoint)_vote$/', query)
        self.assertNotIn('r._field == "anomaly_count"', query)
        self.assertNotIn('from(bucket: "hstree-analysis")', query)
        # Stacked by model after windowing per Group/model; the cross-group sum
        # is direct by _time/_field to avoid a second aggregateWindow pass.
        self.assertEqual(
            panel["fieldConfig"]["defaults"]["custom"]["stacking"]["mode"], "normal"
        )
        self.assertEqual(panel["fieldConfig"]["defaults"]["custom"]["drawStyle"], "bars")
        self.assertIn('keep(columns: ["_time", "_field", "_value", "Group"])', query)
        self.assertIn('group(columns: ["Group", "_field"])', query)
        self.assertIn("aggregateWindow(every: v.windowPeriod, fn: max", query)
        self.assertIn('group(columns: ["_time", "_field"])', query)
        self.assertIn('sum(column: "_value")', query)
        self.assertIn('group(columns: ["_field"])', query)
        self.assertIn("Per-model consensus volume", query)
        self.assertIn("one unit per model", query)
        self.assertIn("not an", query)
        self.assertIn("event counter", query)
        self.assertEqual(query.count("aggregateWindow("), 1)
        self.assertEqual(panel["fieldConfig"]["defaults"]["max"], 5)
        self.assertTrue(panel["options"]["legend"]["showLegend"])

    def test_all_panels_share_the_same_visual_encoding(self) -> None:
        expected = {
            "score": "yellow",
            "threshold": "blue",
            "anomaly_marker": "red",
        }

        for title, prefix in [
            ("HSTree Analysis", "hstree"),
            ("IForest Analysis", "iforest"),
            ("KitNET Analysis", "kitnet"),
            ("DeepAnT Analysis", "deepant"),
            ("mDragStream Point-based Analysis", "mdragpoint"),
        ]:
            panel = self._panel(title)
            overrides = panel["fieldConfig"]["overrides"]
            by_name = {
                override.get("matcher", {}).get("options"): {
                    prop["id"]: prop["value"]
                    for prop in override.get("properties", [])
                }
                for override in overrides
            }

            if prefix == "mdragpoint":
                self.assertEqual(
                    by_name["mdragpoint_score_log10"]["color"]["fixedColor"],
                    expected["score"],
                )
                self.assertNotIn("mdragpoint_threshold", by_name)
                self.assertNotIn("mdragpoint_internal_r", by_name)
                marker = by_name["mdragpoint_anomaly_marker_log10"]
                self.assertEqual(
                    marker["displayName"],
                    'mDragStream Point-based native anomaly log10(score + 1) group="${__field.labels.Group}"',
                )
            else:
                self.assertEqual(by_name[f"{prefix}_score"]["color"]["fixedColor"], expected["score"])
                self.assertEqual(by_name[f"{prefix}_threshold"]["color"]["fixedColor"], expected["threshold"])
                self.assertEqual(by_name[f"{prefix}_threshold"]["custom.lineWidth"], 3)
                marker = by_name[f"{prefix}_anomaly_marker"]
            self.assertEqual(marker["color"]["fixedColor"], expected["anomaly_marker"])
            self.assertEqual(marker["custom.drawStyle"], "points")
            self.assertEqual(marker["custom.showPoints"], "always")

    def test_all_timeseries_panels_share_y_axis_width_for_time_alignment(self) -> None:
        for panel in self.dashboard["panels"]:
            if panel.get("type") != "timeseries":
                continue

            with self.subTest(panel=panel.get("title")):
                custom = panel["fieldConfig"]["defaults"]["custom"]
                self.assertEqual(custom.get("axisWidth"), 60)
                self.assertEqual(custom.get("axisLabel"), "")
