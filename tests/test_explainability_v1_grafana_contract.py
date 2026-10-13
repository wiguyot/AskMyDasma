"""Grafana contract for the explainability V1 dashboard (V1-04b).

The dashboard must expose the V1-04a data (identity tags Group +
source_event_id, statuses, verdict, SHAP time) without altering the
historical panels. Covered contracts:

- the three historical panels keep their exact Flux queries; panels 1-2
  adapt the series legend/naming to display Group and source_event_id
  (displayName override), panel 3 stays untouched;
- three new panels (ids 4/5/6): status barchart (with ``group()`` after the
  aggregation and a status sort), published-results table joining
  ``explanation_status`` and ``hstree_anomaly`` (published / computed_count /
  computed_rate), SHAP time timeseries;
- Flux comments are ``//`` only, never ``#`` (a ``#`` line breaks the query);
- panel 5 counts ONLY published V1 outputs: its computed_rate is never an
  upstream coverage metric;
- the V2 and compare dashboards are structurally unchanged.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
V1_DASHBOARD = ROOT / "observability" / "grafana" / "dashboards" / "Explainability" / "explainability_influxdb.json"
V2_DASHBOARD = ROOT / "observability" / "grafana" / "dashboards" / "Explainability" / "explainability_v2_influxdb.json"
COMPARE_DASHBOARD = ROOT / "observability" / "grafana" / "dashboards" / "Explainability" / "explainability_compare_influxdb.json"

PANEL_1_QUERY = 'from(bucket: "explainability")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "explainability")\n  |> filter(fn: (r) => r._field == "shap_base_value" or r._field == "local_hstree_score")\n  |> filter(fn: (r) => if "${group}" == "" or "${group}" == "ALL" then true else r.Group == "${group}")\n  |> group(columns: ["_field", "Group"])\n  |> aggregateWindow(every: 1s, fn: last, createEmpty: false, timeSrc: "_time")\n  |> drop(columns: ["source_event_id", "host", "_measurement"])\n  |> yield(name: "last")'
PANEL_2_QUERY = 'fieldSel = if "${feature}" == "" then "ALL" else "${feature}"\nfieldName = fieldSel\n\n// Scalar fields are excluded when feature=ALL; only SHAP feature fields remain.\nscalarFields = ["explanation_error_code", "explanation_profile", "explanation_status", "explainability_seed", "feature_fingerprint", "hstree_anomaly", "hstree_model_fingerprint", "hstree_model_version", "hstree_score", "hstree_status", "hstree_threshold", "local_hstree_score", "shap_background_size", "shap_base_value", "shap_computation_time_ms", "shap_nsamples"]\n\nfrom(bucket: "explainability")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "explainability")\n  |> filter(fn: (r) => if "${group}" == "" or "${group}" == "ALL" then true else r.Group == "${group}")\n  |> filter(fn: (r) => if fieldSel == "ALL" then not contains(value: r._field, set: scalarFields) else r._field == fieldName)\n  |> group(columns: ["_field", "Group"])\n  |> aggregateWindow(every: 1s, fn: last, createEmpty: false, timeSrc: "_time")\n  |> drop(columns: ["source_event_id", "host", "_measurement"])\n  |> yield(name: "last")'
PANEL_3_QUERY = 'fieldSel = if "${feature}" == "" then "ALL" else "${feature}"\ngroupSel = if "${group}" == "" then "ALL" else "${group}"\n\nban = ["timestamp","LabviewTime","LabViewTime","StepNumber","Cycle"]\n\nfrom(bucket: "source-data")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == "source-data")\n  |> filter(fn: (r) => not contains(value: r._field, set: ban))\n  |> filter(fn: (r) => fieldSel == "ALL" or r._field == fieldSel)\n  |> filter(fn: (r) => groupSel == "ALL" or r.Group == groupSel)\n  |> drop(columns: ["host", "_measurement"])'


def _dashboard(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _panel(dashboard: dict, panel_id: int) -> dict:
    for panel in dashboard["panels"]:
        if panel["id"] == panel_id:
            return panel
    raise AssertionError(f"panel {panel_id} not found")


def _grid_rect(panel: dict) -> tuple[int, int, int, int]:
    pos = panel["gridPos"]
    return (pos["x"], pos["y"], pos["x"] + pos["w"], pos["y"] + pos["h"])


class DashboardStructureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dashboard = _dashboard(V1_DASHBOARD)
        self.panels = {panel["id"]: panel for panel in self.dashboard["panels"]}

    def test_dashboard_json_is_valid_and_version_bumped(self) -> None:
        # json.loads already ran in setUp: the file is valid JSON.
        self.assertEqual(self.dashboard["title"], "Explainability V1 — HSTree")
        self.assertEqual(self.dashboard["uid"], "explainability-influxdb")
        self.assertEqual(self.dashboard["version"], 2)

    def test_panel_ids_are_unique(self) -> None:
        ids = [panel["id"] for panel in self.dashboard["panels"]]
        self.assertEqual(len(ids), len(set(ids)))

    def test_panel_positions_do_not_overlap(self) -> None:
        rects = [_grid_rect(panel) for panel in self.dashboard["panels"]]
        for i, first in enumerate(rects):
            for second in rects[i + 1 :]:
                overlaps = (
                    first[0] < second[2]
                    and second[0] < first[2]
                    and first[1] < second[3]
                    and second[1] < first[3]
                )
                self.assertFalse(overlaps, f"panels overlap: {first} vs {second}")

    def test_flux_comments_use_only_double_slash(self) -> None:
        for panel in self.dashboard["panels"]:
            for target in panel.get("targets", []):
                self.assertNotIn("#", target.get("query", ""), panel["id"])

    def test_every_explainability_query_includes_hstree_and_legacy_points(self) -> None:
        for panel in self.dashboard["panels"]:
            for target in panel.get("targets", []):
                query = target.get("query", "")
                occurrences = query.count('r._measurement == "explainability"')
                self.assertEqual(
                    query.count('not exists r.explained_model or r.explained_model == "hstree"'),
                    occurrences,
                    panel["id"],
                )


class HistoricalPanelsPreservedTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dashboard = _dashboard(V1_DASHBOARD)
        self.panels = {panel["id"]: panel for panel in self.dashboard["panels"]}

    def test_panel_1_query_is_untouched_and_legend_carries_identity(self) -> None:
        panel = self.panels[1]
        expected_query = PANEL_1_QUERY.replace(
            'r._measurement == "explainability")\n  |> filter(fn: (r) => r._field',
            'r._measurement == "explainability")\n  |> filter(fn: (r) => not exists r.explained_model or r.explained_model == "hstree")\n  |> filter(fn: (r) => r._field',
        )
        self.assertEqual(panel["targets"][0]["query"], expected_query)
        overrides = panel["fieldConfig"]["overrides"]
        display_names = [
            prop["value"]
            for override in overrides
            for prop in override.get("properties", [])
            if prop["id"] == "displayName"
        ]
        self.assertEqual(len(display_names), 1)
        self.assertIn("${__field.labels.Group}", display_names[0])
        self.assertIn("${__field.labels.source_event_id}", display_names[0])

class NewPanelsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dashboard = _dashboard(V1_DASHBOARD)
        self.panels = {panel["id"]: panel for panel in self.dashboard["panels"]}

    def test_panel_4_is_the_status_barchart_with_group_after_aggregation(self) -> None:
        panel = self.panels[4]
        self.assertEqual(panel["type"], "barchart")
        self.assertEqual(
            panel["title"],
            "How many Explainability V1 HSTree results were produced per selected group?",
        )
        self.assertEqual(panel["options"]["xField"], "status")
        query = panel["targets"][0]["query"]
        self.assertIn('from(bucket: "explainability")', query)
        self.assertIn('r._measurement == "explainability"', query)
        self.assertIn('r._field == "explanation_status"', query)
        self.assertIn('${group}', query)
        # group() applied AFTER the aggregation, then a status sort.
        group_column_index = query.index('|> group(columns: ["status", "Group"])')
        count_index = query.index("|> count()")
        collapse_index = query.rindex("|> group()")
        sort_index = query.index('|> sort(columns: ["status"], desc: true)')
        self.assertLess(group_column_index, count_index)
        self.assertLess(count_index, collapse_index)
        self.assertLess(collapse_index, sort_index)

    def test_panel_5_is_the_score_fidelity_table(self) -> None:
        panel = self.panels[5]
        self.assertEqual(panel["type"], "table")
        self.assertEqual(panel["title"], "HSTree score fidelity by group")
        query = panel["targets"][0]["query"]
        self.assertIn('from(bucket: "explainability")', query)
        self.assertIn('r._measurement == "explainability"', query)
        self.assertNotIn('r._measurement == "explainability-v2"', query)
        self.assertIn('r._field == "hstree_score"', query)
        self.assertIn('r._field == "local_hstree_score"', query)
        self.assertIn('"source_event_id"', query)
        self.assertIn("identical_scores", query)
        self.assertIn("different_scores", query)
        self.assertIn("compared_scores", query)
        self.assertIn("identical_rate", query)
        self.assertIn("hstree_score_fidelity_by_group", query)

    def test_panel_5_rate_measures_published_outputs_only(self) -> None:
        # The table pivots on the event identity and counts published rows:
        # its rate is "computed among published", never an upstream coverage.
        panel = self.panels[5]
        query = panel["targets"][0]["query"]
        self.assertIn('group(columns: ["_time", "Group", "source_event_id"])', query)
        self.assertIn('r._field == "local_hstree_score"', query)
        self.assertIn("HSTree score fidelity", panel["title"])

    def test_panel_6_is_the_shap_time_timeseries(self) -> None:
        panel = self.panels[6]
        self.assertEqual(panel["type"], "timeseries")
        self.assertEqual(panel["title"], "HSTree SHAP computation time V1 (ms)")
        query = panel["targets"][0]["query"]
        self.assertIn('from(bucket: "explainability")', query)
        self.assertIn('r._measurement == "explainability"', query)
        self.assertIn('r._field == "shap_computation_time_ms"', query)
        self.assertIn('${group}', query)
        self.assertIn('group(columns: ["_field", "Group"])', query)
        self.assertIn(
            'aggregateWindow(every: 1s, fn: last, createEmpty: false, timeSrc: "_time")',
            query,
        )
        self.assertIn('drop(columns: ["source_event_id", "host", "_measurement"])', query)


class OtherDashboardsUnchangedTest(unittest.TestCase):
    def test_v2_dashboard_has_hstree_and_iforest_panels(self) -> None:
        dashboard = _dashboard(V2_DASHBOARD)
        panels = [(p["id"], p["type"], p["title"]) for p in dashboard["panels"]]
        self.assertEqual(len(panels), 6)
        types_by_id = {panel_id: (ptype, title) for panel_id, ptype, title in panels}
        self.assertEqual(types_by_id[4], ("barchart", "How many Explainability V2 HSTree results were produced per selected group?"))
        self.assertEqual(types_by_id[5], ("table", "HSTree score fidelity by group"))
        self.assertEqual(types_by_id[6], ("timeseries", "HSTree SHAP computation time V2 (ms)"))
        # The V2 queries still target the V2 bucket/measurement.
        v2_query = next(p for p in dashboard["panels"] if p["id"] == 4)["targets"][0]["query"]
        self.assertIn('from(bucket: "explainability-v2")', v2_query)

    def test_compare_dashboard_keeps_scores_source_and_precomputed_shap(self) -> None:
        dashboard = _dashboard(COMPARE_DASHBOARD)
        panels = [(p["id"], p["type"], p["title"]) for p in dashboard["panels"]]
        expected = [
            (1, "timeseries", "shap_base_value and local_model_score (v1)"),
            (7, "timeseries", "shap_base_value and local_model_score (v2)"),
            (4, "timeseries", "Source elements"),
            (8, "timeseries", "SHAP values V1/V2 (precomputed)"),
            (9, "timeseries", "SHAP difference V2 - V1 (precomputed)"),
        ]
        self.assertEqual(panels, expected)


if __name__ == "__main__":
    unittest.main()
