"""Contracts for the iForest panels added to the V1 and V2 dashboards."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = (
    ROOT / "observability/grafana/dashboards/Explainability/explainability_iforest_influxdb.json",
    ROOT / "observability/grafana/dashboards/Explainability/explainability_v2_iforest_influxdb.json",
)
ALL_MODEL_DASHBOARDS = (
    ROOT / "observability/grafana/dashboards/Explainability/explainability_influxdb.json",
    ROOT / "observability/grafana/dashboards/Explainability/explainability_iforest_influxdb.json",
    ROOT / "observability/grafana/dashboards/Explainability/explainability_v2_influxdb.json",
    ROOT / "observability/grafana/dashboards/Explainability/explainability_v2_iforest_influxdb.json",
)


class ExplainabilityIForestDashboardContractTest(TestCase):
    def test_each_dashboard_is_an_iforest_only_dashboard(self) -> None:
        for path in DASHBOARDS:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            panels = {panel["id"]: panel for panel in dashboard["panels"]}
            self.assertEqual(set(panels), {1, 2, 3, 4, 5, 6})
            self.assertIn("iForest", dashboard["title"])
            self.assertTrue(all("iForest" in panel["title"] for panel in panels.values()))

    def test_iforest_explained_value_panel_uses_declared_scale(self) -> None:
        for path in DASHBOARDS:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            query = dashboard["panels"][0]["targets"][0]["query"]
            self.assertIn('r._field == "explained_value"', query)
            self.assertIn('r._field == "explanation_output"', query)
            self.assertIn('on: ["_time", "Group", "source_event_id"]', query)
            self.assertNotIn('r._field == "local_hstree_score"', query)

    def test_every_explainability_query_filters_iforest_points(self) -> None:
        for path in DASHBOARDS:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            for panel in dashboard["panels"]:
                for target in panel.get("targets", []):
                    query = target.get("query", "")
                    occurrences = query.count('r._measurement == "explainability')
                    self.assertEqual(
                        query.count('r.explained_model == "iforest"'),
                        occurrences,
                        f"{path.name} panel {panel['id']}",
                    )

    def test_iforest_feature_panel_is_scoped_to_iforest_events(self) -> None:
        for path in DASHBOARDS:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            query = dashboard["panels"][1]["targets"][0]["query"]
            self.assertIn('r.explained_model == "iforest"', query)
            self.assertIn('not contains(value: r._field, set: scalarFields)', query)
            self.assertIn('group(columns: ["_field", "Group"])', query)

    def test_all_model_feature_panels_scope_shap_values_to_the_dashboard_model(self) -> None:
        expected_models = {
            "explainability_influxdb.json": "hstree",
            "explainability_iforest_influxdb.json": "iforest",
            "explainability_v2_influxdb.json": "hstree",
            "explainability_v2_iforest_influxdb.json": "iforest",
        }
        for path in ALL_MODEL_DASHBOARDS:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            query = dashboard["panels"][1]["targets"][0]["query"]
            model = expected_models[path.name]
            self.assertIn(f'r.explained_model == "{model}"', query)
            self.assertIn('not contains(value: r._field, set: scalarFields)', query)
            self.assertIn('group(columns: ["_field", "Group"])', query)

    def test_iforest_fidelity_compares_reference_and_local_scores(self) -> None:
        for path in DASHBOARDS:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            query = dashboard["panels"][4]["targets"][0]["query"]
            self.assertIn('r._field == "reference_score"', query)
            self.assertIn('r._field == "local_score"', query)
            self.assertIn("exists r.reference_score and exists r.local_score", query)
            self.assertIn("iforest_score_fidelity_by_group", query)
            self.assertNotIn("local_hstree_score", query)
