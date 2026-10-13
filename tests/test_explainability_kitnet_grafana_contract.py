"""Contract tests for the dedicated KitNET explainability dashboards."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = ROOT / "observability/grafana/dashboards/Explainability"
DASHBOARDS = (
    (DASHBOARD_DIR / "explainability_kitnet_influxdb.json", "explainability-kitnet-influxdb", "explainability"),
    (DASHBOARD_DIR / "explainability_v2_kitnet_influxdb.json", "explainability-v2-kitnet-influxdb", "explainability-v2"),
)


class ExplainabilityKitNETDashboardContractTest(TestCase):
    def test_dashboard_identity_and_six_panels(self) -> None:
        for path, uid, _measurement in DASHBOARDS:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(dashboard["uid"], uid)
            self.assertIn("KitNET", dashboard["title"])
            self.assertEqual({panel["id"] for panel in dashboard["panels"]}, {1, 2, 3, 4, 5, 6})
            self.assertTrue(all("KitNET" in panel["title"] for panel in dashboard["panels"]))

    def test_every_explainability_measurement_query_filters_kitnet(self) -> None:
        for path, _uid, measurement in DASHBOARDS:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            for panel in dashboard["panels"]:
                for target in panel.get("targets", []):
                    query = target.get("query", "")
                    occurrences = query.count(f'r._measurement == "{measurement}"')
                    self.assertEqual(
                        query.count('r.explained_model == "kitnet"'),
                        occurrences,
                        f"{path.name} panel {panel['id']}",
                    )

    def test_shap_panel_selects_kitnet_event_metadata(self) -> None:
        for path, _uid, _measurement in DASHBOARDS:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
            query = dashboard["panels"][1]["targets"][0]["query"]
            self.assertIn('r.explained_model == "kitnet"', query)
            self.assertIn('not contains(value: r._field, set: scalarFields)', query)
            self.assertIn('aggregateWindow(every: 1s, fn: last, createEmpty: false, timeSrc: "_time")', query)
            self.assertNotIn("model_stream", query)
            self.assertNotIn("join(", query)
