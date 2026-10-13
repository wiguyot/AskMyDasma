import json
from pathlib import Path
from unittest import TestCase


DASHBOARD_PATH = (
    Path(__file__).resolve().parents[1]
    / "observability" / "grafana"
    / "dashboards"
    / "hstree_metrics.json"
)


class GrafanaMetricsDashboardContractsTest(TestCase):
    def setUp(self) -> None:
        with DASHBOARD_PATH.open(encoding="utf-8") as fh:
            self.dashboard = json.load(fh)

    def test_dashboard_title_and_status_heading(self) -> None:
        self.assertEqual(self.dashboard["title"], "topics metrics")

        heading = next(
            panel
            for panel in self.dashboard["panels"]
            if panel["options"].get("content")
            == "Status of message production of the topics"
        )
        records = next(
            panel for panel in self.dashboard["panels"] if panel["title"] == "Records"
        )

        self.assertEqual(heading["type"], "text")
        self.assertEqual(heading["title"], "")
        self.assertEqual(
            heading["options"]["content"],
            "Status of message production of the topics",
        )
        self.assertEqual(heading["gridPos"]["y"], 0)
        self.assertGreater(records["gridPos"]["y"], heading["gridPos"]["y"])

    def test_records_panel_includes_deepant_progress(self) -> None:
        panel = next(
            panel for panel in self.dashboard["panels"] if panel["title"] == "Records"
        )
        organize = next(
            item for item in panel["transformations"] if item["id"] == "organize"
        )
        options = organize["options"]

        self.assertTrue(options["includeByName"]["deepant-analysis"])
        self.assertTrue(options["excludeByName"]["deepant-analysis-viz"])
        self.assertEqual(
            options["renameByName"]["deepant-analysis"],
            "DeepAnT Analysis",
        )

    def test_records_panel_includes_v2_and_mdragpoint_progress(self) -> None:
        panel = next(
            panel for panel in self.dashboard["panels"] if panel["title"] == "Records"
        )
        organize = next(
            item for item in panel["transformations"] if item["id"] == "organize"
        )
        options = organize["options"]

        self.assertTrue(options["includeByName"]["explainability-v2"])
        self.assertTrue(options["includeByName"]["mdragstream-point-analysis"])
        self.assertTrue(options["excludeByName"]["explainability-v2-viz"])
        self.assertTrue(options["excludeByName"]["mdragstream-point-analysis-viz"])
        self.assertEqual(
            options["renameByName"]["explainability-v2"],
            "Explainability v2",
        )
        self.assertEqual(
            options["renameByName"]["mdragstream-point-analysis"],
            "mDragStream Point Based",
        )
