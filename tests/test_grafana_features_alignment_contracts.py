import json
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_PATHS = [
    ROOT / "observability" / "grafana" / "dashboards" / "By model" / "source-data_hstree_influxdb.json",
    ROOT / "observability" / "grafana" / "dashboards" / "By model" / "source-data_iforest_influxdb.json",
    ROOT / "observability" / "grafana" / "dashboards" / "By model" / "source-data_kitnet_influxdb.json",
    ROOT / "observability" / "grafana" / "dashboards" / "By model" / "source-data_deepant_influxdb.json",
    ROOT / "observability" / "grafana" / "dashboards" / "By model" / "source-data_mdragpoint_influxdb.json",
]


class GrafanaFeaturesAlignmentContractsTest(TestCase):
    def test_feature_model_timeseries_panels_share_y_axis_width(self) -> None:
        for path in DASHBOARD_PATHS:
            with self.subTest(dashboard=path.name):
                dashboard = json.loads(path.read_text(encoding="utf-8"))
                timeseries_panels = [
                    panel
                    for panel in dashboard["panels"]
                    if panel.get("type") == "timeseries"
                ]

                self.assertGreaterEqual(len(timeseries_panels), 2)
                for panel in timeseries_panels:
                    custom = panel["fieldConfig"]["defaults"]["custom"]
                    self.assertEqual(
                        custom.get("axisWidth"),
                        96,
                        f"{path.name} / {panel.get('title')} must keep plot areas aligned",
                    )
