from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
README_PATH = ROOT / "README.md"


class ReadmeContractsTest(TestCase):
    def setUp(self) -> None:
        self.readme = README_PATH.read_text(encoding="utf-8")

    def test_readme_lists_deepant_pipeline_surfaces(self) -> None:
        expected_fragments = [
            "- [`runtime/detectors/deepant/`](runtime/detectors/deepant)",
            "- `deepant-analysis`",
            "- `deepant-analysis-viz`",
            "-> deepant-analysis -> deepant-analysis + deepant-analysis-viz",
            "[`runtime/detectors/deepant/deepant_analysis.py`](runtime/detectors/deepant/deepant_analysis.py)",
            "[`observability/grafana/dashboards/By model/source-data_deepant_influxdb.json`](observability/grafana/dashboards/By%20model/source-data_deepant_influxdb.json)",
        ]

        for fragment in expected_fragments:
            self.assertIn(fragment, self.readme)

    def test_readme_lists_mdragstream_point_pipeline_surfaces(self) -> None:
        expected_fragments = [
            "- [`runtime/detectors/mdragpoint/`](runtime/detectors/mdragpoint)",
            "- `mdragstream-point-analysis`",
            "- `mdragstream-point-analysis-viz`",
            "-> mdragstream-point-analysis -> mdragstream-point-analysis + mdragstream-point-analysis-viz",
            "[`runtime/detectors/mdragpoint/mdragstream_point_analysis.py`](runtime/detectors/mdragpoint/mdragstream_point_analysis.py)",
            "[`runtime/detectors/mdragpoint/mdragpoint-for-beginners.md`](runtime/detectors/mdragpoint/mdragpoint-for-beginners.md)",
            "[`runtime/detectors/mdragpoint/mdragstream_point_analysis.md`](runtime/detectors/mdragpoint/mdragstream_point_analysis.md)",
            "[`observability/grafana/dashboards/By model/source-data_mdragpoint_influxdb.json`](observability/grafana/dashboards/By%20model/source-data_mdragpoint_influxdb.json)",
            "mDragStream's native Point-based anomaly flag",
            "intentionally do not display `mdragpoint_threshold` or `mdragpoint_internal_r` by default",
        ]

        for fragment in expected_fragments:
            self.assertIn(fragment, self.readme)

    def test_readme_describes_consensus_as_event_time_join(self) -> None:
        self.assertIn("joins active model verdicts per `(Group, source_event_id)`", self.readme)
        self.assertIn("keeps `timestamp` for Grafana display", self.readme)
        self.assertIn("timeout/watermark", self.readme)
        self.assertNotIn("latest known verdict per active model and group", self.readme)

    def test_readme_describes_repeatability_runtime_and_dashboard(self) -> None:
        expected_fragments = [
            "## Repeatability (local phase 1)",
            "`repeatability-v1`,",
            "`repeatability-v2`,",
            "`repeatability-diff`",
            "`consensus-analysis` are disabled",
            "`hstree-analysis` topic",
            "Global anomaly score per event — v1 vs v2",
            "`identical = 1` and `abs_diff = 0`",
            "Every standard parameter exposed for the selected model",
            "fixed for a run",
            "[`observability/grafana/dashboards/repeatability_influxdb.json`](observability/grafana/dashboards/repeatability_influxdb.json)",
        ]

        for fragment in expected_fragments:
            self.assertIn(fragment, self.readme)

    def test_mdragstream_point_docs_describe_native_and_external_verdicts(self) -> None:
        beginners = (
            ROOT / "runtime" / "detectors" / "mdragpoint" / "mdragpoint-for-beginners.md"
        ).read_text(encoding="utf-8")
        technical = (
            ROOT / "runtime" / "detectors" / "mdragpoint" / "mdragstream_point_analysis.md"
        ).read_text(encoding="utf-8")

        self.assertIn("MDRAGPOINT_VERDICT_MODE=internal", beginners)
        self.assertIn("MDRAGPOINT_VERDICT_MODE=external", beginners)
        self.assertIn("Native Verdict Mode", technical)
        self.assertIn("External Verdict Mode", technical)
        self.assertIn("B2", technical)
        self.assertIn("B7", technical)
