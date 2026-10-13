import json
from pathlib import Path
import unittest

from dasma_kafka.explainability.features import (
    build_viz_json_payload as build_v1_viz_json_payload,
)
from dasma_kafka.explainability_v2.features import (
    build_viz_json_payload as build_v2_viz_json_payload,
)
from validation.explainability.shap_diff import build_emission_records


ROOT = Path(__file__).resolve().parents[1]


class ExplainabilityShapDiffPipelineContractTest(unittest.TestCase):
    def test_real_v1_v2_json_viz_payloads_keep_transport_tags_out_of_shap_diff(self):
        common = {
            "timestamp": "2026-09-29 12:00:00",
            "Group": "Sortie CTA",
            "source_event_id": "event-001",
            "explained_model": "kitnet",
            "explanation_status": "computed",
            "shap_values": {"BHT": 0.125, "TFA": -0.25},
        }
        v1 = json.loads(build_v1_viz_json_payload(dict(common)))
        v2 = json.loads(build_v2_viz_json_payload(dict(common)))

        rows = build_emission_records(v1, v2)

        self.assertEqual([row.feature for row in rows], ["BHT", "TFA"])
        self.assertTrue(all(row.comparison_status == "complete" for row in rows))
        self.assertEqual([row.value_v1 for row in rows], [0.125, -0.25])
        self.assertEqual([row.value_v2 for row in rows], [0.125, -0.25])
        self.assertEqual([row.difference for row in rows], [0.0, 0.0])

    def test_compose_declares_dedicated_disabled_service_and_topic(self):
        compose = (ROOT / "docker-compose.yml").read_text()
        self.assertIn('TOPIC_NAME26: "explainability-shap-diff"', compose)
        self.assertIn("explainability-shap-diff:", compose)
        self.assertIn('DASMA_ENABLED: "${DASMA_SHAP_DIFF_ENABLED:-0}"', compose)
        self.assertIn("validation/explainability/shap_diff_service.py", compose)
        self.assertIn('ensure_no_retention "explainability-shap-diff"', compose)

    def test_campaign_orchestration_starts_service_for_bilateral_explainability(self):
        config = (ROOT / "scripts/web/campaign_config.py").read_text()
        launch = (ROOT / "scripts/web/launch_campaign.sh").read_text()
        self.assertIn("SHAP_DIFF_COMPOSE_SERVICE", config)
        self.assertIn('"DASMA_SHAP_DIFF_ENABLED"', config)
        self.assertIn("explainability-shap-diff", launch)

    def test_telegraf_uses_identity_and_feature_tags(self):
        telegraf = (ROOT / "observability/telegraf/telegraf.conf").read_text()
        block = telegraf.split('topics = ["explainability-shap-diff"]', 1)[1].split(
            '[[outputs.influxdb_v2]]', 1
        )[0]
        self.assertIn('consumer_group = "telegraf-explainability-shap-diff"', block)
        self.assertIn('json_string_fields = ["timestamp", "source_event_id", "feature", "comparison_status"]', block)
        self.assertIn('tag_keys = ["Group", "source_event_id", "feature"]', block)
        self.assertIn('name_override = "explainability-shap-diff"', block)
        self.assertIn('bucket = "explainability-shap-diff"', telegraf)

    def test_dashboard_keeps_precomputed_panels_and_removes_heavy_queries(self):
        dashboard = json.loads((ROOT / "observability/grafana/dashboards/Explainability/explainability_compare_influxdb.json").read_text())
        panels = {panel["id"]: panel for panel in dashboard["panels"]}
        self.assertEqual(panels[8]["title"], "SHAP values V1/V2 (precomputed)")
        self.assertEqual(panels[9]["title"], "SHAP difference V2 - V1 (precomputed)")
        self.assertIn('from(bucket: "explainability-shap-diff")', panels[8]["targets"][0]["query"])
        self.assertIn('r._field == "value_v1" or r._field == "value_v2"', panels[8]["targets"][0]["query"])
        self.assertIn('r._field == "difference"', panels[9]["targets"][0]["query"])
        self.assertIn('if "${feature}" == ""', panels[8]["targets"][0]["query"])
        self.assertNotIn("\\n", panels[8]["targets"][0]["query"])
        self.assertEqual(panels[8]["fieldConfig"]["defaults"]["custom"]["drawStyle"], "points")
        self.assertEqual(panels[8]["fieldConfig"]["defaults"]["custom"]["showPoints"], "always")
        self.assertNotIn(2, panels)
        self.assertNotIn(3, panels)
        positions = [
            (panel["gridPos"]["x"], panel["gridPos"]["y"], panel["gridPos"]["w"], panel["gridPos"]["h"])
            for panel in dashboard["panels"]
        ]
        for i, (x1, y1, w1, h1) in enumerate(positions):
            for x2, y2, w2, h2 in positions[i + 1:]:
                self.assertFalse(x1 < x2 + w2 and x2 < x1 + w1 and y1 < y2 + h2 and y2 < y1 + h1)


if __name__ == "__main__":
    unittest.main()
