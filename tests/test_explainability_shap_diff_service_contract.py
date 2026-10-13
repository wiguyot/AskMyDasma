from pathlib import Path
import unittest


SERVICE = Path(__file__).resolve().parents[1] / "validation/explainability/shap_diff_service.py"


class ShapDiffServiceContractTest(unittest.TestCase):
    def test_service_has_dedicated_topics_and_flat_output(self):
        source = SERVICE.read_text()
        self.assertIn("SHAP_DIFF_V1_TOPIC", source)
        self.assertIn("SHAP_DIFF_V2_TOPIC", source)
        self.assertIn("SHAP_DIFF_DEST_TOPIC", source)
        self.assertIn("build_emission_records", source)
        self.assertIn("ensure_ascii=False", source)

    def test_service_is_disabled_by_default(self):
        source = SERVICE.read_text()
        self.assertIn('if _env("DASMA_ENABLED", "0") == "0"', source)

    def test_service_does_not_touch_influx_or_docker(self):
        source = SERVICE.read_text()
        self.assertNotIn("influx", source.lower())
        self.assertNotIn("docker", source.lower())

    def test_service_commits_only_after_both_sides_and_delivery_drain(self):
        source = SERVICE.read_text()
        self.assertIn('"enable.auto.commit": False', source)
        self.assertIn("setdefault(side, []).append(message)", source)
        self.assertIn("producer.flush(30)", source)
        self.assertIn("pending_messages.pop(key)", source)


if __name__ == "__main__":
    unittest.main()
