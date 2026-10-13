from pathlib import Path
from unittest import TestCase

from scripts.web.campaign_config import (
    ALL_RUNTIME_COMPOSE_SERVICES,
    RESET_SCHEMA_SUBJECTS,
    RESET_TOPICS,
)


ROOT = Path(__file__).resolve().parents[1]
START_PRODUCTION = ROOT / "scripts" / "web" / "start_production.sh"
LAUNCH_CAMPAIGN = ROOT / "scripts" / "web" / "launch_campaign.sh"
RESET_CAMPAIGN = ROOT / "scripts" / "web" / "reset_campaign.sh"
STOP_CAMPAIGN = ROOT / "scripts" / "web" / "stop_campaign.sh"
CAMPAIGN_CONFIG = ROOT / "scripts" / "web" / "campaign_config.py"
COMPOSE = ROOT / "docker-compose.yml"


class MDragPointScriptContractsTest(TestCase):
    def test_start_production_enables_mdragpoint_only_when_selected(self) -> None:
        script = CAMPAIGN_CONFIG.read_text(encoding="utf-8")

        self.assertIn('"DASMA_MDRAGPOINT_ENABLED": "1" if model_enabled(config, "mdragpoint") else "0"', script)
        self.assertIn('"mdragpoint"', script)

    def test_start_production_starts_mdragpoint_service(self) -> None:
        script = START_PRODUCTION.read_text(encoding="utf-8")
        config = CAMPAIGN_CONFIG.read_text(encoding="utf-8")

        self.assertIn('"mdragpoint": "mdragstream-point-analysis"', config)
        self.assertIn("DASMA_COMPOSE_SERVICES", script)
        self.assertIn('"${COMPOSE_SERVICES[@]}"', script)

    def test_compose_mdragpoint_service_uses_kill_switch_default_off(self) -> None:
        compose = COMPOSE.read_text(encoding="utf-8")

        self.assertIn("mdragstream-point-analysis:", compose)
        self.assertIn('DASMA_ENABLED: "${DASMA_MDRAGPOINT_ENABLED:-0}"', compose)
        self.assertIn('MDRAGPOINT_VERDICT_MODE: "${MDRAGPOINT_VERDICT_MODE:-internal}"', compose)

    def test_launch_campaign_prepares_mdragpoint_image_and_arguments(self) -> None:
        script = LAUNCH_CAMPAIGN.read_text(encoding="utf-8")
        config = CAMPAIGN_CONFIG.read_text(encoding="utf-8")

        self.assertIn("mdragstream-point-analysis", script)
        self.assertIn('"mdragpoint_verdict_mode": "internal"', config)
        self.assertIn('"mdragpoint_estimator": "p99"', config)
        self.assertIn('"MDRAGPOINT_VERDICT_MODE": str(config["mdragpoint_verdict_mode"])', config)
        self.assertIn('"MDRAGPOINT_LOF_NEIGHBORS": str(config["mdragpoint_lof_neighbors"])', config)

    def test_reset_campaign_clears_modern_anomaly_topics_and_subjects(self) -> None:
        script = RESET_CAMPAIGN.read_text(encoding="utf-8")

        self.assertIn("reset-topics", script)
        self.assertIn("reset-schema-subjects", script)
        for expected in (
            "deepant-analysis",
            "deepant-analysis-viz",
            "mdragstream-point-analysis",
            "mdragstream-point-analysis-viz",
            "consensus-analysis",
        ):
            self.assertIn(expected, RESET_TOPICS)
        for expected in (
            "deepant-analysis-value",
            "mdragstream-point-analysis-value",
        ):
            self.assertIn(expected, RESET_SCHEMA_SUBJECTS)

    def test_stop_campaign_stops_mdragpoint_container(self) -> None:
        script = STOP_CAMPAIGN.read_text(encoding="utf-8")

        self.assertIn("all-runtime-services", script)
        self.assertIn("mdragstream-point-analysis", ALL_RUNTIME_COMPOSE_SERVICES)
