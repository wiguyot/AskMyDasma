"""Scaffold contracts for the isolated explainability-v2 model.

These tests verify that explainability-v2 exists as a clean, isolated second
explainability model: distinct topics, consumer group, service, scripts wiring,
observability, and dashboard — while the regular explainability baseline stays
unchanged.
"""

from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from dasma_kafka.explainability.config import ExplainabilityConfig
from dasma_kafka.explainability_v2.config import (
    EXCLUDED_COLUMNS,
    EXPLAINABILITY_V2_VALUE_SCHEMA_STR,
    ExplainabilityV2Config,
)
from dasma_kafka.explainability_v2.runtime import build_worker_config_dict
from dasma_kafka.init_topic.helpers import load_topics_from_env
from dasma_kafka.web.services.progress_service import PROGRESS_TOPICS
from scripts.web.campaign_config import ALL_RUNTIME_COMPOSE_SERVICES, RESET_SCHEMA_SUBJECTS, RESET_TOPICS


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "docker-compose.yml"
TELEGRAF = ROOT / "observability" / "telegraf" / "telegraf.conf"
START_PRODUCTION = ROOT / "scripts" / "web" / "start_production.sh"
LAUNCH_CAMPAIGN = ROOT / "scripts" / "web" / "launch_campaign.sh"
CAMPAIGN_CONFIG = ROOT / "scripts" / "web" / "campaign_config.py"
RESET_CAMPAIGN = ROOT / "scripts" / "web" / "reset_campaign.sh"
STOP_CAMPAIGN = ROOT / "scripts" / "web" / "stop_campaign.sh"
DASHBOARD = ROOT / "observability" / "grafana" / "dashboards" / "Explainability" / "explainability_v2_influxdb.json"
ENTRYPOINT = ROOT / "runtime" / "explainability" / "v2" / "explainability_v2.py"
REGULAR_ENTRYPOINT = (
    ROOT / "runtime" / "explainability" / "regular" / "explainability-multi-thread-by-group.py"
)
USER_INDEX = ROOT / "frontend" / "askmydasma" / "index.html"


class ExplainabilityV2ConfigContractsTest(TestCase):
    def test_default_topics_and_group_id_are_v2(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "BOOTSTRAP_SERVERS": "kafka:9092",
                "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
            },
            clear=True,
        ):
            config = ExplainabilityV2Config.from_environment()

        self.assertEqual(config.source_topic, "")
        self.assertEqual(config.dest_topic, "explainability-v2")
        self.assertEqual(config.group_id, "explainability-v2-consumer")
        self.assertEqual(config.hstree_seed, 42)

    def test_avro_record_name_is_distinct_from_regular(self) -> None:
        schema = json.loads(EXPLAINABILITY_V2_VALUE_SCHEMA_STR)
        self.assertEqual(schema["name"], "ExplainabilityV2Event")
        # v2 explains the same local HSTree score as regular.
        field_names = {field["name"] for field in schema["fields"]}
        self.assertIn("local_hstree_score", field_names)
        self.assertIn("shap_values", field_names)

    def test_excluded_columns_match_regular_semantics(self) -> None:
        for column in ("timestamp", "hstree_score", "hstree_status", "_offset"):
            self.assertIn(column, EXCLUDED_COLUMNS)

    def test_worker_config_preserves_iforest_selection_and_settings(self) -> None:
        config = ExplainabilityV2Config(
            explained_model="iforest",
            explainability_mode="simple",
            explanation_method="kernel",
            hstree_seed=17,
            iforest_initial_window_size=512,
            iforest_window_size=2048,
            iforest_n_estimators=200,
            iforest_max_samples=256,
            iforest_refit_every=128,
        )

        worker_config = build_worker_config_dict(config)

        self.assertEqual(worker_config["explained_model"], "iforest")
        self.assertEqual(worker_config["explainability_mode"], "simple")
        self.assertEqual(worker_config["explanation_method"], "kernel")
        self.assertEqual(worker_config["hstree_seed"], 17)
        self.assertEqual(worker_config["iforest_initial_window_size"], 512)
        self.assertEqual(worker_config["iforest_n_estimators"], 200)
        self.assertEqual(worker_config["iforest_max_samples"], 256)
        self.assertEqual(worker_config["iforest_refit_every"], 128)

    def test_worker_config_transmits_every_dataclass_field(self) -> None:
        config = ExplainabilityV2Config()

        self.assertEqual(
            set(build_worker_config_dict(config)),
            {field.name for field in fields(ExplainabilityV2Config)},
        )

    def test_regular_explainability_config_is_unchanged(self) -> None:
        """The baseline must keep its own topics and consumer group."""
        with patch.dict(
            "os.environ",
            {
                "BOOTSTRAP_SERVERS": "kafka:9092",
                "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
            },
            clear=True,
        ):
            regular = ExplainabilityConfig.from_environment()

        self.assertEqual(regular.dest_topic, "explainability")
        self.assertEqual(regular.group_id, "explainability-consumer")
        self.assertEqual(regular.hstree_seed, 42)


class ExplainabilityV2TopicsTest(TestCase):
    def test_v2_topics_present_and_distinct_from_regular(self) -> None:
        names = {topic.name for topic in load_topics_from_env()}
        self.assertIn("explainability-v2", names)
        self.assertIn("explainability-v2-viz", names)
        # Regular topics still exist and do not collide with v2.
        self.assertIn("explainability", names)
        self.assertIn("explainability-viz", names)
        self.assertNotEqual("explainability", "explainability-v2")


class ExplainabilityV2ServiceTest(TestCase):
    def setUp(self) -> None:
        self.compose = COMPOSE.read_text(encoding="utf-8")

    def test_compose_declares_isolated_v2_service(self) -> None:
        self.assertIn("explainability-v2:", self.compose)
        self.assertIn("container_name: explainability-v2", self.compose)
        self.assertIn('KAFKA_DEST_TOPIC: "explainability-v2"', self.compose)
        self.assertIn('KAFKA_DEST_TOPIC_JSON:  "explainability-v2-viz"', self.compose)
        self.assertIn('EXPLAINABILITY_GROUP_ID: "explainability-v2-consumer"', self.compose)

    def test_compose_v2_kill_switch_defaults_off(self) -> None:
        self.assertIn('DASMA_ENABLED: "${DASMA_EXPLAINABILITY_V2_ENABLED:-0}"', self.compose)

    def test_compose_v2_runs_the_v2_entrypoint(self) -> None:
        self.assertIn(
            'entrypoint: ["python", "/app/runtime/explainability/v2/explainability_v2.py"]',
            self.compose,
        )

    def test_compose_uses_distinct_shap_nsamples_env_vars(self) -> None:
        self.assertIn('SHAP_NSAMPLES: "${EXPLAINABILITY_SHAP_NSAMPLES:-100}"', self.compose)
        self.assertIn('SHAP_NSAMPLES: "${EXPLAINABILITY_V2_SHAP_NSAMPLES:-100}"', self.compose)

    def test_v2_entrypoint_file_exists(self) -> None:
        self.assertTrue(ENTRYPOINT.is_file())

    def test_entrypoints_resolve_the_repository_root_from_runtime(self) -> None:
        for path in (ENTRYPOINT, REGULAR_ENTRYPOINT):
            self.assertIn("Path(__file__).resolve().parents[3]", path.read_text(encoding="utf-8"))


class ExplainabilityV2ScriptsTest(TestCase):
    def test_start_production_enables_v2_only_with_hstree(self) -> None:
        script = START_PRODUCTION.read_text(encoding="utf-8")
        config = CAMPAIGN_CONFIG.read_text(encoding="utf-8")
        self.assertIn('"DASMA_EXPLAINABILITY_V2_ENABLED": "1" if model_enabled(config, "explainability-v2") else "0"', config)
        self.assertIn('return model in selected', config)
        self.assertIn('"explainability-v2": "explainability-v2"', config)
        self.assertIn("DASMA_COMPOSE_SERVICES", script)
        self.assertIn('"EXPLAINABILITY_SHAP_NSAMPLES": str(config["explainability_nsamples"])', config)
        self.assertIn('"EXPLAINABILITY_V2_SHAP_NSAMPLES": str(config["explainability_v2_nsamples"])', config)

    def test_launch_campaign_prepares_v2_image(self) -> None:
        self.assertIn("explainability-v2", LAUNCH_CAMPAIGN.read_text(encoding="utf-8"))

    def test_entrypoints_apply_backpressure_instead_of_dropping_when_worker_queue_is_full(self) -> None:
        for path in (ENTRYPOINT, REGULAR_ENTRYPOINT):
            script = path.read_text(encoding="utf-8")
            self.assertIn("from queue import Full", script)
            self.assertIn("waiting instead of dropping message", script)
            self.assertIn("except Full:", script)

    def test_stop_and_reset_handle_v2(self) -> None:
        self.assertIn("all-runtime-services", STOP_CAMPAIGN.read_text(encoding="utf-8"))
        self.assertIn("explainability-v2", ALL_RUNTIME_COMPOSE_SERVICES)
        reset = RESET_CAMPAIGN.read_text(encoding="utf-8")
        self.assertIn("reset-topics", reset)
        self.assertIn("reset-schema-subjects", reset)
        self.assertIn("explainability-v2", RESET_TOPICS)
        self.assertIn("explainability-v2-viz", RESET_TOPICS)
        self.assertIn("explainability-v2-value", RESET_SCHEMA_SUBJECTS)


class ExplainabilityV2ObservabilityTest(TestCase):
    def test_build_metrics_tracks_v2_topics(self) -> None:
        compose = COMPOSE.read_text(encoding="utf-8")
        self.assertIn("explainability-v2,explainability-v2-viz", compose)

    def test_influxdb_bucket_has_no_retention(self) -> None:
        compose = COMPOSE.read_text(encoding="utf-8")
        self.assertIn('ensure_no_retention "explainability-v2"', compose)

    def test_telegraf_consumes_and_routes_v2(self) -> None:
        telegraf = TELEGRAF.read_text(encoding="utf-8")
        self.assertIn('topics = ["explainability-v2-viz"]', telegraf)
        self.assertIn('consumer_group = "telegraf-explainability-v2"', telegraf)
        self.assertIn('name_override = "explainability-v2"', telegraf)
        self.assertIn('bucket = "explainability-v2"', telegraf)
        self.assertIn('namepass = ["explainability-v2"]', telegraf)

    def test_progress_tracks_v2(self) -> None:
        self.assertIn(("Explainability v2", "explainability-v2"), PROGRESS_TOPICS)


class ExplainabilityV2DashboardTest(TestCase):
    def test_v2_dashboard_is_isolated_and_valid(self) -> None:
        self.assertTrue(DASHBOARD.is_file())
        raw = DASHBOARD.read_text(encoding="utf-8")
        data = json.loads(raw)
        self.assertEqual(data["title"], "Explainability V2 — HSTree")
        self.assertEqual(data["uid"], "explainability-v2-influxdb")
        self.assertIn("explainability-v2", data["tags"])
        # Reads from the v2 bucket/measurement, never the regular one.
        self.assertIn('bucket: \\"explainability-v2\\"', raw)
        self.assertNotIn('bucket: \\"explainability\\"', raw)


class ExplainabilityV2UiTest(TestCase):
    def test_user_page_exposes_v2_checkbox_without_dropping_regular(self) -> None:
        html = USER_INDEX.read_text(encoding="utf-8")
        self.assertIn('id="model-explainability-v2"', html)
        self.assertIn('value="explainability-v2"', html)
        # Regular checkbox is still present.
        self.assertIn('id="model-explainability"', html)
        self.assertIn("explainability-v2-influxdb", html)
