from dataclasses import fields
from unittest import TestCase
from unittest.mock import patch

from dasma_kafka.explainability.config import (
    EXCLUDED_COLUMNS,
    ExplainabilityConfig,
    validate_explainability_selection,
)
from dasma_kafka.explainability.runtime import build_worker_config_dict
from dasma_kafka.explainability_v2.config import ExplainabilityV2Config
from dasma_kafka.explainability_v2.config import validate_explainability_selection as validate_v2_selection
from dasma_kafka.explainability_v2.runtime import build_worker_config_dict as build_v2_worker_config_dict


class ExplainabilityConfigContractsTest(TestCase):
    def test_hstree_dataclass_defaults_are_256_for_v1_and_v2(self) -> None:
        for config in (ExplainabilityConfig(), ExplainabilityV2Config()):
            self.assertEqual((config.hstree_initial_window_size, config.hstree_window_size), (256, 256))

    def test_iforest_defaults_and_invalid_parameters_are_validated_in_v1_and_v2(self) -> None:
        for config_type, validator in (
            (ExplainabilityConfig, validate_explainability_selection),
            (ExplainabilityV2Config, validate_v2_selection),
        ):
            config = config_type(
                explained_model="iforest",
                explainability_mode="simple",
                explanation_method="kernel",
                source_topic="source-data",
            )
            self.assertEqual(config.iforest_initial_window_size, 512)
            self.assertEqual(config.iforest_max_samples, 256)
            self.assertEqual(config.iforest_n_estimators, 100)
            self.assertEqual(config.iforest_refit_every, 128)
            validator(config)

            config.iforest_max_samples = "auto"
            with self.assertRaisesRegex(ValueError, "max_samples doit être un entier"):
                validator(config)
            config.iforest_max_samples = 256
            config.iforest_initial_window_size = 128
            with self.assertRaisesRegex(ValueError, "fenêtre d'amorçage"):
                validator(config)
            config.iforest_initial_window_size = 512
            config.iforest_contamination = "0.1"
            validator(config)
            config.iforest_contamination = "0.51"
            with self.assertRaisesRegex(ValueError, "contamination doit être"):
                validator(config)

    def test_default_topics_and_group_id(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "BOOTSTRAP_SERVERS": "kafka:9092",
                "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
            },
            clear=True,
        ):
            config = ExplainabilityConfig.from_environment()

        self.assertEqual(config.source_topic, "")
        self.assertEqual(config.dest_topic, "explainability")
        self.assertEqual(config.group_id, "explainability-consumer")
        self.assertEqual(config.dest_topic_json, "")
        self.assertEqual((config.hstree_initial_window_size, config.hstree_window_size), (256, 256))

        with patch.dict(
            "os.environ",
            {
                "BOOTSTRAP_SERVERS": "kafka:9092",
                "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
            },
            clear=True,
        ):
            config_v2 = ExplainabilityV2Config.from_environment()
        self.assertEqual(
            (config_v2.hstree_initial_window_size, config_v2.hstree_window_size),
            (256, 256),
        )

    def test_hstree_variant_is_configurable_and_validated_in_v1_and_v2(self) -> None:
        required = {
            "BOOTSTRAP_SERVERS": "kafka:9092",
            "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
        }
        for config_type, validator in (
            (ExplainabilityConfig, validate_explainability_selection),
            (ExplainabilityV2Config, validate_v2_selection),
        ):
            with patch.dict("os.environ", required, clear=True):
                self.assertEqual(config_type.from_environment().hstree_variant, "paper")
            with patch.dict("os.environ", {**required, "HSTREE_VARIANT": "paper"}, clear=True):
                config = config_type.from_environment()
            config.explained_model = "hstree"
            config.explainability_mode = "simple"
            config.explanation_method = "kernel"
            config.source_topic = "source-data"
            validator(config)
            self.assertEqual(config.hstree_variant, "paper")

            config.hstree_variant = "unknown"
            with self.assertRaisesRegex(ValueError, "HSTREE_VARIANT"):
                validator(config)

    def test_json_topic_prefers_canonical_env_name(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "BOOTSTRAP_SERVERS": "kafka:9092",
                "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
                "KAFKA_DEST_TOPIC_JSON": "canonical-topic",
                "KAFKA_DEST_TOPIC_JSNON": "legacy-topic",
            },
            clear=True,
        ):
            config = ExplainabilityConfig.from_environment()

        self.assertEqual(config.dest_topic_json, "canonical-topic")

    def test_excluded_columns_contains_system_fields(self) -> None:
        self.assertIn("timestamp", EXCLUDED_COLUMNS)
        self.assertIn("hstree_score", EXCLUDED_COLUMNS)
        self.assertIn("hstree_status", EXCLUDED_COLUMNS)
        self.assertIn("hstree_anomaly", EXCLUDED_COLUMNS)
        self.assertIn("hstree_threshold", EXCLUDED_COLUMNS)
        self.assertIn("_topic", EXCLUDED_COLUMNS)
        self.assertIn("_partition", EXCLUDED_COLUMNS)
        self.assertIn("_offset", EXCLUDED_COLUMNS)
        self.assertIn("_key", EXCLUDED_COLUMNS)

    def test_worker_config_preserves_explainability_selection(self) -> None:
        config = ExplainabilityConfig(
            explained_model="hstree",
            explainability_mode="simple",
            explanation_method="kernel",
        )

        worker_config = build_worker_config_dict(config)

        self.assertEqual(worker_config["explained_model"], "hstree")
        self.assertEqual(worker_config["explainability_mode"], "simple")
        self.assertEqual(worker_config["explanation_method"], "kernel")

    def test_worker_config_preserves_iforest_settings(self) -> None:
        config = ExplainabilityConfig(
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
        config = ExplainabilityConfig()

        self.assertEqual(
            set(build_worker_config_dict(config)),
            {field.name for field in fields(ExplainabilityConfig)},
        )

    def test_worker_config_preserves_kitnet_settings_in_v1_and_v2(self) -> None:
        values = {
            "kitnet_max_size_ae": 7,
            "kitnet_grace_feature_mapping": None,
            "kitnet_grace_anomaly_detector": 321,
            "kitnet_learning_rate": 0.2,
            "kitnet_hidden_ratio": 0.6,
        }
        for config_type, builder in (
            (ExplainabilityConfig, build_worker_config_dict),
            (ExplainabilityV2Config, build_v2_worker_config_dict),
        ):
            worker_config = builder(config_type(**values))
            for name, expected in values.items():
                self.assertEqual(worker_config[name], expected)

    def test_kitnet_config_reads_detector_environment_and_grace_aliases(self) -> None:
        environment = {
            "BOOTSTRAP_SERVERS": "kafka:9092",
            "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
            "KITNET_MAX_SIZE_AE": "8",
            "KITNET_GRACE_FEATURE_MAPPING": "",
            "KITNET_FM_GRACE_PERIOD": "None",
            "KITNET_GRACE_ANOMALY_DETECTOR": "",
            "KITNET_AD_GRACE_PERIOD": "654",
            "KITNET_LEARNING_RATE": "0.25",
            "KITNET_HIDDEN_RATIO": "0.5",
        }
        with patch.dict("os.environ", environment, clear=True):
            v1 = ExplainabilityConfig.from_environment()
            v2 = ExplainabilityV2Config.from_environment()
        for config in (v1, v2):
            self.assertEqual(config.kitnet_max_size_ae, 8)
            self.assertIsNone(config.kitnet_grace_feature_mapping)
            self.assertEqual(config.kitnet_grace_anomaly_detector, 654)
            self.assertEqual(config.kitnet_learning_rate, 0.25)
            self.assertEqual(config.kitnet_hidden_ratio, 0.5)

    def test_kitnet_is_an_explicitly_supported_kernel_explanation_model(self) -> None:
        for config_type, validator in (
            (ExplainabilityConfig, validate_explainability_selection),
            (ExplainabilityV2Config, validate_v2_selection),
        ):
            validator(
                config_type(
                    explained_model="kitnet",
                    explainability_mode="simple",
                    explanation_method="kernel",
                    source_topic="source-data",
                )
            )
