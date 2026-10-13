import logging
import os
from unittest import TestCase
from unittest.mock import patch

import numpy as np

from dasma_kafka.explainability.config import ExplainabilityConfig
from dasma_kafka.explainability.features import FeatureExtractor, build_output_record
from dasma_kafka.explainability.model import HSTreeModelManager
from dasma_kafka.explainability.shap_engine import seed_for_message


class ExplainabilityHelpersTest(TestCase):
    def test_build_output_record_handles_invalid_values(self) -> None:
        record = {"timestamp": 123, "hstree_score": 2, "a": "bad", "b": 5}
        shap_result = {
            "shap_values": {"a": 1, "b": 2.5},
            "shap_base_value": 0,
            "shap_computation_time_ms": 12,
        }

        output = build_output_record(
            record=record,
            group="G1",
            feature_names=["a", "b"],
            shap_result=shap_result,
            local_score=9.5,
            explained_model="hstree",
        )

        self.assertEqual(output["timestamp"], "123")
        self.assertEqual(output["features"], {"a": 0.0, "b": 5.0})
        self.assertEqual(output["shap_values"], {"a": 1.0, "b": 2.5})
        self.assertEqual(output["local_hstree_score"], 9.5)

    def test_feature_extractor_initializes_sorted_numeric_columns(self) -> None:
        extractor = FeatureExtractor(logging.getLogger("test"))
        extractor.initialize_from_record(
            {
                "Group": "A",
                "z": 1,
                "a": 2.5,
                "timestamp": "x",
                "flag": True,
                "name": "bad",
            }
        )

        self.assertEqual(extractor.get_feature_names(), ["a", "z"])

    def test_build_output_record_resolves_timestamp_fallback(self) -> None:
        record = {"Timestamp": "2026-01-01 10:00:00", "hstree_score": 1.0, "a": 3}
        output = build_output_record(
            record=record,
            group="G1",
            feature_names=["a"],
            shap_result={},
            local_score=None,
            explained_model="hstree",
        )
        self.assertEqual(output["timestamp"], "2026-01-01 10:00:00")

    def test_build_output_record_rejects_missing_or_unknown_explained_model(self) -> None:
        for explained_model in (None, "", "unknown"):
            with self.subTest(explained_model=explained_model):
                with self.assertRaisesRegex(ValueError, "explained_model must be"):
                    build_output_record(
                        record={},
                        group="G1",
                        feature_names=[],
                        shap_result={},
                        local_score=None,
                        explained_model=explained_model,
                    )

    def test_seed_for_message_is_deterministic(self) -> None:
        seed_1 = seed_for_message(42, "Sortie CTA", 3, 10)
        seed_2 = seed_for_message(42, "Sortie CTA", 3, 10)
        seed_3 = seed_for_message(42, "Sortie CTA", 3, 11)

        self.assertEqual(seed_1, seed_2)
        self.assertNotEqual(seed_1, seed_3)

    def test_config_reads_typo_compatible_json_topic(self) -> None:
        env = {
            "BOOTSTRAP_SERVERS": "kafka:9092",
            "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
            "KAFKA_DEST_TOPIC_JSNON": "explainability-viz",
        }
        with patch.dict(os.environ, env, clear=True):
            config = ExplainabilityConfig.from_environment()

        self.assertEqual(config.dest_topic_json, "explainability-viz")


class HSTreeModelManagerFitOnlyTest(TestCase):
    def _make_manager(self, initial_window_size: int = 10) -> HSTreeModelManager:
        config = ExplainabilityConfig(
            bootstrap_servers="kafka:9092",
            hstree_initial_window_size=initial_window_size,
            hstree_window_size=50,
            hstree_num_trees=5,
            hstree_max_depth=5,
            shap_background_size=5,
        )
        return HSTreeModelManager(config, logging.getLogger("test"))

    def _features(self, n: int = 4, seed: int = 0) -> np.ndarray:
        rng = np.random.default_rng(seed)
        return rng.uniform(0.5, 5.0, size=n).astype(np.float64)

    def test_fit_only_returns_false_during_warmup(self) -> None:
        manager = self._make_manager(initial_window_size=10)
        for i in range(9):
            result = manager.fit_only("G1", self._features(seed=i))
            self.assertFalse(result)

    def test_fit_only_returns_false_on_reference_boundary_then_true(self) -> None:
        manager = self._make_manager(initial_window_size=5)
        for i in range(4):
            manager.fit_only("G1", self._features(seed=i))
        self.assertFalse(manager.fit_only("G1", self._features(seed=99)))
        self.assertTrue(manager.fit_only("G1", self._features(seed=100)))

    def test_fit_only_builds_background_buffer(self) -> None:
        manager = self._make_manager(initial_window_size=5)
        for i in range(8):
            manager.fit_only("G1", self._features(seed=i))
        self.assertGreater(len(manager.models["G1"].background_buffer), 0)

    def test_fit_only_model_is_scoreable_after_warmup(self) -> None:
        manager = self._make_manager(initial_window_size=5)
        for i in range(8):
            manager.fit_only("G1", self._features(seed=i))
        model = manager.get_model("G1")
        self.assertIsNotNone(model)
        score = model.score_partial(self._features())
        self.assertIsInstance(float(score), float)

    def test_fit_only_and_fit_score_both_produce_valid_negative_scores(self) -> None:
        """Both variants process the same sequence and produce valid (negative) scores.
        fit_score_partial scores before fitting; fit_partial just fits. PySAD HalfSpaceTrees
        scores are always negative, so both model states are valid even if not identical."""
        n_records = 20
        window = 5
        manager_A = self._make_manager(initial_window_size=window)
        manager_B = self._make_manager(initial_window_size=window)
        for i in range(n_records):
            f = self._features(seed=i)
            manager_A.fit_score("G1", f)
            manager_B.fit_only("G1", f)
        probe = self._features(seed=999)
        score_A = float(manager_A.get_model("G1").score_partial(probe))
        score_B = float(manager_B.get_model("G1").score_partial(probe))
        # Both scores must be finite and negative (HSTree contract)
        self.assertLess(score_A, 0.0)
        self.assertLess(score_B, 0.0)
        # Both scores must be in the same order of magnitude (not wildly different)
        import math
        self.assertAlmostEqual(math.log10(abs(score_A)), math.log10(abs(score_B)), delta=1.5)
