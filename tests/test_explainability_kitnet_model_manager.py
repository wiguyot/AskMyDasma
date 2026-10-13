from __future__ import annotations

import logging
import unittest
from types import SimpleNamespace

import numpy as np

from dasma_kafka.common.explained_models import KitNetModelManager


def _config(**overrides):
    values = {
        "kitnet_max_size_ae": 10,
        "kitnet_grace_feature_mapping": 2,
        "kitnet_grace_anomaly_detector": 2,
        "kitnet_learning_rate": 0.1,
        "kitnet_hidden_ratio": 0.75,
        "shap_background_size": 3,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class KitNetModelManagerTest(unittest.TestCase):
    def test_contract_and_background_only_contains_recent_finite_rows(self) -> None:
        manager = KitNetModelManager(_config(), logging.getLogger("kitnet-manager-test"))
        self.assertEqual(manager.reference_score_field, "kitnet_score")
        self.assertEqual(manager.reference_status_field, "kitnet_status")
        self.assertEqual(manager.reference_anomaly_field, "kitnet_anomaly")
        self.assertEqual(manager.reference_threshold_field, "kitnet_threshold")
        self.assertEqual(manager.model_version, "local-kitnet-v1")
        self.assertEqual(manager.supported_methods, ("kernel",))
        self.assertEqual(manager.mismatch_code, "kitnet_score_mismatch")
        with self.assertRaises(ValueError):
            manager.predict_fn("missing")

        results = []
        vectors = [np.asarray([i, i + 1], dtype=np.float64) for i in range(5)]
        for vector in vectors:
            results.append(manager.fit_score("A", vector))
        self.assertEqual(results[:4], [None, None, None, None])
        self.assertIsNotNone(results[4])
        self.assertEqual(manager.models.samples_seen("A"), 5)
        np.testing.assert_array_equal(
            manager.get_background_data("A"), np.asarray(vectors[-3:])
        )

        # The invalid current event is still scored by the shared detector
        # wrapper, but must be signalled and excluded from the SHAP background.
        with np.errstate(all="ignore"):
            manager.fit_score("A", np.asarray([np.nan, 6.0]))
        self.assertEqual(manager.input_error("A"), "invalid_features")
        self.assertEqual(manager.get_background_data("A").shape, (3, 2))
        manager.fit_score("A", np.asarray([6.0, 7.0]))
        self.assertIsNone(manager.input_error("A"))
        np.testing.assert_array_equal(
            manager.get_background_data("A"),
            np.asarray([vectors[-2], vectors[-1], [6.0, 7.0]]),
        )
        with np.errstate(all="ignore"):
            manager.fit_score("A", np.asarray([np.inf, 8.0]))
        self.assertEqual(manager.input_error("A"), "invalid_features")
        np.testing.assert_array_equal(
            manager.get_background_data("A"),
            np.asarray([vectors[-2], vectors[-1], [6.0, 7.0]]),
        )

    def test_predict_fn_uses_ready_groups_score_batch(self) -> None:
        manager = KitNetModelManager(_config(shap_background_size=2), logging.getLogger("kitnet-manager-test"))
        for i in range(5):
            manager.fit_score("A", np.asarray([float(i), float(i + 1)]))
        predict = manager.predict_fn("A")
        values = np.asarray([[1.5, 2.5], [2.5, 3.5]])
        expected = manager.models.score_batch("A", values)
        np.testing.assert_array_equal(predict(values), expected)
        self.assertIs(manager.get_model("A"), manager.models.models_by_group["A"])

    def test_fingerprint_is_canonical_and_sensitive_to_settings_and_order(self) -> None:
        manager = KitNetModelManager(_config(), logging.getLogger("kitnet-manager-test"))
        baseline = manager.model_fingerprint(["a", "b"])
        self.assertRegex(baseline, r"^[0-9a-f]{64}$")
        self.assertEqual(baseline, manager.model_fingerprint(["a", "b"]))
        self.assertNotEqual(baseline, manager.model_fingerprint(["b", "a"]))
        changed = KitNetModelManager(
            _config(kitnet_hidden_ratio=0.5), logging.getLogger("kitnet-manager-test")
        )
        self.assertNotEqual(baseline, changed.model_fingerprint(["a", "b"]))


if __name__ == "__main__":
    unittest.main()
