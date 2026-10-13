"""Unit tests for the mDragStream Point-based wrapper."""

from __future__ import annotations

import unittest

from dasma_kafka.anomaly.thresholds import WelfordEstimator
from runtime.detectors.mdragpoint.models.mdragpoint import (
    MDragPointGlobalModel,
    classify_mdragpoint_score_internal,
    classify_mdragpoint_score,
)


class MDragPointGlobalModelTest(unittest.TestCase):
    def _make_model(self, **overrides) -> MDragPointGlobalModel:
        defaults = dict(
            num_features=2,
            n_init=5,
            metric="euclidean",
            threshold_method="std",
            threshold_t=2.0,
            max_nb_clusters=5,
            max_cluster_size=10,
            seed=42,
        )
        defaults.update(overrides)
        return MDragPointGlobalModel(**defaults)

    def test_warmup_returns_zero_until_initialized(self) -> None:
        model = self._make_model()
        for point in ([0.0, 0.0], [0.1, 0.0], [0.0, 0.1], [0.1, 0.1]):
            self.assertEqual(model.partial_fit_score(point), 0.0)
            self.assertTrue(model.is_warming_up)

        self.assertEqual(model.partial_fit_score([0.2, 0.1]), 0.0)
        self.assertFalse(model.is_warming_up)
        self.assertIsNotNone(model.get_internal_threshold())
        self.assertEqual(model.samples_buffered, 5)

    def test_score_is_higher_for_far_point_than_near_point(self) -> None:
        model = self._make_model()
        for point in ([0.0, 0.0], [0.1, 0.0], [0.0, 0.1], [0.1, 0.1], [0.2, 0.1]):
            model.partial_fit_score(point)

        near_score = model.partial_fit_score([0.15, 0.05])
        far_score = model.partial_fit_score([10.0, 10.0])

        self.assertGreater(far_score, near_score * 10)
        self.assertTrue(model.last_is_anomaly)

    def test_feature_count_mismatch_raises(self) -> None:
        model = self._make_model()
        with self.assertRaises(ValueError):
            model.partial_fit_score([1.0])
        with self.assertRaises(ValueError):
            model.partial_fit_score([1.0, 2.0, 3.0])

    def test_mahalanobis_initializes_internal_threshold(self) -> None:
        model = self._make_model(metric="mahalanobis")
        for point in ([0.0, 0.0], [0.1, 0.0], [0.0, 0.1], [0.1, 0.1], [0.2, 0.1]):
            model.partial_fit_score(point)

        self.assertFalse(model.is_warming_up)
        self.assertIsNotNone(model.inv_cov)
        self.assertIsNotNone(model.get_internal_threshold())

    def test_dynamic_threshold_updates_when_candidate_becomes_cluster(self) -> None:
        model = self._make_model(dynamic_threshold=True, threshold_t=0.0)
        for point in ([0.0, 0.0], [0.1, 0.0], [0.0, 0.1], [0.1, 0.1], [0.2, 0.1]):
            model.partial_fit_score(point)

        initial_threshold = model.get_internal_threshold()
        model.partial_fit_score([5.0, 5.0])
        model.partial_fit_score([5.1, 5.0])

        self.assertNotEqual(model.get_internal_threshold(), initial_threshold)

    def test_classifier_uses_welford_contract(self) -> None:
        estimator = WelfordEstimator()
        status, anomaly, threshold = classify_mdragpoint_score(
            0.0,
            estimator,
            is_warming_up=True,
            min_samples=2,
            k_sigma=3.0,
        )

        self.assertEqual(status, "warming_up")
        self.assertIsNone(anomaly)
        self.assertIsNone(threshold)
        self.assertEqual(estimator.n, 0)

    def test_internal_classifier_uses_native_mdragstream_verdict(self) -> None:
        status, anomaly, threshold = classify_mdragpoint_score_internal(
            12.0,
            False,
            10.0,
            is_warming_up=False,
        )

        self.assertEqual(status, "normal")
        self.assertFalse(anomaly)
        self.assertEqual(threshold, 10.0)

        status, anomaly, threshold = classify_mdragpoint_score_internal(
            12.0,
            True,
            10.0,
            is_warming_up=False,
        )

        self.assertEqual(status, "anomaly")
        self.assertTrue(anomaly)
        self.assertEqual(threshold, 10.0)

    def test_internal_classifier_waits_for_native_verdict(self) -> None:
        status, anomaly, threshold = classify_mdragpoint_score_internal(
            0.0,
            None,
            10.0,
            is_warming_up=False,
        )

        self.assertEqual(status, "warming_up")
        self.assertIsNone(anomaly)
        self.assertIsNone(threshold)


if __name__ == "__main__":
    unittest.main()
