from unittest import TestCase

from dasma_kafka.anomaly.thresholds import (
    WelfordEstimator,
    WindowedWelfordEstimator,
    classify_score,
)


class AnomalyThresholdsTest(TestCase):
    def test_welford_estimator_updates_mean_std_and_threshold(self) -> None:
        estimator = WelfordEstimator()
        for value in [1.0, 2.0, 3.0]:
            estimator.update(value)

        self.assertEqual(estimator.n, 3)
        self.assertAlmostEqual(estimator.mean, 2.0)
        self.assertAlmostEqual(estimator.std, 1.0)
        self.assertAlmostEqual(estimator.threshold(3.0), 5.0)

    def test_classify_score_ignores_warmup_sentinel(self) -> None:
        estimator = WelfordEstimator()

        status, anomaly, threshold = classify_score(
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

    def test_classify_score_rejects_anomaly_contamination(self) -> None:
        estimator = WelfordEstimator()
        for value in [0.10, 0.20, 0.15]:
            estimator.update(value)

        status, anomaly, threshold = classify_score(
            0.30,
            estimator,
            is_warming_up=False,
            min_samples=3,
            k_sigma=1.0,
        )

        self.assertEqual(status, "anomaly")
        self.assertTrue(anomaly)
        self.assertAlmostEqual(threshold, 0.20)
        self.assertEqual(estimator.n, 3)


class WindowedWelfordEstimatorTest(TestCase):
    def test_window_size_property(self) -> None:
        estimator = WindowedWelfordEstimator(window_size=10)
        self.assertEqual(estimator.window_size, 10)

    def test_mean_and_std_within_window(self) -> None:
        estimator = WindowedWelfordEstimator(window_size=100)
        for value in [1.0, 2.0, 3.0]:
            estimator.update(value)

        self.assertEqual(estimator.n, 3)
        self.assertAlmostEqual(estimator.mean, 2.0)
        self.assertAlmostEqual(estimator.std, 1.0)
        self.assertAlmostEqual(estimator.threshold(3.0), 5.0)

    def test_old_values_fall_out_of_window(self) -> None:
        estimator = WindowedWelfordEstimator(window_size=3)
        for value in [100.0, 200.0, 300.0]:
            estimator.update(value)
        # Window is full: [100, 200, 300]. Now push three new values.
        for value in [1.0, 2.0, 3.0]:
            estimator.update(value)

        # Old values (100, 200, 300) must have been evicted.
        self.assertEqual(estimator.n, 6)
        self.assertAlmostEqual(estimator.mean, 2.0)
        self.assertAlmostEqual(estimator.std, 1.0)

    def test_n_is_cumulative_not_capped(self) -> None:
        estimator = WindowedWelfordEstimator(window_size=3)
        for value in range(10):
            estimator.update(float(value))
        # n counts all points seen, even those that have left the window.
        self.assertEqual(estimator.n, 10)

    def test_empty_estimator_returns_zero_mean_std(self) -> None:
        estimator = WindowedWelfordEstimator(window_size=10)
        self.assertEqual(estimator.mean, 0.0)
        self.assertEqual(estimator.std, 0.0)
        self.assertEqual(estimator.threshold(3.0), 0.0)

    def test_classify_score_with_windowed_welford_rejects_anomaly_contamination(
        self,
    ) -> None:
        estimator = WindowedWelfordEstimator(window_size=100)
        for value in [0.10, 0.20, 0.15]:
            estimator.update(value)

        status, anomaly, threshold = classify_score(
            0.30,
            estimator,
            is_warming_up=False,
            min_samples=3,
            k_sigma=1.0,
        )

        self.assertEqual(status, "anomaly")
        self.assertTrue(anomaly)
        # Anomaly must not be added to the window.
        self.assertEqual(estimator.n, 3)

    def test_classify_score_windowed_evicts_old_baseline(self) -> None:
        # Fill a tiny window with high baseline, then replace it with low values.
        estimator = WindowedWelfordEstimator(window_size=3)
        for value in [10.0, 10.0, 10.0]:
            estimator.update(value)

        # Push three low values to evict the high baseline.
        for value in [1.0, 1.0, 1.0]:
            estimator.update(value)

        # Threshold must now be near 1.0, not near 10.0.
        self.assertAlmostEqual(estimator.mean, 1.0)
        self.assertAlmostEqual(estimator.std, 0.0)
        self.assertAlmostEqual(estimator.threshold(3.0), 1.0)
