"""Unit tests for the DeepAnT streaming model (in isolation from Kafka)."""

from __future__ import annotations

import math
import unittest

import numpy as np

try:
    import torch

    TORCH_AVAILABLE = True
except ImportError:
    torch = None  # type: ignore[assignment]
    TORCH_AVAILABLE = False

try:
    from runtime.detectors.deepant.models.deepant import DeepAntCNN, DeepAntStreamingModel
except Exception:
    DeepAntCNN = None  # type: ignore[assignment, misc]
    DeepAntStreamingModel = None  # type: ignore[assignment]


def _seed_all(seed: int = 42) -> None:
    torch.manual_seed(seed)
    np.random.seed(seed)


def _sine_signal(n: int, period: int = 20, n_features: int = 1) -> list[list[float]]:
    """Generate `n` samples of a stable sine signal."""
    out = []
    for i in range(n):
        base = math.sin(2 * math.pi * i / period)
        out.append([base + 0.01 * (j + 1) for j in range(n_features)])
    return out


@unittest.skipIf(not TORCH_AVAILABLE, reason="torch is not installed")
class DeepAntCNNTest(unittest.TestCase):
    def test_forward_output_shape(self) -> None:
        _seed_all()
        model = DeepAntCNN(lookback_size=16, n_features=3)
        batch = torch.randn(4, 16, 3)
        out = model(batch)
        self.assertEqual(out.shape, (4, 3))

    def test_raises_on_lookback_too_small(self) -> None:
        for lookback in (1, 4, 7):
            with self.assertRaises(ValueError):
                DeepAntCNN(lookback_size=lookback, n_features=2)

    def test_raises_on_zero_features(self) -> None:
        with self.assertRaises(ValueError):
            DeepAntCNN(lookback_size=16, n_features=0)


@unittest.skipIf(not TORCH_AVAILABLE, reason="torch is not installed")
class DeepAntStreamingModelTest(unittest.TestCase):
    def _make_model(self, **overrides) -> DeepAntStreamingModel:
        defaults = dict(
            n_features=1,
            lookback_size=8,
            initial_window_size=64,
            refit_every=1_000_000,
            window_size=256,
            epochs_initial=30,
            epochs_refit=5,
            batch_size=16,
            seed=42,
        )
        defaults.update(overrides)
        return DeepAntStreamingModel(**defaults)

    def test_init_validates_lookback(self) -> None:
        with self.assertRaises(ValueError):
            DeepAntStreamingModel(n_features=1, lookback_size=4)

    def test_init_validates_initial_window_size(self) -> None:
        with self.assertRaises(ValueError):
            DeepAntStreamingModel(
                n_features=1, lookback_size=10, initial_window_size=5
            )

    def test_init_validates_window_size(self) -> None:
        with self.assertRaises(ValueError):
            DeepAntStreamingModel(
                n_features=1,
                lookback_size=10,
                initial_window_size=20,
                window_size=10,
            )

    def test_warmup_returns_zero_and_flag_set(self) -> None:
        _seed_all()
        model = self._make_model()
        required = model.lookback_size + model.initial_window_size
        for vec in _sine_signal(required - 1):
            score = model.partial_fit_score(vec)
            self.assertEqual(score, 0.0)
        self.assertTrue(model.is_warming_up)
        self.assertEqual(model.samples_buffered, required - 1)

    def test_ready_after_initial_window(self) -> None:
        _seed_all()
        model = self._make_model()
        required = model.lookback_size + model.initial_window_size
        signal = _sine_signal(required)
        for vec in signal:
            model.partial_fit_score(vec)
        self.assertFalse(model.is_warming_up)
        next_vec = signal[-1]
        score = model.partial_fit_score(next_vec)
        self.assertGreaterEqual(score, 0.0)

    def test_score_higher_on_artificial_spike(self) -> None:
        _seed_all()
        model = self._make_model()
        required = model.lookback_size + model.initial_window_size
        normal_signal = _sine_signal(required + 50)
        for vec in normal_signal:
            model.partial_fit_score(vec)
        clean_scores = []
        for vec in _sine_signal(20, period=20):
            clean_scores.append(model.partial_fit_score(vec))
        clean_max = max(clean_scores)
        anomaly_score = model.partial_fit_score([50.0])
        self.assertGreater(anomaly_score, clean_max * 3)

    def test_feature_count_mismatch_raises(self) -> None:
        _seed_all()
        model = self._make_model(n_features=2)
        with self.assertRaises(ValueError):
            model.partial_fit_score([1.0])
        with self.assertRaises(ValueError):
            model.partial_fit_score([1.0, 2.0, 3.0])

    def test_normalization_stable_between_refits(self) -> None:
        _seed_all()
        model = self._make_model(refit_every=10_000)
        required = model.lookback_size + model.initial_window_size
        for vec in _sine_signal(required):
            model.partial_fit_score(vec)
        snapshot_min = model._norm_min.copy()
        snapshot_max = model._norm_max.copy()
        for vec in _sine_signal(200):
            model.partial_fit_score(vec)
        np.testing.assert_array_equal(model._norm_min, snapshot_min)
        np.testing.assert_array_equal(model._norm_max, snapshot_max)

    def test_refit_refreshes_normalization_stats(self) -> None:
        _seed_all()
        model = self._make_model(refit_every=10, epochs_refit=1)
        required = model.lookback_size + model.initial_window_size
        for vec in _sine_signal(required):
            model.partial_fit_score(vec)
        baseline_max = model._norm_max.copy()
        for i in range(20):
            model.partial_fit_score([3.0 + 0.1 * i])
        self.assertGreater(model._norm_max[0], baseline_max[0])

    def test_deferred_learning_keeps_anomaly_out_of_refit_history(self) -> None:
        _seed_all()
        model = self._make_model(
            refit_every=1,
            epochs_initial=5,
            epochs_refit=1,
        )
        required = model.lookback_size + model.initial_window_size
        for vec in _sine_signal(required):
            model.partial_fit_score(vec)

        baseline_min = model._norm_min.copy()
        baseline_max = model._norm_max.copy()
        samples_before = model.samples_buffered

        anomaly_score = model.partial_fit_score([50.0], learn=False)
        self.assertTrue(math.isfinite(anomaly_score))
        self.assertEqual(model.samples_buffered, samples_before)
        np.testing.assert_array_equal(model._norm_min, baseline_min)
        np.testing.assert_array_equal(model._norm_max, baseline_max)

        model.accept_sample([50.0])
        self.assertEqual(model.samples_buffered, samples_before + 1)
        self.assertGreater(model._norm_max[0], baseline_max[0])

    def test_score_is_finite(self) -> None:
        _seed_all()
        model = self._make_model()
        required = model.lookback_size + model.initial_window_size
        for vec in _sine_signal(required + 30):
            score = model.partial_fit_score(vec)
            self.assertTrue(math.isfinite(score))


if __name__ == "__main__":
    unittest.main()
