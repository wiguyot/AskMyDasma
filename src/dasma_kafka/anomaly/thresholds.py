"""Reusable online thresholding primitives for anomaly scores."""

from __future__ import annotations

from collections import deque

import numpy as np


class WelfordEstimator:
    """Online mean/std estimator used to threshold anomaly scores."""

    def __init__(self) -> None:
        self.n = 0
        self._mean = 0.0
        self._m2 = 0.0

    def update(self, x: float) -> None:
        value = float(x)
        self.n += 1
        delta = value - self._mean
        self._mean += delta / self.n
        delta2 = value - self._mean
        self._m2 += delta * delta2

    @property
    def mean(self) -> float:
        return self._mean

    @property
    def std(self) -> float:
        if self.n < 2:
            return 0.0
        return float((self._m2 / (self.n - 1)) ** 0.5)

    def threshold(self, k_sigma: float) -> float:
        return self.mean + float(k_sigma) * self.std


class WindowedWelfordEstimator:
    """Sliding-window mean/std estimator for anomaly score thresholding.

    Maintains the last `window_size` scores and computes mean/std within that
    window only. Unlike WelfordEstimator, old scores fall out when the window is
    full, so the threshold adapts to concept drift over long campaigns.

    The default window size should match the model's own reference window so
    the thresholding horizon stays consistent with the anomaly model's memory.
    It can be overridden independently via the corresponding env var.
    """

    def __init__(self, window_size: int = 500) -> None:
        self._window: deque[float] = deque(maxlen=int(window_size))
        self.n = 0

    def update(self, x: float) -> None:
        self._window.append(float(x))
        self.n += 1

    @property
    def mean(self) -> float:
        if not self._window:
            return 0.0
        return float(np.mean(list(self._window)))

    @property
    def std(self) -> float:
        if len(self._window) < 2:
            return 0.0
        return float(np.std(list(self._window), ddof=1))

    def threshold(self, k_sigma: float) -> float:
        return self.mean + float(k_sigma) * self.std

    @property
    def window_size(self) -> int:
        return self._window.maxlen or 0


class SlidingPercentileEstimator:
    """Online sliding-window percentile estimator for anomaly thresholding.

    Maintains the last `window_size` scores and returns their Pth percentile
    as the detection threshold. Unlike WelfordEstimator, this makes no
    distributional assumption and stays within the actual score range.
    """

    def __init__(self, window_size: int = 500, percentile: float = 99.0) -> None:
        self._window: deque[float] = deque(maxlen=int(window_size))
        self._percentile = float(percentile)
        self.n = 0

    def update(self, x: float) -> None:
        self._window.append(float(x))
        self.n += 1

    def threshold(self) -> float:
        if not self._window:
            return 0.0
        return float(np.percentile(list(self._window), self._percentile))


def classify_score_percentile(
    score: float | None,
    estimator: SlidingPercentileEstimator,
    *,
    is_warming_up: bool,
    min_samples: int,
) -> tuple[str, bool | None, float | None]:
    """Classify a score using the sliding-window percentile estimator."""
    if is_warming_up or score is None:
        return "warming_up", None, None

    value = float(score)
    effective_min_samples = max(2, int(min_samples))
    if estimator.n < effective_min_samples:
        estimator.update(value)
        return "warming_up", None, None

    threshold = estimator.threshold()
    is_anomaly = value > threshold
    if not is_anomaly:
        estimator.update(value)

    return ("anomaly" if is_anomaly else "normal"), is_anomaly, threshold


def classify_score(
    score: float | None,
    estimator: WelfordEstimator,
    *,
    is_warming_up: bool,
    min_samples: int,
    k_sigma: float,
) -> tuple[str, bool | None, float | None]:
    """Classify a score and update the estimator when appropriate."""
    if is_warming_up or score is None:
        return "warming_up", None, None

    value = float(score)
    effective_min_samples = max(2, int(min_samples))
    if estimator.n < effective_min_samples:
        estimator.update(value)
        return "warming_up", None, None

    threshold = estimator.threshold(k_sigma)
    is_anomaly = value > threshold
    if not is_anomaly:
        estimator.update(value)

    return ("anomaly" if is_anomaly else "normal"), is_anomaly, threshold
