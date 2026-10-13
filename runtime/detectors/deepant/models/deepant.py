"""DeepAnT model — PyTorch CPU-only wrapper for streaming anomaly detection.

Architecture faithful to Munir, Siddiqui, Dengel & Ahmed (2019).
Corrects defects D1-D5 identified in the historical dasma implementation:

  D1  CNN architecture: Conv1D(k=2) -> MaxPool -> Conv1D(k=2) -> Dense -> Dropout -> Dense.
  D2  Min-Max normalization is recomputed on the rolling window at every refit.
  D3  Score is the L2 distance between prediction and target (torch.dist, p=2).
  D4  Explicit warmup: an initial training pass runs on the warmup buffer.
  D5  PyTorch CPU-only (no TensorFlow dependency).
"""

from __future__ import annotations

from collections import deque
from typing import Iterable, Optional

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F

from dasma_kafka.anomaly.thresholds import (
    SlidingPercentileEstimator,
    WelfordEstimator,
    WindowedWelfordEstimator,
    classify_score,
    classify_score_percentile,
)


def classify_deepant_score(
    deepant_score: float | None,
    estimator: WelfordEstimator | WindowedWelfordEstimator,
    *,
    is_warming_up: bool,
    min_samples: int,
    k_sigma: float,
) -> tuple[str, Optional[bool], Optional[float]]:
    """Classify a DeepAnT score with the Welford mean+k*std threshold."""
    return classify_score(
        deepant_score,
        estimator,
        is_warming_up=is_warming_up,
        min_samples=min_samples,
        k_sigma=k_sigma,
    )


def classify_deepant_score_percentile(
    deepant_score: float | None,
    estimator: SlidingPercentileEstimator,
    *,
    is_warming_up: bool,
    min_samples: int,
) -> tuple[str, Optional[bool], Optional[float]]:
    """Classify a DeepAnT score with the sliding-window percentile threshold."""
    return classify_score_percentile(
        deepant_score,
        estimator,
        is_warming_up=is_warming_up,
        min_samples=min_samples,
    )


class DeepAntCNN(nn.Module):
    """CNN predictor faithful to Munir et al. (2019).

    Input  : (batch, lookback_size, n_features)
    Output : (batch, n_features)  -- prediction of the next sample.
    """

    def __init__(
        self,
        lookback_size: int,
        n_features: int,
        filters: int = 32,
        kernel_size: int = 2,
        dropout: float = 0.25,
        pool_size: int = 2,
    ) -> None:
        super().__init__()
        if lookback_size < 8:
            raise ValueError(
                f"lookback_size must be >= 8 (got {lookback_size}); two Conv1D(k=2) "
                f"with MaxPool(2) collapse smaller windows."
            )
        if n_features < 1:
            raise ValueError(f"n_features must be >= 1 (got {n_features})")

        self.lookback_size = lookback_size
        self.n_features = n_features
        self.filters = filters
        self.kernel_size = kernel_size
        self.pool_size = pool_size

        # PyTorch Conv1d expects (batch, channels, length); channels = n_features.
        self.conv1 = nn.Conv1d(
            in_channels=n_features,
            out_channels=filters,
            kernel_size=kernel_size,
        )
        self.pool = nn.MaxPool1d(kernel_size=pool_size)
        self.conv2 = nn.Conv1d(
            in_channels=filters,
            out_channels=filters,
            kernel_size=kernel_size,
        )

        l1 = lookback_size - (kernel_size - 1)
        l2 = l1 // pool_size
        l3 = l2 - (kernel_size - 1)
        if l3 <= 0:
            raise ValueError(
                f"lookback_size={lookback_size} is too small for the CNN chain "
                f"(kernel_size={kernel_size}, pool_size={pool_size}): the temporal "
                f"length collapses to {l3} after conv->pool->conv."
            )
        self._fc1_in = filters * l3

        self.fc1 = nn.Linear(self._fc1_in, 40)
        self.dropout = nn.Dropout(dropout)
        self.fc2 = nn.Linear(40, n_features)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = x.permute(0, 2, 1)
        x = F.relu(self.conv1(x))
        x = self.pool(x)
        x = F.relu(self.conv2(x))
        x = x.flatten(1)
        x = F.relu(self.fc1(x))
        x = self.dropout(x)
        return self.fc2(x)


class DeepAntStreamingModel:
    """DeepAnT streaming wrapper.

    Lifecycle:
      WARMUP  buffer the first `lookback_size + initial_window_size` samples;
              on the sample that completes the buffer, compute Min-Max stats,
              train the CNN on `initial_window_size` (window, target) pairs,
              switch to READY. partial_fit_score returns 0.0 during warmup.
      READY   each new sample is scored via L2 distance between the model
              prediction and the normalized target. Every `refit_every` scored
              samples, refit on the last `window_size` samples (Min-Max stats
              refreshed on that same window).
              Callers that classify the score outside this wrapper can pass
              `learn=False`, then call `accept_sample()` only for samples that
              should enter the rolling refit history.
    """

    def __init__(
        self,
        n_features: int,
        lookback_size: int = 100,
        initial_window_size: int = 512,
        refit_every: int = 64,
        window_size: int = 2048,
        filters: int = 32,
        kernel_size: int = 2,
        dropout: float = 0.25,
        lr: float = 1e-3,
        epochs_initial: int = 10,
        epochs_refit: int = 5,
        batch_size: int = 32,
        seed: int = 42,
    ) -> None:
        if lookback_size < 8:
            raise ValueError(f"lookback_size must be >= 8 (got {lookback_size})")
        if initial_window_size < lookback_size:
            raise ValueError(
                f"initial_window_size must be >= lookback_size "
                f"({initial_window_size} < {lookback_size})"
            )
        if window_size < lookback_size + 1:
            raise ValueError(
                f"window_size must be >= lookback_size + 1 "
                f"({window_size} < {lookback_size + 1})"
            )
        if n_features < 1:
            raise ValueError(f"n_features must be >= 1 (got {n_features})")

        self.n_features = int(n_features)
        self.lookback_size = int(lookback_size)
        self.initial_window_size = int(initial_window_size)
        self.refit_every = max(1, int(refit_every))
        self.window_size = int(window_size)
        self.filters = int(filters)
        self.kernel_size = int(kernel_size)
        self.dropout = float(dropout)
        self.lr = float(lr)
        self.epochs_initial = max(1, int(epochs_initial))
        self.epochs_refit = max(1, int(epochs_refit))
        self.batch_size = max(1, int(batch_size))
        self.seed = int(seed)

        torch.manual_seed(self.seed)
        np.random.seed(self.seed)

        self._model: DeepAntCNN | None = None
        self._optimizer: torch.optim.Optimizer | None = None
        # During warmup: unbounded deque accumulating samples.
        # In READY mode: bounded to window_size (rolling refit history).
        self._buffer: deque[list[float]] = deque()
        self._norm_min: np.ndarray | None = None
        self._norm_max: np.ndarray | None = None
        self._n_scored: int = 0

    @property
    def is_warming_up(self) -> bool:
        return self._model is None

    @property
    def samples_buffered(self) -> int:
        return len(self._buffer)

    @property
    def n_scored(self) -> int:
        return self._n_scored

    def _ensure_vector(self, x: Iterable[float]) -> list[float]:
        arr = [float(v) for v in x]
        if len(arr) != self.n_features:
            raise ValueError(
                f"Unexpected feature dimension for DeepAntStreamingModel: "
                f"expected {self.n_features}, got {len(arr)}"
            )
        return arr

    @staticmethod
    def _compute_minmax(data: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        return data.min(axis=0), data.max(axis=0)

    def _normalize(self, data: np.ndarray) -> np.ndarray:
        """Min-Max normalize with the current stats; constant features map to 0."""
        eps = 1e-12
        span = self._norm_max - self._norm_min
        safe_span = np.where(span < eps, 1.0, span)
        normed = (data - self._norm_min) / safe_span
        normed = np.where(span < eps, 0.0, normed)
        return normed.astype(np.float32, copy=False)

    def _build_pairs(self, samples: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Build (windows, targets) supervised pairs from an (N, d) array."""
        n = samples.shape[0]
        if n <= self.lookback_size:
            raise ValueError(
                f"not enough samples to build pairs: {n} <= {self.lookback_size}"
            )
        n_pairs = n - self.lookback_size
        # (n - L + 1, d, L) -> (n - L + 1, L, d), then truncate to n_pairs.
        windows = np.lib.stride_tricks.sliding_window_view(
            samples, window_shape=self.lookback_size, axis=0
        ).transpose(0, 2, 1)[:n_pairs]
        targets = samples[self.lookback_size : self.lookback_size + n_pairs]
        return np.ascontiguousarray(windows), np.ascontiguousarray(targets)

    def _train(self, windows: np.ndarray, targets: np.ndarray, epochs: int) -> None:
        assert self._model is not None and self._optimizer is not None
        loss_fn = nn.MSELoss()
        windows_t = torch.from_numpy(windows.astype(np.float32, copy=False))
        targets_t = torch.from_numpy(targets.astype(np.float32, copy=False))
        n = windows_t.shape[0]
        self._model.train()
        for _ in range(epochs):
            perm = torch.randperm(n)
            for start in range(0, n, self.batch_size):
                idx = perm[start : start + self.batch_size]
                self._optimizer.zero_grad()
                pred = self._model(windows_t[idx])
                loss = loss_fn(pred, targets_t[idx])
                loss.backward()
                self._optimizer.step()
        self._model.eval()

    def _initialize_model(self) -> None:
        raw = np.asarray(list(self._buffer), dtype=np.float64)
        self._norm_min, self._norm_max = self._compute_minmax(raw)
        normalized = self._normalize(raw)
        windows, targets = self._build_pairs(normalized)

        torch.manual_seed(self.seed)
        self._model = DeepAntCNN(
            lookback_size=self.lookback_size,
            n_features=self.n_features,
            filters=self.filters,
            kernel_size=self.kernel_size,
            dropout=self.dropout,
        )
        self._optimizer = torch.optim.Adam(self._model.parameters(), lr=self.lr)
        self._train(windows, targets, self.epochs_initial)

        history = list(self._buffer)
        self._buffer = deque(history, maxlen=self.window_size)

    def _refit(self) -> None:
        raw = np.asarray(list(self._buffer), dtype=np.float64)
        if raw.shape[0] <= self.lookback_size:
            return
        self._norm_min, self._norm_max = self._compute_minmax(raw)
        normalized = self._normalize(raw)
        windows, targets = self._build_pairs(normalized)
        self._train(windows, targets, self.epochs_refit)

    def _score_sample(self, vec: list[float]) -> float:
        assert self._model is not None
        history = np.asarray(
            list(self._buffer)[-self.lookback_size :], dtype=np.float64
        )
        target = np.asarray(vec, dtype=np.float64).reshape(1, -1)
        normalized_window = self._normalize(history)
        normalized_target = self._normalize(target)[0]
        window_t = (
            torch.from_numpy(normalized_window).unsqueeze(0).to(torch.float32)
        )
        target_t = torch.from_numpy(normalized_target).to(torch.float32)
        with torch.no_grad():
            pred = self._model(window_t).squeeze(0)
            return float(torch.dist(pred, target_t, p=2))

    def _append_warmup_sample(self, vec: list[float]) -> None:
        self._buffer.append(vec)
        required = self.lookback_size + self.initial_window_size
        if len(self._buffer) >= required:
            self._initialize_model()

    def _append_ready_sample(self, vec: list[float]) -> None:
        self._buffer.append(vec)
        self._n_scored += 1
        if self._n_scored % self.refit_every == 0:
            self._refit()

    def accept_sample(self, x: Iterable[float]) -> None:
        """Add a sample to the training/refit history without re-scoring it."""
        vec = self._ensure_vector(x)

        if self._model is None:
            self._append_warmup_sample(vec)
            return

        self._append_ready_sample(vec)

    def partial_fit_score(self, x: Iterable[float], *, learn: bool = True) -> float:
        """Process a new sample. Returns 0.0 in warmup, L2 score in READY."""
        vec = self._ensure_vector(x)

        if self._model is None:
            self._append_warmup_sample(vec)
            return 0.0

        score = self._score_sample(vec)
        if learn:
            self._append_ready_sample(vec)
        return score
