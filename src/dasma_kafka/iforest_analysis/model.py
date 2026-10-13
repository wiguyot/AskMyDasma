"""Shared streaming IsolationForest model used by analysis and explainability."""

from __future__ import annotations

from numbers import Integral
from typing import List, Optional

import numpy as np
from sklearn.ensemble import IsolationForest
from .helpers import parse_iforest_contamination


def _require_integer(name: str, value: object, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral) or int(value) < minimum:
        raise ValueError(f"iforest: {name} doit être un entier ≥ {minimum} (reçu : {value!r})")
    return int(value)


def validate_iforest_params(
    initial_window_size: int,
    window_size: int,
    max_samples: int,
    n_estimators: int,
    refit_every: int,
) -> None:
    """Reject configurations that cannot keep a fixed paper-sized subsample."""
    initial = _require_integer("initial_window_size", initial_window_size)
    window = _require_integer("window_size", window_size)
    psi = _require_integer("max_samples", max_samples, minimum=2)
    trees = _require_integer("n_estimators", n_estimators)
    cadence = _require_integer("refit_every", refit_every)
    if initial < psi:
        raise ValueError(
            "iforest: la fenêtre d'amorçage "
            f"({initial}) doit être au moins égale à la taille de sous-échantillon ψ ({psi}) : "
            "sinon scikit-learn réduit ψ et le sous-échantillonnage du papier disparaît"
        )
    if window < initial:
        raise ValueError(
            f"iforest: la fenêtre glissante ({window}) doit être au moins égale "
            f"à la fenêtre d'amorçage ({initial})"
        )
    # Keep the normalized values meaningful to static analyzers and document
    # that all five parameters were checked as positive integers above.
    _ = trees, cadence


class IForestGlobalModel:
    """Streaming Isolation Forest with fixed ψ and event-scoped scoring.

    A point is scored by the forest that existed before the point arrived. The
    point is then appended to the window and may trigger the next refit. The
    forest and generation that produced the latest score remain available to
    Explainability until it finishes describing that event.
    """

    def __init__(
        self,
        num_features: int,
        initial_window_size: int = 512,
        window_size: int = 2048,
        random_state: int = 42,
        n_estimators: int = 100,
        max_samples: int = 256,
        refit_every: int = 128,
        contamination: str | float = "auto",
    ):
        self.num_features = _require_integer("num_features", num_features)
        self.initial_window_size = _require_integer("initial_window_size", initial_window_size)
        self.window_size = _require_integer("window_size", window_size)
        self.random_state = int(random_state)
        self.n_estimators = _require_integer("n_estimators", n_estimators)
        self.max_samples = _require_integer("max_samples", max_samples, minimum=2)
        self.refit_every = _require_integer("refit_every", refit_every)
        self.contamination = parse_iforest_contamination(contamination)
        validate_iforest_params(
            self.initial_window_size,
            self.window_size,
            self.max_samples,
            self.n_estimators,
            self.refit_every,
        )

        self._window: List[List[float]] = []
        self._model: Optional[IsolationForest] = None
        self._since_refit = 0
        self._generation = 0
        self._last_event_scored = False
        self._last_scoring_model: Optional[IsolationForest] = None
        self._last_scoring_generation: Optional[int] = None

    @property
    def is_warming_up(self) -> bool:
        """Whether the latest event received the 0.0 warmup sentinel."""
        return not self._last_event_scored

    @property
    def is_ready(self) -> bool:
        """Whether a fitted forest is available for scoring."""
        return self._model is not None

    @property
    def samples_buffered(self) -> int:
        return len(self._window)

    @property
    def generation(self) -> int:
        """Return the number of successfully fitted forest generations."""
        return self._generation

    @property
    def current_model(self) -> Optional[IsolationForest]:
        """Return the latest fitted sklearn forest, if one exists."""
        return self._model

    @property
    def last_scoring_model(self) -> Optional[IsolationForest]:
        """Return the forest that produced the latest non-sentinel score."""
        return self._last_scoring_model

    @property
    def last_scoring_generation(self) -> Optional[int]:
        """Return the generation that produced the latest non-sentinel score."""
        return self._last_scoring_generation

    @property
    def last_scoring_threshold(self) -> Optional[float]:
        """Native cutoff on our positive score scale, for the latest event."""
        if self._last_scoring_model is None:
            return None
        return float(-self._last_scoring_model.offset_)

    def _ensure_vector(self, x) -> List[float]:
        vec = list(x)
        if len(vec) != self.num_features:
            if len(vec) < self.num_features:
                vec = vec + [0.0] * (self.num_features - len(vec))
            else:
                vec = vec[: self.num_features]
        return vec

    def _fit_next_generation(self) -> None:
        X = np.asarray(self._window, dtype=np.float64)
        model = IsolationForest(
            n_estimators=self.n_estimators,
            max_samples=self.max_samples,
            contamination=self.contamination,
            random_state=self.random_state,
            n_jobs=1,
        )
        model.fit(X)
        self._model = model
        self._since_refit = 0
        self._generation += 1

    def partial_fit_score(self, x) -> float:
        """Score first with the existing forest, then learn/refit with ``x``."""
        vec = self._ensure_vector(x)
        scoring_model = self._model
        score: Optional[float] = None
        if scoring_model is not None:
            score = float(-scoring_model.score_samples(np.asarray([vec], dtype=np.float64))[0])
            self._since_refit += 1
            self._last_scoring_model = scoring_model
            self._last_scoring_generation = self._generation
        else:
            self._last_scoring_model = None
            self._last_scoring_generation = None

        self._window.append(vec)
        if len(self._window) > self.window_size:
            self._window = self._window[-self.window_size :]

        if len(self._window) >= self.initial_window_size and (
            self._model is None or self._since_refit >= self.refit_every
        ):
            self._fit_next_generation()

        self._last_event_scored = score is not None
        return 0.0 if score is None else score

    def score_batch(self, X) -> np.ndarray:
        """Score rows without changing the sliding window or refit state."""
        if not self.is_ready or self._model is None:
            raise RuntimeError("IsolationForest model is not ready")

        values = np.asarray(X, dtype=np.float64)
        if values.ndim == 1:
            values = values.reshape(1, -1)
        if values.ndim != 2 or values.shape[1] != self.num_features:
            raise ValueError(
                f"expected a 2D array with {self.num_features} features, "
                f"got shape {values.shape}"
            )
        return -self._model.score_samples(values)
