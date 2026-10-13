"""HSTree model wrapper used by hstree_analysis.py.

This module provides `HSTreeGlobalModel`, a small wrapper around PySAD's
`HalfSpaceTrees` to support an initial warmup window (to compute feature-wise
mins/maxes) followed by online fit+score.
"""

from typing import Any, Callable, Dict, List, Optional, Iterable

import numpy as np
from dasma_kafka.common import isolated_rng
from dasma_kafka.hstree_analysis.paper_model import create_hstree_model
from dasma_kafka.anomaly.thresholds import (
  WelfordEstimator,
  WindowedWelfordEstimator,
  SlidingPercentileEstimator,
  classify_score,
  classify_score_percentile,
)
from pysad.models import HalfSpaceTrees


def classify_hstree_score(
  hstree_score: float | None,
  estimator: WelfordEstimator | WindowedWelfordEstimator,
  *,
  is_warming_up: bool,
  min_samples: int,
  k_sigma: float,
) -> tuple[str, Optional[bool], Optional[float]]:
  """Classify an HSTree score with the Welford mean+k*std threshold."""
  return classify_score(
    hstree_score,
    estimator,
    is_warming_up=is_warming_up,
    min_samples=min_samples,
    k_sigma=k_sigma,
  )


def classify_hstree_score_percentile(
  hstree_score: float | None,
  estimator: SlidingPercentileEstimator,
  *,
  is_warming_up: bool,
  min_samples: int,
) -> tuple[str, Optional[bool], Optional[float]]:
  """Classify an HSTree score with the sliding-window percentile threshold."""
  return classify_score_percentile(
    hstree_score,
    estimator,
    is_warming_up=is_warming_up,
    min_samples=min_samples,
  )


class HSTreeGlobalModel:
  """
  Simple global HSTree model (no Group separation).

  - Maintains a single HalfSpaceTrees instance for all data.
  - Buffers an initial window of instances in order to:
    * infer feature_mins / feature_maxes,
    * provide an initial calibration window to the model.
  - After initialization, each call to `partial_fit_score` scores against the
    current reference, then counts the instance (paper order).
  """

  def __init__(
    self,
    num_features: int,
    window_size: int = 256,
    num_trees: int = 25,
    max_depth: int = 15,
    initial_window_size: int = 256,
    seed: int = 42,
    variant: str = "paper",
    initialization_observer: Optional[Callable[..., None]] = None,
    feature_names: Optional[List[str]] = None,
  ) -> None:
    self.num_features = num_features
    self.window_size = window_size
    self.num_trees = num_trees
    self.max_depth = max_depth
    self.initial_window_size = initial_window_size
    self.seed = int(seed)
    if variant not in {"pysad", "paper"}:
      raise ValueError(f"unknown HSTree variant: {variant!r}")
    self.variant = variant
    self.initialization_observer = initialization_observer
    self.feature_names = list(feature_names or [])

    # Buffer of initial instances as list-of-lists; this is also used as
    # `initial_window_X` for HalfSpaceTrees. We keep it as a Python list so
    # that the truthiness check in PySAD's implementation (`if initial_window_X`)
    # works without raising on numpy arrays.
    self._buffer: List[List[float]] = []
    self._warmup_event_ids: List[str] = []
    self._initialization_context: Dict[str, Any] = {}
    self._model: Optional[HalfSpaceTrees] = None
    self._initialization_event_pending = False

  @property
  def is_warming_up(self) -> bool:
    return self._model is None or self._initialization_event_pending

  @property
  def samples_buffered(self) -> int:
    return len(self._buffer)

  def _ensure_vector(self, x: Iterable[float]) -> List[float]:
    """Convert input to a plain list of floats and validate its length."""
    arr = list(float(v) for v in x)
    if len(arr) != self.num_features:
      raise ValueError(
        f"Unexpected feature dimension for HSTreeGlobalModel: "
        f"expected {self.num_features}, got {len(arr)}"
      )
    return arr

  def _initialize_model_if_ready(self) -> None:
    """
    Instantiate HalfSpaceTrees once the initial buffer is large enough.

    We compute feature-wise mins/maxes over the buffered instances and pass
    the *list-of-lists* buffer as `initial_window_X` so that PySAD can
    perform its initial calibration.
    """
    if self._model is not None:
      return
    if len(self._buffer) < self.initial_window_size:
      return

    X_init = np.asarray(self._buffer, dtype=np.float64)
    feature_mins = X_init.min(axis=0)
    feature_maxes = X_init.max(axis=0)

    if self.initialization_observer is not None:
      self.initialization_observer(
        context=self._initialization_context,
        warmup_event_ids=list(self._warmup_event_ids),
        warmup_values=list(self._buffer),
        feature_mins=feature_mins.copy(),
        feature_maxes=feature_maxes.copy(),
      )

    with isolated_rng(self.seed):
      self._model = create_hstree_model(
        self.variant,
        feature_mins=feature_mins,
        feature_maxes=feature_maxes,
        window_size=self.window_size,
        num_trees=self.num_trees,
        max_depth=self.max_depth,
        initial_window_X=self._buffer,  # keep as list-of-lists
      )

    # We no longer need the buffer once the model is initialized.
    self._buffer = []

  def partial_fit_score(
    self,
    x: Iterable[float],
    *,
    source_event_id: Optional[str] = None,
    trace_context: Optional[Dict[str, Any]] = None,
  ) -> float:
    """
    Score against the current reference, then fit one instance (paper order).

    During the warmup phase (before the internal model is created), this
    method returns 0.0.
    """
    # The call after the initialization event starts normal online scoring.
    self._initialization_event_pending = False
    vec = self._ensure_vector(x)

    if self._model is None:
      # Buffer during warmup.
      self._buffer.append(vec)
      if self.initialization_observer is not None and source_event_id is not None:
        self._warmup_event_ids.append(source_event_id)
      self._initialization_context = trace_context or {}
      self._initialize_model_if_ready()
      if self._model is None:
        return 0.0
      # PySAD has already consumed the whole initial reference window during
      # construction. Do not feed the boundary event through it a second time.
      self._initialization_event_pending = True
      return 0.0
    self._warmup_event_ids = []

    # Paper order: score against the current reference, then count the event.
    x = np.asarray(vec, dtype=np.float64)
    score = self._model.score_partial(x)
    self._model.fit_partial(x)
    return float(score)
