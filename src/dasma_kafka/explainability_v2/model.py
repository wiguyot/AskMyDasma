"""Local management of HSTree models by group (explainability-v2).

Fine copy of the regular explainability model module. Identical behavior; kept in
the v2 package so v2 stays isolated from regular.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

import numpy as np
from pysad.models import HalfSpaceTrees

from dasma_kafka.common import isolated_rng
from dasma_kafka.hstree_analysis.paper_model import create_hstree_model

from .config import ExplainabilityV2Config


@dataclass
class GroupModel:
    """State of a group's model: warmup, model, and SHAP buffer."""

    model: Optional[HalfSpaceTrees]
    warmup_buffer: List[np.ndarray] = field(default_factory=list)
    is_initialized: bool = False
    initial_window_size: int = 256
    background_buffer: deque = field(default_factory=lambda: deque(maxlen=100))
    warmup_event_ids: List[str] = field(default_factory=list)


class HSTreeModelManager:
    """Maintain a separate HSTree model for each group."""

    def __init__(self, config: ExplainabilityV2Config, logger: logging.Logger, initialization_observer: Optional[Callable[..., None]] = None):
        self.config = config
        self.logger = logger
        self.models: Dict[str, GroupModel] = {}
        self.initialization_observer = initialization_observer
        self.model_version = (
            "local-hstree-paper-v1" if config.hstree_variant == "paper" else "local-hstree-v3"
        )

    reference_score_field = "hstree_score"
    reference_status_field = "hstree_status"
    reference_anomaly_field = "hstree_anomaly"
    reference_threshold_field = "hstree_threshold"
    supported_methods = ("kernel",)
    mismatch_code = "hstree_score_mismatch"

    def get_or_create_model(self, group: str) -> GroupModel:
        if group not in self.models:
            self.logger.info("Creating a new model for group '%s'", group)
            self.models[group] = GroupModel(
                model=None,
                warmup_buffer=[],
                is_initialized=False,
                initial_window_size=self.config.hstree_initial_window_size,
                background_buffer=deque(maxlen=self.config.shap_background_size),
            )
        return self.models[group]

    def _create_model(
        self,
        n_features: int,
        feature_mins: np.ndarray,
        feature_maxes: np.ndarray,
        initial_window_x: Optional[List[List[float]]] = None,
    ) -> HalfSpaceTrees:
        with isolated_rng(self.config.hstree_seed):
            return create_hstree_model(
                self.config.hstree_variant,
                window_size=self.config.hstree_window_size,
                num_trees=self.config.hstree_num_trees,
                max_depth=self.config.hstree_max_depth,
                feature_mins=feature_mins,
                feature_maxes=feature_maxes,
                initial_window_X=initial_window_x,
            )

    def fit_only(self, group: str, features: np.ndarray) -> bool:
        """Update the group's model without computing a score.

        Returns False through the reference-window event; the following event is the
        first one that updates an initialized model.
        Maintains the background buffer exactly like fit_score so the model remains
        usable as a SHAP predict function after switching from fit_score to fit_only.
        """
        group_model = self.get_or_create_model(group)

        if not group_model.is_initialized:
            group_model.warmup_buffer.append(features.tolist())

            if len(group_model.warmup_buffer) >= group_model.initial_window_size:
                warmup_array = np.array(group_model.warmup_buffer)
                feature_mins = warmup_array.min(axis=0)
                feature_maxes = warmup_array.max(axis=0)

                self.logger.info(
                    "Group '%s': warmup finished with %s observations, initializing the HSTree model",
                    group,
                    len(group_model.warmup_buffer),
                )

                group_model.model = self._create_model(
                    n_features=len(features),
                    feature_mins=feature_mins,
                    feature_maxes=feature_maxes,
                    initial_window_x=group_model.warmup_buffer,
                )

                for buffered_features in group_model.warmup_buffer:
                    group_model.background_buffer.append(np.array(buffered_features))

                group_model.warmup_buffer = []
                group_model.is_initialized = True

                return False

            return False

        group_model.model.fit_partial(features)
        group_model.background_buffer.append(features)
        return True

    def fit_score(self, group: str, features: np.ndarray, *, source_event_id: Optional[str] = None, trace_context: Optional[Dict[str, Any]] = None) -> Optional[float]:
        """Update the group's model and return the local score."""
        group_model = self.get_or_create_model(group)

        if not group_model.is_initialized:
            group_model.warmup_buffer.append(features.tolist())
            if self.initialization_observer is not None and source_event_id is not None:
                group_model.warmup_event_ids.append(source_event_id)

            if len(group_model.warmup_buffer) >= group_model.initial_window_size:
                warmup_array = np.array(group_model.warmup_buffer)
                feature_mins = warmup_array.min(axis=0)
                feature_maxes = warmup_array.max(axis=0)

                if self.initialization_observer is not None:
                    self.initialization_observer(
                        group=group, source_event_id=source_event_id,
                        trace_context=trace_context or {},
                        warmup_event_ids=list(group_model.warmup_event_ids),
                        warmup_values=list(group_model.warmup_buffer),
                        feature_mins=feature_mins.copy(), feature_maxes=feature_maxes.copy(),
                    )

                self.logger.info(
                    "Group '%s': warmup finished with %s observations, initializing the HSTree model",
                    group,
                    len(group_model.warmup_buffer),
                )

                group_model.model = self._create_model(
                    n_features=len(features),
                    feature_mins=feature_mins,
                    feature_maxes=feature_maxes,
                    initial_window_x=group_model.warmup_buffer,
                )

                for buffered_features in group_model.warmup_buffer:
                    group_model.background_buffer.append(np.array(buffered_features))

                group_model.warmup_buffer = []
                group_model.warmup_event_ids = []
                group_model.is_initialized = True

                return None

            return None

        score = group_model.model.score_partial(features)
        group_model.model.fit_partial(features)
        group_model.background_buffer.append(features)
        return float(score)

    def get_background_data(self, group: str) -> Optional[np.ndarray]:
        if group not in self.models:
            return None

        group_model = self.models[group]
        if len(group_model.background_buffer) < self.config.shap_background_size:
            return None

        return np.array(list(group_model.background_buffer))

    def get_model(self, group: str) -> Optional[HalfSpaceTrees]:
        if group not in self.models or not self.models[group].is_initialized:
            return None
        return self.models[group].model

    def predict_fn(self, group: str) -> Callable[[np.ndarray], np.ndarray]:
        model = self.get_model(group)
        if model is None:
            raise ValueError(f"HSTree model for group '{group}' is not initialized")

        def predict(values: np.ndarray) -> np.ndarray:
            matrix = np.asarray(values, dtype=np.float64)
            if matrix.ndim == 1:
                matrix = matrix.reshape(1, -1)
            return np.asarray([model.score_partial(row) for row in matrix], dtype=np.float64)

        return predict

    def model_fingerprint(self, feature_names: Sequence[str]) -> str:
        from .features import compute_hstree_model_fingerprint

        del feature_names
        return compute_hstree_model_fingerprint(
            hstree_model_version=self.model_version,
            hstree_window_size=self.config.hstree_window_size,
            hstree_num_trees=self.config.hstree_num_trees,
            hstree_max_depth=self.config.hstree_max_depth,
            hstree_initial_window_size=self.config.hstree_initial_window_size,
            hstree_seed=self.config.hstree_seed,
        )
