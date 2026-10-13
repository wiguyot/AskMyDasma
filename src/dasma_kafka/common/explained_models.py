"""Contracts shared by model-specific explainability managers."""

from __future__ import annotations

import hashlib
import json
from collections import deque
from typing import Any, Callable, Protocol, Sequence

import numpy as np

from dasma_kafka.iforest_analysis.helpers import (
    parse_iforest_contamination,
    parse_iforest_max_samples,
)
from dasma_kafka.iforest_analysis.model import IForestGlobalModel, validate_iforest_params
from dasma_kafka.kitnet_analysis.helpers import (
    parse_grace_anomaly_detector,
    parse_grace_feature_mapping,
)
from dasma_kafka.kitnet_analysis.model import KitNetGroupModels, KitNetParams


class ExplainedModelManager(Protocol):
    """Common interface implemented by V1 and V2 model managers."""

    reference_score_field: str
    reference_status_field: str
    reference_anomaly_field: str
    reference_threshold_field: str
    model_version: str
    supported_methods: Sequence[str]
    mismatch_code: str

    def fit_score(self, group: str, x: np.ndarray) -> float | None:
        """Update a group's model and return its local score, if initialized."""
        ...

    def predict_fn(self, group: str) -> Callable[[np.ndarray], np.ndarray]:
        """Return a pure scoring function for the current group model state."""
        ...

    def get_background_data(self, group: str) -> np.ndarray | None:
        ...

    def get_model(self, group: str) -> object | None:
        ...

    def model_fingerprint(self, feature_names: Sequence[str]) -> str:
        ...


class IForestModelManager:
    """Maintain one shared IForest mirror and SHAP background per group."""

    reference_score_field = "iforest_score"
    reference_status_field = "iforest_status"
    reference_anomaly_field = "iforest_anomaly"
    reference_threshold_field = "iforest_threshold"
    model_version = "local-iforest-v2"
    supported_methods = ("kernel", "tree")
    mismatch_code = "iforest_score_mismatch"

    def __init__(self, config: Any, logger) -> None:
        self.config = config
        self.logger = logger
        self.models: dict[str, IForestGlobalModel] = {}
        self.background: dict[str, deque[np.ndarray]] = {}
        self.tree_cache: dict[str, tuple[int, object]] = {}

    def _get_or_create(self, group: str, num_features: int) -> IForestGlobalModel:
        if group not in self.models:
            psi = parse_iforest_max_samples(str(self.config.iforest_max_samples))
            validate_iforest_params(
                self.config.iforest_initial_window_size,
                self.config.iforest_window_size,
                psi,
                self.config.iforest_n_estimators,
                self.config.iforest_refit_every,
            )
            contamination = parse_iforest_contamination(str(getattr(self.config, "iforest_contamination", "auto")))
            self.models[group] = IForestGlobalModel(
                num_features=num_features,
                initial_window_size=self.config.iforest_initial_window_size,
                window_size=self.config.iforest_window_size,
                random_state=self.config.hstree_seed,
                n_estimators=self.config.iforest_n_estimators,
                max_samples=psi,
                refit_every=self.config.iforest_refit_every,
                contamination=contamination,
            )
            self.logger.info(
                "Initialized explainability iForest mirror for Group=%r with %s features, "
                "initial_window_size=%s, window_size=%s, n_estimators=%s, max_samples=%s, "
                "refit_every=%s, random_state=%s",
                group,
                num_features,
                self.config.iforest_initial_window_size,
                self.config.iforest_window_size,
                self.config.iforest_n_estimators,
                psi,
                self.config.iforest_refit_every,
                self.config.hstree_seed,
            )
            self.background[group] = deque(maxlen=self.config.shap_background_size)
        return self.models[group]

    def fit_score(self, group: str, x: np.ndarray) -> float | None:
        values = np.asarray(x, dtype=np.float64)
        model = self._get_or_create(group, values.shape[0])
        score = model.partial_fit_score(values)
        self.background[group].append(values.copy())
        return None if model.is_warming_up else float(score)

    def predict_fn(self, group: str) -> Callable[[np.ndarray], np.ndarray]:
        model = self.models.get(group)
        if model is None or not model.is_ready:
            raise ValueError(f"IForest model for group '{group}' is not initialized")
        scoring_model = model.last_scoring_model
        if scoring_model is None:
            raise ValueError(f"IForest scoring model for group '{group}' is not available")

        def score_batch(X: np.ndarray) -> np.ndarray:
            values = np.asarray(X, dtype=np.float64)
            if values.ndim == 1:
                values = values.reshape(1, -1)
            if values.ndim != 2 or values.shape[1] != model.num_features:
                raise ValueError(
                    f"expected a 2D array with {model.num_features} features, got shape {values.shape}"
                )
            return -scoring_model.score_samples(values)

        return score_batch

    def predict_fn_for_scoring_event(self, group: str) -> Callable[[np.ndarray], np.ndarray]:
        """Explicit alias documenting that SHAP must use this event's scorer."""
        return self.predict_fn(group)

    def get_background_data(self, group: str) -> np.ndarray | None:
        values = self.background.get(group)
        if values is None or len(values) < self.config.shap_background_size:
            return None
        return np.asarray(list(values), dtype=np.float64)

    def get_model(self, group: str) -> object | None:
        model = self.models.get(group)
        return None if model is None else model.last_scoring_model

    def get_scoring_model(self, group: str) -> object | None:
        """Return the exact forest that scored this group's latest event."""
        return self.get_model(group)

    def get_scoring_generation(self, group: str) -> int | None:
        model = self.models.get(group)
        return None if model is None else model.last_scoring_generation

    def get_tree_model(self, group: str):
        """One converted scoring generation per group, never a background-bound explainer."""
        from .iforest_treeshap import convert_iforest

        forest = self.get_scoring_model(group)
        generation = self.get_scoring_generation(group)
        if forest is None or generation is None:
            raise ValueError("IForest scoring model is not available")
        entry = self.tree_cache.get(group)
        if entry is None or entry[0] != generation:
            entry = (generation, convert_iforest(forest))
            self.tree_cache[group] = entry
        return entry[1]

    def model_fingerprint(self, feature_names: Sequence[str]) -> str:
        import sklearn

        payload = {
            "model_version": self.model_version,
            "random_state": self.config.hstree_seed,
            "initial_window_size": self.config.iforest_initial_window_size,
            "window_size": self.config.iforest_window_size,
            "n_estimators": self.config.iforest_n_estimators,
            "max_samples": self.config.iforest_max_samples,
            "refit_every": self.config.iforest_refit_every,
            "contamination": parse_iforest_contamination(str(getattr(self.config, "iforest_contamination", "auto"))),
            "feature_names": list(feature_names),
            "sklearn_version": sklearn.__version__,
        }
        canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class KitNetModelManager:
    """Maintain one online KitNET mirror and valid SHAP background per group."""

    reference_score_field = "kitnet_score"
    reference_status_field = "kitnet_status"
    reference_anomaly_field = "kitnet_anomaly"
    reference_threshold_field = "kitnet_threshold"
    model_version = "local-kitnet-v1"
    supported_methods = ("kernel",)
    mismatch_code = "kitnet_score_mismatch"

    def __init__(self, config: Any, logger) -> None:
        self.config = config
        self.logger = logger
        params = KitNetParams(
            max_size_ae=int(config.kitnet_max_size_ae),
            grace_feature_mapping=parse_grace_feature_mapping(
                str(config.kitnet_grace_feature_mapping)
            ),
            grace_anomaly_detector=parse_grace_anomaly_detector(
                str(config.kitnet_grace_anomaly_detector)
            ),
            learning_rate=float(config.kitnet_learning_rate),
            hidden_ratio=float(config.kitnet_hidden_ratio),
        )
        self.params = params
        self.models = KitNetGroupModels(params)
        self.background: dict[str, deque[np.ndarray]] = {}
        self._background_size = max(1, int(config.shap_background_size))

    def fit_score(self, group: str, x: np.ndarray) -> float | None:
        """Update KitNET once, retaining only finite rows for the SHAP background."""
        values = np.asarray(x, dtype=np.float64)
        if group not in self.background:
            self.background[group] = deque(maxlen=self._background_size)

        score = self.models.score(group, values)
        if np.all(np.isfinite(values)):
            self.background[group].append(values.copy())
        if not self.models.is_ready(group):
            return None
        return float(score)

    def input_error(self, group: str) -> str | None:
        return "invalid_features" if self.models.last_input_invalid(group) else None

    def predict_fn(self, group: str) -> Callable[[np.ndarray], np.ndarray]:
        if not self.models.is_ready(group):
            raise ValueError(f"KitNET model for group '{group}' is not initialized")
        return lambda X: self.models.score_batch(group, X)

    def get_background_data(self, group: str) -> np.ndarray | None:
        values = self.background.get(group)
        if values is None or len(values) < self._background_size:
            return None
        return np.asarray(list(values), dtype=np.float64)

    def get_model(self, group: str) -> object | None:
        return self.models.models_by_group.get(group)

    def model_fingerprint(self, feature_names: Sequence[str]) -> str:
        from importlib.metadata import version

        payload = {
            "model_version": self.model_version,
            "max_size_ae": self.params.max_size_ae,
            "grace_feature_mapping": self.params.grace_feature_mapping,
            "grace_anomaly_detector": self.params.grace_anomaly_detector,
            "learning_rate": self.params.learning_rate,
            "hidden_ratio": self.params.hidden_ratio,
            "feature_names": list(feature_names),
            "pysad_version": version("pysad"),
            "numpy_version": version("numpy"),
            "scipy_version": version("scipy"),
        }
        canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
