"""Per-group PySAD KitNET model used by the detector and Explainability."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
from pysad import models


@dataclass
class KitNetParams:
    max_size_ae: int
    grace_feature_mapping: Optional[int]
    grace_anomaly_detector: int
    learning_rate: float
    hidden_ratio: float


class KitNetGroupModels:
    """Keep one online KitNET model and input state per group."""

    def __init__(self, params: KitNetParams):
        self.params = params
        self.models_by_group: Dict[str, models.KitNet] = {}
        self.seen_by_group: Dict[str, int] = {}
        self.feature_count_by_group: Dict[str, int] = {}
        self.last_input_invalid_by_group: Dict[str, bool] = {}

    @property
    def warmup_samples(self) -> int:
        return int(self.params.grace_feature_mapping or 0) + int(
            self.params.grace_anomaly_detector
        )

    def is_warming_up(self, group: str) -> bool:
        return self.seen_by_group.get(group, 0) <= self.warmup_samples

    def is_ready(self, group: str) -> bool:
        """Whether the feature map exists and this group's grace has elapsed.

        Invalid NaN inputs increment ``seen_by_group`` without reaching PySAD,
        so the grace counter alone does not guarantee that KitNET built its
        feature map.
        """
        model = self.models_by_group.get(group)
        core = None if model is None else getattr(model, "model", None)
        return (
            core is not None
            and core.v is not None
            and not self.is_warming_up(group)
        )

    def samples_seen(self, group: str) -> int:
        return self.seen_by_group.get(group, 0)

    def last_input_invalid(self, group: str) -> bool:
        return self.last_input_invalid_by_group.get(group, False)

    def _new_model(self) -> models.KitNet:
        return models.KitNet(
            max_size_ae=self.params.max_size_ae,
            grace_feature_mapping=self.params.grace_feature_mapping,
            grace_anomaly_detector=self.params.grace_anomaly_detector,
            learning_rate=self.params.learning_rate,
            hidden_ratio=self.params.hidden_ratio,
        )

    def score(self, group: str, x: np.ndarray) -> float:
        self.last_input_invalid_by_group[group] = bool(np.any(~np.isfinite(x)))

        if group not in self.models_by_group:
            self.models_by_group[group] = self._new_model()
            self.seen_by_group[group] = 0
            self.feature_count_by_group[group] = int(x.shape[0])

        if int(x.shape[0]) != self.feature_count_by_group.get(
            group, int(x.shape[0])
        ):
            print(
                f"[analyse-kitnet] Feature count changed for group={group!r}: "
                f"{self.feature_count_by_group.get(group)} -> {int(x.shape[0])}. Resetting model.",
                file=sys.stderr,
                flush=True,
            )
            self.models_by_group[group] = self._new_model()
            self.seen_by_group[group] = 0
            self.feature_count_by_group[group] = int(x.shape[0])

        model = self.models_by_group[group]
        if np.any(np.isnan(x)):
            self.seen_by_group[group] += 1
            return 0.0

        try:
            score = float(model.fit_score_partial(x))
        except Exception as exc:
            print(
                f"[analyse-kitnet] scoring error for group={group!r}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            score = 0.0

        self.seen_by_group[group] += 1
        return score

    def score_batch(self, group: str, X: np.ndarray) -> np.ndarray:
        """Score rows with the learned state, without calling KitNET.process.

        PySAD's ``execute`` increments its ``n_executed`` diagnostic counter,
        but does not train the model or alter later scores.
        """
        if not self.is_ready(group):
            raise ValueError(f"KitNET model for group '{group}' is not ready")
        values = np.asarray(X, dtype=np.float64)
        if values.ndim != 2:
            raise ValueError("KitNET score_batch expects a 2D array")
        model = self.models_by_group[group].model
        return np.asarray(
            [float(model.execute(row)) for row in values], dtype=np.float64
        )
