"""SHAP computation and reproducibility helpers."""

from __future__ import annotations

import time
import zlib
from typing import TYPE_CHECKING, Any, Callable, Dict, List

from .config import ExplainabilityConfig

if TYPE_CHECKING:
    import numpy as np


def stable_crc32(value: str) -> int:
    """Simple stable hash used to derive the per-group seed."""
    return int(zlib.crc32(value.encode("utf-8")) & 0xFFFFFFFF)


def seed_for_message(base_seed: int, group: str, partition: int, offset: int) -> int:
    """Deterministic seed used to stabilize KernelSHAP."""
    group_crc = stable_crc32(group)
    return (base_seed + group_crc + (partition * 1_000_003) + (offset * 1_000_033)) & 0xFFFFFFFF


class SHAPExplainer:
    """Encapsulate KernelSHAP computation without changing its logic."""

    def __init__(self, config: ExplainabilityConfig, logger) -> None:
        self.config = config
        self.logger = logger
        self.feature_names: List[str] = []

    def set_feature_names(self, names: List[str]) -> None:
        self.feature_names = names
        self.logger.info("Features configured for SHAP: %s features", len(names))
        self.logger.debug("Feature names: %s...", names[:10])

    def _create_predict_function(self, predict_fn: Callable[[Any], Any]):
        return predict_fn

    def compute_tree_values(self, model_manager, group, features, background_data):
        """Explain the event-scoped forest; include lazy conversion in published duration."""
        import numpy as np
        from dasma_kafka.common.iforest_treeshap import explain_tree, mean_path_length

        start = time.perf_counter()
        converted = model_manager.get_tree_model(group)
        phi, base, value = explain_tree(converted, features, background_data)
        independent = float(mean_path_length(model_manager.get_scoring_model(group), features)[0])
        if not np.isclose(value, independent, atol=1e-12, rtol=1e-12):
            raise ValueError("TreeSHAP does not explain the scoring forest")
        return {
            "shap_values": dict(zip(self.feature_names, map(float, phi))),
            "shap_base_value": base,
            "shap_computation_time_ms": round((time.perf_counter()-start)*1000, 2),
            "explained_value": independent,
            "explanation_output": "iforest_mean_path_length",
        }

    def compute_shap_values(
        self,
        predict_fn: Callable[[Any], Any],
        features: np.ndarray,
        background_data: np.ndarray,
    ) -> Dict[str, Any]:
        """Compute SHAP contributions for a single observation."""
        import shap

        start_time = time.time()

        try:
            scoring_fn = self._create_predict_function(predict_fn)
            explainer = shap.KernelExplainer(scoring_fn, background_data)
            shap_values = explainer.shap_values(
                features.reshape(1, -1),
                nsamples=self.config.shap_nsamples,
                silent=True,
            )

            computation_time_ms = (time.time() - start_time) * 1000
            shap_dict = {}
            for index, name in enumerate(self.feature_names):
                shap_dict[name] = float(shap_values[0][index])

            return {
                "shap_values": shap_dict,
                "shap_base_value": float(explainer.expected_value),
                "shap_computation_time_ms": round(computation_time_ms, 2),
            }

        except Exception as exc:
            self.logger.error("Error while computing SHAP: %s", exc)
            computation_time_ms = (time.time() - start_time) * 1000
            return {
                "shap_values": None,
                "shap_base_value": None,
                "shap_computation_time_ms": round(computation_time_ms, 2),
                "shap_error": str(exc),
            }
