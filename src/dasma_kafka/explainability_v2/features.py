"""Feature extraction and output message construction (explainability-v2).

Fine copy of the regular explainability features module. Identical behavior; kept
in the v2 package so v2 stays isolated from regular.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

from dasma_kafka.common.timestamps import resolve_timestamp_value

from dasma_kafka.iforest_analysis.helpers import build_feature_vector, infer_feature_names
from dasma_kafka.kitnet_analysis.helpers import select_features

from .config import (
    EXCLUDED_COLUMNS,
    EXPLANATION_STATUSES,
    IFOREST_EXCLUDE_FIELDS,
    KITNET_EXCLUDE_FIELDS,
)


class FeatureExtractor:
    """Extract the useful numeric columns from a Kafka message."""

    def __init__(self, logger, excluded_columns: Optional[set[str]] = None) -> None:
        self.logger = logger
        self.excluded_columns = set(EXCLUDED_COLUMNS if excluded_columns is None else excluded_columns)
        self.feature_names: List[str] = []
        self.is_initialized = False

    def _is_numeric(self, value: Any) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    def initialize_from_record(self, record: Dict[str, Any]) -> None:
        """Determine the ordered feature list once and for all."""
        self.feature_names = []

        for key, value in record.items():
            if key in self.excluded_columns:
                continue
            if self._is_numeric(value):
                self.feature_names.append(key)

        self.feature_names = sorted(self.feature_names)
        self.is_initialized = True
        self.logger.info(
            "Extracted features: %s numeric columns",
            len(self.feature_names),
        )
        self.logger.debug("Feature list: %s", self.feature_names)

    def extract(self, record: Dict[str, Any]) -> np.ndarray:
        """Build the numeric vector in the order of the selected features."""
        import numpy as np

        if not self.is_initialized:
            raise ValueError(
                "FeatureExtractor not initialized. Call initialize_from_record() first."
            )

        values = []
        for name in self.feature_names:
            raw_value = record.get(name, 0.0)
            try:
                values.append(float(raw_value) if raw_value is not None else 0.0)
            except (ValueError, TypeError):
                values.append(0.0)

        return np.array(values, dtype=np.float64)

    def get_feature_names(self) -> List[str]:
        return self.feature_names.copy()


class IForestFeatureExtractor(FeatureExtractor):
    """Feature extractor matching the IForest detector's helper semantics."""

    def __init__(self, logger) -> None:
        super().__init__(logger, excluded_columns=IFOREST_EXCLUDE_FIELDS)

    def initialize_from_record(self, record: Dict[str, Any]) -> None:
        self.feature_names = infer_feature_names(record, self.excluded_columns)
        self.is_initialized = True
        self.logger.info("Extracted IForest features: %s numeric columns", len(self.feature_names))

    def extract(self, record: Dict[str, Any]) -> np.ndarray:
        import numpy as np

        if not self.is_initialized:
            raise ValueError("FeatureExtractor not initialized. Call initialize_from_record() first.")
        return np.asarray(build_feature_vector(record, self.feature_names), dtype=np.float64)


class KitNetFeatureExtractor(FeatureExtractor):
    """Feature extractor matching the KitNET detector's field-order semantics."""

    def __init__(self, logger) -> None:
        super().__init__(logger, excluded_columns=KITNET_EXCLUDE_FIELDS)

    def initialize_from_record(self, record: Dict[str, Any]) -> None:
        self.feature_names, _ = select_features(record, sorted(self.excluded_columns))
        self.is_initialized = True
        self.logger.info("Extracted KitNET features: %s columns", len(self.feature_names))

    def extract(self, record: Dict[str, Any]) -> np.ndarray:
        import numpy as np

        if not self.is_initialized:
            raise ValueError("FeatureExtractor not initialized. Call initialize_from_record() first.")
        names, values = select_features(record, sorted(self.excluded_columns))
        if names != self.feature_names:
            raise ValueError(
                "KitNET feature names changed after initialization: "
                f"expected {self.feature_names!r}, got {names!r}"
            )
        return np.asarray(values, dtype=np.float64)


def _coerce_optional_str(value: Any) -> Optional[str]:
    """Return the value only if it is already a string, else None."""
    if not isinstance(value, str):
        return None
    return value


def _coerce_explanation_status(value: Any) -> Optional[str]:
    """Return the value only if it belongs to the declared vocabulary, else None."""
    if not isinstance(value, str) or value not in EXPLANATION_STATUSES:
        return None
    return value


def _coerce_optional_bool(value: Any) -> Optional[bool]:
    """Return a bool or None; anything else (including "true"/0) is invalid."""
    if not isinstance(value, bool):
        return None
    return value


def _coerce_optional_float(value: Any) -> Optional[float]:
    """Return a float or None; booleans and non-numeric values are invalid."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sha256_hex(canonical_json: str) -> str:
    """Return the SHA-256 hexadecimal digest of a UTF-8 string."""
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _canonical_json(payload: Any) -> str:
    """Return the canonical compact JSON representation used by fingerprints."""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def compute_feature_fingerprint(feature_names: List[str]) -> str:
    """SHA-256 hex fingerprint of the ordered feature list.

    The canonical representation is the compact JSON of the list, so the order
    of the names changes the fingerprint.
    """
    return _sha256_hex(_canonical_json(list(feature_names)))


def compute_hstree_model_fingerprint(
    *,
    hstree_model_version: str,
    hstree_window_size: int,
    hstree_num_trees: int,
    hstree_max_depth: int,
    hstree_initial_window_size: int,
    hstree_seed: int = 42,
) -> str:
    """SHA-256 hex fingerprint of the local HSTree model configuration.

    The canonical representation includes the model version, construction
    seed, and all HSTree hyperparameters, so two constructions with different
    settings are never presented as identical.
    """
    payload = {
        "hstree_model_version": hstree_model_version,
        "hstree_window_size": hstree_window_size,
        "hstree_num_trees": hstree_num_trees,
        "hstree_max_depth": hstree_max_depth,
        "hstree_initial_window_size": hstree_initial_window_size,
        "hstree_seed": hstree_seed,
    }
    return _sha256_hex(_canonical_json(payload))


def build_viz_json_payload(record: Dict[str, Any]) -> str:
    """Serialize the JSON viz copy with event identity/model tags in ``shap_values``.

    Telegraf json_v2 (1.30.3, validated on an isolated probe) resolves an
    object block's ``tags`` only from keys located INSIDE the object at that
    path. Without these identity/model keys the per-feature points are written
    without Group/source_event_id/explained_model tags, collide across models
    or groups on the same second
    (last write wins) and never match a Group filter in Grafana. The Avro
    record (strict ``map<double>`` schema) and the OC exports are untouched:
    only this JSON copy carries the tags inside the map.
    """
    payload = dict(record)
    shap_values = payload.get("shap_values")
    if shap_values:
        payload["shap_values"] = {
            **shap_values,
            "Group": payload.get("Group"),
            "source_event_id": payload.get("source_event_id"),
            "explained_model": payload.get("explained_model"),
        }
    return json.dumps(payload, ensure_ascii=False)


def build_output_record(
    record: Dict[str, Any],
    group: str,
    feature_names: List[str],
    shap_result: Dict[str, Any],
    local_score: Optional[float],
    explanation_status: Optional[str] = None,
    explanation_error_code: Optional[str] = None,
    explanation_profile: Optional[str] = None,
    shap_nsamples: Optional[int] = None,
    shap_background_size: Optional[int] = None,
    explainability_seed: Optional[int] = None,
    hstree_model_version: Optional[str] = None,
    hstree_model_fingerprint: Optional[str] = None,
    feature_fingerprint: Optional[str] = None,
    explained_model: Optional[str] = None,
    explainability_mode: Optional[str] = None,
    explanation_method: Optional[str] = None,
    model_version: Optional[str] = None,
    model_fingerprint: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the compact message published to the explainability-v2 topic."""
    if explained_model not in {"hstree", "iforest", "kitnet"}:
        raise ValueError("explained_model must be 'hstree', 'iforest' or 'kitnet'")
    normalized_model = explained_model
    reference_prefix = normalized_model if explainability_mode == "reference" else None
    features_map: Dict[str, float] = {}
    for name in feature_names:
        try:
            value = record.get(name, 0.0)
            features_map[name] = float(value) if value is not None else 0.0
        except (ValueError, TypeError):
            features_map[name] = 0.0

    shap_values = shap_result.get("shap_values")
    if shap_values is not None:
        shap_values = {key: float(value) for key, value in shap_values.items()}

    timestamp = resolve_timestamp_value(record)
    timestamp_out = str(timestamp) if timestamp is not None else None

    return {
        "timestamp": timestamp_out,
        "Group": str(group) if group is not None else None,
        "hstree_score": (
            float(record.get("hstree_score"))
            if record.get("hstree_score") is not None
            else None
        ),
        "features": features_map,
        "shap_values": shap_values,
        "shap_base_value": (
            float(shap_result.get("shap_base_value"))
            if shap_result.get("shap_base_value") is not None
            else None
        ),
        "shap_computation_time_ms": (
            float(shap_result.get("shap_computation_time_ms"))
            if shap_result.get("shap_computation_time_ms") is not None
            else None
        ),
        "local_hstree_score": (
            float(local_score) if local_score is not None and normalized_model == "hstree" else None
        ),
        "source_event_id": _coerce_optional_str(record.get("source_event_id")),
        "hstree_status": _coerce_optional_str(record.get("hstree_status")),
        "hstree_anomaly": _coerce_optional_bool(record.get("hstree_anomaly")),
        "hstree_threshold": _coerce_optional_float(record.get("hstree_threshold")),
        "explanation_status": _coerce_explanation_status(explanation_status),
        "explanation_error_code": _coerce_optional_str(explanation_error_code),
        "explanation_profile": _coerce_optional_str(explanation_profile),
        "shap_nsamples": shap_nsamples,
        "shap_background_size": shap_background_size,
        "explainability_seed": explainability_seed,
        "hstree_model_version": _coerce_optional_str(hstree_model_version),
        "hstree_model_fingerprint": _coerce_optional_str(hstree_model_fingerprint),
        "feature_fingerprint": _coerce_optional_str(feature_fingerprint),
        "explained_model": _coerce_optional_str(explained_model),
        "explainability_mode": _coerce_optional_str(explainability_mode),
        "explanation_method": _coerce_optional_str(explanation_method),
        "local_score": float(local_score) if local_score is not None else None,
        "reference_model": _coerce_optional_str(reference_prefix),
        "reference_score": _coerce_optional_float(
            record.get(f"{reference_prefix}_score") if reference_prefix else None
        ),
        "reference_status": _coerce_optional_str(
            record.get(f"{reference_prefix}_status") if reference_prefix else None
        ),
        "reference_anomaly": _coerce_optional_bool(
            record.get(f"{reference_prefix}_anomaly") if reference_prefix else None
        ),
        "reference_threshold": _coerce_optional_float(
            record.get(f"{reference_prefix}_threshold") if reference_prefix else None
        ),
        "model_version": _coerce_optional_str(model_version or hstree_model_version),
        "model_fingerprint": _coerce_optional_str(model_fingerprint or hstree_model_fingerprint),
        "explained_value": (
            _coerce_optional_float(
                shap_result.get("explained_value") if explanation_method == "tree" else local_score
            )
            if explanation_status == "computed" and normalized_model == "iforest" else None
        ),
        "explanation_output": (
            shap_result.get("explanation_output") if explanation_method == "tree" else "iforest_score"
        ) if explanation_status == "computed" and normalized_model == "iforest" else None,
    }
