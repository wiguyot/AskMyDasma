"""Reusable pure helpers for kitnet-analysis."""

from __future__ import annotations

from typing import Any

from dasma_kafka.common.grouping import resolve_group_key as resolve_group_key_common

try:
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover - test/runtime fallback
    np = None  # type: ignore[assignment]


class FallbackArray(list[float]):
    """Tiny fallback used when numpy is unavailable in the test environment."""

    @property
    def shape(self) -> tuple[int]:
        return (len(self),)

    def tolist(self) -> list[float]:
        return list(self)


def parse_exclude_fields(raw: str) -> list[str]:
    """Parse the comma-separated exclude field list."""
    return [part.strip() for part in (raw or "").split(",") if part.strip()]


def parse_grace_feature_mapping(raw_value: str | None) -> int | None:
    """Parse the FM grace period, accepting empty values and 'None'."""
    if raw_value is None or raw_value == "" or str(raw_value).lower() == "none":
        return None
    try:
        return int(raw_value)
    except Exception:
        return None


def parse_grace_anomaly_detector(raw_value: str | None, default: int = 50000) -> int:
    """Parse the AD grace period, falling back to the historical default."""
    if raw_value in (None, ""):
        return default
    return int(raw_value)


def resolve_group_key(record: dict[str, Any]) -> str:
    """Resolve the group key from a record with legacy field compatibility."""
    return resolve_group_key_common(record)


def to_float(value: Any) -> float:
    """Best-effort conversion to float, returning NaN for unsupported values."""
    if value is None:
        return float("nan")
    if np is not None and isinstance(value, np.number):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        raw = str(value).strip()
        if raw == "":
            return float("nan")
        return float(raw)
    except Exception:
        return float("nan")


def select_features(record: dict[str, Any], exclude_fields: list[str]) -> tuple[list[str], Any]:
    """Build the feature vector using the current KitNet selection policy."""
    feature_names: list[str] = []
    values: list[float] = []

    for key, value in record.items():
        if key in exclude_fields:
            continue
        if key is None:
            continue
        values.append(to_float(value))
        feature_names.append(key)

    if np is not None:
        return feature_names, np.asarray(values, dtype=np.float64)
    return feature_names, FallbackArray(values)


def ensure_kitnet_score(record: Any, score: float | None) -> tuple[dict[str, Any], float]:
    """Return an enriched record with a guaranteed float kitnet_score."""
    enriched, final_score = ensure_kitnet_analysis_fields(
        record,
        score,
        kitnet_status=None,
        kitnet_anomaly=None,
        kitnet_threshold=None,
    )
    return enriched, final_score


def ensure_kitnet_analysis_fields(
    record: Any,
    score: float | None,
    kitnet_status: str | None,
    kitnet_anomaly: bool | None,
    kitnet_threshold: float | None,
) -> tuple[dict[str, Any], float]:
    """Return an enriched record with KitNET score and trigger fields."""
    final_score = 0.0 if score is None else float(score)
    if isinstance(record, dict):
        enriched = dict(record)
    else:
        enriched = {}
    enriched["kitnet_score"] = final_score
    enriched["kitnet_status"] = kitnet_status or "warming_up"
    enriched["kitnet_anomaly"] = kitnet_anomaly
    enriched["kitnet_threshold"] = (
        None if kitnet_threshold is None else float(kitnet_threshold)
    )
    return enriched, final_score


def strip_excluded_fields(
    record: dict[str, Any],
    exclude_fields: list[str],
    preserve_fields: set[str] | None = None,
) -> dict[str, Any]:
    """Return a shallow copy without the fields excluded from analysis."""
    excluded = set(exclude_fields)
    preserved = preserve_fields or set()
    return {
        key: value
        for key, value in record.items()
        if key not in excluded or key in preserved
    }
