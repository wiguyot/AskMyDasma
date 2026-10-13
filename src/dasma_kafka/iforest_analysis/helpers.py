"""Reusable pure helpers for iforest-analysis."""

from __future__ import annotations

from typing import Any
from decimal import Decimal, InvalidOperation

from dasma_kafka.common.grouping import resolve_group_key as resolve_group_key_common


# Business metadata, under both CSV and producer-sanitized Avro names.
# These exclusions remain mandatory when IFOREST_EXCLUDE_FIELDS is overridden.
IFOREST_METADATA_FIELDS = frozenset({
    "State (0=OK 1=Warning 2=Error)",
    "State__0_OK_1_Warning_2_Error_",
    "BackgroundSubtracted(0/1)",
    "BackgroundSubtracted_0_1_",
})


def resolve_group_key(record: Any) -> str:
    """Return the logical group key for a record."""
    return resolve_group_key_common(record)


def infer_feature_names(record: Any, exclude_fields: set[str]) -> list[str]:
    """Infer sorted numeric features, always excluding IForest business metadata."""
    if not isinstance(record, dict):
        return []

    numeric_fields: list[str] = []
    for key, value in record.items():
        if key in exclude_fields or key in IFOREST_METADATA_FIELDS:
            continue
        if isinstance(value, (int, float)):
            numeric_fields.append(key)

    return sorted(numeric_fields)


def build_feature_vector(record: Any, feature_names: list[str]) -> list[float]:
    """Build the numeric feature vector from a record and expected names."""
    if not isinstance(record, dict):
        raise TypeError("record must be a dict to build a feature vector")
    return [float(record.get(name, 0.0)) for name in feature_names]


def ensure_iforest_score(record: Any, iforest_score: float | None) -> tuple[dict[str, Any], float]:
    """Return an enriched record with a guaranteed float iforest_score."""
    enriched, final_score = ensure_iforest_analysis_fields(
        record,
        iforest_score,
        iforest_status=None,
        iforest_anomaly=None,
        iforest_threshold=None,
    )
    return enriched, final_score


def ensure_iforest_analysis_fields(
    record: Any,
    iforest_score: float | None,
    iforest_status: str | None,
    iforest_anomaly: bool | None,
    iforest_threshold: float | None,
) -> tuple[dict[str, Any], float]:
    """Return an enriched record with iForest score and trigger fields."""
    final_score = 0.0 if iforest_score is None else float(iforest_score)
    if isinstance(record, dict):
        enriched = dict(record)
    else:
        enriched = {}
    enriched["iforest_score"] = final_score
    enriched["iforest_status"] = iforest_status or "warming_up"
    enriched["iforest_anomaly"] = iforest_anomaly
    enriched["iforest_threshold"] = (
        None if iforest_threshold is None else float(iforest_threshold)
    )
    return enriched, final_score


def strip_excluded_fields(
    record: dict[str, Any],
    exclude_fields: set[str],
    preserve_fields: set[str] | None = None,
) -> dict[str, Any]:
    """Return a shallow copy without the fields excluded from analysis."""
    preserved = preserve_fields or set()
    return {
        key: value
        for key, value in record.items()
        if key not in exclude_fields or key in preserved
    }


def parse_iforest_max_samples(raw_value: str) -> int:
    """Parse a fixed integer ψ; never silently fall back to sklearn ``auto``."""
    raw = str(raw_value).strip()
    try:
        # Reject decimal notation (including "256.0"): ψ is a configured
        # sample count, not a fraction of the current window.
        if not raw or any(char in raw for char in ".eE"):
            raise ValueError
        value = int(raw)
        if value < 2:
            raise ValueError
        return value
    except (TypeError, ValueError):
        raise ValueError(
            "iforest: max_samples doit être un entier ψ ≥ 2 "
            f"(reçu : '{raw}') : ψ doit rester constant d'une forêt à l'autre, comme dans le papier"
        ) from None


def parse_iforest_contamination(raw_value: str | float) -> str | float:
    """Accept Auto or 1–50 percent in whole percentage-point increments."""
    raw = str(raw_value).strip()
    if raw.lower() == "auto":
        return "auto"
    try:
        value = Decimal(raw)
        percent = value * 100
        if value.is_finite() and Decimal("0.01") <= value <= Decimal("0.50") and percent == percent.to_integral_value():
            return float(value)
    except InvalidOperation:
        pass
    raise ValueError(
        "iforest: contamination doit être 'auto' ou une fraction de 0.01 à 0.50 par pas de 0.01 "
        f"(reçu : '{raw}')"
    )


def classify_iforest_native(
    score: float | None,
    threshold: float | None,
    *,
    is_warming_up: bool,
) -> tuple[str, bool | None, float | None]:
    """Apply the native cutoff of the forest that scored this event."""
    if is_warming_up or score is None or threshold is None:
        return "warming_up", None, None
    is_anomaly = float(score) > float(threshold)
    return ("anomaly" if is_anomaly else "normal"), is_anomaly, float(threshold)
