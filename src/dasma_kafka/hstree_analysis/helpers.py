"""Reusable pure helpers for hstree-analysis."""

from __future__ import annotations

import math
from collections import deque
from typing import Any

from dasma_kafka.common.grouping import resolve_group_key as resolve_group_key_common


def resolve_group_key(record: Any) -> str:
    """Return the logical group key for a record."""
    return resolve_group_key_common(record)


def infer_feature_names(record: Any, exclude_fields: set[str]) -> list[str]:
    """Infer the sorted numeric feature names from a record."""
    if not isinstance(record, dict):
        return []

    numeric_fields: list[str] = []
    for key, value in record.items():
        if key in exclude_fields:
            continue
        if isinstance(value, (int, float)):
            numeric_fields.append(key)

    return sorted(numeric_fields)


def build_feature_vector(record: Any, feature_names: list[str]) -> list[float]:
    """Build the numeric feature vector from a record and expected names."""
    if not isinstance(record, dict):
        raise TypeError("record must be a dict to build a feature vector")
    return [float(record.get(name, 0.0)) for name in feature_names]


def ensure_hstree_score(record: Any, hstree_score: float | None) -> tuple[dict[str, Any], float]:
    """Return an enriched record with a guaranteed float hstree_score."""
    enriched, final_score = ensure_hstree_analysis_fields(
        record,
        hstree_score,
        hstree_status=None,
        hstree_anomaly=None,
        hstree_threshold=None,
        hstree_variant="paper",
    )
    return enriched, final_score


def ensure_hstree_analysis_fields(
    record: Any,
    hstree_score: float | None,
    hstree_status: str | None,
    hstree_anomaly: bool | None,
    hstree_threshold: float | None,
    hstree_variant: str | None = "paper",
) -> tuple[dict[str, Any], float]:
    """Return an enriched record with HSTree score and trigger fields."""
    final_score = 0.0 if hstree_score is None else float(hstree_score)
    if isinstance(record, dict):
        enriched = dict(record)
    else:
        enriched = {}
    enriched["hstree_score"] = final_score
    enriched["hstree_status"] = hstree_status or "warming_up"
    enriched["hstree_anomaly"] = hstree_anomaly
    enriched["hstree_threshold"] = (
        None if hstree_threshold is None else float(hstree_threshold)
    )
    enriched["hstree_variant"] = hstree_variant
    return enriched, final_score


def strip_excluded_fields(
    record: dict[str, Any],
    exclude_fields: set[str],
    preserve_fields: set[str] | None = None,
) -> dict[str, Any]:
    """Return a shallow copy without the fields excluded from analysis.

    Some excluded metadata still need to survive in the `*-viz` JSON payload so
    Telegraf and Grafana can keep group/time dimensions.
    """
    preserved = preserve_fields or set()
    return {
        key: value
        for key, value in record.items()
        if key not in exclude_fields or key in preserved
    }


def create_viz_state() -> dict[str, Any]:
    """Create the per-group state used for derived viz fields."""
    return {
        "prev_raw_log10": None,
        "prev_ma_log10": None,
        "ma_values": deque(),
        "ma_sum": 0.0,
    }


def enrich_viz_record(
    record: dict[str, Any],
    state: dict[str, Any],
    window_size: int,
) -> dict[str, Any]:
    """Return a viz record enriched with log/diff/moving-average fields."""
    viz_record = dict(record)

    raw_value = viz_record.get("hstree_score")
    if isinstance(raw_value, (int, float)):
        raw_float = float(raw_value)
        raw_log10 = math.log10(abs(raw_float)) if raw_float != 0 else 0.0
    else:
        raw_log10 = 0.0

    prev_raw_log10 = state["prev_raw_log10"]
    raw_log10_diff = 0.0 if prev_raw_log10 is None else (raw_log10 - float(prev_raw_log10))
    state["prev_raw_log10"] = raw_log10

    viz_record["hstree_score_log10"] = raw_log10
    viz_record["hstree_score_log10_diff"] = raw_log10_diff

    if window_size > 0:
        score = viz_record.get("hstree_score")
        if isinstance(score, (int, float)):
            score_val = float(score)
            state["ma_values"].append(score_val)
            state["ma_sum"] += score_val
            if len(state["ma_values"]) > window_size:
                oldest = state["ma_values"].popleft()
                state["ma_sum"] -= oldest
            ma_val = (state["ma_sum"] / window_size) if len(state["ma_values"]) == window_size else 0.0
        else:
            ma_val = 0.0

        ma_field_name = f"hstree_score_ma_{window_size}"
        viz_record[ma_field_name] = ma_val

        ma_log10_val = math.log10(abs(ma_val)) if ma_val != 0 else 0.0
        prev_ma_log10 = state["prev_ma_log10"]
        ma_log10_diff = 0.0 if prev_ma_log10 is None else (ma_log10_val - float(prev_ma_log10))
        state["prev_ma_log10"] = ma_log10_val

        viz_record[f"{ma_field_name}_log10"] = ma_log10_val
        viz_record[f"{ma_field_name}_log10_diff"] = ma_log10_diff

    return viz_record
