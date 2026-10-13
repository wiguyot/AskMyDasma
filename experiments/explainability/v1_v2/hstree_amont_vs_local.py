#!/usr/bin/env python3
"""Pure comparison of upstream HSTree scores with local V1/V2 scores."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any, Iterable

COMPARISON_CATEGORIES = (
    "equal",
    "tolerated_equal",
    "divergent",
    "missing",
    "duplicate",
    "invalid_numeric",
)
DEFAULT_ATOL = 1e-12
DEFAULT_RTOL = 1e-12


class HSTreeComparisonError(ValueError):
    """Raised when comparison input or tolerances are invalid."""


def _validate_tolerance(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise HSTreeComparisonError(f"{name} must be a non-negative finite number")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise HSTreeComparisonError(f"{name} must be a non-negative finite number")
    return result


def _identity(record: Any, side: str, index: int) -> tuple[str, str]:
    if not isinstance(record, dict):
        raise HSTreeComparisonError(f"{side}[{index}] must be a JSON object")
    group = record.get("Group")
    event_id = record.get("source_event_id")
    if not isinstance(group, str) or not group.strip():
        raise HSTreeComparisonError(f"{side}[{index}].Group must be a non-empty string")
    if not isinstance(event_id, str) or not event_id.strip():
        raise HSTreeComparisonError(
            f"{side}[{index}].source_event_id must be a non-empty string"
        )
    return group, event_id


def _index(records: Iterable[Any], side: str) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str], int]]:
    indexed: dict[tuple[str, str], dict[str, Any]] = {}
    duplicate_counts: Counter[tuple[str, str]] = Counter()
    for index, record in enumerate(records, start=1):
        key = _identity(record, side, index)
        if key in indexed:
            duplicate_counts[key] += 1
            continue
        indexed[key] = record
    return indexed, dict(duplicate_counts)


def _numeric_outcome(left: Any, right: Any, *, atol: float, rtol: float) -> tuple[str, dict[str, float]]:
    if isinstance(left, bool) or isinstance(right, bool):
        return "invalid_numeric", {}
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        return "invalid_numeric", {}
    left_float = float(left)
    right_float = float(right)
    if not math.isfinite(left_float) or not math.isfinite(right_float):
        return "invalid_numeric", {}
    absolute_gap = abs(left_float - right_float)
    if absolute_gap == 0:
        return "equal", {"absolute_gap": 0.0, "relative_gap": 0.0}
    denominator = max(abs(left_float), abs(right_float))
    relative_gap = absolute_gap / denominator if denominator else math.inf
    if absolute_gap <= atol or relative_gap <= rtol:
        return "tolerated_equal", {
            "absolute_gap": absolute_gap,
            "relative_gap": relative_gap,
        }
    return "divergent", {
        "absolute_gap": absolute_gap,
        "relative_gap": relative_gap,
    }


def _detail(path: str, outcome: str, left: Any, right: Any, gaps: dict[str, float] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": path,
        "outcome": outcome,
        "upstream": left,
        "local": right,
    }
    if gaps:
        result.update(gaps)
    return result


def compare_pair(
    upstream_records: Iterable[Any],
    local_records: Iterable[Any],
    *,
    upstream_name: str = "upstream",
    local_name: str = "local",
    upstream_score_field: str = "hstree_score",
    local_score_field: str = "local_hstree_score",
    upstream_status_field: str = "hstree_status",
    local_status_field: str = "hstree_status",
    upstream_anomaly_field: str = "hstree_anomaly",
    local_anomaly_field: str = "hstree_anomaly",
    upstream_threshold_field: str = "hstree_threshold",
    local_threshold_field: str = "hstree_threshold",
    local_explanation_status_field: str | None = None,
    local_error_code_field: str | None = None,
    atol: float = DEFAULT_ATOL,
    rtol: float = DEFAULT_RTOL,
) -> dict[str, Any]:
    """Compare one local export against the upstream HSTree export."""

    atol = _validate_tolerance(atol, "atol")
    rtol = _validate_tolerance(rtol, "rtol")
    upstream, upstream_duplicates = _index(upstream_records, upstream_name)
    local, local_duplicates = _index(local_records, local_name)
    keys = sorted(set(upstream) | set(local))
    categories: Counter[str] = Counter()
    numeric_outcomes: Counter[str] = Counter()
    identities: list[dict[str, Any]] = []
    only_upstream: list[list[str]] = []
    only_local: list[list[str]] = []
    score_exclusions: Counter[str] = Counter()
    explanation_statuses: Counter[str] = Counter()
    explanation_error_codes: Counter[str] = Counter()

    for key in keys:
        key_list = list(key)
        if key in upstream_duplicates or key in local_duplicates:
            categories["duplicate"] += 1
            identities.append({"key": key_list, "category": "duplicate"})
            continue
        if key not in upstream:
            categories["missing"] += 1
            only_local.append(key_list)
            identities.append({"key": key_list, "category": "missing", "side": "upstream"})
            continue
        if key not in local:
            categories["missing"] += 1
            only_upstream.append(key_list)
            identities.append({"key": key_list, "category": "missing", "side": "local"})
            continue

        upstream_record = upstream[key]
        local_record = local[key]
        details: list[dict[str, Any]] = []
        category = "equal"

        for left_field, right_field in (
            ("timestamp", "timestamp"),
            (upstream_status_field, local_status_field),
            (upstream_anomaly_field, local_anomaly_field),
        ):
            left = upstream_record.get(left_field)
            right = local_record.get(right_field)
            if left != right:
                category = "divergent"
                details.append(_detail(right_field, "divergent", left, right))

        upstream_status = upstream_record.get(upstream_status_field)
        local_status = local_record.get(local_status_field)
        both_warming = upstream_status == "warming_up" and local_status == "warming_up"
        explanation_status = (
            local_record.get(local_explanation_status_field)
            if local_explanation_status_field is not None
            else None
        )
        error_code = (
            local_record.get(local_error_code_field)
            if local_error_code_field is not None
            else None
        )
        skip_kitnet_score = False
        if local_explanation_status_field is not None:
            if isinstance(explanation_status, str) and explanation_status.strip():
                explanation_statuses[explanation_status] += 1
            if isinstance(error_code, str) and error_code.strip():
                explanation_error_codes[error_code] += 1
            if not isinstance(explanation_status, str) or not explanation_status.strip():
                category = "divergent"
                details.append(
                    _detail(local_explanation_status_field, "divergent", "non-empty status", explanation_status)
                )
            if error_code == "invalid_features":
                skip_kitnet_score = True
                score_exclusions["invalid_features"] += 1
                if explanation_status != "failed":
                    category = "divergent"
                    details.append(
                        _detail(local_explanation_status_field, "divergent", "failed", explanation_status)
                    )
            elif upstream_status == "warming_up":
                skip_kitnet_score = True
                score_exclusions["warmup"] += 1
                if explanation_status != "warmup":
                    category = "divergent"
                    details.append(
                        _detail(local_explanation_status_field, "divergent", "warmup", explanation_status)
                    )
            elif explanation_status == "warmup":
                category = "divergent"
                details.append(
                    _detail(local_explanation_status_field, "divergent", "non-warmup", explanation_status)
                )
        left_score = upstream_record.get(upstream_score_field)
        right_score = local_record.get(local_score_field)
        # hstree_status also covers threshold calibration: the model can
        # already produce a real score while the anomaly verdict is warming.
        # Only the pre-model fallback 0.0 (or two absent local scores) is
        # non-comparable. A non-zero upstream score with a missing local score
        # must remain visible as invalid_numeric.
        upstream_fallback = (
            upstream_score_field == "hstree_score"
            and type(left_score) in (int, float)
            and left_score == 0.0
            and right_score is None
        )
        skip_score = skip_kitnet_score or (both_warming and (
            (left_score is None and right_score is None) or upstream_fallback
        ))
        left_threshold = upstream_record.get(upstream_threshold_field)
        right_threshold = local_record.get(local_threshold_field)
        compare_threshold = not both_warming or left_threshold is not None or right_threshold is not None

        numeric_fields = []
        if not skip_score:
            numeric_fields.append("score")
        if compare_threshold:
            numeric_fields.append(local_threshold_field)

        for field in numeric_fields:
            if field == "score":
                left = left_score
                right = right_score
                detail_path = local_score_field
            else:
                left = left_threshold
                right = right_threshold
                detail_path = local_threshold_field
            if left is None or right is None:
                outcome = "invalid_numeric"
                gaps: dict[str, float] = {}
            else:
                outcome, gaps = _numeric_outcome(left, right, atol=atol, rtol=rtol)
            numeric_outcomes[outcome] += 1
            details.append(_detail(detail_path, outcome, left, right, gaps))
            if outcome == "invalid_numeric":
                category = "invalid_numeric"
            elif outcome == "divergent":
                category = "divergent"
            elif outcome == "tolerated_equal" and category == "equal":
                category = "tolerated_equal"

        categories[category] += 1
        identities.append({"key": key_list, "category": category, "details": details})

    verdict = "ok" if not any(categories[name] for name in ("divergent", "missing", "duplicate", "invalid_numeric")) else "ko"
    by_group: dict[str, Counter[str]] = defaultdict(Counter)
    for identity in identities:
        by_group[identity["key"][0]][identity["category"]] += 1
    first_divergence = next(
        (
            identity
            for identity in identities
            if identity["category"] not in {"equal", "tolerated_equal"}
        ),
        None,
    )
    return {
        "upstream": upstream_name,
        "local": local_name,
        "comparison_key": "(Group, source_event_id)",
        "tolerances": {"absolute": atol, "relative": rtol},
        "upstream_records": len(upstream),
        "local_records": len(local),
        "compared_identities": sum(1 for item in identities if item["category"] not in {"missing", "duplicate"}),
        "categories": {name: categories.get(name, 0) for name in COMPARISON_CATEGORIES},
        "numeric_outcomes": {name: numeric_outcomes.get(name, 0) for name in ("equal", "tolerated_equal", "divergent", "invalid_numeric")},
        **(
            {"score_exclusions": {name: score_exclusions.get(name, 0) for name in ("warmup", "invalid_features")}}
            if local_explanation_status_field is not None
            else {}
        ),
        **(
            {
                "local_explanation_statuses": dict(sorted(explanation_statuses.items())),
                "local_explanation_error_codes": dict(sorted(explanation_error_codes.items())),
            }
            if local_explanation_status_field is not None
            else {}
        ),
        "by_group": {
            group: {name: counts.get(name, 0) for name in COMPARISON_CATEGORIES}
            for group, counts in sorted(by_group.items())
        },
        "first_divergence": first_divergence,
        "only_upstream": sorted(only_upstream),
        "only_local": sorted(only_local),
        "identities": identities,
        "verdict": verdict,
    }


def compare_three_exports(
    upstream_records: Iterable[Any],
    v1_records: Iterable[Any],
    v2_records: Iterable[Any],
    *,
    atol: float = DEFAULT_ATOL,
    rtol: float = DEFAULT_RTOL,
) -> dict[str, Any]:
    """Return deterministic upstream/V1, upstream/V2 and V1/V2 reports."""

    upstream = list(upstream_records)
    v1 = list(v1_records)
    v2 = list(v2_records)
    return {
        "comparison_key": "(Group, source_event_id)",
        "comparisons": {
            "upstream_vs_v1": compare_pair(upstream, v1, local_name="v1", atol=atol, rtol=rtol),
            "upstream_vs_v2": compare_pair(upstream, v2, local_name="v2", atol=atol, rtol=rtol),
            "v1_vs_v2": compare_pair(
                v1,
                v2,
                upstream_name="v1",
                local_name="v2",
                upstream_score_field="local_hstree_score",
                local_score_field="local_hstree_score",
                atol=atol,
                rtol=rtol,
            ),
        },
    }
