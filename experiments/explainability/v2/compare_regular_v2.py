#!/usr/bin/env python3
"""OC-13 — pure comparator between the regular and v2 explainability exports.

Scientific scope (OC-13): check that v2 preserves the regular SHAP baseline on
a controlled single-group fixture with no timestamp collision.

The comparison key is (Group, timestamp) for this witness campaign only,
because regular never publishes source_event_id. The OC-13 manifest must
guarantee the uniqueness of that couple; the OC-12 fixture (which contains a
deliberate simultaneous pair) must NOT be reused with this comparator.

Strictly separated notions:
- numerical divergence (beyond the manifest tolerances);
- an event missing from one export;
- a null SHAP on both sides (admitted only on manifest warmup keys);
- computation time or lag differences (reported, never a divergence).

The dashboard "explainability v1 vs v2" is NOT used as proof: OC-13 compares
the raw Kafka JSON exports.

This module is pure: no Docker, Kafka, InfluxDB or network access. The runner
feeds it parsed exports and lag metrics; pytest imports it directly.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

CAMPAIGN_NAME = "oc13-regular-v2-comparison"

# Report categories (explicit separation of the four OC-13 notions).
BOTH_NULL = "both_null"
UNEXPECTED_BOTH_NULL = "unexpected_both_null"
BOTH_COMPUTED_EQUAL = "both_computed_equal"
BOTH_COMPUTED_DIFFERENT = "both_computed_different"
REGULAR_ONLY_NULL = "regular_only_null"
V2_ONLY_NULL = "v2_only_null"
REGULAR_ONLY = "regular_only"
V2_ONLY = "v2_only"
FEATURE_SET_MISMATCH = "feature_set_mismatch"
INVALID_RECORDS = "invalid_records"
DUPLICATE_KEYS = "duplicate_keys"
UNEXPECTED_KEYS = "unexpected_keys"

INVALID_SHAP_STRUCTURE = "invalid_shap_structure"

TIMING_FIELDS = ("min", "max", "mean", "p50", "p95")


class ComparisonError(ValueError):
    """Fatal comparator input error (invalid manifest or metrics)."""


# ---------------------------------------------------------------------------
# Key indexing
# ---------------------------------------------------------------------------


def canonical_key(record: Dict[str, Any]) -> Optional[str]:
    """Build the (Group, timestamp) comparison key; None when unusable."""
    group = record.get("Group")
    timestamp = record.get("timestamp")
    if not isinstance(group, str) or not group:
        return None
    if not isinstance(timestamp, str) or not timestamp:
        return None
    return f"{group}|{timestamp}"


def _valid_shap_values(value: Any) -> bool:
    """shap_values must be None or a dict str -> finite number."""
    if value is None:
        return True
    if not isinstance(value, dict):
        return False
    for key, item in value.items():
        if not isinstance(key, str):
            return False
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            return False
        if not math.isfinite(float(item)):
            return False
    return True


def index_export(records: List[Dict[str, Any]], export_name: str) -> Dict[str, Dict[str, Any]]:
    """Index one export by (Group, timestamp).

    Raises ComparisonError on unusable records, duplicate keys or an invalid
    shap_values structure: those inputs invalidate the whole campaign.
    """
    index: Dict[str, Dict[str, Any]] = {}
    for position, record in enumerate(records):
        if not isinstance(record, dict):
            raise ComparisonError(
                f"{export_name}: record {position} is not a JSON object"
            )
        key = canonical_key(record)
        if key is None:
            raise ComparisonError(
                f"{export_name}: record {position} misses a usable Group or timestamp"
            )
        if key in index:
            raise ComparisonError(f"{export_name}: duplicate key {key}")
        if not _valid_shap_values(record.get("shap_values")):
            raise ComparisonError(
                f"{export_name}: record {position} ({key}) has an invalid shap_values structure"
            )
        index[key] = record
    return index


# ---------------------------------------------------------------------------
# Numeric comparison
# ---------------------------------------------------------------------------


def values_equal(v1: Optional[float], v2: Optional[float], atol: float, rtol: float) -> bool:
    """Tolerant equality: within atol OR within rtol. None equals only None."""
    if v1 is None and v2 is None:
        return True
    if v1 is None or v2 is None:
        return False
    diff = abs(float(v1) - float(v2))
    if diff <= atol:
        return True
    denominator = max(abs(float(v1)), abs(float(v2)))
    if denominator == 0.0:
        return diff == 0.0
    return (diff / denominator) <= rtol


def compare_numeric(
    key: str,
    kind: str,
    feature: Optional[str],
    v1: Optional[float],
    v2: Optional[float],
    atol: float,
    rtol: float,
) -> Optional[Dict[str, Any]]:
    """Return one divergence detail, or None when within tolerance."""
    if values_equal(v1, v2, atol, rtol):
        return None
    if v1 is None or v2 is None:
        abs_diff = None
        rel_diff = None
    else:
        abs_diff = abs(float(v1) - float(v2))
        denominator = max(abs(float(v1)), abs(float(v2)))
        rel_diff = (abs_diff / denominator) if denominator else (0.0 if abs_diff == 0.0 else None)
    return {
        "key": key,
        "kind": kind,
        "feature": feature,
        "v1": v1,
        "v2": v2,
        "abs_diff": abs_diff,
        "rel_diff": rel_diff,
    }


def _compare_feature_maps(
    key: str,
    map1: Dict[str, float],
    map2: Dict[str, float],
    atol: float,
    rtol: float,
) -> List[Dict[str, Any]]:
    divergences = []
    for feature in sorted(set(map1) | set(map2)):
        divergence = compare_numeric(
            key,
            "shap_value",
            feature,
            map1.get(feature),
            map2.get(feature),
            atol,
            rtol,
        )
        if divergence is not None:
            divergences.append(divergence)
    return divergences


# ---------------------------------------------------------------------------
# Timing statistics (never a scientific divergence)
# ---------------------------------------------------------------------------


def _percentile(sorted_values: List[float], fraction: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = fraction * (len(sorted_values) - 1)
    lower = int(math.floor(position))
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def timing_statistics(values: List[Optional[float]]) -> Dict[str, Any]:
    usable = sorted(float(value) for value in values if value is not None)
    if not usable:
        return {"count": 0, "min": None, "max": None, "mean": None, "p50": None, "p95": None}
    return {
        "count": len(usable),
        "min": usable[0],
        "max": usable[-1],
        "mean": sum(usable) / len(usable),
        "p50": _percentile(usable, 0.50),
        "p95": _percentile(usable, 0.95),
    }


# ---------------------------------------------------------------------------
# Campaign comparison
# ---------------------------------------------------------------------------


def _validate_manifest(manifest: Dict[str, Any]) -> None:
    if manifest.get("campaign") != CAMPAIGN_NAME:
        raise ComparisonError(
            f"manifest campaign must be {CAMPAIGN_NAME!r}, got {manifest.get('campaign')!r}"
        )
    if not isinstance(manifest.get("expected_keys"), list) or not manifest["expected_keys"]:
        raise ComparisonError("manifest must declare a non-empty expected_keys list")
    warmup_keys = manifest.get("warmup_keys")
    if not isinstance(warmup_keys, list):
        raise ComparisonError("manifest must declare a warmup_keys list")
    if len(set(manifest["expected_keys"])) != len(manifest["expected_keys"]):
        raise ComparisonError("manifest expected_keys contains duplicates")
    unknown = sorted(set(warmup_keys) - set(manifest["expected_keys"]))
    if unknown:
        raise ComparisonError(f"manifest warmup_keys outside expected_keys: {unknown[:5]}")
    tolerances = manifest.get("numeric_tolerances")
    if not isinstance(tolerances, dict):
        raise ComparisonError("manifest must declare numeric_tolerances")
    for name in ("absolute_tolerance", "relative_tolerance"):
        value = tolerances.get(name)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            raise ComparisonError(f"manifest numeric_tolerances.{name} must be a non-negative number")


def compare_campaign(
    manifest: Dict[str, Any],
    regular_records: List[Dict[str, Any]],
    v2_records: List[Dict[str, Any]],
    runner_metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compare both exports and return the deterministic OC-13 report dict."""
    _validate_manifest(manifest)
    metrics = dict(runner_metrics or {})

    expected_keys = sorted(str(key) for key in manifest["expected_keys"])
    expected_set = set(expected_keys)
    warmup_keys = set(str(key) for key in manifest["warmup_keys"])
    atol = float(manifest["numeric_tolerances"]["absolute_tolerance"])
    rtol = float(manifest["numeric_tolerances"]["relative_tolerance"])
    group = manifest.get("group")

    regular_index = index_export(regular_records, "regular export")
    v2_index = index_export(v2_records, "v2 export")

    regular_keys = set(regular_index)
    v2_keys = set(v2_index)

    # Wrong group on one side is a campaign error, not a divergence.
    for export_name, index in (("regular", regular_index), ("v2", v2_index)):
        wrong_group = sorted(
            key for key in index if group is not None and not key.startswith(f"{group}|")
        )
        if wrong_group:
            raise ComparisonError(
                f"{export_name} export holds events outside the manifest group {group!r}: {wrong_group[:5]}"
            )

    categories: Dict[str, List[str]] = {
        BOTH_NULL: [],
        UNEXPECTED_BOTH_NULL: [],
        BOTH_COMPUTED_EQUAL: [],
        BOTH_COMPUTED_DIFFERENT: [],
        REGULAR_ONLY_NULL: [],
        V2_ONLY_NULL: [],
        REGULAR_ONLY: [],
        V2_ONLY: [],
        FEATURE_SET_MISMATCH: [],
    }
    divergences: List[Dict[str, Any]] = []
    unexpected_keys = sorted((regular_keys | v2_keys) - expected_set)
    missing_keys = sorted(expected_set - (regular_keys | v2_keys))

    timing_regular: List[Optional[float]] = []
    timing_v2: List[Optional[float]] = []

    for key in expected_keys:
        if key not in regular_keys:
            categories[REGULAR_ONLY].append(key)
        if key not in v2_keys:
            categories[V2_ONLY].append(key)
        regular_record = regular_index.get(key)
        v2_record = v2_index.get(key)
        if regular_record is None or v2_record is None:
            continue

        regular_shap = regular_record.get("shap_values")
        v2_shap = v2_record.get("shap_values")

        timing_regular.append(regular_record.get("shap_computation_time_ms"))
        timing_v2.append(v2_record.get("shap_computation_time_ms"))

        if regular_shap is None and v2_shap is None:
            if key in warmup_keys:
                categories[BOTH_NULL].append(key)
            else:
                categories[UNEXPECTED_BOTH_NULL].append(key)
            continue
        if regular_shap is None:
            categories[REGULAR_ONLY_NULL].append(key)
            continue
        if v2_shap is None:
            categories[V2_ONLY_NULL].append(key)
            continue

        if set(regular_shap) != set(v2_shap):
            categories[FEATURE_SET_MISMATCH].append(key)
            continue

        pair_divergences = _compare_feature_maps(
            key, regular_shap, v2_shap, atol, rtol
        )
        base_divergence = compare_numeric(
            key,
            "shap_base_value",
            None,
            regular_record.get("shap_base_value"),
            v2_record.get("shap_base_value"),
            atol,
            rtol,
        )
        if base_divergence is not None:
            pair_divergences.append(base_divergence)
        score_divergence = compare_numeric(
            key,
            "local_hstree_score",
            None,
            regular_record.get("local_hstree_score"),
            v2_record.get("local_hstree_score"),
            atol,
            rtol,
        )
        if score_divergence is not None:
            pair_divergences.append(score_divergence)

        if pair_divergences:
            categories[BOTH_COMPUTED_DIFFERENT].append(key)
            divergences.extend(pair_divergences)
        else:
            categories[BOTH_COMPUTED_EQUAL].append(key)

    divergences.sort(key=lambda item: (item["key"], item["kind"], item["feature"] or ""))
    for values in categories.values():
        values.sort()

    stats_regular = timing_statistics(timing_regular)
    stats_v2 = timing_statistics(timing_v2)
    ratio = None
    if stats_regular["mean"] and stats_v2["mean"]:
        ratio = round(stats_v2["mean"] / stats_regular["mean"], 6)

    final_lags = metrics.get("final_lags")
    if not isinstance(final_lags, dict) or not {"regular", "v2"} <= set(final_lags):
        raise ComparisonError(
            "runner metrics must declare final_lags with 'regular' and 'v2' integer lags"
        )
    lag_regular = final_lags["regular"]
    lag_v2 = final_lags["v2"]
    for name, lag in (("regular", lag_regular), ("v2", lag_v2)):
        if isinstance(lag, bool) or not isinstance(lag, int) or lag < 0:
            raise ComparisonError(f"final lag for {name} must be a non-negative integer, got {lag!r}")

    checks = {
        "exports_cover_exactly_expected_keys": not (
            categories[REGULAR_ONLY]
            or categories[V2_ONLY]
            or unexpected_keys
            or missing_keys
        ),
        "no_duplicate_keys": True,  # duplicates raise in index_export
        "no_paired_divergence": not categories[BOTH_COMPUTED_DIFFERENT]
        and not categories[FEATURE_SET_MISMATCH],
        "no_one_sided_computation": not (categories[REGULAR_ONLY_NULL] or categories[V2_ONLY_NULL]),
        "both_null_only_on_expected_warmup_keys": not categories[UNEXPECTED_BOTH_NULL],
        "final_lags_are_zero": lag_regular == 0 and lag_v2 == 0,
    }

    report = {
        "campaign": CAMPAIGN_NAME,
        "comparison_key": manifest.get("comparison_key", "(Group, timestamp)"),
        "group": group,
        "configuration": {
            "campaign_env": manifest.get("campaign_env", {}),
            "numeric_tolerances": manifest["numeric_tolerances"],
        },
        "coverage": {
            "expected_total": len(expected_keys),
            "matched": len(expected_set & regular_keys & v2_keys),
            "missing_keys": missing_keys,
            "unexpected_keys": unexpected_keys,
            "regular_only": categories[REGULAR_ONLY],
            "v2_only": categories[V2_ONLY],
        },
        "categories": categories,
        "divergences": divergences,
        "timing": {
            "regular": stats_regular,
            "v2": stats_v2,
            "ratio_mean_v2_over_regular": ratio,
            "note": (
                "computation time differences are reported but are NOT a "
                "scientific divergence"
            ),
        },
        "final_lags": {"regular": lag_regular, "v2": lag_v2},
        "checks": checks,
        "passed": all(checks.values()),
    }
    return report


# ---------------------------------------------------------------------------
# File helpers and CLI (pure module above; the CLI only reads/writes files)
# ---------------------------------------------------------------------------


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    records = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        records.append(json.loads(stripped))
    return records


def build_report(
    manifest_path: Path,
    regular_export: Path,
    v2_export: Path,
    runner_metrics_path: Path,
    output_path: Path,
) -> Dict[str, Any]:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    regular_records = load_jsonl(regular_export)
    v2_records = load_jsonl(v2_export)
    runner_metrics = json.loads(Path(runner_metrics_path).read_text(encoding="utf-8"))
    report = compare_campaign(manifest, regular_records, v2_records, runner_metrics)
    write_report(report, output_path)
    return report


def write_report(report: Dict[str, Any], output_path: Path) -> None:
    Path(output_path).write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--regular-export", required=True)
    parser.add_argument("--v2-export", required=True)
    parser.add_argument("--runner-metrics", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        report = build_report(
            Path(args.manifest),
            Path(args.regular_export),
            Path(args.v2_export),
            Path(args.runner_metrics),
            Path(args.output),
        )
    except (ComparisonError, OSError, json.JSONDecodeError) as exc:
        print(f"[oc13-comparator] FAILED: {exc}", file=__import__("sys").stderr)
        return 1
    print(f"[oc13-comparator] passed={report['passed']}")
    if not report["passed"]:
        print(json.dumps(report["checks"], indent=2, sort_keys=True), file=__import__("sys").stderr)
        return 1
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
