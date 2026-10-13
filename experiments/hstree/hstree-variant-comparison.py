#!/usr/bin/env python3
"""Describe score, verdict, seed-noise and model-time differences between HSTree variants."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dasma_kafka.anomaly.thresholds import (
    SlidingPercentileEstimator,
    WindowedWelfordEstimator,
    classify_score,
    classify_score_percentile,
)
from dasma_kafka.hstree_analysis.helpers import build_feature_vector, infer_feature_names
from runtime.detectors.hstree.models.hstree import HSTreeGlobalModel


EXCLUDED_FIELDS = {
    "source_event_id", "timestamp", "Group", "group", "Cycle", "LabviewTime",
    "time", "StepNumber", "State (0=OK 1=Warning 2=Error)",
    "BackgroundSubtracted(0/1)", "ActionNumber",
}
INITIAL_WINDOW = 256
MODEL_WINDOW = 256
TREE_COUNT = 25
MAX_DEPTH = 15
THRESHOLD_WINDOW = 500
THRESHOLD_WARMUP = 100
WELFORD_WINDOW = 2048
VARIANTS = ("pysad", "paper")
SEEDS = (42, 43)


def _rank(values: list[float]) -> np.ndarray:
    """Return average ranks for tied values without requiring scipy."""
    order = np.argsort(np.asarray(values), kind="mergesort")
    ranks = np.empty(len(values), dtype=np.float64)
    start = 0
    while start < len(order):
        end = start + 1
        current = values[int(order[start])]
        while end < len(order) and values[int(order[end])] == current:
            end += 1
        ranks[order[start:end]] = (start + 1 + end) / 2.0
        start = end
    return ranks


def spearman_correlation(left: list[float], right: list[float]) -> float | None:
    """Compute Spearman's rho, returning None for insufficient/constant data."""
    if len(left) != len(right) or len(left) < 2:
        return None
    left_rank, right_rank = _rank(left), _rank(right)
    if np.ptp(left_rank) == 0 or np.ptp(right_rank) == 0:
        return None
    return float(np.corrcoef(left_rank, right_rank)[0, 1])


def jaccard_index(left: set[str], right: set[str]) -> float | None:
    """Return anomaly-set Jaccard; None means neither run had anomalies."""
    union = left | right
    return len(left & right) / len(union) if union else None


def summarize_variant_pair(left_events: list[dict[str, Any]], right_events: list[dict[str, Any]]) -> dict[str, Any]:
    """Pure per-group comparison helper used by the report and unit tests."""
    left = {event["source_event_id"]: event for event in left_events if event["score"] is not None}
    right = {event["source_event_id"]: event for event in right_events if event["score"] is not None}
    common = sorted(set(left) & set(right))
    left_anomalies = {key for key in common if left[key]["p99_status"] == "anomaly"}
    right_anomalies = {key for key in common if right[key]["p99_status"] == "anomaly"}
    return {
        "compared_scores": len(common),
        "spearman": spearman_correlation(
            [float(left[key]["score"]) for key in common],
            [float(right[key]["score"]) for key in common],
        ),
        "p99_anomalies_left": len(left_anomalies),
        "p99_anomalies_right": len(right_anomalies),
        "p99_jaccard": jaccard_index(left_anomalies, right_anomalies),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_records(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    # Use the same CSV/Avro-name conversion as the pinned P99 study.
    import importlib.util

    study_path = Path(__file__).with_name("hstree-p99-anomaly-study.py")
    spec = importlib.util.spec_from_file_location("hstree_p99_anomaly_study", study_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load feature-conversion helpers from {study_path}")
    study = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(study)
    records = study.load_records(path)
    return records, infer_feature_names(records[0], EXCLUDED_FIELDS)


def _replay_group(group: str, records: list[dict[str, Any]], feature_names: list[str], variant: str, seed: int) -> list[dict[str, Any]]:
    model = HSTreeGlobalModel(
        num_features=len(feature_names),
        window_size=MODEL_WINDOW,
        num_trees=TREE_COUNT,
        max_depth=MAX_DEPTH,
        initial_window_size=INITIAL_WINDOW,
        seed=seed,
        variant=variant,
        feature_names=feature_names,
    )
    p99 = SlidingPercentileEstimator(window_size=THRESHOLD_WINDOW, percentile=99.0)
    welford = WindowedWelfordEstimator(window_size=WELFORD_WINDOW)
    events: list[dict[str, Any]] = []
    for record in records:
        vector = build_feature_vector(record, feature_names)
        started = time.perf_counter_ns()
        score = model.partial_fit_score(vector)
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000.0
        warming = model.is_warming_up
        p99_status, p99_anomaly, p99_threshold = classify_score_percentile(
            score, p99, is_warming_up=warming, min_samples=THRESHOLD_WARMUP
        )
        welford_status, welford_anomaly, welford_threshold = classify_score(
            score, welford, is_warming_up=warming, min_samples=THRESHOLD_WARMUP,
            k_sigma=3.0,
        )
        events.append({
            "Group": group,
            "source_event_id": record["source_event_id"],
            "timestamp": record.get("timestamp"),
            "score": None if warming else float(score),
            "p99_status": p99_status,
            "p99_anomaly": p99_anomaly,
            "p99_threshold": p99_threshold,
            "welford_status": welford_status,
            "welford_anomaly": welford_anomaly,
            "welford_threshold": welford_threshold,
            "model_elapsed_ms": elapsed_ms,
        })
    return events


def run_comparison(source: Path) -> dict[str, Any]:
    records, feature_names = _load_records(source)
    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_group[str(record.get("Group", "__NO_GROUP__"))].append(record)

    runs: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for variant in VARIANTS:
        for seed in SEEDS:
            run_key = f"{variant}-seed{seed}"
            runs[run_key] = {
                group: _replay_group(group, rows, feature_names, variant, seed)
                for group, rows in sorted(by_group.items())
            }

    comparisons: dict[str, Any] = {}
    for group in sorted(by_group):
        comparisons[group] = {
            "pysad_seed_noise": summarize_variant_pair(runs["pysad-seed42"][group], runs["pysad-seed43"][group]),
            "paper_seed_noise": summarize_variant_pair(runs["paper-seed42"][group], runs["paper-seed43"][group]),
            "algorithm_difference": summarize_variant_pair(runs["pysad-seed42"][group], runs["paper-seed42"][group]),
        }

    all_groups = lambda run: [event for group_events in run.values() for event in group_events]
    comparisons["WEIGHTED_ALL_GROUPS"] = {
        "pysad_seed_noise": summarize_variant_pair(all_groups(runs["pysad-seed42"]), all_groups(runs["pysad-seed43"])),
        "paper_seed_noise": summarize_variant_pair(all_groups(runs["paper-seed42"]), all_groups(runs["paper-seed43"])),
        "algorithm_difference": summarize_variant_pair(all_groups(runs["pysad-seed42"]), all_groups(runs["paper-seed42"])),
    }

    variants: dict[str, Any] = {}
    for variant in VARIANTS:
        events = all_groups(runs[f"{variant}-seed42"])
        scored = [event for event in events if event["score"] is not None]
        variants[variant] = {
            "events": len(events),
            "scored": len(scored),
            "warmup": len(events) - len(scored),
            "p99_anomalies": sum(event["p99_status"] == "anomaly" for event in scored),
            "welford_anomalies": sum(event["welford_status"] == "anomaly" for event in scored),
            "exact_zero_scores": sum(event["score"] == 0.0 for event in scored),
            "model_elapsed_ms": {
                "mean": statistics.fmean(event["model_elapsed_ms"] for event in events),
                "median": statistics.median(event["model_elapsed_ms"] for event in events),
                "p95": float(np.percentile([event["model_elapsed_ms"] for event in events], 95)),
            },
            "by_group": {
                group: {
                    "events": len(group_events),
                    "scored": sum(event["score"] is not None for event in group_events),
                    "p99_anomalies": sum(event["p99_status"] == "anomaly" for event in group_events),
                    "welford_anomalies": sum(event["welford_status"] == "anomaly" for event in group_events),
                    "exact_zero_scores": sum(event["score"] == 0.0 for event in group_events if event["score"] is not None),
                }
                for group, group_events in sorted(runs[f"{variant}-seed42"].items())
            },
        }
    return {
        "source": str(source),
        "source_sha256": _sha256(source),
        "rows": len(records),
        "feature_count": len(feature_names),
        "features": feature_names,
        "versions": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pysad": importlib.metadata.version("pysad"),
        },
        "configuration": {
            "initial_window": INITIAL_WINDOW,
            "model_window": MODEL_WINDOW,
            "num_trees": TREE_COUNT,
            "max_depth": MAX_DEPTH,
            "seeds": list(SEEDS),
            "p99_window": THRESHOLD_WINDOW,
            "p99_warmup": THRESHOLD_WARMUP,
            "welford_window": WELFORD_WINDOW,
        },
        "variants": variants,
        "comparisons": comparisons,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run_comparison(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "source": str(args.source),
        "variants": result["variants"],
        "weighted_comparisons": result["comparisons"]["WEIGHTED_ALL_GROUPS"],
        "output": str(args.output),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
