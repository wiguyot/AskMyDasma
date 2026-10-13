#!/usr/bin/env python3
"""Deterministic offline before/after study for the streaming iForest implementation."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import platform
import warnings
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import sklearn
from sklearn.ensemble import IsolationForest

from dasma_kafka.anomaly.thresholds import (
    SlidingPercentileEstimator,
    WelfordEstimator,
    WindowedWelfordEstimator,
    classify_score,
    classify_score_percentile,
)
from dasma_kafka.iforest_analysis.helpers import build_feature_vector, infer_feature_names
from dasma_kafka.iforest_analysis.model import IForestGlobalModel
from dasma_kafka.producer.helpers import sanitize_avro_field_name


EXCLUDED_FIELDS = {
    "source_event_id", "timestamp", "Group", "group", "Cycle", "LabviewTime", "time", "StepNumber",
    "State (0=OK 1=Warning 2=Error)", "BackgroundSubtracted(0/1)", "ActionNumber",
}
INITIAL_THRESHOLD_SCORES = 100
P99_WINDOW = 500
WELFORD_WINDOW = 2048
WELFORD_K = 3.0
PAPER_THRESHOLD_MARKS = (0.5, 0.6)


@dataclass(frozen=True)
class StudyConfig:
    name: str
    feature_set: str
    initial_window: int
    window_size: int
    max_samples: int
    n_estimators: int
    refit_every: int
    score_before_learning: bool


CONFIGURATIONS = (
    StudyConfig("legacy-filter-and-settings", "legacy-81", 128, 2048, 512, 200, 128, False),
    StudyConfig("feature-filter-only", "current-79", 128, 2048, 512, 200, 128, False),
    StudyConfig("paper-defaults-and-order", "current-79", 512, 2048, 256, 100, 128, True),
    StudyConfig("paper-defaults-initial-256", "current-79", 256, 2048, 256, 100, 128, True),
)


class LegacyStreamingIForest:
    """Frozen pre-T2 implementation used only to reproduce historical semantics."""

    def __init__(self, config: StudyConfig, feature_count: int, seed: int = 42):
        self.config = config
        self.feature_count = feature_count
        self.seed = seed
        self.window: list[list[float]] = []
        self.model: IsolationForest | None = None
        self.since_refit = 0
        self.generation = 0

    @property
    def is_warming_up(self) -> bool:
        return self.model is None

    @property
    def samples_buffered(self) -> int:
        return len(self.window)

    def partial_fit_score(self, values: list[float]) -> float:
        if self.config.score_before_learning:
            scoring_model = self.model
            score = None if scoring_model is None else float(-scoring_model.score_samples(np.asarray([values]))[0])
            self.window.append(values)
            self.window = self.window[-self.config.window_size :]
            self.since_refit += scoring_model is not None
            if len(self.window) >= self.config.initial_window and (
                self.model is None or self.since_refit >= self.config.refit_every
            ):
                self._refit()
            return 0.0 if score is None else score

        # Frozen historical order: append, fit if needed, then score this event.
        self.window.append(values)
        self.window = self.window[-self.config.window_size :]
        if len(self.window) < self.config.initial_window:
            return 0.0
        if self.model is None or self.since_refit >= self.config.refit_every:
            self._refit()
        self.since_refit += 1
        return float(-self.model.score_samples(np.asarray([values]))[0])

    def _refit(self) -> None:
        self.model = IsolationForest(
            n_estimators=self.config.n_estimators,
            max_samples=self.config.max_samples,
            contamination="auto",
            random_state=self.seed,
            n_jobs=1,
        ).fit(np.asarray(self.window, dtype=np.float64))
        self.since_refit = 0
        self.generation += 1


def parse_cell(value: str) -> Any:
    if value == "":
        return value
    try:
        return float(value)
    except ValueError:
        return value


def load_records(path: Path) -> list[dict[str, Any]]:
    """Load the checked-in source CSV using the producer's Avro field names."""
    records: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        used: set[str] = set()
        mapping = {name: sanitize_avro_field_name(name, used) for name in (reader.fieldnames or [])}
        for index, row in enumerate(reader, start=1):
            record = {mapping[key]: parse_cell(value) for key, value in row.items()}
            record["source_event_id"] = f"event-{index:012d}"
            records.append(record)
    return records


def legacy_feature_names(record: dict[str, Any]) -> list[str]:
    """Reproduce feature selection before the mandatory metadata exclusions (TM)."""
    return sorted(
        key for key, value in record.items()
        if key not in EXCLUDED_FIELDS and isinstance(value, (int, float))
    )


def feature_inventory(records: list[dict[str, Any]]) -> dict[str, Any]:
    old_names = legacy_feature_names(records[0])
    current_names = infer_feature_names(records[0], EXCLUDED_FIELDS)
    lost_float32_distinct: dict[str, dict[str, int]] = {}
    tiny_amplitudes: dict[str, float] = {}
    for name in current_names:
        values = np.asarray([float(record.get(name, 0.0)) for record in records], dtype=np.float64)
        count64 = int(np.unique(values).size)
        count32 = int(np.unique(values.astype(np.float32)).size)
        if count32 < count64:
            lost_float32_distinct[name] = {"float64": count64, "float32": count32}
        amplitude = float(np.max(values) - np.min(values))
        if amplitude <= 1e-7:
            tiny_amplitudes[name] = amplitude
    return {
        "legacy_feature_names": old_names,
        "current_feature_names": current_names,
        "legacy_count": len(old_names),
        "current_count": len(current_names),
        "legacy_sha256": hashlib.sha256("\n".join(old_names).encode()).hexdigest(),
        "current_sha256": hashlib.sha256("\n".join(current_names).encode()).hexdigest(),
        "float32_cardinality_losses": lost_float32_distinct,
        "feature_ranges_at_or_below_1e-7": tiny_amplitudes,
    }


def k3_tolerance_probe() -> dict[str, Any]:
    """Exercise sklearn's absolute near-constant split guard and rescaling effect."""
    results: dict[str, Any] = {}
    for name, scale in (("unscaled", 1.0), ("scaled_1e9", 1e9)):
        values = (np.arange(8, dtype=np.float64) * 1e-9 * scale).reshape(-1, 1)
        model = IsolationForest(n_estimators=1, max_samples=8, random_state=42, n_jobs=1).fit(values)
        tree = model.estimators_[0].tree_
        results[name] = {
            "values": values[:, 0].tolist(),
            "float32_unique_values": int(np.unique(values.astype(np.float32)).size),
            "amplitude": float(np.ptp(values)),
            "tree_node_count": int(tree.node_count),
            "root_is_leaf": bool(tree.children_left[0] == tree.children_right[0]),
        }
    return results


def _model_details(model: IsolationForest) -> dict[str, Any]:
    return {
        "effective_psi": int(model.max_samples_),
        "height_limits": sorted({int(est.tree_.max_depth) for est in model.estimators_}),
    }


def _score_statistics(scores: list[float]) -> dict[str, Any]:
    if not scores:
        return {"count": 0, "mean": None, "std": None, "median": None, "max": None}
    return {
        "count": len(scores),
        "mean": float(np.mean(scores)),
        "std": float(np.std(scores, ddof=1)) if len(scores) > 1 else 0.0,
        "median": float(np.median(scores)),
        "max": float(max(scores)),
    }


def replay_group(records: list[dict[str, Any]], features: list[str], config: StudyConfig) -> dict[str, Any]:
    if config.score_before_learning:
        model: Any = IForestGlobalModel(
            num_features=len(features),
            initial_window_size=config.initial_window,
            window_size=config.window_size,
            random_state=42,
            n_estimators=config.n_estimators,
            max_samples=config.max_samples,
            refit_every=config.refit_every,
        )
    else:
        model = LegacyStreamingIForest(config, len(features))

    p99 = SlidingPercentileEstimator(P99_WINDOW, percentile=99.0)
    welford = WelfordEstimator()
    welford_windowed = WindowedWelfordEstimator(WELFORD_WINDOW)
    score_events: list[dict[str, Any]] = []
    generations: dict[int, dict[str, Any]] = {}
    warnings_seen: dict[str, int] = defaultdict(int)
    max_window_samples_observed = 0

    for record in records:
        values = build_feature_vector(record, features)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            score = float(model.partial_fit_score(values))
        max_window_samples_observed = max(max_window_samples_observed, int(model.samples_buffered))
        for warning in caught:
            warnings_seen[str(warning.message).split("(")[0].strip()] += 1

        warming = model.is_warming_up
        p99_status, _, p99_threshold = classify_score_percentile(
            score, p99, is_warming_up=warming, min_samples=INITIAL_THRESHOLD_SCORES,
        )
        wf_status, _, wf_threshold = classify_score(
            score, welford, is_warming_up=warming, min_samples=INITIAL_THRESHOLD_SCORES, k_sigma=WELFORD_K,
        )
        wfw_status, _, wfw_threshold = classify_score(
            score, welford_windowed, is_warming_up=warming, min_samples=INITIAL_THRESHOLD_SCORES,
            k_sigma=WELFORD_K,
        )

        if hasattr(model, "last_scoring_generation"):
            scoring_generation = model.last_scoring_generation
            scoring_model = model.last_scoring_model
        else:
            scoring_generation = int(model.generation)
            scoring_model = model.model
        if not warming:
            generation = int(scoring_generation)
            if scoring_model is not None and generation not in generations:
                generations[generation] = _model_details(scoring_model)
            score_events.append({
                "source_event_id": record["source_event_id"],
                "score": score,
                "generation": generation,
                "p99_status": p99_status,
                "welford_status": wf_status,
                "welford_windowed_status": wfw_status,
                "p99_threshold": p99_threshold,
                "welford_threshold": wf_threshold,
                "welford_windowed_threshold": wfw_threshold,
            })

    grouped_scores: dict[str, list[float]] = defaultdict(list)
    for event in score_events:
        grouped_scores[str(event["generation"])].append(float(event["score"]))
    by_generation = {
        generation: {
            **generations[int(generation)],
            **_score_statistics(scores),
            "scores_gt_0_5": sum(value > 0.5 for value in scores),
            "scores_ge_0_6": sum(value >= 0.6 for value in scores),
        }
        for generation, scores in sorted(grouped_scores.items(), key=lambda item: int(item[0]))
    }
    classified = {
        "p99": _verdict_summary(score_events, "p99_status"),
        "welford": _verdict_summary(score_events, "welford_status"),
        "welford_windowed": _verdict_summary(score_events, "welford_windowed_status"),
    }
    return {
        "events": len(records),
        "scored_events": len(score_events),
        "warmup_events": len(records) - len(score_events),
        "generation_count": len(generations),
        "max_window_samples_observed": max_window_samples_observed,
        "generations": by_generation,
        "verdicts": classified,
        "sklearn_warnings": dict(sorted(warnings_seen.items())),
        "scores": score_events,
    }


def _verdict_summary(events: list[dict[str, Any]], key: str) -> dict[str, int | float]:
    statuses = [event[key] for event in events]
    return {
        "normal": statuses.count("normal"),
        "anomaly": statuses.count("anomaly"),
        "threshold_warmup": statuses.count("warming_up"),
        "anomaly_rate_of_classified": (
            statuses.count("anomaly") / (statuses.count("normal") + statuses.count("anomaly"))
            if statuses.count("normal") + statuses.count("anomaly") else 0.0
        ),
    }


def replay_configuration(records: list[dict[str, Any]], inventory: dict[str, Any], config: StudyConfig) -> dict[str, Any]:
    features = inventory["legacy_feature_names"] if config.feature_set == "legacy-81" else inventory["current_feature_names"]
    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_group[str(record.get("Group", "__NO_GROUP__"))].append(record)
    per_group = {
        group: replay_group(sorted(group_records, key=lambda row: row["source_event_id"]), features, config)
        for group, group_records in sorted(by_group.items())
    }
    return {
        "feature_set": config.feature_set,
        "feature_count": len(features),
        "feature_names_sha256": hashlib.sha256("\n".join(features).encode()).hexdigest(),
        "initial_window": config.initial_window,
        "window_size": config.window_size,
        "max_samples_psi": config.max_samples,
        "n_estimators": config.n_estimators,
        "refit_every": config.refit_every,
        "score_before_learning": config.score_before_learning,
        "groups": per_group,
    }


def replay_dataset(path: Path, *, selected_configs: tuple[StudyConfig, ...] = CONFIGURATIONS) -> dict[str, Any]:
    records = load_records(path)
    if not records:
        raise ValueError(f"empty CSV: {path}")
    inventory = feature_inventory(records)
    return {
        "source_file": path.name,
        "source_sha256": _sha256(path),
        "rows": len(records),
        "groups": dict(sorted(_group_counts(records).items())),
        "features": inventory,
        "configurations": {
            config.name: replay_configuration(records, inventory, config)
            for config in selected_configs
        },
    }


def _group_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for record in records:
        counts[str(record.get("Group", "__NO_GROUP__"))] += 1
    return dict(counts)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_report(
    paths: list[Path], selected_configs: tuple[StudyConfig, ...] = CONFIGURATIONS
) -> dict[str, Any]:
    return {
        "study": "iForest paper-conformance offline before/after",
        "versions": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "scikit_learn": sklearn.__version__,
        },
        "thresholds": {
            "p99_window": P99_WINDOW,
            "p99_percentile": 99.0,
            "welford_minimum_scores": INITIAL_THRESHOLD_SCORES,
            "welford_k_sigma": WELFORD_K,
            "welford_window": WELFORD_WINDOW,
            "paper_score_marks": list(PAPER_THRESHOLD_MARKS),
        },
        "k3_probe": k3_tolerance_probe(),
        "datasets": {
            path.name: replay_dataset(path, selected_configs=selected_configs) for path in paths
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", action="append", type=Path, required=True, help="Source CSV; repeatable.")
    parser.add_argument("--output", type=Path, required=True, help="JSON evidence destination.")
    parser.add_argument("--configs", nargs="*", choices=[config.name for config in CONFIGURATIONS])
    args = parser.parse_args()
    selected = tuple(config for config in CONFIGURATIONS if not args.configs or config.name in args.configs)
    report = build_report(args.source, selected_configs=selected)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    print(f"Wrote deterministic evidence to {args.output}")


if __name__ == "__main__":
    main()
