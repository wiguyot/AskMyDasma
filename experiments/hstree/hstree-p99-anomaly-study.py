#!/usr/bin/env python3
"""Replay HSTree and its threshold estimators on the checked-in source CSVs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pysad

from dasma_kafka.anomaly.thresholds import (
    SlidingPercentileEstimator,
    WindowedWelfordEstimator,
    classify_score,
    classify_score_percentile,
)
from dasma_kafka.hstree_analysis.helpers import build_feature_vector, infer_feature_names
from dasma_kafka.producer.helpers import sanitize_avro_field_name
from runtime.detectors.hstree.models.hstree import HSTreeGlobalModel


EXCLUDED_FIELDS = {
    "source_event_id", "timestamp", "Group", "group", "Cycle",
    "LabviewTime", "time", "StepNumber",
    "State (0=OK 1=Warning 2=Error)", "BackgroundSubtracted(0/1)",
    "ActionNumber",
}
DEFAULT_TREE_COUNT = 50
TREE_COUNT = DEFAULT_TREE_COUNT
MAX_DEPTH = 15
SEED = 42
THRESHOLD_WINDOW = 500
THRESHOLD_WARMUP = 100
PERCENTILE = 99.0
WELFORD_K = 3.0
WELFORD_WINDOW = 2048


def _parse_cell(value: str) -> Any:
    if value == "":
        return value
    try:
        return float(value)
    except ValueError:
        return value


def load_records(path: Path) -> list[dict[str, Any]]:
    """Read producer rows, apply Avro field names, and assign producer IDs."""
    records: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        used_names: set[str] = set()
        names = {
            name: sanitize_avro_field_name(name, used_names)
            for name in (reader.fieldnames or [])
        }
        for index, row in enumerate(reader, start=1):
            parsed = {names[name]: _parse_cell(value) for name, value in row.items()}
            parsed["source_event_id"] = f"event-{index:012d}"
            records.append(parsed)
    return records


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    return float(np.percentile(values, percentile))


def summarize_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Pure summary helper shared with unit tests."""
    scored = [event for event in events if event["p99_status"] in {"normal", "anomaly"}]
    anomalies = [event for event in scored if event["p99_status"] == "anomaly"]
    groups = sorted({event["Group"] for event in events})
    per_group: dict[str, dict[str, int]] = {}
    for group in groups:
        own = [event for event in events if event["Group"] == group]
        per_group[group] = {
            "events": len(own),
            "model_scores": sum(event["score"] is not None for event in own),
            "threshold_warmup": sum(
                event["score"] is not None and event["p99_status"] == "warming_up"
                for event in own
            ),
            "scored": sum(event["p99_status"] in {"normal", "anomaly"} for event in own),
            "anomalies": sum(event["p99_status"] == "anomaly" for event in own),
            "welford_anomalies": sum(event["welford_status"] == "anomaly" for event in own),
        }

    by_window: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        if event.get("score") is not None:
            by_window[str(event["reference_window"])].append(event)
    window_summaries: dict[str, Any] = {}
    for window, own in sorted(by_window.items(), key=lambda item: int(item[0])):
        scores = [float(event["score"]) for event in own]
        window_summaries[window] = {
            "scores": len(scores),
            "p99_evaluated": sum(event["p99_status"] in {"normal", "anomaly"} for event in own),
            "anomalies": sum(event["p99_status"] == "anomaly" for event in own),
            "score_min": min(scores),
            "score_p10": _percentile(scores, 10),
            "score_median": _percentile(scores, 50),
            "score_p90": _percentile(scores, 90),
            "score_max": max(scores),
        }

    offsets: dict[str, int] = defaultdict(int)
    for event in anomalies:
        offsets[str(event["position_after_rollover"])] += 1

    minute_groups: dict[str, set[str]] = defaultdict(set)
    timestamp_anomalies = 0
    for event in anomalies:
        raw = str(event.get("timestamp", ""))
        try:
            minute = datetime.fromisoformat(raw).replace(second=0, microsecond=0).isoformat(sep=" ")
        except ValueError:
            continue
        minute_groups[minute].add(str(event["Group"]))
        timestamp_anomalies += 1
    shared_minutes = {
        minute: sorted(groups_at_minute)
        for minute, groups_at_minute in sorted(minute_groups.items())
        if len(groups_at_minute) > 1
    }

    return {
        "rows": len(events),
        "scored": len(scored),
        "p99_anomalies": len(anomalies),
        "p99_anomaly_rate": len(anomalies) / len(scored) if scored else 0.0,
        "welford_anomalies": sum(event["welford_status"] == "anomaly" for event in events),
        "by_group": per_group,
        "by_reference_window": window_summaries,
        "anomaly_position_after_rollover": dict(sorted(offsets.items(), key=lambda item: int(item[0]))),
        "anomaly_timestamps_parseable": timestamp_anomalies,
        "minutes_with_anomalies_in_multiple_groups": shared_minutes,
    }


def replay_group(
    group: str,
    records: list[dict[str, Any]],
    feature_names: list[str],
    initial_window: int,
    model_window: int,
) -> list[dict[str, Any]]:
    model = HSTreeGlobalModel(
        num_features=len(feature_names),
        window_size=model_window,
        num_trees=TREE_COUNT,
        max_depth=MAX_DEPTH,
        initial_window_size=initial_window,
        seed=SEED,
        feature_names=feature_names,
    )
    p99 = SlidingPercentileEstimator(window_size=THRESHOLD_WINDOW, percentile=PERCENTILE)
    welford = WindowedWelfordEstimator(window_size=WELFORD_WINDOW)
    events: list[dict[str, Any]] = []
    scored_count = 0
    for record in records:
        score = model.partial_fit_score(build_feature_vector(record, feature_names))
        warming = model.is_warming_up
        p99_status, _p99_anomaly, p99_threshold = classify_score_percentile(
            score,
            p99,
            is_warming_up=warming,
            min_samples=THRESHOLD_WARMUP,
        )
        welford_status, _welford_anomaly, welford_threshold = classify_score(
            score,
            welford,
            is_warming_up=warming,
            min_samples=THRESHOLD_WARMUP,
            k_sigma=WELFORD_K,
        )
        event: dict[str, Any] = {
            "Group": group,
            "source_event_id": record["source_event_id"],
            "timestamp": record.get("timestamp"),
            "row_number": int(str(record["source_event_id"]).split("-")[-1]),
            "score": float(score) if not warming else None,
            "p99_status": p99_status,
            "p99_threshold": p99_threshold,
            "welford_status": welford_status,
            "welford_threshold": welford_threshold,
            "reference_window": None,
            "position_after_rollover": None,
        }
        if not warming:
            scored_count += 1
            event["reference_window"] = scored_count // model_window
            event["position_after_rollover"] = scored_count % model_window
        events.append(event)
    return events


def replay_configuration(
    records: list[dict[str, Any]], initial_window: int, model_window: int
) -> dict[str, Any]:
    if not records:
        raise ValueError("source CSV contains no records")
    feature_names = infer_feature_names(records[0], EXCLUDED_FIELDS)
    if not feature_names:
        raise ValueError("no numeric features found after applying runtime exclusions")
    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_group[str(record.get("Group", ""))].append(record)

    events: list[dict[str, Any]] = []
    excluded_groups: dict[str, dict[str, int]] = {}
    for group, group_records in sorted(by_group.items()):
        if len(group_records) <= initial_window:
            excluded_groups[group] = {"rows": len(group_records), "scores": 0}
            events.extend(
                {
                    "Group": group,
                    "source_event_id": record["source_event_id"],
                    "timestamp": record.get("timestamp"),
                    "row_number": int(str(record["source_event_id"]).split("-")[-1]),
                    "score": None,
                    "p99_status": "warming_up",
                    "p99_threshold": None,
                    "welford_status": "warming_up",
                    "welford_threshold": None,
                    "reference_window": None,
                    "position_after_rollover": None,
                }
                for record in group_records
            )
            continue
        events.extend(replay_group(group, group_records, feature_names, initial_window, model_window))
    events.sort(key=lambda event: event["row_number"])
    summary = summarize_events(events)
    summary.update({
        "initial_window": initial_window,
        "model_window": model_window,
        "feature_count": len(feature_names),
        "feature_names": feature_names,
        "groups_not_scored": excluded_groups,
    })
    return {"summary": summary, "events": events}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    global TREE_COUNT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--trees", type=int, default=DEFAULT_TREE_COUNT)
    parser.add_argument(
        "--config",
        action="append",
        help="Run only this INITIAL/MODEL window pair; may be repeated.",
    )
    args = parser.parse_args()
    if args.trees < 1:
        parser.error("--trees must be >= 1")
    TREE_COUNT = args.trees
    records = load_records(args.source)
    configurations = (
        [(128, 2048), (256, 256), (512, 512)]
        if "5000" in args.source.name
        else [(256, 256), (512, 512), (1024, 1024)]
    )
    if args.config:
        available = {f"{initial}/{model}" for initial, model in configurations}
        unknown = set(args.config) - available
        if unknown:
            parser.error(f"configuration(s) not available for this source: {', '.join(sorted(unknown))}")
        configurations = [
            pair for pair in configurations if f"{pair[0]}/{pair[1]}" in args.config
        ]
    result: dict[str, Any] = {
        "source": str(args.source),
        "source_sha256": sha256(args.source),
        "source_rows": len(records),
        "versions": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pysad": getattr(pysad, "__version__", "unknown"),
        },
        "model": {"trees": TREE_COUNT, "max_depth": MAX_DEPTH, "seed": SEED},
        "thresholds": {
            "p99": PERCENTILE,
            "threshold_window": THRESHOLD_WINDOW,
            "threshold_warmup": THRESHOLD_WARMUP,
            "welford_k_sigma": WELFORD_K,
            "welford_window": WELFORD_WINDOW,
        },
        "configurations": {},
    }
    for initial_window, model_window in configurations:
        key = f"{initial_window}/{model_window}"
        print(f"Replay {args.source.name}: {key}", flush=True)
        replay = replay_configuration(records, initial_window, model_window)
        result["configurations"][key] = replay["summary"]
        result["configurations"][key]["events"] = replay["events"]
    encoded = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")
    else:
        print(encoded)


if __name__ == "__main__":
    main()
