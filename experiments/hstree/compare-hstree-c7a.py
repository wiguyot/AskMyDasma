#!/usr/bin/env python3
"""Compare archived C7a HSTree output with an offline replay by event identity."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from dasma_kafka.anomaly.thresholds import (
    SlidingPercentileEstimator,
    classify_score_percentile,
)


def event_key(record: dict[str, Any]) -> tuple[str, str]:
    group = record.get("Group")
    source_event_id = record.get("source_event_id")
    if not isinstance(group, str) or not isinstance(source_event_id, str):
        raise ValueError("each record must have string Group and source_event_id")
    return group, source_event_id


def compare_records(
    campaign_records: list[dict[str, Any]], replay_records: list[dict[str, Any]]
) -> dict[str, Any]:
    campaign_counts = Counter(event_key(record) for record in campaign_records)
    replay_counts = Counter(event_key(record) for record in replay_records)
    campaign_by_key = {event_key(record): record for record in campaign_records}
    replay_by_key = {event_key(record): record for record in replay_records}
    shared = sorted(campaign_by_key.keys() & replay_by_key.keys())
    mismatches: list[dict[str, Any]] = []

    for key in shared:
        actual = campaign_by_key[key]
        expected = replay_by_key[key]
        expected_score = expected.get("score")
        expected_score = 0.0 if expected_score is None else float(expected_score)
        actual_score = float(actual.get("hstree_score", 0.0))
        expected_status = expected.get("p99_status")
        expected_anomaly = (
            True if expected_status == "anomaly"
            else False if expected_status == "normal"
            else None
        )
        fields = {
            "hstree_score": (actual_score, expected_score),
            "hstree_status": (actual.get("hstree_status"), expected_status),
            "hstree_anomaly": (actual.get("hstree_anomaly"), expected_anomaly),
            "hstree_threshold": (
                actual.get("hstree_threshold"), expected.get("p99_threshold")
            ),
        }
        different = {
            name: {"campaign": left, "replay": right}
            for name, (left, right) in fields.items()
            if left != right
        }
        if different:
            mismatches.append({"Group": key[0], "source_event_id": key[1], "fields": different})

    campaign_only = sorted(campaign_by_key.keys() - replay_by_key.keys())
    replay_only = sorted(replay_by_key.keys() - campaign_by_key.keys())
    duplicate_campaign = sorted(key for key, count in campaign_counts.items() if count != 1)
    duplicate_replay = sorted(key for key, count in replay_counts.items() if count != 1)
    return {
        "campaign_count": len(campaign_records),
        "replay_count": len(replay_records),
        "shared_identities": len(shared),
        "campaign_only": [list(key) for key in campaign_only],
        "replay_only": [list(key) for key in replay_only],
        "duplicate_campaign": [list(key) for key in duplicate_campaign],
        "duplicate_replay": [list(key) for key in duplicate_replay],
        "mismatch_count": len(mismatches),
        "first_mismatch": mismatches[0] if mismatches else None,
        "verdict": "match" if not (
            campaign_only or replay_only or duplicate_campaign or duplicate_replay or mismatches
        ) else "mismatch",
    }


def replay_campaign_p99(
    campaign_records: list[dict[str, Any]],
    *,
    initial_window: int = 256,
    percentile: float = 99.0,
    threshold_window: int = 500,
    min_samples: int = 100,
) -> list[dict[str, Any]]:
    """Replay only the P99 verdicts from archived scores in topic order."""
    counts: Counter[str] = Counter()
    estimators: dict[str, SlidingPercentileEstimator] = {}
    replay: list[dict[str, Any]] = []
    for record in campaign_records:
        group = record.get("Group")
        if not isinstance(group, str):
            raise ValueError("each campaign record must have a string Group")
        counts[group] += 1
        score = float(record.get("hstree_score", 0.0))
        estimator = estimators.setdefault(
            group,
            SlidingPercentileEstimator(window_size=threshold_window, percentile=percentile),
        )
        status, anomaly, threshold = classify_score_percentile(
            score,
            estimator,
            is_warming_up=counts[group] <= initial_window,
            min_samples=min_samples,
        )
        replay.append({
            "Group": group,
            "source_event_id": record.get("source_event_id"),
            "score": score,
            "p99_status": status,
            "p99_anomaly": anomaly,
            "p99_threshold": threshold,
        })
    return replay


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError(f"{path}:{line_number}: expected a JSON object")
            records.append(record)
    return records


def _load_replay(path: Path, config: str) -> list[dict[str, Any]]:
    result = json.loads(path.read_text(encoding="utf-8"))
    try:
        return result["configurations"][config]["events"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"{path}: replay has no configuration {config!r}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--replay", type=Path)
    parser.add_argument("--config", default="256/256")
    parser.add_argument("--initial-window", type=int, default=256)
    parser.add_argument("--percentile", type=float, default=99.0)
    parser.add_argument("--threshold-window", type=int, default=500)
    parser.add_argument("--min-samples", type=int, default=100)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    campaign_records = _load_jsonl(args.campaign)
    replay_records = (
        _load_replay(args.replay, args.config)
        if args.replay
        else replay_campaign_p99(
            campaign_records,
            initial_window=args.initial_window,
            percentile=args.percentile,
            threshold_window=args.threshold_window,
            min_samples=args.min_samples,
        )
    )
    report = compare_records(campaign_records, replay_records)
    rendered = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    if report["verdict"] != "match":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
