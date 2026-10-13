"""Tests for the deterministic offline iForest conformance study."""

from __future__ import annotations

import runpy
from pathlib import Path

import pytest


STUDY = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "experiments" / "iforest" / "iforest-paper-conformance-study.py")
)


def test_legacy_and_current_feature_filters_isolate_the_two_metadata_fields() -> None:
    record = {
        "feature_a": 1.0,
        "State (0=OK 1=Warning 2=Error)": 2.0,
        "State__0_OK_1_Warning_2_Error_": 1.0,
        "BackgroundSubtracted(0/1)": 1.0,
        "BackgroundSubtracted_0_1_": 0.0,
    }
    assert STUDY["legacy_feature_names"](record) == [
        "BackgroundSubtracted_0_1_",
        "State__0_OK_1_Warning_2_Error_",
        "feature_a",
    ]
    assert STUDY["infer_feature_names"](record, STUDY["EXCLUDED_FIELDS"]) == ["feature_a"]


def test_replay_reports_first_score_before_or_after_warmup_boundary() -> None:
    records = [
        {"source_event_id": f"event-{index:012d}", "Group": "A", "x": float(index), "y": float(index % 3)}
        for index in range(1, 9)
    ]
    features = ["x", "y"]
    old = STUDY["StudyConfig"]("old", "current-79", 4, 8, 2, 3, 3, False)
    new = STUDY["StudyConfig"]("new", "current-79", 4, 8, 2, 3, 3, True)

    old_result = STUDY["replay_group"](records, features, old)
    assert STUDY["replay_group"](records, features, old) == old_result
    new_result = STUDY["replay_group"](records, features, new)
    assert STUDY["replay_group"](records, features, new) == new_result
    assert old_result["warmup_events"] == 3
    assert new_result["warmup_events"] == 4
    assert old_result["scored_events"] == 5
    assert new_result["scored_events"] == 4
    assert old_result["generations"]
    assert new_result["generations"]
    assert new_result["max_window_samples_observed"] == 8


def test_k3_probe_exposes_scale_sensitive_near_constant_guard() -> None:
    result = STUDY["k3_tolerance_probe"]()
    assert result["unscaled"]["float32_unique_values"] == 8
    assert result["unscaled"]["root_is_leaf"] is True
    assert result["scaled_1e9"]["float32_unique_values"] == 8
    assert result["scaled_1e9"]["root_is_leaf"] is False


def test_float32_inventory_counts_only_distinct_value_losses() -> None:
    records = [{"x": float(index), "y": float(index) * 1e-10, "Group": "A"} for index in range(8)]
    inventory = STUDY["feature_inventory"](records)
    assert inventory["legacy_count"] == inventory["current_count"] == 2
    assert set(inventory["float32_cardinality_losses"]) <= {"x", "y"}
    assert inventory["feature_ranges_at_or_below_1e-7"]["y"] == pytest.approx(7e-10)
