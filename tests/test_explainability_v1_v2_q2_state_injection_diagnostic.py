"""Q2.2 offline replay tests with small deterministic HSTree models."""

import logging
import random
from collections import defaultdict

import numpy as np

from dasma_kafka.explainability.model import HSTreeModelManager
from experiments.explainability.v1_v2.q2_state_injection_diagnostic import (
    audit,
    config_for_replay,
    vector_for,
)
from runtime.detectors.hstree.models.hstree import HSTreeGlobalModel


def _rows_with_published_scores():
    rows = [
        {"source_event_id": f"event-{index:012d}", "Group": group,
         "timestamp": f"2020-01-01 00:00:{index:02d}",
         "features": {"a": float(value), "constant": 1.0}}
        for index, (group, value) in enumerate((
            ("A", 1), ("B", 3), ("A", 4), ("B", 7),
            ("A", 2), ("B", 4), ("A", 8), ("B", 1),
        ), start=1)
    ]
    features = ["a", "constant"]
    config = config_for_replay(num_trees=3, max_depth=4, initial_window_size=2)
    np.random.seed(42)
    random.seed(42)
    upstream_models = {}
    for row in rows:
        group = row["Group"]
        if group not in upstream_models:
            upstream_models[group] = HSTreeGlobalModel(
                num_features=2, window_size=2048, num_trees=3,
                max_depth=4, initial_window_size=2,
            )
        row["hstree_score"] = upstream_models[group].partial_fit_score(
            vector_for(row, features)
        )
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["Group"]].append(row)
    for group, group_rows in grouped.items():
        np.random.seed(42)
        random.seed(42)
        manager = HSTreeModelManager(config, logging.getLogger("q2-test"))
        for row in group_rows:
            row["local_hstree_score"] = manager.fit_score(
                group, vector_for(row, features)
            )
    return rows, features


def test_injected_rng_and_upstream_bounds_restore_all_published_scores():
    rows, features = _rows_with_published_scores()
    result = audit(rows, features, num_trees=3, max_depth=4,
                   initial_window_size=2)

    assert result["group_initialization_order"] == ["A", "B"]
    assert result["variants"]["baseline"]["compared"] == 4
    assert result["variants"]["baseline"]["published_local_mismatches"] == []
    assert result["variants"]["injected_rng_upstream_bounds"]["exact"] == 4
    assert result["variants"]["injected_rng_upstream_bounds"]["divergent"] == 0
    assert result["variants"]["injected_rng_local_bounds"]["compared"] == 4


def test_groups_without_completed_warmup_are_excluded_from_score_comparison():
    rows, features = _rows_with_published_scores()
    rows.append({"source_event_id": "event-000000000009", "Group": "C",
                 "timestamp": "2020-01-01 00:00:09",
                 "features": {"a": 1.0, "constant": 1.0},
                 "hstree_score": 0.0, "local_hstree_score": None})
    result = audit(rows, features, num_trees=3, max_depth=4,
                   initial_window_size=2)

    assert result["groups_below_initial_window"] == {"C": 1}
    assert result["variants"]["injected_rng_upstream_bounds"]["compared"] == 4
