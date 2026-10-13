"""Offline Q2.1 construction audit contracts."""

from experiments.explainability.v1_v2.q2_model_construction_audit import audit


def _row(index, group, value):
    return {
        "source_event_id": f"event-{index:012d}",
        "timestamp": f"2020-01-01 00:00:{index:02d}",
        "Group": group,
        "features": {"a": float(value), "b": float(value + 1)},
    }


def test_each_group_replays_the_same_seeded_upstream_construction():
    rows = [_row(1, "A", 1), _row(2, "B", 2),
            _row(3, "A", 3), _row(4, "B", 4)]
    result = audit(rows, ["a", "b"], num_trees=2, max_depth=3,
                   initial_window_size=2)

    assert result["group_initialization_order"] == ["A", "B"]
    first, second = result["comparisons"]
    assert first["same_warmup_values"]
    assert first["same_raw_bounds"]
    assert first["same_model_bounds"]
    assert first["same_numpy_rng_before"]
    assert first["same_tree_topology"]
    assert first["same_tree"]
    assert first["amont_first_score"] == 0.0
    assert first["local_first_score"] is None
    assert not first["same_first_score"]  # detector filler and mirror warmup sentinel differ by contract
    assert second["same_warmup_values"]
    assert second["same_raw_bounds"]
    # E-1 isolates ANALYSE_SEED for every upstream construction.  A later
    # group must therefore no longer inherit the RNG state consumed by the
    # preceding group's model construction.
    assert second["same_numpy_rng_before"]
    assert second["same_tree_topology"]
    assert second["same_tree"]
    assert second["amont_first_score"] == 0.0
    assert second["local_first_score"] is None
    assert not second["same_first_score"]
    assert result["upstream"]["A"]["tree_node_count"] == 30
    assert result["local"]["B"]["warmup_event_ids"] == [
        "event-000000000002", "event-000000000004",
    ]


def test_constant_feature_bounds_match_the_upstream_model_bounds():
    rows = [_row(1, "A", 1), _row(2, "A", 1)]
    result = audit(rows, ["a", "b"], num_trees=1, max_depth=2,
                   initial_window_size=2)

    comparison = result["comparisons"][0]
    assert comparison["same_raw_bounds"]
    assert comparison["same_model_bounds"]
    assert comparison["same_tree_topology"]
    assert result["upstream"]["A"]["model_feature_maxes"]["a"] == 1.0
    assert result["local"]["A"]["model_feature_maxes"]["a"] == 1.0
