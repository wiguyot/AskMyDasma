from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


_SCRIPT = Path(__file__).parents[1] / "experiments/hstree/hstree-variant-comparison.py"
_SPEC = importlib.util.spec_from_file_location("hstree_variant_comparison", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)


def test_spearman_uses_average_ranks_for_ties_and_rejects_constant_input() -> None:
    assert _MODULE.spearman_correlation([1.0, 2.0, 2.0, 4.0], [4.0, 3.0, 3.0, 1.0]) == -1.0
    assert _MODULE.spearman_correlation([1.0, 1.0], [2.0, 3.0]) is None
    assert _MODULE.spearman_correlation([1.0], [2.0]) is None


def test_jaccard_empty_and_nonempty_sets() -> None:
    assert _MODULE.jaccard_index(set(), set()) is None
    assert _MODULE.jaccard_index({"a", "b"}, {"b", "c"}) == 1 / 3


def test_variant_summary_matches_scores_by_identity() -> None:
    left = [
        {"source_event_id": "a", "score": -1.0, "p99_status": "normal"},
        {"source_event_id": "b", "score": -2.0, "p99_status": "anomaly"},
        {"source_event_id": "warmup", "score": None, "p99_status": "warming_up"},
    ]
    right = [
        {"source_event_id": "a", "score": -10.0, "p99_status": "normal"},
        {"source_event_id": "b", "score": -20.0, "p99_status": "normal"},
        {"source_event_id": "right-only", "score": -30.0, "p99_status": "anomaly"},
    ]
    summary = _MODULE.summarize_variant_pair(left, right)
    assert summary["compared_scores"] == 2
    assert summary["spearman"] == pytest.approx(1.0)
    assert summary["p99_anomalies_left"] == 1
    assert summary["p99_anomalies_right"] == 0
    assert summary["p99_jaccard"] == 0.0
