"""Test the reference-window boundary specified by the HSTree paper plan."""

from __future__ import annotations

import copy
import logging

import numpy as np

from dasma_kafka.anomaly.thresholds import WelfordEstimator
from dasma_kafka.explainability.config import ExplainabilityConfig
from dasma_kafka.explainability.model import HSTreeModelManager as V1ModelManager
from dasma_kafka.explainability_v2.config import ExplainabilityV2Config
from dasma_kafka.explainability_v2.model import HSTreeModelManager as V2ModelManager
from runtime.detectors.hstree.models.hstree import (
    HSTreeGlobalModel,
    classify_hstree_score,
)


PSI = 16
GROUPS = ("A", "B")
TREE_COUNT = 5
MAX_DEPTH = 6


def _rows(group_index: int, count: int = 35) -> list[np.ndarray]:
    """Return deterministic, non-constant rows for one interleaved group."""
    return [
        np.asarray(
            [group_index * 100.0 + index, (index * 7) % 13 + group_index * 0.5],
            dtype=np.float64,
        )
        for index in range(count)
    ]


def _v1_config() -> ExplainabilityConfig:
    return ExplainabilityConfig(
        hstree_window_size=PSI,
        hstree_num_trees=TREE_COUNT,
        hstree_max_depth=MAX_DEPTH,
        hstree_initial_window_size=PSI,
        hstree_seed=42,
        shap_background_size=PSI,
    )


def _v2_config() -> ExplainabilityV2Config:
    return ExplainabilityV2Config(
        hstree_window_size=PSI,
        hstree_num_trees=TREE_COUNT,
        hstree_max_depth=MAX_DEPTH,
        hstree_initial_window_size=PSI,
        hstree_seed=42,
        shap_background_size=PSI,
    )


def _assert_reference_window(model: object) -> None:
    roots = model.roots  # type: ignore[attr-defined]
    assert len(roots) == TREE_COUNT
    assert all(root.r_mass == PSI for root in roots)
    assert all(root.l_mass == 0 for root in roots)
    assert model.is_first_window is False  # type: ignore[attr-defined]


def test_interleaved_upstream_groups_warm_up_once_and_score_before_update() -> None:
    rows_by_group = {group: _rows(index) for index, group in enumerate(GROUPS)}
    models = {
        group: HSTreeGlobalModel(
            num_features=2,
            window_size=PSI,
            num_trees=TREE_COUNT,
            max_depth=MAX_DEPTH,
            initial_window_size=PSI,
            seed=42,
        )
        for group in GROUPS
    }
    thresholds = {group: WelfordEstimator() for group in GROUPS}
    for estimator in thresholds.values():
        estimator.update(-2.0)
        estimator.update(-1.0)

    for index in range(len(rows_by_group[GROUPS[0]])):
        for group in GROUPS:
            model = models[group]
            row = rows_by_group[group][index]
            before = copy.deepcopy(model._model) if model._model is not None else None
            score = model.partial_fit_score(row)

            if index < PSI:
                assert score == 0.0
                if index == PSI - 1:
                    assert model.is_warming_up is True
                    assert model._model is not None
                    _assert_reference_window(model._model)
                    status, anomaly, threshold = classify_hstree_score(
                        score,
                        thresholds[group],
                        is_warming_up=model.is_warming_up,
                        min_samples=2,
                        k_sigma=3.0,
                    )
                    assert (status, anomaly, threshold) == ("warming_up", None, None)
                    assert thresholds[group].n == 2
                continue

            assert model.is_warming_up is False
            assert before is not None
            is_window_rollover = (before.step + 1) % PSI == 0
            if not is_window_rollover:
                expected = float(before.score_partial(row))
                assert score == expected


def test_v1_and_v2_mirrors_match_upstream_and_keep_boundary_in_warmup() -> None:
    rows_by_group = {group: _rows(index) for index, group in enumerate(GROUPS)}
    upstream = {
        group: HSTreeGlobalModel(
            num_features=2,
            window_size=PSI,
            num_trees=TREE_COUNT,
            max_depth=MAX_DEPTH,
            initial_window_size=PSI,
            seed=42,
        )
        for group in GROUPS
    }
    v1 = V1ModelManager(_v1_config(), logging.getLogger("paper-window-v1"))
    v2 = V2ModelManager(_v2_config(), logging.getLogger("paper-window-v2"))

    for index in range(len(rows_by_group[GROUPS[0]])):
        for group in GROUPS:
            row = rows_by_group[group][index]
            upstream_score = upstream[group].partial_fit_score(row)
            v1_score = v1.fit_score(group, row)
            v2_score = v2.fit_score(group, row)

            if index < PSI:
                assert v1_score is None
                assert v2_score is None
                if index == PSI - 1:
                    assert upstream_score == 0.0
                    _assert_reference_window(v1.models[group].model)
                    _assert_reference_window(v2.models[group].model)
                    assert len(v1.get_background_data(group)) == PSI
                    assert len(v2.get_background_data(group)) == PSI
                continue

            assert v1_score == upstream_score
            assert v2_score == upstream_score


def test_fit_only_keeps_the_reference_boundary_in_warmup() -> None:
    managers = (
        V1ModelManager(_v1_config(), logging.getLogger("fit-only-v1")),
        V2ModelManager(_v2_config(), logging.getLogger("fit-only-v2")),
    )

    for manager in managers:
        group = "fit-only"
        rows = _rows(0)
        for index, row in enumerate(rows):
            ready = manager.fit_only(group, row)
            if index <= PSI - 1:
                assert ready is False
            else:
                assert ready is True

            if index == PSI - 1:
                _assert_reference_window(manager.models[group].model)
                assert len(manager.get_background_data(group)) == PSI


def test_rollover_scores_follow_paper_order_in_detector_and_both_mirrors() -> None:
    """P3-1 corrects R4: score against the current reference, then count the event.

    See R4 in docs/models/hstree/paper-conformity-plan.md and
    docs/models/hstree/paper-deviations.md. Every scored
    event, including window rollovers, must match PySAD ``score_partial`` on the
    model state immediately before that event.
    """
    rows_by_group = {group: _rows(index, count=60) for index, group in enumerate(GROUPS)}
    upstream = {
        group: HSTreeGlobalModel(
            num_features=2,
            window_size=PSI,
            num_trees=TREE_COUNT,
            max_depth=MAX_DEPTH,
            initial_window_size=PSI,
            seed=42,
        )
        for group in GROUPS
    }
    v1 = V1ModelManager(_v1_config(), logging.getLogger("rollover-v1"))
    v2 = V2ModelManager(_v2_config(), logging.getLogger("rollover-v2"))
    rollover_indices = {group: [] for group in GROUPS}

    for index in range(60):
        for group in GROUPS:
            row = rows_by_group[group][index]
            detector = upstream[group]
            before = copy.deepcopy(detector._model) if detector._model is not None else None
            detector_score = detector.partial_fit_score(row)
            v1_score = v1.fit_score(group, row)
            v2_score = v2.fit_score(group, row)

            if index < PSI:
                assert detector_score == 0.0
                assert v1_score is None and v2_score is None
                continue

            assert before is not None
            is_rollover = (before.step + 1) % PSI == 0
            expected_before = float(before.score_partial(row))
            assert detector_score == expected_before
            assert v1_score == detector_score
            assert v2_score == detector_score
            if is_rollover:
                rollover_indices[group].append(index + 1)
                assert all(root.r_mass == PSI for root in detector._model.roots)
                assert all(root.r_mass == PSI for root in v1.models[group].model.roots)
                assert all(root.r_mass == PSI for root in v2.models[group].model.roots)

    for group in GROUPS:
        # With ψ=16 and 60 events, each group rolls at event 32 and event 48.
        assert rollover_indices[group] == [2 * PSI, 3 * PSI]
