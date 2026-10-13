"""Phase 1: verify upstream/local HSTree construction fidelity."""

from __future__ import annotations

import logging
import random
from unittest.mock import patch

import numpy as np
from dasma_kafka.explainability.config import ExplainabilityConfig
from dasma_kafka.explainability.model import HSTreeModelManager as V1ModelManager
from dasma_kafka.explainability_v2.config import ExplainabilityV2Config
from dasma_kafka.explainability_v2.model import HSTreeModelManager as V2ModelManager
from runtime.detectors.hstree.models.hstree import HSTreeGlobalModel
import runtime.detectors.hstree.models.hstree as upstream_hstree_module


INITIAL_WINDOW = 4
FEATURE_COUNT = 2


def _rows() -> list[np.ndarray]:
    """Warmup keeps feature 0 constant; later values make its bound matter."""
    return [
        np.array([10.0, 1.0]),
        np.array([10.0, 2.0]),
        np.array([10.0, 3.0]),
        np.array([10.0, 4.0]),
        np.array([10.2, 4.5]),
        np.array([9.7, 5.0]),
        np.array([11.1, 5.5]),
        np.array([8.9, 6.0]),
    ]


def _upstream_scores(rows: list[np.ndarray]) -> list[float]:
    """Build group A first, then group B, as the shared upstream process does."""
    np.random.seed(42)
    group_a = HSTreeGlobalModel(
        num_features=FEATURE_COUNT,
        window_size=32,
        num_trees=3,
        max_depth=4,
        initial_window_size=INITIAL_WINDOW,
    )
    group_b = HSTreeGlobalModel(
        num_features=FEATURE_COUNT,
        window_size=32,
        num_trees=3,
        max_depth=4,
        initial_window_size=INITIAL_WINDOW,
    )
    for row in rows:
        group_a.partial_fit_score(row)
    return [group_b.partial_fit_score(row) for row in rows]


def _upstream_model(rows: list[np.ndarray], *, seed: int) -> HSTreeGlobalModel:
    model = HSTreeGlobalModel(
        num_features=FEATURE_COUNT,
        window_size=32,
        num_trees=3,
        max_depth=4,
        initial_window_size=INITIAL_WINDOW,
        seed=seed,
    )
    for row in rows:
        model.partial_fit_score(row)
    return model


def _local_scores(manager: object, rows: list[np.ndarray]) -> list[float | None]:
    return [manager.fit_score("B", row) for row in rows]  # type: ignore[attr-defined]


def _v1_config() -> ExplainabilityConfig:
    return ExplainabilityConfig(
        hstree_window_size=32,
        hstree_num_trees=3,
        hstree_max_depth=4,
        hstree_initial_window_size=INITIAL_WINDOW,
        shap_background_size=INITIAL_WINDOW,
    )


def _v2_config() -> ExplainabilityV2Config:
    return ExplainabilityV2Config(
        hstree_window_size=32,
        hstree_num_trees=3,
        hstree_max_depth=4,
        hstree_initial_window_size=INITIAL_WINDOW,
        shap_background_size=INITIAL_WINDOW,
    )


def test_upstream_v1_and_v2_scores_are_exactly_equal_after_warmup() -> None:
    """The local V1/V2 models reproduce the seeded upstream scores."""
    rows = _rows()
    upstream = _upstream_scores(rows)

    np.random.seed(42)
    v1 = _local_scores(V1ModelManager(_v1_config(), logging.getLogger("v1-fidelity")), rows)
    np.random.seed(42)
    v2 = _local_scores(V2ModelManager(_v2_config(), logging.getLogger("v2-fidelity")), rows)

    assert v1 == v2
    assert v1[INITIAL_WINDOW - 1] is None
    assert v1[INITIAL_WINDOW:] == upstream[INITIAL_WINDOW:]


def test_initialization_trigger_remains_warmup_for_upstream_v1_and_v2() -> None:
    """The event that initializes a model is not scored a second time."""
    rows = _rows()
    upstream = HSTreeGlobalModel(
        num_features=FEATURE_COUNT,
        window_size=32,
        num_trees=3,
        max_depth=4,
        initial_window_size=INITIAL_WINDOW,
        seed=42,
    )
    upstream_scores = [upstream.partial_fit_score(row) for row in rows[:INITIAL_WINDOW]]

    np.random.seed(42)
    v1 = V1ModelManager(_v1_config(), logging.getLogger("v1-boundary"))
    v1_scores = _local_scores(v1, rows[:INITIAL_WINDOW])

    np.random.seed(42)
    v2 = V2ModelManager(_v2_config(), logging.getLogger("v2-boundary"))
    v2_scores = _local_scores(v2, rows[:INITIAL_WINDOW])

    assert upstream_scores[-1] == 0.0
    assert v1_scores[-1] is None
    assert v2_scores[-1] is None


def test_upstream_group_construction_is_independent_of_previous_groups() -> None:
    rows = _rows()
    np.random.seed(123)
    group_a = HSTreeGlobalModel(
        num_features=FEATURE_COUNT, window_size=32, num_trees=3,
        max_depth=4, initial_window_size=INITIAL_WINDOW, seed=42,
    )
    group_b = HSTreeGlobalModel(
        num_features=FEATURE_COUNT, window_size=32, num_trees=3,
        max_depth=4, initial_window_size=INITIAL_WINDOW, seed=42,
    )
    for row in rows:
        group_a.partial_fit_score(row)
    scores_after_group_a = [group_b.partial_fit_score(row) for row in rows]

    np.random.seed(999)
    standalone_b = HSTreeGlobalModel(
        num_features=FEATURE_COUNT, window_size=32, num_trees=3,
        max_depth=4, initial_window_size=INITIAL_WINDOW, seed=42,
    )
    scores_standalone = [standalone_b.partial_fit_score(row) for row in rows]
    assert scores_after_group_a == scores_standalone


def test_upstream_model_construction_restores_global_rng_state() -> None:
    rows = _rows()
    np.random.seed(123)
    state_before = np.random.get_state()
    _upstream_model(rows, seed=42)
    state_after = np.random.get_state()
    assert state_after[0] == state_before[0]
    assert np.array_equal(state_after[1], state_before[1])
    assert state_after[2:] == state_before[2:]


def test_v1_and_v2_local_models_use_the_same_hstree_seed() -> None:
    rows = [np.array([10.0 + index * 0.1, 1.0 + index]) for index in range(6)]
    v1_config = ExplainabilityConfig(
        hstree_window_size=32,
        hstree_num_trees=3,
        hstree_max_depth=4,
        hstree_initial_window_size=INITIAL_WINDOW,
        shap_background_size=INITIAL_WINDOW,
        hstree_seed=17,
    )
    v2_config = ExplainabilityV2Config(
        hstree_window_size=32,
        hstree_num_trees=3,
        hstree_max_depth=4,
        hstree_initial_window_size=INITIAL_WINDOW,
        shap_background_size=INITIAL_WINDOW,
        hstree_seed=17,
    )

    v1 = V1ModelManager(v1_config, logging.getLogger("v1-seed"))
    v2 = V2ModelManager(v2_config, logging.getLogger("v2-seed"))
    v1_scores = _local_scores(v1, rows)
    v2_scores = _local_scores(v2, rows)

    assert v1_scores == v2_scores


def test_multiple_groups_match_upstream_in_both_construction_orders() -> None:
    groups = {
        "A": [np.array([1.0 + i, 10.0 + i * 2.0]) for i in range(6)],
        "B": [np.array([20.0 + i * 0.5, 3.0 + i]) for i in range(6)],
    }

    def run_upstream(order: list[str]) -> dict[str, list[float]]:
        models = {
            group: HSTreeGlobalModel(
                num_features=FEATURE_COUNT,
                window_size=32,
                num_trees=3,
                max_depth=4,
                initial_window_size=INITIAL_WINDOW,
                seed=42,
            )
            for group in order
        }
        return {
            group: [models[group].partial_fit_score(row) for row in groups[group]]
            for group in order
        }

    def run_local(order: list[str]) -> dict[str, list[float | None]]:
        manager = V1ModelManager(_v1_config(), logging.getLogger("multi-group"))
        return {
            group: [manager.fit_score(group, row) for row in groups[group]]
            for group in order
        }

    for order in (["A", "B"], ["B", "A"]):
        upstream = run_upstream(list(order))
        local = run_local(list(order))
        for group in order:
            assert local[group][INITIAL_WINDOW - 1] is None
            assert local[group][INITIAL_WINDOW:] == upstream[group][INITIAL_WINDOW:]


def test_fidelity_holds_when_hstree_window_changes() -> None:
    rows = [np.array([float(i), float(i * i + 1)]) for i in range(6)]
    initial_window = 3
    upstream = HSTreeGlobalModel(
        num_features=FEATURE_COUNT,
        window_size=16,
        num_trees=2,
        max_depth=3,
        initial_window_size=initial_window,
        seed=7,
    )
    config = ExplainabilityConfig(
        hstree_window_size=16,
        hstree_num_trees=2,
        hstree_max_depth=3,
        hstree_initial_window_size=initial_window,
        shap_background_size=initial_window,
        hstree_seed=7,
        explainability_seed=99,
    )
    local = V1ModelManager(config, logging.getLogger("window-fidelity"))

    upstream_scores = [upstream.partial_fit_score(row) for row in rows]
    local_scores = [local.fit_score("G", row) for row in rows]
    assert local_scores[initial_window - 1] is None
    assert local_scores[initial_window:] == upstream_scores[initial_window:]
    assert config.hstree_seed != config.explainability_seed


def test_feature_order_is_preserved_in_identical_models() -> None:
    rows = [np.array([100.0 + i, 1.0 + i * 0.25]) for i in range(5)]
    upstream = HSTreeGlobalModel(
        num_features=FEATURE_COUNT,
        window_size=32,
        num_trees=3,
        max_depth=4,
        initial_window_size=INITIAL_WINDOW,
        seed=42,
    )
    upstream_scores = [upstream.partial_fit_score(row) for row in rows]
    local = V2ModelManager(_v2_config(), logging.getLogger("feature-order"))
    local_scores = [local.fit_score("G", row) for row in rows]
    local_model = local.models["G"].model

    assert local_model is not None
    assert np.array_equal(local_model.feature_mins, upstream._model.feature_mins)
    assert np.array_equal(local_model.feature_maxes, upstream._model.feature_maxes)
    assert local_scores[INITIAL_WINDOW - 1] is None
    assert local_scores[INITIAL_WINDOW:] == upstream_scores[INITIAL_WINDOW:]
    coalitions = [
        np.array([100.5, 1.125]),
        np.array([102.0, 1.75]),
        np.array([101.25, 1.5]),
    ]
    assert [float(local_model.score_partial(row)) for row in coalitions] == [
        float(upstream._model.score_partial(row)) for row in coalitions
    ]


def test_hstree_construction_restores_rng_state_when_constructor_raises() -> None:
    np.random.seed(1234)
    random.seed(5678)
    expected_numpy = np.random.get_state()
    expected_python = random.getstate()
    model = HSTreeGlobalModel(
        num_features=FEATURE_COUNT,
        window_size=32,
        num_trees=2,
        max_depth=3,
        initial_window_size=INITIAL_WINDOW,
        seed=42,
    )

    with patch("dasma_kafka.hstree_analysis.paper_model.HalfSpaceTrees", side_effect=RuntimeError("constructor failed")):
        try:
            for row in _rows()[:INITIAL_WINDOW]:
                model.partial_fit_score(row)
        except RuntimeError as exc:
            assert str(exc) == "constructor failed"
        else:  # pragma: no cover - assertion keeps the failure explicit
            raise AssertionError("expected the HSTree constructor to fail")

    after_numpy = np.random.get_state()
    assert after_numpy[0] == expected_numpy[0]
    assert np.array_equal(after_numpy[1], expected_numpy[1])
    assert after_numpy[2:] == expected_numpy[2:]
    assert random.getstate() == expected_python
