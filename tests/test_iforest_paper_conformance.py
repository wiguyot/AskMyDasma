"""Paper-oriented contract for the streaming iForest model and its SHAP mirror.

These tests are written before the implementation change (plan T1). They make
the expected score order, fixed subsample size and event-scoped scorer explicit.
"""

from __future__ import annotations

import logging
import math
import warnings
from types import SimpleNamespace

import numpy as np
import pytest
from sklearn.ensemble import IsolationForest

from dasma_kafka.anomaly.thresholds import SlidingPercentileEstimator, classify_score_percentile
from dasma_kafka.common.explained_models import IForestModelManager
from dasma_kafka.iforest_analysis.model import IForestGlobalModel


INITIAL_WINDOW = 32
WINDOW_SIZE = 64
MAX_SAMPLES = 16
N_ESTIMATORS = 7
REFIT_EVERY = 8
SEED = 19
N_FEATURES = 3


def _row(group_index: int, event_index: int) -> np.ndarray:
    """Stable, non-degenerate feature vector for interleaved groups."""
    return np.asarray(
        [
            group_index * 3.0 + event_index * 0.13,
            math.sin(event_index * 0.17 + group_index),
            math.cos(event_index * 0.11 - group_index * 0.2) + event_index * 0.01,
        ],
        dtype=np.float64,
    )


def _c_path_length(size: int) -> float:
    if size <= 1:
        return 0.0
    if size == 2:
        return 1.0
    return 2.0 * (math.log(size - 1) + 0.5772156649) - 2.0 * (size - 1) / size


def _paper_score(model: IsolationForest, row: np.ndarray, psi: int) -> float:
    """Recompute Liu et al.'s score by walking sklearn's fitted trees."""
    value = np.asarray(row, dtype=np.float32)
    path_lengths: list[float] = []
    for estimator in model.estimators_:
        tree = estimator.tree_
        node = 0
        depth = 0
        while tree.children_left[node] != tree.children_right[node]:
            feature = tree.feature[node]
            node = (
                tree.children_left[node]
                if value[feature] <= tree.threshold[node]
                else tree.children_right[node]
            )
            depth += 1
        path_lengths.append(depth + _c_path_length(int(tree.n_node_samples[node])))
    return 2.0 ** (-float(np.mean(path_lengths)) / _c_path_length(psi))


def _params() -> dict[str, int]:
    return {
        "num_features": N_FEATURES,
        "initial_window_size": INITIAL_WINDOW,
        "window_size": WINDOW_SIZE,
        "random_state": SEED,
        "n_estimators": N_ESTIMATORS,
        "max_samples": MAX_SAMPLES,
        "refit_every": REFIT_EVERY,
    }


def _manager_config() -> SimpleNamespace:
    return SimpleNamespace(
        iforest_initial_window_size=INITIAL_WINDOW,
        iforest_window_size=WINDOW_SIZE,
        iforest_n_estimators=N_ESTIMATORS,
        iforest_max_samples=str(MAX_SAMPLES),
        iforest_refit_every=REFIT_EVERY,
        hstree_seed=SEED,
        shap_background_size=INITIAL_WINDOW,
    )


def _interleaved_events() -> list[tuple[str, np.ndarray]]:
    return [
        (group, _row(group_index, event_index))
        for event_index in range(INITIAL_WINDOW + REFIT_EVERY * 3)
        for group_index, group in enumerate(("A", "B", "C"))
    ]


@pytest.mark.parametrize("group", ["A", "B", "C"])
def test_detector_scores_each_event_before_fitting_its_next_generation(group: str) -> None:
    events = [(name, row) for name, row in _interleaved_events() if name == group]
    model = IForestGlobalModel(**_params())
    observed_generations: list[int] = []

    for event_index, (_, row) in enumerate(events, start=1):
        previous_forest = model.current_model
        previous_generation = model.generation
        score = model.partial_fit_score(row)

        if event_index <= INITIAL_WINDOW:
            assert score == 0.0
            assert model.is_warming_up
            assert model.last_scoring_model is None
            if event_index == INITIAL_WINDOW:
                assert model.is_ready
                assert model.generation == 1
            continue

        assert previous_forest is not None
        expected = float(-previous_forest.score_samples(row.reshape(1, -1))[0])
        assert score == expected
        assert not model.is_warming_up
        assert model.last_scoring_model is previous_forest
        assert model.last_scoring_generation == previous_generation
        assert score == pytest.approx(_paper_score(previous_forest, row, MAX_SAMPLES), abs=1e-12)
        observed_generations.append(model.last_scoring_generation)

        fitted = model.current_model
        assert fitted is not None
        assert fitted.max_samples_ == MAX_SAMPLES
        for samples in fitted.estimators_samples_:
            assert len(samples) == MAX_SAMPLES
            assert len(set(samples)) == MAX_SAMPLES
        assert max(estimator.tree_.max_depth for estimator in fitted.estimators_) <= math.ceil(
            math.log2(MAX_SAMPLES)
        )

    assert observed_generations == (
        [1] * REFIT_EVERY + [2] * REFIT_EVERY + [3] * REFIT_EVERY
    )


@pytest.mark.parametrize("manager_version", ["v1", "v2"])
def test_explainability_manager_keeps_the_scorer_for_the_event_across_refit(
    manager_version: str,
) -> None:
    # V1 and V2 intentionally share this manager; parameterization protects
    # both runtime contracts against diverging configuration.
    del manager_version
    manager = IForestModelManager(_manager_config(), logging.getLogger("iforest-contract"))
    events = [(name, row) for name, row in _interleaved_events() if name == "A"]

    for event_index, (_, row) in enumerate(events, start=1):
        state = manager.models.get("A")
        previous_forest = None if state is None else state.current_model
        score = manager.fit_score("A", row)
        if event_index <= INITIAL_WINDOW:
            assert score is None
            assert manager.get_scoring_model("A") is None
            continue

        assert previous_forest is not None
        assert score == pytest.approx(
            float(-previous_forest.score_samples(row.reshape(1, -1))[0]), abs=0.0
        )
        assert manager.get_scoring_model("A") is previous_forest
        predict_fn = manager.predict_fn_for_scoring_event("A")
        np.testing.assert_allclose(predict_fn(row.reshape(1, -1)), [score], rtol=0.0, atol=0.0)
        assert manager.get_scoring_generation("A") == (event_index - INITIAL_WINDOW - 1) // REFIT_EVERY + 1


def test_explainability_manager_logs_the_effective_iforest_configuration(caplog) -> None:
    caplog.set_level(logging.INFO)
    manager = IForestModelManager(_manager_config(), logging.getLogger("iforest-config-contract"))

    manager.fit_score("A", _row(0, 0))

    assert (
        "Initialized explainability iForest mirror for Group='A' with 3 features, "
        "initial_window_size=32, window_size=64, n_estimators=7, max_samples=16, "
        "refit_every=8, random_state=19"
    ) in caplog.text


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_samples": "auto"},
        {"max_samples": 0.5},
        {"max_samples": 1},
        {"max_samples": True},
        {"initial_window_size": 15},
        {"window_size": 31},
    ],
)
def test_invalid_paper_parameters_are_rejected(kwargs: dict[str, object]) -> None:
    values = _params() | kwargs
    with pytest.raises(ValueError):
        IForestGlobalModel(**values)


def test_native_contamination_defaults_to_auto() -> None:
    assert IForestGlobalModel(**_params()).contamination == "auto"
    assert IForestGlobalModel(**(_params() | {"contamination": "0.10"})).contamination == 0.1


def test_fixed_psi_never_emits_sklearn_subsample_warning() -> None:
    models_by_group: dict[str, IForestGlobalModel] = {}
    with warnings.catch_warnings():
        warnings.filterwarnings("error", message=".*max_samples.*total number of samples.*")
        for group, row in _interleaved_events():
            # One model per group, as in the streaming detector.
            state = models_by_group.setdefault(group, IForestGlobalModel(**_params()))
            state.partial_fit_score(row)


    assert len(models_by_group) == 3


def test_percentile_threshold_uses_strict_comparison_and_does_not_learn_anomalies() -> None:
    estimator = SlidingPercentileEstimator(window_size=4, percentile=99.0)
    assert classify_score_percentile(0.2, estimator, is_warming_up=False, min_samples=2) == (
        "warming_up", None, None
    )
    assert classify_score_percentile(0.4, estimator, is_warming_up=False, min_samples=2) == (
        "warming_up", None, None
    )

    status, anomaly, threshold = classify_score_percentile(
        0.3, estimator, is_warming_up=False, min_samples=2
    )
    assert status == "normal"
    assert anomaly is False
    assert threshold is not None
    assert estimator.n == 3

    status, anomaly, threshold = classify_score_percentile(
        0.9, estimator, is_warming_up=False, min_samples=2
    )
    assert status == "anomaly"
    assert anomaly is True
    assert threshold is not None
    assert estimator.n == 3

    constant = SlidingPercentileEstimator(window_size=4, percentile=99.0)
    for _ in range(5):
        status, anomaly, _ = classify_score_percentile(
            0.5, constant, is_warming_up=False, min_samples=2
        )
        if status != "warming_up":
            assert status == "normal"
            assert anomaly is False
