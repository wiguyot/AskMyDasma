"""Independent fidelity and adversarial split tests for ordinal iForest trees."""
import csv
from pathlib import Path

import numpy as np
import pytest
import shap
from sklearn.ensemble import IsolationForest
from sklearn.ensemble._iforest import _average_path_length

from dasma_kafka.common.iforest_treeshap import convert_iforest, mean_path_length
from dasma_kafka.common.tree_encoding import build_ordinal_encoder


@pytest.mark.parametrize("seed", [7, 42, 103])
@pytest.mark.parametrize("max_features", [1., .5])
def test_prediction_and_score_identity(seed, max_features):
    X = np.random.RandomState(seed).normal(size=(256, 6))
    forest = IsolationForest(n_estimators=10, max_samples=256, max_features=max_features,
                             random_state=seed, n_jobs=1).fit(X)
    converted = convert_iforest(forest)
    expected = mean_path_length(forest, X)
    np.testing.assert_allclose(converted.predict(X), expected, rtol=1e-12, atol=1e-12)
    score = -forest.score_samples(X)
    c = _average_path_length([256])[0]
    np.testing.assert_allclose(expected, -c * np.log2(score), rtol=1e-12, atol=1e-12)
    np.testing.assert_allclose(2 ** (-expected / c), score, rtol=0, atol=1e-12)
    if max_features == 1.:
        for indices in forest.estimators_features_:
            np.testing.assert_array_equal(indices, np.arange(6))


def test_threshold_adversary_exposes_native_rounding_not_ordinal():
    X = np.random.RandomState(7).normal(size=(32, 3))
    forest = IsolationForest(n_estimators=1, max_samples=32, random_state=42, n_jobs=1).fit(X)
    tree = forest.estimators_[0].tree_
    points = np.repeat(X, 3, axis=0)
    points[:, tree.feature[0]] = np.tile([
        tree.threshold[0], np.nextafter(tree.threshold[0], -np.inf),
        np.nextafter(tree.threshold[0], np.inf),
    ], len(X))
    expected = mean_path_length(forest, points)
    np.testing.assert_allclose(convert_iforest(forest).predict(points), expected, rtol=1e-12, atol=1e-12)
    native = shap.TreeExplainer(forest, data=X[:10], feature_perturbation="interventional", model_output="raw")
    assert np.any(abs(native.model.predict(points) - expected) > 1e-6)
    np.testing.assert_allclose(native.model.predict(X), mean_path_length(forest, X), rtol=1e-12, atol=1e-12)


def test_real_source_values_and_nearly_constant_features():
    with Path("data/source-5000.csv").open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))[:300]
    columns = []
    for name in rows[0]:
        try:
            column = [float(r[name]) for r in rows]
        except (TypeError, ValueError):
            continue
        if np.isfinite(column).all():
            columns.append(column)
    X = np.asarray(columns).T
    forest = IsolationForest(n_estimators=100, max_samples=256, random_state=42, n_jobs=1).fit(X)
    np.testing.assert_allclose(convert_iforest(forest).predict(X), mean_path_length(forest, X),
                               rtol=1e-12, atol=1e-12)
    tiny = np.column_stack([np.ones(32), 1 + np.arange(32) * 1e-12])
    forest = IsolationForest(n_estimators=3, max_samples=32, random_state=42).fit(tiny)
    np.testing.assert_allclose(convert_iforest(forest).predict(tiny), mean_path_length(forest, tiny), rtol=1e-12)


def test_path_correction_edge_cases():
    np.testing.assert_array_equal(_average_path_length([1, 2]), [0, 1])
    assert 10 < _average_path_length([256])[0] < 11


@pytest.mark.parametrize("semantics", ["float32_gt", "float64_gt"])
def test_ordinal_hybrids_and_equal_threshold(semantics):
    encoder = build_ordinal_encoder({0: [1., 2.], 1: [0.]}, right_if=semantics, n_features=2)
    np.testing.assert_array_equal(encoder.encode([[1., 0.], [2., 1.]]), [[0, 0], [1, 1]])
    x, z = np.array([3., -1.]), np.array([0., 2.])
    for subset in ([True, False], [False, True], [True, True], [False, False]):
        np.testing.assert_array_equal(encoder.encode(np.where(subset, x, z)),
                                      np.where(subset, encoder.encode(x), encoder.encode(z)))


@pytest.mark.parametrize("bad", [[[np.nan, 0]], [[np.inf, 0]], [[1e300, 0]], [[1, 2, 3]]])
def test_input_guardrails(bad):
    encoder = build_ordinal_encoder({}, right_if="float32_gt", n_features=2)
    with pytest.raises(ValueError):
        encoder.encode(bad)
