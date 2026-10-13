"""Exact coalition oracles, numerical qualification and deterministic execution."""
import itertools
import json
import math

import numpy as np
import pytest
import shap
from sklearn.ensemble import IsolationForest

from dasma_kafka.common import isolated_rng
from dasma_kafka.common.iforest_treeshap import (
    ADDITIVITY_ATOL, ADDITIVITY_RTOL, PHI_ATOL, PHI_RTOL,
    convert_iforest, explain_tree, mean_path_length,
)


def exhaustive(forest, x, background):
    m = len(x)
    values = {}
    for bits in itertools.product((False, True), repeat=m):
        hybrid = np.where(bits, x, background)
        values[bits] = mean_path_length(forest, hybrid).mean()
    phi = np.zeros(m)
    for j in range(m):
        for bits, value in values.items():
            if bits[j]:
                continue
            k = sum(bits)
            union = list(bits)
            union[j] = True
            phi[j] += math.factorial(k) * math.factorial(m-k-1) / math.factorial(m) * (values[tuple(union)]-value)
    return phi, values[(False,) * m]


@pytest.mark.parametrize("m,trees,b", [(3, 1, 5), (5, 3, 10), (7, 5, 20)])
@pytest.mark.parametrize("seed", [7, 42, 103])
def test_three_independent_oracles(m, trees, b, seed):
    X = np.random.RandomState(seed).normal(size=(48, m))
    forest = IsolationForest(n_estimators=trees, max_samples=32, random_state=seed, n_jobs=1).fit(X[:32])
    x, background = X[40], X[:b]
    phi, base, fx = explain_tree(convert_iforest(forest), x, background)
    direct, expected_base = exhaustive(forest, x, background)
    exact = shap.ExactExplainer(lambda a: mean_path_length(forest, a),
                               shap.maskers.Independent(background, max_samples=b))(x.reshape(1, -1))
    with isolated_rng(seed):
        kernel = shap.KernelExplainer(lambda a: mean_path_length(forest, a), background)
        dense = kernel.shap_values(x.reshape(1, -1), nsamples=2**m, l1_reg=0, silent=True)[0]
    tolerance = PHI_ATOL + PHI_RTOL * max(abs(direct))
    assert max(abs(phi-direct)) <= tolerance
    np.testing.assert_allclose(exact.values[0], direct, rtol=1e-10, atol=1e-10)
    np.testing.assert_allclose(dense, direct, rtol=1e-10, atol=1e-10)
    assert abs(base-expected_base) < 1e-12
    assert abs(base+phi.sum()-fx) <= ADDITIVITY_ATOL + ADDITIVITY_RTOL*abs(fx)
    print(json.dumps(dict(case=[m,trees,b,seed], phi_error=float(max(abs(phi-direct))),
                         additivity_error=float(abs(base+phi.sum()-fx)))))


def test_real_configuration_additivity_and_bit_determinism():
    X = np.random.RandomState(42).normal(size=(550, 79))
    forest = IsolationForest(n_estimators=100, max_samples=256, random_state=42, n_jobs=1).fit(X[:512])
    converted = convert_iforest(forest)
    errors = []
    for x in X[512:532]:
        phi, base, value = explain_tree(converted, x, X[400:500])
        assert abs(value-mean_path_length(forest, x)[0]) < 1e-12
        errors.append(abs(base+phi.sum()-value))
    one = explain_tree(converted, X[532], X[400:500])
    two = explain_tree(converted, X[532], X[400:500])
    np.testing.assert_array_equal(one[0], two[0])
    assert one[1:] == two[1:]
    print(json.dumps(dict(real_additivity_max=float(max(errors)))))


def test_entire_background_not_implicitly_subsampled():
    X = np.random.RandomState(4).normal(size=(160, 3))
    forest = IsolationForest(n_estimators=3, max_samples=32, random_state=42).fit(X)
    phi, base, value = explain_tree(convert_iforest(forest), X[-1], X[:150])
    assert abs(base-mean_path_length(forest, X[:150]).mean()) < 1e-12


def test_constant_function_and_invalid_background():
    X = np.ones((32, 3))
    forest = IsolationForest(n_estimators=3, max_samples=32, random_state=42).fit(X)
    converted = convert_iforest(forest)
    phi, base, value = explain_tree(converted, X[0], X[:10])
    np.testing.assert_array_equal(phi, [0, 0, 0])
    assert base == value
    with pytest.raises(ValueError):
        explain_tree(converted, X[0], np.empty((0, 3)))
    with pytest.raises(ValueError):
        explain_tree(converted, [np.nan, 1, 1], X[:10])
