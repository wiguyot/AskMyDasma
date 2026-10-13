"""Interventional TreeSHAP on mean corrected Isolation Forest path length.

This is not an additive explanation of the exponentially transformed score.
The ordinal representation avoids float32 threshold rounding in SHAP's C++ core.
"""

from dataclasses import dataclass
import time

import numpy as np
from sklearn.ensemble._iforest import _average_path_length

from .tree_encoding import OrdinalEncoder, build_ordinal_encoder

PHI_ATOL = 2e-7
PHI_RTOL = 1e-6
ADDITIVITY_ATOL = 1e-6
ADDITIVITY_RTOL = 1e-7


def _matrix(X, n_features):
    values = np.asarray(X, dtype=np.float64)
    if values.ndim == 1:
        values = values.reshape(1, -1)
    if values.ndim != 2 or values.shape[1] != n_features or not np.isfinite(values).all():
        raise ValueError("IForest TreeSHAP requires a finite feature matrix")
    with np.errstate(over="ignore"):
        cast = values.astype(np.float32)
    if not np.isfinite(cast).all():
        raise ValueError("IForest features overflow float32")
    return cast


def mean_path_length(forest, X) -> np.ndarray:
    """Independent original-tree traversal with sklearn's leaf correction."""
    values = _matrix(X, forest.n_features_in_)
    total = np.zeros(len(values), dtype=np.float64)
    for tree, features in zip(forest.estimators_, forest.estimators_features_):
        subset = values if forest._max_features == forest.n_features_in_ else values[:, features]
        leaves = tree.apply(subset)
        depths = tree.tree_.compute_node_depths() - 1
        total += depths[leaves] + _average_path_length(tree.tree_.n_node_samples[leaves])
    return total / len(forest.estimators_)


@dataclass(frozen=True)
class ConvertedEnsemble:
    model: dict
    encoder: OrdinalEncoder

    def predict(self, X) -> np.ndarray:
        """Predict the converted function without consulting original trees."""
        encoded = self.encoder.encode(X)
        result = np.zeros(len(encoded))
        for tree in self.model["trees"]:
            for row_index, row in enumerate(encoded):
                node = 0
                while tree["children_left"][node] >= 0:
                    right = row[tree["features"][node]] > tree["thresholds"][node]
                    node = tree["children_right" if right else "children_left"][node]
                result[row_index] += tree["values"][node, 0]
        return result


def convert_iforest(forest) -> ConvertedEnsemble:
    if not getattr(forest, "estimators_", None):
        raise ValueError("a fitted IsolationForest is required")
    n_features = forest.n_features_in_
    cuts = {j: [] for j in range(n_features)}
    trees = []
    for estimator, selected in zip(forest.estimators_, forest.estimators_features_):
        original = estimator.tree_
        if original.node_count > 32767:
            raise ValueError("tree exceeds SHAP interventional node limit")
        features = original.feature.copy()
        internal = features >= 0
        if forest._max_features != n_features:
            features[internal] = selected[features[internal]]
        for j in np.unique(features[internal]):
            cuts[int(j)].extend(original.threshold[features == j])
        depths = original.compute_node_depths() - 1
        values = (depths + _average_path_length(original.n_node_samples)) / len(forest.estimators_)
        trees.append(dict(
            children_left=original.children_left.copy(), children_right=original.children_right.copy(),
            children_default=original.children_left.copy(), features=features,
            thresholds=original.threshold.copy(), values=values[:, None],
            node_sample_weight=original.weighted_n_node_samples.copy(),
        ))
    encoder = build_ordinal_encoder(cuts, right_if="float32_gt", n_features=n_features)
    for tree in trees:
        for j in np.unique(tree["features"][tree["features"] >= 0]):
            mask = tree["features"] == j
            tree["thresholds"][mask] = np.searchsorted(encoder.thresholds[j], tree["thresholds"][mask]) + .5
    return ConvertedEnsemble(dict(trees=trees, base_offset=0., input_dtype=np.float64,
                                  internal_dtype=np.float64, tree_output="raw_value"), encoder)


def explain_tree(converted: ConvertedEnsemble, x, background, *, timings=None):
    """Return (contributions, base, converted value) with the entire supplied background."""
    import shap

    start = time.perf_counter()
    target = converted.encoder.encode(x)
    if len(target) != 1:
        raise ValueError("explain_tree expects one event")
    reference = converted.encoder.encode(background)
    if not len(reference):
        raise ValueError("TreeSHAP requires a non-empty background")
    encoded_at = time.perf_counter()
    explainer = shap.TreeExplainer(
        converted.model,
        data=shap.maskers.Independent(reference, max_samples=len(reference)),
        feature_perturbation="interventional", model_output="raw",
    )
    constructed_at = time.perf_counter()
    phi = np.asarray(explainer.shap_values(target, approximate=False, check_additivity=False))[0]
    base = float(explainer.expected_value)
    value = float(converted.predict(x)[0])
    if not np.isfinite(phi).all() or not np.isfinite([base, value]).all():
        raise ValueError("non-finite TreeSHAP output")
    if not np.isclose(base + phi.sum(), value, atol=ADDITIVITY_ATOL, rtol=ADDITIVITY_RTOL):
        raise ValueError("IForest TreeSHAP additivity failure")
    if timings is not None:
        timings.update(encoding_ms=(encoded_at-start)*1000,
                       construction_ms=(constructed_at-encoded_at)*1000,
                       calculation_and_validation_ms=(time.perf_counter()-constructed_at)*1000)
    return phi, base, value
