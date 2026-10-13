"""Contract tests for the opt-in Tan et al. HSTree implementation."""

from __future__ import annotations

import logging

import numpy as np
import pytest

from dasma_kafka.common import isolated_rng
from dasma_kafka.explainability.config import ExplainabilityConfig
from dasma_kafka.explainability.model import HSTreeModelManager as V1ModelManager
from dasma_kafka.explainability_v2.config import ExplainabilityV2Config
from dasma_kafka.explainability_v2.model import HSTreeModelManager as V2ModelManager
from dasma_kafka.hstree_analysis.paper_model import PaperHalfSpaceTrees, create_hstree_model
from runtime.detectors.hstree.models.hstree import HSTreeGlobalModel


PSI = 16
SEED = 42
TREE_COUNT = 5
MAX_DEPTH = 6
INITIAL = np.column_stack((np.linspace(0.0, 1.0, PSI), np.linspace(1.0, 2.0, PSI)))


def make_paper(*, constant: bool = False) -> PaperHalfSpaceTrees:
    rows = INITIAL.copy()
    if constant:
        rows[:, 0] = 3.0
    mins, maxs = rows.min(axis=0), rows.max(axis=0)
    with isolated_rng(SEED):
        return PaperHalfSpaceTrees(
            mins,
            maxs,
            window_size=PSI,
            num_trees=TREE_COUNT,
            max_depth=MAX_DEPTH,
            initial_window_X=rows.tolist(),
        )


def terminal_node(model: PaperHalfSpaceTrees, values: np.ndarray, root: object) -> object:
    node = root
    while node.k < model.max_depth and node.r_mass > model.size_limit:
        node = node.right if values[node.split_att] > node.split_value else node.left
    return node


def reference_score(model: PaperHalfSpaceTrees, values: np.ndarray) -> float:
    total = 0.0
    for root in model.roots:
        node = terminal_node(model, values, root)
        total += node.r_mass * (2**node.k)
    return -total


def test_work_ranges_follow_paper_formula_and_vary_by_tree() -> None:
    model = make_paper()
    assert len(model.work_ranges) == TREE_COUNT
    reconstructed_s = []
    for low, high in model.work_ranges:
        assert np.all(low <= model.feature_mins)
        assert np.all(high >= model.feature_maxes)
        width = model.feature_maxes - model.feature_mins
        s = ((low + high) / 2.0 - model.feature_mins) / width
        m = np.maximum(s, 1.0 - s)
        np.testing.assert_allclose(low, model.feature_mins + (s - 2.0 * m) * width)
        np.testing.assert_allclose(high, model.feature_mins + (s + 2.0 * m) * width)
        reconstructed_s.append(s)
    assert any(not np.array_equal(reconstructed_s[0], other) for other in reconstructed_s[1:])


def test_tree_cut_points_differ_and_margin_can_change_terminal_node() -> None:
    model = make_paper()
    cut_points: dict[int, set[float]] = {}
    for root in model.roots:
        stack = [root]
        while stack:
            node = stack.pop()
            if node.k == model.max_depth:
                continue
            cut_points.setdefault(node.split_att, set()).add(node.split_value)
            stack.extend((node.left, node.right))
    assert any(len(values) > 1 for values in cut_points.values())

    x_max = model.feature_maxes.copy()
    x_out = x_max + 1.0
    assert any(
        terminal_node(model, x_max, root) is not terminal_node(model, x_out, root)
        for root in model.roots
    )


def test_terminal_score_matches_independent_reference_and_differs_from_pysad() -> None:
    paper = make_paper()
    values = np.asarray([0.2, 1.8], dtype=np.float64)
    expected = reference_score(paper, values)
    assert paper.score_partial(values) == expected

    with isolated_rng(SEED):
        pysad = create_hstree_model(
            "pysad", feature_mins=INITIAL.min(axis=0), feature_maxes=INITIAL.max(axis=0),
            window_size=PSI, num_trees=TREE_COUNT, max_depth=MAX_DEPTH,
            initial_window_X=INITIAL.tolist(),
        )
    assert paper.score_partial(values) != pysad.score_partial(values)


def test_constant_feature_gets_unit_width_and_four_m_margins() -> None:
    model = make_paper(constant=True)
    low, high = model.work_ranges[0]
    s = ((low[0] + high[0]) / 2.0 - model.feature_mins[0])
    m = max(s, 1.0 - s)
    assert high[0] - low[0] == pytest.approx(4.0 * m * 1.0)


def test_paper_order_reproducibility_and_detector_mirror_fidelity() -> None:
    configs = (
        ExplainabilityConfig(hstree_window_size=PSI, hstree_num_trees=TREE_COUNT,
                             hstree_max_depth=MAX_DEPTH, hstree_initial_window_size=PSI,
                             hstree_seed=SEED, shap_background_size=PSI, hstree_variant="paper"),
        ExplainabilityV2Config(hstree_window_size=PSI, hstree_num_trees=TREE_COUNT,
                               hstree_max_depth=MAX_DEPTH, hstree_initial_window_size=PSI,
                               hstree_seed=SEED, shap_background_size=PSI, hstree_variant="paper"),
    )

    def run() -> tuple[list[float | None], V1ModelManager, V2ModelManager, HSTreeGlobalModel]:
        upstream = HSTreeGlobalModel(
            num_features=2, window_size=PSI, num_trees=TREE_COUNT,
            max_depth=MAX_DEPTH, initial_window_size=PSI, seed=SEED, variant="paper",
        )
        v1 = V1ModelManager(configs[0], logging.getLogger("paper-v1-test"))
        v2 = V2ModelManager(configs[1], logging.getLogger("paper-v2-test"))
        scores: list[float | None] = []
        for index, row in enumerate(INITIAL.tolist() + [[0.15, 1.85], [0.3, 1.7], [0.45, 1.6]]):
            x = np.asarray(row, dtype=np.float64)
            upstream_score = upstream.partial_fit_score(x)
            v1_score = v1.fit_score("A", x)
            v2_score = v2.fit_score("A", x)
            if index < PSI:
                assert upstream_score == 0.0 and v1_score is None and v2_score is None
                scores.append(None)
                continue
            assert v1_score == upstream_score == v2_score
            scores.append(upstream_score)
        return scores, v1, v2, upstream

    scores1, v1a, v2a, _ = run()
    scores2, _, _, _ = run()
    assert scores1 == scores2
    assert v1a.model_version == "local-hstree-paper-v1"
    assert v2a.model_version == "local-hstree-paper-v1"


def test_mirror_predict_fn_matches_paper_score_and_zero_can_be_real() -> None:
    config = ExplainabilityConfig(hstree_window_size=PSI, hstree_num_trees=TREE_COUNT,
                                  hstree_max_depth=MAX_DEPTH, hstree_initial_window_size=PSI,
                                  hstree_seed=SEED, shap_background_size=PSI)
    manager = V1ModelManager(config, logging.getLogger("paper-predict-test"))
    for row in INITIAL:
        manager.fit_score("A", row)
    model = manager.get_model("A")
    predict = manager.predict_fn("A")
    sample = np.asarray([[0.25, 1.75]], dtype=np.float64)
    assert predict(sample)[0] == model.score_partial(sample[0])

    # With a tree of depth one, a test point in an empty child has terminal mass 0.
    with isolated_rng(SEED):
        shallow = PaperHalfSpaceTrees(
            [0.0], [0.0], window_size=PSI, num_trees=1, max_depth=1,
            initial_window_X=[[0.0] for _ in range(PSI)],
        )
    assert shallow.score_partial(np.asarray([100.0])) == 0.0
