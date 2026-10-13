"""Unit tests for the explainability-v2 SHAP stability metrics.

These cover the metric maths independently of any SHAP runtime cost
(v2 backlog, P1 Task 4 acceptance criterion).
"""

from __future__ import annotations

from unittest import TestCase

from dasma_kafka.explainability_v2 import stability as S


class SpearmanTest(TestCase):
    def test_identical_ranking_is_one(self) -> None:
        self.assertAlmostEqual(S.spearman([1, 2, 3, 4], [10, 20, 30, 40]), 1.0)

    def test_reversed_ranking_is_minus_one(self) -> None:
        self.assertAlmostEqual(S.spearman([1, 2, 3, 4], [40, 30, 20, 10]), -1.0)

    def test_both_constant_is_one(self) -> None:
        self.assertEqual(S.spearman([5, 5, 5], [2, 2, 2]), 1.0)

    def test_one_constant_is_zero(self) -> None:
        self.assertEqual(S.spearman([5, 5, 5], [1, 2, 3]), 0.0)


class JaccardTest(TestCase):
    def test_same_top_features(self) -> None:
        a = [5, 4, 3, 0, 0]
        b = [50, 40, 30, 1, 0]
        self.assertEqual(S.jaccard_at_k(a, b, 3), 1.0)

    def test_disjoint_top_features(self) -> None:
        a = [9, 8, 0, 0]
        b = [0, 0, 9, 8]
        self.assertEqual(S.jaccard_at_k(a, b, 2), 0.0)

    def test_uses_absolute_value(self) -> None:
        a = [-9, 8, 0]
        b = [9, -8, 0]
        self.assertEqual(S.jaccard_at_k(a, b, 2), 1.0)


class SignFlipTest(TestCase):
    def test_no_flips(self) -> None:
        self.assertEqual(S.sign_flip_rate([1, -2, 3], [2, -4, 6], k=3), 0.0)

    def test_all_flips(self) -> None:
        self.assertEqual(S.sign_flip_rate([1, 2, 3], [-1, -2, -3], k=3), 1.0)

    def test_ranks_by_reference_magnitude(self) -> None:
        # Only the single largest-|reference| feature is checked; its sign flips.
        a = [-5, 1, 1]
        b = [5, 1, 1]
        self.assertEqual(S.sign_flip_rate(a, b, k=1, reference=b), 1.0)


class RelativeDiffTest(TestCase):
    def test_zero_when_equal(self) -> None:
        self.assertEqual(S.relative_mean_abs_diff([1, 2, 3], [1, 2, 3]), 0.0)

    def test_relative_scale(self) -> None:
        # mean|a-b| = mean(1, 1) = 1 ; mean|b| = mean(2, 4) = 3 -> 1/3
        self.assertAlmostEqual(S.relative_mean_abs_diff([3, 3], [2, 4]), 1.0 / 3.0)


class InstabilityTest(TestCase):
    def test_single_vector_is_zero(self) -> None:
        self.assertEqual(S.cross_seed_instability([[1, 2, 3]]), 0.0)

    def test_identical_vectors_is_zero(self) -> None:
        self.assertEqual(S.cross_seed_instability([[1, 2, 3], [1, 2, 3]]), 0.0)

    def test_dispersion_is_positive(self) -> None:
        self.assertGreater(S.cross_seed_instability([[1, 0, -1], [-1, 0, 1]]), 0.0)


class DecisionTest(TestCase):
    def test_meets_targets(self) -> None:
        good = {"spearman": 0.97, "jaccard_at_10": 0.92, "sign_flip_rate": 0.01, "rel_mean_abs_diff": 0.03}
        self.assertTrue(S.meets_targets(good, S.VALIDATION_TARGETS))
        self.assertTrue(S.meets_targets(good, S.SCIENTIFIC_TARGETS))

    def test_fails_targets(self) -> None:
        bad = {"spearman": 0.5, "jaccard_at_10": 0.4, "sign_flip_rate": 0.3, "rel_mean_abs_diff": 0.5}
        self.assertFalse(S.meets_targets(bad, S.VALIDATION_TARGETS))

    def test_smallest_stable_budget_picks_lowest_passing(self) -> None:
        passing = {"spearman": 0.99, "jaccard_at_10": 0.95, "sign_flip_rate": 0.0, "rel_mean_abs_diff": 0.02}
        failing = {"spearman": 0.2, "jaccard_at_10": 0.1, "sign_flip_rate": 0.5, "rel_mean_abs_diff": 0.9}
        results = [
            S.BudgetResult(nsamples=700, convergence=passing, instability=0.1, seconds_per_shap=1.0),
            S.BudgetResult(nsamples=100, convergence=failing, instability=0.9, seconds_per_shap=0.1),
            S.BudgetResult(nsamples=300, convergence=passing, instability=0.3, seconds_per_shap=0.3),
        ]
        self.assertEqual(S.smallest_stable_budget(results, S.VALIDATION_TARGETS), 300)

    def test_aggregate_is_inf_safe(self) -> None:
        rows = [
            {"rel_mean_abs_diff": 0.1, "spearman": 0.9},
            {"rel_mean_abs_diff": float("inf"), "spearman": 0.8},
        ]
        agg = S.aggregate(rows)
        self.assertAlmostEqual(agg["rel_mean_abs_diff"], 0.1)
        self.assertAlmostEqual(agg["spearman"], 0.85)
