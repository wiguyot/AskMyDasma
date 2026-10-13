"""Characterize the detector's KitNET wrapper before sharing it with Explainability."""

from __future__ import annotations

import contextlib
import io
import math
from dataclasses import dataclass
from typing import Optional
from unittest import TestCase

import numpy as np
from pysad import models

from dasma_kafka.kitnet_analysis.model import KitNetGroupModels, KitNetParams


@dataclass
class _ReferenceParams:
    max_size_ae: int
    grace_feature_mapping: Optional[int]
    grace_anomaly_detector: int
    learning_rate: float
    hidden_ratio: float


class _ReferenceGroups:
    """Test-local copy of the pre-extraction per-group score path."""

    def __init__(self, params: _ReferenceParams) -> None:
        self.params = params
        self.models_by_group: dict[str, models.KitNet] = {}
        self.seen_by_group: dict[str, int] = {}
        self.feature_count_by_group: dict[str, int] = {}

    @property
    def warmup_samples(self) -> int:
        return int(self.params.grace_feature_mapping or 0) + int(
            self.params.grace_anomaly_detector
        )

    def is_warming_up(self, group: str) -> bool:
        return self.seen_by_group.get(group, 0) <= self.warmup_samples

    def score(self, group: str, x: np.ndarray) -> float:
        if group not in self.models_by_group:
            self.models_by_group[group] = models.KitNet(
                max_size_ae=self.params.max_size_ae,
                grace_feature_mapping=self.params.grace_feature_mapping,
                grace_anomaly_detector=self.params.grace_anomaly_detector,
                learning_rate=self.params.learning_rate,
                hidden_ratio=self.params.hidden_ratio,
            )
            self.seen_by_group[group] = 0
            self.feature_count_by_group[group] = int(x.shape[0])

        if int(x.shape[0]) != self.feature_count_by_group[group]:
            self.models_by_group[group] = models.KitNet(
                max_size_ae=self.params.max_size_ae,
                grace_feature_mapping=self.params.grace_feature_mapping,
                grace_anomaly_detector=self.params.grace_anomaly_detector,
                learning_rate=self.params.learning_rate,
                hidden_ratio=self.params.hidden_ratio,
            )
            self.seen_by_group[group] = 0
            self.feature_count_by_group[group] = int(x.shape[0])

        model = self.models_by_group[group]
        if np.any(np.isnan(x)):
            self.seen_by_group[group] += 1
            return 0.0

        try:
            score = float(model.fit_score_partial(x))
        except Exception:
            score = 0.0
        self.seen_by_group[group] += 1
        return score


def _core_state(groups, group: str) -> tuple[object, ...]:
    model = groups.models_by_group[group]
    core = model.model
    return (
        groups.samples_seen(group) if hasattr(groups, "samples_seen") else groups.seen_by_group[group],
        groups.is_warming_up(group),
        core.n_trained,
        core.n_executed,
        core.FM.N,
        core.v is not None,
        None if core.v is None else tuple(tuple(int(index) for index in cluster) for cluster in core.v),
    )


def _assert_same_observation(test, observed: float, expected: float) -> None:
    if math.isnan(expected):
        test.assertTrue(math.isnan(observed))
    elif math.isinf(expected):
        test.assertEqual(observed, expected)
    else:
        test.assertEqual(observed, expected)


class KitNetGroupModelsCharacterizationTest(TestCase):
    """Compare current production behavior with a reference run in the test."""

    def test_interleaved_groups_nan_and_feature_count_reset(self) -> None:
        params = KitNetParams(10, 2, 2, 0.1, 0.75)
        reference_params = _ReferenceParams(10, 2, 2, 0.1, 0.75)
        actual = KitNetGroupModels(params)
        reference = _ReferenceGroups(reference_params)
        events = [
            ("A", [1.0, 2.0]),
            ("B", [2.0, 1.0]),
            ("A", [2.0, 3.0]),
            ("B", [1.0, 3.0]),
            ("A", [float("nan"), 4.0]),
            ("B", [4.0, 5.0]),
            ("A", [3.0, 4.0]),
            ("B", [2.0, 4.0]),
            ("A", [4.0, 5.0]),
            ("B", [3.0, 5.0]),
            ("A", [5.0, 6.0]),
            ("B", [4.0, 6.0]),
            ("A", [1.0, 2.0, 3.0]),
            ("A", [2.0, 3.0, 4.0]),
            ("A", [3.0, 4.0, 5.0]),
            ("A", [4.0, 5.0, 6.0]),
            ("A", [5.0, 6.0, 7.0]),
            ("A", [6.0, 7.0, 8.0]),
        ]

        for group, values in events:
            vector = np.asarray(values, dtype=np.float64)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                observed = actual.score(group, vector)
                expected = reference.score(group, vector)
            _assert_same_observation(self, observed, expected)
            self.assertEqual(_core_state(actual, group), _core_state(reference, group))

    def test_positive_and_negative_infinity_paths_match_reference(self) -> None:
        params = KitNetParams(10, 2, 2, 0.1, 0.75)
        reference_params = _ReferenceParams(10, 2, 2, 0.1, 0.75)
        finite = [
            np.asarray([1.0 + index, 2.0 + index], dtype=np.float64)
            for index in range(8)
        ]

        for infinite_value in (float("inf"), float("-inf")):
            for position in (0, 3, 6):
                actual = KitNetGroupModels(params)
                reference = _ReferenceGroups(reference_params)
                events = list(finite)
                events[position] = np.asarray([infinite_value, 4.0], dtype=np.float64)

                for vector in events:
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                        io.StringIO()
                    ):
                        observed = actual.score("A", vector)
                        expected = reference.score("A", vector)
                    _assert_same_observation(self, observed, expected)
                    self.assertEqual(_core_state(actual, "A"), _core_state(reference, "A"))

    def test_readiness_checks_feature_map_and_elapsed_grace(self) -> None:
        groups = KitNetGroupModels(KitNetParams(10, 2, 2, 0.1, 0.75))
        self.assertFalse(groups.is_ready("missing"))

        # NaN counts as seen in the existing wrapper, but it is not trained by
        # PySAD and therefore cannot establish readiness on its own.
        for _ in range(5):
            groups.score("A", np.asarray([float("nan"), 1.0]))
        self.assertFalse(groups.is_warming_up("A"))
        self.assertFalse(groups.is_ready("A"))
        self.assertTrue(groups.last_input_invalid("A"))

        for index in range(5):
            groups.score("B", np.asarray([float(index), float(index + 1)]))
        self.assertTrue(groups.is_ready("B"))
        self.assertFalse(groups.last_input_invalid("B"))

    def test_score_batch_is_read_only_for_learned_state_and_tracks_invalid_inputs(self) -> None:
        params = KitNetParams(10, 2, 2, 0.1, 0.75)
        actual = KitNetGroupModels(params)
        reference = _ReferenceGroups(_ReferenceParams(10, 2, 2, 0.1, 0.75))
        with self.assertRaises(ValueError):
            actual.score_batch("A", np.asarray([[1.0, 2.0]]))

        training = [np.asarray([float(i), float(i + 1)]) for i in range(5)]
        for vector in training:
            actual.score("A", vector)
            reference.score("A", vector)
        self.assertTrue(actual.is_ready("A"))

        core = actual.models_by_group["A"].model
        seen, trained = actual.samples_seen("A"), core.n_trained
        batch = np.asarray([[1.5, 2.5], [2.5, 3.5]], dtype=np.float64)
        actual_scores = actual.score_batch("A", batch)
        expected_scores = np.asarray(
            [reference.models_by_group["A"].model.execute(row) for row in batch]
        )
        np.testing.assert_array_equal(actual_scores, expected_scores)
        self.assertEqual(actual.samples_seen("A"), seen)
        self.assertEqual(core.n_trained, trained)

        actual.score("A", np.asarray([float("inf"), 6.0]))
        self.assertTrue(actual.last_input_invalid("A"))
        actual.score("A", np.asarray([6.0, 7.0]))
        self.assertFalse(actual.last_input_invalid("A"))

    def test_score_batch_requires_matrix_input(self) -> None:
        groups = KitNetGroupModels(KitNetParams(10, 2, 2, 0.1, 0.75))
        for index in range(5):
            groups.score("A", np.asarray([float(index), float(index + 1)]))
        with self.assertRaises(ValueError):
            groups.score_batch("A", np.asarray([1.0, 2.0]))
