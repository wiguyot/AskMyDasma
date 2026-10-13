import random
from unittest import TestCase

import numpy as np

from dasma_kafka.common import isolated_rng


def _numpy_states_equal(left, right) -> bool:
    return (
        left[0] == right[0]
        and np.array_equal(left[1], right[1])
        and left[2:] == right[2:]
    )


class IsolatedRngTest(TestCase):
    def test_success_restores_numpy_and_python_states_exactly(self) -> None:
        np.random.seed(101)
        random.seed(202)
        expected_numpy_state = np.random.get_state()
        expected_python_state = random.getstate()

        with isolated_rng(42):
            np.random.random(5)
            random.random()

        self.assertTrue(_numpy_states_equal(np.random.get_state(), expected_numpy_state))
        self.assertEqual(random.getstate(), expected_python_state)

    def test_exception_restores_numpy_and_python_states_exactly(self) -> None:
        np.random.seed(303)
        random.seed(404)
        expected_numpy_state = np.random.get_state()
        expected_python_state = random.getstate()

        with self.assertRaisesRegex(RuntimeError, "shap failed"):
            with isolated_rng(42):
                np.random.random(5)
                random.random()
                raise RuntimeError("shap failed")

        self.assertTrue(_numpy_states_equal(np.random.get_state(), expected_numpy_state))
        self.assertEqual(random.getstate(), expected_python_state)

    def test_same_seed_produces_same_sequence_from_different_initial_states(self) -> None:
        np.random.seed(1)
        random.seed(2)
        with isolated_rng(42):
            first_numpy = np.random.random(5)
            first_python = [random.random() for _ in range(5)]

        np.random.seed(999)
        random.seed(888)
        with isolated_rng(42):
            second_numpy = np.random.random(5)
            second_python = [random.random() for _ in range(5)]

        np.testing.assert_array_equal(first_numpy, second_numpy)
        self.assertEqual(first_python, second_python)
