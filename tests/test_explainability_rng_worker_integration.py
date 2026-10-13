import random
from contextlib import contextmanager
from unittest import TestCase
from unittest.mock import patch

import numpy as np

from dasma_kafka.common import isolated_rng
from dasma_kafka.explainability import runtime as v1_runtime
from dasma_kafka.explainability_v2 import runtime as v2_runtime
from tests import test_explainability_v1_runtime_statuses as v1_harness
from tests import test_explainability_v2_runtime_statuses as v2_harness


def _numpy_states_equal(left, right) -> bool:
    return (
        left[0] == right[0]
        and np.array_equal(left[1], right[1])
        and left[2:] == right[2:]
    )


def _recording_context_factory(observations):
    def factory(seed):
        @contextmanager
        def context():
            before_numpy = np.random.get_state()
            before_python = random.getstate()
            with isolated_rng(seed):
                yield
            after_numpy = np.random.get_state()
            after_python = random.getstate()
            observations.append(
                (
                    _numpy_states_equal(before_numpy, after_numpy),
                    before_python == after_python,
                )
            )

        return context()

    return factory


class ExplainabilityWorkerRngIntegrationTest(TestCase):
    def _assert_runtime_restores_rng(self, runtime_module, harness_class, base_record, **kwargs):
        observations = []
        harness = harness_class()
        with patch.object(
            runtime_module,
            "isolated_rng",
            side_effect=_recording_context_factory(observations),
        ):
            run = harness._run_worker([base_record], **kwargs)

        self.assertIsNone(run.error)
        self.assertEqual(observations, [(True, True)])

    def test_v1_restores_rng_after_computed_failed_and_exception(self) -> None:
        cases = (
            {},
            {
                "shap_return": {
                    "shap_values": None,
                    "shap_base_value": None,
                    "shap_computation_time_ms": 1.0,
                }
            },
            {"shap_side_effect": RuntimeError("shap failure")},
        )
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                self._assert_runtime_restores_rng(
                    v1_runtime,
                    v1_harness.ExplainabilityV1RuntimeStatusTest,
                    v1_harness._base_record(),
                    **kwargs,
                )

    def test_v2_restores_rng_after_computed_failed_and_exception(self) -> None:
        cases = (
            {},
            {
                "shap_return": {
                    "shap_values": None,
                    "shap_base_value": None,
                    "shap_computation_time_ms": 1.0,
                }
            },
            {"shap_side_effect": RuntimeError("shap failure")},
        )
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                self._assert_runtime_restores_rng(
                    v2_runtime,
                    v2_harness.ExplainabilityV2RuntimeStatusTest,
                    v2_harness._base_record(),
                    **kwargs,
                )
