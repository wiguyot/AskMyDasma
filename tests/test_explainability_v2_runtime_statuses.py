"""Runtime status attribution tests for explainability-v2 (OC-02).

These tests exercise ``group_worker_main`` in isolation: Kafka producers, the
HSTree model manager, the SHAP engine and the feature extractor are mocked, and
the work queue is a plain in-process queue. The asserted contract is the
``explanation_status`` vocabulary and the identity gate:

- SHAP disabled                    -> ``disabled``            (no SHAP call)
- HSTree local not initialized     -> ``warmup``              (no SHAP call)
- background or model unavailable  -> ``background_not_ready``(no SHAP call)
- SHAP computed with values        -> ``computed``
- SHAP result without shap_values  -> ``failed`` + ``shap_exception``
- SHAP raising unexpectedly        -> ``failed`` + ``shap_exception``
  (the worker survives; the exception detail never reaches the published
  message, only the logs)
- missing, empty or blank ``source_event_id`` -> explicit fatal error and
  nothing published, before any business processing (no model fit, no SHAP)

SHAP must stay exhaustive: no branch may filter on ``hstree_anomaly``.
"""

from __future__ import annotations

import hashlib
import json
import queue
import unittest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import numpy as np

from dasma_kafka.explainability_v2 import runtime as runtime_module
from dasma_kafka.explainability_v2.runtime import GroupWorkItem, group_worker_main


WORKER_CONFIG: Dict[str, Any] = {
    "bootstrap_servers": "kafka:9092",
    "source_topic": "hstree-analysis",
    "explained_model": "hstree",
    "explainability_mode": "reference",
    "explanation_method": "kernel",
    "dest_topic": "explainability-v2",
    "dest_topic_json": "explainability-v2-viz",
    "schema_registry_url": "http://schema-registry:8081",
    "group_id": "explainability-v2-consumer",
    "log_level": "CRITICAL",
    "hstree_window_size": 2048,
    "hstree_num_trees": 25,
    "hstree_max_depth": 15,
    "hstree_initial_window_size": 512,
    "hstree_seed": 42,
    "shap_background_size": 100,
    "shap_nsamples": 100,
    "shap_enabled": True,
    "explainability_seed": 42,
}

# Independently recomputed expectations (not via the production helpers) so the
# runtime tests also pin the canonical JSON representation.
EXPECTED_FEATURE_FINGERPRINT = hashlib.sha256(
    json.dumps(["A", "B"], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
).hexdigest()
EXPECTED_MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(
        {
            "hstree_model_version": "local-hstree-v3",
            "hstree_window_size": 2048,
            "hstree_num_trees": 25,
            "hstree_max_depth": 15,
            "hstree_initial_window_size": 512,
            "hstree_seed": 42,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()

EXPECTED_METADATA: Dict[str, Any] = {
    "explanation_profile": "exhaustive",
    "shap_nsamples": 100,
    "shap_background_size": 100,
    "explainability_seed": 42,
    "hstree_model_version": "local-hstree-v3",
    "hstree_model_fingerprint": EXPECTED_MODEL_FINGERPRINT,
    "feature_fingerprint": EXPECTED_FEATURE_FINGERPRINT,
}

COMPUTED_SHAP_RESULT = {
    "shap_values": {"A": 0.1, "B": -0.2},
    "shap_base_value": 0.05,
    "shap_computation_time_ms": 1.5,
}


@dataclass
class WorkerRun:
    """Everything a test needs to assert on a worker run."""

    error: Optional[BaseException]
    avro_producer: MagicMock
    json_producer: MagicMock
    model_manager: MagicMock
    shap_explainer: MagicMock
    feature_extractor: MagicMock
    published_records: List[Dict[str, Any]] = field(default_factory=list)
    published_payloads: List[Dict[str, Any]] = field(default_factory=list)


def _work_item(record: Dict[str, Any], offset: int) -> GroupWorkItem:
    return GroupWorkItem(
        key_str=None,
        record=record,
        topic="hstree-analysis",
        partition=0,
        offset=offset,
    )


def _base_record(**overrides: Any) -> Dict[str, Any]:
    record = {
        "timestamp": "2026-01-01 00:00:01",
        "Group": "g1",
        "source_event_id": "row-000001",
        "hstree_score": 0.5,
        "iforest_score": 0.5,
        "hstree_status": "anomaly",
        "hstree_anomaly": True,
        "hstree_threshold": 0.7,
        "A": 1.5,
        "B": 2.5,
    }
    record.update(overrides)
    return record


class ExplainabilityV2RuntimeStatusTest(unittest.TestCase):
    def test_iforest_tree_method_publishes_named_scale_without_kernel_metadata(self) -> None:
        run = self._run_worker(
            [_base_record()], explained_model="iforest", explanation_method="tree"
        )

        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_not_called()
        run.shap_explainer.compute_tree_values.assert_called_once()
        record = run.published_records[0]
        self.assertEqual(record["explanation_status"], "computed")
        self.assertEqual(record["explained_value"], 4.9)
        self.assertEqual(record["explanation_output"], "iforest_mean_path_length")
        self.assertIsNone(record["shap_nsamples"])
        self.assertIsNone(record["explainability_seed"])
        self.assertEqual(record["shap_background_size"], WORKER_CONFIG["shap_background_size"])

    def test_iforest_tree_method_keeps_background_not_ready_status(self) -> None:
        run = self._run_worker(
            [_base_record()], explained_model="iforest", explanation_method="tree",
            background_return=None,
        )

        record = run.published_records[0]
        self.assertEqual(record["explanation_status"], "background_not_ready")
        self.assertIsNone(record["explained_value"])
        self.assertIsNone(record["explanation_output"])
        run.shap_explainer.compute_tree_values.assert_not_called()

    def test_iforest_tree_method_exception_publishes_failed_without_tree_scale(self) -> None:
        run = self._run_worker(
            [_base_record()], explained_model="iforest", explanation_method="tree",
            tree_side_effect=RuntimeError("conversion failed"),
        )

        record = run.published_records[0]
        self.assertEqual(record["explanation_status"], "failed")
        self.assertEqual(record["explanation_error_code"], "shap_exception")
        self.assertIsNone(record["shap_values"])
        self.assertIsNone(record["shap_base_value"])
        self.assertIsNone(record["explained_value"])
        self.assertIsNone(record["explanation_output"])

    def test_tree_method_does_not_enter_kernel_rng_isolation(self) -> None:
        with patch.object(runtime_module, "isolated_rng") as isolated_rng:
            run = self._run_worker(
                [_base_record()], explained_model="iforest", explanation_method="tree"
            )
        self.assertIsNone(run.error)
        isolated_rng.assert_not_called()

    def test_worker_rejects_missing_model_or_mode_with_group_context(self) -> None:
        for field, invalid_value in (("explained_model", ""), ("explainability_mode", "")):
            with self.subTest(field=field):
                config_dict = dict(WORKER_CONFIG, **{field: invalid_value})
                with self.assertRaisesRegex(ValueError, r"\[test-group\].*must be"):
                    group_worker_main("test-group", config_dict, ["A", "B"], queue.Queue())

    def _run_worker(
        self,
        records: List[Dict[str, Any]],
        *,
        shap_enabled: bool = True,
        explained_model: str = "hstree",
        explainability_mode: str = "reference",
        explanation_method: str = "kernel",
        fit_score_return: Optional[float] = 0.5,
        input_error_return: Optional[str] = None,
        background_return: Any = "__array__",
        model_return: Any = "__model__",
        shap_return: Optional[Dict[str, Any]] = COMPUTED_SHAP_RESULT,
        shap_side_effect: Optional[BaseException] = None,
        tree_side_effect: Optional[BaseException] = None,
    ) -> WorkerRun:
        config_dict = dict(
            WORKER_CONFIG,
            shap_enabled=shap_enabled,
            explained_model=explained_model,
            explainability_mode=explainability_mode,
            explanation_method=explanation_method,
        )

        model_manager = MagicMock(name="model-manager")
        model_manager.reference_score_field = (
            {"iforest": "iforest_score", "kitnet": "kitnet_score"}.get(explained_model, "hstree_score")
        )
        model_manager.mismatch_code = (
            {"iforest": "iforest_score_mismatch", "kitnet": "kitnet_score_mismatch"}.get(explained_model, "hstree_score_mismatch")
        )
        model_manager.model_version = (
            {"iforest": "local-iforest-v1", "kitnet": "local-kitnet-v1"}.get(explained_model, "local-hstree-v3")
        )
        model_manager.model_fingerprint.return_value = "f" * 64
        model_manager.fit_score.return_value = fit_score_return
        model_manager.input_error.return_value = input_error_return
        model_manager.get_background_data.return_value = (
            np.zeros((100, 2)) if background_return == "__array__" else background_return
        )
        model_manager.get_model.return_value = (
            MagicMock(name="hstree-model") if model_return == "__model__" else model_return
        )

        shap_explainer = MagicMock(name="shap-explainer")
        if shap_side_effect is not None:
            shap_explainer.compute_shap_values.side_effect = shap_side_effect
        else:
            shap_explainer.compute_shap_values.return_value = shap_return
        shap_explainer.compute_tree_values.return_value = {
            "shap_values": {"A": 0.1, "B": -0.2},
            "shap_base_value": 5.0,
            "shap_computation_time_ms": 1.5,
            "explained_value": 4.9,
            "explanation_output": "iforest_mean_path_length",
        }
        if tree_side_effect is not None:
            shap_explainer.compute_tree_values.side_effect = tree_side_effect

        avro_producer = MagicMock(name="avro-producer")
        json_producer = MagicMock(name="json-producer")
        feature_extractor = MagicMock(name="feature-extractor")
        feature_extractor.extract.return_value = np.array([1.5, 2.5])

        def _fire_delivery(topic, key=None, value=None, on_delivery=None, **kwargs):
            # OC-07 mocks: fire the delivery callback immediately so the
            # worker never waits for the bounded delivery timeout.
            if on_delivery is not None:
                on_delivery(None, MagicMock())
            return 0

        avro_producer.produce.side_effect = _fire_delivery
        json_producer.produce.side_effect = _fire_delivery

        work_q: queue.Queue = queue.Queue()
        for offset, record in enumerate(records):
            work_q.put(_work_item(record, offset))
        work_q.put(None)

        error: Optional[BaseException] = None
        with patch.object(
            runtime_module, "create_schema_registry_client", return_value=MagicMock()
        ), patch.object(
            runtime_module, "AvroSerializer", MagicMock()
        ), patch.object(
            runtime_module,
            "SerializingProducer",
            side_effect=[avro_producer, json_producer],
        ), patch.object(
            runtime_module, "HSTreeModelManager", return_value=model_manager
        ), patch.object(
            runtime_module, "IForestModelManager", return_value=model_manager
        ), patch.object(
            runtime_module, "KitNetModelManager", return_value=model_manager
        ), patch.object(
            runtime_module, "SHAPExplainer", return_value=shap_explainer
        ), patch.object(
            runtime_module, "FeatureExtractor", return_value=feature_extractor
        ), patch.object(
            runtime_module, "KitNetFeatureExtractor", return_value=feature_extractor
        ):
            try:
                group_worker_main("g1", config_dict, ["A", "B"], work_q)
            except Exception as exc:  # noqa: BLE001 - the test asserts on it
                error = exc

        published_records = [
            call.kwargs["value"] for call in avro_producer.produce.call_args_list
        ]
        published_payloads = []
        for call in json_producer.produce.call_args_list:
            published_payloads.append(json.loads(call.kwargs["value"]))

        return WorkerRun(
            error=error,
            avro_producer=avro_producer,
            json_producer=json_producer,
            model_manager=model_manager,
            shap_explainer=shap_explainer,
            feature_extractor=feature_extractor,
            published_records=published_records,
            published_payloads=published_payloads,
        )

    def test_disabled_without_shap_call(self) -> None:
        run = self._run_worker([_base_record()], shap_enabled=False)

        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_not_called()
        self.assertEqual(len(run.published_records), 1)
        record = run.published_records[0]
        self.assertEqual(record["explanation_status"], "disabled")
        self.assertIsNone(record["explanation_error_code"])
        self.assertIsNone(record["shap_values"])

    def test_warmup_without_shap_call(self) -> None:
        run = self._run_worker([_base_record()], fit_score_return=None)

        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_not_called()
        self.assertEqual(len(run.published_records), 1)
        record = run.published_records[0]
        self.assertEqual(record["explanation_status"], "warmup")
        self.assertIsNone(record["explanation_error_code"])
        self.assertIsNone(record["shap_values"])
        self.assertIsNone(record["local_hstree_score"])

    def test_invalid_features_fail_before_warmup(self) -> None:
        run = self._run_worker(
            [_base_record()], fit_score_return=None, input_error_return="invalid_features"
        )
        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_not_called()
        self.assertEqual(run.published_records[0]["explanation_status"], "failed")
        self.assertEqual(run.published_records[0]["explanation_error_code"], "invalid_features")

    def test_kitnet_model_selects_kitnet_manager_and_extractor(self) -> None:
        run = self._run_worker(
            [_base_record()], explained_model="kitnet", explainability_mode="simple"
        )
        self.assertIsNone(run.error)
        run.model_manager.fit_score.assert_called_once()
        run.feature_extractor.extract.assert_called_once()
        self.assertEqual(run.published_records[0]["explained_model"], "kitnet")

    def test_background_not_ready_without_shap_call(self) -> None:
        run = self._run_worker(
            [_base_record()],
            fit_score_return=0.5,
            background_return=None,
        )

        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_not_called()
        self.assertEqual(len(run.published_records), 1)
        self.assertEqual(run.published_records[0]["explanation_status"], "background_not_ready")
        self.assertIsNone(run.published_records[0]["shap_values"])

    def test_background_not_ready_when_model_unavailable(self) -> None:
        run = self._run_worker(
            [_base_record()],
            fit_score_return=0.5,
            model_return=None,
        )

        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_not_called()
        self.assertEqual(run.published_records[0]["explanation_status"], "background_not_ready")

    def test_computed_with_shap_values(self) -> None:
        run = self._run_worker([_base_record()], fit_score_return=0.5)

        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_called_once()
        record = run.published_records[0]
        self.assertEqual(record["explanation_status"], "computed")
        self.assertIsNone(record["explanation_error_code"])
        self.assertEqual(record["shap_values"], {"A": 0.1, "B": -0.2})
        self.assertEqual(record["shap_base_value"], 0.05)
        self.assertEqual(record["local_hstree_score"], 0.5)

    def test_model_mismatch_skips_shap_and_publishes_both_scores(self) -> None:
        run = self._run_worker(
            [_base_record(hstree_score=0.75)], fit_score_return=0.5
        )

        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_not_called()
        record = run.published_records[0]
        self.assertEqual(record["explanation_status"], "model_mismatch")
        self.assertEqual(record["explanation_error_code"], "hstree_score_mismatch")
        self.assertEqual(record["hstree_score"], 0.75)
        self.assertEqual(record["local_hstree_score"], 0.5)
        self.assertIsNone(record["shap_values"])
        self.assertIsNone(record["shap_base_value"])

    def test_invalid_upstream_score_skips_shap(self) -> None:
        for invalid in (None, "0.5", True, float("nan"), float("inf")):
            with self.subTest(invalid=repr(invalid)):
                run = self._run_worker(
                    [_base_record(hstree_score=invalid)], fit_score_return=0.5
                )
                self.assertIsNone(run.error)
                run.shap_explainer.compute_shap_values.assert_not_called()
                record = run.published_records[0]
                self.assertEqual(record["explanation_status"], "failed")
                self.assertEqual(record["explanation_error_code"], "invalid_upstream_score")

    def test_reference_prioritizes_invalid_upstream_when_both_scores_are_invalid(self) -> None:
        run = self._run_worker(
            [_base_record(hstree_score=float("nan"))],
            fit_score_return=float("nan"),
        )

        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_not_called()
        self.assertEqual(run.published_records[0]["explanation_error_code"], "invalid_upstream_score")

    def test_invalid_local_score_skips_shap(self) -> None:
        for invalid in (True, float("nan"), float("-inf")):
            with self.subTest(invalid=repr(invalid)):
                run = self._run_worker(
                    [_base_record()], fit_score_return=invalid
                )
                self.assertIsNone(run.error)
                run.shap_explainer.compute_shap_values.assert_not_called()
                record = run.published_records[0]
                self.assertEqual(record["explanation_status"], "failed")
                self.assertEqual(record["explanation_error_code"], "invalid_local_score")

    def test_simple_mode_rejects_invalid_local_score_without_calling_shap(self) -> None:
        run = self._run_worker(
            [_base_record()],
            fit_score_return=float("nan"),
            explainability_mode="simple",
        )

        self.assertIsNone(run.error)
        run.shap_explainer.compute_shap_values.assert_not_called()
        self.assertEqual(run.published_records[0]["explanation_error_code"], "invalid_local_score")

    def test_model_mismatch_does_not_stop_the_worker(self) -> None:
        run = self._run_worker(
            [_base_record(hstree_score=0.75), _base_record(source_event_id="row-2")],
            fit_score_return=0.5,
        )

        self.assertIsNone(run.error)
        self.assertEqual(
            [record["explanation_status"] for record in run.published_records],
            ["model_mismatch", "computed"],
        )
        self.assertEqual(run.shap_explainer.compute_shap_values.call_count, 1)

    def test_failed_when_shap_returns_null_values(self) -> None:
        run = self._run_worker(
            [_base_record()],
            shap_return={
                "shap_values": None,
                "shap_base_value": None,
                "shap_computation_time_ms": 12.0,
            },
        )

        self.assertIsNone(run.error)
        record = run.published_records[0]
        self.assertEqual(record["explanation_status"], "failed")
        self.assertEqual(record["explanation_error_code"], "shap_exception")
        # Coherent with the available result: the engine failure timing is
        # kept, values stay null.
        self.assertIsNone(record["shap_values"])
        self.assertIsNone(record["shap_base_value"])
        self.assertEqual(record["shap_computation_time_ms"], 12.0)

    def test_failed_when_shap_raises_unexpected_exception(self) -> None:
        run = self._run_worker(
            [_base_record()],
            shap_side_effect=RuntimeError("boom - secret detail"),
        )

        # The worker survives: the exception is caught and turned into a
        # failed result.
        self.assertIsNone(run.error)
        self.assertEqual(len(run.published_records), 1)
        record = run.published_records[0]
        self.assertEqual(record["explanation_status"], "failed")
        self.assertEqual(record["explanation_error_code"], "shap_exception")
        self.assertIsNone(record["shap_values"])
        self.assertIsNone(record["shap_base_value"])
        self.assertIsNone(record["shap_computation_time_ms"])
        # The technical detail never reaches the published message.
        self.assertNotIn("boom", json.dumps(record))

    def test_invalid_identity_is_fatal_and_publishes_nothing(self) -> None:
        for invalid in (None, "", "   ", 42):
            with self.subTest(invalid=invalid):
                record = _base_record()
                if invalid is None:
                    del record["source_event_id"]
                else:
                    record["source_event_id"] = invalid

                run = self._run_worker([record])

                self.assertIsNotNone(run.error, repr(invalid))
                self.assertIsInstance(run.error, ValueError)
                self.assertIn("source_event_id", str(run.error))
                # Nothing published, and no business processing at all: the
                # gate runs before the model fit and before SHAP.
                run.avro_producer.produce.assert_not_called()
                run.json_producer.produce.assert_not_called()
                run.model_manager.fit_score.assert_not_called()
                run.shap_explainer.compute_shap_values.assert_not_called()

    def test_valid_identity_and_hstree_verdict_are_propagated(self) -> None:
        run = self._run_worker([_base_record()])

        self.assertIsNone(run.error)
        self.assertEqual(len(run.published_records), 1)
        record = run.published_records[0]
        self.assertEqual(record["source_event_id"], "row-000001")
        self.assertEqual(record["hstree_status"], "anomaly")
        self.assertIs(record["hstree_anomaly"], True)
        self.assertEqual(record["hstree_threshold"], 0.7)
        self.assertEqual(record["hstree_score"], 0.5)
        self.assertEqual(record["explanation_status"], "computed")
        # The JSON duplication carries the same contract.
        self.assertEqual(len(run.published_payloads), 1)
        self.assertEqual(run.published_payloads[0]["source_event_id"], "row-000001")
        self.assertEqual(run.published_payloads[0]["explanation_status"], "computed")

    def test_shap_stays_exclusive_of_anomaly_filtering(self) -> None:
        run = self._run_worker(
            [
                _base_record(source_event_id="row-000001", hstree_anomaly=True),
                _base_record(
                    source_event_id="row-000002",
                    hstree_status="normal",
                    hstree_anomaly=False,
                ),
            ]
        )

        self.assertIsNone(run.error)
        self.assertEqual(run.shap_explainer.compute_shap_values.call_count, 2)
        statuses = [record["explanation_status"] for record in run.published_records]
        self.assertEqual(statuses, ["computed", "computed"])
        identities = [record["source_event_id"] for record in run.published_records]
        self.assertEqual(identities, ["row-000001", "row-000002"])

    def test_all_statuses_carry_the_computation_metadata(self) -> None:
        status_runs = {
            "disabled": {"shap_enabled": False},
            "warmup": {"fit_score_return": None},
            "background_not_ready": {"background_return": None},
            "computed": {},
            "failed": {
                "shap_return": {
                    "shap_values": None,
                    "shap_base_value": None,
                    "shap_computation_time_ms": 3.0,
                }
            },
        }
        for status, kwargs in status_runs.items():
            with self.subTest(status=status):
                run = self._run_worker([_base_record()], **kwargs)

                self.assertIsNone(run.error, status)
                self.assertEqual(len(run.published_records), 1, status)
                record = run.published_records[0]
                self.assertEqual(record["explanation_status"], status)
                for name, expected in EXPECTED_METADATA.items():
                    self.assertEqual(record[name], expected, name)

    def test_model_metadata_matches_all_model_and_mode_combinations(self) -> None:
        for model in ("hstree", "iforest"):
            for mode in ("simple", "reference"):
                with self.subTest(model=model, mode=mode):
                    run = self._run_worker(
                        [_base_record()], explained_model=model, explainability_mode=mode
                    )

                    self.assertIsNone(run.error)
                    record = run.published_records[0]
                    if model == "hstree":
                        self.assertEqual(record["hstree_model_version"], "local-hstree-v3")
                        self.assertRegex(record["hstree_model_fingerprint"], r"^[0-9a-f]{64}$")
                        self.assertEqual(record["model_version"], "local-hstree-v3")
                    else:
                        self.assertIsNone(record["hstree_model_version"])
                        self.assertIsNone(record["hstree_model_fingerprint"])
                        self.assertEqual(record["model_version"], "local-iforest-v1")
                    self.assertEqual(record["model_fingerprint"], "f" * 64)


if __name__ == "__main__":
    unittest.main()
