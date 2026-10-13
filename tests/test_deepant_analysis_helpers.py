"""Unit tests for deepant-analysis pure helpers and classifiers (no Kafka)."""

from __future__ import annotations

import unittest
from unittest import TestCase

try:
    import torch

    TORCH_AVAILABLE = True
except ImportError:
    torch = None  # type: ignore[assignment]
    TORCH_AVAILABLE = False

try:
    from runtime.detectors.deepant.models.deepant import (
        classify_deepant_score,
        classify_deepant_score_percentile,
    )
    CLASSIFIERS_AVAILABLE = True
except Exception:
    classify_deepant_score = None  # type: ignore[assignment]
    classify_deepant_score_percentile = None  # type: ignore[assignment]
    CLASSIFIERS_AVAILABLE = False

import json

from dasma_kafka.anomaly.thresholds import (
    SlidingPercentileEstimator,
    WelfordEstimator,
)
from dasma_kafka.common.threshold_modes import (
    DEEPANT_ANALYSIS_SCHEMA_FIELDS,
    extend_deepant_avro_schema,
    normalize_deepant_threshold_mode,
    parse_deepant_exclude_anomalies_from_refit,
)
from dasma_kafka.deepant_analysis.helpers import (
    build_feature_vector,
    create_viz_state,
    ensure_deepant_analysis_fields,
    ensure_deepant_score,
    enrich_viz_record,
    infer_feature_names,
    resolve_group_key,
    strip_excluded_fields,
)


class DeepAntAnalysisHelpersTest(TestCase):
    def test_resolve_group_key(self) -> None:
        self.assertEqual(resolve_group_key({"Group": "A"}), "A")
        self.assertEqual(resolve_group_key({"group": "B"}), "B")
        self.assertEqual(resolve_group_key({"x": 1}), "__NO_GROUP__")

    def test_infer_feature_names_excludes_meta_fields(self) -> None:
        record = {"timestamp": "x", "a": 1, "b": 2.5, "Group": "g", "c": "nope"}
        self.assertEqual(
            infer_feature_names(record, {"timestamp", "Group"}), ["a", "b"]
        )

    def test_infer_feature_names_returns_sorted(self) -> None:
        record = {"z": 1.0, "a": 2.0, "m": 3.0}
        self.assertEqual(infer_feature_names(record, set()), ["a", "m", "z"])

    def test_infer_feature_names_handles_non_dict(self) -> None:
        self.assertEqual(infer_feature_names("nope", set()), [])

    def test_build_feature_vector_uses_default_zero(self) -> None:
        record = {"a": 1}
        self.assertEqual(build_feature_vector(record, ["a", "b"]), [1.0, 0.0])

    def test_build_feature_vector_preserves_feature_order(self) -> None:
        record = {"a": 1, "b": 2, "c": 3}
        self.assertEqual(
            build_feature_vector(record, ["c", "a", "b"]), [3.0, 1.0, 2.0]
        )

    def test_build_feature_vector_rejects_non_dict(self) -> None:
        with self.assertRaises(TypeError):
            build_feature_vector("nope", ["a"])

    def test_ensure_deepant_score_with_none(self) -> None:
        record, score = ensure_deepant_score({"a": 1}, None)
        self.assertEqual(score, 0.0)
        self.assertEqual(record["deepant_score"], 0.0)
        self.assertEqual(record["deepant_status"], "warming_up")
        self.assertIsNone(record["deepant_anomaly"])
        self.assertIsNone(record["deepant_threshold"])

    def test_ensure_deepant_score_passes_through_value(self) -> None:
        record, score = ensure_deepant_score({"a": 1}, 1.5)
        self.assertEqual(score, 1.5)
        self.assertEqual(record["deepant_score"], 1.5)

    def test_ensure_deepant_analysis_fields_full(self) -> None:
        record, score = ensure_deepant_analysis_fields(
            {"a": 1}, 0.7, "anomaly", True, 0.6
        )
        self.assertEqual(score, 0.7)
        self.assertEqual(record["deepant_score"], 0.7)
        self.assertEqual(record["deepant_status"], "anomaly")
        self.assertTrue(record["deepant_anomaly"])
        self.assertEqual(record["deepant_threshold"], 0.6)

    def test_ensure_deepant_analysis_fields_handles_non_dict(self) -> None:
        record, score = ensure_deepant_analysis_fields(None, None, None, None, None)
        self.assertEqual(score, 0.0)
        self.assertEqual(record["deepant_score"], 0.0)
        self.assertEqual(record["deepant_status"], "warming_up")

    def test_strip_excluded_fields_basic(self) -> None:
        record = strip_excluded_fields(
            {"Group": "A", "timestamp": "t", "a": 1, "b": 2},
            {"Group", "timestamp"},
        )
        self.assertEqual(record, {"a": 1, "b": 2})

    def test_strip_excluded_fields_preserves_metadata(self) -> None:
        record = strip_excluded_fields(
            {"Group": "A", "timestamp": "t", "source_event_id": "event-1", "a": 1},
            {"Group", "timestamp", "source_event_id"},
            preserve_fields={"Group", "timestamp", "source_event_id"},
        )
        self.assertEqual(record, {"Group": "A", "timestamp": "t", "source_event_id": "event-1", "a": 1})

    def test_enrich_viz_record_without_moving_average(self) -> None:
        state = create_viz_state()
        record = enrich_viz_record({"deepant_score": 100.0}, state, 0)
        self.assertEqual(record["deepant_score_log10"], 2.0)
        self.assertEqual(record["deepant_score_log10_diff"], 0.0)
        self.assertNotIn("deepant_score_ma_0", record)

    def test_enrich_viz_record_log10_diff_between_calls(self) -> None:
        state = create_viz_state()
        enrich_viz_record({"deepant_score": 10.0}, state, 0)
        record = enrich_viz_record({"deepant_score": 100.0}, state, 0)
        self.assertEqual(record["deepant_score_log10"], 2.0)
        self.assertEqual(record["deepant_score_log10_diff"], 1.0)

    def test_enrich_viz_record_zero_score_yields_zero_log10(self) -> None:
        state = create_viz_state()
        record = enrich_viz_record({"deepant_score": 0.0}, state, 0)
        self.assertEqual(record["deepant_score_log10"], 0.0)

    def test_enrich_viz_record_with_moving_average(self) -> None:
        state = create_viz_state()
        enrich_viz_record({"deepant_score": 4.0}, state, 2)
        record = enrich_viz_record({"deepant_score": 16.0}, state, 2)
        self.assertEqual(record["deepant_score_ma_2"], 10.0)
        self.assertAlmostEqual(record["deepant_score_ma_2_log10"], 1.0)

    def test_enrich_viz_record_moving_average_not_emitted_before_full_window(
        self,
    ) -> None:
        state = create_viz_state()
        record = enrich_viz_record({"deepant_score": 4.0}, state, 3)
        self.assertEqual(record["deepant_score_ma_3"], 0.0)

    def test_extend_deepant_avro_schema_adds_fields_idempotently(self) -> None:
        schema = {
            "type": "record",
            "name": "Component",
            "fields": [{"name": "temperature", "type": "double"}],
        }
        extended_once = extend_deepant_avro_schema(json.dumps(schema))
        extended_twice = extend_deepant_avro_schema(extended_once)

        fields = json.loads(extended_twice)["fields"]
        fields_by_name = {field["name"]: field for field in fields}

        self.assertEqual(fields_by_name["deepant_score"]["type"], ["null", "double"])
        self.assertEqual(fields_by_name["deepant_status"]["type"], ["null", "string"])
        self.assertEqual(
            fields_by_name["deepant_anomaly"]["type"], ["null", "boolean"]
        )
        self.assertEqual(
            fields_by_name["deepant_threshold"]["type"], ["null", "double"]
        )
        self.assertTrue(
            all(
                fields_by_name[name]["default"] is None
                for name in fields_by_name
                if name.startswith("deepant_")
            )
        )
        self.assertEqual(
            [f["name"] for f in fields].count("deepant_score"), 1
        )

    def test_deepant_analysis_schema_fields_are_well_formed(self) -> None:
        names = {n for n, _ in DEEPANT_ANALYSIS_SCHEMA_FIELDS}
        self.assertEqual(
            names,
            {"deepant_score", "deepant_status", "deepant_anomaly", "deepant_threshold"},
        )
        for _, avro_type in DEEPANT_ANALYSIS_SCHEMA_FIELDS:
            self.assertEqual(avro_type[0], "null")

    def test_normalize_deepant_threshold_mode_accepts_ui_p99_alias(self) -> None:
        self.assertEqual(normalize_deepant_threshold_mode("p99"), "percentile")
        self.assertEqual(normalize_deepant_threshold_mode(" P99 "), "percentile")

    def test_normalize_deepant_threshold_mode_keeps_internal_modes(self) -> None:
        self.assertEqual(
            normalize_deepant_threshold_mode("percentile"), "percentile"
        )
        self.assertEqual(normalize_deepant_threshold_mode("welford"), "welford")
        self.assertEqual(
            normalize_deepant_threshold_mode("welford-windowed"), "welford-windowed"
        )

    def test_normalize_deepant_threshold_mode_defaults_to_percentile(self) -> None:
        self.assertEqual(normalize_deepant_threshold_mode(None), "percentile")

    def test_parse_deepant_exclude_anomalies_from_refit(self) -> None:
        for value in ("1", "true", "TRUE", " yes ", "on"):
            self.assertTrue(parse_deepant_exclude_anomalies_from_refit(value))
        for value in (None, "", "0", "false", "no", "off"):
            self.assertFalse(parse_deepant_exclude_anomalies_from_refit(value))

    def test_enrich_viz_record_preserves_trigger_fields(self) -> None:
        state = create_viz_state()
        record = enrich_viz_record(
            {
                "deepant_score": 0.8,
                "deepant_status": "anomaly",
                "deepant_anomaly": True,
                "deepant_threshold": 0.5,
            },
            state,
            0,
        )
        self.assertEqual(record["deepant_status"], "anomaly")
        self.assertTrue(record["deepant_anomaly"])
        self.assertEqual(record["deepant_threshold"], 0.5)


@unittest.skipIf(not CLASSIFIERS_AVAILABLE, reason="torch or deepant model not installed")
class DeepAntClassifiersTest(TestCase):
    def test_classify_welford_warming_up_during_model_warmup(self) -> None:
        estimator = WelfordEstimator()
        status, anomaly, threshold = classify_deepant_score(
            0.0,
            estimator,
            is_warming_up=True,
            min_samples=2,
            k_sigma=3.0,
        )
        self.assertEqual(status, "warming_up")
        self.assertIsNone(anomaly)
        self.assertIsNone(threshold)
        self.assertEqual(estimator.n, 0)

    def test_classify_welford_detects_anomaly_and_rejects_contamination(self) -> None:
        estimator = WelfordEstimator()
        for value in [0.10, 0.20, 0.15]:
            estimator.update(value)
        status, anomaly, threshold = classify_deepant_score(
            0.30,
            estimator,
            is_warming_up=False,
            min_samples=3,
            k_sigma=1.0,
        )
        self.assertEqual(status, "anomaly")
        self.assertTrue(anomaly)
        self.assertIsNotNone(threshold)
        self.assertEqual(estimator.n, 3)

    def test_classify_welford_normal_score_updates_estimator(self) -> None:
        estimator = WelfordEstimator()
        for value in [0.10, 0.20, 0.15]:
            estimator.update(value)
        status, anomaly, _ = classify_deepant_score(
            0.16,
            estimator,
            is_warming_up=False,
            min_samples=3,
            k_sigma=3.0,
        )
        self.assertEqual(status, "normal")
        self.assertFalse(anomaly)
        self.assertEqual(estimator.n, 4)

    def test_classify_percentile_warming_up_during_model_warmup(self) -> None:
        est = SlidingPercentileEstimator(window_size=10, percentile=99.0)
        status, anomaly, threshold = classify_deepant_score_percentile(
            0.5, est, is_warming_up=True, min_samples=5,
        )
        self.assertEqual(status, "warming_up")
        self.assertIsNone(anomaly)
        self.assertIsNone(threshold)
        self.assertEqual(est.n, 0)

    def test_classify_percentile_detects_anomaly(self) -> None:
        est = SlidingPercentileEstimator(window_size=100, percentile=99.0)
        for _ in range(100):
            est.update(0.05)
        status, anomaly, threshold = classify_deepant_score_percentile(
            5.0, est, is_warming_up=False, min_samples=100,
        )
        self.assertEqual(status, "anomaly")
        self.assertTrue(anomaly)
        self.assertIsNotNone(threshold)
        self.assertEqual(est.n, 100)

    def test_classify_percentile_normal_score_updates_window(self) -> None:
        est = SlidingPercentileEstimator(window_size=100, percentile=99.0)
        import numpy as np
        for s in np.linspace(0.0, 1.0, 100):
            est.update(float(s))
        status, anomaly, _ = classify_deepant_score_percentile(
            0.4, est, is_warming_up=False, min_samples=100,
        )
        self.assertEqual(status, "normal")
        self.assertFalse(anomaly)
        self.assertEqual(est.n, 101)


if __name__ == "__main__":
    unittest.main()
