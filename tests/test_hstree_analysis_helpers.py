import json
from unittest import TestCase
from unittest.mock import patch

from dasma_kafka.hstree_analysis.helpers import (
    build_feature_vector,
    create_viz_state,
    ensure_hstree_analysis_fields,
    ensure_hstree_score,
    enrich_viz_record,
    infer_feature_names,
    resolve_group_key,
    strip_excluded_fields,
)
from runtime.detectors.hstree.models.hstree import (
    HSTreeGlobalModel,
    WelfordEstimator,
    SlidingPercentileEstimator,
    classify_hstree_score,
    classify_hstree_score_percentile,
)
from runtime.detectors.hstree.hstree_analysis import (
    _hstree_variant_from_environment,
    _warn_if_hstree_windows_differ,
    extend_hstree_avro_schema,
    normalize_hstree_threshold_mode,
)


class HSTreeAnalysisHelpersTest(TestCase):
    def test_hstree_variant_environment_defaults_and_rejects_unknown(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(_hstree_variant_from_environment(), "paper")
        with patch.dict("os.environ", {"HSTREE_VARIANT": "paper"}, clear=True):
            self.assertEqual(_hstree_variant_from_environment(), "paper")
        with patch.dict("os.environ", {"HSTREE_VARIANT": "typo"}, clear=True):
            with self.assertRaises(SystemExit) as error:
                _hstree_variant_from_environment()
        self.assertEqual(error.exception.code, 1)

    def test_resolve_group_key(self) -> None:
        self.assertEqual(resolve_group_key({"Group": "A"}), "A")
        self.assertEqual(resolve_group_key({"group": "B"}), "B")
        self.assertEqual(resolve_group_key({"x": 1}), "__NO_GROUP__")

    def test_infer_feature_names_excludes_meta_fields(self) -> None:
        record = {"timestamp": "x", "a": 1, "b": 2.5, "Group": "g", "c": "nope"}
        self.assertEqual(infer_feature_names(record, {"timestamp", "Group"}), ["a", "b"])

    def test_build_feature_vector_uses_default_zero(self) -> None:
        record = {"a": 1}
        self.assertEqual(build_feature_vector(record, ["a", "b"]), [1.0, 0.0])

    def test_ensure_hstree_score(self) -> None:
        record, score = ensure_hstree_score({"a": 1}, None)
        self.assertEqual(score, 0.0)
        self.assertEqual(record["hstree_score"], 0.0)
        self.assertEqual(record["hstree_status"], "warming_up")
        self.assertIsNone(record["hstree_anomaly"])
        self.assertIsNone(record["hstree_threshold"])

    def test_ensure_hstree_analysis_fields(self) -> None:
        record, score = ensure_hstree_analysis_fields(
            {"a": 1},
            0.7,
            "anomaly",
            True,
            0.6,
        )

        self.assertEqual(score, 0.7)
        self.assertEqual(record["hstree_score"], 0.7)
        self.assertEqual(record["hstree_status"], "anomaly")
        self.assertTrue(record["hstree_anomaly"])
        self.assertEqual(record["hstree_threshold"], 0.6)
        self.assertEqual(record["hstree_variant"], "paper")

    def test_strip_excluded_fields_can_preserve_group_metadata(self) -> None:
        record = strip_excluded_fields(
            {"Group": "A", "timestamp": "2026-01-01 00:00:00", "source_event_id": "event-1", "a": 1},
            {"Group", "timestamp", "source_event_id"},
            preserve_fields={"Group", "timestamp", "source_event_id"},
        )
        self.assertEqual(
            record,
            {"Group": "A", "timestamp": "2026-01-01 00:00:00", "source_event_id": "event-1", "a": 1},
        )

    def test_enrich_viz_record_without_moving_average(self) -> None:
        state = create_viz_state()
        record = enrich_viz_record({"hstree_score": 100.0}, state, 0)
        self.assertEqual(record["hstree_score_log10"], 2.0)
        self.assertEqual(record["hstree_score_log10_diff"], 0.0)

    def test_enrich_viz_record_with_moving_average(self) -> None:
        state = create_viz_state()
        enrich_viz_record({"hstree_score": 4.0}, state, 2)
        record = enrich_viz_record({"hstree_score": 16.0}, state, 2)

        self.assertEqual(record["hstree_score_ma_2"], 10.0)
        self.assertAlmostEqual(record["hstree_score_ma_2_log10"], 1.0)

    def test_enrich_viz_record_preserves_trigger_fields(self) -> None:
        state = create_viz_state()
        record = enrich_viz_record(
            {
                "hstree_score": 0.8,
                "hstree_status": "anomaly",
                "hstree_anomaly": True,
                "hstree_threshold": 0.5,
                "hstree_variant": "paper",
            },
            state,
            0,
        )

        self.assertEqual(record["hstree_status"], "anomaly")
        self.assertTrue(record["hstree_anomaly"])
        self.assertEqual(record["hstree_threshold"], 0.5)
        self.assertEqual(record["hstree_variant"], "paper")

    def test_extend_hstree_avro_schema_adds_trigger_fields_once(self) -> None:
        schema = {
            "type": "record",
            "name": "Component",
            "fields": [{"name": "temperature", "type": "double"}],
        }

        extended_once = extend_hstree_avro_schema(json.dumps(schema))
        extended_twice = extend_hstree_avro_schema(extended_once)
        fields = json.loads(extended_twice)["fields"]
        fields_by_name = {field["name"]: field for field in fields}

        self.assertEqual(fields_by_name["hstree_score"]["type"], ["null", "double"])
        self.assertEqual(fields_by_name["hstree_status"]["type"], ["null", "string"])
        self.assertEqual(fields_by_name["hstree_anomaly"]["type"], ["null", "boolean"])
        self.assertEqual(fields_by_name["hstree_threshold"]["type"], ["null", "double"])
        self.assertEqual(fields_by_name["hstree_variant"]["type"], ["null", "string"])
        self.assertTrue(all(fields_by_name[name]["default"] is None for name in fields_by_name if name.startswith("hstree_")))
        self.assertEqual([field["name"] for field in fields].count("hstree_score"), 1)

    def test_normalize_hstree_threshold_mode_accepts_ui_p99_alias(self) -> None:
        self.assertEqual(normalize_hstree_threshold_mode("p99"), "percentile")
        self.assertEqual(normalize_hstree_threshold_mode(" P99 "), "percentile")

    def test_normalize_hstree_threshold_mode_keeps_internal_modes(self) -> None:
        self.assertEqual(normalize_hstree_threshold_mode("percentile"), "percentile")
        self.assertEqual(normalize_hstree_threshold_mode("welford"), "welford")
        self.assertEqual(normalize_hstree_threshold_mode("welford-windowed"), "welford-windowed")

    def test_normalize_hstree_threshold_mode_defaults_to_percentile(self) -> None:
        self.assertEqual(normalize_hstree_threshold_mode(None), "percentile")
        self.assertEqual(normalize_hstree_threshold_mode("unknown"), "percentile")

    def test_welford_estimator_updates_mean_std_and_threshold(self) -> None:
        estimator = WelfordEstimator()
        for value in [1.0, 2.0, 3.0]:
            estimator.update(value)

        self.assertEqual(estimator.n, 3)
        self.assertAlmostEqual(estimator.mean, 2.0)
        self.assertAlmostEqual(estimator.std, 1.0)
        self.assertAlmostEqual(estimator.threshold(3.0), 5.0)

    def test_classifier_returns_warming_up_during_model_warmup(self) -> None:
        estimator = WelfordEstimator()

        status, anomaly, threshold = classify_hstree_score(
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

    def test_classifier_updates_estimator_before_min_samples(self) -> None:
        estimator = WelfordEstimator()

        status, anomaly, threshold = classify_hstree_score(
            0.2,
            estimator,
            is_warming_up=False,
            min_samples=2,
            k_sigma=3.0,
        )

        self.assertEqual(status, "warming_up")
        self.assertIsNone(anomaly)
        self.assertIsNone(threshold)
        self.assertEqual(estimator.n, 1)

    def test_classifier_detects_anomaly_and_rejects_contamination(self) -> None:
        estimator = WelfordEstimator()
        for value in [0.10, 0.20, 0.15]:
            estimator.update(value)

        status, anomaly, threshold = classify_hstree_score(
            0.30,
            estimator,
            is_warming_up=False,
            min_samples=3,
            k_sigma=1.0,
        )

        self.assertEqual(status, "anomaly")
        self.assertTrue(anomaly)
        self.assertAlmostEqual(threshold, 0.20)
        self.assertEqual(estimator.n, 3)

    def test_classifier_updates_estimator_for_normal_scores(self) -> None:
        estimator = WelfordEstimator()
        for value in [0.10, 0.20, 0.15]:
            estimator.update(value)

        status, anomaly, threshold = classify_hstree_score(
            0.16,
            estimator,
            is_warming_up=False,
            min_samples=3,
            k_sigma=3.0,
        )

        self.assertEqual(status, "normal")
        self.assertFalse(anomaly)
        self.assertIsNotNone(threshold)
        self.assertEqual(estimator.n, 4)

    def test_sliding_percentile_estimator_threshold_within_score_range(self) -> None:
        est = SlidingPercentileEstimator(window_size=10, percentile=99.0)
        scores = [-500_000, -400_000, -300_000, -200_000, -100_000,
                  -450_000, -350_000, -250_000, -150_000, -80_000]
        for s in scores:
            est.update(s)
        self.assertEqual(est.n, 10)
        thr = est.threshold()
        # P99 must be within the actual score range (always negative here)
        self.assertLessEqual(thr, max(scores))
        self.assertGreaterEqual(thr, min(scores))

    def test_sliding_percentile_estimator_window_evicts_oldest(self) -> None:
        est = SlidingPercentileEstimator(window_size=3, percentile=50.0)
        for s in [-900_000, -800_000, -700_000]:
            est.update(s)
        # Window is full: [-900k, -800k, -700k], P50 = -800k
        thr_before = est.threshold()
        est.update(-100_000)
        # Oldest (-900k) evicted: [-800k, -700k, -100k], P50 shifts up
        thr_after = est.threshold()
        self.assertGreater(thr_after, thr_before)

    def test_classify_hstree_score_percentile_warming_up_during_model_warmup(self) -> None:
        est = SlidingPercentileEstimator(window_size=10, percentile=99.0)
        status, anomaly, threshold = classify_hstree_score_percentile(
            -500_000.0, est, is_warming_up=True, min_samples=5,
        )
        self.assertEqual(status, "warming_up")
        self.assertIsNone(anomaly)
        self.assertIsNone(threshold)
        self.assertEqual(est.n, 0)

    def test_classify_hstree_score_percentile_accumulates_during_min_samples(self) -> None:
        est = SlidingPercentileEstimator(window_size=10, percentile=99.0)
        for _ in range(4):
            classify_hstree_score_percentile(
                -500_000.0, est, is_warming_up=False, min_samples=5,
            )
        self.assertEqual(est.n, 4)
        status, anomaly, threshold = classify_hstree_score_percentile(
            -500_000.0, est, is_warming_up=False, min_samples=5,
        )
        self.assertEqual(status, "warming_up")
        self.assertEqual(est.n, 5)

    def test_classify_hstree_score_percentile_detects_anomaly(self) -> None:
        est = SlidingPercentileEstimator(window_size=100, percentile=99.0)
        # Feed 100 normal scores (very negative)
        for _ in range(100):
            est.update(-500_000.0)
        # A score much closer to 0 is above P99 → anomaly
        status, anomaly, threshold = classify_hstree_score_percentile(
            -10_000.0, est, is_warming_up=False, min_samples=100,
        )
        self.assertEqual(status, "anomaly")
        self.assertTrue(anomaly)
        self.assertIsNotNone(threshold)
        # Anomaly must NOT contaminate the window
        self.assertEqual(est.n, 100)

    def test_classify_hstree_score_percentile_normal_score_updates_window(self) -> None:
        # Feed a spread of normal scores so P99 ≈ -102k; -490k is below that → normal.
        est = SlidingPercentileEstimator(window_size=100, percentile=99.0)
        import numpy as np
        for s in np.linspace(-600_000, -100_000, 100):
            est.update(float(s))
        status, anomaly, _ = classify_hstree_score_percentile(
            -490_000.0, est, is_warming_up=False, min_samples=100,
        )
        self.assertEqual(status, "normal")
        self.assertFalse(anomaly)
        self.assertEqual(est.n, 101)

    def test_hstree_model_exposes_warmup_state(self) -> None:
        class FakeHalfSpaceTrees:
            def __init__(self, *_args, **_kwargs):
                pass

            def score_partial(self, _x):
                return 0.42

            def fit_partial(self, _x):
                return self

        with patch(
            "runtime.detectors.hstree.models.hstree.create_hstree_model",
            FakeHalfSpaceTrees,
        ):
            model = HSTreeGlobalModel(num_features=1, initial_window_size=2)
            self.assertTrue(model.is_warming_up)
            self.assertEqual(model.samples_buffered, 0)

            self.assertEqual(model.partial_fit_score([1.0]), 0.0)
            self.assertTrue(model.is_warming_up)
            self.assertEqual(model.samples_buffered, 1)

            self.assertEqual(model.partial_fit_score([2.0]), 0.0)
            self.assertTrue(model.is_warming_up)
            self.assertEqual(model.samples_buffered, 0)

            self.assertEqual(model.partial_fit_score([3.0]), 0.42)
            self.assertFalse(model.is_warming_up)

    def test_warning_for_different_hstree_windows(self) -> None:
        with patch("builtins.print") as emit:
            _warn_if_hstree_windows_differ(128, 2048)
        emit.assert_called_once()
        self.assertIn("HSTREE_INITIAL_WINDOW_SIZE=128", emit.call_args.args[0])
        self.assertIn("HSTREE_WINDOW_SIZE=2048", emit.call_args.args[0])

    def test_no_warning_for_equal_hstree_windows(self) -> None:
        with patch("builtins.print") as emit:
            _warn_if_hstree_windows_differ(256, 256)
        emit.assert_not_called()
