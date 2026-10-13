import json
from unittest import TestCase
from unittest.mock import patch

from dasma_kafka.iforest_analysis.helpers import (
    build_feature_vector,
    ensure_iforest_analysis_fields,
    ensure_iforest_score,
    infer_feature_names,
    parse_iforest_contamination,
    parse_iforest_max_samples,
    resolve_group_key,
    strip_excluded_fields,
)
from runtime.detectors.iforest.iforest_analysis import (
    IForestGlobalModel,
    extend_iforest_avro_schema,
    normalize_iforest_threshold_mode,
)


class IForestAnalysisHelpersTest(TestCase):
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

    def test_metadata_is_excluded_even_without_configured_exclusions(self) -> None:
        record = {
            "State (0=OK 1=Warning 2=Error)": 2,
            "State__0_OK_1_Warning_2_Error_": 1,
            "BackgroundSubtracted(0/1)": 0,
            "BackgroundSubtracted_0_1_": True,
            "a": 1.5,
            "b": 2,
        }
        original = record.copy()
        for excluded in (set(), {"b"}, {"State (0=OK 1=Warning 2=Error)"}):
            with self.subTest(excluded=excluded):
                expected = ["a"] if "b" in excluded else ["a", "b"]
                self.assertEqual(infer_feature_names(record, excluded), expected)
        self.assertEqual(record, original)

    def test_ensure_iforest_score(self) -> None:
        record, score = ensure_iforest_score({"a": 1}, None)
        self.assertEqual(score, 0.0)
        self.assertEqual(record["iforest_score"], 0.0)
        self.assertEqual(record["iforest_status"], "warming_up")
        self.assertIsNone(record["iforest_anomaly"])
        self.assertIsNone(record["iforest_threshold"])

    def test_ensure_iforest_analysis_fields(self) -> None:
        record, score = ensure_iforest_analysis_fields(
            {"a": 1},
            0.7,
            "anomaly",
            True,
            0.6,
        )

        self.assertEqual(score, 0.7)
        self.assertEqual(record["iforest_score"], 0.7)
        self.assertEqual(record["iforest_status"], "anomaly")
        self.assertTrue(record["iforest_anomaly"])
        self.assertEqual(record["iforest_threshold"], 0.6)

    def test_extend_iforest_avro_schema_adds_trigger_fields_once(self) -> None:
        schema = {
            "type": "record",
            "name": "Component",
            "fields": [{"name": "temperature", "type": "double"}],
        }

        extended_once = extend_iforest_avro_schema(json.dumps(schema))
        extended_twice = extend_iforest_avro_schema(extended_once)
        fields = json.loads(extended_twice)["fields"]
        fields_by_name = {field["name"]: field for field in fields}

        self.assertEqual(fields_by_name["iforest_score"]["type"], ["null", "double"])
        self.assertEqual(fields_by_name["iforest_status"]["type"], ["null", "string"])
        self.assertEqual(fields_by_name["iforest_anomaly"]["type"], ["null", "boolean"])
        self.assertEqual(fields_by_name["iforest_threshold"]["type"], ["null", "double"])
        self.assertTrue(
            all(
                fields_by_name[name]["default"] is None
                for name in fields_by_name
                if name.startswith("iforest_")
            )
        )
        self.assertEqual([field["name"] for field in fields].count("iforest_score"), 1)

    def test_normalize_iforest_threshold_mode_accepts_percentile_aliases(self) -> None:
        self.assertEqual(normalize_iforest_threshold_mode("p99"), "p99")
        self.assertEqual(normalize_iforest_threshold_mode("percentile"), "p99")
        self.assertEqual(normalize_iforest_threshold_mode(" Percentile "), "p99")

    def test_normalize_iforest_threshold_mode_keeps_welford_modes(self) -> None:
        self.assertEqual(normalize_iforest_threshold_mode("welford"), "welford")
        self.assertEqual(
            normalize_iforest_threshold_mode("welford-windowed"), "welford-windowed"
        )

    def test_normalize_iforest_threshold_mode_defaults_to_native(self) -> None:
        self.assertEqual(normalize_iforest_threshold_mode(None), "native")
        self.assertEqual(normalize_iforest_threshold_mode("unknown"), "native")

    def test_iforest_model_exposes_warmup_state(self) -> None:
        class FakeIsolationForest:
            def __init__(self, **_kwargs):
                pass

            def fit(self, _x):
                return self

            def score_samples(self, _x):
                return [-0.42]

        with patch("dasma_kafka.iforest_analysis.model.IsolationForest", FakeIsolationForest):
            model = IForestGlobalModel(num_features=1, initial_window_size=2, max_samples=2)
            self.assertTrue(model.is_warming_up)
            self.assertEqual(model.samples_buffered, 0)

            self.assertEqual(model.partial_fit_score([1.0]), 0.0)
            self.assertTrue(model.is_warming_up)
            self.assertEqual(model.samples_buffered, 1)

            self.assertEqual(model.partial_fit_score([2.0]), 0.0)
            self.assertTrue(model.is_warming_up)
            self.assertEqual(model.partial_fit_score([3.0]), 0.42)
            self.assertFalse(model.is_warming_up)
            self.assertEqual(model.samples_buffered, 3)

    def test_strip_excluded_fields_can_preserve_group_metadata(self) -> None:
        record = strip_excluded_fields(
            {"group": "A", "timestamp": "2026-01-01 00:00:00", "source_event_id": "event-1", "a": 1},
            {"group", "timestamp", "source_event_id"},
            preserve_fields={"group", "timestamp", "source_event_id"},
        )
        self.assertEqual(
            record,
            {"group": "A", "timestamp": "2026-01-01 00:00:00", "source_event_id": "event-1", "a": 1},
        )

    def test_parse_iforest_max_samples(self) -> None:
        self.assertEqual(parse_iforest_max_samples("12"), 12)
        for value in ("auto", "0.5", "1", "bad"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_iforest_max_samples(value)

    def test_parse_iforest_contamination(self) -> None:
        self.assertEqual(parse_iforest_contamination("auto"), "auto")
        self.assertEqual(parse_iforest_contamination(" AUTO "), "auto")
        for value, expected in (("0.01", 0.01), ("0.1", 0.1), ("0.50", 0.5)):
            self.assertEqual(parse_iforest_contamination(value), expected)
        for value in ("0", "0.51", "1", "0.015", "nan", "inf", "bad"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_iforest_contamination(value)
