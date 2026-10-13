"""Unit tests for mdragpoint-analysis pure helpers."""

from __future__ import annotations

import json
from unittest import TestCase

from dasma_kafka.mdragpoint_analysis.helpers import (
    build_feature_vector,
    create_viz_state,
    ensure_mdragpoint_analysis_fields,
    ensure_mdragpoint_score,
    enrich_viz_record,
    infer_feature_names,
    resolve_group_key,
    strip_excluded_fields,
)
from runtime.detectors.mdragpoint.mdragstream_point_analysis import (
    MDRAGPOINT_ANALYSIS_SCHEMA_FIELDS,
    extend_mdragpoint_avro_schema,
    normalize_mdragpoint_threshold_mode,
    normalize_mdragpoint_verdict_mode,
)


class MDragPointAnalysisHelpersTest(TestCase):
    def test_resolve_group_key(self) -> None:
        self.assertEqual(resolve_group_key({"Group": "A"}), "A")
        self.assertEqual(resolve_group_key({"group": "B"}), "B")
        self.assertEqual(resolve_group_key({"x": 1}), "__NO_GROUP__")

    def test_infer_feature_names_excludes_meta_fields(self) -> None:
        record = {"timestamp": "x", "a": 1, "b": 2.5, "Group": "g", "c": "nope"}
        self.assertEqual(
            infer_feature_names(record, {"timestamp", "Group"}), ["a", "b"]
        )

    def test_build_feature_vector_uses_default_zero(self) -> None:
        record = {"a": 1}
        self.assertEqual(build_feature_vector(record, ["a", "b"]), [1.0, 0.0])

    def test_ensure_mdragpoint_score_with_none(self) -> None:
        record, score = ensure_mdragpoint_score({"a": 1}, None)
        self.assertEqual(score, 0.0)
        self.assertEqual(record["mdragpoint_score"], 0.0)
        self.assertEqual(record["mdragpoint_status"], "warming_up")
        self.assertIsNone(record["mdragpoint_anomaly"])
        self.assertIsNone(record["mdragpoint_threshold"])
        self.assertIsNone(record["mdragpoint_internal_r"])

    def test_ensure_mdragpoint_analysis_fields_full(self) -> None:
        record, score = ensure_mdragpoint_analysis_fields(
            {"a": 1}, 0.7, "anomaly", True, 0.6, 0.5
        )
        self.assertEqual(score, 0.7)
        self.assertEqual(record["mdragpoint_score"], 0.7)
        self.assertEqual(record["mdragpoint_status"], "anomaly")
        self.assertTrue(record["mdragpoint_anomaly"])
        self.assertEqual(record["mdragpoint_threshold"], 0.6)
        self.assertEqual(record["mdragpoint_internal_r"], 0.5)

    def test_strip_excluded_fields_can_preserve_group_metadata(self) -> None:
        record = strip_excluded_fields(
            {"Group": "A", "timestamp": "t", "source_event_id": "event-1", "a": 1},
            {"Group", "timestamp", "source_event_id"},
            preserve_fields={"Group", "timestamp", "source_event_id"},
        )
        self.assertEqual(record, {"Group": "A", "timestamp": "t", "source_event_id": "event-1", "a": 1})

    def test_enrich_viz_record_with_moving_average(self) -> None:
        state = create_viz_state()
        enrich_viz_record({"mdragpoint_score": 4.0}, state, 2)
        record = enrich_viz_record({"mdragpoint_score": 16.0}, state, 2)
        self.assertEqual(record["mdragpoint_score_ma_2"], 10.0)
        self.assertAlmostEqual(record["mdragpoint_score_ma_2_log10"], 1.0)

    def test_enrich_viz_record_preserves_trigger_fields(self) -> None:
        state = create_viz_state()
        record = enrich_viz_record(
            {
                "mdragpoint_score": 0.8,
                "mdragpoint_status": "anomaly",
                "mdragpoint_anomaly": True,
                "mdragpoint_threshold": 0.5,
                "mdragpoint_internal_r": 0.4,
            },
            state,
            0,
        )
        self.assertEqual(record["mdragpoint_status"], "anomaly")
        self.assertTrue(record["mdragpoint_anomaly"])
        self.assertEqual(record["mdragpoint_threshold"], 0.5)
        self.assertEqual(record["mdragpoint_internal_r"], 0.4)

    def test_extend_mdragpoint_avro_schema_adds_fields_idempotently(self) -> None:
        schema = {
            "type": "record",
            "name": "Component",
            "fields": [{"name": "temperature", "type": "double"}],
        }
        extended_once = extend_mdragpoint_avro_schema(json.dumps(schema))
        extended_twice = extend_mdragpoint_avro_schema(extended_once)

        fields = json.loads(extended_twice)["fields"]
        fields_by_name = {field["name"]: field for field in fields}

        self.assertEqual(fields_by_name["mdragpoint_score"]["type"], ["null", "double"])
        self.assertEqual(fields_by_name["mdragpoint_status"]["type"], ["null", "string"])
        self.assertEqual(
            fields_by_name["mdragpoint_anomaly"]["type"], ["null", "boolean"]
        )
        self.assertEqual(
            fields_by_name["mdragpoint_threshold"]["type"], ["null", "double"]
        )
        self.assertEqual(
            fields_by_name["mdragpoint_internal_r"]["type"], ["null", "double"]
        )
        self.assertEqual(
            [field["name"] for field in fields].count("mdragpoint_score"), 1
        )

    def test_mdragpoint_analysis_schema_fields_are_well_formed(self) -> None:
        names = {name for name, _ in MDRAGPOINT_ANALYSIS_SCHEMA_FIELDS}
        self.assertEqual(
            names,
            {
                "mdragpoint_score",
                "mdragpoint_status",
                "mdragpoint_anomaly",
                "mdragpoint_threshold",
                "mdragpoint_internal_r",
            },
        )
        for _, avro_type in MDRAGPOINT_ANALYSIS_SCHEMA_FIELDS:
            self.assertEqual(avro_type[0], "null")

    def test_normalize_mdragpoint_threshold_mode_accepts_ui_p99_alias(self) -> None:
        self.assertEqual(normalize_mdragpoint_threshold_mode("p99"), "percentile")
        self.assertEqual(normalize_mdragpoint_threshold_mode(" P99 "), "percentile")

    def test_normalize_mdragpoint_threshold_mode_keeps_internal_modes(self) -> None:
        self.assertEqual(normalize_mdragpoint_threshold_mode("percentile"), "percentile")
        self.assertEqual(normalize_mdragpoint_threshold_mode("welford"), "welford")
        self.assertEqual(
            normalize_mdragpoint_threshold_mode("welford-windowed"), "welford-windowed"
        )

    def test_normalize_mdragpoint_threshold_mode_defaults_to_percentile(self) -> None:
        self.assertEqual(normalize_mdragpoint_threshold_mode(None), "percentile")
        self.assertEqual(normalize_mdragpoint_threshold_mode("unknown"), "percentile")

    def test_normalize_mdragpoint_verdict_mode_defaults_to_internal(self) -> None:
        self.assertEqual(normalize_mdragpoint_verdict_mode(None), "internal")
        self.assertEqual(normalize_mdragpoint_verdict_mode("unknown"), "internal")
        self.assertEqual(normalize_mdragpoint_verdict_mode("native"), "internal")
        self.assertEqual(normalize_mdragpoint_verdict_mode("external"), "external")
