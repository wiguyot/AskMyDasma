import json
from unittest import TestCase

from dasma_kafka.kitnet_analysis.helpers import (
    ensure_kitnet_analysis_fields,
    ensure_kitnet_score,
    parse_exclude_fields,
    parse_grace_anomaly_detector,
    parse_grace_feature_mapping,
    resolve_group_key,
    select_features,
    strip_excluded_fields,
    to_float,
)
from runtime.detectors.kitnet.kitnet_analysis_pysad_cpu import (
    KitNetGroupModels,
    KitNetParams,
    extend_kitnet_avro_schema,
    normalize_kitnet_threshold_mode,
)


class KitNetAnalysisHelpersTest(TestCase):
    def test_parse_exclude_fields(self) -> None:
        self.assertEqual(parse_exclude_fields("a, b ,,c "), ["a", "b", "c"])

    def test_parse_grace_feature_mapping(self) -> None:
        self.assertIsNone(parse_grace_feature_mapping(None))
        self.assertIsNone(parse_grace_feature_mapping(""))
        self.assertIsNone(parse_grace_feature_mapping("None"))
        self.assertEqual(parse_grace_feature_mapping("12"), 12)
        self.assertIsNone(parse_grace_feature_mapping("bad"))

    def test_parse_grace_anomaly_detector(self) -> None:
        self.assertEqual(parse_grace_anomaly_detector(None, 5), 5)
        self.assertEqual(parse_grace_anomaly_detector("", 5), 5)
        self.assertEqual(parse_grace_anomaly_detector("12", 5), 12)

    def test_resolve_group_key(self) -> None:
        self.assertEqual(resolve_group_key({"Group": "A"}), "A")
        self.assertEqual(resolve_group_key({"group": "B"}), "B")
        self.assertEqual(resolve_group_key({"x": 1}), "__NO_GROUP__")

    def test_to_float(self) -> None:
        self.assertTrue(str(to_float(None)) == "nan")
        self.assertEqual(to_float("1.5"), 1.5)
        self.assertTrue(str(to_float("bad")) == "nan")

    def test_select_features(self) -> None:
        names, vector = select_features({"timestamp": "x", "a": 1, "b": "2"}, ["timestamp"])
        self.assertEqual(names, ["a", "b"])
        self.assertEqual(vector.tolist(), [1.0, 2.0])

    def test_ensure_kitnet_score(self) -> None:
        record, score = ensure_kitnet_score({"a": 1}, None)
        self.assertEqual(score, 0.0)
        self.assertEqual(record["kitnet_score"], 0.0)
        self.assertEqual(record["kitnet_status"], "warming_up")
        self.assertIsNone(record["kitnet_anomaly"])
        self.assertIsNone(record["kitnet_threshold"])

    def test_ensure_kitnet_analysis_fields(self) -> None:
        record, score = ensure_kitnet_analysis_fields(
            {"a": 1},
            0.7,
            "anomaly",
            True,
            0.6,
        )

        self.assertEqual(score, 0.7)
        self.assertEqual(record["kitnet_score"], 0.7)
        self.assertEqual(record["kitnet_status"], "anomaly")
        self.assertTrue(record["kitnet_anomaly"])
        self.assertEqual(record["kitnet_threshold"], 0.6)

    def test_extend_kitnet_avro_schema_adds_trigger_fields_once(self) -> None:
        schema = {
            "type": "record",
            "name": "Component",
            "fields": [{"name": "temperature", "type": "double"}],
        }

        extended_once = extend_kitnet_avro_schema(json.dumps(schema))
        extended_twice = extend_kitnet_avro_schema(extended_once)
        fields = json.loads(extended_twice)["fields"]
        fields_by_name = {field["name"]: field for field in fields}

        self.assertEqual(fields_by_name["kitnet_score"]["type"], ["null", "double"])
        self.assertEqual(fields_by_name["kitnet_status"]["type"], ["null", "string"])
        self.assertEqual(fields_by_name["kitnet_anomaly"]["type"], ["null", "boolean"])
        self.assertEqual(fields_by_name["kitnet_threshold"]["type"], ["null", "double"])
        self.assertTrue(
            all(
                fields_by_name[name]["default"] is None
                for name in fields_by_name
                if name.startswith("kitnet_")
            )
        )
        self.assertEqual([field["name"] for field in fields].count("kitnet_score"), 1)

    def test_normalize_kitnet_threshold_mode_accepts_percentile_aliases(self) -> None:
        self.assertEqual(normalize_kitnet_threshold_mode("p99"), "p99")
        self.assertEqual(normalize_kitnet_threshold_mode("percentile"), "p99")
        self.assertEqual(normalize_kitnet_threshold_mode(" Percentile "), "p99")

    def test_normalize_kitnet_threshold_mode_keeps_welford_modes(self) -> None:
        self.assertEqual(normalize_kitnet_threshold_mode("welford"), "welford")
        self.assertEqual(
            normalize_kitnet_threshold_mode("welford-windowed"), "welford-windowed"
        )

    def test_normalize_kitnet_threshold_mode_defaults_to_welford(self) -> None:
        self.assertEqual(normalize_kitnet_threshold_mode(None), "welford")
        self.assertEqual(normalize_kitnet_threshold_mode("unknown"), "welford")

    def test_kitnet_group_models_exposes_grace_warmup_state(self) -> None:
        models = KitNetGroupModels(
            KitNetParams(
                max_size_ae=10,
                grace_feature_mapping=2,
                grace_anomaly_detector=3,
                learning_rate=0.1,
                hidden_ratio=0.75,
            )
        )

        self.assertEqual(models.warmup_samples, 5)
        self.assertTrue(models.is_warming_up("A"))
        models.seen_by_group["A"] = 5
        self.assertTrue(models.is_warming_up("A"))
        models.seen_by_group["A"] = 6
        self.assertFalse(models.is_warming_up("A"))

    def test_strip_excluded_fields_can_preserve_group_metadata(self) -> None:
        record = strip_excluded_fields(
            {"Group": "A", "timestamp": "2026-01-01 00:00:00", "source_event_id": "event-1", "a": 1},
            ["Group", "timestamp", "source_event_id"],
            preserve_fields={"Group", "timestamp", "source_event_id"},
        )
        self.assertEqual(
            record,
            {"Group": "A", "timestamp": "2026-01-01 00:00:00", "source_event_id": "event-1", "a": 1},
        )
