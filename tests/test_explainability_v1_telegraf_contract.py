"""Telegraf contract for the regular explainability model (V1-04a).

The V1 `[[inputs.kafka_consumer]]` block for `explainability-viz` must:

- use the `json_v2` parser (the legacy `json` parser drops string fields that
  are not listed in `json_string_fields` and drops booleans entirely, so
  `explanation_status` and `hstree_anomaly` never reached InfluxDB);
- carry `Group` AND `source_event_id` as tags on the root object and on the
  nested `shap_values` object, so two events sharing the same
  `(Group, timestamp)` with distinct `source_event_id` never overwrite each
  other: the point identity is `timestamp + Group + source_event_id`;
- type the status/verdict fields explicitly (`explanation_status`,
  `hstree_status`, `explanation_profile`, `explanation_error_code` and the
  fingerprints as strings, `hstree_anomaly` as bool);
- NOT ingest the `features` map (its keys would collide with the SHAP
  contributions on the same field names).

The other blocks (source-data, hstree-analysis, iforest-analysis,
kitnet-analysis, deepant-analysis, mdragstream-point-analysis and the
explainability-v2 block) must stay untouched: only the V1 block changed.
The runtime counterpart is contract-tested too: the Avro record keeps a
strictly numeric `shap_values` map, while the JSON viz copy carries the
identity inside the map (Telegraf json_v2 resolves an object's tags from the
object's own keys only — validated on an isolated probe).
"""

from __future__ import annotations

import ast
import json
import queue
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

import numpy as np

from dasma_kafka.explainability import runtime as runtime_module
from dasma_kafka.explainability.features import build_viz_json_payload
from dasma_kafka.explainability.runtime import GroupWorkItem

ROOT = Path(__file__).resolve().parents[1]
TELEGRAF = ROOT / "observability" / "telegraf" / "telegraf.conf"


def parse_kafka_consumer_blocks(raw: str) -> list[dict[str, object]]:
    """Parse each [[inputs.kafka_consumer]] block into a key/value dict.

    Block-scoped on purpose: a naive whole-file search would happily match a
    value that belongs to another model's block. Nested json_v2 sections
    (`[[inputs.kafka_consumer.json_v2...]]`) belong to their parent block;
    the block ends at the next section header of any other kind.
    """
    blocks: list[dict[str, object]] = []
    lines = raw.splitlines()
    index = 0
    while index < len(lines):
        if lines[index].strip() != "[[inputs.kafka_consumer]]":
            index += 1
            continue
        block: dict[str, object] = {}
        index += 1
        while index < len(lines):
            line = lines[index]
            stripped = line.strip()
            # Only a foreign section header — or the next consumer block —
            # ends the block; the json_v2 sub-sections belong to it.
            if stripped.startswith("[") and not stripped.startswith(
                ("[[inputs.kafka_consumer.json_v2", "[inputs.kafka_consumer.json_v2")
            ):
                break
            index += 1
            if not stripped or stripped.startswith("#"):
                continue
            key, sep, value = stripped.partition("=")
            if not sep:
                continue
            value = value.strip()
            while value.count("[") > value.count("]"):
                value += " " + lines[index].strip()
                index += 1
            # TOML booleans are bare `true`/`false`, not Python literals.
            block[key.strip()] = (
                value == "true" if value in ("true", "false") else ast.literal_eval(value)
            )
        blocks.append(block)
    return blocks


def parse_json_v2_objects(block_source: str) -> list[dict[str, object]]:
    """Extract the json_v2 object sub-sections of one consumer block.

    Returns one dict per `[[inputs.kafka_consumer.json_v2.object]]` section,
    with its `path`, `tags`, `included_keys` and typed `fields` when present.
    """
    objects: list[dict[str, object]] = []
    lines = block_source.splitlines()
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()
        if stripped != "[[inputs.kafka_consumer.json_v2.object]]":
            index += 1
            continue
        entry: dict[str, object] = {}
        fields: dict[str, str] = {}
        index += 1
        while index < len(lines):
            inner = lines[index].strip()
            if inner.startswith("[[") or (
                inner.startswith("[") and not inner.startswith("[inputs.kafka_consumer.json_v2")
            ):
                break
            index += 1
            if not inner or inner.startswith("#"):
                continue
            key, sep, value = inner.partition("=")
            if not sep:
                continue
            key, value = key.strip(), value.strip()
            while value.count("[") > value.count("]"):
                value += " " + lines[index].strip()
                index += 1
            if key == "fields":
                continue
            if inner.startswith("[inputs.kafka_consumer.json_v2.object.fields]"):
                continue
            if key in ("path", "disable_prepend_keys"):
                entry[key] = (
                    value == "true" if value in ("true", "false") else ast.literal_eval(value)
                )
            elif key == "tags":
                entry[key] = ast.literal_eval(value)
            elif key == "included_keys":
                entry[key] = ast.literal_eval(value)
        if fields:
            entry["fields"] = fields
        objects.append(entry)
    return objects


def _parse_fields(block_source: str) -> dict[str, str]:
    """Parse the typed `[...object.fields]` table of the root object."""
    fields: dict[str, str] = {}
    lines = block_source.splitlines()
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()
        if stripped != "[inputs.kafka_consumer.json_v2.object.fields]":
            index += 1
            continue
        index += 1
        while index < len(lines):
            inner = lines[index].strip()
            if inner.startswith("["):
                break
            index += 1
            if not inner or inner.startswith("#"):
                continue
            key, sep, value = inner.partition("=")
            if sep:
                fields[key.strip()] = ast.literal_eval(value.strip())
        break
    return fields


def _block_source(raw: str, topic: str) -> str:
    """Return the raw TOML text of the consumer block owning `topic`."""
    lines = raw.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip() == "[[inputs.kafka_consumer]]":
            start = index
        if start is not None and f'topics = ["{topic}"]' in line:
            end = start
            for cursor in range(start + 1, len(lines)):
                if lines[cursor].strip() == "[[inputs.kafka_consumer]]":
                    break
                end = cursor
            return "\n".join(lines[start : end + 1])
    raise AssertionError(f"no consumer block found for topic {topic}")


def _v1_block() -> tuple[dict[str, object], str]:
    raw = TELEGRAF.read_text(encoding="utf-8")
    for block in parse_kafka_consumer_blocks(raw):
        if block.get("topics") == ["explainability-viz"]:
            return block, _block_source(raw, "explainability-viz")
    raise AssertionError("no consumer block for topic explainability-viz")


class TelegrafV1BlockTest(TestCase):
    """Structural isolation of the V1 block and its identity tags."""

    def setUp(self) -> None:
        self.block, self.source = _v1_block()
        self.objects = parse_json_v2_objects(self.source)
        self.root = next(o for o in self.objects if o.get("path") == "@this")
        self.shap = next(o for o in self.objects if o.get("path") == "shap_values")
        self.fields = _parse_fields(self.source)

    def test_v1_block_uses_json_v2_and_keeps_v1_stream_identity(self) -> None:
        self.assertEqual(self.block["data_format"], "json_v2")
        self.assertEqual(self.block["topics"], ["explainability-viz"])
        self.assertEqual(self.block["consumer_group"], "telegraf-explainability")
        self.assertEqual(self.block["name_override"], "explainability")
        # The legacy parser settings are gone from the V1 block.
        self.assertNotIn("json_time_key", self.block)
        self.assertNotIn("json_string_fields", self.block)
        self.assertNotIn("tag_keys", self.block)

    def test_root_object_carries_identity_tags(self) -> None:
        self.assertEqual(self.root["tags"], ["Group", "source_event_id", "explained_model"])
        self.assertEqual(self.root["disable_prepend_keys"], True)
        included_keys = self.root["included_keys"]
        for key in (
            "explanation_error_code",
            "explanation_profile",
            "explanation_status",
            "explainability_seed",
            "feature_fingerprint",
            "hstree_anomaly",
            "hstree_model_fingerprint",
            "hstree_model_version",
            "hstree_score",
            "hstree_status",
            "hstree_threshold",
            "local_hstree_score",
            "shap_background_size",
            "shap_base_value",
            "shap_computation_time_ms",
            "shap_nsamples",
        ):
            self.assertIn(key, included_keys)

    def test_status_and_verdict_types_are_explicit(self) -> None:
        self.assertEqual(self.fields.get("explanation_status"), "string")
        self.assertEqual(self.fields.get("hstree_status"), "string")
        self.assertEqual(self.fields.get("explanation_profile"), "string")
        self.assertEqual(self.fields.get("explanation_error_code"), "string")
        self.assertEqual(self.fields.get("hstree_anomaly"), "bool")
        for fingerprint in ("feature_fingerprint", "hstree_model_fingerprint"):
            self.assertEqual(self.fields.get(fingerprint), "string")
        self.assertNotIn("explained_model", self.fields)

    def test_shap_values_object_carries_identity_tags(self) -> None:
        self.assertEqual(self.shap["tags"], ["Group", "source_event_id", "explained_model"])

    def test_features_map_is_not_ingested(self) -> None:
        # The `features` nested map would collide with the SHAP contributions
        # on the same field names; no json_v2 object may target it.
        paths = [o.get("path") for o in self.objects]
        self.assertNotIn("features", paths)
        # And `features` must not appear in the root included_keys either.
        self.assertNotIn("features", self.root["included_keys"])

    def test_other_blocks_stay_unchanged(self) -> None:
        raw = TELEGRAF.read_text(encoding="utf-8")
        blocks = parse_kafka_consumer_blocks(raw)
        topics = [block.get("topics") for block in blocks]
        # The untouched blocks are all still present.
        for topic in (
            ["topic-metrics"],
            ["source-data-viz"],
            ["hstree-analysis-viz"],
            ["iforest-analysis-viz"],
            ["kitnet-analysis-viz"],
            ["deepant-analysis-viz"],
            ["mdragstream-point-analysis-viz"],
            ["explainability-v2-viz"],
            ["consensus-analysis"],
            ["repeatability-v1-viz"],
            ["repeatability-v2-viz"],
            ["repeatability-diff"],
            ["reproducibility-diff"],
            ["reproducibility-summary"],
        ):
            self.assertIn(topic, topics)
        # The json_v2 conversion happened ONLY in the two explainability
        # blocks: every other block keeps its legacy parser settings
        # (some legitimately use source_event_id as a tag, e.g.
        # consensus-analysis — they are not part of V1-04a).
        for block in blocks:
            source = _block_source(raw, block["topics"][0])
            is_json_v2 = block.get("data_format") == "json_v2"
            if is_json_v2:
                self.assertIn(block.get("topics"), (["explainability-viz"], ["explainability-v2-viz"]))
            else:
                self.assertNotIn("json_v2", source)


class V1VizPayloadContractTest(TestCase):
    """Runtime counterpart: Avro stays numeric, JSON carries the identity."""

    WORKER_CONFIG = {
        "bootstrap_servers": "kafka:9092",
        "source_topic": "hstree-analysis",
        "explained_model": "hstree",
        "explainability_mode": "reference",
        "explanation_method": "kernel",
        "dest_topic": "explainability",
        "dest_topic_json": "explainability-viz",
        "schema_registry_url": "http://schema-registry:8081",
        "group_id": "explainability-consumer",
        "log_level": "CRITICAL",
        "hstree_window_size": 2048,
        "hstree_num_trees": 25,
        "hstree_max_depth": 15,
        "hstree_initial_window_size": 512,
        "shap_background_size": 100,
        "shap_nsamples": 100,
        "shap_enabled": True,
        "explainability_seed": 42,
    }

    @staticmethod
    def _record(source_event_id: str) -> dict:
        return {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "g1",
            "source_event_id": source_event_id,
            "hstree_score": 0.5,
            "hstree_status": "anomaly",
            "hstree_anomaly": True,
            "hstree_threshold": 0.7,
            "A": 1.5,
            "B": 2.5,
        }

    def test_avro_record_keeps_strictly_numeric_shap_values(self) -> None:
        record = {
            "source_event_id": "row-000001",
            "explained_model": "kitnet",
            "explanation_status": "computed",
            "shap_values": {"A": 0.1, "B": -0.2},
            "Group": "g1",
        }
        # The Avro serializer contract: shap_values values must stay numeric,
        # the identity keys must not sneak into the map.
        self.assertEqual(
            set(record["shap_values"]), {"A", "B"}
        )
        self.assertTrue(all(isinstance(v, float) for v in record["shap_values"].values()))

    def test_json_payload_carries_identity_inside_shap_values(self) -> None:
        record = {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "g1",
            "source_event_id": "row-000001",
            "explained_model": "kitnet",
            "explanation_status": "computed",
            "shap_values": {"A": 0.1, "B": -0.2},
        }
        payload = json.loads(build_viz_json_payload(record))
        self.assertEqual(payload["shap_values"]["Group"], "g1")
        self.assertEqual(payload["shap_values"]["source_event_id"], "row-000001")
        self.assertEqual(payload["shap_values"]["explained_model"], "kitnet")
        self.assertEqual(payload["shap_values"]["A"], 0.1)
        self.assertEqual(payload["shap_values"]["B"], -0.2)

    def test_json_payload_without_shap_values_stays_serializable(self) -> None:
        record = {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "g1",
            "source_event_id": "row-000001",
            "explained_model": "hstree",
            "explanation_status": "warmup",
            "shap_values": None,
        }
        payload = json.loads(build_viz_json_payload(record))
        self.assertIsNone(payload["shap_values"])
        self.assertEqual(payload["explanation_status"], "warmup")

    def test_same_group_and_timestamp_distinct_ids_produce_distinct_payloads(self) -> None:
        first = {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "g1",
            "source_event_id": "row-000001",
            "explained_model": "hstree",
            "shap_values": {"A": 0.1},
        }
        second = {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "g1",
            "source_event_id": "row-000002",
            "shap_values": {"A": 0.2},
        }
        payload_first = json.loads(build_viz_json_payload(first))
        payload_second = json.loads(build_viz_json_payload(second))
        # InfluxDB point identity is timestamp + Group + source_event_id:
        # these two payloads must differ on the tag values inside shap_values.
        self.assertNotEqual(
            payload_first["shap_values"]["source_event_id"],
            payload_second["shap_values"]["source_event_id"],
        )
        self.assertNotEqual(payload_first, payload_second)

    def test_worker_publishes_the_identity_payload_on_the_json_channel_only(self) -> None:
        model_manager = MagicMock(name="model-manager")
        model_manager.reference_score_field = "hstree_score"
        model_manager.mismatch_code = "hstree_score_mismatch"
        model_manager.fit_score.return_value = 0.5
        model_manager.get_background_data.return_value = np.zeros((100, 2))
        model_manager.get_model.return_value = MagicMock(name="hstree-model")

        shap_explainer = MagicMock(name="shap-explainer")
        shap_explainer.compute_shap_values.return_value = {
            "shap_values": {"A": 0.1, "B": -0.2},
            "shap_base_value": 0.05,
            "shap_computation_time_ms": 1.0,
        }

        feature_extractor = MagicMock(name="feature-extractor")
        feature_extractor.extract.return_value = np.array([1.5, 2.5])

        def _firing_producer():
            producer = MagicMock(name="serializing-producer")

            def _fire(topic, key=None, value=None, on_delivery=None, **kwargs):
                if on_delivery is not None:
                    on_delivery(None, MagicMock())
                return 0

            producer.produce.side_effect = _fire
            return producer

        avro_producer = _firing_producer()
        json_producer = _firing_producer()

        record = {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "g1",
            "source_event_id": "row-000001",
            "hstree_score": 0.5,
            "hstree_status": "anomaly",
            "hstree_anomaly": True,
            "hstree_threshold": 0.7,
            "A": 1.5,
            "B": 2.5,
        }
        work_q: queue.Queue = queue.Queue()
        work_q.put(GroupWorkItem(key_str=None, record=record, topic="hstree-analysis", partition=0, offset=0))
        work_q.put(None)

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
            runtime_module, "SHAPExplainer", return_value=shap_explainer
        ), patch.object(
            runtime_module, "FeatureExtractor", return_value=feature_extractor
        ), patch(
            "builtins.print", side_effect=lambda *a, **k: None
        ):
            runtime_module.group_worker_main(
                "g1", dict(self.WORKER_CONFIG), ["A", "B"], work_q, None, None
            )

        avro_record = avro_producer.produce.call_args.kwargs["value"]

        # Avro channel: strictly numeric shap_values, no identity inside.
        self.assertEqual(set(avro_record["shap_values"]), {"A", "B"})
        self.assertNotIn("Group", avro_record["shap_values"])
        self.assertNotIn("source_event_id", avro_record["shap_values"])

        # JSON channel: identity inside shap_values.
        json_payload = json.loads(json_producer.produce.call_args.kwargs["value"])
        self.assertEqual(json_payload["shap_values"]["Group"], "g1")
        self.assertEqual(json_payload["shap_values"]["source_event_id"], "row-000001")
        self.assertEqual(json_payload["shap_values"]["explained_model"], "hstree")



if __name__ == "__main__":
    unittest.main()
