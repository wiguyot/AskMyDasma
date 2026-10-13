"""Telegraf contract for the isolated explainability-v2 model.

The v2 `[[inputs.kafka_consumer]]` block tags `Group`, `source_event_id`, and
`explained_model` on scalar and SHAP contribution points so distinct events
and models remain distinct in InfluxDB. The regular explainability block stays
aligned with the same contract.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from unittest import TestCase

from dasma_kafka.explainability_v2.features import build_viz_json_payload


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


def find_unique_block(blocks: list[dict[str, object]], name_override: str) -> dict[str, object]:
    matches = [block for block in blocks if block.get("name_override") == name_override]
    if len(matches) != 1:
        raise AssertionError(
            f"expected exactly one kafka_consumer block for {name_override!r}, "
            f"found {len(matches)}"
        )
    return matches[0]


def _extract_block_raw(raw: str, name_override: str) -> str:
    """Return the raw TOML text of the block whose name_override matches."""
    blocks = raw.split("[[inputs.kafka_consumer]]")
    for candidate in blocks[1:]:
        if f'name_override = "{name_override}"' in candidate:
            return candidate
    raise AssertionError(f"no raw block found for {name_override!r}")


class ExplainabilityV2TelegrafBlockTest(TestCase):
    def setUp(self) -> None:
        self.blocks = parse_kafka_consumer_blocks(TELEGRAF.read_text(encoding="utf-8"))

    def test_v2_block_is_isolated_by_all_three_markers(self) -> None:
        v2 = find_unique_block(self.blocks, "explainability-v2")
        self.assertEqual(v2["topics"], ["explainability-v2-viz"])
        self.assertEqual(v2["consumer_group"], "telegraf-explainability-v2")
        self.assertEqual(v2["name_override"], "explainability-v2")
        # Proof comes from the v2 block itself, never from another model's
        # block (e.g. consensus-analysis also tags source_event_id).
        for block in self.blocks:
            if block is v2:
                continue
            self.assertNotEqual(block.get("topics"), ["explainability-v2-viz"])
            self.assertNotEqual(
                block.get("consumer_group"), "telegraf-explainability-v2"
            )

    def test_v2_event_identity_is_preserved_through_json_v2(self) -> None:
        # OC-12R: the legacy json_string_fields/tag_keys are gone from the v2
        # block; the identity now lives in the json_v2 tags list.
        v2 = find_unique_block(self.blocks, "explainability-v2")
        self.assertNotIn("json_string_fields", v2)
        self.assertNotIn("tag_keys", v2)
        v2_raw = _extract_block_raw(TELEGRAF.read_text(encoding="utf-8"), "explainability-v2")
        self.assertIn('tags = ["Group", "source_event_id", "explained_model"]', v2_raw)
        self.assertIn('timestamp_path = "timestamp"', v2_raw)


class ExplainabilityV2TelegrafJsonV2ContractTest(TestCase):
    """OC-12R: the v2 block must store real field types in InfluxDB.

    The legacy `json` parser (telegraf 1.30) drops string fields that are not
    listed in json_string_fields AND drops booleans entirely, so
    `explanation_status` and `hstree_anomaly` never reached InfluxDB. The v2
    block must use `json_v2` with explicit types.
    """

    def setUp(self) -> None:
        raw = TELEGRAF.read_text(encoding="utf-8")
        self.blocks = parse_kafka_consumer_blocks(raw)
        self.v2 = find_unique_block(self.blocks, "explainability-v2")
        self.v2_raw = _extract_block_raw(raw, "explainability-v2")

    def test_v2_block_uses_json_v2_with_explicit_types(self) -> None:
        self.assertEqual(self.v2["data_format"], "json_v2")
        self.assertIn('explanation_status = "string"', self.v2_raw)
        self.assertIn('hstree_anomaly = "bool"', self.v2_raw)

    def test_v2_block_preserves_event_identity(self) -> None:
        # timestamp drives the InfluxDB time; Group and source_event_id are
        # tags so simultaneous events never collapse.
        self.assertIn('timestamp_path = "timestamp"', self.v2_raw)
        self.assertIn('tags = ["Group", "source_event_id", "explained_model"]', self.v2_raw)
        self.assertNotIn('explained_model = "string"', self.v2_raw)

    def test_v2_nested_shap_values_without_features_collision(self) -> None:
        # OC-12R empirical contract (telegraf 1.30.3): json_v2 stores nested
        # object keys WITHOUT the parent prefix, so ingesting both `features`
        # and `shap_values` would collide on the same bare `<feature>` field
        # names. Only `shap_values` is ingested; no panel reads `features_*`.
        # The `@this` object keeps disable_prepend_keys=true (validated live:
        # scalars stay unprefixed and the tags are preserved).
        self.assertIn('path = "shap_values"', self.v2_raw)
        self.assertNotIn('path = "features"', self.v2_raw)
        self.assertNotIn("features", self.v2.get("included_keys", []))
        # The disable_prepend_keys flag belongs to the @this object only
        # (validated live: scalars stay unprefixed, tags preserved).
        this_object = self.v2_raw.split('path = "shap_values"')[0]
        self.assertIn("disable_prepend_keys = true", this_object)

    def test_nested_shap_values_object_carries_event_identity(self) -> None:
        # json_v2 resolves an object block's tags ONLY from keys located
        # inside the object at that path (telegraf 1.30.3, validated on an
        # isolated probe). The runtime therefore publishes Group and
        # source_event_id inside the shap_values map of the JSON viz copy,
        # and this object block pulls them as tags: without them the
        # per-feature points carry no identity, collide across groups sharing
        # the same second (last write wins) and never match a Group filter.
        shap_object = self.v2_raw.split('path = "shap_values"')[1]
        self.assertIn('tags = ["Group", "source_event_id", "explained_model"]', shap_object)

    def test_kitnet_model_is_a_tag_for_scalar_and_shap_points(self) -> None:
        payload = {
            "timestamp": "2026-01-01 00:00:01",
            "Group": "g1",
            "source_event_id": "row-1",
            "explained_model": "kitnet",
            "kitnet_score": 0.25,
            "shap_values": {"feature_a": 0.1},
        }
        json_copy = json.loads(build_viz_json_payload(payload))
        self.assertEqual(json_copy["explained_model"], "kitnet")
        self.assertEqual(json_copy["shap_values"]["explained_model"], "kitnet")
        self.assertEqual(json_copy["shap_values"]["feature_a"], 0.1)
        self.assertIsInstance(json_copy["shap_values"]["feature_a"], float)

    def test_regular_block_keeps_the_legacy_parser(self) -> None:
        # V1-04a modernized the regular explainability block to json_v2 with
        # identity tags; every OTHER regular stream keeps the legacy parser.
        regular = find_unique_block(self.blocks, "source-data")
        self.assertEqual(regular["data_format"], "json")
        self.assertEqual(regular["json_string_fields"], ["timestamp"])
        self.assertEqual(regular["tag_keys"], ["Group"])
        v1 = find_unique_block(self.blocks, "explainability")
        self.assertEqual(v1["data_format"], "json_v2")

    def test_regular_explainability_block_is_unchanged(self) -> None:
        # V1-04a: the V1 stream identity markers (topic, consumer group,
        # measurement) stay untouched; the parser is now json_v2 with the
        # same identity tags as V2.
        regular = find_unique_block(self.blocks, "explainability")
        self.assertEqual(regular["topics"], ["explainability-viz"])
        self.assertEqual(regular["consumer_group"], "telegraf-explainability")
        self.assertEqual(regular["name_override"], "explainability")
        self.assertEqual(regular["data_format"], "json_v2")
