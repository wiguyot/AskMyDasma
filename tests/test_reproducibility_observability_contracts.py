"""Contracts for the reproducibility observability wiring (P2-7).

Static inspection of docker-compose.yml, telegraf.conf and the topic loader,
without any daemon: topics, buckets, manual service, Telegraf inputs/outputs
and the runtime lists that must NOT contain reproducibility-publish.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest import TestCase

from src.dasma_kafka.web.orchestration.campaign_orchestrator import _LAUNCH_IMAGES
from scripts.web.campaign_config import ALL_RUNTIME_COMPOSE_SERVICES, RESET_SCHEMA_SUBJECTS, RESET_TOPICS

ROOT = Path(__file__).resolve().parents[1]


def _compose_text() -> str:
    return (ROOT / "docker-compose.yml").read_text(encoding="utf-8")


def _compose_service_section(service_name: str) -> str:
    compose = _compose_text()
    marker = f"  {service_name}:"
    section = compose.split(marker, 1)[1]
    next_service = re.search(r"\n  [A-Za-z0-9_-]+:", section)
    return section if next_service is None else section[: next_service.start()]


class TopicContractTest(TestCase):
    """Topics 24/25 declared after TOPIC_NAME23 and created by init-topic only."""

    def test_topics_24_and_25_follow_23_in_compose(self) -> None:
        compose = _compose_text()
        position_23 = compose.index('TOPIC_NAME23: "repeatability-diff"')
        position_24 = compose.index('TOPIC_NAME24: "reproducibility-diff"')
        position_25 = compose.index('TOPIC_NAME25: "reproducibility-summary"')
        self.assertLess(position_23, position_24)
        self.assertLess(position_24, position_25)
        self.assertIn('TOPIC_PARTITIONS24: "1"', compose)
        self.assertIn('TOPIC_RF24: "3"', compose)
        self.assertIn('TOPIC_PARTITIONS25: "1"', compose)
        self.assertIn('TOPIC_RF25: "3"', compose)

    def test_loader_creates_topics_24_and_25(self) -> None:
        from src.dasma_kafka.init_topic.helpers import load_topics_from_env

        names = {topic.name for topic in load_topics_from_env()}
        self.assertIn("reproducibility-diff", names)
        self.assertIn("reproducibility-summary", names)
        self.assertIn("repeatability-diff", names)

    def test_init_topic_is_the_only_creator(self) -> None:
        compose = _compose_text()
        for topic in ("reproducibility-diff", "reproducibility-summary"):
            for service in ("producer", "consensus-analysis", "telegraf"):
                section = _compose_service_section(service)
                self.assertNotIn(topic, section, service)


class BucketContractTest(TestCase):
    """Two no-retention buckets, identical to their topic and measurement."""

    def test_buckets_ensured_once_with_no_retention(self) -> None:
        compose = _compose_text()
        self.assertEqual(compose.count('ensure_no_retention "reproducibility-diff"'), 1)
        self.assertEqual(compose.count('ensure_no_retention "reproducibility-summary"'), 1)
        # They follow the repeatability buckets.
        position_repeat = compose.index('ensure_no_retention "repeatability-diff"')
        position_diff = compose.index('ensure_no_retention "reproducibility-diff"')
        position_summary = compose.index('ensure_no_retention "reproducibility-summary"')
        self.assertLess(position_repeat, position_diff)
        self.assertLess(position_diff, position_summary)

    def test_buckets_absent_from_reset_lists(self) -> None:
        for topic in RESET_TOPICS:
            self.assertNotIn("reproducibility", topic)
        for subject in RESET_SCHEMA_SUBJECTS:
            self.assertNotIn("reproducibility", subject)
        reset_script = (ROOT / "scripts" / "web" / "reset_campaign.sh").read_text(encoding="utf-8")
        self.assertNotIn("reproducibility", reset_script)


class ManualServiceContractTest(TestCase):
    """The manual reproducibility-publish service, never automatic."""

    def setUp(self) -> None:
        self.section = _compose_service_section("reproducibility-publish")
        export_section = _compose_service_section("reproducibility-export")
        compose = _compose_text()
        self.assertLess(
            compose.index("  reproducibility-export:"),
            compose.index("  reproducibility-publish:"),
        )
        self.export_section = export_section

    def test_identity_and_runtime(self) -> None:
        self.assertIn("image: dasma-kafka-kafka-tools:0.1.0", self.section)
        self.assertIn("container_name: reproducibility-publish", self.section)
        self.assertIn(
            'entrypoint: ["python", "/app/validation/reproducibility/publish_comparison_to_kafka.py"]',
            self.section,
        )
        self.assertIn('restart: "no"', self.section)
        self.assertIn("networks: [kafka-net]", self.section)
        self.assertIn("init-topic:", self.section)
        self.assertIn("service_completed_successfully", self.section)

    def test_read_only_mounts_only(self) -> None:
        self.assertIn(
            "- ./validation/reproducibility:/app/validation/reproducibility:ro",
            self.section,
        )
        self.assertIn("- ./data/reproducibility:/app/data/reproducibility:ro", self.section)
        # No writable bind anywhere in the service.
        for line in self.section.splitlines():
            stripped = line.strip()
            if stripped.startswith("- .") and stripped.endswith(":ro"):
                continue
            if stripped.startswith("- ./") and ":ro" not in stripped:
                self.fail(f"non read-only mount in reproducibility-publish: {stripped}")

    def test_never_starts_automatically(self) -> None:
        self.assertNotIn("DASMA_ENABLED", self.section)
        # Not part of any runtime or launch list.
        self.assertNotIn("reproducibility-publish", ALL_RUNTIME_COMPOSE_SERVICES)
        self.assertNotIn("reproducibility-publish", _LAUNCH_IMAGES)
        for script in ("launch_campaign.sh", "stop_campaign.sh", "reset_campaign.sh"):
            text = (ROOT / "scripts" / "web" / script).read_text(encoding="utf-8")
            self.assertNotIn("reproducibility-publish", text, script)

    def test_environment_is_minimal(self) -> None:
        self.assertIn('BOOTSTRAP_SERVERS: "kafka1:9092,kafka2:9092,kafka3:9092"', self.section)
        self.assertIn('REPRODUCIBILITY_DIFF_TOPIC: "reproducibility-diff"', self.section)
        self.assertIn('REPRODUCIBILITY_SUMMARY_TOPIC: "reproducibility-summary"', self.section)
        for forbidden in ("REPRODUCIBILITY_MODEL_ID", "REPRODUCIBILITY_CAMPAIGN_ID", "OUTPUT_ROOT"):
            self.assertNotIn(forbidden, self.section)


class TelegrafContractTest(TestCase):
    """Exactly two new inputs and two new outputs, no repeatability change."""

    def setUp(self) -> None:
        self.text = (ROOT / "observability" / "telegraf" / "telegraf.conf").read_text(encoding="utf-8")

    def _blocks(self, kind: str) -> list[str]:
        header_pattern = re.compile(r"^\[\[[^\]]+\]\]", re.MULTILINE)
        positions = [match.start() for match in header_pattern.finditer(self.text)]
        blocks = []
        for index, start in enumerate(positions):
            end = positions[index + 1] if index + 1 < len(positions) else len(self.text)
            block = self.text[start:end]
            if block.startswith(f"[[{kind}]]"):
                blocks.append(block)
        return blocks

    def test_exactly_two_reproducibility_inputs(self) -> None:
        inputs = [
            block for block in self._blocks("inputs.kafka_consumer")
            if "reproducibility" in block
        ]
        self.assertEqual(len(inputs), 2)
        diff_block = next(b for b in inputs if 'topics = ["reproducibility-diff"]' in b)
        summary_block = next(b for b in inputs if 'topics = ["reproducibility-summary"]' in b)
        for block, group, measurement in (
            (diff_block, "telegraf-reproducibility-diff", "reproducibility-diff"),
            (summary_block, "telegraf-reproducibility-summary", "reproducibility-summary"),
        ):
            self.assertIn(f'consumer_group = "{group}"', block)
            self.assertIn('offset = "oldest"', block)
            self.assertIn('data_format = "json"', block)
            self.assertIn('json_time_key = "timestamp"', block)
            self.assertIn('json_time_format = "2006-01-02 15:04:05"', block)
            self.assertIn(f'name_override = "{measurement}"', block)

    def test_diff_input_tags_and_string_fields(self) -> None:
        diff_block = next(
            block for block in self._blocks("inputs.kafka_consumer")
            if 'topics = ["reproducibility-diff"]' in block
        )
        self.assertIn(
            'tag_keys = ["campaign_id", "model", "Group", "source_event_id"]', diff_block
        )
        self.assertIn('"source_event_id"', diff_block)

    def test_summary_input_tags_and_string_fields(self) -> None:
        summary_block = next(
            block for block in self._blocks("inputs.kafka_consumer")
            if 'topics = ["reproducibility-summary"]' in block
        )
        self.assertIn('tag_keys = ["campaign_id", "model"]', summary_block)
        # The two source images are plain strings consumed by the dashboard.
        self.assertIn('"source_1_image"', summary_block)
        self.assertIn('"source_2_image"', summary_block)

    def test_exactly_two_reproducibility_outputs(self) -> None:
        outputs = [
            block for block in self._blocks("outputs.influxdb_v2")
            if "reproducibility" in block
        ]
        self.assertEqual(len(outputs), 2)
        for block, measurement in (
            (outputs[0], "reproducibility-diff"),
            (outputs[1], "reproducibility-summary"),
        ):
            self.assertIn(f'bucket = "{measurement}"', block)
            self.assertIn(f'namepass = ["{measurement}"]', block)
            self.assertIn('urls = ["http://influxdb:8086"]', block)

    def test_no_repeatability_block_changed(self) -> None:
        for measurement in ("repeatability-v1", "repeatability-v2", "repeatability-diff"):
            inputs = [
                block for block in self._blocks("inputs.kafka_consumer")
                if f'name_override = "{measurement}"' in block
            ]
            self.assertEqual(len(inputs), 1, measurement)
            outputs = [
                block for block in self._blocks("outputs.influxdb_v2")
                if f'namepass = ["{measurement}"]' in block
            ]
            self.assertEqual(len(outputs), 1, measurement)

    def test_no_boolean_fields_expected(self) -> None:
        # The publisher never emits JSON booleans; nothing to declare beyond
        # the documented string fields.
        diff_block = next(
            block for block in self._blocks("inputs.kafka_consumer")
            if 'topics = ["reproducibility-diff"]' in block
        )
        self.assertNotIn("anomaly", diff_block)
