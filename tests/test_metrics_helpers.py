import json
from unittest import TestCase

from dasma_kafka.metrics.helpers import format_snapshot_summary, parse_topics, snapshot_to_json


class MetricsHelpersTest(TestCase):
    def test_parse_topics_strips_empty_values(self) -> None:
        self.assertEqual(parse_topics("a, b ,,c "), ["a", "b", "c"])

    def test_format_snapshot_summary_ignores_timestamp(self) -> None:
        summary = format_snapshot_summary(
            {"timestamp": "2026-01-01T00:00:00Z", "b": 2, "a": 1},
            max_items=12,
        )
        self.assertEqual(summary, "a=1, b=2")

    def test_snapshot_to_json(self) -> None:
        payload = snapshot_to_json({"timestamp": "2026-01-01T00:00:00Z", "a": 1})
        self.assertEqual(json.loads(payload)["a"], 1)
