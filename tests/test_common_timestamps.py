from unittest import TestCase

from dasma_kafka.common.timestamps import resolve_timestamp_key, resolve_timestamp_value


class CommonTimestampsTest(TestCase):
    def test_resolve_timestamp_key_prefers_canonical_timestamp(self) -> None:
        self.assertEqual(resolve_timestamp_key({"timestamp": "a", "time": "b"}), "timestamp")

    def test_resolve_timestamp_key_uses_fallback_candidates(self) -> None:
        self.assertEqual(resolve_timestamp_key({"Timestamp": "2020-01-01"}), "Timestamp")
        self.assertEqual(resolve_timestamp_key({"LabviewTime": "2020-01-01"}), "LabviewTime")

    def test_resolve_timestamp_key_handles_missing_values(self) -> None:
        self.assertIsNone(resolve_timestamp_key({"timestamp": ""}))
        self.assertIsNone(resolve_timestamp_key({"x": 1}))
        self.assertIsNone(resolve_timestamp_key(None))

    def test_resolve_timestamp_value_returns_first_available(self) -> None:
        self.assertEqual(resolve_timestamp_value({"time": "11:00:00"}), "11:00:00")
        self.assertEqual(resolve_timestamp_value({"Timestamp": 123}), 123)
        self.assertIsNone(resolve_timestamp_value({"timestamp": None, "Timestamp": ""}))

