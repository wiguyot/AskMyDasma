from unittest import TestCase

from dasma_kafka.common.grouping import resolve_group_key


class CommonGroupingTest(TestCase):
    def test_resolve_group_key_prefers_group_field(self) -> None:
        self.assertEqual(resolve_group_key({"Group": "A"}), "A")
        self.assertEqual(resolve_group_key({"group": "B"}), "B")

    def test_resolve_group_key_uses_fallback_on_missing_values(self) -> None:
        self.assertEqual(resolve_group_key({"Group": ""}), "__NO_GROUP__")
        self.assertEqual(resolve_group_key({"x": 1}), "__NO_GROUP__")
        self.assertEqual(resolve_group_key(None), "__NO_GROUP__")

