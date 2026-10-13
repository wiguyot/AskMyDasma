"""Contiguous offset tracker tests for explainability V1 (V1-03).

The tracker is pure and in-memory: no Kafka, no threads, no I/O. The tested
contract is the commit convention (exclusive): the value returned (and exposed
as ``next_committable``) is always ``last_contiguous_successful_offset + 1``.
A failure is a barrage: itself and every later offset never become committable.
Partitions are independent; the tracker never sees groups — offset 11 (group
A) and offset 12 (group B) may finish in any order without changing the rule.
"""

from __future__ import annotations

import unittest

from dasma_kafka.explainability.offset_tracker import (
    ContiguousOffsetTracker,
    OffsetTrackerError,
)


class ContiguousOffsetTrackerCommitTest(unittest.TestCase):
    def test_in_order_successes_return_exclusive_commit_values(self) -> None:
        tracker = ContiguousOffsetTracker()
        for offset in (10, 11, 12):
            tracker.register_received(0, offset)

        self.assertEqual(tracker.record_outcome(0, 10, True), 11)
        self.assertEqual(tracker.record_outcome(0, 11, True), 12)
        self.assertEqual(tracker.record_outcome(0, 12, True), 13)

        snapshot = tracker.snapshot()[0]
        self.assertEqual(snapshot["first_offset"], 10)
        self.assertEqual(snapshot["next_committable"], 13)
        self.assertEqual(snapshot["pending"], [])
        self.assertEqual(snapshot["succeeded_blocked"], [])
        self.assertEqual(snapshot["failed"], [])

    def test_out_of_order_success_is_remembered_without_commit(self) -> None:
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 11)
        tracker.register_received(0, 12)

        # Offset 12 succeeds first: no commit (11 is a hole).
        self.assertIsNone(tracker.record_outcome(0, 12, True))
        snapshot = tracker.snapshot()[0]
        self.assertIsNone(snapshot["next_committable"])
        self.assertEqual(snapshot["succeeded_blocked"], [12])
        self.assertEqual(snapshot["pending"], [11])

        # When 11 succeeds, the commit value jumps to 13 at once.
        self.assertEqual(tracker.record_outcome(0, 11, True), 13)
        self.assertEqual(tracker.snapshot()[0]["next_committable"], 13)

    def test_groups_a_and_b_share_a_partition_without_changing_the_commit_rule(self) -> None:
        # The tracker never receives the Group: 11/A then 12/B may terminate in
        # the reverse order; only (partition, offset) matters and the commit
        # rule stays the same.
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 11)  # group A
        tracker.register_received(0, 12)  # group B

        self.assertIsNone(tracker.record_outcome(0, 12, True))  # B finishes first
        self.assertEqual(tracker.record_outcome(0, 11, True), 13)  # A unlocks both

    def test_first_offset_does_not_have_to_be_zero(self) -> None:
        tracker = ContiguousOffsetTracker()
        for offset in (2048, 2049):
            tracker.register_received(3, offset)
        self.assertEqual(tracker.record_outcome(3, 2048, True), 2049)
        self.assertEqual(tracker.record_outcome(3, 2049, True), 2050)


class ContiguousOffsetTrackerPartitionTest(unittest.TestCase):
    def test_partitions_are_independent(self) -> None:
        tracker = ContiguousOffsetTracker()
        for offset in (5, 6, 7):
            tracker.register_received(0, offset)
            tracker.register_received(1, offset)

        # Partition 0 is barraged at offset 5.
        tracker.record_outcome(0, 5, False)
        self.assertIsNone(tracker.record_outcome(0, 6, True))
        self.assertIsNone(tracker.record_outcome(0, 7, True))

        # Partition 1 keeps producing its own commit values.
        self.assertEqual(tracker.record_outcome(1, 5, True), 6)
        self.assertEqual(tracker.record_outcome(1, 6, True), 7)
        self.assertEqual(tracker.record_outcome(1, 7, True), 8)

        snapshot = tracker.snapshot()
        # Partition 0: barrage on its first offset, no success at all -> no
        # commit value (None), nothing skipped.
        self.assertIsNone(snapshot[0]["next_committable"])
        self.assertEqual(snapshot[0]["failed"], [5])
        self.assertEqual(snapshot[0]["succeeded_blocked"], [6, 7])
        self.assertEqual(snapshot[1]["next_committable"], 8)


class ContiguousOffsetTrackerFailureTest(unittest.TestCase):
    def test_failure_is_a_barrage_and_blocks_later_offsets(self) -> None:
        tracker = ContiguousOffsetTracker()
        for offset in (10, 11, 12):
            tracker.register_received(0, offset)

        self.assertEqual(tracker.record_outcome(0, 10, True), 11)
        self.assertIsNone(tracker.record_outcome(0, 11, False))
        self.assertIsNone(tracker.record_outcome(0, 12, True))

        snapshot = tracker.snapshot()[0]
        # No commit ever goes beyond 11: the value 11 restarts from the
        # failed offset itself (at-least-once, nothing skipped).
        self.assertEqual(snapshot["next_committable"], 11)
        self.assertEqual(snapshot["failed"], [11])
        self.assertEqual(snapshot["succeeded_blocked"], [12])
        self.assertEqual(snapshot["pending"], [])

    def test_failure_recorded_before_its_turn_caps_the_commit_value(self) -> None:
        tracker = ContiguousOffsetTracker()
        for offset in (10, 11, 12):
            tracker.register_received(0, offset)

        self.assertIsNone(tracker.record_outcome(0, 11, False))
        self.assertIsNone(tracker.record_outcome(0, 12, True))
        self.assertIsNone(tracker.snapshot()[0]["next_committable"])

        # Success 10 advances the cursor up to the barrage at 11.
        self.assertEqual(tracker.record_outcome(0, 10, True), 11)
        snapshot = tracker.snapshot()[0]
        self.assertEqual(snapshot["next_committable"], 11)
        self.assertEqual(snapshot["failed"], [11])
        self.assertEqual(snapshot["succeeded_blocked"], [12])

    def test_failure_on_the_first_offset_blocks_the_whole_partition(self) -> None:
        tracker = ContiguousOffsetTracker()
        for offset in (10, 11):
            tracker.register_received(0, offset)

        self.assertIsNone(tracker.record_outcome(0, 10, False))
        self.assertIsNone(tracker.record_outcome(0, 11, True))
        snapshot = tracker.snapshot()[0]
        self.assertIsNone(snapshot["next_committable"])
        self.assertEqual(snapshot["failed"], [10])
        self.assertEqual(snapshot["succeeded_blocked"], [11])


class ContiguousOffsetTrackerIntegrityTest(unittest.TestCase):
    def test_duplicate_registration_is_rejected(self) -> None:
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 10)
        with self.assertRaises(OffsetTrackerError):
            tracker.register_received(0, 10)

    def test_registration_gap_is_rejected(self) -> None:
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 10)
        with self.assertRaises(OffsetTrackerError):
            tracker.register_received(0, 12)

    def test_outcome_for_an_unregistered_offset_is_rejected(self) -> None:
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 10)
        with self.assertRaises(OffsetTrackerError):
            tracker.record_outcome(0, 11, True)

    def test_outcome_for_an_unknown_partition_is_rejected(self) -> None:
        tracker = ContiguousOffsetTracker()
        with self.assertRaises(OffsetTrackerError):
            tracker.record_outcome(3, 10, True)

    def test_duplicate_outcome_is_rejected(self) -> None:
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 10)
        tracker.record_outcome(0, 10, True)
        with self.assertRaises(OffsetTrackerError):
            tracker.record_outcome(0, 10, True)
        with self.assertRaises(OffsetTrackerError):
            tracker.record_outcome(0, 10, False)

    def test_success_after_a_failure_on_the_same_offset_is_a_duplicate(self) -> None:
        # Policy: exactly one terminal outcome per offset. A second outcome
        # after a terminal failure is a programming error, never a silent
        # override.
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 10)
        tracker.record_outcome(0, 10, False)
        with self.assertRaises(OffsetTrackerError):
            tracker.record_outcome(0, 10, True)

    def test_outcomes_after_a_failure_are_still_recorded(self) -> None:
        # Later offsets keep their outcomes for observability, but they never
        # become committable beyond the barrage.
        tracker = ContiguousOffsetTracker()
        for offset in (10, 11, 12):
            tracker.register_received(0, offset)
        tracker.record_outcome(0, 10, True)
        tracker.record_outcome(0, 11, False)
        tracker.record_outcome(0, 12, True)

        self.assertEqual(tracker.snapshot()[0]["succeeded_blocked"], [12])

    def test_negative_partition_or_offset_are_rejected_explicitly(self) -> None:
        tracker = ContiguousOffsetTracker()
        with self.assertRaises(OffsetTrackerError):
            tracker.register_received(-1, 10)
        with self.assertRaises(OffsetTrackerError):
            tracker.register_received(0, -5)
        with self.assertRaises(OffsetTrackerError):
            tracker.record_outcome(-1, 0, True)
        with self.assertRaises(OffsetTrackerError):
            tracker.record_outcome(0, -1, True)

    def test_non_int_values_are_rejected_explicitly(self) -> None:
        tracker = ContiguousOffsetTracker()
        with self.assertRaises(OffsetTrackerError):
            tracker.register_received("0", 10)
        with self.assertRaises(OffsetTrackerError):
            tracker.register_received(0, 10.0)
        with self.assertRaises(OffsetTrackerError):
            tracker.register_received(0, True)
        with self.assertRaises(OffsetTrackerError):
            tracker.record_outcome(0, 10, "yes")


class ContiguousOffsetTrackerOffByOneTest(unittest.TestCase):
    def test_commit_value_is_always_last_contiguous_success_plus_one(self) -> None:
        tracker = ContiguousOffsetTracker()
        for offset in (10, 11, 12, 13):
            tracker.register_received(0, offset)

        tracker.record_outcome(0, 10, True)
        self.assertEqual(tracker.snapshot()[0]["next_committable"], 11)  # 10 + 1
        tracker.record_outcome(0, 11, True)
        self.assertEqual(tracker.snapshot()[0]["next_committable"], 12)  # 11 + 1
        tracker.record_outcome(0, 12, True)
        self.assertEqual(tracker.snapshot()[0]["next_committable"], 13)  # 12 + 1
        tracker.record_outcome(0, 13, True)
        self.assertEqual(tracker.snapshot()[0]["next_committable"], 14)  # 13 + 1

    def test_commit_value_never_exposes_an_unprocessed_offset(self) -> None:
        tracker = ContiguousOffsetTracker()
        tracker.register_received(0, 10)
        # Nothing processed yet: no commit value at all.
        self.assertIsNone(tracker.snapshot()[0]["next_committable"])


if __name__ == "__main__":
    unittest.main()
