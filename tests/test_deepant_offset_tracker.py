"""Unit tests for the DeepAnT per-partition commit watermark."""

from __future__ import annotations

import unittest

from src.dasma_kafka.deepant_analysis.offset_tracker import CommitOffsetTracker

T = "source-data"
P = 0


class CommitOffsetTrackerTest(unittest.TestCase):
    def test_in_order_completion_commits_each_step(self) -> None:
        tr = CommitOffsetTracker()
        for off in (0, 1, 2):
            tr.register(T, P, off)
            tr.mark_done(T, P, off)
        # frontier advanced to 3 (next to read) -> commit position 3
        self.assertEqual(tr.drain_commits(), [(T, P, 3)])

    def test_out_of_order_completion_waits_for_contiguity(self) -> None:
        tr = CommitOffsetTracker()
        for off in (0, 1, 2):
            tr.register(T, P, off)

        # Worker for offset 2 finishes before offset 1.
        tr.mark_done(T, P, 0)
        tr.mark_done(T, P, 2)
        # Only offset 0 is contiguous -> commit position 1, offset 2 held.
        self.assertEqual(tr.drain_commits(), [(T, P, 1)])
        self.assertEqual(tr.pending_count(), 1)

        # Offset 1 completes -> 1 and 2 absorbed -> commit position 3.
        tr.mark_done(T, P, 1)
        self.assertEqual(tr.drain_commits(), [(T, P, 3)])
        self.assertEqual(tr.pending_count(), 0)

    def test_worker_failure_stalls_commit_past_offset(self) -> None:
        tr = CommitOffsetTracker()
        for off in (0, 1, 2, 3):
            tr.register(T, P, off)

        tr.mark_done(T, P, 0)
        # Offset 1 is a worker failure: never marked done.
        tr.mark_done(T, P, 2)
        tr.mark_done(T, P, 3)

        # Watermark cannot pass the failed offset 1 -> commit position 1 only.
        self.assertEqual(tr.drain_commits(), [(T, P, 1)])
        # Offsets 2 and 3 are held pending behind the failure.
        self.assertEqual(tr.pending_count(), 2)

    def test_skipped_offset_advances_like_done(self) -> None:
        tr = CommitOffsetTracker()
        for off in (0, 1, 2):
            tr.register(T, P, off)
        tr.mark_done(T, P, 0)
        tr.mark_done(T, P, 1)  # e.g. deserialization error skip
        tr.mark_done(T, P, 2)
        self.assertEqual(tr.drain_commits(), [(T, P, 3)])

    def test_drain_is_idempotent_without_progress(self) -> None:
        tr = CommitOffsetTracker()
        tr.register(T, P, 0)
        tr.mark_done(T, P, 0)
        self.assertEqual(tr.drain_commits(), [(T, P, 1)])
        # No new completion -> nothing to commit.
        self.assertEqual(tr.drain_commits(), [])

    def test_resume_from_nonzero_offset(self) -> None:
        tr = CommitOffsetTracker()
        # Service resumes from a committed position; first seen offset is 100.
        for off in (100, 101):
            tr.register(T, P, off)
        tr.mark_done(T, P, 100)
        tr.mark_done(T, P, 101)
        self.assertEqual(tr.drain_commits(), [(T, P, 102)])

    def test_multiple_partitions_tracked_independently(self) -> None:
        tr = CommitOffsetTracker()
        tr.register(T, 0, 0)
        tr.register(T, 1, 0)
        tr.mark_done(T, 0, 0)
        # Only partition 0 advanced.
        self.assertEqual(tr.drain_commits(), [(T, 0, 1)])
        tr.mark_done(T, 1, 0)
        self.assertEqual(tr.drain_commits(), [(T, 1, 1)])

    def test_late_duplicate_ack_is_ignored(self) -> None:
        tr = CommitOffsetTracker()
        tr.register(T, P, 0)
        tr.mark_done(T, P, 0)
        self.assertEqual(tr.drain_commits(), [(T, P, 1)])
        # A duplicate ack for an already-absorbed offset must not regress.
        tr.mark_done(T, P, 0)
        self.assertEqual(tr.drain_commits(), [])
        self.assertEqual(tr.pending_count(), 0)


if __name__ == "__main__":
    unittest.main()
