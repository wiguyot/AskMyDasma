"""Per-partition commit watermark for the parallel-by-group DeepAnT service.

When DeepAnT dispatches each Kafka message to a per-group worker process, the
main process can no longer commit an offset right after producing it: the worker
publishes asynchronously. To preserve the current *publish-before-commit*
guarantee, the main process tracks, per ``(topic, partition)``, which offsets a
worker has confirmed as published (or that the main deliberately skipped), and
only commits the highest *contiguous* completed offset.

Semantics (Option A, strict):
- An offset is marked done only after its worker acked a successful publish, or
  when the main process skips it (deserialization error, no dispatch). Skips
  advance the watermark, mirroring the previous behavior where a later commit
  implicitly covered skipped offsets.
- A worker *failure* is never marked done, so the contiguous watermark stalls at
  that offset and nothing past it is committed. This is the "no commit on worker
  failure" guarantee: the message is reprocessed on restart, no data loss.

The source topic currently has a single partition, but tracking is keyed by
``(topic, partition)`` so the design stays correct if partitions are added.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class _PartitionState:
    # Next offset that must complete for the watermark to advance. Seeded from
    # the first offset observed on the partition.
    frontier: int | None = None
    # Completed offsets at or above ``frontier`` waiting to be absorbed.
    completed: set[int] = field(default_factory=set)
    # The frontier value already returned by a previous commit drain.
    committed_frontier: int | None = None


class CommitOffsetTracker:
    """Track contiguous completion per partition and yield committable offsets."""

    def __init__(self) -> None:
        self._partitions: dict[tuple[str, int], _PartitionState] = {}

    def register(self, topic: str, partition: int, offset: int) -> None:
        """Record that an offset has been observed (dispatched) on a partition.

        Seeds the partition frontier on first sight. Offsets are observed in
        increasing order within a partition (Kafka per-partition total order).
        """
        state = self._partitions.setdefault((topic, partition), _PartitionState())
        if state.frontier is None:
            # Seed the frontier and the committed baseline at the resume point so
            # a partition only emits a commit once its watermark moves forward.
            state.frontier = offset
            state.committed_frontier = offset

    def mark_done(self, topic: str, partition: int, offset: int) -> None:
        """Mark an offset as completed (worker publish acked, or skipped).

        Absorbs as many contiguous completed offsets as possible into the
        frontier.
        """
        state = self._partitions.setdefault((topic, partition), _PartitionState())
        if state.frontier is None:
            state.frontier = offset
            state.committed_frontier = offset

        if offset < state.frontier:
            # Already absorbed; ignore late or duplicate acks.
            return

        state.completed.add(offset)
        while state.frontier in state.completed:
            state.completed.discard(state.frontier)
            state.frontier += 1

    def drain_commits(self) -> list[tuple[str, int, int]]:
        """Return the offsets to commit since the last drain.

        Each entry is ``(topic, partition, commit_offset)`` where
        ``commit_offset`` is the Kafka commit position (next offset to read =
        highest contiguous completed offset + 1). Only partitions whose
        watermark advanced are returned.
        """
        commits: list[tuple[str, int, int]] = []
        for (topic, partition), state in self._partitions.items():
            if state.frontier is None:
                continue
            if state.committed_frontier == state.frontier:
                continue
            # frontier is the next offset NOT yet completed, i.e. exactly the
            # Kafka commit position.
            commits.append((topic, partition, state.frontier))
            state.committed_frontier = state.frontier
        return commits

    def pending_count(self) -> int:
        """Total number of completed-but-not-yet-contiguous offsets held."""
        return sum(len(state.completed) for state in self._partitions.values())
