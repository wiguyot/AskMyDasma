"""Pure contiguous offset tracker for Kafka commits (explainability-v2, OC-06).

This helper prepares OC-07: commit only what is really finished, without ever
jumping over a hole. It holds no Kafka dependency, no I/O, no thread: it is a
pure in-memory structure, entirely testable.

Three distinct notions (do not confuse them):

- **processed offset**: an offset with a terminal outcome (success or failure),
  recorded via ``record_outcome``;
- **last contiguous successful offset**: the last offset of the unbroken run
  of successes starting at the partition's first offset; the run never crosses
  a pending offset and never crosses a failure (a failure is a barrage);
- **Kafka commit value (exclusive)**: ``last_contiguous_successful_offset + 1``.
  Committing value V tells Kafka "restart from V": every offset < V is
  considered done, V itself will be re-consumed. A failure at offset F caps the
  commit value at F (restarting from F re-processes the failed message:
  at-least-once is preserved, nothing is skipped).

Group independence note: several groups share a partition, so offset 12 (group
B) may finish before offset 11 (group A). The tracker only sees
(partition, offset): an out-of-order success is remembered without advancing
the cursor beyond the first hole; the commit value only moves when the hole is
filled.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


class OffsetTrackerError(ValueError):
    """Programming error detected by the tracker (duplicate, gap, unknown)."""


@dataclass
class _PartitionState:
    first_offset: Optional[int] = None
    # First offset not contiguously successful; never advances past a failure.
    cursor: Optional[int] = None
    registered: set = field(default_factory=set)
    successes: set = field(default_factory=set)
    failures: set = field(default_factory=set)
    # Last commit value emitted by record_outcome (to detect "new commit").
    last_emitted: Optional[int] = None


class ContiguousOffsetTracker:
    """Track terminal outcomes of registered offsets per Kafka partition.

    API:

    - ``register_received(partition, offset)``: the main process accepted the
      message. Registrations of a partition must be strictly contiguous in
      call order (first offset initializes the partition; a duplicate or a gap
      is a programming error).
    - ``record_outcome(partition, offset, success)``: the terminal outcome of a
      registered offset. Returns the new Kafka commit value (exclusive) when
      this outcome advanced the contiguous run of successes, ``None``
      otherwise. Exactly one outcome per offset; an out-of-order success is
      remembered but never advances the cursor beyond a hole; a failure is a
      barrage: itself and every later offset never become committable.
    - ``snapshot()``: read-only per-partition view for tests and monitoring.

    The tracker never touches a real consumer or real Kafka offsets (OC-07
    will wire it); it never treats an unregistered offset as processed and
    never silently drops data.
    """

    def __init__(self) -> None:
        self._partitions: Dict[int, _PartitionState] = {}

    @staticmethod
    def _validate_partition_offset(partition: int, offset: int) -> None:
        for name, value in (("partition", partition), ("offset", offset)):
            if isinstance(value, bool) or not isinstance(value, int):
                raise OffsetTrackerError(
                    f"{name} must be an int, got {value!r}"
                )
            if value < 0:
                raise OffsetTrackerError(
                    f"{name} must be >= 0, got {value}"
                )

    def _state_for(self, partition: int) -> _PartitionState:
        state = self._partitions.get(partition)
        if state is None:
            raise OffsetTrackerError(
                f"unknown partition {partition}: no offset registered yet"
            )
        return state

    def register_received(self, partition: int, offset: int) -> None:
        """Register a message accepted for processing (strictly contiguous)."""
        self._validate_partition_offset(partition, offset)
        state = self._partitions.get(partition)
        if state is None:
            self._partitions[partition] = _PartitionState(
                first_offset=offset,
                cursor=offset,
                registered={offset},
            )
            return
        expected = max(state.registered) + 1
        if offset != expected:
            raise OffsetTrackerError(
                f"non-contiguous registration for partition {partition}: "
                f"offset {offset} received, expected {expected} "
                f"(duplicate or gap)"
            )
        state.registered.add(offset)

    def record_outcome(
        self, partition: int, offset: int, success: bool
    ) -> Optional[int]:
        """Record a terminal outcome; return the new commit value if any.

        The commit value is exclusive: ``last_contiguous_successful + 1``.
        """
        self._validate_partition_offset(partition, offset)
        if not isinstance(success, bool):
            raise OffsetTrackerError(f"success must be a bool, got {success!r}")
        state = self._state_for(partition)
        if offset not in state.registered:
            raise OffsetTrackerError(
                f"outcome for an unregistered offset: partition {partition}, "
                f"offset {offset}"
            )
        if offset in state.successes or offset in state.failures:
            raise OffsetTrackerError(
                f"duplicate outcome for partition {partition}, offset {offset}"
            )

        if success:
            state.successes.add(offset)
        else:
            state.failures.add(offset)

        if not success or offset != state.cursor:
            # A failure is a barrage. A success behind a hole is remembered
            # without advancing the cursor; the cursor will consume it when
            # the hole is filled.
            return None

        # The success extends the contiguous run: advance the cursor over
        # every already-successful offset, stopping at a pending offset or at
        # a failure (the barrage).
        state.cursor += 1
        while state.cursor in state.successes:
            state.cursor += 1
        commit_value = state.cursor if state.cursor > state.first_offset else None
        if commit_value is not None and commit_value != state.last_emitted:
            state.last_emitted = commit_value
            return commit_value
        return None

    def snapshot(self) -> Dict[int, Dict[str, object]]:
        """Return a read-only per-partition view of the tracker state."""
        result: Dict[int, Dict[str, object]] = {}
        for partition, state in self._partitions.items():
            contiguous_run = set(range(state.first_offset, state.cursor))
            result[partition] = {
                "first_offset": state.first_offset,
                "next_committable": (
                    state.cursor if state.cursor > state.first_offset else None
                ),
                "pending": sorted(state.registered - state.successes - state.failures),
                "succeeded_blocked": sorted(state.successes - contiguous_run),
                "failed": sorted(state.failures),
            }
        return result
