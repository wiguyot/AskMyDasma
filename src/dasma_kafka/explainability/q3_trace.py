"""Opt-in, buffered side-channel trace for the Q3 partition experiment.

No recorder is created unless ``DASMA_Q3_TRACE_DIR`` is set. Trace data is
written to local JSONL files and is never added to Kafka messages.
"""

from __future__ import annotations

import json
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np


class Q3TraceRecorder:
    """Buffer Q3 observations and persist them once, on orderly shutdown."""

    def __init__(self, service: str, directory: Optional[str] = None) -> None:
        trace_dir = directory if directory is not None else os.getenv("DASMA_Q3_TRACE_DIR")
        self.enabled = bool(trace_dir)
        self.service = service
        self.run_id = os.getenv("DASMA_Q3_TRACE_RUN_ID", "q3-unlabelled")
        self.process_instance = f"{service}:{os.getpid()}"
        self._directory = Path(trace_dir) if trace_dir else None
        self._records: list[Dict[str, Any]] = []
        self._record_seq = 0
        self._commit_seq = 0

    def _base(self, record_type: str) -> Dict[str, Any]:
        self._record_seq += 1
        return {
            "schema_version": "q3-partition-trace-v1",
            "run_id": self.run_id,
            "service": self.service,
            "record_type": record_type,
            "process_instance": self.process_instance,
            "record_seq": self._record_seq,
            "record_monotonic_ns": time.monotonic_ns(),
        }

    def message_lifecycle(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        group: str,
        source_event_id: str,
        receive_seq: int,
        dispatch_seq: Optional[int],
        worker_id: Optional[str],
        group_seq: Optional[int],
        start_seq: Optional[int],
        complete_seq: Optional[int],
        published_outputs: list[Dict[str, Any]],
        output_status: Optional[str],
        consumer_process_instance: Optional[str] = None,
    ) -> None:
        if not self.enabled:
            return
        self._records.append({
            **self._base("message_lifecycle"),
            "source_topic": topic,
            "source_partition": int(partition),
            "source_offset": int(offset),
            "group": group,
            "source_event_id": source_event_id,
            "consumer": {
                "process_instance": consumer_process_instance or f"consumer:{os.getppid() if worker_id else os.getpid()}",
                "receive_seq": int(receive_seq),
                "dispatch_seq": dispatch_seq,
            },
            "worker": {
                "process_instance": self.process_instance if worker_id else None,
                "worker_id": worker_id,
                "group_seq": group_seq,
                "start_seq": start_seq,
                "complete_seq": complete_seq,
            },
            "published_outputs": published_outputs,
            "output_status": output_status,
        })

    def offset_commit(self, topic: str, partition: int, next_offset: int, outcome: str) -> None:
        if not self.enabled:
            return
        self._commit_seq += 1
        self._records.append({
            **self._base("offset_commit"),
            "commit_seq": self._commit_seq,
            "offsets": [{"topic": topic, "partition": int(partition), "next_offset": int(next_offset)}],
            "commit_outcome": outcome,
        })

    def model_initialization(self, **fields: Any) -> None:
        if not self.enabled:
            return
        self._records.append({**self._base("model_initialization"), **fields})

    def close(self) -> None:
        """Append this process's buffered observations to its unique JSONL."""
        if not self.enabled or not self._records or self._directory is None:
            return
        self._directory.mkdir(parents=True, exist_ok=True)
        path = self._directory / f"{self.service}-{os.getpid()}.jsonl"
        with path.open("a", encoding="utf-8") as stream:
            for record in self._records:
                stream.write(json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False))
                stream.write("\n")
        self._records.clear()


def initialization_fields(
    *,
    topic: str,
    partition: int,
    offset: int,
    group: str,
    source_event_id: str,
    initialization_order_seq: int,
    warmup_event_ids: list[str],
    warmup_values: list[list[float]],
    feature_names: list[str],
    feature_mins: Any,
    feature_maxes: Any,
    hstree_parameters: Dict[str, int],
    pysad_version: str,
) -> Dict[str, Any]:
    """Build the pre-construction snapshot without consuming random values."""
    import hashlib
    import importlib.metadata
    import pickle

    numpy_state = np.random.get_state()
    python_state = __import__("random").getstate()
    warmup_blob = json.dumps(warmup_values, separators=(",", ":"), allow_nan=False).encode()
    return {
        "source_topic": topic,
        "source_partition": int(partition),
        "source_offset": int(offset),
        "group": group,
        "source_event_id": source_event_id,
        "initialization_order_seq": int(initialization_order_seq),
        "warmup_event_ids": list(warmup_event_ids),
        "warmup_values_sha256": hashlib.sha256(warmup_blob).hexdigest(),
        "feature_names": list(feature_names),
        "feature_mins": {name: float(value) for name, value in zip(feature_names, feature_mins)},
        "feature_maxes": {name: float(value) for name, value in zip(feature_names, feature_maxes)},
        "hstree_parameters": dict(hstree_parameters),
        "numpy_state": {
            "algorithm": numpy_state[0],
            "keys": [int(value) for value in numpy_state[1]],
            "position": int(numpy_state[2]),
            "has_gaussian": int(numpy_state[3]),
            "cached_gaussian": float(numpy_state[4]),
        },
        "python_state_sha256": hashlib.sha256(pickle.dumps(python_state, protocol=4)).hexdigest(),
        "versions": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pysad": pysad_version or importlib.metadata.version("pysad"),
        },
    }
