#!/usr/bin/env python3
"""Pure contract validator for the HSTree upstream JSON viz export.

The validator deliberately has no Kafka, Docker or network dependency.  It
normalizes the upstream ``*-hstree-viz`` JSONL contract needed by the Q1
amont/local comparison and rejects malformed identities, scores, verdicts and
duplicates explicitly.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

HSTREE_STATUSES = frozenset({"warming_up", "normal", "anomaly"})
IDENTITY_FIELDS = ("Group", "source_event_id")
REQUIRED_FIELDS = (
    "timestamp",
    "Group",
    "source_event_id",
    "hstree_score",
    "hstree_status",
    "hstree_anomaly",
    "hstree_threshold",
)


class HSTreeExportError(ValueError):
    """Raised when an upstream HSTree export violates its contract."""


@dataclass(frozen=True)
class HSTreeExportValidation:
    """Deterministic summary of one validated upstream export."""

    export_name: str
    record_count: int
    identities: tuple[tuple[str, str], ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "export_name": self.export_name,
            "record_count": self.record_count,
            "identities": [list(identity) for identity in self.identities],
        }


def _line_error(export_name: str, line_number: int, message: str) -> HSTreeExportError:
    return HSTreeExportError(f"{export_name}:{line_number}: {message}")


def _finite_number(value: Any, field: str, export_name: str, line_number: int) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _line_error(export_name, line_number, f"{field} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise _line_error(export_name, line_number, f"{field} must be finite")
    return number


def _identity(record: dict[str, Any], export_name: str, line_number: int) -> tuple[str, str]:
    values: list[str] = []
    for field in IDENTITY_FIELDS:
        value = record.get(field)
        if not isinstance(value, str) or not value.strip():
            raise _line_error(export_name, line_number, f"{field} must be a non-empty string")
        values.append(value)
    return values[0], values[1]


def validate_hstree_records(
    records: Iterable[Any],
    *,
    export_name: str = "hstree-analysis-viz",
) -> HSTreeExportValidation:
    """Validate records and return an order-independent identity summary."""

    identities: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for line_number, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise _line_error(export_name, line_number, "record must be a JSON object")
        missing = [field for field in REQUIRED_FIELDS if field not in record]
        if missing:
            raise _line_error(export_name, line_number, f"missing fields: {', '.join(missing)}")

        identity = _identity(record, export_name, line_number)
        if identity in seen:
            raise _line_error(export_name, line_number, f"duplicate identity: {identity!r}")
        seen.add(identity)
        identities.append(identity)

        timestamp = record["timestamp"]
        if not isinstance(timestamp, str) or not timestamp.strip():
            raise _line_error(export_name, line_number, "timestamp must be a non-empty string")
        _finite_number(record["hstree_score"], "hstree_score", export_name, line_number)

        status = record["hstree_status"]
        if status not in HSTREE_STATUSES:
            raise _line_error(export_name, line_number, f"invalid hstree_status: {status!r}")

        anomaly = record["hstree_anomaly"]
        threshold = record["hstree_threshold"]
        if status == "warming_up":
            if anomaly is not None or threshold is not None:
                raise _line_error(
                    export_name,
                    line_number,
                    "warming_up requires hstree_anomaly and hstree_threshold to be null",
                )
        else:
            if not isinstance(anomaly, bool):
                raise _line_error(
                    export_name,
                    line_number,
                    f"{status} requires boolean hstree_anomaly",
                )
            _finite_number(threshold, "hstree_threshold", export_name, line_number)
            expected_anomaly = status == "anomaly"
            if anomaly is not expected_anomaly:
                raise _line_error(
                    export_name,
                    line_number,
                    f"{status} requires hstree_anomaly={expected_anomaly}",
                )

    return HSTreeExportValidation(
        export_name=export_name,
        record_count=len(identities),
        identities=tuple(sorted(identities)),
    )


def load_hstree_jsonl(path: str | Path, *, export_name: str | None = None) -> HSTreeExportValidation:
    """Load and validate a JSONL HSTree viz export."""

    source = Path(path)
    name = export_name or str(source)
    try:
        handle = source.open(encoding="utf-8")
    except OSError as exc:
        raise HSTreeExportError(f"{name}: cannot read file: {exc}") from exc

    records: list[Any] = []
    with handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise _line_error(name, line_number, f"invalid JSON: {exc.msg}") from exc
    return validate_hstree_records(records, export_name=name)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export", type=Path)
    args = parser.parse_args()
    print(json.dumps(load_hstree_jsonl(args.export).as_dict(), indent=2))
