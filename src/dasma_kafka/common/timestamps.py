"""Shared timestamp resolution helpers for all processing services."""

from __future__ import annotations

from typing import Any, Iterable

DEFAULT_TIMESTAMP_CANDIDATES: tuple[str, ...] = (
    "timestamp",
    "Timestamp",
    "time",
    "Time",
    "LabviewTime",
    "datetime",
    "Datetime",
    "date",
    "Date",
)


def resolve_timestamp_key(
    record: Any,
    candidates: Iterable[str] = DEFAULT_TIMESTAMP_CANDIDATES,
) -> str | None:
    """Resolve the first available timestamp-like key in a record."""
    if not isinstance(record, dict):
        return None
    for key in candidates:
        value = record.get(key)
        if value not in (None, ""):
            return key
    return None


def resolve_timestamp_value(
    record: Any,
    candidates: Iterable[str] = DEFAULT_TIMESTAMP_CANDIDATES,
) -> Any | None:
    """Resolve the first available timestamp-like value in a record."""
    key = resolve_timestamp_key(record, candidates=candidates)
    if key is None or not isinstance(record, dict):
        return None
    return record.get(key)

