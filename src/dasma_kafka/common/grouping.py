"""Shared group resolution helpers for all processing services."""

from __future__ import annotations

from typing import Any

DEFAULT_GROUP_FALLBACK = "__NO_GROUP__"


def resolve_group_key(record: Any, fallback: str = DEFAULT_GROUP_FALLBACK) -> str:
    """Resolve the logical group key from a record.

    Resolution order:
    1. ``record["Group"]``
    2. ``record["group"]``
    3. fallback value
    """
    if isinstance(record, dict):
        for key in ("Group", "group"):
            value = record.get(key)
            if value not in (None, ""):
                return str(value)
    return fallback

