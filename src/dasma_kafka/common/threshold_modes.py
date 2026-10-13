"""Pure threshold-mode normalizers, schema extenders and parsing helpers.

No heavy dependencies (torch, Kafka, pysad). Safe to import from tests or
CI runs that lack those packages.
"""

from __future__ import annotations

import json
from typing import Optional


DEEPANT_ANALYSIS_SCHEMA_FIELDS: tuple[tuple[str, list[str]], ...] = (
    ("deepant_score", ["null", "double"]),
    ("deepant_status", ["null", "string"]),
    ("deepant_anomaly", ["null", "boolean"]),
    ("deepant_threshold", ["null", "double"]),
)

HSTREE_ANALYSIS_SCHEMA_FIELDS: tuple[tuple[str, list[str]], ...] = (
    ("hstree_score", ["null", "double"]),
    ("hstree_status", ["null", "string"]),
    ("hstree_anomaly", ["null", "boolean"]),
    ("hstree_threshold", ["null", "double"]),
    ("hstree_variant", ["null", "string"]),
)

IFOREST_ANALYSIS_SCHEMA_FIELDS: tuple[tuple[str, list[str]], ...] = (
    ("iforest_score", ["null", "double"]),
    ("iforest_status", ["null", "string"]),
    ("iforest_anomaly", ["null", "boolean"]),
    ("iforest_threshold", ["null", "double"]),
)

KITNET_ANALYSIS_SCHEMA_FIELDS: tuple[tuple[str, list[str]], ...] = (
    ("kitnet_score", ["null", "double"]),
    ("kitnet_status", ["null", "string"]),
    ("kitnet_anomaly", ["null", "boolean"]),
    ("kitnet_threshold", ["null", "double"]),
)

MDRAGPOINT_ANALYSIS_SCHEMA_FIELDS: tuple[tuple[str, list[str]], ...] = (
    ("mdragpoint_score", ["null", "double"]),
    ("mdragpoint_status", ["null", "string"]),
    ("mdragpoint_anomaly", ["null", "boolean"]),
    ("mdragpoint_threshold", ["null", "double"]),
    ("mdragpoint_internal_r", ["null", "double"]),
)


def _extend_avro_schema(
    source_avro_schema: str,
    fields: tuple[tuple[str, list[str]], ...],
) -> str:
    """Return the source Avro schema extended with the given fields (idempotent)."""
    schema_dict = json.loads(source_avro_schema)
    if "fields" not in schema_dict:
        schema_dict["fields"] = []
    existing = {f.get("name") for f in schema_dict["fields"]}
    for name, avro_type in fields:
        if name not in existing:
            schema_dict["fields"].append(
                {"name": name, "type": avro_type, "default": None}
            )
    return json.dumps(schema_dict)


def normalize_deepant_threshold_mode(raw_mode: Optional[str]) -> str:
    """Normalize DeepAnT threshold mode values accepted by scripts and UI."""
    mode = (raw_mode if raw_mode is not None else "percentile").strip().lower()
    if mode == "p99":
        return "percentile"
    if mode in {"percentile", "welford", "welford-windowed"}:
        return mode
    return "percentile"


def normalize_hstree_threshold_mode(raw_mode: Optional[str]) -> str:
    """Normalize HSTree threshold mode values accepted by scripts and UI."""
    mode = (raw_mode if raw_mode is not None else "percentile").strip().lower()
    if mode == "p99":
        return "percentile"
    if mode in {"percentile", "welford", "welford-windowed"}:
        return mode
    return "percentile"


def normalize_iforest_threshold_mode(raw_mode: Optional[str]) -> str:
    """Normalize iForest threshold mode values accepted by scripts and UI."""
    mode = (raw_mode if raw_mode is not None else "native").strip().lower()
    if mode == "native":
        return mode
    if mode in {"p99", "percentile"}:
        return "p99"
    if mode in {"welford", "welford-windowed"}:
        return mode
    return "native"


def normalize_kitnet_threshold_mode(raw_mode: Optional[str]) -> str:
    """Normalize KitNET threshold mode values accepted by scripts and UI."""
    mode = (raw_mode if raw_mode is not None else "welford").strip().lower()
    if mode in {"p99", "percentile"}:
        return "p99"
    if mode in {"welford", "welford-windowed"}:
        return mode
    return "welford"


def normalize_mdragpoint_threshold_mode(raw_mode: Optional[str]) -> str:
    """Normalize mDragPoint threshold mode values accepted by scripts and UI."""
    value = (raw_mode or "percentile").strip().lower()
    if value == "p99":
        return "percentile"
    if value in {"percentile", "welford", "welford-windowed"}:
        return value
    return "percentile"


def normalize_mdragpoint_verdict_mode(raw_mode: Optional[str]) -> str:
    """Normalize the mDragPoint verdict mode."""
    value = (raw_mode or "internal").strip().lower()
    if value in {"internal", "native", "mdragstream"}:
        return "internal"
    if value in {"external", "score", "threshold"}:
        return "external"
    return "internal"


def parse_deepant_exclude_anomalies_from_refit(raw_value: Optional[str]) -> bool:
    """Return whether detected anomalies should be excluded from refit history."""
    value = (raw_value or "0").strip().lower()
    return value in {"1", "true", "yes", "on"}


def extend_deepant_avro_schema(source_avro_schema: str) -> str:
    return _extend_avro_schema(source_avro_schema, DEEPANT_ANALYSIS_SCHEMA_FIELDS)


def extend_hstree_avro_schema(source_avro_schema: str) -> str:
    return _extend_avro_schema(source_avro_schema, HSTREE_ANALYSIS_SCHEMA_FIELDS)


def extend_iforest_avro_schema(source_avro_schema: str) -> str:
    return _extend_avro_schema(source_avro_schema, IFOREST_ANALYSIS_SCHEMA_FIELDS)


def extend_kitnet_avro_schema(source_avro_schema: str) -> str:
    return _extend_avro_schema(source_avro_schema, KITNET_ANALYSIS_SCHEMA_FIELDS)


def extend_mdragpoint_avro_schema(source_avro_schema: str) -> str:
    return _extend_avro_schema(source_avro_schema, MDRAGPOINT_ANALYSIS_SCHEMA_FIELDS)
