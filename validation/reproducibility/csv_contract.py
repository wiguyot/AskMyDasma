"""Pure CSV format v1 contract for cross-machine reproducibility.

Defines, without any Kafka, Docker, analyzer or I/O dependency and without any
import-time side effect:

* the exact CSV v1 column layout per model (:data:`FORMAT_VERSION`,
  :data:`COMMON_COLUMNS`, :data:`MODEL_COLUMNS`, :func:`columns_for_model`);
* the single projection from a model viz JSON record to a stable CSV row
  (:data:`MODEL_FIELD_MAP`, :func:`normalize_viz_record`);
* the deterministic cell encoding (``None`` → empty cell, booleans →
  ``"true"``/``"false"``, finite numbers → ``repr(float(value))``, ``NaN`` →
  ``"NaN"``, infinities → ``"Infinity"``/``"-Infinity"``) so the comparator can
  rebuild values without ambiguity;
* the canonical comparison key and ordering helpers
  (:func:`canonical_key`, :func:`sort_canonical_rows`);
* the companion metadata dictionary (:func:`build_metadata`).

Rows are keyed by ``(Group, source_event_id)`` — machine- and clock-independent.
An incomplete viz JSON must surface as an error here rather than produce a
silently truncated, apparently comparable CSV.
"""

from __future__ import annotations

import copy
import json
import math
import re
from collections.abc import Iterable, Mapping
from typing import Any

FORMAT_VERSION = 1

COMMON_COLUMNS = (
    "source_event_id",
    "timestamp",
    "Group",
    "score",
    "status",
    "threshold",
    "anomaly",
)

MODEL_COLUMNS = {
    "hstree": COMMON_COLUMNS,
    "iforest": COMMON_COLUMNS,
    "kitnet": COMMON_COLUMNS,
    "deepant": COMMON_COLUMNS,
    "mdragpoint": COMMON_COLUMNS + ("mdragpoint_internal_r",),
}

# Single projection table: normalized CSV column -> source key in the viz JSON.
# Common source keys are exactly "source_event_id", "timestamp" and "Group":
# there is no fallback to "group", "LabviewTime" or any Kafka timestamp, so an
# incomplete viz JSON fails loudly instead of producing an ambiguous join.
_MODEL_PREFIXES = {
    "hstree": "hstree",
    "iforest": "iforest",
    "kitnet": "kitnet",
    "deepant": "deepant",
    "mdragpoint": "mdragpoint",
}

MODEL_FIELD_MAP: dict[str, dict[str, str]] = {
    model: {
        "source_event_id": "source_event_id",
        "timestamp": "timestamp",
        "Group": "Group",
        "score": f"{prefix}_score",
        "status": f"{prefix}_status",
        "threshold": f"{prefix}_threshold",
        "anomaly": f"{prefix}_anomaly",
    }
    for model, prefix in _MODEL_PREFIXES.items()
}
MODEL_FIELD_MAP["mdragpoint"]["mdragpoint_internal_r"] = "mdragpoint_internal_r"

VALID_SOURCE_ROLES = ("source-1", "source-2")
_CAMPAIGN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def columns_for_model(model: str) -> tuple[str, ...]:
    """Return the exact CSV v1 columns for a detection model.

    Explainability services never carry a reproducibility export; any other
    unknown model name is rejected.
    """
    try:
        return MODEL_COLUMNS[model]
    except KeyError:
        raise ValueError(
            f"unknown reproducibility model: {model!r}; expected one of "
            f"{', '.join(sorted(MODEL_COLUMNS))}"
        ) from None


def _require_numeric_or_none(source_key: str, value: Any) -> None:
    """Numeric or null: booleans are never numbers, NaN/Infinity are allowed."""
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            f"viz record field '{source_key}' must be a number or null, got {value!r}"
        )


def _encode_cell(value: Any) -> str:
    """Encode a validated value into its deterministic CSV v1 cell."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        number = float(value)
        if math.isnan(number):
            return "NaN"
        if math.isinf(number):
            return "Infinity" if number > 0 else "-Infinity"
        return repr(number)
    return str(value)


def normalize_viz_record(model: str, record: Mapping[str, Any]) -> dict[str, str]:
    """Project one viz JSON record to a CSV v1 row.

    The returned dict contains all and only the columns of
    :func:`columns_for_model`, in that order, with string cells ready for
    ``csv.DictWriter``. Derived visualization fields (``*_score_log10``, moving
    averages, ...) and unknown source fields are not exported.

    ``source_event_id`` and ``Group`` are mandatory, non-null and non-empty
    after text conversion; their content is not stripped since they are the
    comparison key. ``timestamp`` is optional (``None`` becomes an empty cell).
    Score, status, threshold, anomaly and the mDragPoint extra must be present
    in the record — their absence is an error — while every one of them may
    hold ``None`` to preserve a degraded output.
    """
    columns = columns_for_model(model)
    field_map = MODEL_FIELD_MAP[model]
    if not isinstance(record, Mapping):
        raise ValueError(f"viz record for {model!r} must be a mapping")

    row: dict[str, str] = {}
    for column in columns:
        source_key = field_map[column]
        if source_key not in record:
            raise ValueError(
                f"viz record is missing mandatory field '{source_key}' for model '{model}'"
            )
        raw = record[source_key]

        if column in ("source_event_id", "Group"):
            if raw is None:
                raise ValueError(f"viz record field '{source_key}' must not be null")
            text = str(raw)
            if text == "":
                raise ValueError(f"viz record field '{source_key}' must not be empty")
            row[column] = text
        elif column == "timestamp":
            row[column] = "" if raw is None else str(raw)
        elif column in ("score", "threshold", "mdragpoint_internal_r"):
            _require_numeric_or_none(source_key, raw)
            row[column] = _encode_cell(raw)
        elif column == "anomaly":
            if raw is not None and not isinstance(raw, bool):
                raise ValueError(
                    f"viz record field '{source_key}' must be true, false or null, got {raw!r}"
                )
            row[column] = _encode_cell(raw)
        else:  # status
            if raw is not None and not isinstance(raw, str):
                raise ValueError(
                    f"viz record field '{source_key}' must be a string or null, got {raw!r}"
                )
            row[column] = _encode_cell(raw)
    return row


def canonical_key(row: Mapping[str, str]) -> tuple[str, str]:
    """Return the comparison key ``(Group, source_event_id)``.

    No timestamp and no numeric conversion take part in the key. A missing or
    empty value raises ``ValueError``.
    """
    group = row.get("Group")
    event_id = row.get("source_event_id")
    if group in (None, "") or event_id in (None, ""):
        raise ValueError(
            "canonical key requires non-empty 'Group' and 'source_event_id'"
        )
    return str(group), str(event_id)


def sort_canonical_rows(rows: Iterable[Mapping[str, str]]) -> list[dict[str, str]]:
    """Sort copies of ``rows`` by :func:`canonical_key`, rejecting duplicates.

    Pure test-oracle helper for small datasets: production sorting (external
    or bounded) is the exporter's responsibility, not this function's.
    """
    normalized = [dict(row) for row in rows]
    normalized.sort(key=canonical_key)
    seen: set[tuple[str, str]] = set()
    for row in normalized:
        key = canonical_key(row)
        if key in seen:
            raise ValueError(f"duplicate canonical key in rows: {key}")
        seen.add(key)
    return normalized


def _decode_number(column: str, cell: str) -> float | None:
    """Decode a numeric v1 cell: the exact symmetric inverse of the encoder.

    Only the canonical v1 grammar is accepted: empty cell → ``None``, the
    tokens ``NaN`` / ``Infinity`` / ``-Infinity``, or a finite number spelled
    exactly like ``repr(float(value))``. Added spaces, alternative spellings
    (``nan``, ``inf``), wrong casing and non-canonical decimals are rejected.
    """
    if cell == "":
        return None
    if cell in ("NaN", "Infinity", "-Infinity"):
        return float(cell)
    lowered = cell.lower()
    if lowered in {"nan", "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity"}:
        raise ValueError(f"invalid CSV v1 numeric cell for '{column}': {cell!r}")
    try:
        value = float(cell)
    except ValueError as exc:
        raise ValueError(
            f"invalid CSV v1 numeric cell for '{column}': {cell!r}"
        ) from exc
    if math.isnan(value) or math.isinf(value) or cell != repr(value):
        raise ValueError(f"non-canonical CSV v1 numeric cell for '{column}': {cell!r}")
    return value


def _decode_anomaly(cell: str) -> bool | None:
    if cell == "":
        return None
    if cell == "true":
        return True
    if cell == "false":
        return False
    raise ValueError(f"invalid CSV v1 boolean cell for 'anomaly': {cell!r}")


def decode_v1_row(model: str, row: Mapping[str, str]) -> dict[str, Any]:
    """Decode one CSV v1 row back to typed values (symmetric of the encoder).

    The mapping must provide exactly the columns of :func:`columns_for_model`.
    Empty cells decode to ``None`` for the optional-capable fields; the v1
    format does not distinguish a ``None`` status from an empty status string
    (both decode to ``None`` and therefore compare equal). Identifiers,
    timestamps and status strings are returned unchanged — their content is
    never stripped. Numeric cells must follow the strict canonical grammar of
    the encoder; any other spelling is rejected.
    """
    columns = columns_for_model(model)
    decoded: dict[str, Any] = {}
    for column in columns:
        if column not in row:
            raise ValueError(f"CSV row is missing column '{column}'")
        cell = row[column]
        if column in ("score", "threshold", "mdragpoint_internal_r"):
            decoded[column] = _decode_number(column, cell)
        elif column == "anomaly":
            if cell == "":
                decoded[column] = None
            elif cell == "true":
                decoded[column] = True
            elif cell == "false":
                decoded[column] = False
            else:
                raise ValueError(f"invalid CSV v1 boolean cell for 'anomaly': {cell!r}")
        elif column == "timestamp" or column == "status":
            # v1 encodes None and the empty string with the same cell; the
            # comparison uses this shared normalized representation.
            decoded[column] = None if cell == "" else cell
        else:  # source_event_id, Group
            if cell == "":
                raise ValueError(f"canonical key column '{column}' must not be empty")
            decoded[column] = cell
    return decoded


def build_metadata(
    *,
    model: str,
    source_role: str,
    campaign_id: str,
    campaign_parameters: Mapping[str, Any],
    image: str,
) -> dict[str, Any]:
    """Build the companion metadata dictionary for one CSV export.

    ``columns`` always derives from :func:`columns_for_model` — never from a
    caller-provided list. ``campaign_parameters`` is deep-copied and must be
    standard JSON (``json.dumps(..., allow_nan=False)``). ``image`` is stored
    verbatim (possibly empty in P2-2); no image is deduced and no hash or
    manifest is computed.
    """
    columns = columns_for_model(model)
    if source_role not in VALID_SOURCE_ROLES:
        raise ValueError(
            f"source role must be exactly 'source-1' or 'source-2', got {source_role!r}"
        )
    if not _CAMPAIGN_ID_PATTERN.match(str(campaign_id)):
        raise ValueError(
            "campaign-id must be a portable directory name (alphanumeric, dot, "
            "underscore or hyphen; starting with an alphanumeric character; at "
            f"most 128 characters); got {campaign_id!r}"
        )
    if not isinstance(image, str):
        raise ValueError(f"image must be a string, got {type(image).__name__}")
    parameters = copy.deepcopy(dict(campaign_parameters))
    try:
        json.dumps(parameters, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"campaign_parameters must be standard JSON (allow_nan=False): {exc}"
        ) from exc

    return {
        "format_version": FORMAT_VERSION,
        "model": model,
        "source_role": source_role,
        "campaign_id": str(campaign_id),
        "columns": list(columns),
        "campaign_parameters": parameters,
        "image": image,
    }
