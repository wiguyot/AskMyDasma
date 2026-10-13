#!/usr/bin/env python3
"""
compare_csv.py

Autonomous comparator of the two CSV v1 exports produced by the reproducibility
exporter (P2-3). It performs a streaming merge join on the canonical key
``(Group, source_event_id)`` — never on a timestamp — and writes:

* a diagnostic comparison report (CSV), published first;
* a JSON summary, published last: its presence is the success marker.

Exact equality of the normalized CSV v1 values is the primary verdict. The
tolerances (--abs-tolerance, --relative-tolerance, default 0.0) form a second,
explicitly labelled indicator for small cross-machine floating-point drift:
``within_tolerance`` never hides a text, boolean, null, NaN or infinite
divergence, and it is never presented as exact. The report rows carry the
hierarchical flags (exact implies within-tolerance) while the report status and
the summary counter only label the tolerated non-exact rows so the summary
categories stay disjoint.

Direct execution from the repository root:

    python validation/reproducibility/compare_csv.py \\
      --source-1 data/reproducibility/campaign-42/hstree-source-1.csv \\
      --source-2 data/reproducibility/campaign-42/hstree-source-2.csv \\
      --report /tmp/hstree-comparison.csv
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
import json
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from validation.reproducibility.csv_contract import (
        FORMAT_VERSION,
        canonical_key,
        columns_for_model,
        decode_v1_row,
    )
else:
    from .csv_contract import (
        FORMAT_VERSION,
        canonical_key,
        columns_for_model,
        decode_v1_row,
    )

NUMERIC_COLUMNS = ("score", "threshold", "mdragpoint_internal_r")
KEY_COLUMNS = ("Group", "source_event_id")
TIMESTAMP_COLUMN = "timestamp"
METADATA_KEYS = (
    "format_version",
    "model",
    "source_role",
    "campaign_id",
    "columns",
    "campaign_parameters",
    "image",
)
STATUSES = ("exact", "within_tolerance", "divergent", "missing_source_1", "missing_source_2")
_CAMPAIGN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ComparisonError(Exception):
    """Fatal precondition or contract error of a comparison run."""


@dataclass(frozen=True)
class ComparisonConfig:
    """Immutable configuration of one comparison run."""

    source_1: Path
    source_2: Path
    report: Path
    summary: Path
    abs_tolerance: float
    relative_tolerance: float


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def values_equal(a: Any, b: Any) -> bool:
    """Exact v1 equality: null==null, NaN==NaN, same-sign infinities, ``==``."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) and math.isnan(b):
            return True
        return a == b  # -0.0 == 0.0; same-sign infinities are equal
    return a == b


def _is_finite_number(value: Any) -> bool:
    return (
        value is not None
        and not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def compare_numeric_pair(
    a: float, b: float, abs_tolerance: float, relative_tolerance: float
) -> tuple[bool, float, float]:
    """Return ``(tolerated, abs_diff, relative_diff)`` for two finite numbers."""
    value_1, value_2 = float(a), float(b)
    abs_diff = abs(value_1 - value_2)
    if abs(value_1) == 0.0 and abs(value_2) == 0.0:
        relative_diff = 0.0
    else:
        relative_diff = abs_diff / max(abs(value_1), abs(value_2))
    tolerated = abs_diff <= max(
        abs_tolerance, relative_tolerance * max(abs(value_1), abs(value_2))
    )
    return tolerated, abs_diff, relative_diff


def _reject_non_standard_constant(token: str) -> Any:
    raise ComparisonError(f"non-standard JSON constant {token!r} in metadata")


def validate_metadata(csv_path: Path, expected_role: str) -> dict[str, Any]:
    """Load and fully validate the companion metadata of one export."""
    metadata_path = csv_path.with_name(csv_path.stem + ".metadata.json")
    if not metadata_path.exists():
        raise ComparisonError(f"missing companion metadata: {metadata_path}")
    try:
        metadata = json.loads(
            metadata_path.read_text(encoding="utf-8"),
            parse_constant=_reject_non_standard_constant,
        )
    except json.JSONDecodeError as exc:
        raise ComparisonError(f"invalid metadata JSON in {metadata_path}: {exc}") from exc
    if not isinstance(metadata, dict):
        raise ComparisonError(f"metadata {metadata_path} must be a JSON object")
    unknown = sorted(set(metadata) - set(METADATA_KEYS))
    missing = sorted(set(METADATA_KEYS) - set(metadata))
    if unknown or missing:
        raise ComparisonError(
            f"metadata {metadata_path} key mismatch — unknown: {unknown}, missing: {missing}"
        )
    if metadata["format_version"] != FORMAT_VERSION:
        raise ComparisonError(
            f"metadata {metadata_path} has format_version "
            f"{metadata['format_version']!r}, expected {FORMAT_VERSION}"
        )
    if metadata["source_role"] != expected_role:
        raise ComparisonError(
            f"metadata {metadata_path} must carry source_role {expected_role!r}, "
            f"got {metadata['source_role']!r}"
        )
    model = metadata["model"]
    try:
        columns = columns_for_model(model)
    except ValueError as exc:
        raise ComparisonError(f"metadata {metadata_path}: {exc}") from exc
    if metadata["columns"] != list(columns):
        raise ComparisonError(
            f"metadata {metadata_path} columns {metadata['columns']!r} do not match "
            f"the contract for model {model!r}: {list(columns)!r}"
        )
    campaign_id = metadata["campaign_id"]
    if not isinstance(campaign_id, str) or not _CAMPAIGN_ID_PATTERN.match(campaign_id):
        raise ComparisonError(
            f"metadata {metadata_path} campaign-id {campaign_id!r} is not a "
            "portable directory name"
        )
    if not isinstance(metadata["image"], str):
        raise ComparisonError(f"metadata {metadata_path} image must be a string")
    parameters = metadata["campaign_parameters"]
    if not isinstance(parameters, dict):
        raise ComparisonError(
            f"metadata {metadata_path} campaign_parameters must be a JSON object"
        )
    try:
        json.dumps(parameters, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ComparisonError(
            f"metadata {metadata_path} campaign_parameters must be standard JSON: {exc}"
        ) from exc
    return metadata


def iter_v1_rows(csv_path: Path, model: str) -> Iterator[dict[str, Any]]:
    """Stream decoded v1 rows, enforcing the exact header and canonical order."""
    columns = columns_for_model(model)
    with open(csv_path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != list(columns):
            raise ComparisonError(
                f"{csv_path} header {reader.fieldnames!r} does not match the "
                f"contract columns {list(columns)!r}"
            )
        previous_key: tuple[str, str] | None = None
        for line_number, raw in enumerate(reader, start=2):
            if None in raw or any(value is None for value in raw.values()):
                raise ComparisonError(
                    f"{csv_path} line {line_number}: column mismatch against the header"
                )
            row = decode_v1_row(model, raw)
            key = canonical_key(row)
            if previous_key is not None and key <= previous_key:
                raise ComparisonError(
                    f"{csv_path} line {line_number}: canonical key {key} is not "
                    f"strictly greater than the previous one {previous_key} — the "
                    "export must be canonically ordered and duplicate-free"
                )
            previous_key = key
            yield row


def merge_join(
    rows_1: Iterator[dict[str, Any]], rows_2: Iterator[dict[str, Any]]
) -> Iterator[tuple[str, dict[str, Any] | None, dict[str, Any] | None]]:
    """Stream a merge join on :func:`canonical_key` (bounded memory).

    Yields ``(status, row_1_or_None, row_2_or_None)`` where status is one of
    ``joined``, ``missing_source_2``, ``missing_source_1``.
    """
    stream_1, stream_2 = iter(rows_1), iter(rows_2)
    current_1 = next(stream_1, None)
    current_2 = next(stream_2, None)
    while current_1 is not None or current_2 is not None:
        if current_2 is None:
            yield "missing_source_2", current_1, None
            current_1 = next(stream_1, None)
        elif current_1 is None:
            yield "missing_source_1", None, current_2
            current_2 = next(stream_2, None)
        else:
            key_1 = canonical_key(current_1)
            key_2 = canonical_key(current_2)
            if key_1 == key_2:
                yield "joined", current_1, current_2
                current_1 = next(stream_1, None)
                current_2 = next(stream_2, None)
            elif key_1 < key_2:
                yield "missing_source_2", current_1, None
                current_1 = next(stream_1, None)
            else:
                yield "missing_source_1", None, current_2
                current_2 = next(stream_2, None)


def _encode_report_value(value: Any) -> str:
    """Encode a source/metric value with the canonical v1 cell grammar."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        if math.isnan(float(value)):
            return "NaN"
        if math.isinf(float(value)):
            return "Infinity" if float(value) > 0 else "-Infinity"
        return repr(float(value))
    return str(value)


def _format_metric(value: float | None) -> str:
    """Encode a diff metric with the canonical v1 grammar (``None`` → empty)."""
    return _encode_report_value(value)


def timestamp_offset_seconds(value_1: Any, value_2: Any) -> float | None:
    """Return ``source-2 - source-1`` in seconds when both timestamps parse.

    Timestamps document when each machine processed an event.  They are not a
    model result: two independently run campaigns naturally have different
    wall-clock timestamps.  A non-parseable timestamp is still retained in the
    report, but has no numeric offset.
    """
    if value_1 is None or value_2 is None:
        return None
    try:
        timestamp_1 = datetime.fromisoformat(str(value_1))
        timestamp_2 = datetime.fromisoformat(str(value_2))
        # Python cannot subtract an aware from a naive datetime.  Treat this
        # as a diagnostic format mismatch, not as a model divergence.
        if (timestamp_1.tzinfo is None) != (timestamp_2.tzinfo is None):
            return None
        return (timestamp_2 - timestamp_1).total_seconds()
    except ValueError:
        return None


def build_report_row(
    model: str,
    status: str,
    row_1: Mapping[str, Any] | None,
    row_2: Mapping[str, Any] | None,
    abs_tolerance: float,
    relative_tolerance: float,
) -> dict[str, Any]:
    """Build one report row for any comparison status (pure, testable).

    The status must be one of :data:`STATUSES`. Missing sides carry empty
    values and empty metrics; joined rows carry per-field source values,
    ``*_abs_diff`` / ``*_relative_diff`` for every numeric column and the
    per-event ``max_abs_diff`` / ``max_relative_diff``.
    """
    if status not in STATUSES:
        raise ComparisonError(f"unknown comparison status: {status!r}")
    columns = columns_for_model(model)
    numeric_columns = [column for column in columns if column in NUMERIC_COLUMNS]
    text_columns = [
        column
        for column in columns
        if column not in NUMERIC_COLUMNS
        and column not in KEY_COLUMNS
        and column != TIMESTAMP_COLUMN
    ]

    row_out: dict[str, Any] = {
        "Group": (row_1 or row_2)["Group"],
        "source_event_id": (row_1 or row_2)["source_event_id"],
        "comparison_status": status,
        "exact": 1 if status == "exact" else 0,
        # Hierarchical flag: an exact row is trivially within tolerance, while
        # the status and the summary counter only label tolerated non-exact
        # rows so the summary categories stay disjoint.
        "within_tolerance": 1 if status in ("exact", "within_tolerance") else 0,
    }

    if status in ("missing_source_1", "missing_source_2"):
        present = row_1 if status == "missing_source_2" else row_2
        row_out["timestamp_equal"] = ""
        row_out["timestamp_offset_seconds"] = ""
        row_out["differing_fields"] = ""
        row_out["max_abs_diff"] = ""
        row_out["max_relative_diff"] = ""
        left_timestamp = present[TIMESTAMP_COLUMN] if status == "missing_source_2" else None
        right_timestamp = None if status == "missing_source_2" else present[TIMESTAMP_COLUMN]
        row_out["timestamp_source_1"] = _encode_report_value(left_timestamp)
        row_out["timestamp_source_2"] = _encode_report_value(right_timestamp)
        for column in text_columns:
            left = present[column] if status == "missing_source_2" else None
            right = None if status == "missing_source_2" else present[column]
            row_out[f"{column}_source_1"] = _encode_report_value(left)
            row_out[f"{column}_source_2"] = _encode_report_value(right)
        for column in numeric_columns:
            left = present[column] if status == "missing_source_2" else None
            right = None if status == "missing_source_2" else present[column]
            row_out[f"{column}_source_1"] = _encode_report_value(left)
            row_out[f"{column}_source_2"] = _encode_report_value(right)
            row_out[f"{column}_abs_diff"] = ""
            row_out[f"{column}_relative_diff"] = ""
        return row_out

    # Joined rows: timestamp is diagnostic only.  It captures the elapsed time
    # between the two executions without making an otherwise identical model
    # output divergent.
    timestamp_1 = row_1[TIMESTAMP_COLUMN]
    timestamp_2 = row_2[TIMESTAMP_COLUMN]
    row_out["timestamp_equal"] = 1 if values_equal(timestamp_1, timestamp_2) else 0
    row_out["timestamp_offset_seconds"] = _encode_report_value(
        timestamp_offset_seconds(timestamp_1, timestamp_2)
    )
    row_out["timestamp_source_1"] = _encode_report_value(timestamp_1)
    row_out["timestamp_source_2"] = _encode_report_value(timestamp_2)

    # Compare every model-result column, in canonical column order.
    differing: list[str] = []
    max_abs: float | None = None
    max_rel: float | None = None
    all_differences_tolerated = True
    has_nontolerable_divergence = False
    for column in columns:
        if column in KEY_COLUMNS or column == TIMESTAMP_COLUMN:
            continue
        value_1 = row_1[column]
        value_2 = row_2[column]
        if column in numeric_columns:
            row_out[f"{column}_source_1"] = _encode_report_value(value_1)
            row_out[f"{column}_source_2"] = _encode_report_value(value_2)
            if values_equal(value_1, value_2):
                abs_diff, relative_diff = 0.0, 0.0
            elif _is_finite_number(value_1) and _is_finite_number(value_2):
                tolerated, abs_diff, relative_diff = compare_numeric_pair(
                    value_1, value_2, abs_tolerance, relative_tolerance
                )
                differing.append(column)
                if not tolerated:
                    all_differences_tolerated = False
            else:
                # None/NaN/infinite divergence: never tolerated, no metrics.
                differing.append(column)
                has_nontolerable_divergence = True
                abs_diff, relative_diff = None, None
            row_out[f"{column}_abs_diff"] = _encode_report_value(abs_diff)
            row_out[f"{column}_relative_diff"] = _encode_report_value(relative_diff)
            if abs_diff is not None:
                max_abs = abs_diff if max_abs is None else max(max_abs, abs_diff)
            if relative_diff is not None:
                max_rel = relative_diff if max_rel is None else max(max_rel, relative_diff)
        else:
            row_out[f"{column}_source_1"] = _encode_report_value(value_1)
            row_out[f"{column}_source_2"] = _encode_report_value(value_2)
            if not values_equal(value_1, value_2):
                differing.append(column)
                has_nontolerable_divergence = True

    row_out["differing_fields"] = ",".join(differing)
    row_out["max_abs_diff"] = _encode_report_value(max_abs)
    row_out["max_relative_diff"] = _encode_report_value(max_rel)
    if not differing:
        row_out["comparison_status"] = "exact"
    elif not has_nontolerable_divergence and all_differences_tolerated:
        row_out["comparison_status"] = "within_tolerance"
    else:
        row_out["comparison_status"] = "divergent"
    # The flags always follow the recomputed status: exact implies
    # within_tolerance, and a divergent row carries neither flag.
    row_out["exact"] = 1 if row_out["comparison_status"] == "exact" else 0
    row_out["within_tolerance"] = (
        1 if row_out["comparison_status"] in ("exact", "within_tolerance") else 0
    )
    return row_out


def build_summary(
    *,
    model: str,
    campaign_id: str,
    metadata_1: Mapping[str, Any],
    metadata_2: Mapping[str, Any],
    config: ComparisonConfig,
    counters: Mapping[str, int],
    first_divergence: Mapping[str, Any] | None,
    first_missing_source_1: Mapping[str, Any] | None,
    first_missing_source_2: Mapping[str, Any] | None,
    max_abs_diff: float | None,
    max_relative_diff: float | None,
) -> dict[str, Any]:
    """Assemble the summary dictionary (pure)."""
    return {
        "format_version": FORMAT_VERSION,
        "model": model,
        "campaign_id": campaign_id,
        "source_1": {"csv": str(config.source_1), "image": metadata_1["image"]},
        "source_2": {"csv": str(config.source_2), "image": metadata_2["image"]},
        "abs_tolerance": config.abs_tolerance,
        "relative_tolerance": config.relative_tolerance,
        "compared": counters["compared"],
        "exact": counters["exact"],
        "within_tolerance": counters["within_tolerance"],
        "divergent": counters["divergent"],
        "missing_source_1": counters["missing_source_1"],
        "missing_source_2": counters["missing_source_2"],
        # These keys are deliberately canonical event identifiers rather than
        # physical CSV line numbers: they remain meaningful after a sorted
        # export is regenerated, and take the user directly to the report row.
        "first_divergence": first_divergence,
        "first_missing_source_1": first_missing_source_1,
        "first_missing_source_2": first_missing_source_2,
        "max_abs_diff": max_abs_diff,
        "max_relative_diff": max_relative_diff,
    }


# ---------------------------------------------------------------------------
# CLI and I/O
# ---------------------------------------------------------------------------


def _finite_non_negative_float(text: str) -> float:
    try:
        value = float(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{text!r} is not a float") from exc
    if math.isnan(value) or math.isinf(value) or value < 0.0:
        raise argparse.ArgumentTypeError(
            f"{text!r} must be a finite float greater than or equal to zero"
        )
    return value


def build_config(argv: list[str] | None = None) -> ComparisonConfig:
    """Parse and validate the CLI arguments into a configuration."""
    parser = argparse.ArgumentParser(
        prog="reproducibility.compare_csv",
        description=(
            "Compare two reproducibility CSV v1 exports with a streaming merge "
            "join on (Group, source_event_id) and publish a diagnostic report "
            "plus a JSON summary."
        ),
    )
    parser.add_argument("--source-1", required=True, type=Path, help="CSV of source-1")
    parser.add_argument("--source-2", required=True, type=Path, help="CSV of source-2")
    parser.add_argument("--report", required=True, type=Path, help="comparison report CSV to write")
    parser.add_argument(
        "--summary",
        type=Path,
        default=None,
        help="summary JSON path (default: <report>.summary.json)",
    )
    parser.add_argument(
        "--abs-tolerance",
        type=_finite_non_negative_float,
        default=0.0,
        help="absolute tolerance for finite numeric pairs (default: 0.0)",
    )
    parser.add_argument(
        "--relative-tolerance",
        type=_finite_non_negative_float,
        default=0.0,
        help="relative tolerance for finite numeric pairs (default: 0.0)",
    )
    args = parser.parse_args(argv)

    summary = args.summary if args.summary is not None else args.report.with_name(
        args.report.name + ".summary.json"
    )
    source_1 = args.source_1.resolve()
    source_2 = args.source_2.resolve()
    report = args.report.resolve()
    summary = summary.resolve()
    if report == summary:
        raise ComparisonError("--report and --summary must be different paths")
    for source in (source_1, source_2):
        if report == source:
            raise ComparisonError(f"--report must differ from the input {source}")
        if summary == source:
            raise ComparisonError(f"--summary must differ from the input {source}")
    for path in (report, summary):
        if path.exists():
            raise ComparisonError(
                f"refusing to overwrite an existing file: {path} — remove it "
                "explicitly before comparing again"
            )
    return ComparisonConfig(
        source_1=source_1,
        source_2=source_2,
        report=report,
        summary=summary,
        abs_tolerance=args.abs_tolerance,
        relative_tolerance=args.relative_tolerance,
    )


def run(config: ComparisonConfig) -> dict[str, Any]:
    """Run the comparison and publish report then summary; returns the summary."""
    for path in (config.report, config.summary):
        if path.exists():
            raise ComparisonError(
                f"refusing to overwrite an existing file: {path}"
            )
    metadata_1 = validate_metadata(config.source_1, "source-1")
    metadata_2 = validate_metadata(config.source_2, "source-2")
    if metadata_1["model"] != metadata_2["model"]:
        raise ComparisonError(
            "metadata models differ: "
            f"{metadata_1['model']!r} vs {metadata_2['model']!r}"
        )
    model = metadata_1["model"]
    if metadata_1["campaign_id"] != metadata_2["campaign_id"]:
        raise ComparisonError(
            "metadata campaign-ids differ: "
            f"{metadata_1['campaign_id']!r} vs {metadata_2['campaign_id']!r}"
        )
    if metadata_1["campaign_parameters"] != metadata_2["campaign_parameters"]:
        raise ComparisonError(
            "metadata campaign_parameters differ between source-1 and source-2"
        )

    report_columns = _report_columns(model)
    counters = {
        "compared": 0,
        "exact": 0,
        "within_tolerance": 0,
        "divergent": 0,
        "missing_source_1": 0,
        "missing_source_2": 0,
    }
    max_abs_diff: float | None = None
    max_relative_diff: float | None = None
    first_divergence: dict[str, Any] | None = None
    first_missing_source_1: dict[str, Any] | None = None
    first_missing_source_2: dict[str, Any] | None = None

    report_tmp = config.report.with_name(config.report.name + ".tmp")
    summary_tmp = config.summary.with_name(config.summary.name + ".tmp")
    report_published = False
    try:
        with open(report_tmp, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=report_columns, extrasaction="raise")
            writer.writeheader()
            joined = merge_join(
                iter_v1_rows(config.source_1, model), iter_v1_rows(config.source_2, model)
            )
            for status, row_1, row_2 in joined:
                if status == "joined":
                    report_row = build_report_row(
                        model,
                        "exact",
                        row_1,
                        row_2,
                        config.abs_tolerance,
                        config.relative_tolerance,
                    )
                    status = report_row["comparison_status"]
                    counters["exact"] += 1 if status == "exact" else 0
                    counters["within_tolerance"] += 1 if status == "within_tolerance" else 0
                    counters["divergent"] += 1 if status == "divergent" else 0
                    counters["compared"] += 1
                    if status == "divergent" and first_divergence is None:
                        first_divergence = {
                            "Group": report_row["Group"],
                            "source_event_id": report_row["source_event_id"],
                            "differing_fields": report_row["differing_fields"].split(","),
                        }
                elif status == "missing_source_2":
                    report_row = build_report_row(model, "missing_source_2", row_1, None, 0.0, 0.0)
                    counters["missing_source_2"] += 1
                    if first_missing_source_2 is None:
                        first_missing_source_2 = {
                            "Group": report_row["Group"],
                            "source_event_id": report_row["source_event_id"],
                        }
                else:
                    report_row = build_report_row(model, "missing_source_1", None, row_2, 0.0, 0.0)
                    counters["missing_source_1"] += 1
                    if first_missing_source_1 is None:
                        first_missing_source_1 = {
                            "Group": report_row["Group"],
                            "source_event_id": report_row["source_event_id"],
                        }
                writer.writerow(report_row)
                row_max_abs = report_row["max_abs_diff"]
                row_max_rel = report_row["max_relative_diff"]
                if row_max_abs != "":
                    value = float(row_max_abs)
                    max_abs_diff = value if max_abs_diff is None else max(max_abs_diff, value)
                if row_max_rel != "":
                    value = float(row_max_rel)
                    max_relative_diff = value if max_relative_diff is None else max(max_relative_diff, value)

        summary = build_summary(
            model=model,
            campaign_id=metadata_1["campaign_id"],
            metadata_1=metadata_1,
            metadata_2=metadata_2,
            config=config,
            counters=counters,
            first_divergence=first_divergence,
            first_missing_source_1=first_missing_source_1,
            first_missing_source_2=first_missing_source_2,
            max_abs_diff=max_abs_diff,
            max_relative_diff=max_relative_diff,
        )
        summary_tmp.write_text(
            json.dumps(summary, ensure_ascii=False, sort_keys=True, allow_nan=False, indent=2) + "\n",
            encoding="utf-8",
        )
        report_tmp.replace(config.report)
        report_published = True
        summary_tmp.replace(config.summary)
        return summary
    except Exception:
        # Never leave a final or temporary output behind after a failure. When
        # the report was already published, remove it too: the summary is the
        # success marker, and a report alone must never survive an error.
        report_tmp.unlink(missing_ok=True)
        summary_tmp.unlink(missing_ok=True)
        if report_published:
            config.report.unlink(missing_ok=True)
        raise


def _report_columns(model: str) -> list[str]:
    columns = columns_for_model(model)
    numeric_columns = [column for column in columns if column in NUMERIC_COLUMNS]
    text_columns = [
        column
        for column in columns
        if column not in NUMERIC_COLUMNS
        and column not in KEY_COLUMNS
        and column != TIMESTAMP_COLUMN
    ]
    report_columns = ["Group", "source_event_id"]
    report_columns += [
        "comparison_status", "exact", "within_tolerance",
        "timestamp_equal", "timestamp_offset_seconds", "differing_fields",
        "max_abs_diff", "max_relative_diff",
    ]
    report_columns += ["timestamp_source_1", "timestamp_source_2"]
    for column in text_columns:
        report_columns += [f"{column}_source_1", f"{column}_source_2"]
    for column in numeric_columns:
        report_columns += [
            f"{column}_source_1",
            f"{column}_source_2",
            f"{column}_abs_diff",
            f"{column}_relative_diff",
        ]
    return report_columns


def main(argv: list[str] | None = None) -> int:
    try:
        config = build_config(argv)
    except SystemExit as exc:  # argparse already explained the CLI error
        return int(exc.code) if isinstance(exc.code, int) else 2
    try:
        run(config)
    except Exception as exc:
        print(f"[reproducibility-compare] error: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
