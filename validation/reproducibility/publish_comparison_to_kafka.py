#!/usr/bin/env python3
"""
publish_comparison_to_kafka.py

Manual observability publisher (P2-7). After a successful comparison produced
by validation/reproducibility/compare_csv.py, it streams the report rows as flat JSON
events to the dedicated ``reproducibility-diff`` topic and publishes one
summary event on ``reproducibility-summary``. The comparison itself is never
replayed and no file is modified: the local CSV and its summary remain the
proofs.

Direct execution from the repository root:

    python validation/reproducibility/publish_comparison_to_kafka.py \\
      --report data/reproducibility/<campaign-id>/<model>-comparison.csv

Or as a manual one-shot Compose service (see docker-compose.yml):

    docker compose run --rm --no-deps reproducibility-publish \\
      --report /app/data/reproducibility/<campaign-id>/<model>-comparison.csv

The ``confluent_kafka`` import is deferred until an actual publication so the
module imports cleanly without a broker or any Kafka dependency.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from validation.reproducibility.compare_csv import _report_columns
    from validation.reproducibility.csv_contract import FORMAT_VERSION, columns_for_model
else:
    from .compare_csv import _report_columns
    from .csv_contract import FORMAT_VERSION, columns_for_model

SUMMARY_FORMAT_VERSION = 1
STATUS_KEYS = ("exact", "within_tolerance", "divergent", "missing_source_1", "missing_source_2")
SUMMARY_KEYS = (
    "format_version",
    "model",
    "campaign_id",
    "source_1",
    "source_2",
    "abs_tolerance",
    "relative_tolerance",
    "compared",
    "exact",
    "within_tolerance",
    "divergent",
    "missing_source_1",
    "missing_source_2",
    "first_divergence",
    "first_missing_source_1",
    "first_missing_source_2",
    "max_abs_diff",
    "max_relative_diff",
)
COUNTER_KEYS = ("compared", "exact", "within_tolerance", "divergent", "missing_source_1", "missing_source_2")
FIRST_MARKER_KEYS = ("first_divergence", "first_missing_source_1", "first_missing_source_2")
TEXT_COLUMNS = ("status", "anomaly", "timestamp")  # per-side report columns derive from these
_ANOMALY_CELLS = {"true": 1, "false": 0}
_FLAG_CELLS = {"0": 0, "1": 1}
_CAMPAIGN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
# Bounded delivery: a flush that cannot complete within this delay is fatal.
FLUSH_TIMEOUT_SECONDS = 30.0


class PublishError(Exception):
    """Fatal validation, serialization or delivery error of a publication."""


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _publication_timestamp() -> str:
    """UTC instant of publication, formatted ``YYYY-MM-DD HH:MM:SS``."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_timestamp(text: str) -> datetime | None:
    """Parse a report timestamp cell exactly like the comparator does.

    ``None`` when absent or uninterpretable (``datetime.fromisoformat``, the
    comparator's own parsing).
    """
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _decode_number(cell: str) -> float | None:
    """Canonical v1 numeric cell → finite JSON number or ``null``."""
    if cell == "":
        return None
    if cell == "NaN" or cell == "Infinity" or cell == "-Infinity":
        return None  # never emit non-standard JSON: non-finite becomes null
    try:
        value = float(cell)
    except ValueError as exc:
        raise PublishError(f"invalid numeric cell {cell!r} in the report") from exc
    return value


def _decode_anomaly(cell: str) -> int | None:
    """``anomaly_source_*`` cell → ``0``/``1``/``null``, never a boolean."""
    if cell == "":
        return None
    try:
        return _ANOMALY_CELLS[cell]
    except KeyError as exc:
        raise PublishError(f"invalid anomaly cell {cell!r} in the report") from exc


def _reject_json_constant(token: str) -> Any:
    raise PublishError(f"non-standard JSON constant {token!r} in the summary")


def load_summary(summary_path: Path) -> dict[str, Any]:
    """Load and strictly validate the comparison summary JSON."""
    if not summary_path.exists():
        raise PublishError(f"summary file not found: {summary_path}")
    try:
        text = summary_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise PublishError(f"summary {summary_path} is not valid UTF-8: {exc}") from exc
    try:
        summary = json.loads(text, parse_constant=_reject_json_constant)
    except json.JSONDecodeError as exc:
        raise PublishError(f"summary {summary_path} is not valid JSON: {exc}") from exc
    if not isinstance(summary, dict):
        raise PublishError(f"summary {summary_path} must be a JSON object")
    unknown = sorted(set(summary) - set(SUMMARY_KEYS))
    missing = sorted(set(SUMMARY_KEYS) - set(summary))
    if unknown or missing:
        raise PublishError(
            f"summary {summary_path} key mismatch — unknown: {unknown}, missing: {missing}"
        )
    if summary["format_version"] != SUMMARY_FORMAT_VERSION:
        raise PublishError(
            f"summary {summary_path} has format_version {summary['format_version']!r}, "
            f"expected {SUMMARY_FORMAT_VERSION}"
        )
    model = summary["model"]
    try:
        columns_for_model(model)
    except ValueError as exc:
        raise PublishError(f"summary {summary_path}: {exc}") from exc
    campaign_id = summary["campaign_id"]
    if not isinstance(campaign_id, str) or not _CAMPAIGN_ID_PATTERN.match(campaign_id):
        raise PublishError(
            f"summary {summary_path} campaign-id {campaign_id!r} is not a portable name"
        )
    for key in COUNTER_KEYS:
        if not isinstance(summary[key], int) or isinstance(summary[key], bool):
            raise PublishError(f"summary {summary_path} counter {key!r} must be an integer")
    if summary["compared"] != summary["exact"] + summary["within_tolerance"] + summary["divergent"]:
        raise PublishError(
            f"summary {summary_path} counters are inconsistent: compared "
            f"({summary['compared']}) != exact + within_tolerance + divergent"
        )
    for side_key in ("source_1", "source_2"):
        side = summary[side_key]
        if not isinstance(side, dict) or set(side) != {"csv", "image"}:
            raise PublishError(
                f"summary {summary_path} {side_key} must contain exactly 'csv' and 'image'"
            )
        if not isinstance(side["image"], str):
            raise PublishError(f"summary {summary_path} {side_key} image must be a string")
    for key in ("first_divergence", "first_missing_source_1", "first_missing_source_2"):
        marker = summary[key]
        if marker is None:
            continue
        expected = (
            {"Group", "source_event_id", "differing_fields"}
            if key == "first_divergence"
            else {"Group", "source_event_id"}
        )
        if not isinstance(marker, dict) or set(marker) != expected:
            raise PublishError(
                f"summary {summary_path} diagnostic marker {key!r} must be an object "
                f"with exactly the keys {sorted(expected)}, or null"
            )
        if key == "first_divergence" and not isinstance(marker["differing_fields"], list):
            raise PublishError(
                f"summary {summary_path} marker {key!r} differing_fields must be a list"
            )
    return summary


def _event_key(summary: dict[str, Any], row: dict[str, str]) -> str:
    """Deterministic UTF-8 Kafka key of a detailed event."""
    return "|".join((summary["campaign_id"], summary["model"], row["Group"], row["source_event_id"]))


def _summary_event_key(summary: dict[str, Any]) -> str:
    return f"{summary['campaign_id']}|{summary['model']}|summary"


def _pick_timestamp(row: dict[str, str]) -> tuple[str, str]:
    """Return ``(timestamp, timestamp_origin)`` for one report row.

    ``timestamp`` reprend ``timestamp_source_1``, sinon ``timestamp_source_2``:
    only interpretable cells are eligible (the comparator's
    ``datetime.fromisoformat`` parsing). The fallback uses the publication
    instant when both sides are absent or uninterpretable. The chosen value is
    never trimmed.
    """
    candidates = (
        ("source-1", row["timestamp_source_1"]),
        ("source-2", row["timestamp_source_2"]),
    )
    for origin, cell in candidates:
        if cell != "" and _parse_timestamp(cell) is not None:
            return cell, origin
    return _publication_timestamp(), "publication"


def _decode_flag(cell: str) -> int | None:
    """Report flag cell (``timestamp_equal``) → ``0``/``1``/``null``."""
    if cell == "":
        return None
    if cell in _FLAG_CELLS:
        return _FLAG_CELLS[cell]
    raise PublishError(f"invalid flag cell {cell!r} in the report")


def build_diff_event(summary: dict[str, Any], row: dict[str, str], model: str) -> dict[str, Any]:
    """Project one report row onto its flat JSON event (pure, testable).

    Numeric v1 cells (NaN and infinities included) become a finite JSON number
    or ``null``; empty metrics become ``null``; ``anomaly_source_*`` becomes
    ``0``/``1``/``null``. Identifiers, timestamps, statuses and
    ``differing_fields`` stay strings without trim. ``timestamp_equal`` and
    ``timestamp_offset_seconds`` are copied verbatim from the report (the
    comparator's convention: source-2 − source-1, positive meaning source-2 is
    later); they are never recomputed nor sign-inverted.
    """
    event: dict[str, Any] = {}
    timestamp, origin = _pick_timestamp(row)
    event["timestamp"] = timestamp
    event["timestamp_origin"] = origin
    event["campaign_id"] = summary["campaign_id"]
    event["model"] = model
    event["Group"] = row["Group"]
    event["source_event_id"] = row["source_event_id"]
    event["comparison_status"] = row["comparison_status"]
    event["exact"] = int(row["exact"])
    event["within_tolerance"] = int(row["within_tolerance"])
    event["differing_fields"] = row["differing_fields"]

    cell_1, cell_2 = row["timestamp_source_1"], row["timestamp_source_2"]
    # Both diagnostic cells are copied from the report, never recomputed.
    event["timestamp_equal"] = _decode_flag(row["timestamp_equal"])
    event["timestamp_offset_seconds"] = _decode_number(row["timestamp_offset_seconds"])
    event["timestamp_source_1"] = cell_1 if cell_1 != "" else None
    event["timestamp_source_2"] = cell_2 if cell_2 != "" else None

    event["max_abs_diff"] = _decode_number(row["max_abs_diff"])
    event["max_relative_diff"] = _decode_number(row["max_relative_diff"])

    for column in ("score", "threshold"):
        event[f"{column}_source_1"] = _decode_number(row[f"{column}_source_1"])
        event[f"{column}_source_2"] = _decode_number(row[f"{column}_source_2"])
        event[f"{column}_abs_diff"] = _decode_number(row[f"{column}_abs_diff"])
        event[f"{column}_relative_diff"] = _decode_number(row[f"{column}_relative_diff"])
    for suffix in ("source_1", "source_2"):
        cell = row[f"status_{suffix}"]
        event[f"status_{suffix}"] = cell if cell != "" else None
        event[f"anomaly_{suffix}"] = _decode_anomaly(row[f"anomaly_{suffix}"])

    # mDragPoint extra: present if and only if the report owns the columns.
    if "mdragpoint_internal_r_source_1" in row:
        event["mdragpoint_internal_r_source_1"] = _decode_number(row["mdragpoint_internal_r_source_1"])
        event["mdragpoint_internal_r_source_2"] = _decode_number(row["mdragpoint_internal_r_source_2"])
        event["mdragpoint_internal_r_abs_diff"] = _decode_number(row["mdragpoint_internal_r_abs_diff"])
        event["mdragpoint_internal_r_relative_diff"] = _decode_number(
            row["mdragpoint_internal_r_relative_diff"]
        )
    return event


def build_summary_event(summary: dict[str, Any]) -> dict[str, Any]:
    """Assemble the single summary event (pure, testable).

    The comparator already carries the first divergent/missing markers in the
    summary; the event flattens them. ``differing_fields`` is a list in the
    summary and becomes a comma-joined string. An absence is ``null``, never
    the string ``"null"``; local CSV paths are never published.
    """
    event: dict[str, Any] = {
        "timestamp": _publication_timestamp(),
        "campaign_id": summary["campaign_id"],
        "model": summary["model"],
        "abs_tolerance": summary["abs_tolerance"],
        "relative_tolerance": summary["relative_tolerance"],
    }
    for key in COUNTER_KEYS:
        event[key] = summary[key]
    event["max_abs_diff"] = summary["max_abs_diff"]
    event["max_relative_diff"] = summary["max_relative_diff"]
    event["source_1_image"] = summary["source_1"]["image"]
    event["source_2_image"] = summary["source_2"]["image"]
    first_divergence = summary["first_divergence"]
    event["first_divergence_group"] = (
        first_divergence["Group"] if first_divergence is not None else None
    )
    event["first_divergence_source_event_id"] = (
        first_divergence["source_event_id"] if first_divergence is not None else None
    )
    event["first_divergence_fields"] = (
        ",".join(first_divergence["differing_fields"])
        if first_divergence is not None
        else None
    )
    for suffix, key in (
        ("1", "first_missing_source_1"),
        ("2", "first_missing_source_2"),
    ):
        marker = summary[key]
        event[f"first_missing_source_{suffix}_group"] = (
            marker["Group"] if marker is not None else None
        )
        event[f"first_missing_source_{suffix}_source_event_id"] = (
            marker["source_event_id"] if marker is not None else None
        )
    return event




def build_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="reproducibility.publish_comparison_to_kafka",
        description=(
            "Publish an existing comparison report (CSV rows + JSON summary) to "
            "the dedicated reproducibility-diff and reproducibility-summary "
            "topics. The comparison is never replayed."
        ),
    )
    parser.add_argument(
        "--report", required=True, type=Path,
        help="comparison report CSV to publish",
    )
    parser.add_argument(
        "--summary", type=Path, default=None,
        help="summary JSON path (default: <report>.summary.json)",
    )
    parser.add_argument(
        "--bootstrap-servers", default="kafka1:9092,kafka2:9092,kafka3:9092",
        help="comma-separated Kafka bootstrap servers",
    )
    parser.add_argument("--diff-topic", default="reproducibility-diff")
    parser.add_argument("--summary-topic", default="reproducibility-summary")
    return parser.parse_args(argv)


def _iter_report_rows(report_path: Path, model: str) -> Iterator[dict[str, str]]:
    """Stream the report rows (bounded memory: one row held at a time)."""
    with open(report_path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != _report_columns(model):
            raise PublishError(
                f"{report_path} header {reader.fieldnames!r} does not match the "
                f"contract columns {_report_columns(model)!r} for model {model!r}"
            )
        for line_number, row in enumerate(reader, start=2):
            if None in row or any(value is None for value in row.values()):
                raise PublishError(
                    f"{report_path} line {line_number}: column mismatch against the header"
                )
            yield row


def _validate_report_stream(
    report_path: Path, model: str, summary: dict[str, Any]
) -> int:
    """First streaming pass: header, per-row columns, counters and markers.

    Counts every status in the report and compares it with the summary
    counters, then verifies that the summary's first divergent/missing markers
    match the report's first occurrences. Returns the total row count.
    """
    if not report_path.exists():
        raise PublishError(f"report file not found: {report_path}")
    status_counts = {status: 0 for status in STATUS_KEYS}
    scanned: dict[str, dict[str, Any] | None] = {key: None for key in FIRST_MARKER_KEYS}
    total = 0
    for row in _iter_report_rows(report_path, model):
        total += 1
        status = row["comparison_status"]
        if status not in status_counts:
            raise PublishError(
                f"{report_path} line {total + 1}: unknown comparison_status {status!r}"
            )
        status_counts[status] += 1
        if status == "divergent" and scanned["first_divergence"] is None:
            scanned["first_divergence"] = {
                "Group": row["Group"],
                "source_event_id": row["source_event_id"],
                "differing_fields": (
                    row["differing_fields"].split(",") if row["differing_fields"] else []
                ),
            }
        elif status == "missing_source_1" and scanned["first_missing_source_1"] is None:
            scanned["first_missing_source_1"] = {
                "Group": row["Group"],
                "source_event_id": row["source_event_id"],
            }
        elif status == "missing_source_2" and scanned["first_missing_source_2"] is None:
            scanned["first_missing_source_2"] = {
                "Group": row["Group"],
                "source_event_id": row["source_event_id"],
            }
    expected_total = (
        summary["compared"] + summary["missing_source_1"] + summary["missing_source_2"]
    )
    if total != expected_total:
        raise PublishError(
            f"{report_path} holds {total} rows but the summary counters account for "
            f"{expected_total}"
        )
    for status, count in status_counts.items():
        if count != summary[status]:
            raise PublishError(
                f"{report_path} reports {count} {status} row(s) but the summary "
                f"counts {summary[status]}"
            )
    for key, scanned_marker in scanned.items():
        summary_marker = summary[key]
        if summary_marker != scanned_marker:
            raise PublishError(
                f"summary first-marker {key!r} ({summary_marker}) does not match the "
                f"report's first occurrence ({scanned_marker})"
            )
    return total


def publish(
    report_path: Path,
    summary_path: Path,
    *,
    bootstrap_servers: str,
    diff_topic: str,
    summary_topic: str,
    producer_factory: Any = None,
) -> int:
    """Validate then publish the report; returns the number of diff events.

    The report is streamed twice with bounded memory: a first validation pass
    (header, columns, counters, first markers), then the publication pass.
    ``producer_factory`` is a test seam: when given, it replaces the
    ``confluent_kafka.Producer`` construction (the import stays deferred).
    """
    if report_path == summary_path:
        raise PublishError("--report and --summary must be different paths")
    summary = load_summary(summary_path)
    model = summary["model"]
    _validate_report_stream(report_path, model, summary)
    # Local CSV paths are evidence locations: they are never published.
    _ = (summary["source_1"]["csv"], summary["source_2"]["csv"])

    if producer_factory is None:
        # Deferred import: the module stays importable without confluent_kafka.
        import confluent_kafka

        def producer_factory(config: dict[str, Any]) -> Any:
            return confluent_kafka.Producer(config)

    producer_config = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 1,
    }
    producer = producer_factory(producer_config)

    delivery_errors: list[str] = []

    def _delivery_callback(err: Any, _msg: Any) -> None:
        if err is not None:
            delivery_errors.append(f"{diff_topic}: delivery failed: {err}")

    event_count = 0
    for line_number, row in enumerate(_iter_report_rows(report_path, model), start=2):
        try:
            event = build_diff_event(summary, row, model)
            payload = json.dumps(event, ensure_ascii=False, sort_keys=True, allow_nan=False)
        except Exception as exc:
            raise PublishError(
                f"{report_path} line {line_number}: cannot serialize the event: {exc}"
            ) from exc
        try:
            producer.produce(
                diff_topic,
                key=_event_key(summary, row),
                value=payload,
                on_delivery=_delivery_callback,
            )
        except BufferError as exc:  # pragma: no cover - local queue full
            producer.poll(0)
            raise PublishError(f"{diff_topic}: producer queue is full: {exc}") from exc
        event_count += 1
        producer.poll(0)

    undelivered = producer.flush(FLUSH_TIMEOUT_SECONDS)
    if undelivered:
        raise PublishError(
            f"{diff_topic}: {undelivered} message(s) still undelivered after "
            f"{FLUSH_TIMEOUT_SECONDS}s — giving up, no summary will be published"
        )
    if delivery_errors:
        raise PublishError("; ".join(delivery_errors))

    # The summary is the marker of a fully delivered batch: it is published
    # only after every detailed event has been confirmed.
    summary_event = build_summary_event(summary)
    summary_payload = json.dumps(
        summary_event, ensure_ascii=False, sort_keys=True, allow_nan=False
    )
    summary_errors: list[str] = []

    def _summary_delivery_callback(err: Any, _msg: Any) -> None:
        if err is not None:
            summary_errors.append(f"{summary_topic}: delivery failed: {err}")

    try:
        producer.produce(
            summary_topic,
            key=_summary_event_key(summary),
            value=summary_payload,
            on_delivery=_summary_delivery_callback,
        )
    except BufferError as exc:  # pragma: no cover - local queue full
        producer.poll(0)
        raise PublishError(f"{summary_topic}: producer queue is full: {exc}") from exc
    undelivered = producer.flush(FLUSH_TIMEOUT_SECONDS)
    if undelivered:
        raise PublishError(
            f"{summary_topic}: {undelivered} message(s) still undelivered after "
            f"{FLUSH_TIMEOUT_SECONDS}s"
        )
    if summary_errors:
        raise PublishError("; ".join(summary_errors))
    return event_count


def main(argv: list[str] | None = None) -> int:
    args = build_args(argv)
    report_path = args.report.resolve()
    summary_path = (
        args.summary if args.summary is not None else report_path.with_name(report_path.name + ".summary.json")
    ).resolve()
    try:
        count = publish(
            report_path,
            summary_path,
            bootstrap_servers=args.bootstrap_servers,
            diff_topic=args.diff_topic,
            summary_topic=args.summary_topic,
        )
    except Exception as exc:
        print(f"[reproducibility-publish] error: {exc}", file=sys.stderr, flush=True)
        return 1
    print(
        f"[reproducibility-publish] published {count} diff event(s) and 1 summary",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
