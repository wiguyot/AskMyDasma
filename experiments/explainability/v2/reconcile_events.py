#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Reconciliation tool for explainability-v2 outputs (OC-05).

Answers, from local exports only (no Kafka, InfluxDB, Grafana or Docker):

    "Which hstree-analysis events received a v2 explanation, with which
    status, and which ones are missing or duplicated?"

Reconciliation key: (Group, source_event_id). The timestamp never takes part
in the identity.

The core is a set of pure, directly testable functions (validation, indexing,
report); the CLI only reads the files, calls them and renders the result. A
non-zero exit code means invalid input; missing or duplicated keys are
analysis results, not errors.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dasma_kafka.explainability_v2.config import EXPLANATION_STATUSES

Key = Tuple[str, str]
RowWithLine = Tuple[int, Dict[str, str]]

KEY_GROUP = "Group"
KEY_SOURCE_EVENT_ID = "source_event_id"
KEY_HSTREE_ANOMALY = "hstree_anomaly"
KEY_EXPLANATION_STATUS = "explanation_status"

NON_ELIGIBLE_STATUSES = ("warmup", "background_not_ready", "disabled")
VERDICT_NORMAL = "normal"
VERDICT_ANOMALY = "anomaly"


class ReconciliationInputError(ValueError):
    """Invalid input row, file or format; the message carries file and line."""


@dataclass(frozen=True)
class HstreeIndex:
    """Indexed hstree-analysis export."""

    verdicts: Dict[Key, bool]
    duplicates: List[Key]
    total: int


@dataclass(frozen=True)
class V2Index:
    """Indexed explainability-v2 export (first status wins per key)."""

    statuses: Dict[Key, str]
    duplicates: List[Key]
    total: int
    status_counts: Dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _require_non_empty_str(value: Any, field_name: str, file: str, line: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReconciliationInputError(
            f"{file}:{line}: field '{field_name}' must be a non-empty string, got {value!r}"
        )
    return value.strip()


def _parse_hstree_anomaly(value: Any, file: str, line: int) -> bool:
    """Accept a JSON boolean or the exact CSV spelling 'true' / 'false'."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip() in ("true", "false"):
        return value.strip() == "true"
    raise ReconciliationInputError(
        f"{file}:{line}: field '{KEY_HSTREE_ANOMALY}' must be a JSON boolean "
        f"or the exact spelling 'true'/'false', got {value!r}"
    )


def build_hstree_key(row: Dict[str, Any], file: str, line: int) -> Key:
    """Validate one hstree row and return its (Group, source_event_id) key."""
    group = _require_non_empty_str(row.get(KEY_GROUP), KEY_GROUP, file, line)
    event_id = _require_non_empty_str(
        row.get(KEY_SOURCE_EVENT_ID), KEY_SOURCE_EVENT_ID, file, line
    )
    _parse_hstree_anomaly(row.get(KEY_HSTREE_ANOMALY), file, line)
    return (group, event_id)


def build_v2_key(row: Dict[str, Any], file: str, line: int) -> Key:
    """Validate one v2 output row and return its (Group, source_event_id) key."""
    group = _require_non_empty_str(row.get(KEY_GROUP), KEY_GROUP, file, line)
    event_id = _require_non_empty_str(
        row.get(KEY_SOURCE_EVENT_ID), KEY_SOURCE_EVENT_ID, file, line
    )
    status = row.get(KEY_EXPLANATION_STATUS)
    if isinstance(status, str):
        status = status.strip()
    if status not in EXPLANATION_STATUSES:
        raise ReconciliationInputError(
            f"{file}:{line}: field '{KEY_EXPLANATION_STATUS}' must be one of "
            f"{sorted(EXPLANATION_STATUSES)}, got {status!r}"
        )
    return (group, event_id)


# ---------------------------------------------------------------------------
# Indexing
# ---------------------------------------------------------------------------


def index_hstree_rows(rows: List[RowWithLine], source_name: str) -> HstreeIndex:
    """Index hstree rows (key -> verdict); every row is validated."""
    verdicts: Dict[Key, bool] = {}
    occurrences: Dict[Key, int] = {}
    total = 0
    for line, row in rows:
        key = build_hstree_key(row, source_name, line)
        anomaly = _parse_hstree_anomaly(row.get(KEY_HSTREE_ANOMALY), source_name, line)
        total += 1
        occurrences[key] = occurrences.get(key, 0) + 1
        if key not in verdicts:
            verdicts[key] = anomaly
    duplicates = sorted(key for key, count in occurrences.items() if count > 1)
    return HstreeIndex(verdicts=verdicts, duplicates=duplicates, total=total)


def index_v2_rows(rows: List[RowWithLine], source_name: str) -> V2Index:
    """Index v2 output rows (key -> first status read) with duplicate detection."""
    statuses: Dict[Key, str] = {}
    occurrences: Dict[Key, int] = {}
    status_counts: Dict[str, int] = {}
    total = 0
    for line, row in rows:
        key = build_v2_key(row, source_name, line)
        status = row[KEY_EXPLANATION_STATUS].strip()
        total += 1
        occurrences[key] = occurrences.get(key, 0) + 1
        status_counts[status] = status_counts.get(status, 0) + 1
        if key not in statuses:
            statuses[key] = status
    duplicates = sorted(key for key, count in occurrences.items() if count > 1)
    return V2Index(
        statuses=statuses,
        duplicates=duplicates,
        total=total,
        status_counts=status_counts,
    )


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def build_reconciliation_report(hstree: HstreeIndex, v2: V2Index) -> Dict[str, Any]:
    """Compute the full reconciliation report from the two indexes.

    Pure function: identical inputs produce an identical, fully sorted report,
    whatever the order of the rows in the exports.

    Per-verdict accounting uses only HSTree keys with a unique, reliable
    verdict. A key duplicated on the HSTree side has an ambiguous source
    verdict: it is reported in the dedicated deterministic section
    ``verdict_matrix.ambiguous_hstree`` and never bucketed as normal/anomaly,
    never counted in ``computed`` / ``failed`` / ``non_eligible`` /
    ``missing_v2`` — whatever its v2 outputs and whatever the read order.

    A key with a reliable HSTree verdict but one or more duplicated v2 outputs
    has an ambiguous v2 status: it is counted in ``ambiguous_v2`` of its
    verdict and never in ``computed`` / ``failed`` / ``non_eligible`` — so no
    read order can influence the coverage measurement.
    """
    hstree_keys = set(hstree.verdicts)
    v2_keys = set(v2.statuses)
    ambiguous_hstree_keys = set(hstree.duplicates)

    matched_unique = sorted(
        key
        for key in hstree_keys & v2_keys
        if key not in hstree.duplicates and key not in v2.duplicates
    )
    missing_v2 = sorted(
        key for key in hstree_keys - v2_keys if key not in ambiguous_hstree_keys
    )
    unexpected_v2 = sorted(v2_keys - hstree_keys)

    verdict_buckets: Dict[str, Dict[str, int]] = {
        VERDICT_NORMAL: {},
        VERDICT_ANOMALY: {},
    }
    for verdict in verdict_buckets:
        verdict_buckets[verdict] = {
            "total": 0,
            "computed": 0,
            "failed": 0,
            "model_mismatch": 0,
            "non_eligible": 0,
            "ambiguous_v2": 0,
            "missing_v2": 0,
        }
    for key in sorted(hstree_keys):
        if key in ambiguous_hstree_keys:
            # Ambiguous source verdict: reported in the dedicated section,
            # never arbitrarily bucketed as normal or anomaly.
            continue
        anomaly = hstree.verdicts[key]
        bucket = verdict_buckets[VERDICT_ANOMALY if anomaly else VERDICT_NORMAL]
        bucket["total"] += 1
        if key not in v2.statuses:
            bucket["missing_v2"] += 1
            continue
        if key in v2.duplicates:
            # Ambiguous by construction: the duplicated outputs may agree or
            # conflict, they are never used to measure SHAP coverage.
            bucket["ambiguous_v2"] += 1
            continue
        status = v2.statuses[key]
        if status == "computed":
            bucket["computed"] += 1
        elif status == "failed":
            bucket["failed"] += 1
        elif status == "model_mismatch":
            bucket["model_mismatch"] += 1
        elif status in NON_ELIGIBLE_STATUSES:
            bucket["non_eligible"] += 1

    return {
        "key_fields": [KEY_GROUP, KEY_SOURCE_EVENT_ID],
        "hstree_total": hstree.total,
        "v2_total": v2.total,
        "hstree_unique": len(hstree_keys),
        "v2_unique": len(v2_keys),
        "matched_unique": [list(key) for key in matched_unique],
        "missing_v2": [list(key) for key in missing_v2],
        "unexpected_v2": [list(key) for key in unexpected_v2],
        "duplicate_hstree_keys": [list(key) for key in hstree.duplicates],
        "duplicate_v2_keys": [list(key) for key in v2.duplicates],
        "v2_status_distribution": dict(sorted(v2.status_counts.items())),
        "verdict_matrix": {
            VERDICT_NORMAL: verdict_buckets[VERDICT_NORMAL],
            VERDICT_ANOMALY: verdict_buckets[VERDICT_ANOMALY],
            "ambiguous_hstree": {
                "total": len(hstree.duplicates),
                "keys": [list(key) for key in hstree.duplicates],
            },
        },
    }


def format_compact_summary(report: Dict[str, Any]) -> str:
    """Compact human-readable summary printed on stdout."""
    normal = report["verdict_matrix"][VERDICT_NORMAL]
    anomaly = report["verdict_matrix"][VERDICT_ANOMALY]
    return (
        f"hstree_total={report['hstree_total']} v2_total={report['v2_total']} "
        f"hstree_unique={report['hstree_unique']} v2_unique={report['v2_unique']} "
        f"matched_unique={len(report['matched_unique'])} "
        f"missing_v2={len(report['missing_v2'])} "
        f"unexpected_v2={len(report['unexpected_v2'])} "
        f"duplicate_hstree_keys={len(report['duplicate_hstree_keys'])} "
        f"duplicate_v2_keys={len(report['duplicate_v2_keys'])}\n"
        f"computed: normal={normal['computed']} anomaly={anomaly['computed']} | "
        f"failed: normal={normal['failed']} anomaly={anomaly['failed']} | "
        f"model_mismatch: normal={normal['model_mismatch']} "
        f"anomaly={anomaly['model_mismatch']}"
    )


# ---------------------------------------------------------------------------
# File readers (.csv / .jsonl / .json)
# ---------------------------------------------------------------------------


def read_rows_with_lines(path: str) -> List[RowWithLine]:
    """Read an export file as (line_number, row) pairs, format by extension."""
    file_path = Path(path)
    if not file_path.is_file():
        raise ReconciliationInputError(f"input file not found: {file_path}")
    extension = file_path.suffix.lower()
    if extension == ".csv":
        return _read_csv_rows(file_path)
    if extension == ".jsonl":
        return _read_jsonl_rows(file_path)
    if extension == ".json":
        return _read_json_rows(file_path)
    raise ReconciliationInputError(
        f"unsupported input format '{extension}' for {file_path} "
        f"(expected .csv, .jsonl or .json)"
    )


def _read_csv_rows(file_path: Path) -> List[RowWithLine]:
    rows: List[RowWithLine] = []
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append((reader.line_num, dict(row)))
    return rows


def _read_jsonl_rows(file_path: Path) -> List[RowWithLine]:
    rows: List[RowWithLine] = []
    with file_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue
            try:
                row = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise ReconciliationInputError(
                    f"{file_path}:{line_number}: invalid JSON line: {exc}"
                ) from exc
            if not isinstance(row, dict):
                raise ReconciliationInputError(
                    f"{file_path}:{line_number}: expected a JSON object, "
                    f"got {type(row).__name__}"
                )
            rows.append((line_number, row))
    return rows


def _read_json_rows(file_path: Path) -> List[RowWithLine]:
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ReconciliationInputError(
            f"{file_path}: invalid JSON document: {exc}"
        ) from exc
    if not isinstance(payload, list):
        raise ReconciliationInputError(
            f"{file_path}: expected a JSON array of objects, got {type(payload).__name__}"
        )
    rows: List[RowWithLine] = []
    for index, row in enumerate(payload, start=1):
        if not isinstance(row, dict):
            raise ReconciliationInputError(
                f"{file_path}:{index}: expected a JSON object, got {type(row).__name__}"
            )
        rows.append((index, row))
    return rows


def load_hstree_index(path: str) -> HstreeIndex:
    return index_hstree_rows(read_rows_with_lines(path), source_name=str(path))


def load_v2_index(path: str) -> V2Index:
    return index_v2_rows(read_rows_with_lines(path), source_name=str(path))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile hstree-analysis events with explainability-v2 outputs "
            "on the (Group, source_event_id) key."
        )
    )
    parser.add_argument(
        "--hstree-input",
        required=True,
        help="hstree-analysis export (.csv, .jsonl or .json)",
    )
    parser.add_argument(
        "--v2-output",
        required=True,
        help="explainability-v2 (or explainability-v2-viz) export (.csv, .jsonl or .json)",
    )
    parser.add_argument("--report", help="path of the full JSON report to write")
    args = parser.parse_args(argv)

    try:
        hstree = index_hstree_rows(
            read_rows_with_lines(args.hstree_input), source_name=args.hstree_input
        )
        v2 = index_v2_rows(
            read_rows_with_lines(args.v2_output), source_name=args.v2_output
        )
        report = build_reconciliation_report(hstree, v2)
    except ReconciliationInputError as exc:
        print(f"[reconcile-events] ERROR: {exc}", file=sys.stderr)
        return 1

    if args.report:
        Path(args.report).write_text(
            json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    print(format_compact_summary(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
