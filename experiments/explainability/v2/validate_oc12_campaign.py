"""OC-12B — pure validator for the short explainability-v2 integration campaign.

Inputs:
- the OC-12 manifest JSON;
- the raw JSONL export of `hstree-analysis-viz`;
- the raw JSONL export of `explainability-v2-viz`;
- the InfluxDB identity proof (JSON list of points).

It reuses the OC-05 reconciliation on the events carrying a reliable HSTree
verdict (non-null `hstree_anomaly`): warmup events have no verdict by
construction and are validated separately against the manifest.

Exit code 0 only when every criterion is satisfied. The report is a
deterministic JSON document written to --output.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

import reconcile_events as reconciliation


class OC12ValidationError(ValueError):
    """Invalid OC-12 input artifact; the message names the file and line."""


MANIFEST_REQUIRED_FIELDS = (
    "campaign",
    "expected_total_events",
    "warmup_ids",
    "post_warmup_ids",
    "simultaneous_ids",
    "simultaneous_group",
    "simultaneous_timestamp",
)

KEY_HSTREE_ANOMALY = "hstree_anomaly"
KEY_EXPLANATION_STATUS = "explanation_status"


def validate_manifest(manifest: Dict[str, Any]) -> List[str]:
    """Validate the manifest contract; returns the list of contract errors.

    Rules (OC-12R): ids are non-empty and unique inside each list, the warmup
    and post-warmup lists are disjoint, their union is exactly
    ``expected_total_events`` ids, and the simultaneous ids belong to the
    post-warmup list.
    """
    errors: List[str] = []
    warmup_ids = manifest.get("warmup_ids")
    post_warmup_ids = manifest.get("post_warmup_ids")
    simultaneous_ids = manifest.get("simultaneous_ids")
    expected_total = manifest.get("expected_total_events")

    for name, ids in (("warmup_ids", warmup_ids), ("post_warmup_ids", post_warmup_ids)):
        if not isinstance(ids, list) or not ids or not all(
            isinstance(event_id, str) and event_id.strip() for event_id in ids
        ):
            errors.append(f"manifest field '{name}' must be a non-empty list of non-empty ids")
            continue
        duplicates = sorted({event_id for event_id in ids if ids.count(event_id) > 1})
        if duplicates:
            errors.append(f"manifest field '{name}' contains duplicate ids: {duplicates}")

    if (
        not isinstance(simultaneous_ids, list)
        or len(simultaneous_ids) != 2
        or not all(isinstance(event_id, str) and event_id.strip() for event_id in simultaneous_ids)
    ):
        errors.append("manifest field 'simultaneous_ids' must be a list of two non-empty ids")

    if not isinstance(expected_total, int) or expected_total <= 0:
        errors.append("manifest field 'expected_total_events' must be a positive integer")
        return errors

    if not isinstance(warmup_ids, list) or not isinstance(post_warmup_ids, list):
        return errors

    warmup_set = {event_id for event_id in warmup_ids if isinstance(event_id, str)}
    post_set = {event_id for event_id in post_warmup_ids if isinstance(event_id, str)}
    overlap = sorted(warmup_set & post_set)
    if overlap:
        errors.append(
            f"manifest contract violated: warmup_ids and post_warmup_ids must be "
            f"disjoint, overlapping ids: {overlap}"
        )
    union = warmup_set | post_set
    if len(union) != expected_total:
        errors.append(
            f"manifest contract violated: |warmup_ids ∪ post_warmup_ids| = {len(union)} "
            f"does not match expected_total_events = {expected_total}"
        )
    if isinstance(simultaneous_ids, list):
        outside = sorted(set(simultaneous_ids) - post_set)
        if outside:
            errors.append(
                f"manifest contract violated: simultaneous ids must belong to "
                f"post_warmup_ids, outside: {outside}"
            )
    return errors


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def load_manifest(path: str) -> Dict[str, Any]:
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    for field in MANIFEST_REQUIRED_FIELDS:
        if field not in manifest:
            raise OC12ValidationError(f"manifest {path}: missing required field '{field}'")
    return manifest


def load_jsonl_rows(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise OC12ValidationError(f"{path}:{line_number}: invalid JSON line: {exc}") from exc
            if not isinstance(row, dict):
                raise OC12ValidationError(
                    f"{path}:{line_number}: expected a JSON object, got {type(row).__name__}"
                )
            rows.append(row)
    return rows


def load_influx_proof(path: str) -> List[Dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    points = payload.get("points") if isinstance(payload, dict) else payload
    if not isinstance(points, list):
        raise OC12ValidationError(f"influx proof {path}: expected a list of points")
    for index, point in enumerate(points, start=1):
        if not isinstance(point, dict):
            raise OC12ValidationError(f"influx proof {path}: point {index} is not an object")
    return points


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _event_key(row: Dict[str, Any], source: str, line: int) -> str:
    group = row.get("Group")
    event_id = row.get("source_event_id")
    if not isinstance(group, str) or not group.strip():
        raise OC12ValidationError(f"{source}:{line}: missing or empty 'Group'")
    if not isinstance(event_id, str) or not event_id.strip():
        raise OC12ValidationError(f"{source}:{line}: missing or empty 'source_event_id'")
    return f"{group.strip()}|{event_id.strip()}"


def _reliable_verdict(row: Dict[str, Any], source: str, line: int) -> Optional[bool]:
    value = row.get("hstree_anomaly")
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip() in ("true", "false"):
        return value.strip() == "true"
    raise OC12ValidationError(
        f"{source}:{line}: hstree_anomaly must be null, a boolean or 'true'/'false', got {value!r}"
    )


def _check_statuses(
    manifest: Dict[str, Any],
    v2_keys: Dict[str, Dict[str, Any]],
    event_ids: List[str],
    expected_status: Optional[str],
    allowed_statuses: Optional[set],
    errors: List[str],
) -> Dict[str, int]:
    group = manifest["simultaneous_group"]
    counts: Dict[str, int] = {}
    for event_id in event_ids:
        row = v2_keys.get(f"{group}|{event_id}")
        if row is None:
            errors.append(f"v2 export is missing expected event {event_id}")
            counts["absent"] = counts.get("absent", 0) + 1
            continue
        status = row.get("explanation_status")
        counts[status] = counts.get(status, 0) + 1
        if expected_status is not None and status != expected_status:
            errors.append(
                f"event {event_id} has explanation_status={status!r}, expected {expected_status!r}"
            )
        if allowed_statuses is not None and status not in allowed_statuses:
            errors.append(
                f"event {event_id} has explanation_status={status!r}, "
                f"expected one of {sorted(allowed_statuses)}"
            )
    return counts


def _check_influx_identity(
    points: List[Dict[str, Any]],
    group: str,
    shared_timestamp: str,
    simultaneous_ids: List[str],
    errors: List[str],
) -> List[Dict[str, Any]]:
    matching = [
        {
            "timestamp": str(point.get("timestamp", "")).strip(),
            "Group": str(point.get("Group", "")).strip(),
            "source_event_id": str(point.get("source_event_id", "")).strip(),
        }
        for point in points
        if str(point.get("Group", "")).strip() == group
        and str(point.get("timestamp", "")).strip() == shared_timestamp
        and str(point.get("source_event_id", "")).strip() in simultaneous_ids
    ]
    found_ids = {point["source_event_id"] for point in matching}
    for event_id in simultaneous_ids:
        if event_id not in found_ids:
            errors.append(
                f"InfluxDB proof has no point for source_event_id={event_id} "
                f"at timestamp={shared_timestamp} group={group}"
            )
    if len(matching) < 2:
        errors.append(
            f"InfluxDB proof must contain at least two distinct points for the shared "
            f"timestamp+Group pair ({shared_timestamp}, {group}), found {len(matching)}"
        )
    return sorted(matching, key=lambda point: point["source_event_id"])


def build_oc12_report(
    manifest: Dict[str, Any],
    hstree_rows: List[Dict[str, Any]],
    v2_rows: List[Dict[str, Any]],
    influx_points: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Pure OC-12 validation; raises OC12ValidationError on broken artifacts."""
    errors: List[str] = []
    # OC-12R: the manifest contract is validated before anything else.
    manifest_errors = validate_manifest(manifest)
    errors.extend(manifest_errors)
    group = manifest["simultaneous_group"]
    expected_total = int(manifest["expected_total_events"])
    warmup_ids = list(manifest["warmup_ids"])
    post_warmup_ids = list(manifest["post_warmup_ids"])
    simultaneous_ids = list(manifest["simultaneous_ids"])
    shared_timestamp = str(manifest["simultaneous_timestamp"])
    manifest_ids = set(warmup_ids) | set(post_warmup_ids) | set(simultaneous_ids)

    hstree_keys: Dict[str, Dict[str, Any]] = {}
    duplicate_hstree_keys: set = set()
    for index, row in enumerate(hstree_rows, start=1):
        key = _event_key(row, "hstree-analysis-viz", index)
        if key in hstree_keys:
            duplicate_hstree_keys.add(key)
            errors.append(f"duplicate hstree event: {key}")
        hstree_keys[key] = row

    v2_keys: Dict[str, Dict[str, Any]] = {}
    duplicate_v2_keys: set = set()
    for index, row in enumerate(v2_rows, start=1):
        key = _event_key(row, "explainability-v2-viz", index)
        if key in v2_keys:
            duplicate_v2_keys.add(key)
            errors.append(f"duplicate v2 event: {key}")
        v2_keys[key] = row

    # Totals: absence and unexpected events.
    if len(hstree_rows) != expected_total:
        errors.append(
            f"hstree export holds {len(hstree_rows)} events, manifest expects {expected_total}"
        )
    if len(v2_rows) != expected_total:
        errors.append(
            f"v2 export holds {len(v2_rows)} events, manifest expects {expected_total}"
        )
    for key in sorted(set(hstree_keys) - {f"{group}|{event_id}" for event_id in manifest_ids}):
        errors.append(f"unexpected hstree event: {key}")
    for key in sorted(set(v2_keys) - {f"{group}|{event_id}" for event_id in manifest_ids}):
        errors.append(f"unexpected v2 event: {key}")
    for event_id in sorted(manifest_ids):
        if f"{group}|{event_id}" not in hstree_keys:
            errors.append(f"hstree export is missing expected event {event_id}")
        if f"{group}|{event_id}" not in v2_keys:
            errors.append(f"v2 export is missing expected event {event_id}")

    warmup_counts = _check_statuses(manifest, v2_keys, warmup_ids, "warmup", None, errors)
    post_counts = _check_statuses(
        manifest, v2_keys, post_warmup_ids, None, {"computed", "failed"}, errors
    )
    if post_counts.get("computed", 0) < 1:
        errors.append("no post-warmup event has explanation_status='computed'")

    # OC-12R: warmup events must carry a null verdict, post-warmup events a
    # strictly boolean one, and both must exist in HSTree and v2. The
    # pipeline's threshold calibration (HSTREE_ANOMALY_MIN_SAMPLES >= 2) makes
    # the first scored events post-warmup with a still-null verdict: the
    # manifest may declare them explicitly as threshold_warmup_ids.
    threshold_warmup_ids = set(manifest.get("threshold_warmup_ids") or [])
    unknown_threshold_ids = sorted(threshold_warmup_ids - set(post_warmup_ids))
    if unknown_threshold_ids:
        errors.append(
            f"manifest contract violated: threshold_warmup_ids must be a subset "
            f"of post_warmup_ids, outside: {unknown_threshold_ids}"
        )
    warmup_validated = 0
    post_warmup_validated = 0
    threshold_warmup_validated = 0
    for event_id in warmup_ids:
        key = f"{group}|{event_id}"
        hstree_row = hstree_keys.get(key)
        v2_row = v2_keys.get(key)
        if hstree_row is None or v2_row is None:
            continue  # absence is already reported above
        if hstree_row.get(KEY_HSTREE_ANOMALY) is not None:
            errors.append(
                f"warmup event {event_id} must have hstree_anomaly=null in the "
                f"hstree export, got {hstree_row.get(KEY_HSTREE_ANOMALY)!r}"
            )
            continue
        if v2_row.get(KEY_EXPLANATION_STATUS) != "warmup":
            continue  # already reported by _check_statuses
        warmup_validated += 1
    for event_id in post_warmup_ids:
        key = f"{group}|{event_id}"
        hstree_row = hstree_keys.get(key)
        v2_row = v2_keys.get(key)
        if hstree_row is None or v2_row is None:
            continue
        verdict = hstree_row.get(KEY_HSTREE_ANOMALY)
        if event_id in threshold_warmup_ids:
            if verdict is not None:
                errors.append(
                    f"threshold-warmup event {event_id} must have "
                    f"hstree_anomaly=null in the hstree export, got {verdict!r}"
                )
                continue
            if v2_row.get(KEY_EXPLANATION_STATUS) != "computed":
                errors.append(
                    f"threshold-warmup event {event_id} must have "
                    f"explanation_status='computed', got "
                    f"{v2_row.get(KEY_EXPLANATION_STATUS)!r}"
                )
                continue
            threshold_warmup_validated += 1
            continue
        if not isinstance(verdict, bool):
            errors.append(
                f"post-warmup event {event_id} must have a boolean hstree_anomaly "
                f"in the hstree export, got {verdict!r}"
            )
            continue
        if v2_row.get(KEY_EXPLANATION_STATUS) not in ("computed", "failed"):
            continue  # already reported by _check_statuses
        post_warmup_validated += 1

    # Reliable verdicts: at least one true and one false.
    reliable_verdicts: Dict[str, bool] = {}
    for key, row in hstree_keys.items():
        verdict = _reliable_verdict(row, "hstree-analysis-viz", 0)
        if verdict is not None:
            reliable_verdicts[key] = verdict
    reliable_true = sorted(key for key, verdict in reliable_verdicts.items() if verdict)
    reliable_false = sorted(key for key, verdict in reliable_verdicts.items() if not verdict)
    if not reliable_true:
        errors.append("no reliable hstree_anomaly=true event in the hstree export")
    if not reliable_false:
        errors.append("no reliable hstree_anomaly=false event in the hstree export")

    # OC-05 reconciliation on the reliable subset.
    hstree_filtered = [
        (index, row)
        for index, row in enumerate(hstree_rows, start=1)
        if _event_key(row, "hstree-analysis-viz", index) in reliable_verdicts
    ]
    v2_filtered = [
        (index, row)
        for index, row in enumerate(v2_rows, start=1)
        if _event_key(row, "explainability-v2-viz", index) in reliable_verdicts
    ]
    hstree_index = reconciliation.index_hstree_rows(hstree_filtered, "hstree-analysis-viz")
    v2_index = reconciliation.index_v2_rows(v2_filtered, "explainability-v2-viz")
    reconciliation_report = reconciliation.build_reconciliation_report(hstree_index, v2_index)

    for field in ("missing_v2", "unexpected_v2", "duplicate_hstree_keys", "duplicate_v2_keys"):
        if reconciliation_report[field]:
            errors.append(f"OC-05 {field} is not empty: {reconciliation_report[field]}")
    ambiguous_hstree = reconciliation_report["verdict_matrix"]["ambiguous_hstree"]
    if ambiguous_hstree["total"]:
        errors.append(f"OC-05 ambiguous_hstree is not empty: {ambiguous_hstree['keys']}")
    if reconciliation_report["verdict_matrix"]["normal"]["total"] < 1:
        errors.append("OC-05 verdict matrix has no 'normal' bucket")
    if reconciliation_report["verdict_matrix"]["anomaly"]["total"] < 1:
        errors.append("OC-05 verdict matrix has no 'anomaly' bucket")

    # Simultaneous identity in the v2 export.
    simultaneous_v2 = []
    for event_id in simultaneous_ids:
        row = v2_keys.get(f"{group}|{event_id}")
        if row is None:
            errors.append(f"simultaneous event {event_id} is missing from the v2 export")
            continue
        row_timestamp = row.get("timestamp")
        if str(row_timestamp) != shared_timestamp:
            errors.append(
                f"simultaneous event {event_id} has timestamp {row_timestamp!r}, "
                f"expected {shared_timestamp!r}"
            )
        simultaneous_v2.append(
            {
                "source_event_id": event_id,
                "timestamp": str(row_timestamp),
                "Group": row.get("Group"),
            }
        )
    if len({entry["source_event_id"] for entry in simultaneous_v2}) != len(simultaneous_ids):
        errors.append(
            "the simultaneous events are not both present in the v2 export under "
            "distinct source_event_id values"
        )

    identity_influx = _check_influx_identity(
        influx_points, group, shared_timestamp, simultaneous_ids, errors
    )

    return {
        "campaign": manifest["campaign"],
        "passed": not errors,
        "errors": errors,
        "manifest_contract_errors": manifest_errors,
        "counters": {
            "expected_total_events": expected_total,
            "hstree_events": len(hstree_rows),
            "v2_events": len(v2_rows),
            "warmup_expected": len(warmup_ids),
            "post_warmup_expected": len(post_warmup_ids),
            "warmup_validated": warmup_validated,
            "post_warmup_validated": post_warmup_validated,
            "threshold_warmup_validated": threshold_warmup_validated,
            "reliable_true": len(reliable_true),
            "reliable_false": len(reliable_false),
            "duplicate_hstree_keys": sorted(duplicate_hstree_keys),
            "duplicate_v2_keys": sorted(duplicate_v2_keys),
        },
        "status_distribution": {
            "warmup_ids": dict(sorted(warmup_counts.items())),
            "post_warmup_ids": dict(sorted(post_counts.items())),
        },
        "verdict_matrix": reconciliation_report["verdict_matrix"],
        "reconciliation": {
            "hstree_total": reconciliation_report["hstree_total"],
            "v2_total": reconciliation_report["v2_total"],
            "matched_unique": len(reconciliation_report["matched_unique"]),
            "missing_v2": reconciliation_report["missing_v2"],
            "unexpected_v2": reconciliation_report["unexpected_v2"],
            "v2_status_distribution": reconciliation_report["v2_status_distribution"],
        },
        "simultaneous_identity": {
            "group": group,
            "timestamp": shared_timestamp,
            "ids": simultaneous_ids,
            "v2_events": simultaneous_v2,
            "influx_points": identity_influx,
        },
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the OC-12 explainability-v2 integration campaign."
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--hstree-export", required=True)
    parser.add_argument("--v2-export", required=True)
    parser.add_argument("--influx-proof", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    try:
        manifest = load_manifest(args.manifest)
        hstree_rows = load_jsonl_rows(args.hstree_export)
        v2_rows = load_jsonl_rows(args.v2_export)
        influx_points = load_influx_proof(args.influx_proof)
        report = build_oc12_report(manifest, hstree_rows, v2_rows, influx_points)
    except (OC12ValidationError, reconciliation.ReconciliationInputError, OSError, ValueError) as exc:
        report = {
            "campaign": "oc12",
            "passed": False,
            "errors": [str(exc)],
            "counters": {},
            "status_distribution": {},
            "verdict_matrix": None,
            "reconciliation": None,
            "simultaneous_identity": None,
        }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False, sort_keys=True)
        handle.write("\n")

    if report["passed"]:
        counters = report["counters"]
        print(
            "OC-12 PASSED: "
            f"hstree={counters['hstree_events']} v2={counters['v2_events']} "
            f"warmup={report['status_distribution']['warmup_ids'].get('warmup', 0)} "
            f"post_warmup={report['status_distribution']['post_warmup_ids']} "
            f"true={counters['reliable_true']} false={counters['reliable_false']} "
            f"simultaneous={counters.get('simultaneous_ids', report['simultaneous_identity']['ids'])}"
        )
        return 0

    print("OC-12 FAILED:", file=sys.stderr)
    for error in report["errors"]:
        print(f"  - {error}", file=sys.stderr)
    print(f"report: {output_path}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
