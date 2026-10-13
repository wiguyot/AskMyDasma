"""Pure V1/V2 SHAP comparison primitives.

The module deliberately does not depend on Kafka, InfluxDB, Docker or Grafana.
The eventual stream adapter can use these transitions while the same contract
is tested with ordinary Python records.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple


SideRecord = Dict[str, Any]
Identity = Tuple[str, str]

COMPUTED_STATUS = "computed"
COMPARISON_STATUSES = frozenset({
    "complete",
    "timestamp_mismatch",
    "feature_missing_v1",
    "feature_missing_v2",
    "invalid_shap_v1",
    "invalid_shap_v2",
    "status_mismatch",
})


class ShapDiffError(ValueError):
    """Invalid input or ambiguous comparison state."""


def strict_identity(record: Mapping[str, Any]) -> Identity:
    """Return the exact published identity; whitespace is never normalized."""
    group = record.get("Group")
    event_id = record.get("source_event_id")
    if not isinstance(group, str) or not group.strip():
        raise ShapDiffError("invalid Group identity")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ShapDiffError("invalid source_event_id identity")
    return group, event_id


def _finite_number(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ShapDiffError(f"{path} is not numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ShapDiffError(f"{path} is not finite")
    return number


def normalized_shap_values(record: Mapping[str, Any], side: str) -> Dict[str, float]:
    """Validate and normalize a computed record's SHAP map.

    ``Group``, ``source_event_id`` and ``explained_model`` may be injected into
    the JSON viz map so Telegraf can emit them as tags. They are transport
    metadata, never SHAP features, and are removed before numeric conversion.
    """
    raw = record.get("shap_values")
    if not isinstance(raw, Mapping):
        raise ShapDiffError(f"invalid_shap_{side}: shap_values is not an object")
    result: Dict[str, float] = {}
    for name, value in raw.items():
        if name in {"Group", "source_event_id", "explained_model"}:
            continue
        if not isinstance(name, str) or not name:
            raise ShapDiffError(f"invalid_shap_{side}: feature name is invalid")
        result[name] = _finite_number(value, f"shap_values.{name} ({side})")
    if not result:
        raise ShapDiffError(f"invalid_shap_{side}: shap_values is empty")
    return result


@dataclass(frozen=True)
class ShapDiffRow:
    """One event/feature point suitable for a flat JSON/Influx record."""

    timestamp: str
    group: str
    source_event_id: str
    feature: str
    value_v1: Optional[float]
    value_v2: Optional[float]
    difference: Optional[float]
    comparison_status: str

    def as_record(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "Group": self.group,
            "source_event_id": self.source_event_id,
            "feature": self.feature,
            "value_v1": self.value_v1,
            "value_v2": self.value_v2,
            "difference": self.difference,
            "comparison_status": self.comparison_status,
        }


def _timestamp(record: Mapping[str, Any]) -> str:
    value = record.get("timestamp")
    if not isinstance(value, str) or not value:
        raise ShapDiffError("timestamp is missing or invalid")
    return value


def compare_shap_records(v1: Mapping[str, Any], v2: Mapping[str, Any]) -> Tuple[ShapDiffRow, ...]:
    """Compare two complete records, preserving one row per feature.

    Non-computed events return no SHAP rows. They remain observable through the
    input topics and the stream adapter's event counters; inventing zero-valued
    SHAP rows would change the meaning of the dashboard.
    """
    key1 = strict_identity(v1)
    key2 = strict_identity(v2)
    if key1 != key2:
        raise ShapDiffError(f"identity mismatch: {key1!r} != {key2!r}")
    group, event_id = key1
    time1 = _timestamp(v1)
    time2 = _timestamp(v2)
    status1 = v1.get("explanation_status")
    status2 = v2.get("explanation_status")
    if status1 != COMPUTED_STATUS or status2 != COMPUTED_STATUS:
        return ()
    values1 = normalized_shap_values(v1, "v1")
    values2 = normalized_shap_values(v2, "v2")
    features = sorted(set(values1) | set(values2))
    timestamp_status = "complete" if time1 == time2 else "timestamp_mismatch"
    rows = []
    for feature in features:
        in1, in2 = feature in values1, feature in values2
        if not in1:
            status = "feature_missing_v1"
        elif not in2:
            status = "feature_missing_v2"
        else:
            status = timestamp_status
        value1 = values1.get(feature)
        value2 = values2.get(feature)
        difference = value2 - value1 if in1 and in2 and status == "complete" else None
        rows.append(ShapDiffRow(
            timestamp=time2,
            group=group,
            source_event_id=event_id,
            feature=feature,
            value_v1=value1,
            value_v2=value2,
            difference=difference,
            comparison_status=status,
        ))
    return tuple(rows)


def records_signature(records: Iterable[Mapping[str, Any]]) -> Tuple[Tuple[Any, ...], ...]:
    """Stable signature used to suppress identical redeliveries."""
    return tuple(sorted(
        (r.timestamp, r.group, r.source_event_id, r.feature,
         r.value_v1, r.value_v2, r.difference, r.comparison_status)
        for r in records
    ))


@dataclass
class ComparisonBucket:
    """The two latest records for one exact event identity."""

    group: str
    source_event_id: str
    first_seen: float
    v1: Optional[SideRecord] = None
    v2: Optional[SideRecord] = None
    last_signature: Optional[Tuple[Tuple[Any, ...], ...]] = None


def process_arrival(
    pending: Dict[Identity, ComparisonBucket],
    side: str,
    record: SideRecord,
    now: Optional[float] = None,
) -> Tuple[ComparisonBucket, Optional[str]]:
    """Store one side and return ``(bucket, emission_reason)``.

    Identical redeliveries are suppressed. A changed redelivery emits a
    ``progress`` update, which lets an at-least-once source repair a previous
    partial comparison without silently losing the update.
    """
    if side not in {"v1", "v2"}:
        raise ShapDiffError(f"unknown comparison side: {side}")
    group, event_id = strict_identity(record)
    key = (group, event_id)
    bucket = pending.get(key)
    if bucket is None:
        bucket = ComparisonBucket(group, event_id, time.monotonic() if now is None else now)
        pending[key] = bucket
    old = bucket.v1 if side == "v1" else bucket.v2
    if old == record:
        return bucket, None
    if side == "v1":
        bucket.v1 = dict(record)
    else:
        bucket.v2 = dict(record)
    if bucket.v1 is not None and bucket.v2 is not None:
        reason = "complete" if bucket.last_signature is None else "progress"
        bucket.last_signature = records_signature(build_emission_records(bucket.v1, bucket.v2))
        return bucket, reason
    return bucket, None


def _status_record(v1: Mapping[str, Any], v2: Mapping[str, Any], status: str) -> ShapDiffRow:
    group, event_id = strict_identity(v1)
    return ShapDiffRow(
        timestamp=_timestamp(v2), group=group, source_event_id=event_id,
        feature="", value_v1=None, value_v2=None, difference=None,
        comparison_status=status,
    )


def build_emission_records(v1: Mapping[str, Any], v2: Mapping[str, Any]) -> Tuple[ShapDiffRow, ...]:
    """Build flat rows, including one metadata row for non-computed events."""
    key1 = strict_identity(v1)
    key2 = strict_identity(v2)
    if key1 != key2:
        raise ShapDiffError(f"identity mismatch: {key1!r} != {key2!r}")
    if v1.get("explanation_status") != v2.get("explanation_status"):
        return (_status_record(v1, v2, "status_mismatch"),)
    if v1.get("explanation_status") != COMPUTED_STATUS:
        return (_status_record(v1, v2, "non_computed"),)
    try:
        return compare_shap_records(v1, v2)
    except ShapDiffError as error:
        message = str(error)
        if "timestamp" in message:
            status = "timestamp_mismatch"
        elif "v1" in message:
            status = "invalid_shap_v1"
        else:
            status = "invalid_shap_v2"
        return (_status_record(v1, v2, status),)
