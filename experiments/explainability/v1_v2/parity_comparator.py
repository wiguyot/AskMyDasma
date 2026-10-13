#!/usr/bin/env python3
"""V1-05a/b/c — pure, deterministic V1/V2 parity comparator.

This module is the deterministic, auditable base of the exact comparator:
it loads two JSONL exports (V1 and V2) and a parity manifest, indexes both
exports by the exclusive event identity ``(Group, source_event_id)``,
compares the comparable identities with the manifest tolerances (V1-05b)
and produces a deterministic report with named per-category metrics plus a
CLI with distinct exit codes (V1-05c).

Scope limits:

- identity and duplicates: unusable identities, identical duplicates
  (delivery duplication) and conflicting duplicates (ambiguous output);
- numeric/status comparison and systematic facts (V1-05b); NO scientific
  conclusion: nothing here claims any general SHAP validity;
- deterministic report, named metrics and CLI (V1-05c); no Docker campaign
  is ever launched here: V1-06 owns the parity witness campaign;
- pure module: no Docker, Kafka, network or campaign access, no side effects.

The canonical parity projection intentionally excludes
``shap_computation_time_ms`` and every purely operational field: two records
differing only by computation time are the same parity output
(``duplicate_identical``).
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Stable category names and report version (V1-05a).
# Numeric/status comparison categories belong to V1-05b and are not defined
# here.
# ---------------------------------------------------------------------------

PARITY_REPORT_VERSION = 1
COMPARISON_KEY = "(Group, source_event_id)"

V1_EXPORT = "v1"
V2_EXPORT = "v2"

UNUSABLE_IDENTITY = "unusable_identity"
DUPLICATE_IDENTICAL = "duplicate_identical"
DUPLICATE_CONFLICTING = "duplicate_conflicting"

# Comparison categories (V1-05b): semantic outcome per comparable identity.
BOTH_NON_COMPUTED_EQUAL = "both_non_computed_equal"
BOTH_NON_COMPUTED_DIVERGENT = "both_non_computed_divergent"
BOTH_COMPUTED_EQUAL = "both_computed_equal"
BOTH_COMPUTED_DIVERGENT = "both_computed_divergent"
ONE_SIDED_COMPUTED = "one_sided_computed"
FEATURE_SET_MISMATCH = "feature_set_mismatch"

# Numeric outcomes (V1-05b): strict bit equality, tolerated technical parity,
# divergence beyond the declared tolerances, and invalid numerics (NaN, inf,
# non-numeric values) which are never an equality.
STRICT_EQUAL = "strict_equal"
TOLERATED_EQUAL = "tolerated_equal"
DIVERGENT = "divergent"
INVALID_NUMERIC = "invalid_numeric"

# Global verdicts (V1-05c): outcome of a whole parity run.
VERDICT_PARITY_OK = "parity_ok"
VERDICT_PARITY_KO = "parity_ko"
VERDICT_INCOMPLETE_INPUTS = "incomplete_inputs"

# Divergent categories: any of these on a compared identity is a parity
# failure, never a tolerated equality.
DIVERGENT_CATEGORIES = frozenset(
    {
        BOTH_NON_COMPUTED_DIVERGENT,
        BOTH_COMPUTED_DIVERGENT,
        ONE_SIDED_COMPUTED,
        FEATURE_SET_MISMATCH,
    }
)

# Canonical orders shared by the summary, the named metrics and the CLI.
CATEGORY_ORDER = (
    BOTH_NON_COMPUTED_EQUAL,
    BOTH_NON_COMPUTED_DIVERGENT,
    BOTH_COMPUTED_EQUAL,
    BOTH_COMPUTED_DIVERGENT,
    ONE_SIDED_COMPUTED,
    FEATURE_SET_MISMATCH,
)
NUMERIC_OUTCOME_ORDER = (STRICT_EQUAL, TOLERATED_EQUAL, DIVERGENT, INVALID_NUMERIC)

# Stable metric names for a future V1-V2 Grafana series (one per category).
METRIC_PREFIX = "v1_v2_parity_"

COMPUTED_STATUS = "computed"

# Facts compared systematically, even outside computed SHAP (no SHAP
# comparison is ever invented for non-computed statuses).
COMMON_PLAIN_FACTS = ("timestamp", "explanation_status", "hstree_status", "hstree_anomaly")
COMMON_NUMERIC_FACTS = ("hstree_threshold", "hstree_score")
# Extra comparisons performed only when both sides are computed.
COMPUTED_NUMERIC_SCALARS = (
    "shap_base_value",
    "local_hstree_score",
    "local_score",
    "explained_value",
)
COMPUTED_METADATA_FACTS = (
    "explained_model",
    "explainability_mode",
    "explanation_method",
    "explanation_output",
    "explanation_profile",
    "shap_nsamples",
    "shap_background_size",
    "explainability_seed",
    "hstree_model_version",
    "hstree_model_fingerprint",
    "feature_fingerprint",
)

# Transport metadata carried INSIDE the shap_values map by the real JSONL
# exports (V1-06 witness discovery): they are text fields, not numeric
# contributions. They are excluded from the shap_values comparison and stay
# mandatory through the canonical (Group, source_event_id) identity of the
# record itself — never removed from the JSON output, never replaced by the
# timestamp.
SHAP_TRANSPORT_FIELDS = ("Group", "source_event_id", "explained_model")

# Fields carried by the canonical parity projection: identity, timestamp,
# features, HSTree status/verdict, scores, SHAP values and computation
# metadata. Everything else (shap_computation_time_ms, operational fields)
# is excluded by construction.
PARITY_PROJECTION_FIELDS = (
    "Group",
    "source_event_id",
    "timestamp",
    "hstree_score",
    "features",
    "shap_values",
    "shap_base_value",
    "local_hstree_score",
    "local_score",
    "explained_value",
    "explanation_output",
    "hstree_status",
    "hstree_anomaly",
    "hstree_threshold",
    "explanation_status",
    "explained_model",
    "explainability_mode",
    "explanation_method",
    "explanation_error_code",
    "explanation_profile",
    "shap_nsamples",
    "shap_background_size",
    "explainability_seed",
    "hstree_model_version",
    "hstree_model_fingerprint",
    "feature_fingerprint",
)
EXCLUDED_PARITY_FIELDS = ("shap_computation_time_ms",)


class ParityComparatorError(ValueError):
    """Fatal comparator input error (malformed JSONL, invalid manifest)."""


class ParityManifestError(ParityComparatorError):
    """Invalid parity manifest content."""


class ParityInputFileError(ParityComparatorError):
    """Missing or unreadable input file."""


class ParityJsonlError(ParityComparatorError):
    """Malformed JSONL content (bad JSON or non-object line)."""


class ParityUsageError(ParityComparatorError):
    """Invalid CLI invocation."""


# ---------------------------------------------------------------------------
# Identity and canonical projection
# ---------------------------------------------------------------------------


def _usable_identity_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def identity_of(record: Any) -> Optional[str]:
    """Return ``Group|source_event_id`` or None when the identity is unusable.

    The key is exclusive: no timestamp fallback, no coercion. Both parts must
    be real non-empty strings (whitespace-only counts as unusable). The strip
    is used ONLY to detect empty values: once validated, the key is built
    from the raw published values, so ``"row-1"`` and ``" row-1 "`` are two
    distinct identities and never merge.
    """
    if not isinstance(record, dict):
        return None
    group = record.get("Group")
    source_event_id = record.get("source_event_id")
    if not _usable_identity_string(group) or not _usable_identity_string(source_event_id):
        return None
    return f"{group}|{source_event_id}"


def unusable_identity_shape(record: Any) -> str:
    """Deterministic identity shape of an unusable record (order-free)."""
    raw = record if isinstance(record, dict) else {}
    return json.dumps(
        {"Group": raw.get("Group"), "source_event_id": raw.get("source_event_id")},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        ensure_ascii=False,
    )


def canonical_projection(record: Any) -> str:
    """Deterministic JSON projection of the parity fields, keys sorted.

    Carries the identity, timestamp, features, HSTree status/verdict, scores,
    SHAP values and computation metadata; excludes shap_computation_time_ms
    and every operational field. Missing fields are canonicalized as null so
    two records of the same key collapse onto the same projection only when
    their parity content really matches.
    """
    if not isinstance(record, dict):
        record = {}
    projection = {field_name: record.get(field_name) for field_name in PARITY_PROJECTION_FIELDS}
    return json.dumps(projection, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Input loading (JSONL exports and parity manifest)
# ---------------------------------------------------------------------------


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load a JSONL file; a malformed line is an explicit, located error."""
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                records.append(json.loads(stripped))
            except json.JSONDecodeError as exc:
                raise ParityJsonlError(
                    f"{path.name}: malformed JSON line {line_number}: {exc}"
                ) from exc
    return records


def load_jsonl_export(path: Path, export_name: str) -> List[Dict[str, Any]]:
    """Load one export and require every line to be a JSON object."""
    records = load_jsonl(path)
    for position, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ParityJsonlError(
                f"{export_name}: line {position} of {path.name} is not a JSON object"
            )
    return records


def load_manifest(manifest: Any) -> Dict[str, Any]:
    """Validate a parity manifest; any violation is an explicit error.

    The structure follows the OC-13 precedent
    (tests/fixtures/explainability_v2/oc13_manifest.json) with the exclusive
    identity key: ``expected_keys`` entries are ``Group|source_event_id``.
    Since V1-05c the manifest also references the JSONL exports it describes
    (``exports.v1`` / ``exports.v2``, non-empty string paths; the files
    themselves are checked by the CLI). The tolerances are validated and
    carried for the comparison, never silently widened.
    """
    if not isinstance(manifest, dict):
        raise ParityManifestError("manifest must be a JSON object")

    campaign = manifest.get("campaign")
    if not isinstance(campaign, str) or not campaign.strip():
        raise ParityManifestError("manifest 'campaign' must be a non-empty string")

    if manifest.get("comparison_key") != COMPARISON_KEY:
        raise ParityManifestError(
            f"manifest comparison_key must be exactly {COMPARISON_KEY!r}, "
            f"got {manifest.get('comparison_key')!r}"
        )

    expected_keys = manifest.get("expected_keys")
    if not isinstance(expected_keys, list) or not expected_keys:
        raise ParityManifestError("manifest 'expected_keys' must be a non-empty list")
    for key in expected_keys:
        if not isinstance(key, str) or key.count("|") != 1:
            raise ParityManifestError(
                f"manifest expected key {key!r} must be 'Group|source_event_id'"
            )
        group, source_event_id = key.split("|", 1)
        if not group.strip() or not source_event_id.strip():
            raise ParityManifestError(
                f"manifest expected key {key!r} has an empty identity part"
            )
    if len(set(expected_keys)) != len(expected_keys):
        raise ParityManifestError("manifest 'expected_keys' contains duplicates")

    tolerances = manifest.get("numeric_tolerances")
    if not isinstance(tolerances, dict):
        raise ParityManifestError("manifest 'numeric_tolerances' must be an object")
    for name in ("absolute_tolerance", "relative_tolerance"):
        value = tolerances.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ParityManifestError(
                f"manifest numeric_tolerances.{name} must be a number"
            )
        if value < 0:
            raise ParityManifestError(
                f"manifest numeric_tolerances.{name} must be >= 0"
            )

    exports = manifest.get("exports")
    if not isinstance(exports, dict):
        raise ParityManifestError(
            "manifest 'exports' must be an object with 'v1' and 'v2' JSONL paths"
        )
    for side in (V1_EXPORT, V2_EXPORT):
        export_path = exports.get(side)
        if not isinstance(export_path, str) or not export_path.strip():
            raise ParityManifestError(
                f"manifest exports.{side} must be a non-empty string path"
            )

    return manifest


# ---------------------------------------------------------------------------
# Indexing (deterministic, order-independent)
# ---------------------------------------------------------------------------


@dataclass
class ParityIndex:
    """Deterministic identity index of one export (V1 or V2).

    - ``identities``: comparable keys -> canonical projection. Unique keys
      and duplicate-identical keys both land here (identical duplicates
      collapse onto their single distinct projection).
    - ``duplicate_identical``: sorted keys seen more than once with the same
      projection (delivery duplication, not a value divergence).
    - ``duplicate_conflicting``: key -> sorted distinct projections
      (ambiguous output; V1-05b must not compare such values).
    - ``unusable_identity_shapes`` / ``unusable_identity_count``: sorted,
      distinct identity shapes of the records excluded from the comparison
      space.
    """

    export_name: str
    record_count: int
    identities: Dict[str, str] = field(default_factory=dict)
    duplicate_identical: List[str] = field(default_factory=list)
    duplicate_conflicting: Dict[str, List[str]] = field(default_factory=dict)
    unusable_identity_shapes: List[str] = field(default_factory=list)
    unusable_identity_count: int = 0
    # Representative raw record per comparable key (V1-05b). For
    # duplicate-identical keys any representative yields the same comparison:
    # the projection is identical on every parity field.
    records: Dict[str, Dict[str, Any]] = field(default_factory=dict)


def index_export(records: List[Any], export_name: str) -> ParityIndex:
    """Index one export by ``(Group, source_event_id)``, order-independently.

    For a key seen more than once inside one export: identical projections
    collapse onto one representation (no read-order dependence) and the key
    is reported in ``duplicate_identical``; distinct projections are reported
    under ``duplicate_conflicting`` with every distinct projection sorted,
    and the key leaves the comparable identities.
    """
    grouped: Dict[str, List[str]] = {}
    grouped_records: Dict[str, List[Dict[str, Any]]] = {}
    unusable_shapes: set = set()
    unusable_count = 0

    for record in records:
        if not isinstance(record, dict):
            raise ParityComparatorError(
                f"{export_name}: a record is not a JSON object"
            )
        key = identity_of(record)
        if key is None:
            unusable_count += 1
            unusable_shapes.add(unusable_identity_shape(record))
            continue
        grouped.setdefault(key, []).append(canonical_projection(record))
        grouped_records.setdefault(key, []).append(record)

    identities: Dict[str, str] = {}
    duplicate_identical: List[str] = []
    duplicate_conflicting: Dict[str, List[str]] = {}
    records_by_key: Dict[str, Dict[str, Any]] = {}
    for key in sorted(grouped):
        projections = grouped[key]
        distinct = sorted(set(projections))
        if len(distinct) == 1:
            identities[key] = distinct[0]
            # Any representative works: identical projections mean identical
            # parity content, so the comparison never depends on which one.
            records_by_key[key] = grouped_records[key][0]
            if len(projections) > 1:
                duplicate_identical.append(key)
        else:
            duplicate_conflicting[key] = distinct

    return ParityIndex(
        export_name=export_name,
        record_count=len(records),
        identities=identities,
        duplicate_identical=sorted(duplicate_identical),
        duplicate_conflicting=duplicate_conflicting,
        unusable_identity_shapes=sorted(unusable_shapes),
        unusable_identity_count=unusable_count,
        records=records_by_key,
    )


# ---------------------------------------------------------------------------
# Semantic comparison (V1-05b)
# ---------------------------------------------------------------------------


def _numeric_outcome(v1: Any, v2: Any, atol: float, rtol: float) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Numeric rule of the contract; returns (outcome, gaps-or-None).

    - NaN, Infinity or a non-numeric value -> INVALID_NUMERIC, never an
      equality;
    - both None -> STRICT_EQUAL; exactly one None -> DIVERGENT;
    - bit equality (zero against zero included) -> STRICT_EQUAL;
    - then abs(a - b) <= atol -> TOLERATED_EQUAL;
    - then relative check only when the denominator is non-zero;
    - beyond atol with a zero denominator -> DIVERGENT.
    """
    for value in (v1, v2):
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
            return INVALID_NUMERIC, None
    if v1 is None and v2 is None:
        return STRICT_EQUAL, None
    if v1 is None or v2 is None:
        return DIVERGENT, None
    left, right = float(v1), float(v2)
    if not (math.isfinite(left) and math.isfinite(right)):
        return INVALID_NUMERIC, None
    absolute_gap = abs(left - right)
    if absolute_gap == 0.0:
        return STRICT_EQUAL, None
    if absolute_gap <= atol:
        return TOLERATED_EQUAL, {"absolute_gap": absolute_gap, "relative_gap": None}
    denominator = max(abs(left), abs(right))
    if denominator == 0.0:
        return DIVERGENT, {"absolute_gap": absolute_gap, "relative_gap": None}
    relative_gap = absolute_gap / denominator
    if relative_gap <= rtol:
        return TOLERATED_EQUAL, {"absolute_gap": absolute_gap, "relative_gap": relative_gap}
    return DIVERGENT, {"absolute_gap": absolute_gap, "relative_gap": relative_gap}


class _ComparisonAccumulator:
    """Collects equalities and divergences for one identity, order-free."""

    def __init__(self) -> None:
        self.equalities: Dict[str, int] = {STRICT_EQUAL: 0, TOLERATED_EQUAL: 0}
        self.divergences: List[Dict[str, Any]] = []

    def plain(self, path: str, v1: Any, v2: Any) -> None:
        """String/bool/None fact: plain equality, no tolerance, no gaps."""
        if isinstance(v1, bool) != isinstance(v2, bool):
            self.divergences.append({"path": path, "outcome": DIVERGENT, "v1": v1, "v2": v2})
            return
        if v1 == v2:
            self.equalities[STRICT_EQUAL] += 1
        else:
            self.divergences.append({"path": path, "outcome": DIVERGENT, "v1": v1, "v2": v2})

    def numeric(self, path: str, v1: Any, v2: Any, atol: float, rtol: float) -> None:
        outcome, gaps = _numeric_outcome(v1, v2, atol, rtol)
        if outcome == STRICT_EQUAL:
            self.equalities[STRICT_EQUAL] += 1
            return
        if outcome == TOLERATED_EQUAL:
            self.equalities[TOLERATED_EQUAL] += 1
            return
        entry: Dict[str, Any] = {"path": path, "outcome": outcome, "v1": v1, "v2": v2}
        if gaps is not None:
            entry["absolute_gap"] = gaps["absolute_gap"]
            entry["relative_gap"] = gaps["relative_gap"]
        self.divergences.append(entry)

    @property
    def has_divergence(self) -> bool:
        return bool(self.divergences)


def compare_identities(
    key: str,
    v1_record: Dict[str, Any],
    v2_record: Dict[str, Any],
    atol: float,
    rtol: float,
) -> Dict[str, Any]:
    """Compare one identity pair; deterministic, order-independent.

    Branches (V1-05 contract):

    1. non-computed on both sides: timestamp, status and HSTree verdict/score
       facts only — no SHAP comparison is invented;
    2. computed on both sides: additionally features, shap_values feature by
       feature (transport metadata Group/source_event_id excluded from the
       map: identity is owned by the record key), shap_base_value,
       local_hstree_score, fingerprints and computation metadata;
    3. computed on exactly one side -> ONE_SIDED_COMPUTED (explicit
       divergence);
    4. different feature sets -> FEATURE_SET_MISMATCH, with no silent or
       partial value comparison.
    """
    accumulator = _ComparisonAccumulator()
    v1_status = v1_record.get("explanation_status")
    v2_status = v2_record.get("explanation_status")
    v1_computed = v1_status == COMPUTED_STATUS
    v2_computed = v2_status == COMPUTED_STATUS

    # Systematic facts, whatever the statuses.
    for field_name in COMMON_PLAIN_FACTS:
        accumulator.plain(field_name, v1_record.get(field_name), v2_record.get(field_name))
    for field_name in COMMON_NUMERIC_FACTS:
        accumulator.numeric(field_name, v1_record.get(field_name), v2_record.get(field_name), atol, rtol)

    if v1_computed and v2_computed:
        v1_features = v1_record.get("features") or {}
        v2_features = v2_record.get("features") or {}
        if set(v1_features) != set(v2_features):
            # Explicit divergence: no silent or partial feature comparison.
            accumulator.divergences.append(
                {
                    "path": "features",
                    "outcome": FEATURE_SET_MISMATCH,
                    "v1_features": sorted(map(str, v1_features)),
                    "v2_features": sorted(map(str, v2_features)),
                }
            )
        else:
            for feature in sorted(v1_features):
                accumulator.numeric(f"features.{feature}", v1_features.get(feature), v2_features.get(feature), atol, rtol)
            v1_shap = v1_record.get("shap_values") or {}
            v2_shap = v2_record.get("shap_values") or {}
            # shap_values is a map of NUMERIC contributions; the transport
            # metadata it may carry (Group, source_event_id) is excluded
            # before the numeric comparison: identity is already owned by the
            # canonical record key.
            shap_keys = (set(v1_shap) | set(v2_shap)) - set(SHAP_TRANSPORT_FIELDS)
            for feature in sorted(shap_keys):
                accumulator.numeric(
                    f"shap_values.{feature}", v1_shap.get(feature), v2_shap.get(feature), atol, rtol
                )
            for field_name in COMPUTED_NUMERIC_SCALARS:
                accumulator.numeric(field_name, v1_record.get(field_name), v2_record.get(field_name), atol, rtol)
            for field_name in COMPUTED_METADATA_FACTS:
                accumulator.plain(field_name, v1_record.get(field_name), v2_record.get(field_name))

    if v1_computed != v2_computed:
        category = ONE_SIDED_COMPUTED
    elif any(d["outcome"] == FEATURE_SET_MISMATCH for d in accumulator.divergences):
        category = FEATURE_SET_MISMATCH
    elif accumulator.has_divergence:
        if v1_computed and v2_computed:
            category = BOTH_COMPUTED_DIVERGENT
        else:
            category = BOTH_NON_COMPUTED_DIVERGENT
    else:
        category = BOTH_COMPUTED_EQUAL if (v1_computed and v2_computed) else BOTH_NON_COMPUTED_EQUAL

    return {
        "category": category,
        "explanation_status": {"v1": v1_status, "v2": v2_status},
        "timestamp_equal": not any(d["path"] == "timestamp" for d in accumulator.divergences),
        "equalities": accumulator.equalities,
        "divergences": sorted(accumulator.divergences, key=lambda entry: entry["path"]),
    }


def compare_exports(
    manifest: Dict[str, Any],
    v1_index: ParityIndex,
    v2_index: ParityIndex,
) -> Dict[str, Any]:
    """Compare the identities comparable on both sides (V1-05b section).

    Only usable, non-ambiguous identities present in both exports are
    compared here; one-sided coverage stays reported by the V1-05a index
    sections. Tolerances come from the manifest (never applied in V1-05a).
    """
    tolerances = manifest.get("numeric_tolerances", {})
    atol = float(tolerances.get("absolute_tolerance", 0.0))
    rtol = float(tolerances.get("relative_tolerance", 0.0))

    common_keys = sorted(set(v1_index.identities) & set(v2_index.identities))
    identities: Dict[str, Dict[str, Any]] = {}
    categories: Dict[str, int] = {}
    numeric_outcomes: Dict[str, int] = {
        STRICT_EQUAL: 0,
        TOLERATED_EQUAL: 0,
        DIVERGENT: 0,
        INVALID_NUMERIC: 0,
    }

    for key in common_keys:
        comparison = compare_identities(
            key,
            v1_index.records[key],
            v2_index.records[key],
            atol,
            rtol,
        )
        identities[key] = comparison
        categories[comparison["category"]] = categories.get(comparison["category"], 0) + 1
        for outcome, count in comparison["equalities"].items():
            numeric_outcomes[outcome] += count
        for divergence in comparison["divergences"]:
            outcome = divergence["outcome"]
            if outcome in (DIVERGENT, INVALID_NUMERIC):
                numeric_outcomes[outcome] += 1

    return {
        "compared_identities": len(common_keys),
        "categories": dict(sorted(categories.items())),
        "numeric_outcomes": numeric_outcomes,
        "identities": identities,
        "only_v1": sorted(set(v1_index.identities) - set(v2_index.identities)),
        "only_v2": sorted(set(v2_index.identities) - set(v1_index.identities)),
    }


# ---------------------------------------------------------------------------
# Report envelope (versioned, diffable)
# ---------------------------------------------------------------------------


def _export_section(index: ParityIndex) -> Dict[str, Any]:
    return {
        "record_count": index.record_count,
        "comparable_identities": sorted(index.identities),
        UNUSABLE_IDENTITY: {
            "count": index.unusable_identity_count,
            "shapes": list(index.unusable_identity_shapes),
        },
        DUPLICATE_IDENTICAL: sorted(index.duplicate_identical),
        DUPLICATE_CONFLICTING: {
            key: sorted(projections)
            for key, projections in sorted(index.duplicate_conflicting.items())
        },
    }


def build_parity_report(
    manifest: Dict[str, Any],
    v1_index: ParityIndex,
    v2_index: ParityIndex,
) -> Dict[str, Any]:
    """Versioned, diffable parity report (V1-05a envelope + V1-05c summary).

    The envelope separates unusable identities, identical duplicates and
    conflicting duplicates for both exports, carries the V1-05b comparison
    and adds the deterministic ``summary`` section (verdict, per-section
    counts, named metrics). Combined with ``write_report`` (sorted keys),
    two runs on the same inputs produce byte-identical JSON.
    """
    comparison = compare_exports(manifest, v1_index, v2_index)
    return {
        "report_version": PARITY_REPORT_VERSION,
        "comparator": "v1_v2_parity",
        "comparison_key": COMPARISON_KEY,
        "campaign": manifest.get("campaign"),
        "excluded_from_projection": list(EXCLUDED_PARITY_FIELDS),
        "manifest_tolerances": {
            "absolute_tolerance": manifest.get("numeric_tolerances", {}).get(
                "absolute_tolerance"
            ),
            "relative_tolerance": manifest.get("numeric_tolerances", {}).get(
                "relative_tolerance"
            ),
        },
        V1_EXPORT: _export_section(v1_index),
        V2_EXPORT: _export_section(v2_index),
        "comparison": comparison,
        "summary": build_parity_summary(manifest, v1_index, v2_index, comparison),
    }


def write_report(report: Dict[str, Any], output_path: Path) -> None:
    """Write the report as deterministic, diffable JSON (sorted keys)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Verdict, named metrics and deterministic summary (V1-05c)
# ---------------------------------------------------------------------------

# CLI exit codes: three distinct verdicts, then one distinct code per input
# error kind (manifest, file, JSONL), then CLI usage errors.
EXIT_PARITY_OK = 0
EXIT_PARITY_KO = 1
EXIT_INCOMPLETE_INPUTS = 2
EXIT_MANIFEST_ERROR = 3
EXIT_INPUT_FILE_ERROR = 4
EXIT_JSONL_ERROR = 5
EXIT_USAGE_ERROR = 6

_VERDICT_EXIT_CODES = {
    VERDICT_PARITY_OK: EXIT_PARITY_OK,
    VERDICT_PARITY_KO: EXIT_PARITY_KO,
    VERDICT_INCOMPLETE_INPUTS: EXIT_INCOMPLETE_INPUTS,
}


def expected_keys_missing(manifest: Dict[str, Any], index: ParityIndex) -> List[str]:
    """Sorted expected keys absent from the comparable identities of one side."""
    expected = manifest.get("expected_keys", [])
    return sorted(key for key in expected if key not in index.identities)


def compute_parity_verdict(
    manifest: Dict[str, Any],
    v1_index: ParityIndex,
    v2_index: ParityIndex,
    comparison: Dict[str, Any],
) -> str:
    """Global verdict of a parity run (V1-05c contract).

    - ``incomplete_inputs``: the comparison cannot conclude on the manifest
      expectations — an expected key is not comparable on both sides, some
      usable identity was delivered on only one side (lag), a conflicting
      duplicate makes the output ambiguous, or an unusable identity left
      the comparison space;
    - ``parity_ko``: everything was comparable and at least one compared
      identity landed in a divergent category;
    - ``parity_ok``: exact coverage and every compared identity equal.
    """
    incomplete = bool(
        expected_keys_missing(manifest, v1_index)
        or expected_keys_missing(manifest, v2_index)
        or comparison["only_v1"]
        or comparison["only_v2"]
        or v1_index.duplicate_conflicting
        or v2_index.duplicate_conflicting
        or v1_index.unusable_identity_count
        or v2_index.unusable_identity_count
    )
    if incomplete:
        return VERDICT_INCOMPLETE_INPUTS
    if any(category in DIVERGENT_CATEGORIES for category in comparison["categories"]):
        return VERDICT_PARITY_KO
    return VERDICT_PARITY_OK


def build_parity_metrics(
    manifest: Dict[str, Any],
    v1_index: ParityIndex,
    v2_index: ParityIndex,
    comparison: Dict[str, Any],
) -> Dict[str, int]:
    """Named, stable counters: one metric per category, zero included.

    The names are the contract for a future V1-V2 Grafana series; they never
    change with the observed values, so a dashboard can query every series
    even when a category stays at zero.
    """
    categories = {name: 0 for name in CATEGORY_ORDER}
    categories.update(comparison["categories"])
    numeric = {name: 0 for name in NUMERIC_OUTCOME_ORDER}
    numeric.update(comparison["numeric_outcomes"])
    metrics: Dict[str, int] = {
        METRIC_PREFIX + "compared_identities": comparison["compared_identities"],
        METRIC_PREFIX + "only_v1": len(comparison["only_v1"]),
        METRIC_PREFIX + "only_v2": len(comparison["only_v2"]),
        METRIC_PREFIX + "expected_keys_missing_v1": len(
            expected_keys_missing(manifest, v1_index)
        ),
        METRIC_PREFIX + "expected_keys_missing_v2": len(
            expected_keys_missing(manifest, v2_index)
        ),
        METRIC_PREFIX + "duplicate_identical_v1": len(v1_index.duplicate_identical),
        METRIC_PREFIX + "duplicate_identical_v2": len(v2_index.duplicate_identical),
        METRIC_PREFIX + "duplicate_conflicting_v1": len(v1_index.duplicate_conflicting),
        METRIC_PREFIX + "duplicate_conflicting_v2": len(v2_index.duplicate_conflicting),
        METRIC_PREFIX + "unusable_identity_v1": v1_index.unusable_identity_count,
        METRIC_PREFIX + "unusable_identity_v2": v2_index.unusable_identity_count,
    }
    metrics.update(
        {METRIC_PREFIX + "category_" + name: count for name, count in categories.items()}
    )
    metrics.update(
        {METRIC_PREFIX + "numeric_" + name: count for name, count in numeric.items()}
    )
    return metrics


def build_parity_summary(
    manifest: Dict[str, Any],
    v1_index: ParityIndex,
    v2_index: ParityIndex,
    comparison: Dict[str, Any],
) -> Dict[str, Any]:
    """Deterministic summary separating coverage, delivery, identity, status
    and numeric values, with the named metrics and the global verdict."""
    metrics = build_parity_metrics(manifest, v1_index, v2_index, comparison)
    prefix = METRIC_PREFIX
    return {
        "verdict": compute_parity_verdict(manifest, v1_index, v2_index, comparison),
        "coverage": {
            "compared_identities": metrics[prefix + "compared_identities"],
            "only_v1": metrics[prefix + "only_v1"],
            "only_v2": metrics[prefix + "only_v2"],
            "expected_keys_missing_v1": metrics[prefix + "expected_keys_missing_v1"],
            "expected_keys_missing_v2": metrics[prefix + "expected_keys_missing_v2"],
        },
        "delivery": {
            "duplicate_identical_v1": metrics[prefix + "duplicate_identical_v1"],
            "duplicate_identical_v2": metrics[prefix + "duplicate_identical_v2"],
            "duplicate_conflicting_v1": metrics[prefix + "duplicate_conflicting_v1"],
            "duplicate_conflicting_v2": metrics[prefix + "duplicate_conflicting_v2"],
        },
        "identity": {
            "unusable_identity_v1": metrics[prefix + "unusable_identity_v1"],
            "unusable_identity_v2": metrics[prefix + "unusable_identity_v2"],
        },
        "status": {
            "categories": {
                category: metrics[prefix + "category_" + category]
                for category in CATEGORY_ORDER
            }
        },
        "values": {
            "numeric_outcomes": {
                outcome: metrics[prefix + "numeric_" + outcome]
                for outcome in NUMERIC_OUTCOME_ORDER
            }
        },
        "metrics": metrics,
    }


def format_cli_summary(report: Dict[str, Any]) -> str:
    """Readable summary: one line per non-zero category, verdict last.

    Deterministic: the same report always renders the same lines in the
    same order (canonical category order, structural facts, verdict).
    """
    summary = report["summary"]
    lines = [f"campaign: {report['campaign']}"]
    coverage = summary["coverage"]
    lines.append(f"compared_identities: {coverage['compared_identities']}")
    for category in CATEGORY_ORDER:
        count = summary["status"]["categories"][category]
        if count:
            lines.append(f"category {category}: {count}")
    for outcome in NUMERIC_OUTCOME_ORDER:
        count = summary["values"]["numeric_outcomes"][outcome]
        if count:
            lines.append(f"numeric {outcome}: {count}")
    if coverage["only_v1"]:
        lines.append(f"only_v1: {coverage['only_v1']}")
    if coverage["only_v2"]:
        lines.append(f"only_v2: {coverage['only_v2']}")
    delivery = summary["delivery"]
    if delivery["duplicate_identical_v1"] or delivery["duplicate_identical_v2"]:
        lines.append(
            f"duplicate_identical: v1={delivery['duplicate_identical_v1']} "
            f"v2={delivery['duplicate_identical_v2']}"
        )
    if (
        delivery["duplicate_conflicting_v1"]
        or delivery["duplicate_conflicting_v2"]
    ):
        lines.append(
            f"duplicate_conflicting: v1={delivery['duplicate_conflicting_v1']} "
            f"v2={delivery['duplicate_conflicting_v2']}"
        )
    identity = summary["identity"]
    if identity["unusable_identity_v1"] or identity["unusable_identity_v2"]:
        lines.append(
            f"unusable_identity: v1={identity['unusable_identity_v1']} "
            f"v2={identity['unusable_identity_v2']}"
        )
    if coverage["expected_keys_missing_v1"] or coverage["expected_keys_missing_v2"]:
        lines.append(
            f"expected_keys_missing: v1={coverage['expected_keys_missing_v1']} "
            f"v2={coverage['expected_keys_missing_v2']}"
        )
    lines.append(f"verdict: {summary['verdict']}")
    return "".join(line + "\n" for line in lines)


# ---------------------------------------------------------------------------
# CLI (V1-05c): manifest + JSONL exports -> deterministic report + summary
# ---------------------------------------------------------------------------

USAGE = """\
usage: parity_comparator --manifest MANIFEST --output REPORT

Loads the parity manifest, reads the V1/V2 JSONL exports it references,
writes the deterministic parity report and prints one summary line per
non-zero category followed by the global verdict. Relative export paths
are resolved against the current working directory.

Exit codes:
  0  parity_ok            3  invalid manifest
  1  parity_ko            4  missing or unreadable input file
  2  incomplete_inputs    5  malformed JSONL export
  6  usage error
"""

_CLI_OPTIONS = ("--manifest", "--output")


def _parse_cli_options(arguments: List[str]) -> Dict[str, str]:
    options: Dict[str, str] = {}
    index = 0
    while index < len(arguments):
        token = arguments[index]
        name, separator, inline_value = token.partition("=")
        if name not in _CLI_OPTIONS:
            raise ParityUsageError(f"unknown argument {token!r}")
        if separator:
            value = inline_value
            index += 1
        else:
            if index + 1 >= len(arguments):
                raise ParityUsageError(f"{name} expects a value")
            value = arguments[index + 1]
            index += 2
        if not value.strip():
            raise ParityUsageError(f"{name} expects a non-empty value")
        options[name] = value
    for name in _CLI_OPTIONS:
        if name not in options:
            raise ParityUsageError(f"missing required option {name}")
    return options


def _build_report_from_manifest(manifest_path: Path) -> Dict[str, Any]:
    """Load the manifest, then the exports it references, and build the report.

    Raises ParityManifestError, ParityInputFileError or ParityJsonlError so
    the CLI can map each kind to its distinct exit code.
    """
    if not manifest_path.is_file():
        raise ParityInputFileError(f"manifest file not found: {manifest_path}")
    try:
        raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ParityManifestError(f"invalid JSON in {manifest_path}: {exc}") from exc
    except OSError as exc:
        raise ParityInputFileError(f"unreadable manifest {manifest_path}: {exc}") from exc
    try:
        manifest = load_manifest(raw_manifest)
    except ParityManifestError:
        raise
    except ParityComparatorError as exc:
        raise ParityManifestError(str(exc)) from exc

    export_paths: Dict[str, Path] = {}
    for side in (V1_EXPORT, V2_EXPORT):
        raw_path = Path(manifest["exports"][side])
        resolved = raw_path if raw_path.is_absolute() else Path.cwd() / raw_path
        if not resolved.is_file():
            raise ParityInputFileError(
                f"{side} export file not found: {resolved} (manifest: {manifest_path})"
            )
        export_paths[side] = resolved

    v1_index = index_export(
        _load_export(export_paths[V1_EXPORT], V1_EXPORT), V1_EXPORT
    )
    v2_index = index_export(
        _load_export(export_paths[V2_EXPORT], V2_EXPORT), V2_EXPORT
    )
    return build_parity_report(manifest, v1_index, v2_index)


def _load_export(path: Path, export_name: str) -> List[Dict[str, Any]]:
    """Load one export, mapping OS errors to ParityInputFileError."""
    try:
        return load_jsonl_export(path, export_name)
    except OSError as exc:
        raise ParityInputFileError(f"unreadable {export_name} export {path}: {exc}") from exc


def run_cli(
    argv: Optional[List[str]] = None,
    stdout: Optional[Any] = None,
    stderr: Optional[Any] = None,
) -> int:
    """CLI entry point; returns the process exit code (see USAGE)."""
    out = sys.stdout if stdout is None else stdout
    err = sys.stderr if stderr is None else stderr
    arguments = list(sys.argv[1:] if argv is None else argv)
    if "--help" in arguments or "-h" in arguments:
        print(USAGE, file=out, end="")
        return EXIT_PARITY_OK
    try:
        options = _parse_cli_options(arguments)
    except ParityUsageError as exc:
        print(USAGE, file=err, end="")
        print(f"parity_comparator: {exc}", file=err)
        return EXIT_USAGE_ERROR
    try:
        report = _build_report_from_manifest(Path(options["--manifest"]))
    except ParityManifestError as exc:
        print(f"parity_comparator: invalid manifest: {exc}", file=err)
        return EXIT_MANIFEST_ERROR
    except ParityInputFileError as exc:
        print(f"parity_comparator: missing input file: {exc}", file=err)
        return EXIT_INPUT_FILE_ERROR
    except ParityJsonlError as exc:
        print(f"parity_comparator: malformed JSONL: {exc}", file=err)
        return EXIT_JSONL_ERROR
    try:
        write_report(report, Path(options["--output"]))
    except OSError as exc:
        print(f"parity_comparator: cannot write report: {exc}", file=err)
        return EXIT_INPUT_FILE_ERROR
    print(format_cli_summary(report), file=out, end="")
    return _VERDICT_EXIT_CODES[report["summary"]["verdict"]]


if __name__ == "__main__":
    raise SystemExit(run_cli())
