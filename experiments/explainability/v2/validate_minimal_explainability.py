#!/usr/bin/env python3
"""OC-15 — pure validator of the explainability-v2 minimal validation.

Two layers, both pure (no Docker, Kafka or InfluxDB):

1. STRUCTURAL validation of a SINGLE export against the manifest
   (`validate_single_export`), reusable by:
   - the strict comparison of the two reference runs;
   - each secondary-seed export (WITHOUT comparing its SHAP values to the
     reference: only the exploratory multi-seed measurement does that).
   It verifies: exact coverage of the expected keys, no missing/unexpected/
   duplicated key, valid (Group, source_event_id) identity, coherent
   timestamp against the manifest, valid explanation_status, HSTree verdict
   matching the declared cases, SHAP structure coherent with the status,
   finite numbers, and the SHAP additivity against local_hstree_score
   (base + sum of contributions ~= local_hstree_score).

2. STRICT same-seed reproducibility between the two reference runs
   (`compare_minimal_validation`): same statuses, same features, same values
   within the manifest-declared numeric tolerance, and equal SHAP sums
   between the runs (reported as shap_sum_reproducibility_mismatch — this is
   a reproducibility check, NOT the additivity contract).

Additivity tolerance: KernelSHAP is an ill-conditioned weighted regression,
so base + sum ~= f(x) is NOT expected to hold bit-for-bit. The manifest may
declare a dedicated, documented `additivity_tolerance`; when absent, the
reproducibility tolerances are reused. The deterministic JSON report lists
errors per key (with run-specific codes), counts by status and verdict, and
the minimal_validation_passed / minimal_validation_failed verdict.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

CAMPAIGN_NAME = "oc15-explainability-v2-minimal-validation"
VERDICT_PASSED = "minimal_validation_passed"
VERDICT_FAILED = "minimal_validation_failed"

VALID_STATUSES = {
    "warmup",
    "computed",
    "failed",
    "model_mismatch",
    "background_not_ready",
    "disabled",
}
COMPUTED_STATUS = "computed"
NON_COMPUTED_STATUSES = VALID_STATUSES - {COMPUTED_STATUS}

# The JSON visualization copy embeds event identity and model metadata in the
# ``shap_values`` map so Telegraf json_v2 can preserve them as tags. The event
# identity fields are required as a pair when injected; ``explained_model`` is
# optional for compatibility with pre-K7 copies. None are SHAP contributions.
JSON_VIZ_SHAP_IDENTITY_FIELDS = ("Group", "source_event_id")
JSON_VIZ_SHAP_OPTIONAL_TRANSPORT_FIELDS = ("explained_model",)
JSON_VIZ_SHAP_TRANSPORT_FIELDS = (
    *JSON_VIZ_SHAP_IDENTITY_FIELDS,
    *JSON_VIZ_SHAP_OPTIONAL_TRANSPORT_FIELDS,
)


class ValidationError(ValueError):
    """Fatal validator input error (invalid manifest)."""


# ---------------------------------------------------------------------------
# Indexing and numeric helpers
# ---------------------------------------------------------------------------


def event_key(record: Dict[str, Any]) -> Optional[str]:
    group = record.get("Group")
    event_id = record.get("source_event_id")
    if not isinstance(group, str) or not group:
        return None
    if not isinstance(event_id, str) or not event_id:
        return None
    return f"{group}|{event_id}"


def _validate_manifest(manifest: Dict[str, Any]) -> None:
    if manifest.get("campaign") != CAMPAIGN_NAME:
        raise ValidationError(
            f"manifest campaign must be {CAMPAIGN_NAME!r}, got {manifest.get('campaign')!r}"
        )
    for field in ("expected_keys", "warmup_keys", "shap_eligible_keys", "expected_verdicts"):
        if field not in manifest:
            raise ValidationError(f"manifest misses {field}")
    tolerances = manifest.get("numeric_tolerances")
    if not isinstance(tolerances, dict):
        raise ValidationError("manifest must declare numeric_tolerances")
    for name in ("absolute_tolerance", "relative_tolerance"):
        value = tolerances.get(name)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
            raise ValidationError(f"numeric_tolerances.{name} must be a non-negative number")
    if "additivity_tolerance" in manifest:
        for name in ("absolute_tolerance", "relative_tolerance"):
            value = manifest["additivity_tolerance"].get(name)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
                raise ValidationError(f"additivity_tolerance.{name} must be a non-negative number")
    for seed_field in ("reference_seed", "secondary_seeds"):
        if seed_field not in manifest:
            raise ValidationError(f"manifest misses {seed_field}")


def tolerances_of(manifest: Dict[str, Any]) -> Tuple[float, float, float, float]:
    """(reproducibility atol, reproducibility rtol, additivity atol, additivity rtol).

    The additivity tolerance falls back to the reproducibility tolerances
    when the manifest does not declare the dedicated key.
    """
    repro = manifest["numeric_tolerances"]
    additivity = manifest.get("additivity_tolerance", repro)
    return (
        float(repro["absolute_tolerance"]),
        float(repro["relative_tolerance"]),
        float(additivity["absolute_tolerance"]),
        float(additivity["relative_tolerance"]),
    )


def values_equal(v1: Optional[float], v2: Optional[float], atol: float, rtol: float) -> bool:
    if v1 is None and v2 is None:
        return True
    if v1 is None or v2 is None:
        return False
    diff = abs(float(v1) - float(v2))
    if diff <= atol:
        return True
    denominator = max(abs(float(v1)), abs(float(v2)))
    if denominator == 0.0:
        return diff == 0.0
    return (diff / denominator) <= rtol


def is_finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def shap_sum(record: Dict[str, Any]) -> Optional[float]:
    """base value + sum of the SHAP contributions (additivity identity)."""
    base = record.get("shap_base_value")
    shap_values = record.get("shap_values")
    if not is_finite_number(base) or not _finite_shap_map(shap_values):
        return None
    return float(base) + math.fsum(float(value) for value in shap_values.values())


def normalize_json_viz_shap_values(record: Dict[str, Any]) -> Tuple[Any, Optional[str]]:
    """Return numeric SHAP contributions from canonical or JSON-viz payloads.

    When the JSON visualization serializer injected identity fields, both
    must be present and agree with the outer event identity. The optional
    ``explained_model`` tag must agree with its outer field when present. A
    partial or contradictory injection remains an explicit structural error;
    arbitrary non-numeric values are never silently accepted.
    """
    shap_values = record.get("shap_values")
    if not isinstance(shap_values, dict):
        return shap_values, None

    present = [field for field in JSON_VIZ_SHAP_TRANSPORT_FIELDS if field in shap_values]
    if not present:
        return shap_values, None

    numeric_contributions = {
        key: value
        for key, value in shap_values.items()
        if key not in JSON_VIZ_SHAP_TRANSPORT_FIELDS
    }
    identity_present = [
        field for field in JSON_VIZ_SHAP_IDENTITY_FIELDS if field in shap_values
    ]
    if identity_present and len(identity_present) != len(JSON_VIZ_SHAP_IDENTITY_FIELDS):
        return numeric_contributions, "shap_values_identity_metadata_incomplete"
    if (
        identity_present
        and (
            shap_values["Group"] != record.get("Group")
            or shap_values["source_event_id"] != record.get("source_event_id")
        )
    ):
        return numeric_contributions, "shap_values_identity_metadata_mismatch"
    if (
        "explained_model" in shap_values
        and shap_values["explained_model"] != record.get("explained_model")
    ):
        return numeric_contributions, "shap_values_identity_metadata_mismatch"
    return numeric_contributions, None


def normalized_record_for_validation(record: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str]]:
    """Copy one record with JSON-viz SHAP metadata removed for validation."""
    normalized = dict(record)
    shap_values, identity_error = normalize_json_viz_shap_values(record)
    normalized["shap_values"] = shap_values
    return normalized, identity_error


def _finite_shap_map(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    for key, item in value.items():
        if not isinstance(key, str):
            return False
        if not is_finite_number(item):
            return False
    return True


def observed_verdict(record: Dict[str, Any]) -> Optional[str]:
    """threshold_warmup (null verdict), anomaly, normal — or None unknown."""
    anomaly = record.get("hstree_anomaly")
    if anomaly is None:
        return "threshold_warmup"
    if anomaly is True:
        return "anomaly"
    if anomaly is False:
        return "normal"
    return None


# ---------------------------------------------------------------------------
# R2 — structural validation of a SINGLE export
# ---------------------------------------------------------------------------


def validate_single_export(
    manifest: Dict[str, Any],
    records: List[Dict[str, Any]],
    export_name: str = "export",
) -> Dict[str, Any]:
    """Validate one export against the manifest; return a structural report.

    A duplicated key is a reported error (never a silent overwrite, never a
    lost record). The SHAP values are never compared to another export here.
    """
    _validate_manifest(manifest)
    repro_atol, repro_rtol, add_atol, add_rtol = tolerances_of(manifest)
    expected_keys = set(str(key) for key in manifest["expected_keys"])
    expected_anomaly = set(manifest["expected_verdicts"].get("anomaly", []))
    expected_normal = set(manifest["expected_verdicts"].get("normal", []))
    expected_threshold = set(manifest["expected_verdicts"].get("threshold_warmup", []))
    expected_timestamps = manifest.get("expected_timestamps", {})

    index: Dict[str, Dict[str, Any]] = {}
    duplicates: List[str] = []
    invalid_identity: List[str] = []
    shap_identity_errors: Dict[str, str] = {}
    for position, record in enumerate(records):
        if not isinstance(record, dict):
            invalid_identity.append(f"record_{position}_not_an_object")
            continue
        key = event_key(record)
        if key is None:
            invalid_identity.append(f"record_{position}_invalid_identity")
            continue
        if key in index:
            duplicates.append(key)
            continue
        normalized, identity_error = normalized_record_for_validation(record)
        index[key] = normalized
        if identity_error is not None:
            shap_identity_errors[key] = identity_error

    errors_by_key: Dict[str, List[Dict[str, Any]]] = {}

    def add_error(key: str, code: str, detail: Optional[Dict[str, Any]] = None) -> None:
        errors_by_key.setdefault(key, []).append({"code": code, **(detail or {})})

    for key in sorted(expected_keys - set(index)):
        add_error(key, "missing_key")
    for key in sorted(set(index) - expected_keys):
        add_error(key, "unexpected_key")
    for key, code in sorted(shap_identity_errors.items()):
        add_error(key, code)

    status_counts: Dict[str, int] = {}
    verdict_counts: Dict[str, int] = {}

    for key in sorted(index):
        record = index[key]
        status = record.get("explanation_status")
        status_counts[str(status)] = status_counts.get(str(status), 0) + 1

        timestamp = record.get("timestamp")
        if not isinstance(timestamp, str) or not timestamp:
            add_error(key, "missing_timestamp")
        elif expected_timestamps and key in expected_timestamps:
            if timestamp != expected_timestamps[key]:
                add_error(
                    key,
                    "timestamp_mismatch_vs_manifest",
                    {"expected": expected_timestamps[key], "observed": timestamp},
                )

        if status not in VALID_STATUSES:
            add_error(key, "invalid_status", {"status": status})

        # A model mismatch is a valid runtime outcome and must remain visible
        # as such, but it invalidates scientific fidelity for this campaign:
        # the local model is not the upstream model that the explanation is
        # supposed to describe.  It is therefore never accepted as a
        # successful minimal-validation result.
        if status == "model_mismatch":
            add_error(key, "model_mismatch_not_acceptable")

        observed = observed_verdict(record)
        verdict_counts[str(observed)] = verdict_counts.get(str(observed), 0) + 1
        if status == COMPUTED_STATUS:
            if observed == "anomaly" and key not in expected_anomaly:
                add_error(key, "unexpected_anomaly_verdict")
            if observed == "normal" and key not in expected_normal:
                add_error(key, "unexpected_normal_verdict")
            if observed == "threshold_warmup" and key not in expected_threshold:
                add_error(key, "unexpected_null_verdict")
            if key in expected_anomaly and observed != "anomaly":
                add_error(key, "expected_anomaly_verdict_missing", {"observed": observed})
            if key in expected_normal and observed != "normal":
                add_error(key, "expected_normal_verdict_missing", {"observed": observed})
            # SHAP structure for a computed event.
            if not _finite_shap_map(record.get("shap_values")):
                add_error(key, "invalid_shap_values", {"must_be": "dict of finite numbers"})
            if not is_finite_number(record.get("shap_base_value")):
                add_error(key, "invalid_shap_base_value")
            if not is_finite_number(record.get("local_hstree_score")):
                add_error(key, "invalid_local_hstree_score")
            if not is_finite_number(record.get("shap_computation_time_ms")):
                add_error(key, "invalid_shap_computation_time")
            # R1 — SHAP additivity against the local HSTree score.
            total = shap_sum(record)
            score = record.get("local_hstree_score")
            if total is not None and is_finite_number(score):
                if not values_equal(total, float(score), add_atol, add_rtol):
                    add_error(
                        key,
                        "shap_additivity_vs_score_mismatch",
                        {
                            "shap_sum": total,
                            "local_hstree_score": float(score),
                            "absolute_tolerance": add_atol,
                            "relative_tolerance": add_rtol,
                        },
                    )
        elif status in NON_COMPUTED_STATUSES and record.get("shap_values") is not None:
            add_error(key, "shap_values_should_be_absent", {"status": status})

    error_count = sum(len(errors) for errors in errors_by_key.values())
    return {
        "export": export_name,
        "valid": error_count == 0 and not duplicates and not invalid_identity,
        "error_count": error_count,
        "errors_by_key": {key: errors_by_key[key] for key in sorted(errors_by_key)},
        "duplicates": sorted(duplicates),
        "invalid_identity_records": sorted(invalid_identity),
        "coverage": {
            "expected_total": len(expected_keys),
            "observed_events": len(index),
            "missing": sorted(expected_keys - set(index)),
            "unexpected": sorted(set(index) - expected_keys),
        },
        "status_counts": status_counts,
        "verdict_counts": verdict_counts,
    }


# ---------------------------------------------------------------------------
# Strict same-seed reproducibility between the two reference runs
# ---------------------------------------------------------------------------


def compare_minimal_validation(
    manifest: Dict[str, Any],
    run_a_records: List[Dict[str, Any]],
    run_b_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Structural validation of both runs + strict reproducibility checks."""
    structure_a = validate_single_export(manifest, run_a_records, "run_a")
    structure_b = validate_single_export(manifest, run_b_records, "run_b")
    repro_atol, repro_rtol, _, _ = tolerances_of(manifest)
    index_a = {
        event_key(record): normalized_record_for_validation(record)[0]
        for record in run_a_records
        if event_key(record)
    }
    index_b = {
        event_key(record): normalized_record_for_validation(record)[0]
        for record in run_b_records
        if event_key(record)
    }

    cross_errors: Dict[str, List[Dict[str, Any]]] = {}

    def add_error(key: str, code: str, detail: Optional[Dict[str, Any]] = None) -> None:
        cross_errors.setdefault(key, []).append({"code": code, **(detail or {})})

    shared_keys = set(index_a) & set(index_b) & set(
        str(key) for key in manifest["expected_keys"]
    )
    for key in sorted(shared_keys):
        record_a = index_a[key]
        record_b = index_b[key]
        if record_a.get("timestamp") != record_b.get("timestamp"):
            add_error(
                key,
                "timestamp_mismatch",
                {"run_a": record_a.get("timestamp"), "run_b": record_b.get("timestamp")},
            )
        if record_a.get("hstree_anomaly") != record_b.get("hstree_anomaly"):
            add_error(
                key,
                "hstree_verdict_mismatch",
                {
                    "run_a": record_a.get("hstree_anomaly"),
                    "run_b": record_b.get("hstree_anomaly"),
                },
            )
        status_a = record_a.get("explanation_status")
        status_b = record_b.get("explanation_status")
        if status_a != status_b:
            add_error(key, "status_reproducibility_mismatch", {"run_a": status_a, "run_b": status_b})

        if status_a == COMPUTED_STATUS and status_b == COMPUTED_STATUS:
            shap_a = record_a.get("shap_values")
            shap_b = record_b.get("shap_values")
            if isinstance(shap_a, dict) and isinstance(shap_b, dict):
                if set(shap_a) != set(shap_b):
                    add_error(
                        key,
                        "feature_set_mismatch",
                        {"run_a": sorted(shap_a), "run_b": sorted(shap_b)},
                    )
                else:
                    for feature in sorted(shap_a):
                        if not values_equal(shap_a[feature], shap_b[feature], repro_atol, repro_rtol):
                            add_error(
                                key,
                                "shap_value_reproducibility_mismatch",
                                {
                                    "feature": feature,
                                    "run_a": shap_a[feature],
                                    "run_b": shap_b[feature],
                                },
                            )
            if not values_equal(
                record_a.get("shap_base_value"), record_b.get("shap_base_value"), repro_atol, repro_rtol
            ):
                add_error(
                    key,
                    "shap_base_value_reproducibility_mismatch",
                    {"run_a": record_a.get("shap_base_value"), "run_b": record_b.get("shap_base_value")},
                )
            if not values_equal(
                record_a.get("local_hstree_score"), record_b.get("local_hstree_score"), repro_atol, repro_rtol
            ):
                add_error(
                    key,
                    "local_hstree_score_reproducibility_mismatch",
                    {
                        "run_a": record_a.get("local_hstree_score"),
                        "run_b": record_b.get("local_hstree_score"),
                    },
                )
            # Reproducibility between the two SHAP sums (NOT the additivity
            # contract: the additivity against the score is checked per run
            # in the structural validation above).
            sum_a = shap_sum(record_a)
            sum_b = shap_sum(record_b)
            if sum_a is not None and sum_b is not None and not values_equal(sum_a, sum_b, repro_atol, repro_rtol):
                add_error(
                    key,
                    "shap_sum_reproducibility_mismatch",
                    {"run_a": sum_a, "run_b": sum_b},
                )

    # Merge the structural errors per run with run-specific codes.
    merged_errors: Dict[str, List[Dict[str, Any]]] = {}
    for run_name, structure in (("run_a", structure_a), ("run_b", structure_b)):
        for key, errors in structure["errors_by_key"].items():
            for error in errors:
                merged_errors.setdefault(key, []).append(
                    {"code": f"{error['code']}_{run_name}", **{k: v for k, v in error.items() if k != "code"}}
                )
    for key, errors in cross_errors.items():
        merged_errors.setdefault(key, []).extend(errors)

    passed = (
        structure_a["valid"]
        and structure_b["valid"]
        and not cross_errors
    )
    return {
        "campaign": CAMPAIGN_NAME,
        "verdict": VERDICT_PASSED if passed else VERDICT_FAILED,
        "configuration": {
            "reference_seed": manifest.get("reference_seed"),
            "numeric_tolerances": manifest["numeric_tolerances"],
            "additivity_tolerance": manifest.get(
                "additivity_tolerance", manifest["numeric_tolerances"]
            ),
            "campaign_env": manifest.get("campaign_env", {}),
        },
        "structural_validation": {"run_a": structure_a, "run_b": structure_b},
        "coverage": {
            "expected_total": len(set(str(key) for key in manifest["expected_keys"])),
            "run_a_events": len(index_a),
            "run_b_events": len(index_b),
        },
        "status_counts": {
            "run_a": structure_a["status_counts"],
            "run_b": structure_b["status_counts"],
        },
        "verdict_counts": {
            "run_a": structure_a["verdict_counts"],
            "run_b": structure_b["verdict_counts"],
        },
        "errors_by_key": {key: merged_errors[key] for key in sorted(merged_errors)},
        "error_count": sum(len(errors) for errors in merged_errors.values()),
        "passed": passed,
    }


# ---------------------------------------------------------------------------
# File helpers and CLI
# ---------------------------------------------------------------------------


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    records = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        records.append(json.loads(stripped))
    return records


def write_report(report: Dict[str, Any], output_path: Path) -> None:
    Path(output_path).write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main(argv: Optional[List[str]] = None) -> int:
    import sys

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run-a", required=True, help="first reference-run JSONL export")
    parser.add_argument("--run-b", required=True, help="second reference-run JSONL export")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
        report = compare_minimal_validation(
            manifest, load_jsonl(Path(args.run_a)), load_jsonl(Path(args.run_b))
        )
        write_report(report, Path(args.output))
    except (ValidationError, OSError, json.JSONDecodeError) as exc:
        print(f"[oc15-validator] FAILED: {exc}", file=sys.stderr)
        return 1
    print(f"[oc15-validator] verdict={report['verdict']} errors={report['error_count']}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
