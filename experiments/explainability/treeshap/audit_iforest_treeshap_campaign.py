#!/usr/bin/env python3
"""Audit archived Docker V-C1/V-C2 exports by (Group, source_event_id)."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import numpy as np

from experiments.explainability.v1_v2.parity_comparator import (
    build_parity_report,
    compute_parity_verdict,
    index_export,
    load_jsonl_export,
    load_manifest,
    write_report,
)


ROOT = Path(__file__).resolve().parents[3]
TRANSPORT_FIELDS = {"Group", "source_event_id", "explained_model"}
INITIAL_WINDOW = 512
REFIT_EVERY = 128
MAX_SAMPLES = 256
BACKGROUND_SIZE = 100
ADDITIVITY_ATOL = 1e-6
ADDITIVITY_RTOL = 1e-7


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_expectations(source: Path) -> tuple[list[str], dict[str, int]]:
    expected: list[str] = []
    group_counts: Counter[str] = Counter()
    with source.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=1):
            group = str(row.get("Group") or "")
            event_id = str(row.get("source_event_id") or f"event-{index:012d}")
            expected.append(f"{group}|{event_id}")
            group_counts[group] += 1
    return expected, dict(sorted(group_counts.items()))


def load_records(path: Path, label: str) -> list[dict[str, Any]]:
    return load_jsonl_export(path, label)


def identity(record: dict[str, Any]) -> str:
    return f"{record.get('Group')}|{record.get('source_event_id')}"


def index_records(records: list[dict[str, Any]], label: str) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for record in records:
        key = identity(record)
        if key in indexed:
            raise ValueError(f"{label}: duplicate identity {key}")
        indexed[key] = record
    return indexed


def harmonic_path_length(psi: int) -> float:
    # scikit-learn's IsolationForest normalizer uses the standard asymptotic
    # approximation for H_(psi-1), not the exact finite harmonic sum.
    if psi <= 1:
        return 0.0
    if psi == 2:
        return 1.0
    return 2.0 * (math.log(psi - 1) + 0.5772156649015329) - 2.0 * (psi - 1) / psi


def percentile95(values: list[float]) -> float | None:
    return float(np.percentile(np.asarray(values, dtype=np.float64), 95)) if values else None


def audit(args: argparse.Namespace) -> dict[str, Any]:
    source = args.source.resolve()
    out_dir = args.output_dir.resolve()
    v1_path, v2_path = args.v1.resolve(), args.v2.resolve()
    detector_path = args.detector.resolve() if args.detector else None

    expected_keys, group_counts = source_expectations(source)
    expected_set = set(expected_keys)
    v1_records = load_records(v1_path, "v1")
    v2_records = load_records(v2_path, "v2")
    v1 = index_records(v1_records, "v1")
    v2 = index_records(v2_records, "v2")
    detector_records = load_records(detector_path, "iforest") if detector_path else []
    detector = index_records(detector_records, "iforest") if detector_path else {}

    for label, records in (("v1", v1), ("v2", v2)):
        if set(records) != expected_set:
            missing = sorted(expected_set - set(records))[:10]
            extra = sorted(set(records) - expected_set)[:10]
            raise ValueError(f"{label} identity coverage mismatch; missing={missing}, extra={extra}")
    if args.mode == "reference" and set(detector) != expected_set:
        raise ValueError("detector identity coverage does not exactly match the source dataset")
    if args.mode == "simple" and detector_path:
        raise ValueError("independent-mode audit must not be given a detector export")

    expected_computed = sum(max(0, count - INITIAL_WINDOW) for count in group_counts.values())
    summaries: dict[str, Any] = {}
    max_additivity_error = 0.0
    max_score_relation_error = 0.0
    refit_boundary_count = 0
    computed_identities: set[str] = set()
    max_kernel_efficiency_error = 0.0
    for label, records in (("v1", v1), ("v2", v2)):
        statuses = Counter(str(record.get("explanation_status")) for record in records.values())
        computed = [record for record in records.values() if record.get("explanation_status") == "computed"]
        failures = {
            status: count
            for status, count in statuses.items()
            if status in {"failed", "model_mismatch"}
        }
        if failures:
            raise ValueError(f"{label} has failed or model_mismatch statuses: {failures}")
        if statuses.get("computed", 0) != expected_computed:
            raise ValueError(
                f"{label} computed {statuses.get('computed', 0)} identities; "
                f"expected {expected_computed} from per-group warmup"
            )

        durations: list[float] = []
        for key, record in records.items():
            if record.get("explained_model") != "iforest":
                raise ValueError(f"{label} {key}: wrong explained_model")
            if record.get("explainability_mode") != args.mode:
                raise ValueError(f"{label} {key}: wrong mode {record.get('explainability_mode')!r}")
            if record.get("explanation_method") != args.method:
                raise ValueError(f"{label} {key}: wrong explanation method")
            if args.method == "tree" and (
                record.get("shap_nsamples") is not None or record.get("explainability_seed") is not None
            ):
                raise ValueError(f"{label} {key}: tree method published KernelSHAP metadata")
            if args.method == "kernel" and (
                record.get("shap_nsamples") != 100
                or not isinstance(record.get("explainability_seed"), int)
            ):
                raise ValueError(f"{label} {key}: incorrect KernelSHAP metadata")
            if record.get("explanation_status") != "computed":
                if any(record.get(field) is not None for field in (
                    "shap_values", "shap_base_value", "explained_value", "explanation_output"
                )):
                    raise ValueError(f"{label} {key}: non-computed event has explanation values")
                continue

            computed_identities.add(key)
            expected_output = "iforest_mean_path_length" if args.method == "tree" else "iforest_score"
            if record.get("explanation_output") != expected_output:
                raise ValueError(f"{label} {key}: unexpected explained scale")
            if record.get("shap_background_size") != BACKGROUND_SIZE:
                raise ValueError(f"{label} {key}: wrong effective background size")
            shap = record.get("shap_values")
            if not isinstance(shap, dict):
                raise ValueError(f"{label} {key}: missing SHAP feature map")
            numeric_shap = [value for feature, value in shap.items() if feature not in TRANSPORT_FIELDS]
            if not numeric_shap or any(
                isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                for value in numeric_shap
            ):
                raise ValueError(f"{label} {key}: invalid SHAP contributions")
            base = float(record["shap_base_value"])
            explained = float(record["explained_value"])
            error = abs(base + math.fsum(map(float, numeric_shap)) - explained)
            if args.method == "tree":
                max_additivity_error = max(max_additivity_error, error)
                if error > ADDITIVITY_ATOL + ADDITIVITY_RTOL * abs(explained):
                    raise ValueError(f"{label} {key}: TreeSHAP additivity error {error}")
            else:
                max_kernel_efficiency_error = max(max_kernel_efficiency_error, error)
                if explained != float(record["local_score"]):
                    raise ValueError(f"{label} {key}: KernelSHAP explained_value differs from local_score")
            duration = record.get("shap_computation_time_ms")
            if not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration < 0:
                raise ValueError(f"{label} {key}: invalid computation duration")
            durations.append(float(duration))

            local_score = record.get("local_score")
            if not isinstance(local_score, (int, float)) or not math.isfinite(local_score):
                raise ValueError(f"{label} {key}: missing local iForest score")
            if args.method == "tree":
                psi = min(MAX_SAMPLES, group_counts[record["Group"]])
                c_psi = harmonic_path_length(psi)
                derived_score = 2.0 ** (-explained / c_psi)
                relation_error = abs(derived_score - float(local_score))
                max_score_relation_error = max(max_score_relation_error, relation_error)
                if relation_error > 1e-12:
                    raise ValueError(f"{label} {key}: path-length/score relation error {relation_error}")

            if args.mode == "reference":
                detector_record = detector[key]
                upstream = detector_record.get("iforest_score")
                reference_score = record.get("reference_score")
                if upstream is None or reference_score != upstream or local_score != upstream:
                    raise ValueError(
                        f"{label} {key}: upstream/reference/local scores differ "
                        f"({upstream!r}, {reference_score!r}, {local_score!r})"
                    )
                if record.get("explanation_status") == "model_mismatch":
                    raise ValueError(f"{label} {key}: model mismatch")
        summaries[label] = {
            "records": len(records),
            "status_counts": dict(sorted(statuses.items())),
            "computed": statuses.get("computed", 0),
            "failed_or_mismatch": failures,
            "method_time_ms": {
                "count": len(durations),
                "median": statistics.median(durations) if durations else None,
                "p95": percentile95(durations),
            },
        }

    if v1.keys() != v2.keys():
        raise ValueError("V1/V2 identities differ")
    if args.mode == "simple":
        for key in expected_set:
            if v1[key].get("local_score") != v2[key].get("local_score"):
                raise ValueError(f"independent-mode local score differs across V1/V2 at {key}")

    # Source input order identifies the exact scored event at each refit edge.
    # Such a row scores with A and installs B; it must still have a computed
    # output, while the next scored row uses B. This is additionally enforced
    # by the pure runtime boundary test in tests/test_iforest_upstream_local_fidelity.py.
    per_group_index: Counter[str] = Counter()
    for key in expected_keys:
        group = key.split("|", 1)[0]
        per_group_index[group] += 1
        event_number = per_group_index[group]
        if event_number > INITIAL_WINDOW and (event_number - INITIAL_WINDOW) % REFIT_EVERY == 0:
            refit_boundary_count += 1
            if key not in computed_identities:
                raise ValueError(f"refit-trigger event {key} did not receive a computed explanation")

    campaign_name = f"iforest-{args.method}-{args.mode}"
    manifest_path = out_dir / "parity-manifest.json"
    manifest_data = {
        "campaign": campaign_name,
        "comparison_key": "(Group, source_event_id)",
        "exports": {"v1": str(v1_path), "v2": str(v2_path)},
        "expected_keys": expected_keys,
        "numeric_tolerances": {"absolute_tolerance": 0.0, "relative_tolerance": 0.0},
        "design_notes": [
            "Expected identities derive from the source CSV and producer event-id rule.",
            "Strict zero-tolerance V1/V2 parity; no relaxation is used.",
            "The comparator includes local_score, explained_value, explanation_output and method metadata.",
        ],
    }
    manifest = load_manifest(manifest_data)
    manifest_path.write_text(json.dumps(manifest_data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    v1_index = index_export(v1_records, "v1")
    v2_index = index_export(v2_records, "v2")
    parity_report = build_parity_report(manifest, v1_index, v2_index)
    verdict = compute_parity_verdict(manifest, v1_index, v2_index, parity_report["comparison"])
    parity_report["summary"]["verdict"] = verdict
    parity_path = out_dir / "parity-report.json"
    write_report(parity_report, parity_path)
    if verdict != "parity_ok":
        raise ValueError(f"V1/V2 strict parity verdict is {verdict}")

    if args.mode == "reference" and len(detector) != len(expected_keys):
        raise ValueError("reference detector export coverage differs from source")

    kernel_baseline = None
    if args.kernel_baseline_summary:
        baseline_path = args.kernel_baseline_summary.resolve()
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
        if baseline.get("dataset_sha256") != sha256(source):
            raise ValueError("Kernel baseline dataset hash differs from this campaign")
        if baseline.get("mode") != args.mode or baseline.get("campaign") != "iforest-kernel-reference":
            raise ValueError("Kernel baseline must be the matching online V-B1 reference campaign")
        kernel_baseline = {
            "source": str(baseline_path),
            "method": "online KernelSHAP iforest_score, nsamples=100, production configuration",
            "services": {
                label: {
                    "median_ms": baseline["services"][label]["method_time_ms"]["median"],
                    "p95_ms": baseline["services"][label]["method_time_ms"]["p95"],
                }
                for label in ("v1", "v2")
            },
        }
    elif args.method == "tree":
        kernel_baseline = {
            "source": "I4b offline benchmark, 300 deterministic events (not directly comparable to this runtime campaign)",
            "method": "KernelSHAP iforest_score, nsamples=100, production LARS regularization",
            "median_ms": 23.314,
            "p95_ms": 24.671,
            "note": "Historical offline result only; use --kernel-baseline-summary for an online comparison.",
        }
    result = {
        "campaign": campaign_name,
        "mode": args.mode,
        "dataset": str(args.source),
        "dataset_sha256": sha256(source),
        "expected_identities": len(expected_keys),
        "group_counts": group_counts,
        "expected_computed": expected_computed,
        "refit_trigger_events_checked": refit_boundary_count,
        "v1_v2_parity": {
            "verdict": verdict,
            "compared_identities": parity_report["comparison"]["compared_identities"],
            "categories": parity_report["comparison"]["categories"],
            "numeric_outcomes": parity_report["comparison"]["numeric_outcomes"],
        },
        "services": summaries,
        "max_additivity_error": max_additivity_error if args.method == "tree" else None,
        "max_kernel_efficiency_error": max_kernel_efficiency_error if args.method == "kernel" else None,
        "max_path_length_score_relation_error": max_score_relation_error,
        "tolerances": {
            "contributions_atol": 2e-7 if args.method == "tree" else None,
            "contributions_rtol": 1e-6 if args.method == "tree" else None,
            "additivity_atol": ADDITIVITY_ATOL if args.method == "tree" else None,
            "additivity_rtol": ADDITIVITY_RTOL if args.method == "tree" else None,
            "score_relation_atol": 1e-12 if args.method == "tree" else None,
            "parity_atol": 0.0,
            "parity_rtol": 0.0,
        },
        "kernel_baseline": kernel_baseline if args.method == "tree" else None,
        "exports": {
            "v1_path": str(v1_path),
            "v1_sha256": sha256(v1_path),
            "v2_path": str(v2_path),
            "v2_sha256": sha256(v2_path),
            "detector_path": str(detector_path) if detector_path else None,
            "detector_sha256": sha256(detector_path) if detector_path else None,
        },
        "parity_manifest": str(manifest_path),
        "parity_report": str(parity_path),
    }
    (out_dir / "audit-summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("reference", "simple"), required=True)
    parser.add_argument("--method", choices=("tree", "kernel"), default="tree")
    parser.add_argument("--source", type=Path, default=ROOT / "data/source-5000.csv")
    parser.add_argument("--v1", type=Path, required=True)
    parser.add_argument("--v2", type=Path, required=True)
    parser.add_argument("--detector", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--kernel-baseline-summary",
        type=Path,
        help="online V-B1 audit-summary.json with the same dataset and mode",
    )
    args = parser.parse_args()
    try:
        result = audit(args)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
