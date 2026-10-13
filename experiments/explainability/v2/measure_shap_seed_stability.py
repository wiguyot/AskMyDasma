#!/usr/bin/env python3
"""OC-15 — pure exploratory multi-seed SHAP stability measurement.

Compares, for the computed events, the reference-seed export against each
secondary-seed export:

- rank correlation of the features (Spearman);
- top-k feature overlap (Jaccard, on absolute values);
- sign flips of the contributions;
- relative/absolute gap of the contributions;
- identity, status and HSTree verdict conservation.

IMPORTANT: these are MEASURES, not acceptance thresholds. The scientific
decision on inter-seed stability requires later business or scientific
criteria; this report deliberately produces numbers without a pass/fail
verdict on stability.

The conservation aggregate is deliberately strict: identity_status_verdict_
all_conserved is False as soon as ONE event is missing from one side, is no
longer computed on one side, diverges in identity, status, verdict or feature
set. Events missing or no longer computed stay in the per-event details.

This module is pure: no Docker, Kafka or InfluxDB. It reuses the metrics of
`src/dasma_kafka/explainability_v2/stability.py` (the existing contract).
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

from dasma_kafka.explainability_v2 import stability as S
from experiments.explainability.v2.validate_minimal_explainability import (
    is_finite_number,
    normalize_json_viz_shap_values,
)

CAMPAIGN_NAME = "oc15-explainability-v2-minimal-validation"

STABILITY_NOTE = (
    "Ces mesures exploratoires ne constituent PAS un seuil d'acceptation : la "
    "décision scientifique sur la stabilité inter-seed requiert ultérieurement "
    "des critères métier ou scientifiques."
)


class StabilityError(ValueError):
    """Fatal stability input error."""


def _validate_manifest(manifest: Dict[str, Any]) -> None:
    if manifest.get("campaign") != CAMPAIGN_NAME:
        raise StabilityError(
            f"manifest campaign must be {CAMPAIGN_NAME!r}, got {manifest.get('campaign')!r}"
        )
    if "reference_seed" not in manifest or "secondary_seeds" not in manifest:
        raise StabilityError("manifest must declare reference_seed and secondary_seeds")


def _computed_index(records: List[Dict[str, Any]], export_name: str) -> Dict[str, Dict[str, Any]]:
    """Index the computed events by (Group, source_event_id).

    Duplicated computed keys are REJECTED (StabilityError), never silently
    overwritten. Non-computed records are simply skipped: whether an event is
    missing or no longer computed is a per-event divergence reported below.
    """
    index: Dict[str, Dict[str, Any]] = {}
    for position, record in enumerate(records):
        if not isinstance(record, dict):
            raise StabilityError(f"{export_name}: record {position} is not a JSON object")
        group = record.get("Group")
        event_id = record.get("source_event_id")
        if not isinstance(group, str) or not isinstance(event_id, str) or not group or not event_id:
            raise StabilityError(f"{export_name}: record {position} misses Group/source_event_id")
        if record.get("explanation_status") != "computed":
            continue  # only computed events carry SHAP values
        shap_values, identity_error = normalize_json_viz_shap_values(record)
        if identity_error is not None:
            raise StabilityError(
                f"{export_name}: computed event {group}|{event_id} has "
                f"{identity_error}"
            )
        if not isinstance(shap_values, dict):
            raise StabilityError(
                f"{export_name}: computed event {group}|{event_id} has no shap_values dict"
            )
        if any(
            not isinstance(name, str) or not is_finite_number(value)
            for name, value in shap_values.items()
        ):
            raise StabilityError(
                f"{export_name}: computed event {group}|{event_id} has "
                "non-numeric shap_values"
            )
        key = f"{group}|{event_id}"
        if key in index:
            raise StabilityError(f"{export_name}: duplicate computed key {key}")
        normalized_record = dict(record)
        normalized_record["shap_values"] = shap_values
        index[key] = normalized_record
    return index


def _vector(record: Dict[str, Any], features: List[str]) -> List[float]:
    return [float(record["shap_values"][feature]) for feature in features]


def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return round(math.fsum(values) / len(values), 6)


def measure_seed_stability(
    manifest: Dict[str, Any],
    reference_records: List[Dict[str, Any]],
    secondary_records_by_seed: Dict[int, List[Dict[str, Any]]],
    top_k: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Measure inter-seed stability; no pass/fail verdict is produced."""
    _validate_manifest(manifest)
    secondary_records_by_seed = {
        int(seed): records for seed, records in secondary_records_by_seed.items()
    }
    if not secondary_records_by_seed:
        raise StabilityError("no secondary seed export provided")
    declared_seeds = {int(seed) for seed in manifest["secondary_seeds"]}
    for seed in secondary_records_by_seed:
        if seed not in declared_seeds:
            raise StabilityError(
                f"seed {seed} is not declared in the manifest secondary_seeds"
            )
    if top_k is None:
        feature_count = int(manifest.get("feature_count", 0))
        top_k = sorted({1, 2}) if feature_count >= 2 else [1]

    reference = _computed_index(reference_records, "reference export")
    results_by_seed: Dict[str, Any] = {}
    for seed in sorted(secondary_records_by_seed):
        secondary = _computed_index(secondary_records_by_seed[seed], f"seed {seed} export")

        per_event = []
        for key in sorted(set(reference) & set(secondary)):
            ref = reference[key]
            sec = secondary[key]
            if set(ref["shap_values"]) != set(sec["shap_values"]):
                per_event.append(
                    {
                        "key": key,
                        "identity_conserved": False,
                        "status_conserved": False,
                        "verdict_conserved": False,
                        "error": "feature_set_mismatch",
                        "run_a_features": sorted(ref["shap_values"]),
                        "run_b_features": sorted(sec["shap_values"]),
                    }
                )
                continue
            features = sorted(ref["shap_values"])
            ref_vector = _vector(ref, features)
            sec_vector = _vector(sec, features)
            per_event.append(
                {
                    "key": key,
                    "identity_conserved": (
                        ref.get("Group") == sec.get("Group")
                        and ref.get("source_event_id") == sec.get("source_event_id")
                    ),
                    "status_conserved": (
                        ref.get("explanation_status") == sec.get("explanation_status")
                    ),
                    "verdict_conserved": (
                        ref.get("hstree_anomaly") == sec.get("hstree_anomaly")
                    ),
                    "spearman": S.spearman(ref_vector, sec_vector),
                    **{
                        f"jaccard_at_{k}": S.jaccard_at_k(ref_vector, sec_vector, k)
                        for k in top_k
                    },
                    "sign_flip_rate_top1": S.sign_flip_rate(ref_vector, sec_vector, 1),
                    "relative_mean_abs_diff": S.relative_mean_abs_diff(ref_vector, sec_vector),
                    "max_abs_diff": max(
                        (abs(a - b) for a, b in zip(ref_vector, sec_vector)), default=0.0
                    ),
                }
            )
        # Events missing from one side or no longer computed there stay in the
        # details, flagged as a divergence.
        missing_or_not_computed = sorted(set(reference) ^ set(secondary))
        for key in missing_or_not_computed:
            per_event.append(
                {
                    "key": key,
                    "identity_conserved": False,
                    "status_conserved": False,
                    "verdict_conserved": False,
                    "error": "event_not_computed_on_both_sides",
                }
            )

        per_event.sort(key=lambda event: event["key"])
        numeric = [event for event in per_event if "spearman" in event]
        missing_count = len(missing_or_not_computed) + sum(
            1 for event in per_event if event.get("error") == "feature_set_mismatch"
        )
        all_conserved = (
            not missing_or_not_computed
            and bool(numeric)
            and all(
                event.get("identity_conserved")
                and event.get("status_conserved")
                and event.get("verdict_conserved")
                for event in per_event
            )
        )
        aggregate = {
            "seed": seed,
            "computed_events_compared": len(numeric),
            "events_missing_on_one_side": missing_count,
            "spearman_mean": _mean([event["spearman"] for event in numeric]),
            "relative_mean_abs_diff_mean": _mean(
                [event["relative_mean_abs_diff"] for event in numeric]
            ),
            "sign_flip_rate_top1_mean": _mean([event["sign_flip_rate_top1"] for event in numeric]),
            **{
                f"jaccard_at_{k}_mean": _mean([event[f"jaccard_at_{k}"] for event in numeric])
                for k in top_k
            },
            "identity_status_verdict_all_conserved": all_conserved,
        }
        results_by_seed[str(seed)] = {
            "aggregate": aggregate,
            "per_event": per_event,
        }

    return {
        "campaign": CAMPAIGN_NAME,
        "kind": "exploratory_seed_stability",
        "reference_seed": manifest.get("reference_seed"),
        "disclaimer": STABILITY_NOTE,
        "seeds": results_by_seed,
    }


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    records = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        records.append(json.loads(stripped))
    return records


def main(argv: Optional[List[str]] = None) -> int:
    import sys

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--reference", required=True, help="reference-seed JSONL export")
    parser.add_argument(
        "--secondary",
        action="append",
        required=True,
        help="secondary-seed JSONL export as SEED=PATH (repeatable)",
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
        secondary: Dict[int, List[Dict[str, Any]]] = {}
        for item in args.secondary:
            seed_text, _, path = item.partition("=")
            secondary[int(seed_text)] = load_jsonl(Path(path))
        report = measure_seed_stability(
            manifest, load_jsonl(Path(args.reference)), secondary
        )
        Path(args.output).write_text(
            json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    except (StabilityError, OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"[oc15-seed-stability] FAILED: {exc}", file=sys.stderr)
        return 1
    print(
        "[oc15-seed-stability] measured "
        + ", ".join(
            f"seed {seed}: spearman_mean={data['aggregate']['spearman_mean']}"
            for seed, data in report["seeds"].items()
        )
    )
    print(f"[oc15-seed-stability] {STABILITY_NOTE}")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
