#!/usr/bin/env python3
"""OC-14 — capacity report: pure aggregation of the capacity campaigns.

Inputs (written by the runner, one per budget):

- campaign JSON: derived pipeline metrics for one budget (totals, status
  distribution, producer/drain durations, lag samples, shap time stats);
- offline benchmark JSON: KernelSHAP arithmetic cost per budget (no Kafka).

Outputs: a deterministic JSON aggregate and a Markdown decision report.

The report NEVER invents a business threshold:
- with both SLO options (--target-events-per-second-per-group and
  --max-drain-seconds) it computes margins, compliance and the maximal
  compatible budget, and concludes "optimisation requise / non requise";
- without them it states honestly that no automatic decision is possible
  and lists the plan options only.

This module is pure: no Docker, Kafka, InfluxDB or network access. It uses
the standard library only, so pytest can exercise every function.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

CAMPAIGN_NAME = "oc14-explainability-v2-capacity"
DECISION_WITHOUT_SLO = (
    "Décision automatique impossible : débit cible et délai de drain non définis."
)
# Recommendation is limited to the plan options; nothing is implemented here.
RECOMMENDATION_OPTIONS = [
    "conserver le différé actuel",
    "expérimenter un background/modèle figé avec cache",
    "étudier des snapshots",
    "ouvrir une recherche d'explication spécifique à HSTree",
]


class ReportError(ValueError):
    """Fatal report input error (invalid campaign or benchmark data)."""


# ---------------------------------------------------------------------------
# Deterministic numeric helpers
# ---------------------------------------------------------------------------


def percentile(sorted_values: List[float], fraction: float) -> float:
    """Linear-interpolation percentile over an already sorted list."""
    if not sorted_values:
        raise ReportError("percentile of an empty list")
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = fraction * (len(sorted_values) - 1)
    lower = int(position // 1)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def summarize_shap_times(values: List[Optional[float]]) -> Dict[str, Any]:
    """Deterministic min/p50/p95/max/mean stats; order independent."""
    usable = sorted(float(value) for value in values if value is not None)
    if not usable:
        return {"count": 0, "min": None, "p50": None, "p95": None, "max": None, "mean": None}
    return {
        "count": len(usable),
        "min": usable[0],
        "p50": percentile(usable, 0.50),
        "p95": percentile(usable, 0.95),
        "max": usable[-1],
        "mean": sum(usable) / len(usable),
    }


def theoretical_capacity_per_group(mean_shap_seconds: Optional[float]) -> Optional[float]:
    """1 / mean_shap_seconds: events per second one worker can serve."""
    if mean_shap_seconds is None or mean_shap_seconds <= 0:
        return None
    return 1.0 / mean_shap_seconds


# ---------------------------------------------------------------------------
# Lag sample analysis
# ---------------------------------------------------------------------------


def analyze_lag_samples(
    samples: List[Dict[str, Any]],
    producer_exit_time: Optional[float],
    completion_time: Optional[float],
) -> Dict[str, Any]:
    """Max lag, time to max lag, lag area (event·s) and final lag.

    Samples carry {'t': seconds since campaign start, 'lag': int or None}.
    A None lag (consumer group not yet registered) contributes zero to the
    area and is skipped for the maximum. Samples are sorted internally so the
    result never depends on the input order.
    """
    usable = sorted(
        (sample for sample in samples if isinstance(sample.get("t"), (int, float))),
        key=lambda sample: sample["t"],
    )
    if not usable:
        return {
            "max_lag": None,
            "time_to_max_lag_seconds": None,
            "lag_area_event_seconds": 0.0,
            "final_lag": None,
        }

    max_lag: Optional[int] = None
    time_to_max: Optional[float] = None
    area = 0.0
    previous_t: Optional[float] = None
    previous_lag = 0.0
    for sample in usable:
        lag = sample.get("lag")
        lag_value = float(lag) if isinstance(lag, (int, float)) and not isinstance(lag, bool) else 0.0
        if previous_t is not None:
            area += (sample["t"] - previous_t) * (previous_lag + lag_value) / 2.0
        if max_lag is None or lag_value > max_lag:
            max_lag = int(lag_value) if lag is not None else 0
            time_to_max = sample["t"]
        previous_t = sample["t"]
        previous_lag = lag_value

    final_lag: Optional[int] = None
    for sample in reversed(usable):
        lag = sample.get("lag")
        if isinstance(lag, (int, float)) and not isinstance(lag, bool):
            final_lag = int(lag)
            break

    return {
        "max_lag": max_lag,
        "time_to_max_lag_seconds": time_to_max,
        "lag_area_event_seconds": round(area, 3),
        "final_lag": final_lag,
        "drain_seconds": (
            round(completion_time - producer_exit_time, 3)
            if producer_exit_time is not None and completion_time is not None
            and completion_time >= producer_exit_time
            else None
        ),
    }


def delivery_throughput(eligible_events: int, drain_seconds: Optional[float]) -> Optional[float]:
    """Eligible events delivered per second of drain."""
    if drain_seconds is None or drain_seconds <= 0 or eligible_events < 0:
        return None
    return round(eligible_events / drain_seconds, 6)


# ---------------------------------------------------------------------------
# Campaign validity
# ---------------------------------------------------------------------------

ALLOWED_STATUSES = {
    "warmup",
    "computed",
    "failed",
    "model_mismatch",
    "background_not_ready",
    "disabled",
}


def validate_campaign(campaign: Dict[str, Any], manifest: Dict[str, Any]) -> List[str]:
    """Return the invalidity reasons; an empty list means a valid measurement.

    A failure, a missing event, a duplicate or an unexpected status invalidates
    the budget measurement: no performance conclusion may be drawn from it.
    """
    reasons: List[str] = []
    expected_total = int(manifest["expected_total_events"])
    expected_warmup = int(manifest["expected_warmup_events"])
    expected_shap = int(manifest["expected_shap_events"])
    totals = campaign.get("totals", {})
    for name in ("hstree", "v2", "v2_json"):
        if totals.get(name) != expected_total:
            reasons.append(f"total_{name}_is_{totals.get(name)}_expected_{expected_total}")

    distribution = campaign.get("status_distribution", {})
    unexpected = sorted(set(distribution) - ALLOWED_STATUSES)
    if unexpected:
        reasons.append(f"unexpected_statuses_{unexpected}")
    if distribution.get("warmup") != expected_warmup:
        reasons.append(
            f"warmup_is_{distribution.get('warmup')}_expected_{expected_warmup}"
        )
    if distribution.get("computed") != expected_shap:
        reasons.append(
            f"computed_is_{distribution.get('computed')}_expected_{expected_shap}"
        )
    if distribution.get("failed", 0) != 0:
        reasons.append(f"failed_is_{distribution.get('failed')}")
    if distribution.get("model_mismatch", 0) != 0:
        reasons.append(f"model_mismatch_is_{distribution.get('model_mismatch')}")

    lag = campaign.get("lag_analysis", {})
    if lag.get("final_lag") != 0:
        reasons.append(f"final_lag_is_{lag.get('final_lag')}")

    duplicates = campaign.get("duplicates", {})
    if any(duplicates.values()):
        reasons.append(f"duplicate_keys_{ {k: v for k, v in duplicates.items() if v} }")
    missing_count = campaign.get("missing_keys_count", len(campaign.get("missing_keys", [])))
    if missing_count:
        reasons.append(f"missing_events_{missing_count}")
    return reasons


# ---------------------------------------------------------------------------
# Aggregation (four budgets) and SLO
# ---------------------------------------------------------------------------


def aggregate_budgets(
    manifest: Dict[str, Any],
    campaigns: List[Dict[str, Any]],
    offline: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Aggregate one campaign JSON per budget into the deterministic report."""
    budgets = [int(budget) for budget in manifest["expected_budgets"]]
    by_budget: Dict[int, Dict[str, Any]] = {}
    for campaign in campaigns:
        budget = int(campaign["budget"])
        if budget in by_budget:
            raise ReportError(f"duplicate campaign for budget {budget}")
        by_budget[budget] = campaign
    missing = [budget for budget in budgets if budget not in by_budget]
    if missing:
        raise ReportError(f"missing campaigns for budgets {missing}")

    rows = []
    for budget in budgets:
        campaign = by_budget[budget]
        reasons = validate_campaign(campaign, manifest)
        campaign["invalid_reasons"] = reasons
        campaign["valid"] = not reasons
        lag = campaign.get("lag_analysis", {})
        stats = campaign.get("shap_time_stats", {})
        rows.append(
            {
                "budget": budget,
                "valid": campaign["valid"],
                "invalid_reasons": reasons,
                "shap_mean_ms": stats.get("mean"),
                "shap_p95_ms": stats.get("p95"),
                "theoretical_capacity_per_group": theoretical_capacity_per_group(
                    stats["mean"] / 1000.0 if stats.get("mean") else None
                ),
                "delivered_throughput_events_per_second": campaign.get(
                    "delivery_throughput_events_per_second"
                ),
                "max_lag": lag.get("max_lag"),
                "drain_seconds": lag.get("drain_seconds"),
                "lag_area_event_seconds": lag.get("lag_area_event_seconds"),
                "final_lag": lag.get("final_lag"),
                "producer_duration_seconds": campaign.get("producer", {}).get("duration_seconds"),
                "total_duration_seconds": campaign.get("total_duration_seconds"),
            }
        )

    offline_by_budget: Dict[int, Dict[str, Any]] = {}
    if offline:
        for entry in offline.get("budgets", []):
            offline_by_budget[int(entry["budget"])] = entry

    for row in rows:
        offline_entry = offline_by_budget.get(row["budget"])
        if offline_entry:
            row["offline_mean_ms"] = offline_entry.get("mean_ms")
            row["offline_p95_ms"] = offline_entry.get("p95_ms")
            row["offline_capacity_per_group"] = offline_entry.get("capacity_per_group")

    return {
        "campaign": CAMPAIGN_NAME,
        "manifest": {
            "fixture_version": manifest.get("fixture_version"),
            "source_csv": manifest.get("source_csv"),
            "source_csv_sha256": manifest.get("source_csv_sha256"),
            "feature_count": manifest.get("feature_count"),
            "expected_total_events": manifest.get("expected_total_events"),
            "producer_emission": manifest.get("producer_emission"),
            "campaign_env": manifest.get("campaign_env"),
        },
        "budgets": rows,
    }


def apply_slo(aggregate: Dict[str, Any], target_rate: float, max_drain_seconds: float) -> Dict[str, Any]:
    """Compute per-budget SLO margin/compliance and the maximal compatible
    budget. Requires BOTH SLO values; otherwise the decision is impossible."""
    if target_rate <= 0 or max_drain_seconds <= 0:
        raise ReportError("SLO values must be strictly positive")
    evaluations = []
    for row in aggregate["budgets"]:
        throughput = row["delivered_throughput_events_per_second"]
        drain = row["drain_seconds"]
        valid = row["valid"]
        rate_margin = None if throughput is None else round(throughput - target_rate, 6)
        drain_margin = None if drain is None else round(max_drain_seconds - drain, 3)
        compliant = bool(
            valid
            and throughput is not None
            and drain is not None
            and throughput >= target_rate
            and drain <= max_drain_seconds
        )
        evaluations.append(
            {
                "budget": row["budget"],
                "measurement_valid": valid,
                "throughput_margin_events_per_second": rate_margin,
                "drain_margin_seconds": drain_margin,
                "compliant": compliant,
            }
        )
    compatible = [item["budget"] for item in evaluations if item["compliant"]]
    max_compatible = max(compatible) if compatible else None
    if max_compatible is not None and max_compatible == max(b["budget"] for b in aggregate["budgets"]):
        decision = "optimisation non requise : le budget le plus élevé satisfait les deux SLO"
    elif compatible:
        decision = "optimisation requise au-delà du budget compatible maximal"
    else:
        decision = "optimisation requise : aucun budget ne satisfait les deux SLO"
    return {
        "target_events_per_second_per_group": target_rate,
        "max_drain_seconds": max_drain_seconds,
        "budgets": evaluations,
        "max_compatible_budget": max_compatible,
        "decision": decision,
    }


def assumed_input_consequences(aggregate: Dict[str, Any], rates: List[float]) -> List[Dict[str, Any]]:
    """Consequences of assumed steady input rates (events/s per group).

    Pure arithmetic against the observed/theoretical capacity: no business
    threshold is invented. For each rate and budget: whether the rate is
    sustainable, the steady-state utilization and the backlog growth rate.
    """
    consequences = []
    for rate in sorted(rates):
        if rate <= 0:
            raise ReportError("assumed input rates must be strictly positive")
        per_budget = []
        for row in aggregate["budgets"]:
            capacity = row["theoretical_capacity_per_group"]
            if capacity is None:
                per_budget.append({"budget": row["budget"], "capacity": None, "sustainable": None})
                continue
            utilization = round(rate / capacity, 6)
            per_budget.append(
                {
                    "budget": row["budget"],
                    "capacity": round(capacity, 6),
                    "assumed_rate": rate,
                    "utilization": utilization,
                    "sustainable": rate < capacity,
                    "backlog_growth_events_per_second": (
                        0.0 if rate < capacity else round(rate - capacity, 6)
                    ),
                }
            )
        consequences.append({"assumed_rate_events_per_second_per_group": rate, "budgets": per_budget})
    return consequences


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def render_markdown(
    aggregate: Dict[str, Any],
    slo: Optional[Dict[str, Any]] = None,
    consequences: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Deterministic Markdown decision report."""
    manifest = aggregate["manifest"]
    lines = [
        "# OC-14 — Capacité explainability-v2 (background production 100)",
        "",
        f"- fixture : `{manifest['source_csv']}` (v{manifest['fixture_version']},"
        f" sha256 `{manifest['source_csv_sha256'][:16]}…`)",
        f"- {manifest['feature_count']} features | {manifest['expected_total_events']} événements"
        f" | émission producteur : {manifest['producer_emission']['nominal_events_per_second']} év/s",
        "",
        "## Campagnes par budget (pipeline Kafka réel)",
        "",
        "| nsamples | SHAP moyen/p95 (ms) | capacité théorique/groupe (év/s) | débit v2 observé (év/s) | lag max | drain (s) | aire lag (év·s) | échecs |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in aggregate["budgets"]:
        failures = 0 if row["valid"] else len(row["invalid_reasons"])
        lines.append(
            f"| {row['budget']} | {_fmt(row['shap_mean_ms'], 1)}/{_fmt(row['shap_p95_ms'], 1)}"
            f" | {_fmt(row['theoretical_capacity_per_group'])}"
            f" | {_fmt(row['delivered_throughput_events_per_second'])}"
            f" | {_fmt(row['max_lag'])} | {_fmt(row['drain_seconds'], 1)}"
            f" | {_fmt(row['lag_area_event_seconds'], 1)} | {failures} |"
        )

    lines += [
        "",
        "## Coût arithmétique hors ligne (KernelSHAP seul, sans Kafka)",
        "",
        "| nsamples | coût hors ligne moyen/p95 (ms) | capacité hors ligne/groupe (év/s) |",
        "|---:|---:|---:|",
    ]
    for row in aggregate["budgets"]:
        lines.append(
            f"| {row['budget']} | {_fmt(row.get('offline_mean_ms'), 1)}/{_fmt(row.get('offline_p95_ms'), 1)}"
            f" | {_fmt(row.get('offline_capacity_per_group'))} |"
        )

    lines += [
        "",
        "Lecture : le coût hors ligne isole KernelSHAP ; le débit observé est ce que"
        " Kafka a réellement livré (éligibles / durée de drain) ; le lag max et"
        " l'aire de lag mesurent le backlog ; la validité garantit que tous les"
        " événements ont été délivrés sans échec ni doublon.",
    ]

    invalid = [row for row in aggregate["budgets"] if not row["valid"]]
    if invalid:
        lines += ["", "Mesures invalidées (aucune conclusion de performance ne doit en être tirée) :"]
        for row in invalid:
            lines.append(f"- budget {row['budget']} : {'; '.join(row['invalid_reasons'])}")
    else:
        lines += [
            "",
            "Fiabilité : pour chaque budget, tous les événements attendus ont été"
            " délivrés (warmup + calculés), sans échec, doublon ni lag final non nul.",
        ]

    if consequences:
        lines += ["", "## Conséquences de débits d'entrée supposés (option, sans seuil métier)", ""]
        for entry in consequences:
            lines.append(f"- entrée supposée {entry['assumed_rate_events_per_second_per_group']} év/s/groupe :")
            for item in entry["budgets"]:
                if item.get("sustainable") is None:
                    lines.append(f"  - budget {item['budget']} : capacité indisponible")
                elif item["sustainable"]:
                    lines.append(
                        f"  - budget {item['budget']} : tenable (utilisation {item['utilization']})"
                    )
                else:
                    lines.append(
                        f"  - budget {item['budget']} : non tenable, backlog +{item['backlog_growth_events_per_second']} év/s"
                    )

    lines += ["", "## Décision", ""]
    if slo:
        lines += [
            f"- SLO fournis : débit cible {slo['target_events_per_second_per_group']} év/s/groupe,"
            f" drain max {slo['max_drain_seconds']} s",
            "",
            "| nsamples | marge débit (év/s) | marge drain (s) | conforme |",
            "|---:|---:|---:|---:|",
        ]
        for item in slo["budgets"]:
            lines.append(
                f"| {item['budget']} | {_fmt(item['throughput_margin_events_per_second'])}"
                f" | {_fmt(item['drain_margin_seconds'], 1)} | {'oui' if item['compliant'] else 'non'} |"
            )
        lines += [
            "",
            f"- budget maximal compatible : {_fmt(slo['max_compatible_budget'])}",
            f"- conclusion : {slo['decision']}",
        ]
    else:
        lines += [f"- {DECISION_WITHOUT_SLO}"]

    lines += [
        "",
        "Recommandation (limitée aux options du plan ; aucune implémentée ici) :",
    ]
    for option in RECOMMENDATION_OPTIONS:
        lines.append(f"- {option}")

    lines += [
        "",
        "Limites : mesures mono-groupe sur un fixture dédié ; la capacité observée"
        " dépend de la machine hôte (voir le JSON d'agrégat pour la configuration) ;"
        " les budgets invalidés n'autorisent aucune conclusion.",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def load_json(path: Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def build_aggregate(
    manifest_path: Path,
    campaign_paths: List[Path],
    offline_path: Optional[Path],
    host_info_path: Optional[Path] = None,
) -> Dict[str, Any]:
    manifest = load_json(manifest_path)
    campaigns = [load_json(path) for path in campaign_paths]
    offline = load_json(offline_path) if offline_path else None
    aggregate = aggregate_budgets(manifest, campaigns, offline)
    if host_info_path is not None:
        host = load_json(host_info_path)
        if not isinstance(host, dict):
            raise ReportError("host info must be a JSON object")
        aggregate["host"] = host
    return aggregate


def main(argv: Optional[List[str]] = None) -> int:
    import sys

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--campaign", action="append", required=True, help="campaign JSON per budget (repeatable)")
    parser.add_argument("--offline", default=None, help="offline benchmark JSON")
    parser.add_argument("--host-info", default=None, help="observed host configuration JSON")
    parser.add_argument("--target-events-per-second-per-group", type=float, default=None)
    parser.add_argument("--max-drain-seconds", type=float, default=None)
    parser.add_argument("--assumed-input-rates", default=None, help="comma-separated assumed input rates (events/s per group)")
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--markdown-out", required=True)
    args = parser.parse_args(argv)

    try:
        aggregate = build_aggregate(
            Path(args.manifest),
            [Path(path) for path in args.campaign],
            Path(args.offline) if args.offline else None,
            Path(args.host_info) if args.host_info else None,
        )
        rates = (
            [float(rate) for rate in args.assumed_input_rates.split(",") if rate.strip()]
            if args.assumed_input_rates
            else None
        )
        consequences = assumed_input_consequences(aggregate, rates) if rates else None
        slo = None
        if args.target_events_per_second_per_group is not None and args.max_drain_seconds is not None:
            slo = apply_slo(
                aggregate,
                args.target_events_per_second_per_group,
                args.max_drain_seconds,
            )
        elif args.target_events_per_second_per_group is not None or args.max_drain_seconds is not None:
            print(
                "[oc14-report] both SLO options are required for an automatic decision; "
                "reporting data only",
                file=sys.stderr,
            )

        output = {
            **aggregate,
            "slo": slo,
            "assumed_input_consequences": consequences,
            "decision_without_slo": None if slo else DECISION_WITHOUT_SLO,
        }
        Path(args.json_out).write_text(
            json.dumps(output, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        Path(args.markdown_out).write_text(
            render_markdown(aggregate, slo, consequences), encoding="utf-8"
        )
    except (ReportError, OSError, json.JSONDecodeError, KeyError) as exc:
        print(f"[oc14-report] FAILED: {exc}", file=sys.stderr)
        return 1
    print(f"[oc14-report] written: {args.json_out} | {args.markdown_out}")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
