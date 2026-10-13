#!/usr/bin/env python3
"""OC-14 — offline KernelSHAP capacity benchmark (production background 100).

Complements `shap_stability_runner.py` WITHOUT changing its interface: this
module imports its building blocks (load_group_rows, build_model,
shap_vector) and re-measures them with the production settings that the
stability runner does not use by default:

- SHAP background 100 (stability runner default: 40);
- production HSTree parameters (initial window 100, window 2048, 50 trees,
  depth 15);
- the 81-feature OC-14 capacity fixture;
- budgets 100, 1000, 1500 and 2000 with at least 3 SHAP calls per budget;
- BLAS/OpenMP pinned to one thread (deterministic KernelSHAP).

Per budget it reports every call duration, min/p50/p95/max/mean, the
theoretical capacity per worker/group (1 / mean_shap_seconds), the exact
configuration and the fixture hash.

This benchmark does NOT conclude on Kafka delivery: it isolates only the
arithmetic cost of KernelSHAP. It is never executed by pytest.

Usage:
    PYTHONPATH=src:. .venv/bin/python experiments/explainability/v2/capacity_benchmark.py \
        --csv tests/fixtures/explainability_v2/oc14_capacity_source.csv \
        --manifest tests/fixtures/explainability_v2/oc14_capacity_manifest.json \
        --output /tmp/oc14-capacity-<run>/offline-benchmark.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Deterministic floating point: pin BLAS/OpenMP BEFORE importing numpy
# (same pinning as the production compose environment).
for _thread_var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_thread_var, "1")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
for _path in (str(SRC_DIR), str(PROJECT_ROOT / "experiments" / "explainability" / "v2")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from dasma_kafka.explainability_v2.config import ExplainabilityV2Config  # noqa: E402
from shap_stability_runner import build_model, load_group_rows, shap_vector  # noqa: E402

DEFAULT_BUDGETS = [100, 1000, 1500, 2000]
DEFAULT_CALLS_PER_BUDGET = 3


def sha256_of_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def offline_config(manifest: Dict[str, Any], max_budget: int) -> ExplainabilityV2Config:
    """Production-shaped v2 config for the offline measurement."""
    env = manifest["campaign_env"]
    return ExplainabilityV2Config(
        hstree_window_size=int(env["HSTREE_WINDOW_SIZE"]),
        hstree_num_trees=int(env["HSTREE_NUM_TREES"]),
        hstree_max_depth=int(env["HSTREE_MAX_DEPTH"]),
        hstree_initial_window_size=int(env["HSTREE_INITIAL_WINDOW_SIZE"]),
        shap_background_size=int(env["SHAP_BACKGROUND_SIZE"]),
        shap_nsamples=max_budget,
        shap_enabled=True,
        explainability_seed=int(env["EXPLAINABILITY_SEED"]),
    )


def run_offline_benchmark(
    csv_path: str,
    manifest: Dict[str, Any],
    budgets: List[int],
    calls_per_budget: int,
    base_seed: int = 42,
) -> Dict[str, Any]:
    """Measure KernelSHAP per budget; pure computation, no Kafka."""
    if calls_per_budget < 3:
        raise ValueError("OC-14 requires at least 3 SHAP calls per budget")
    env = manifest["campaign_env"]
    warmup_rows = int(env["HSTREE_INITIAL_WINDOW_SIZE"])
    group, feat_names, vectors = load_group_rows(csv_path, manifest["group"], warmup_rows + calls_per_budget)
    if len(vectors) < warmup_rows + 1:
        raise ValueError(f"fixture has {len(vectors)} usable rows, need at least {warmup_rows + 1}")

    max_budget = max(budgets)
    cfg = offline_config(manifest, max_budget)
    _, model, background, benchmark_group = build_model(cfg, feat_names, vectors[:warmup_rows])
    targets = vectors[warmup_rows : warmup_rows + calls_per_budget]

    budget_results = []
    for budget in sorted(set(budgets)):
        durations_ms: List[float] = []
        for call_index, target in enumerate(targets):
            seed = base_seed + call_index
            start = time.perf_counter()
            shap_vector(cfg, model, background, feat_names, target, budget, seed)
            durations_ms.append((time.perf_counter() - start) * 1000.0)
        durations_sorted = sorted(durations_ms)
        mean_ms = sum(durations_sorted) / len(durations_sorted)
        mean_seconds = mean_ms / 1000.0
        budget_results.append(
            {
                "budget": budget,
                "durations_ms": durations_sorted,
                "min_ms": durations_sorted[0],
                "p50_ms": percentile_of(durations_sorted, 0.50),
                "p95_ms": percentile_of(durations_sorted, 0.95),
                "max_ms": durations_sorted[-1],
                "mean_ms": mean_ms,
                "capacity_per_group": (1.0 / mean_seconds) if mean_seconds > 0 else None,
            }
        )

    return {
        "campaign": "oc14-explainability-v2-capacity",
        "kind": "offline_kernelshap",
        "fixture": {
            "path": str(csv_path),
            "sha256": sha256_of_file(Path(csv_path)),
        },
        "configuration": {
            "group": group,
            "feature_count": len(feat_names),
            "warmup_rows": warmup_rows,
            "background_size": cfg.shap_background_size,
            "hstree_initial_window_size": cfg.hstree_initial_window_size,
            "hstree_window_size": cfg.hstree_window_size,
            "hstree_num_trees": cfg.hstree_num_trees,
            "hstree_max_depth": cfg.hstree_max_depth,
            "explainability_seed": cfg.explainability_seed,
            "calls_per_budget": calls_per_budget,
            "blas_threads_pinned": True,
        },
        "budgets": budget_results,
    }


def percentile_of(sorted_values: List[float], fraction: float) -> float:
    """Linear-interpolation percentile (shared semantics with capacity_report)."""
    if not sorted_values:
        raise ValueError("percentile of an empty list")
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = fraction * (len(sorted_values) - 1)
    lower = int(position // 1)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--csv", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--budgets", default="100,1000,1500,2000")
    parser.add_argument("--calls-per-budget", type=int, default=DEFAULT_CALLS_PER_BUDGET)
    parser.add_argument("--base-seed", type=int, default=42)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    budgets = [int(budget) for budget in args.budgets.split(",") if budget.strip()]
    try:
        report = run_offline_benchmark(
            args.csv, manifest, budgets, args.calls_per_budget, args.base_seed
        )
    except (ValueError, KeyError) as exc:
        print(f"[oc14-offline] FAILED: {exc}", file=sys.stderr)
        return 1
    Path(args.output).write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    for entry in report["budgets"]:
        print(
            f"[oc14-offline] budget {entry['budget']}: mean {entry['mean_ms']:.1f} ms | "
            f"p95 {entry['p95_ms']:.1f} ms | capacity {entry['capacity_per_group']:.3f} ev/s/group"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
