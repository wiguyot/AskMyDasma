#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline SHAP stability benchmark for explainability-v2.

Industrialises the "instability vs nsamples" investigation (see
``docs/models/explainability/v2/explainabilities-problems.md``) into a reproducible
runner over real data.

It builds the SAME local HSTree model + SHAP background as the production
explainability service (``HSTreeModelManager`` / ``SHAPExplainer``), then, for a
set of ``nsamples`` budgets, measures:

* convergence to a high-budget reference (Spearman, Jaccard@10, sign-flip rate,
  relative mean abs diff) — "from which budget do explanations stop changing?";
* sampling instability (cross-seed dispersion at a fixed budget);
* runtime per SHAP call.

It writes JSON metrics and a Markdown decision log, and recommends the smallest
budget meeting the validation / scientific target profiles.

BLAS/OpenMP are pinned to a single thread so the regression is deterministic
(see ``docs/models/explainability/v2/explainabilities-problems.md``, action 1).

Usage:
    PYTHONPATH=src:. .venv/bin/python experiments/explainability/v2/shap_stability_runner.py \
        --csv data/source.csv --budgets 100,300,700 --reference 700 \
        --benchmark-rows 6 --seeds 3 --background 40 --warmup 200
"""

import os

# Must be set before numpy / BLAS import to take effect.
for _var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_var, "1")

import argparse
import csv
import json
import logging
import random
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

ROOT_DIR = Path(__file__).resolve().parents[3]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dasma_kafka.explainability_v2.config import ExplainabilityV2Config  # noqa: E402
from dasma_kafka.explainability_v2.model import HSTreeModelManager  # noqa: E402
from dasma_kafka.explainability_v2.shap_engine import SHAPExplainer  # noqa: E402
from dasma_kafka.explainability_v2 import stability as S  # noqa: E402

EXCLUDED = {"timestamp", "Group", "group", "Cycle", "LabviewTime", "LabViewTime", "time", "StepNumber", "ActionNumber"}
logging.disable(logging.CRITICAL)
_LOG = logging.getLogger("stability")


def load_group_rows(csv_path: str, group: Optional[str], needed: int, scan_cap: int = 60000):
    """Read rows for one group from the CSV, plus the numeric feature schema."""
    with open(csv_path, newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        rows = list(zip(range(len(header)), header))
        g_idx = next((i for i, h in enumerate(header) if h in ("Group", "group")), None)

        # First pass (capped) to choose the most frequent group if not given.
        sampled: List[List[str]] = []
        counts: Counter = Counter()
        for n, row in enumerate(reader):
            if not row:
                continue
            sampled.append(row)
            if g_idx is not None:
                counts[row[g_idx]] += 1
            if n + 1 >= scan_cap:
                break
        if group is None and counts:
            group = counts.most_common(1)[0][0]

    # Determine numeric feature columns from the first matching row.
    def is_num(v: str) -> bool:
        try:
            float(v)
            return True
        except (TypeError, ValueError):
            return False

    feat_idx: List[int] = []
    first = None
    for row in sampled:
        if g_idx is None or row[g_idx] == group:
            first = row
            break
    if first is None:
        raise SystemExit(f"No rows found for group {group!r} within scan window.")
    for i, name in enumerate(header):
        if name in EXCLUDED:
            continue
        if i < len(first) and is_num(first[i]):
            feat_idx.append(i)
    feat_idx.sort(key=lambda i: header[i])
    feat_names = [header[i] for i in feat_idx]

    vectors: List[np.ndarray] = []
    for row in sampled:
        if g_idx is not None and row[g_idx] != group:
            continue
        vec = np.array(
            [float(row[i]) if i < len(row) and is_num(row[i]) else 0.0 for i in feat_idx],
            dtype=np.float64,
        )
        vectors.append(vec)
        if len(vectors) >= needed:
            break
    return group, feat_names, vectors


def build_model(cfg: ExplainabilityV2Config, feat_names, warmup_vectors):
    """Build the local HSTree model + SHAP background like production."""
    np.random.seed(cfg.explainability_seed)
    random.seed(cfg.explainability_seed)
    mgr = HSTreeModelManager(cfg, _LOG)
    group = "benchmark"
    for vec in warmup_vectors:
        mgr.fit_score(group, vec)
    bg = mgr.get_background_data(group)
    model = mgr.get_model(group)
    if bg is None or model is None:
        raise SystemExit(
            "Model/background not ready: increase --warmup (need >= initial_window + background)."
        )
    return mgr, model, bg, group


def shap_vector(cfg, model, bg, feat_names, target, nsamples, seed):
    cfg.shap_nsamples = nsamples
    expl = SHAPExplainer(cfg, _LOG)
    expl.set_feature_names(feat_names)
    np.random.seed(seed)
    random.seed(seed)
    res = expl.compute_shap_values(model=model, features=target, background_data=bg)
    sv = res["shap_values"]
    if sv is None:
        return None
    return np.array([sv[f] for f in feat_names], dtype=np.float64)


def main() -> None:
    p = argparse.ArgumentParser(description="explainability-v2 SHAP stability benchmark")
    p.add_argument("--csv", default="data/source.csv")
    p.add_argument("--group", default=None, help="group key (default: most frequent)")
    p.add_argument("--budgets", default="100,300,700", help="comma-separated nsamples budgets")
    p.add_argument("--reference", type=int, default=None, help="reference nsamples (default: max budget)")
    p.add_argument("--benchmark-rows", type=int, default=6)
    p.add_argument("--seeds", type=int, default=3, help="seeds per budget for instability")
    p.add_argument("--background", type=int, default=40)
    p.add_argument("--warmup", type=int, default=200, help="rows used to build the model")
    p.add_argument("--base-seed", type=int, default=42)
    p.add_argument("--out-dir", default=os.path.join("experiments", "explainability", "v2", "reports"))
    args = p.parse_args()

    budgets = sorted({int(b) for b in args.budgets.split(",") if b.strip()})
    reference = args.reference or max(budgets)
    all_budgets = sorted(set(budgets) | {reference})
    needed = args.warmup + args.benchmark_rows

    print(f"[stability] loading {args.csv} (group={args.group or 'auto'}, need {needed} rows)")
    group, feat_names, vectors = load_group_rows(args.csv, args.group, needed)
    if len(vectors) < needed:
        raise SystemExit(f"Only {len(vectors)} rows for group {group!r}; need {needed}.")
    n_feat = len(feat_names)
    warmup_vectors = vectors[: args.warmup]
    bench = vectors[args.warmup : args.warmup + args.benchmark_rows]
    print(f"[stability] group={group!r} | features={n_feat} | warmup={len(warmup_vectors)} | bench={len(bench)}")
    print(f"[stability] budgets={budgets} | reference={reference} | seeds={args.seeds}")
    print(f"[stability] KernelSHAP auto-rule for this model ~ 2*{n_feat}+2048 = {2 * n_feat + 2048}")

    cfg = ExplainabilityV2Config(
        bootstrap_servers="x", schema_registry_url="x",
        hstree_initial_window_size=64, hstree_window_size=2048,
        hstree_num_trees=25, hstree_max_depth=15,
        shap_background_size=args.background, shap_nsamples=max(all_budgets),
        explainability_seed=args.base_seed,
    )
    if args.warmup < 64 + args.background:
        raise SystemExit("--warmup must be >= 64 (initial window) + --background.")
    _, model, bg, _ = build_model(cfg, feat_names, warmup_vectors)

    # Reference attributions (seed = base) for each benchmark row.
    print(f"[stability] computing reference (nsamples={reference}) ...")
    ref_vecs = [shap_vector(cfg, model, bg, feat_names, t, reference, args.base_seed) for t in bench]

    results: List[S.BudgetResult] = []
    runtime_rows: List[Dict] = []
    for ns in all_budgets:
        t0 = time.time()
        conv_rows: List[Dict[str, float]] = []
        instab_rows: List[float] = []
        n_calls = 0
        for ti, target in enumerate(bench):
            seed_vecs = []
            for s in range(args.seeds):
                v = shap_vector(cfg, model, bg, feat_names, target, ns, args.base_seed + s)
                n_calls += 1
                if v is not None:
                    seed_vecs.append(v)
            if not seed_vecs:
                continue
            # Convergence: seed-0 attribution vs reference (same seed).
            conv_rows.append(S.compare_to_reference(seed_vecs[0], ref_vecs[ti]))
            instab_rows.append(S.cross_seed_instability(seed_vecs))
        dt = time.time() - t0
        conv = S.aggregate(conv_rows)
        instab = float(np.mean(instab_rows)) if instab_rows else 0.0
        spc = dt / n_calls if n_calls else 0.0
        results.append(S.BudgetResult(nsamples=ns, convergence=conv, instability=instab, seconds_per_shap=spc))
        runtime_rows.append({"nsamples": ns, "seconds_total": round(dt, 2),
                             "seconds_per_shap": round(spc, 4), "n_calls": n_calls})
        print(f"[stability] nsamples={ns:>5} | spearman={conv.get('spearman', float('nan')):.3f}"
              f" | jaccard@10={conv.get('jaccard_at_10', float('nan')):.3f}"
              f" | instab={instab*100:5.1f}% | {spc*1000:.0f} ms/shap")

    val = S.smallest_stable_budget([r for r in results if r.nsamples < reference], S.VALIDATION_TARGETS)
    sci = S.smallest_stable_budget([r for r in results if r.nsamples < reference], S.SCIENTIFIC_TARGETS)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    config_block = {
        "csv": args.csv, "group": group, "n_features": n_feat,
        "budgets": budgets, "reference_nsamples": reference,
        "benchmark_rows": len(bench), "seeds": args.seeds,
        "background_size": args.background, "warmup_rows": len(warmup_vectors),
        "base_seed": args.base_seed, "threads_pinned": True,
        "kernelshap_auto_rule": 2 * n_feat + 2048,
    }
    metrics_doc = {
        "config": config_block,
        "per_budget": [
            {"nsamples": r.nsamples, **r.convergence, "instability": r.instability}
            for r in results
        ],
        "decision": {"validation_nsamples": val, "scientific_nsamples": sci,
                     "validation_targets": S.VALIDATION_TARGETS,
                     "scientific_targets": S.SCIENTIFIC_TARGETS},
    }
    (out_dir / "shap_stability_metrics.json").write_text(
        json.dumps(metrics_doc, indent=2) + "\n", encoding="utf-8")
    (out_dir / "shap_runtime_metrics.json").write_text(
        json.dumps({"config": config_block, "per_budget": runtime_rows}, indent=2) + "\n",
        encoding="utf-8")

    lines = [
        "# Explainability-v2 — SHAP stability decision log", "",
        f"- dataset: `{args.csv}` | group: `{group}` | features: {n_feat}",
        f"- reference: nsamples={reference} | background: {args.background} | seeds: {args.seeds}"
        f" | benchmark rows: {len(bench)} | BLAS threads pinned: yes",
        f"- KernelSHAP auto-rule for this model: ~{2 * n_feat + 2048}", "",
        "## Metrics vs reference (mean over benchmark rows)", "",
        "| nsamples | Spearman | Jaccard@10 | sign-flip | rel.diff | instability (cross-seed) | ms/shap |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r, rt in zip(results, runtime_rows):
        c = r.convergence
        ref_tag = " *(ref)*" if r.nsamples == reference else ""
        lines.append(
            f"| {r.nsamples}{ref_tag} | {c.get('spearman', float('nan')):.3f}"
            f" | {c.get('jaccard_at_10', float('nan')):.3f}"
            f" | {c.get('sign_flip_rate', float('nan')) * 100:.1f}%"
            f" | {c.get('rel_mean_abs_diff', float('nan')) * 100:.1f}%"
            f" | {r.instability * 100:.1f}%"
            f" | {rt['seconds_per_shap'] * 1000:.0f} |"
        )
    lines += [
        "", "## Decision", "",
        f"- Smallest budget meeting **validation** targets "
        f"(Spearman≥{S.VALIDATION_TARGETS['spearman']}, Jaccard@10≥{S.VALIDATION_TARGETS['jaccard_at_10']}, "
        f"sign-flip≤{S.VALIDATION_TARGETS['sign_flip_rate']*100:.0f}%, rel.diff≤{S.VALIDATION_TARGETS['rel_mean_abs_diff']*100:.0f}%): "
        f"**{val if val is not None else 'none below reference — raise reference / add budgets'}**",
        f"- Smallest budget meeting **scientific** targets: "
        f"**{sci if sci is not None else 'none below reference — raise reference / add budgets'}**",
        "",
        "> Instability is the cross-seed dispersion at a fixed budget (sum conserved). "
        "It quantifies KernelSHAP's sampling noise; it shrinks as nsamples grows. "
        "Reproducibility across runs additionally requires pinned BLAS threads "
        "(see `docs/models/explainability/v2/explainabilities-problems.md`).",
    ]
    (out_dir / "explainability_decision_log.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"\n[stability] reports written to {out_dir}/")
    print(f"[stability] recommended validation nsamples: {val} | scientific: {sci}")


if __name__ == "__main__":
    main()
