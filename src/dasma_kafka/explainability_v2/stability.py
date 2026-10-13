"""Pure metric helpers for the explainability-v2 SHAP stability benchmark.

These functions are deliberately free of any SHAP / Kafka / heavy dependency so
they can be unit-tested independently of SHAP runtime cost (see the v2 backlog,
P1 "Build Offline SHAP Stability Runner").

Two facets are measured:

1. Convergence to a reference budget — does a lower ``nsamples`` budget produce
   the same important explanations as a high reference budget?
   Metrics: Spearman on per-feature importance ranking, Jaccard@k of top
   features, sign-flip rate on top features, relative mean absolute difference.
2. Sampling instability — at a fixed budget, how much do attributions move when
   only the sampling seed changes? (cross-seed dispersion, sum conserved).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

import numpy as np


# Acceptance targets from the v2 backlog (P1 Task 4).
VALIDATION_TARGETS: Dict[str, float] = {
    "spearman": 0.90,
    "jaccard_at_10": 0.80,
    "sign_flip_rate": 0.05,
    "rel_mean_abs_diff": 0.10,
}
SCIENTIFIC_TARGETS: Dict[str, float] = {
    "spearman": 0.95,
    "jaccard_at_10": 0.90,
    "sign_flip_rate": 0.02,
    "rel_mean_abs_diff": 0.05,
}


def _rankdata(a: Sequence[float]) -> np.ndarray:
    """Tie-aware average ranks (small, dependency-free Spearman helper)."""
    arr = np.asarray(a, dtype=float)
    n = arr.size
    if n == 0:
        return np.empty(0, dtype=float)
    sorter = np.argsort(arr, kind="mergesort")
    arr_sorted = arr[sorter]
    positions = np.arange(1, n + 1, dtype=float)
    is_new = np.r_[True, arr_sorted[1:] != arr_sorted[:-1]]
    group = np.cumsum(is_new) - 1
    sums = np.zeros(group[-1] + 1, dtype=float)
    counts = np.zeros(group[-1] + 1, dtype=float)
    np.add.at(sums, group, positions)
    np.add.at(counts, group, 1.0)
    mean_ranks = sums / counts
    ranks_sorted = mean_ranks[group]
    out = np.empty(n, dtype=float)
    out[sorter] = ranks_sorted
    return out


def spearman(a: Sequence[float], b: Sequence[float]) -> float:
    """Spearman rank correlation between two importance vectors.

    Returns 1.0 when both vectors are constant and equal in ordering terms,
    0.0 when exactly one is constant (undefined ordering).
    """
    ra, rb = _rankdata(a), _rankdata(b)
    sa, sb = ra.std(), rb.std()
    if sa == 0.0 and sb == 0.0:
        return 1.0
    if sa == 0.0 or sb == 0.0:
        return 0.0
    return float(np.corrcoef(ra, rb)[0, 1])


def jaccard_at_k(a: Sequence[float], b: Sequence[float], k: int) -> float:
    """Jaccard similarity of the top-``k`` features by absolute value."""
    va, vb = np.abs(np.asarray(a, float)), np.abs(np.asarray(b, float))
    k = min(k, va.size, vb.size)
    if k <= 0:
        return 1.0
    top_a = set(np.argsort(va, kind="mergesort")[::-1][:k].tolist())
    top_b = set(np.argsort(vb, kind="mergesort")[::-1][:k].tolist())
    union = top_a | top_b
    if not union:
        return 1.0
    return len(top_a & top_b) / len(union)


def sign_flip_rate(
    a: Sequence[float],
    b: Sequence[float],
    k: int,
    reference: Sequence[float] | None = None,
) -> float:
    """Fraction of the top-``k`` features (ranked by ``reference`` or ``b``)
    whose sign differs between ``a`` and ``b`` (both non-zero)."""
    va, vb = np.asarray(a, float), np.asarray(b, float)
    ref = np.abs(np.asarray(reference, float)) if reference is not None else np.abs(vb)
    k = min(k, va.size)
    if k <= 0:
        return 0.0
    idx = np.argsort(ref, kind="mergesort")[::-1][:k]
    flips = int(np.sum(np.sign(va[idx]) * np.sign(vb[idx]) < 0))
    return flips / k


def relative_mean_abs_diff(a: Sequence[float], b: Sequence[float]) -> float:
    """mean(|a-b|) / mean(|b|): typical relative gap to the reference ``b``."""
    va, vb = np.asarray(a, float), np.asarray(b, float)
    denom = float(np.mean(np.abs(vb)))
    num = float(np.mean(np.abs(va - vb)))
    if denom == 0.0:
        return 0.0 if num == 0.0 else float("inf")
    return num / denom


def cross_seed_instability(vectors: Sequence[Sequence[float]]) -> float:
    """Relative dispersion of attributions across seeds at a fixed budget.

    mean over features of std(seed)/scale, where scale is the mean absolute
    attribution. 0 means perfectly stable; ~1 means dispersion as large as the
    signal itself. Returns 0.0 for a single vector.
    """
    mat = np.asarray(vectors, dtype=float)
    if mat.ndim != 2 or mat.shape[0] < 2:
        return 0.0
    per_feature_std = mat.std(axis=0)
    scale = float(np.mean(np.abs(mat)))
    if scale == 0.0:
        return 0.0
    return float(np.mean(per_feature_std) / scale)


def compare_to_reference(
    attribution: Sequence[float],
    reference: Sequence[float],
    k: int = 10,
) -> Dict[str, float]:
    """All convergence metrics for one attribution vector vs the reference."""
    return {
        "spearman": spearman(attribution, reference),
        "jaccard_at_10": jaccard_at_k(attribution, reference, k),
        "sign_flip_rate": sign_flip_rate(attribution, reference, k, reference=reference),
        "rel_mean_abs_diff": relative_mean_abs_diff(attribution, reference),
    }


def aggregate(rows: Sequence[Dict[str, float]]) -> Dict[str, float]:
    """Mean of each metric across benchmark rows (inf-safe)."""
    if not rows:
        return {}
    keys = rows[0].keys()
    out: Dict[str, float] = {}
    for key in keys:
        vals = [r[key] for r in rows if np.isfinite(r[key])]
        out[key] = float(np.mean(vals)) if vals else float("inf")
    return out


def meets_targets(metrics: Dict[str, float], targets: Dict[str, float]) -> bool:
    """True if aggregated metrics satisfy a target profile."""
    return (
        metrics.get("spearman", 0.0) >= targets["spearman"]
        and metrics.get("jaccard_at_10", 0.0) >= targets["jaccard_at_10"]
        and metrics.get("sign_flip_rate", 1.0) <= targets["sign_flip_rate"]
        and metrics.get("rel_mean_abs_diff", float("inf")) <= targets["rel_mean_abs_diff"]
    )


@dataclass
class BudgetResult:
    """Aggregated metrics for one ``nsamples`` budget vs the reference."""

    nsamples: int
    convergence: Dict[str, float]
    instability: float
    seconds_per_shap: float


def smallest_stable_budget(
    results: Sequence[BudgetResult],
    targets: Dict[str, float],
) -> int | None:
    """Smallest ``nsamples`` (below the reference) meeting the target profile."""
    for res in sorted(results, key=lambda r: r.nsamples):
        if meets_targets(res.convergence, targets):
            return res.nsamples
    return None
