"""Local implementation of the HSTree algorithm described by Tan et al. (2011)."""

from __future__ import annotations

from typing import Any

import numpy as np
from pysad.models import HalfSpaceTrees


class PaperHalfSpaceTrees(HalfSpaceTrees):
    """Half-Space Trees with per-tree work spaces and terminal-node scoring."""

    def __init__(
        self,
        feature_mins: Any,
        feature_maxes: Any,
        window_size: int = 100,
        num_trees: int = 25,
        max_depth: int = 15,
        initial_window_X: Any = None,
        size_limit_ratio: float = 0.1,
    ) -> None:
        if window_size < 1 or num_trees < 1 or max_depth < 1:
            raise ValueError("window_size, num_trees, and max_depth must be positive")
        if not 0.0 <= size_limit_ratio <= 1.0:
            raise ValueError("size_limit_ratio must be between 0 and 1")

        mins = np.asarray(feature_mins, dtype=np.float64)
        maxs = np.asarray(feature_maxes, dtype=np.float64)
        if mins.ndim != 1 or maxs.shape != mins.shape or mins.size == 0:
            raise ValueError("feature_mins and feature_maxes must be matching non-empty vectors")
        if not np.all(np.isfinite(mins)) or not np.all(np.isfinite(maxs)) or np.any(maxs < mins):
            raise ValueError("feature bounds must be finite and maxes must not be below mins")

        self.window_size = int(window_size)
        self.num_trees = int(num_trees)
        self.max_depth = int(max_depth)
        self.feature_mins = mins
        self.feature_maxes = maxs
        self.num_dimensions = int(maxs.size)
        self.size_limit_ratio = float(size_limit_ratio)
        self.size_limit = self.size_limit_ratio * self.window_size
        widths = np.where(maxs > mins, maxs - mins, 1.0)
        self.work_ranges: list[tuple[np.ndarray, np.ndarray]] = []
        self.roots = []

        for _ in range(self.num_trees):
            s = np.random.uniform(0.0, 1.0, size=self.num_dimensions)
            margin = np.maximum(s, 1.0 - s)
            low = mins + (s - 2.0 * margin) * widths
            high = mins + (s + 2.0 * margin) * widths
            self.work_ranges.append((low.copy(), high.copy()))
            self.roots.append(self._build_single_hs_tree(low.copy(), high.copy(), 0))

        self.is_first_window = True
        self.step = 0
        if initial_window_X is not None and len(initial_window_X) > 0:
            self.fit(initial_window_X)

    def _terminal_score(self, x: np.ndarray, node: Any) -> float:
        while node.k < self.max_depth and node.r_mass > self.size_limit:
            node = node.right if x[node.split_att] > node.split_value else node.left
        return float(node.r_mass * (2**node.k))

    def score_partial(self, X: Any, y: Any = None) -> float:
        """Return the negated sum of terminal-node reference masses."""
        del y
        values = np.asarray(X, dtype=np.float64)
        if values.shape != (self.num_dimensions,):
            raise ValueError(f"expected a vector of shape ({self.num_dimensions},)")
        return -sum(self._terminal_score(values, root) for root in self.roots)


def create_hstree_model(variant: str, **kwargs: Any) -> HalfSpaceTrees:
    """Create the requested HSTree implementation; unknown variants are errors."""
    if variant == "pysad":
        return HalfSpaceTrees(**kwargs)
    if variant == "paper":
        return PaperHalfSpaceTrees(**kwargs)
    raise ValueError(f"unknown HSTree variant: {variant!r}")
