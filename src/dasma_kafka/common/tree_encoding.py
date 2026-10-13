"""Coordinate-wise ordinal coding preserving tree split decisions."""

from dataclasses import dataclass
from typing import Mapping

import numpy as np


@dataclass(frozen=True)
class OrdinalEncoder:
    thresholds: tuple[np.ndarray, ...]
    right_if: str

    def encode(self, X) -> np.ndarray:
        values = np.asarray(X, dtype=np.float64)
        if values.ndim == 1:
            values = values.reshape(1, -1)
        if values.ndim != 2 or values.shape[1] != len(self.thresholds):
            raise ValueError("ordinal encoder: incompatible feature shape")
        if not np.isfinite(values).all():
            raise ValueError("ordinal encoder requires finite features")
        if self.right_if == "float32_gt":
            with np.errstate(over="ignore"):
                values = values.astype(np.float32)
            if not np.isfinite(values).all():
                raise ValueError("features overflow float32")
        return np.column_stack([
            np.searchsorted(cuts, values[:, j], side="left")
            for j, cuts in enumerate(self.thresholds)
        ]).astype(np.float64)


def build_ordinal_encoder(
    thresholds_by_feature: Mapping[int, object], *, right_if: str, n_features: int
) -> OrdinalEncoder:
    """Support PySAD's float64 > and sklearn's float32-input > semantics."""
    if right_if not in ("float64_gt", "float32_gt") or n_features < 1:
        raise ValueError("invalid ordinal comparison or feature count")
    if any(j < 0 or j >= n_features for j in thresholds_by_feature):
        raise ValueError("invalid split feature")
    tables = []
    for j in range(n_features):
        cuts = np.unique(np.asarray(thresholds_by_feature.get(j, []), dtype=np.float64))
        if not np.isfinite(cuts).all() or len(cuts) >= 2**23:
            raise ValueError("thresholds cannot be encoded safely")
        cuts.setflags(write=False)
        tables.append(cuts)
    return OrdinalEncoder(tuple(tables), right_if)
