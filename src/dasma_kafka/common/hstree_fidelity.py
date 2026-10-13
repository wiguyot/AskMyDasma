"""Runtime checks for the local HSTree score fidelity contract."""

from __future__ import annotations

import math
from numbers import Real
from typing import Optional


def local_score_error(local_score: object) -> Optional[str]:
    if isinstance(local_score, bool) or not isinstance(local_score, Real):
        return "invalid_local_score"
    if not math.isfinite(float(local_score)):
        return "invalid_local_score"
    return None


def score_fidelity_error(
    upstream_score: object,
    local_score: object,
    *,
    mismatch_code: str = "hstree_score_mismatch",
) -> Optional[str]:
    """Return a stable error code when two model scores are unusable.

    ``None`` means that both values are finite real numbers and exactly equal.
    Booleans, strings, NaN and infinities are rejected rather than coerced.
    """

    if isinstance(upstream_score, bool) or not isinstance(upstream_score, Real):
        return "invalid_upstream_score"
    if not math.isfinite(float(upstream_score)):
        return "invalid_upstream_score"
    local_error = local_score_error(local_score)
    if local_error is not None:
        return local_error
    if float(upstream_score) != float(local_score):
        return mismatch_code
    return None
