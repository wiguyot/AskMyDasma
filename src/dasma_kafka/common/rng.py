"""Helpers for isolating process-global random number generators."""

from __future__ import annotations

import random
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def isolated_rng(seed: int) -> Iterator[None]:
    """Run a block with deterministic NumPy and Python random state.

    The process-global states are restored even when the block raises.  This
    helper is intentionally process-local and is not thread-safe: callers must
    ensure that no other thread uses either global generator concurrently.
    """

    import numpy as np

    numpy_state = np.random.get_state()
    python_state = random.getstate()
    try:
        np.random.seed(seed)
        random.seed(seed)
        yield
    finally:
        np.random.set_state(numpy_state)
        random.setstate(python_state)
