#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compatibility wrapper for the former `explainability.py` entry point.

File status:
- this file is no longer the reference implementation
- the active reference is `explainability-multi-thread-by-group.py`
- it is kept only to avoid breaking older local usage

To read the current service:
- simple entry point: `explainability-multi-thread-by-group.py`
- simple documentation: `explainability-for-beginners.md`
- technical documentation: `explainability.md`

The scientific logic is not redefined here.
This wrapper only delegates to the reference entry point.
"""

from __future__ import annotations

import runpy
from pathlib import Path


def main() -> None:
    script_path = Path(__file__).with_name("explainability-multi-thread-by-group.py")
    runpy.run_path(str(script_path), run_name="__main__")


if __name__ == "__main__":
    main()
