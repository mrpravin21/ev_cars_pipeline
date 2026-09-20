#!/usr/bin/env python3
"""Thin CLI wrapper so `python scripts/run_pipeline.py` works from any cwd."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.etl import main

if __name__ == "__main__":
    main()
