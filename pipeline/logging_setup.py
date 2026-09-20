"""Shared logging setup — matches Week3/Week4 style.

Uses stdout (not stderr) so Airflow task logs do not mis-label INFO lines as ERROR.
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pipeline.config import LOG_DIR


def setup_logging(name: str = "ev_pipeline", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)s  %(filename)s:%(lineno)d  %(message)s"
    )

    # stdout: Airflow treats stderr lines as ERROR regardless of record level
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    log_path = Path(LOG_DIR) / "pipeline.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    pkg = logging.getLogger("pipeline")
    pkg.setLevel(level)
    if not pkg.handlers:
        pkg.addHandler(stream)
        pkg.addHandler(file_handler)
        pkg.propagate = False

    return logger
