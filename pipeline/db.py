"""
db.py — connection helper + schema bootstrap.
"""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as PgConnection

from pipeline.config import DB_CONFIG, SQL_DIR

logger = logging.getLogger(__name__)


def get_connection(**overrides) -> PgConnection:
    cfg = {**DB_CONFIG, **overrides}
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False
    return conn


def run_sql_file(conn: PgConnection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info("Applied SQL file %s", path.name)


def ensure_schema(conn: PgConnection) -> None:
    for name in (
        "schema_staging.sql",
        "schema_warehouse.sql",
        "views_analytics.sql",
    ):
        run_sql_file(conn, SQL_DIR / name)
