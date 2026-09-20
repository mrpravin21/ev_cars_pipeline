#!/usr/bin/env python3
"""Create database (if needed) and apply staging + warehouse + views SQL."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from pipeline.db import ensure_schema, get_connection
from pipeline.logging_setup import setup_logging

logger = setup_logging("init_db")


def create_database_if_missing() -> None:
    dbname = os.getenv("DB_NAME", "ev_dw")
    cfg = dict(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname="postgres",
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            if cur.fetchone():
                logger.info("Database %s already exists", dbname)
            else:
                cur.execute(f'CREATE DATABASE "{dbname}"')
                logger.info("Created database %s", dbname)
    finally:
        conn.close()


def main() -> None:
    create_database_if_missing()
    conn = get_connection()
    try:
        ensure_schema(conn)
        logger.info("Schema bootstrap complete")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
