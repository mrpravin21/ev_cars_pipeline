"""
config.py — env-driven settings for the EV catalog pipeline.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOG_DIR = PROJECT_ROOT / "logs"
SQL_DIR = PROJECT_ROOT / "sql"

RAW_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

DB_CONFIG = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "ev_dw"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "postgres"),
)

REQUEST_TIMEOUT_SEC = float(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
REQUEST_SLEEP_SEC = float(os.getenv("REQUEST_SLEEP_SEC", "0.6"))
USER_AGENT = os.getenv(
    "USER_AGENT",
    "ev-cars-pipeline/1.0 (course project; attribution: Nepal AutoMart)",
)
DETAIL_MAX_MODELS = int(os.getenv("DETAIL_MAX_MODELS", "0"))

BASE_URL = "https://nepalautomart.com"
CSV_ENDPOINTS = {
    "ev_tracker": f"{BASE_URL}/api/data/ev-price-tracker",
    "price_changes": f"{BASE_URL}/api/data/price-changes",
    "car_brand_snapshot": f"{BASE_URL}/api/data/new-car-price-snapshot",
    "bike_brand_snapshot": f"{BASE_URL}/api/data/new-bike-price-snapshot",
    "provincial_tax": f"{BASE_URL}/api/data/provincial-vehicle-tax",
    "resale_by_brand": f"{BASE_URL}/api/data/resale-value-by-brand",
}

SOURCE_SYSTEM = "nepalautomart"
ATTRIBUTION = (
    "Data © Nepal AutoMart — free to reuse with attribution. "
    "Primary datasets: https://nepalautomart.com/data/ev-price-tracker"
)

# Heuristic WLTP normalization factors (NOT certified).
# Stored as config so the pipeline stays reproducible and auditable.
WLTP_ESTIMATE_FACTORS = {
    "WLTP": 1.0,
    "CLTC": 0.82,
    "NEDC": 0.78,
    "MIDC": 0.88,
    "ARA": 0.90,
    "UNKNOWN": None,
}
