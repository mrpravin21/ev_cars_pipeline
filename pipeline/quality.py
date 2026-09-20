"""
quality.py — fail-closed gate before warehouse loads (Week4/5 pattern).
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when a quality check fails — pipeline must not load."""


def check_row_count(rows: list, min_rows: int = 1) -> dict:
    count = len(rows)
    return {
        "check": "row_count",
        "passed": count >= min_rows,
        "detail": f"{count} rows (min: {min_rows})",
    }


def check_required_keys(rows: list[dict[str, Any]]) -> dict:
    bad = [
        r
        for r in rows
        if not r.get("natural_key") or not r.get("brand_name") or not r.get("model_name")
    ]
    return {
        "check": "required_natural_keys",
        "passed": len(bad) == 0,
        "detail": f"{len(bad)} rows missing natural/brand/model",
    }


def check_valid_kind(rows: list[dict[str, Any]]) -> dict:
    valid = {"car", "bike"}
    bad = [r for r in rows if r.get("kind_code") not in valid]
    return {
        "check": "valid_kind",
        "passed": len(bad) == 0,
        "detail": f"{len(bad)} rows with invalid kind_code",
    }


def check_prices_non_negative(rows: list[dict[str, Any]]) -> dict:
    bad = []
    for r in rows:
        for field in ("price_lo_npr", "price_hi_npr", "price_mid_npr"):
            val = r.get(field)
            if val is not None and val < 0:
                bad.append(r)
                break
    return {
        "check": "prices_non_negative",
        "passed": len(bad) == 0,
        "detail": f"{len(bad)} rows with negative prices",
    }


def check_price_band_order(rows: list[dict[str, Any]]) -> dict:
    bad = [
        r
        for r in rows
        if r.get("price_lo_npr") is not None
        and r.get("price_hi_npr") is not None
        and r["price_lo_npr"] > r["price_hi_npr"]
    ]
    return {
        "check": "price_lo_le_hi",
        "passed": len(bad) == 0,
        "detail": f"{len(bad)} rows with price_lo > price_hi",
    }


def check_unique_natural_keys(rows: list[dict[str, Any]]) -> dict:
    seen = set()
    dupes = 0
    for r in rows:
        key = r.get("natural_key")
        if key in seen:
            dupes += 1
        else:
            seen.add(key)
    return {
        "check": "unique_natural_keys",
        "passed": dupes == 0,
        "detail": f"{dupes} duplicate natural_key values",
    }


def check_specs_plausible(rows: list[dict[str, Any]]) -> dict:
    """Soft-plausibility hard-fail only for absurd outliers."""
    bad = []
    for r in rows:
        batt = r.get("battery_kwh")
        rng = r.get("range_km")
        motor = r.get("motor_kw")
        if batt is not None and not (0 < batt < 500):
            bad.append(r)
            continue
        if rng is not None and not (0 < rng < 2000):
            bad.append(r)
            continue
        if motor is not None and not (0 < motor < 2000):
            bad.append(r)
    return {
        "check": "specs_plausible",
        "passed": len(bad) == 0,
        "detail": f"{len(bad)} rows with implausible battery/range/motor",
    }


def _log_summary(checks: list[dict]) -> None:
    logger.info("Quality check summary")
    logger.info("%-28s %-8s %s", "check", "status", "detail")
    logger.info("-" * 72)
    for c in checks:
        status = "PASS" if c["passed"] else "FAIL"
        logger.info("%-28s %-8s %s", c["check"], status, c["detail"])


def run_catalog_quality_checks(
    rows: list[dict[str, Any]], min_rows: int = 50
) -> dict:
    checks = [
        check_row_count(rows, min_rows=min_rows),
        check_required_keys(rows),
        check_valid_kind(rows),
        check_prices_non_negative(rows),
        check_price_band_order(rows),
        check_unique_natural_keys(rows),
        check_specs_plausible(rows),
    ]
    _log_summary(checks)
    failed = [c for c in checks if not c["passed"]]
    if failed:
        first = failed[0]
        raise DataQualityError(
            f"Quality check failed: {first['check']} — {first['detail']}"
        )
    logger.info("Quality gate passed: %s catalog rows", len(rows))
    return {"passed": True, "checks": checks, "row_count": len(rows)}


def run_price_change_quality_checks(rows: list[dict[str, Any]]) -> dict:
    # Price-change feed can be empty on quiet days — allow zero rows.
    checks = [
        check_row_count(rows, min_rows=0),
    ]
    bad_dates = [r for r in rows if not r.get("change_date") or not r.get("natural_event_key")]
    checks.append(
        {
            "check": "price_change_keys",
            "passed": len(bad_dates) == 0,
            "detail": f"{len(bad_dates)} rows missing change_date/natural_event_key",
        }
    )
    _log_summary(checks)
    failed = [c for c in checks if not c["passed"]]
    if failed:
        first = failed[0]
        raise DataQualityError(
            f"Quality check failed: {first['check']} — {first['detail']}"
        )
    return {"passed": True, "checks": checks, "row_count": len(rows)}
