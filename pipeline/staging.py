"""
staging.py — land raw extracts into staging_* tables for a pipeline run.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json, execute_batch

logger = logging.getLogger(__name__)


def start_run(conn: PgConnection, mode: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (mode, status)
            VALUES (%s, 'running')
            RETURNING run_id
            """,
            (mode,),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    logger.info("Started pipeline_runs run_id=%s mode=%s", run_id, mode)
    return run_id


def finish_run(
    conn: PgConnection,
    run_id: int,
    status: str,
    rows_catalog: int = 0,
    rows_details: int = 0,
    rows_price_chg: int = 0,
    error_message: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET finished_at = CURRENT_TIMESTAMP,
                status = %s,
                rows_catalog = %s,
                rows_details = %s,
                rows_price_chg = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (status, rows_catalog, rows_details, rows_price_chg, error_message, run_id),
        )
    conn.commit()
    logger.info("Finished run_id=%s status=%s", run_id, status)


def clear_staging_for_run(conn: PgConnection, run_id: int) -> None:
    tables = (
        "staging_ev_tracker",
        "staging_price_changes",
        "staging_brand_snapshot",
        "staging_provincial_tax",
        "staging_model_details",
    )
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"DELETE FROM {t} WHERE run_id = %s", (run_id,))
    conn.commit()


def _blank_to_none(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def stage_ev_tracker(
    conn: PgConnection,
    run_id: int,
    rows: list[dict[str, Any]],
    source_file: str,
    as_of: date,
) -> int:
    payload = []
    for r in rows:
        payload.append(
            {
                "run_id": run_id,
                "kind": (r.get("kind") or "").strip().lower(),
                "brand": (r.get("brand") or "").strip(),
                "model": (r.get("model") or "").strip(),
                "price_lo": _blank_to_none(r.get("priceLo") or r.get("price_lo")),
                "price_hi": _blank_to_none(r.get("priceHi") or r.get("price_hi")),
                "battery_kwh": _blank_to_none(
                    r.get("batteryKwh") if "batteryKwh" in r else r.get("battery_kwh")
                ),
                "range_km": _blank_to_none(
                    r.get("rangeKm") if "rangeKm" in r else r.get("range_km")
                ),
                "motor_kw": _blank_to_none(
                    r.get("motorKw") if "motorKw" in r else r.get("motor_kw")
                ),
                "source_file": source_file,
                "as_of_date": as_of,
            }
        )
    sql = """
    INSERT INTO staging_ev_tracker (
        run_id, kind, brand, model, price_lo, price_hi,
        battery_kwh, range_km, motor_kw, source_file, as_of_date
    ) VALUES (
        %(run_id)s, %(kind)s, %(brand)s, %(model)s, %(price_lo)s, %(price_hi)s,
        %(battery_kwh)s, %(range_km)s, %(motor_kw)s, %(source_file)s, %(as_of_date)s
    )
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, payload, page_size=200)
    conn.commit()
    logger.info("Staged %s ev_tracker rows for run_id=%s", len(payload), run_id)
    return len(payload)


def stage_price_changes(
    conn: PgConnection,
    run_id: int,
    rows: list[dict[str, Any]],
    source_file: str,
) -> int:
    payload = []
    for r in rows:
        payload.append(
            {
                "run_id": run_id,
                "change_date": r.get("date"),
                "kind": r.get("kind"),
                "brand": r.get("brand"),
                "model": r.get("model"),
                "variant": r.get("variant"),
                "old_price": r.get("oldPrice"),
                "new_price": r.get("newPrice"),
                "pct_change": r.get("pct"),
                "source_file": source_file,
            }
        )
    sql = """
    INSERT INTO staging_price_changes (
        run_id, change_date, kind, brand, model, variant,
        old_price, new_price, pct_change, source_file
    ) VALUES (
        %(run_id)s, %(change_date)s, %(kind)s, %(brand)s, %(model)s, %(variant)s,
        %(old_price)s, %(new_price)s, %(pct_change)s, %(source_file)s
    )
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, payload, page_size=200)
    conn.commit()
    logger.info("Staged %s price_change rows", len(payload))
    return len(payload)


def stage_brand_snapshots(
    conn: PgConnection,
    run_id: int,
    kind: str,
    rows: list[dict[str, Any]],
    source_file: str,
    as_of: date,
) -> int:
    payload = [
        {
            "run_id": run_id,
            "kind": kind,
            "brand": r.get("brand"),
            "model_count": r.get("modelCount"),
            "min_price": r.get("minPrice"),
            "median_price": r.get("medianPrice"),
            "max_price": r.get("maxPrice"),
            "distributor": r.get("distributor"),
            "source_file": source_file,
            "as_of_date": as_of,
        }
        for r in rows
    ]
    sql = """
    INSERT INTO staging_brand_snapshot (
        run_id, kind, brand, model_count, min_price, median_price, max_price,
        distributor, source_file, as_of_date
    ) VALUES (
        %(run_id)s, %(kind)s, %(brand)s, %(model_count)s, %(min_price)s,
        %(median_price)s, %(max_price)s, %(distributor)s, %(source_file)s, %(as_of_date)s
    )
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, payload, page_size=200)
    conn.commit()
    return len(payload)


def stage_provincial_tax(
    conn: PgConnection,
    run_id: int,
    rows: list[dict[str, Any]],
    source_file: str,
    as_of: date,
) -> int:
    payload = [
        {
            "run_id": run_id,
            "province": r.get("province"),
            "verified": str(r.get("verified")).lower() in {"true", "1", "yes"},
            "vehicle_type": r.get("vehicleType"),
            "band": r.get("band"),
            "annual_tax_npr": r.get("annualTaxNpr"),
            "source_file": source_file,
            "as_of_date": as_of,
        }
        for r in rows
    ]
    sql = """
    INSERT INTO staging_provincial_tax (
        run_id, province, verified, vehicle_type, band, annual_tax_npr,
        source_file, as_of_date
    ) VALUES (
        %(run_id)s, %(province)s, %(verified)s, %(vehicle_type)s, %(band)s,
        %(annual_tax_npr)s, %(source_file)s, %(as_of_date)s
    )
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, payload, page_size=200)
    conn.commit()
    return len(payload)


def stage_model_details(
    conn: PgConnection,
    run_id: int,
    variants: list[dict[str, Any]],
    as_of: date,
) -> int:
    payload = []
    for v in variants:
        payload.append(
            {
                "run_id": run_id,
                "kind": v.get("kind"),
                "brand": v.get("brand"),
                "model": v.get("model"),
                "detail_url": v.get("detail_url"),
                "variant_name": v.get("variant_name"),
                "battery_kwh": v.get("battery_kwh"),
                "range_km": v.get("range_km"),
                "range_cycle": v.get("range_cycle"),
                "motor_kw": v.get("motor_kw"),
                "dc_charge_kw": v.get("dc_charge_kw"),
                "seating": v.get("seating"),
                "body_type": v.get("body_type"),
                "drivetrain": v.get("drivetrain"),
                "price_npr": v.get("price_npr"),
                "raw_json": Json(v),
                "as_of_date": as_of,
            }
        )
    sql = """
    INSERT INTO staging_model_details (
        run_id, kind, brand, model, detail_url, variant_name,
        battery_kwh, range_km, range_cycle, motor_kw, dc_charge_kw,
        seating, body_type, drivetrain, price_npr, raw_json, as_of_date
    ) VALUES (
        %(run_id)s, %(kind)s, %(brand)s, %(model)s, %(detail_url)s, %(variant_name)s,
        %(battery_kwh)s, %(range_km)s, %(range_cycle)s, %(motor_kw)s, %(dc_charge_kw)s,
        %(seating)s, %(body_type)s, %(drivetrain)s, %(price_npr)s, %(raw_json)s, %(as_of_date)s
    )
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, payload, page_size=100)
    conn.commit()
    logger.info("Staged %s model detail variants", len(payload))
    return len(payload)
