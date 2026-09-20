"""
load.py — idempotent dimension / fact upserts (ON CONFLICT).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from psycopg2.extensions import connection as PgConnection

from pipeline.transform import build_dim_date_row, date_key

logger = logging.getLogger(__name__)


def _executemany(conn: PgConnection, sql: str, rows: list[dict], label: str) -> int:
    if not rows:
        logger.info("No rows to load into %s — skipping", label)
        return 0
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            count = cur.rowcount
        conn.commit()
        logger.info("Upserted/affected %s rows for %s", count, label)
        return count
    except Exception:
        conn.rollback()
        logger.exception("Load failed for %s", label)
        raise


def ensure_dates(conn: PgConnection, dates: list[date]) -> None:
    rows = [build_dim_date_row(d) for d in sorted(set(dates))]
    sql = """
    INSERT INTO dim_date (
        date_key, full_date, year, quarter, month, month_name,
        week_of_year, day_of_week, day_name, is_weekend
    ) VALUES (
        %(date_key)s, %(full_date)s, %(year)s, %(quarter)s, %(month)s, %(month_name)s,
        %(week_of_year)s, %(day_of_week)s, %(day_name)s, %(is_weekend)s
    )
    ON CONFLICT (date_key) DO NOTHING
    """
    _executemany(conn, sql, rows, "dim_date")


def load_dim_brands(conn: PgConnection, brand_rows: list[dict[str, Any]]) -> None:
    # Prefer distributor from brand snapshot when present
    sql = """
    INSERT INTO dim_brand (kind_code, brand_name, distributor, updated_at)
    VALUES (%(kind_code)s, %(brand_name)s, %(distributor)s, CURRENT_TIMESTAMP)
    ON CONFLICT (kind_code, brand_name) DO UPDATE SET
        distributor = COALESCE(EXCLUDED.distributor, dim_brand.distributor),
        updated_at = CURRENT_TIMESTAMP
    """
    payload = []
    for r in brand_rows:
        payload.append(
            {
                "kind_code": r["kind_code"],
                "brand_name": r["brand_name"],
                "distributor": r.get("distributor"),
            }
        )
    _executemany(conn, sql, payload, "dim_brand")


def load_dim_brands_from_catalog(conn: PgConnection, catalog_rows: list[dict]) -> None:
    seen = set()
    payload = []
    for r in catalog_rows:
        key = (r["kind_code"], r["brand_name"])
        if key in seen:
            continue
        seen.add(key)
        payload.append(
            {
                "kind_code": r["kind_code"],
                "brand_name": r["brand_name"],
                "distributor": None,
            }
        )
    load_dim_brands(conn, payload)


def load_dim_vehicles(conn: PgConnection, catalog_rows: list[dict], as_of: date) -> None:
    sql = """
    INSERT INTO dim_vehicle (
        source_system, kind_code, brand_name, model_name, natural_key,
        detail_url, body_type, is_active, first_seen_date, last_seen_date, updated_at
    ) VALUES (
        %(source_system)s, %(kind_code)s, %(brand_name)s, %(model_name)s, %(natural_key)s,
        %(detail_url)s, %(body_type)s, TRUE, %(as_of)s, %(as_of)s, CURRENT_TIMESTAMP
    )
    ON CONFLICT (natural_key) DO UPDATE SET
        detail_url = COALESCE(EXCLUDED.detail_url, dim_vehicle.detail_url),
        body_type = COALESCE(EXCLUDED.body_type, dim_vehicle.body_type),
        is_active = TRUE,
        last_seen_date = EXCLUDED.last_seen_date,
        updated_at = CURRENT_TIMESTAMP
    """
    payload = []
    for r in catalog_rows:
        payload.append(
            {
                "source_system": r.get("source_system") or "nepalautomart",
                "kind_code": r["kind_code"],
                "brand_name": r["brand_name"],
                "model_name": r["model_name"],
                "natural_key": r["natural_key"],
                "detail_url": r.get("detail_url"),
                "body_type": r.get("body_type"),
                "as_of": as_of,
            }
        )
    _executemany(conn, sql, payload, "dim_vehicle")


def load_dim_tax(conn: PgConnection, tax_rows: list[dict]) -> None:
    if not tax_rows:
        return
    with conn.cursor() as cur:
        for r in tax_rows:
            cur.execute(
                """
                INSERT INTO dim_province (province_name)
                VALUES (%(province_name)s)
                ON CONFLICT (province_name) DO NOTHING
                """,
                r,
            )
            cur.execute(
                "SELECT province_key FROM dim_province WHERE province_name = %(province_name)s",
                r,
            )
            province_key = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO dim_tax_band (
                    province_key, vehicle_type, band, annual_tax_npr, verified
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (province_key, vehicle_type, band) DO UPDATE SET
                    annual_tax_npr = EXCLUDED.annual_tax_npr,
                    verified = EXCLUDED.verified
                """,
                (
                    province_key,
                    r["vehicle_type"],
                    r["band"],
                    r.get("annual_tax_npr"),
                    r.get("verified"),
                ),
            )
    conn.commit()
    logger.info("Upserted tax bands for %s input rows", len(tax_rows))


def _lookup_maps(conn: PgConnection) -> tuple[dict[str, int], dict[tuple[str, str], int]]:
    with conn.cursor() as cur:
        cur.execute("SELECT natural_key, vehicle_key FROM dim_vehicle")
        vehicle_map = {nk: vk for nk, vk in cur.fetchall()}
        cur.execute("SELECT kind_code, brand_name, brand_key FROM dim_brand")
        brand_map = {(k, b): bk for k, b, bk in cur.fetchall()}
    return vehicle_map, brand_map


def load_fact_catalog_snapshot(
    conn: PgConnection,
    catalog_rows: list[dict],
    run_id: int,
    full_reload: bool = False,
) -> int:
    vehicle_map, brand_map = _lookup_maps(conn)
    payload = []
    missing = 0
    for r in catalog_rows:
        vk = vehicle_map.get(r["natural_key"])
        bk = brand_map.get((r["kind_code"], r["brand_name"]))
        if vk is None or bk is None:
            missing += 1
            continue
        payload.append(
            {
                "as_of_date": r["as_of_date"],
                "date_key": r["date_key"],
                "vehicle_key": vk,
                "brand_key": bk,
                "kind_code": r["kind_code"],
                "price_lo_npr": r.get("price_lo_npr"),
                "price_hi_npr": r.get("price_hi_npr"),
                "price_mid_npr": r.get("price_mid_npr"),
                "battery_kwh": r.get("battery_kwh"),
                "range_km": r.get("range_km"),
                "range_cycle": r.get("range_cycle"),
                "range_km_wltp_est": r.get("range_km_wltp_est"),
                "motor_kw": r.get("motor_kw"),
                "dc_charge_kw": r.get("dc_charge_kw"),
                "seating": r.get("seating"),
                "npr_per_km": r.get("npr_per_km"),
                "npr_per_kwh": r.get("npr_per_kwh"),
                "spec_source": r.get("spec_source") or "csv",
                "run_id": run_id,
            }
        )
    if missing:
        logger.warning("%s catalog rows skipped — missing dim lookups", missing)

    conflict = """
    ON CONFLICT (as_of_date, vehicle_key) DO UPDATE SET
        brand_key = EXCLUDED.brand_key,
        kind_code = EXCLUDED.kind_code,
        price_lo_npr = EXCLUDED.price_lo_npr,
        price_hi_npr = EXCLUDED.price_hi_npr,
        price_mid_npr = EXCLUDED.price_mid_npr,
        battery_kwh = EXCLUDED.battery_kwh,
        range_km = EXCLUDED.range_km,
        range_cycle = EXCLUDED.range_cycle,
        range_km_wltp_est = EXCLUDED.range_km_wltp_est,
        motor_kw = EXCLUDED.motor_kw,
        dc_charge_kw = EXCLUDED.dc_charge_kw,
        seating = EXCLUDED.seating,
        npr_per_km = EXCLUDED.npr_per_km,
        npr_per_kwh = EXCLUDED.npr_per_kwh,
        spec_source = EXCLUDED.spec_source,
        run_id = EXCLUDED.run_id,
        loaded_at = CURRENT_TIMESTAMP
    """
    if not full_reload:
        # incremental: still upsert today's snapshot (idempotent re-run)
        pass

    sql = f"""
    INSERT INTO fact_ev_catalog_snapshot (
        as_of_date, date_key, vehicle_key, brand_key, kind_code,
        price_lo_npr, price_hi_npr, price_mid_npr,
        battery_kwh, range_km, range_cycle, range_km_wltp_est,
        motor_kw, dc_charge_kw, seating,
        npr_per_km, npr_per_kwh, spec_source, run_id
    ) VALUES (
        %(as_of_date)s, %(date_key)s, %(vehicle_key)s, %(brand_key)s, %(kind_code)s,
        %(price_lo_npr)s, %(price_hi_npr)s, %(price_mid_npr)s,
        %(battery_kwh)s, %(range_km)s, %(range_cycle)s, %(range_km_wltp_est)s,
        %(motor_kw)s, %(dc_charge_kw)s, %(seating)s,
        %(npr_per_km)s, %(npr_per_kwh)s, %(spec_source)s, %(run_id)s
    )
    {conflict}
    """
    return _executemany(conn, sql, payload, "fact_ev_catalog_snapshot")


def load_fact_variant_specs(
    conn: PgConnection, variant_rows: list[dict], run_id: int
) -> int:
    vehicle_map, _ = _lookup_maps(conn)
    payload = []
    for r in variant_rows:
        vk = vehicle_map.get(r["natural_key"])
        if vk is None:
            continue
        payload.append(
            {
                "as_of_date": r["as_of_date"],
                "vehicle_key": vk,
                "variant_name": r["variant_name"],
                "battery_kwh": r.get("battery_kwh"),
                "range_km": r.get("range_km"),
                "range_cycle": r.get("range_cycle"),
                "range_km_wltp_est": r.get("range_km_wltp_est"),
                "motor_kw": r.get("motor_kw"),
                "dc_charge_kw": r.get("dc_charge_kw"),
                "seating": r.get("seating"),
                "drivetrain": r.get("drivetrain"),
                "price_npr": r.get("price_npr"),
                "detail_url": r.get("detail_url"),
                "run_id": run_id,
            }
        )
    sql = """
    INSERT INTO fact_ev_variant_spec (
        as_of_date, vehicle_key, variant_name,
        battery_kwh, range_km, range_cycle, range_km_wltp_est,
        motor_kw, dc_charge_kw, seating, drivetrain, price_npr,
        detail_url, run_id
    ) VALUES (
        %(as_of_date)s, %(vehicle_key)s, %(variant_name)s,
        %(battery_kwh)s, %(range_km)s, %(range_cycle)s, %(range_km_wltp_est)s,
        %(motor_kw)s, %(dc_charge_kw)s, %(seating)s, %(drivetrain)s, %(price_npr)s,
        %(detail_url)s, %(run_id)s
    )
    ON CONFLICT (as_of_date, vehicle_key, variant_name) DO UPDATE SET
        battery_kwh = EXCLUDED.battery_kwh,
        range_km = EXCLUDED.range_km,
        range_cycle = EXCLUDED.range_cycle,
        range_km_wltp_est = EXCLUDED.range_km_wltp_est,
        motor_kw = EXCLUDED.motor_kw,
        dc_charge_kw = EXCLUDED.dc_charge_kw,
        seating = EXCLUDED.seating,
        drivetrain = EXCLUDED.drivetrain,
        price_npr = EXCLUDED.price_npr,
        detail_url = EXCLUDED.detail_url,
        run_id = EXCLUDED.run_id,
        loaded_at = CURRENT_TIMESTAMP
    """
    return _executemany(conn, sql, payload, "fact_ev_variant_spec")


def get_price_change_watermark(conn: PgConnection) -> date | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT watermark_value FROM etl_watermark WHERE watermark_name = %s",
            ("price_changes_date",),
        )
        row = cur.fetchone()
    if not row:
        return None
    return date.fromisoformat(row[0])


def set_price_change_watermark(conn: PgConnection, value: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_watermark (watermark_name, watermark_value, updated_at)
            VALUES ('price_changes_date', %s, CURRENT_TIMESTAMP)
            ON CONFLICT (watermark_name) DO UPDATE SET
                watermark_value = EXCLUDED.watermark_value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (value.isoformat(),),
        )
    conn.commit()


def load_fact_price_changes(
    conn: PgConnection,
    rows: list[dict],
    run_id: int,
    full_reload: bool = False,
) -> int:
    ensure_dates(conn, [r["change_date"] for r in rows])
    vehicle_map, _ = _lookup_maps(conn)

    watermark = None if full_reload else get_price_change_watermark(conn)
    filtered = []
    for r in rows:
        if watermark and r["change_date"] <= watermark:
            continue
        filtered.append(r)

    payload = []
    for r in filtered:
        nk = r.get("natural_key")
        payload.append(
            {
                "change_date": r["change_date"],
                "date_key": r["date_key"],
                "vehicle_key": vehicle_map.get(nk) if nk else None,
                "kind_code": r.get("kind_code"),
                "brand_name": r.get("brand_name"),
                "model_name": r.get("model_name"),
                "variant_name": r.get("variant_name"),
                "old_price_npr": r.get("old_price_npr"),
                "new_price_npr": r.get("new_price_npr"),
                "pct_change": r.get("pct_change"),
                "natural_event_key": r["natural_event_key"],
                "run_id": run_id,
            }
        )

    sql = """
    INSERT INTO fact_ev_price_change (
        change_date, date_key, vehicle_key, kind_code, brand_name, model_name,
        variant_name, old_price_npr, new_price_npr, pct_change,
        natural_event_key, run_id
    ) VALUES (
        %(change_date)s, %(date_key)s, %(vehicle_key)s, %(kind_code)s, %(brand_name)s,
        %(model_name)s, %(variant_name)s, %(old_price_npr)s, %(new_price_npr)s,
        %(pct_change)s, %(natural_event_key)s, %(run_id)s
    )
    ON CONFLICT (natural_event_key) DO NOTHING
    """
    count = _executemany(conn, sql, payload, "fact_ev_price_change")
    if filtered:
        max_date = max(r["change_date"] for r in filtered)
        set_price_change_watermark(conn, max_date)
        logger.info("Updated price_changes watermark to %s", max_date)
    return count


def mark_processed_file(
    conn: PgConnection,
    filename: str,
    dataset: str,
    checksum: str,
    row_count: int,
    as_of: date,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO processed_files (
                filename, dataset, checksum_sha256, row_count, as_of_date
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (filename) DO UPDATE SET
                checksum_sha256 = EXCLUDED.checksum_sha256,
                row_count = EXCLUDED.row_count,
                as_of_date = EXCLUDED.as_of_date,
                processed_at = CURRENT_TIMESTAMP
            """,
            (filename, dataset, checksum, row_count, as_of),
        )
    conn.commit()


def is_file_processed(conn: PgConnection, filename: str, checksum: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM processed_files
            WHERE filename = %s AND checksum_sha256 = %s
            """,
            (filename, checksum),
        )
        return cur.fetchone() is not None
