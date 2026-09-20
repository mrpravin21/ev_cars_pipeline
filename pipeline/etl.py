"""
etl.py — orchestrates extract → stage → transform → quality → load.

CLI still calls run_etl() end-to-end.
Airflow DAG calls the stage helpers separately (Week5-style task graph).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import ensure_schema, get_connection
from pipeline.extract import extract_all_csvs, extract_model_details
from pipeline.load import (
    ensure_dates,
    is_file_processed,
    load_dim_brands,
    load_dim_brands_from_catalog,
    load_dim_tax,
    load_dim_vehicles,
    load_fact_catalog_snapshot,
    load_fact_price_changes,
    load_fact_variant_specs,
    mark_processed_file,
)
from pipeline.logging_setup import setup_logging
from pipeline.quality import run_catalog_quality_checks, run_price_change_quality_checks
from pipeline.staging import (
    clear_staging_for_run,
    finish_run,
    stage_brand_snapshots,
    stage_ev_tracker,
    stage_model_details,
    stage_price_changes,
    stage_provincial_tax,
    start_run,
)
from pipeline.transform import (
    enrich_with_details,
    transform_brand_snapshots,
    transform_price_changes,
    transform_provincial_tax,
    transform_tracker_rows,
)

logger = setup_logging()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Nepal EV catalog ETL")
    p.add_argument("--full-reload", action="store_true")
    p.add_argument("--skip-details", action="store_true")
    p.add_argument("--details-max", type=int, default=None)
    p.add_argument("--as-of", type=str, default=None)
    p.add_argument("--init-schema", action="store_true")
    return p.parse_args(argv)


def _serialize_dates(obj: Any) -> Any:
    """Make batch payload JSON-safe for Airflow XCom."""
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize_dates(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize_dates(v) for v in obj]
    return obj


def _deserialize_batch(batch: dict[str, Any]) -> dict[str, Any]:
    """Restore date objects after XCom round-trip."""
    batch = dict(batch)
    batch["as_of"] = date.fromisoformat(batch["as_of"])
    for row in batch.get("catalog", []):
        if isinstance(row.get("as_of_date"), str):
            row["as_of_date"] = date.fromisoformat(row["as_of_date"])
    for row in batch.get("variant_facts", []):
        if isinstance(row.get("as_of_date"), str):
            row["as_of_date"] = date.fromisoformat(row["as_of_date"])
    for row in batch.get("price_rows", []):
        if isinstance(row.get("change_date"), str):
            row["change_date"] = date.fromisoformat(row["change_date"])
    return batch


def prepare_batch(
    full_reload: bool = False,
    skip_details: bool = False,
    details_max: int | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    """
    Extract → stage → transform → quality gate.

    Returns a serializable batch dict for downstream load tasks.
    Does not finish pipeline_runs (caller / finalize_run does).
    """
    as_of = as_of or date.today()
    mode = "FULL" if full_reload else "INCREMENTAL"
    conn = get_connection()
    run_id = None
    rows_details = 0

    try:
        run_id = start_run(conn, mode)
        clear_staging_for_run(conn, run_id)

        logger.info("=== EXTRACT CSVs (as_of=%s, mode=%s) ===", as_of, mode)
        csvs = extract_all_csvs(as_of=as_of, force=full_reload)
        tracker = csvs["ev_tracker"]
        tracker_path = Path(tracker["path"])

        if (
            not full_reload
            and is_file_processed(conn, tracker_path.name, tracker["checksum"])
        ):
            logger.info(
                "Tracker file %s already processed with same checksum — "
                "still refreshing today's snapshot idempotently",
                tracker_path.name,
            )

        stage_ev_tracker(conn, run_id, tracker["rows"], tracker_path.name, as_of)
        stage_price_changes(
            conn,
            run_id,
            csvs["price_changes"]["rows"],
            Path(csvs["price_changes"]["path"]).name,
        )
        stage_brand_snapshots(
            conn,
            run_id,
            "car",
            csvs["car_brand_snapshot"]["rows"],
            Path(csvs["car_brand_snapshot"]["path"]).name,
            as_of,
        )
        stage_brand_snapshots(
            conn,
            run_id,
            "bike",
            csvs["bike_brand_snapshot"]["rows"],
            Path(csvs["bike_brand_snapshot"]["path"]).name,
            as_of,
        )
        stage_provincial_tax(
            conn,
            run_id,
            csvs["provincial_tax"]["rows"],
            Path(csvs["provincial_tax"]["path"]).name,
            as_of,
        )

        detail_variants: list[dict] = []
        if not skip_details:
            logger.info("=== EXTRACT model detail pages ===")
            detail_result = extract_model_details(
                tracker["rows"],
                as_of=as_of,
                force=full_reload,
                kinds={"car"},
                max_models=details_max,
            )
            detail_variants = detail_result["variants"]
            rows_details = stage_model_details(conn, run_id, detail_variants, as_of)

        logger.info("=== TRANSFORM ===")
        catalog = transform_tracker_rows(tracker["rows"], as_of)
        catalog, variant_facts = enrich_with_details(catalog, detail_variants)
        brand_rows = transform_brand_snapshots(
            csvs["car_brand_snapshot"]["rows"],
            csvs["bike_brand_snapshot"]["rows"],
        )
        tax_rows = transform_provincial_tax(csvs["provincial_tax"]["rows"])
        price_rows = transform_price_changes(csvs["price_changes"]["rows"])

        logger.info("=== QUALITY GATE ===")
        min_rows = 10 if details_max and details_max > 0 and details_max < 50 else 50
        run_catalog_quality_checks(catalog, min_rows=min_rows)
        run_price_change_quality_checks(price_rows)

        batch = {
            "run_id": run_id,
            "as_of": as_of,
            "full_reload": full_reload,
            "mode": mode,
            "catalog": catalog,
            "variant_facts": variant_facts,
            "brand_rows": brand_rows,
            "tax_rows": tax_rows,
            "price_rows": price_rows,
            "rows_details": rows_details,
            "tracker_filename": tracker_path.name,
            "tracker_checksum": tracker["checksum"],
            "tracker_row_count": tracker["row_count"],
        }
        return _serialize_dates(batch)
    except Exception as exc:
        logger.exception("prepare_batch failed")
        if run_id is not None:
            try:
                finish_run(conn, run_id, "failed", error_message=str(exc)[:2000])
            except Exception:
                logger.exception("Could not mark run failed")
        raise
    finally:
        conn.close()


def load_dimensions_batch(batch: dict[str, Any]) -> dict[str, Any]:
    """Load dim_date / dim_brand / dim_vehicle / dim_tax (Week5 dim-sync equivalent)."""
    batch = _deserialize_batch(batch)
    conn = get_connection()
    try:
        logger.info("=== LOAD DIMENSIONS (run_id=%s) ===", batch["run_id"])
        ensure_dates(
            conn,
            [batch["as_of"]] + [r["change_date"] for r in batch["price_rows"]],
        )
        load_dim_brands_from_catalog(conn, batch["catalog"])
        if batch["brand_rows"]:
            load_dim_brands(conn, batch["brand_rows"])
        load_dim_vehicles(conn, batch["catalog"], batch["as_of"])
        load_dim_tax(conn, batch["tax_rows"])
        return _serialize_dates(batch)
    finally:
        conn.close()


def load_dim_dates_batch(batch: dict[str, Any]) -> dict[str, Any]:
    batch = _deserialize_batch(batch)
    conn = get_connection()
    try:
        ensure_dates(
            conn,
            [batch["as_of"]] + [r["change_date"] for r in batch["price_rows"]],
        )
        return _serialize_dates(batch)
    finally:
        conn.close()


def load_dim_brands_batch(batch: dict[str, Any]) -> dict[str, Any]:
    batch = _deserialize_batch(batch)
    conn = get_connection()
    try:
        load_dim_brands_from_catalog(conn, batch["catalog"])
        if batch["brand_rows"]:
            load_dim_brands(conn, batch["brand_rows"])
        return _serialize_dates(batch)
    finally:
        conn.close()


def load_dim_vehicles_batch(batch: dict[str, Any]) -> dict[str, Any]:
    batch = _deserialize_batch(batch)
    conn = get_connection()
    try:
        load_dim_vehicles(conn, batch["catalog"], batch["as_of"])
        return _serialize_dates(batch)
    finally:
        conn.close()


def load_dim_tax_batch(batch: dict[str, Any]) -> dict[str, Any]:
    batch = _deserialize_batch(batch)
    conn = get_connection()
    try:
        load_dim_tax(conn, batch["tax_rows"])
        return _serialize_dates(batch)
    finally:
        conn.close()


def merge_fact_batches(
    catalog_batch: dict[str, Any], price_batch: dict[str, Any]
) -> dict[str, Any]:
    """Combine parallel fact-load outputs before finalize."""
    out = dict(catalog_batch)
    out["rows_price_chg"] = price_batch.get("rows_price_chg", 0)
    return out


def load_catalog_facts_batch(batch: dict[str, Any]) -> dict[str, Any]:
    """Load fact_ev_catalog_snapshot + fact_ev_variant_spec."""
    batch = _deserialize_batch(batch)
    conn = get_connection()
    try:
        logger.info("=== LOAD CATALOG FACTS (run_id=%s) ===", batch["run_id"])
        rows_catalog = load_fact_catalog_snapshot(
            conn,
            batch["catalog"],
            batch["run_id"],
            full_reload=batch["full_reload"],
        )
        rows_variants = 0
        if batch["variant_facts"]:
            rows_variants = load_fact_variant_specs(
                conn, batch["variant_facts"], batch["run_id"]
            )
        batch["rows_catalog"] = rows_catalog
        batch["rows_variants"] = rows_variants
        return _serialize_dates(batch)
    finally:
        conn.close()


def load_price_change_facts_batch(batch: dict[str, Any]) -> dict[str, Any]:
    """Load fact_ev_price_change (watermarked unless full_reload)."""
    batch = _deserialize_batch(batch)
    conn = get_connection()
    try:
        logger.info("=== LOAD PRICE-CHANGE FACTS (run_id=%s) ===", batch["run_id"])
        rows_price = load_fact_price_changes(
            conn,
            batch["price_rows"],
            batch["run_id"],
            full_reload=batch["full_reload"],
        )
        if rows_price == 0 and not batch["full_reload"]:
            logger.info(
                "Incremental mode: no new price-change rows after watermark "
                "(already up to date). Use full_reload=true to reload all events."
            )
        batch["rows_price_chg"] = rows_price
        return _serialize_dates(batch)
    finally:
        conn.close()


def finalize_batch(batch: dict[str, Any]) -> dict[str, Any]:
    """Mark processed_files + pipeline_runs success."""
    batch = _deserialize_batch(batch)
    conn = get_connection()
    try:
        mark_processed_file(
            conn,
            batch["tracker_filename"],
            "ev_tracker",
            batch["tracker_checksum"],
            batch["tracker_row_count"],
            batch["as_of"],
        )
        finish_run(
            conn,
            batch["run_id"],
            "success",
            rows_catalog=int(batch.get("rows_catalog") or 0),
            rows_details=int(batch.get("rows_details") or 0),
            rows_price_chg=int(batch.get("rows_price_chg") or 0),
        )
        summary = {
            "run_id": batch["run_id"],
            "rows_catalog": batch.get("rows_catalog", 0),
            "rows_details": batch.get("rows_details", 0),
            "rows_price_chg": batch.get("rows_price_chg", 0),
            "as_of": batch["as_of"].isoformat(),
            "status": "success",
            "mode": batch["mode"],
        }
        logger.info("ETL success %s", summary)
        return summary
    finally:
        conn.close()


def run_etl(
    full_reload: bool = False,
    skip_details: bool = False,
    details_max: int | None = None,
    as_of: date | None = None,
    init_schema: bool = False,
) -> dict:
    """End-to-end CLI path (same stages Airflow runs as separate tasks)."""
    if init_schema:
        conn = get_connection()
        try:
            ensure_schema(conn)
        finally:
            conn.close()

    batch = prepare_batch(
        full_reload=full_reload,
        skip_details=skip_details,
        details_max=details_max,
        as_of=as_of,
    )
    batch = load_dimensions_batch(batch)
    batch = load_catalog_facts_batch(batch)
    batch = load_price_change_facts_batch(batch)
    return finalize_batch(batch)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    as_of = date.fromisoformat(args.as_of) if args.as_of else date.today()
    run_etl(
        full_reload=args.full_reload,
        skip_details=args.skip_details,
        details_max=args.details_max,
        as_of=as_of,
        init_schema=args.init_schema,
    )


if __name__ == "__main__":
    main()
