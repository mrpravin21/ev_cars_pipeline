"""
transform.py — clean CSV/detail rows into warehouse-ready records.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any

from pipeline.config import SOURCE_SYSTEM, WLTP_ESTIMATE_FACTORS

logger = logging.getLogger(__name__)


def _num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"—", "-", "N/A", "na", "null"}:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    return float(m.group(0)) if m else None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def natural_vehicle_key(kind: str, brand: str, model: str) -> str:
    return f"{kind.strip().lower()}|{brand.strip()}|{model.strip()}"


def estimate_wltp_km(range_km: float | None, range_cycle: str | None) -> float | None:
    """Heuristic only — never treat as certified WLTP."""
    if range_km is None:
        return None
    cycle = (range_cycle or "UNKNOWN").upper()
    factor = WLTP_ESTIMATE_FACTORS.get(cycle)
    if factor is None:
        return None
    return round(range_km * factor, 2)


def date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def build_dim_date_row(d: date) -> dict[str, Any]:
    return {
        "date_key": date_key(d),
        "full_date": d,
        "year": d.year,
        "quarter": (d.month - 1) // 3 + 1,
        "month": d.month,
        "month_name": d.strftime("%B"),
        "week_of_year": int(d.strftime("%V")),
        "day_of_week": (d.weekday() + 1) % 7,  # 0=Sun
        "day_name": d.strftime("%A"),
        "is_weekend": d.weekday() >= 5,
    }


def transform_tracker_rows(
    rows: list[dict[str, Any]], as_of: date
) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        kind = (_clean_text(r.get("kind")) or "").lower()
        brand = _clean_text(r.get("brand"))
        model = _clean_text(r.get("model"))
        if kind not in {"car", "bike"} or not brand or not model:
            continue
        price_lo = _num(r.get("priceLo") or r.get("price_lo"))
        price_hi = _num(r.get("priceHi") or r.get("price_hi"))
        battery = _num(r.get("batteryKwh") or r.get("battery_kwh"))
        range_km = _num(r.get("rangeKm") or r.get("range_km"))
        motor = _num(r.get("motorKw") or r.get("motor_kw"))
        price_mid = None
        if price_lo is not None and price_hi is not None:
            price_mid = round((price_lo + price_hi) / 2.0, 2)
        elif price_lo is not None:
            price_mid = price_lo
        elif price_hi is not None:
            price_mid = price_hi

        out.append(
            {
                "source_system": SOURCE_SYSTEM,
                "kind_code": kind,
                "brand_name": brand,
                "model_name": model,
                "natural_key": natural_vehicle_key(kind, brand, model),
                "price_lo_npr": price_lo,
                "price_hi_npr": price_hi,
                "price_mid_npr": price_mid,
                "battery_kwh": battery,
                "range_km": range_km,
                "range_cycle": None,
                "range_km_wltp_est": None,
                "motor_kw": motor,
                "dc_charge_kw": None,
                "seating": None,
                "body_type": None,
                "detail_url": None,
                "spec_source": "csv",
                "as_of_date": as_of,
                "date_key": date_key(as_of),
            }
        )
    logger.info("Transformed %s tracker rows for as_of=%s", len(out), as_of)
    return out


def enrich_with_details(
    catalog_rows: list[dict[str, Any]], detail_variants: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Merge detail-page specs into catalog rows (prefer detail over blank CSV).
    Also return variant-level fact rows.
    """
    by_key: dict[str, list[dict[str, Any]]] = {}
    for v in detail_variants:
        kind = (_clean_text(v.get("kind")) or "").lower()
        brand = _clean_text(v.get("brand"))
        model = _clean_text(v.get("model"))
        if not kind or not brand or not model:
            continue
        key = natural_vehicle_key(kind, brand, model)
        by_key.setdefault(key, []).append(v)

    enriched = []
    for row in catalog_rows:
        key = row["natural_key"]
        variants = by_key.get(key) or []
        if not variants:
            enriched.append(row)
            continue

        # Prefer first non-null battery/range/cycle across variants (often base/Superior)
        best = dict(row)
        best["spec_source"] = "detail"
        best["detail_url"] = variants[0].get("detail_url") or best.get("detail_url")
        best["body_type"] = variants[0].get("body_type") or best.get("body_type")

        for field in ("battery_kwh", "range_km", "range_cycle", "motor_kw", "dc_charge_kw", "seating"):
            for var in variants:
                val = var.get(field)
                if val not in (None, ""):
                    best[field] = val
                    break

        # Prefer the highest published range among variants for catalog summary
        ranges = [v.get("range_km") for v in variants if v.get("range_km") is not None]
        if ranges:
            best["range_km"] = max(ranges)
            # keep cycle from the variant that had that max range if possible
            for var in variants:
                if var.get("range_km") == best["range_km"] and var.get("range_cycle"):
                    best["range_cycle"] = var["range_cycle"]
                    break

        batteries = [v.get("battery_kwh") for v in variants if v.get("battery_kwh") is not None]
        if batteries:
            best["battery_kwh"] = max(batteries)

        best["range_km_wltp_est"] = estimate_wltp_km(
            best.get("range_km"), best.get("range_cycle")
        )
        enriched.append(best)

    variant_facts = []
    for key, variants in by_key.items():
        for var in variants:
            kind = (_clean_text(var.get("kind")) or "").lower()
            brand = _clean_text(var.get("brand"))
            model = _clean_text(var.get("model"))
            as_of_raw = var.get("as_of_date")
            if isinstance(as_of_raw, date):
                as_of = as_of_raw
            elif as_of_raw:
                as_of = date.fromisoformat(str(as_of_raw)[:10])
            else:
                as_of = date.today()
            cycle = _clean_text(var.get("range_cycle"))
            rng = _num(var.get("range_km"))
            variant_facts.append(
                {
                    "natural_key": key,
                    "kind_code": kind,
                    "brand_name": brand,
                    "model_name": model,
                    "variant_name": _clean_text(var.get("variant_name")) or "Model",
                    "battery_kwh": _num(var.get("battery_kwh")),
                    "range_km": rng,
                    "range_cycle": cycle,
                    "range_km_wltp_est": estimate_wltp_km(rng, cycle),
                    "motor_kw": _num(var.get("motor_kw")),
                    "dc_charge_kw": _num(var.get("dc_charge_kw")),
                    "seating": int(var["seating"]) if var.get("seating") not in (None, "") else None,
                    "drivetrain": _clean_text(var.get("drivetrain")),
                    "price_npr": _num(var.get("price_npr")),
                    "detail_url": _clean_text(var.get("detail_url")),
                    "as_of_date": as_of,
                }
            )

    # derived metrics
    for row in enriched:
        mid = row.get("price_mid_npr")
        rng = row.get("range_km_wltp_est") or row.get("range_km")
        batt = row.get("battery_kwh")
        row["npr_per_km"] = round(mid / rng, 4) if mid and rng else None
        row["npr_per_kwh"] = round(mid / batt, 4) if mid and batt else None
        if row.get("range_km") and not row.get("range_km_wltp_est") and row.get("range_cycle"):
            row["range_km_wltp_est"] = estimate_wltp_km(row["range_km"], row["range_cycle"])

    logger.info(
        "Enrichment complete: catalog=%s variants=%s detail-matched=%s",
        len(enriched),
        len(variant_facts),
        sum(1 for r in enriched if r.get("spec_source") == "detail"),
    )
    return enriched, variant_facts


def transform_price_changes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        change_raw = r.get("date") or r.get("change_date")
        if not change_raw:
            continue
        change_date = date.fromisoformat(str(change_raw)[:10])
        kind = (_clean_text(r.get("kind")) or "").lower() or None
        brand = _clean_text(r.get("brand"))
        model = _clean_text(r.get("model"))
        variant = _clean_text(r.get("variant"))
        old_p = _num(r.get("oldPrice") or r.get("old_price"))
        new_p = _num(r.get("newPrice") or r.get("new_price"))
        pct = _num(r.get("pct") or r.get("pct_change"))
        natural = "|".join(
            [
                change_date.isoformat(),
                kind or "",
                brand or "",
                model or "",
                variant or "",
                str(old_p or ""),
                str(new_p or ""),
            ]
        )
        out.append(
            {
                "change_date": change_date,
                "date_key": date_key(change_date),
                "kind_code": kind,
                "brand_name": brand,
                "model_name": model,
                "variant_name": variant,
                "old_price_npr": old_p,
                "new_price_npr": new_p,
                "pct_change": pct,
                "natural_event_key": natural,
                "natural_key": (
                    natural_vehicle_key(kind, brand, model)
                    if kind and brand and model
                    else None
                ),
            }
        )
    logger.info("Transformed %s price-change rows", len(out))
    return out


def transform_brand_snapshots(
    car_rows: list[dict[str, Any]],
    bike_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out = []
    for kind, rows in (("car", car_rows), ("bike", bike_rows)):
        for r in rows:
            brand = _clean_text(r.get("brand"))
            if not brand:
                continue
            out.append(
                {
                    "kind_code": kind,
                    "brand_name": brand,
                    "distributor": _clean_text(r.get("distributor")),
                    "model_count": int(_num(r.get("modelCount")) or 0),
                    "min_price": _num(r.get("minPrice")),
                    "median_price": _num(r.get("medianPrice")),
                    "max_price": _num(r.get("maxPrice")),
                }
            )
    return out


def transform_provincial_tax(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        province = _clean_text(r.get("province"))
        vehicle_type = _clean_text(r.get("vehicleType") or r.get("vehicle_type"))
        band = _clean_text(r.get("band"))
        if not province or not vehicle_type or not band:
            continue
        verified_raw = r.get("verified")
        if isinstance(verified_raw, bool):
            verified = verified_raw
        else:
            verified = str(verified_raw).strip().lower() in {"true", "1", "yes"}
        out.append(
            {
                "province_name": province,
                "vehicle_type": vehicle_type,
                "band": band,
                "annual_tax_npr": _num(r.get("annualTaxNpr") or r.get("annual_tax_npr")),
                "verified": verified,
            }
        )
    return out
