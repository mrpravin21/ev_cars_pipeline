"""
extract.py — land Nepal AutoMart CSVs + optional model-detail HTML into data/raw/.

Idempotent behaviours:
  - Same-day CSV with identical SHA-256 is skipped (no re-download overwrite needed)
  - Detail pages cached under data/raw/details/{as_of}/{slug}.html
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from pipeline.config import (
    BASE_URL,
    CSV_ENDPOINTS,
    DETAIL_MAX_MODELS,
    RAW_DIR,
    REQUEST_SLEEP_SEC,
    REQUEST_TIMEOUT_SEC,
    USER_AGENT,
)

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _today() -> date:
    return date.today()


def _slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[\s_]+", "-", text)
    return text.strip("-")


def fetch_bytes(url: str) -> tuple[bytes, dict[str, str]]:
    logger.info("GET %s", url)
    resp = SESSION.get(url, timeout=REQUEST_TIMEOUT_SEC)
    resp.raise_for_status()
    meta = {
        "content_type": resp.headers.get("Content-Type", ""),
        "content_disposition": resp.headers.get("Content-Disposition", ""),
        "url": str(resp.url),
    }
    return resp.content, meta


def extract_csv_dataset(
    dataset: str,
    url: str | None = None,
    as_of: date | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Download one CSV dataset into data/raw/{dataset}/{as_of}/...

    Returns metadata including path, checksum, row_count, skipped flag.
    """
    as_of = as_of or _today()
    url = url or CSV_ENDPOINTS[dataset]
    out_dir = RAW_DIR / dataset / as_of.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    payload, meta = fetch_bytes(url)
    checksum = _sha256_bytes(payload)

    # Prefer Content-Disposition filename when present
    fname = f"{dataset}_{as_of.isoformat()}.csv"
    cd = meta.get("content_disposition") or ""
    m = re.search(r'filename="?([^";]+)"?', cd)
    if m:
        fname = m.group(1).strip()

    out_path = out_dir / fname
    checksum_path = out_dir / f"{fname}.sha256"

    if (
        not force
        and out_path.exists()
        and checksum_path.exists()
        and checksum_path.read_text().strip() == checksum
    ):
        rows = list(csv.DictReader(io.StringIO(payload.decode("utf-8"))))
        logger.info(
            "Skip re-write %s (checksum match, %s rows)", out_path.name, len(rows)
        )
        return {
            "dataset": dataset,
            "path": str(out_path),
            "checksum": checksum,
            "row_count": len(rows),
            "as_of_date": as_of.isoformat(),
            "skipped": True,
            "rows": rows,
        }

    out_path.write_bytes(payload)
    checksum_path.write_text(checksum + "\n", encoding="utf-8")
    rows = list(csv.DictReader(io.StringIO(payload.decode("utf-8"))))
    logger.info("Wrote %s (%s rows, sha256=%s…)", out_path, len(rows), checksum[:12])
    time.sleep(REQUEST_SLEEP_SEC)
    return {
        "dataset": dataset,
        "path": str(out_path),
        "checksum": checksum,
        "row_count": len(rows),
        "as_of_date": as_of.isoformat(),
        "skipped": False,
        "rows": rows,
    }


def extract_all_csvs(as_of: date | None = None, force: bool = False) -> dict[str, Any]:
    results = {}
    for name, url in CSV_ENDPOINTS.items():
        try:
            results[name] = extract_csv_dataset(name, url, as_of=as_of, force=force)
        except Exception:
            logger.exception("Failed extracting dataset=%s", name)
            raise
    return results


def model_detail_url(kind: str, brand: str, model: str) -> str:
    kind = (kind or "").strip().lower()
    prefix = "new-cars" if kind == "car" else "new-bikes"
    return urljoin(BASE_URL + "/", f"{prefix}/{_slugify(brand)}/{_slugify(model)}")


def _parse_float(text: str | None) -> float | None:
    if text is None:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", str(text).replace(",", ""))
    return float(m.group(1)) if m else None


def _detect_cycle(text: str) -> str | None:
    for cycle in ("WLTP", "CLTC", "NEDC", "MIDC", "ARA"):
        if re.search(rf"\b{cycle}\b", text, flags=re.I):
            return cycle.upper()
    return None


def parse_detail_html(
    html: str, kind: str, brand: str, model: str, url: str
) -> list[dict[str, Any]]:
    """
    Parse Nepal AutoMart model page into one or more variant-spec dicts.
    Prefer JSON-LD Product variants; fall back to visible 'Key specs' text.
    """
    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text("\n", strip=True)

    cycle = _detect_cycle(page_text)
    body_type = None
    m_body = re.search(
        r"\b(SUV|Sedan|Hatchback|MPV|Crossover|Pickup|Coupe)\b", page_text, re.I
    )
    if m_body:
        body_type = m_body.group(1)

    motor_kw = None
    m_motor = re.search(r"(?:Motor|MOTOR)[^\n]{0,40}?(\d+(?:\.\d+)?)\s*kW", page_text, re.I)
    if m_motor:
        motor_kw = float(m_motor.group(1))

    dc_kw = None
    m_dc = re.search(
        r"(?:DC(?:\s+fast)?\s*charg(?:e|ing)|CCS2?)[^\n]{0,40}?(\d+(?:\.\d+)?)\s*kW",
        page_text,
        re.I,
    )
    if m_dc:
        dc_kw = float(m_dc.group(1))

    seating = None
    m_seat = re.search(r"Seating[^\n]{0,20}?(\d+)", page_text, re.I)
    if m_seat:
        seating = int(m_seat.group(1))

    drivetrain = None
    m_drive = re.search(r"\b(AWD|FWD|RWD|4WD)\b", page_text)
    if m_drive:
        drivetrain = m_drive.group(1)

    variants: list[dict[str, Any]] = []

    # JSON-LD variants
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            has_variant = node.get("hasVariant") or []
            if not isinstance(has_variant, list):
                continue
            for var in has_variant:
                if not isinstance(var, dict):
                    continue
                name = var.get("name") or "Standard"
                price = None
                offers = var.get("offers")
                if isinstance(offers, dict):
                    price = _parse_float(offers.get("price"))
                elif isinstance(offers, list) and offers:
                    price = _parse_float(offers[0].get("price"))
                # AdditionalProperty range/battery if present
                batt = None
                rng = None
                for prop in var.get("additionalProperty") or []:
                    if not isinstance(prop, dict):
                        continue
                    pname = (prop.get("name") or "").lower()
                    val = prop.get("value")
                    if "battery" in pname:
                        batt = _parse_float(val)
                    if "range" in pname:
                        rng = _parse_float(val)
                variants.append(
                    {
                        "kind": kind,
                        "brand": brand,
                        "model": model,
                        "detail_url": url,
                        "variant_name": name,
                        "battery_kwh": batt,
                        "range_km": rng,
                        "range_cycle": cycle,
                        "motor_kw": motor_kw,
                        "dc_charge_kw": dc_kw,
                        "seating": seating,
                        "body_type": body_type,
                        "drivetrain": drivetrain,
                        "price_npr": price,
                    }
                )

    # Text blocks like "49.92 kWh" + "345 km claimed range, WLTP"
    if not variants:
        pair_hits = re.findall(
            r"(\d+(?:\.\d+)?)\s*kWh[^\n]{0,80}?(\d+(?:\.\d+)?)\s*km[^\n]{0,40}?(WLTP|CLTC|NEDC|MIDC|ARA)?",
            page_text,
            flags=re.I,
        )
        if pair_hits:
            for i, (batt, rng, cyc) in enumerate(pair_hits, start=1):
                variants.append(
                    {
                        "kind": kind,
                        "brand": brand,
                        "model": model,
                        "detail_url": url,
                        "variant_name": f"Variant {i}",
                        "battery_kwh": float(batt),
                        "range_km": float(rng),
                        "range_cycle": (cyc or cycle or "").upper() or cycle,
                        "motor_kw": motor_kw,
                        "dc_charge_kw": dc_kw,
                        "seating": seating,
                        "body_type": body_type,
                        "drivetrain": drivetrain,
                        "price_npr": None,
                    }
                )

    if not variants:
        # At least one shell row so we record the URL visit
        rng = None
        m_rng = re.search(r"Claimed range[^\n]{0,40}?(\d+(?:\.\d+)?)", page_text, re.I)
        if m_rng:
            rng = float(m_rng.group(1))
        batt = None
        m_batt = re.search(r"Battery[^\n]{0,40}?(\d+(?:\.\d+)?)\s*kWh", page_text, re.I)
        if m_batt:
            batt = float(m_batt.group(1))
        variants.append(
            {
                "kind": kind,
                "brand": brand,
                "model": model,
                "detail_url": url,
                "variant_name": "Model",
                "battery_kwh": batt,
                "range_km": rng,
                "range_cycle": cycle,
                "motor_kw": motor_kw,
                "dc_charge_kw": dc_kw,
                "seating": seating,
                "body_type": body_type,
                "drivetrain": drivetrain,
                "price_npr": None,
            }
        )

    return variants


def extract_model_details(
    tracker_rows: list[dict[str, Any]],
    as_of: date | None = None,
    force: bool = False,
    kinds: set[str] | None = None,
    max_models: int | None = None,
) -> dict[str, Any]:
    """
    Fetch & parse detail pages for models in the EV tracker CSV.
    Default focus: cars (richer specs). Set kinds={'car','bike'} for all.
    """
    as_of = as_of or _today()
    kinds = kinds or {"car"}
    max_models = DETAIL_MAX_MODELS if max_models is None else max_models

    candidates = [
        r
        for r in tracker_rows
        if (r.get("kind") or "").strip().lower() in kinds
    ]
    if max_models and max_models > 0:
        candidates = candidates[:max_models]

    out_dir = RAW_DIR / "details" / as_of.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    all_variants: list[dict[str, Any]] = []
    fetched = 0
    skipped = 0
    failed = 0

    for row in candidates:
        brand = (row.get("brand") or "").strip()
        model = (row.get("model") or "").strip()
        kind = (row.get("kind") or "").strip().lower()
        if not brand or not model:
            continue
        url = model_detail_url(kind, brand, model)
        slug = f"{kind}__{_slugify(brand)}__{_slugify(model)}"
        html_path = out_dir / f"{slug}.html"
        meta_path = out_dir / f"{slug}.json"

        try:
            if html_path.exists() and not force:
                html = html_path.read_text(encoding="utf-8", errors="ignore")
                skipped += 1
                logger.info("Reuse cached detail %s", html_path.name)
            else:
                payload, _ = fetch_bytes(url)
                html = payload.decode("utf-8", errors="ignore")
                html_path.write_text(html, encoding="utf-8")
                fetched += 1
                time.sleep(REQUEST_SLEEP_SEC)

            variants = parse_detail_html(html, kind, brand, model, url)
            for v in variants:
                v["as_of_date"] = as_of.isoformat()
            all_variants.extend(variants)
            meta_path.write_text(
                json.dumps(
                    {
                        "url": url,
                        "parsed_at": datetime.utcnow().isoformat() + "Z",
                        "variant_count": len(variants),
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception as exc:
            failed += 1
            logger.error("Detail extract failed for %s %s: %s", brand, model, exc)
            continue

    logger.info(
        "Detail extract done: fetched=%s skipped=%s failed=%s variants=%s",
        fetched,
        skipped,
        failed,
        len(all_variants),
    )
    return {
        "as_of_date": as_of.isoformat(),
        "fetched": fetched,
        "skipped": skipped,
        "failed": failed,
        "variants": all_variants,
    }
