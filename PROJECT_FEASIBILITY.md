# Nepal EV Cars Market — Final Project Brief & Feasibility Analysis

**Status:** Analysis only (no pipeline code yet)  
**Proposed folder:** `ev_cars_pipeline/`  
**Course scope:** Capstone combining Week 1–Week 5 practices  
**Primary market:** Electric vehicles sold in Nepal (cars + optionally bikes/scooters)  
**Analysis date:** 2026-09-18

---

## 1. Executive verdict

| Question | Answer |
|---|---|
| Is this project achievable as a course final project? | **Yes — strongly feasible** |
| Is HTML scraping of `https://nepalautomart.com/electric-vehicles` the best extract method? | **No — workable, but not ideal** |
| Is there a better free, structured source? | **Yes — Nepal AutoMart’s official free CSV datasets under `/api/data/`** |
| Can it demonstrate production pipeline properties? | **Yes** (modular, logged, quality-gated, incremental, idempotent) |
| Can Streamlit answer real buyer/market questions? | **Yes**, especially if we land price + battery/range/motor + change history |

**Recommended approach (updated):** Use a **dual extract from Nepal AutoMart**:

1. **CSV `/api/data/ev-price-tracker`** — authoritative Nepal ex-showroom price spine (numeric NPR, polite, attributed).
2. **Model detail pages** (`/new-cars/{brand}/{model}`) — Nepal-verified deep specs the CSV lacks (especially **range cycle**: WLTP / CLTC / NEDC / MIDC, per-variant battery/range, charging, seating, etc.).

Global EV APIs are **optional secondary enrichment only** — coverage is weak for Nepal’s China/India-heavy catalog, and other-market packs often differ from Nepal trims.

See **§3.4 Detailed specs & enrichment strategy** below.

---

## 2. Project goal (what we will build later)

Build a production-shaped **Nepal EV market data platform**:

1. **Ingest** EV listing / price / spec data from Nepal AutoMart (and optional enrichment sources).
2. **ETL** into PostgreSQL with staging → quality gate → dimensional warehouse tables.
3. **Orchestrate** with Airflow DAGs (Week 5 pattern).
4. **Serve analytics** via a Streamlit dashboard for buyers and market analysts.

### Target outcomes for stakeholders

**For buyers**

- Compare EVs by budget (NPR), battery (kWh), claimed range (km), motor power (kW).
- See brand/model price bands (`priceLo`–`priceHi`) and value proxies (NPR per km / NPR per kWh).
- Filter cars vs bikes/scooters; spot “just launched” / recently changed prices when available.

**For market analysts**

- Brand concentration and price distribution in Nepal’s EV catalog.
- Spec completeness gaps (missing battery/range — data-quality signal).
- Price-change activity over time (from official change feed).
- Optional: relate catalog pricing to Nepal EV import growth (customs / published stats).

---

## 3. Source analysis

### 3.1 Nepal AutoMart — primary source (recommended)

**Listing page (user-proposed):**  
https://nepalautomart.com/electric-vehicles

**Observed as of 2026-09-18**

- Next.js site; listing cards for ~**98 electric cars** and ~**74 bikes/scooters** (~172 models on the hub page).
- Card fields visible without detail crawl: brand, model, ex-showroom price band, variant count, body type (sometimes), EMI hint, “Just launched” badge.
- Detail URLs follow a stable pattern: `/new-cars/{brand}/{model}` (e.g. `/new-cars/byd/atto-3`).
- Schema.org JSON-LD present (Organization, BreadcrumbList, Product/Offer-style markup on detail pages) — useful but incomplete vs a full warehouse grain.

**Critical discovery — free structured datasets (better than HTML scrape)**

Nepal AutoMart publishes **free market datasets** at https://nepalautomart.com/data with explicit reuse language on the EV tracker page:

> *“free to reuse with attribution”* · *“please attribute Nepal AutoMart and link back”*

| Dataset page | CSV API endpoint | Schema (columns) | Notes |
|---|---|---|---|
| [EV Price & Spec Tracker](https://nepalautomart.com/data/ev-price-tracker) | `GET /api/data/ev-price-tracker` | `kind, brand, model, priceLo, priceHi, batteryKwh, rangeKm, motorKw` | **173 rows** (98 `car` + 75 `bike`); `Content-Type: text/csv`; filename like `nepal-ev-price-tracker-2026-09-18.csv`; `Cache-Control: s-maxage=3600` |
| Price changes | `GET /api/data/price-changes` | `date, kind, brand, model, variant, oldPrice, newPrice, pct` | Natural **incremental / CDC-style** feed |
| New car price snapshot | `GET /api/data/new-car-price-snapshot` | brand-level aggregates + distributor | Enrichment for dim_brand |
| New bike price snapshot | `GET /api/data/new-bike-price-snapshot` | same shape for bikes | Optional |
| Provincial vehicle tax | `GET /api/data/provincial-vehicle-tax` | `province, verified, vehicleType, band, annualTaxNpr` | Buyer TCO / tax context |
| Resale value by brand | `GET /api/data/resale-value-by-brand` | `kind, brand, retainedPct, marketPct, vsMarket, sample` | Analyst charts |

**robots.txt alignment**

```
Allow: /
Allow: /api/data/
Disallow: /admin/
Disallow: /api/
```

So `/api/data/*` is explicitly allowed for bots; general `/api/` is not. This is a strong signal that CSV downloads are the intended programmatic access path.

**Sample raw rows (EV tracker CSV)**

```text
kind,brand,model,priceLo,priceHi,batteryKwh,rangeKm,motorKw
car,Seres,Mini,1649000,1799000,16.8,220,30
car,MG,Comet,1875000,2274000,17.3,230,3.3
bike,Yadea,E8S Pro,230000,230000,2.74,150,
```

Prices are already **numeric NPR** in the CSV (much cleaner than parsing “Rs 62.9 lakh” from HTML).

**Data-quality reality check (important for gates)**

From the 2026-09-18 EV tracker snapshot (~172 data rows):

- Battery and range are frequently blank (on the order of ~⅓–⅔ depending on field).
- Motor is denser but still incomplete on some models.
- Pipeline must treat blanks as **allowed nulls with monitoring**, not hard-fail the whole load — while still failing on broken schema, non-positive prices, duplicate natural keys, etc.

### 3.2 Is web scraping ideal?

| Approach | Pros | Cons | Verdict for this project |
|---|---|---|---|
| **Official CSV `/api/data/ev-price-tracker`** | Structured, attributed, robots-allowed, numeric prices, daily-ish refresh, low maintenance | Model-level grain (not every variant); some null specs | **Primary extract — preferred** |
| **HTML scrape listing + detail pages** | Extra fields (body type, variants, EMI, badges, showrooms) | Brittle DOM, rate limits, heavier ops, more ToS risk if aggressive | **Optional enrichment only** |
| **Parse JSON-LD only** | Semi-structured | Incomplete for battery/range analytics | Supplement, not core |

**Conclusion:** Scraping the electric-vehicles HTML page is **achievable** and good pedagogy, but for a **production-shaped** pipeline the ideal path is:

1. Land official CSVs into `data/raw/` (immutable daily snapshots).  
2. Optionally scrape a **small, polite** set of detail pages for fields missing from CSV.  
3. Never depend solely on fragile card HTML for the core fact table.

### 3.3 Alternative sources (free / structured?)

| Source | What you get | Free structured API? | Fit |
|---|---|---|---|
| **Nepal AutoMart `/api/data/*`** | EV catalog + prices + specs + change feed + tax | **Yes (CSV downloads)** | **Best primary** |
| [EV News Nepal](https://evnewsnepal.com/) | Large EV catalog, stats widgets | Embed widgets only (limit ~10); **terms prohibit scraping/API bulk use without permission** | Avoid as scrape target; OK for citation / manual context |
| [NepalEVs](https://nepalevs.com/) | Charging stations, route tools | No public bulk EV price API found | Optional geo enrichment later |
| [MeroMoto](https://meromoto.com/) | Marketplace listings (used + new) | No free public API; third-party Apify scrapers are commercial | Different grain (listings/sellers), not ideal as primary catalog |
| [customs.gov.np](https://customs.gov.np/) | Official EV **import aggregates** by HS code / fiscal year | Published tables / reports, not a clean model-price API | Excellent **secondary** fact for “market growth” dashboard section |
| IEA / Statbase style BEV sales charts | Country-level sales history | Often paywalled for bulk download | Nice-to-have narrative, not core ETL |
| OMDb-style global car APIs | Not Nepal-priced | N/A | Poor fit for Nepal ex-showroom economics |

**No better general-purpose free REST “Nepal EV cars JSON API” was found** that beats Nepal AutoMart’s attributed CSV endpoints for this use case.

### 3.4 Detailed specs & enrichment strategy (CSV vs listings vs global)

The free EV tracker CSV is excellent for **prices**, but intentionally thin on buyer-critical engineering detail:

| Field | In CSV? | On NAM detail pages? | Why it matters |
|---|---|---|---|
| Ex-showroom `priceLo` / `priceHi` (NPR) | Yes | Yes (variant-level) | Core commercial spine |
| Battery kWh / range km / motor kW | Partial (often blank) | Stronger, **per variant** | Buyer comparison |
| **Range cycle** (WLTP / CLTC / NEDC / MIDC) | **No** | **Yes** | Same “420 km” is not comparable across cycles |
| Variant names & per-variant prices | No (model band only) | Yes | Real purchase grain |
| DC charge kW, AC type, seating, boot, GC, drivetrain | No | Yes (“Key specs & features”) | Buyer decisioning |
| Distributor / showrooms / on-road tax context | No | Yes | Nepal ownership costs |

**Evidence from live detail pages (2026-09-18):**

- BYD ATTO 3: variants with **49.92 kWh / 345 km WLTP** vs **60.48 kWh / 420 km WLTP**; page also mentions CLTC in places; motor 100 kW; CCS2 DC up to 80 kW.
- Wuling Mini EV: **CLTC / NEDC**-oriented copy (Chinese-origin labeling).
- Tata Nexon EV: **MIDC** references (India-origin labeling).

Nepal AutoMart themselves warn that other-market sheets can differ and leave gaps rather than invent Nepal figures — so **Nepal listing pages are the right primary deep-spec source**.

#### Can global sources fill the gap?

| Global source | Free structured access? | Range standard | Coverage vs Nepal CSV brands | Verdict |
|---|---|---|---|---|
| **EPA FuelEconomy.gov** REST/CSV | Yes (public US gov) | EPA (not WLTP/CLTC) | Probe: empty model menus for BYD/Wuling/Tata in common years | **Poor fit** for Nepal catalog |
| **EV-Database.org** | Browse free; **API/export is paid** (demo only free) | Strong WLTP + real-world estimates | EU-centric; misses many Nepal Chinese/Indian nameplates | Good quality, wrong cost/coverage for course project |
| **Gaia Charge open EVDB** (`vehicles.json`) | Yes (CC BY-SA) | Mostly WLTP fields | ~526 vehicles; BYD/MG/Hyundai/Kia present; **Tata / Wuling / Kaiyi / LS Auto / Honri / Mahindra / Chery ≈ absent** | Useful niche fill for global brands only |
| **OpenEV Data** dumps | Yes | Often WLTP | Small / uneven; not Nepal-priced | Optional experiment only |

**Matching problem (even when the brand exists globally):**

- Same badge, different **battery pack / motor / software** for Nepal vs EU/US/CN/IN.
- Naming aliases (e.g. BYD Yuan Plus ↔ ATTO 3).
- Nepali trims may not exist abroad.

So blindly joining global APIs on `(brand, model)` risks **wrong range cycle and wrong kWh** — worse than a NULL.

#### Best option (recommended architecture)

```text
                    ┌─────────────────────────────────────┐
  PRIMARY PRICE     │ NAM CSV /api/data/ev-price-tracker  │──► fact price spine
                    └─────────────────────────────────────┘
                    ┌─────────────────────────────────────┐
  PRIMARY SPECS     │ NAM detail pages /new-cars/...      │──► variant facts + range_cycle
  (Nepal-verified)  │ polite, cached, incremental by URL  │
                    └─────────────────────────────────────┘
                    ┌─────────────────────────────────────┐
  OPTIONAL GLOBAL   │ Gaia / EPA / (paid EVDB later)      │──► enrichment_* with confidence
                    │ only where NAM field is NULL        │    + source + market tag
                    └─────────────────────────────────────┘
```

**Practical rule for the warehouse**

1. Never overwrite Nepal AutoMart Nepal-spec fields with global values.
2. Store `range_km` **and** `range_cycle` together; comparisons in Streamlit must group/filter by cycle (or show a disclaimer).
3. Add `spec_source` / `confidence` columns (`nepalautomart_detail` > `nepalautomart_csv` > `global_enrichment`).
4. Incremental: re-fetch a detail URL only when CSV checksum for that model changes, or on a weekly refresh / `full_reload`.

**Bottom line:** For detailed data, **do not rely on global APIs as the main fill**. The best option is **Nepal AutoMart model specification pages**, with the free CSV kept as the price/identity backbone. Global sources are a secondary, low-confidence enrichment for internationally sold models only.

---

## 4. Feasibility assessment

### 4.1 Technical feasibility — **High**

| Layer | Feasibility | Notes |
|---|---|---|
| Extract | High | Stable CSV URLs; Content-Disposition dated filenames |
| Transform | High | Normalize kinds, brands, price bands, null specs, derived metrics |
| Postgres warehouse | High | Same fact/dim + `ON CONFLICT` patterns as Week 3–5 |
| Incremental + idempotent | High | Daily snapshot hash + `price-changes` feed + upsert keys |
| Airflow | High | Mirror `Week5/dags/ride_db_dw.py` task graph |
| Streamlit | High | Direct SQL against warehouse views |

### 4.2 Scope / pedagogy fit — **High**

Maps cleanly onto skills already practiced in this repo:

| Week | Concepts to reuse | How they show up in EV project |
|---|---|---|
| **Week 1** | SQL fundamentals, Python, CSV loading, basic logging | Raw CSV land + load scripts; `logging.basicConfig` style from `Week3/etl.py` / assignment starters |
| **Week 2** | Normalization, joins, migrations, dimension thinking | `dim_brand`, `dim_vehicle_type`, `dim_province` (tax), clean FK design |
| **Week 3** | Modular ETL, warehouse SQL, API → raw files, re-run skip, polite clients | Extract to `data/raw/`; manifest / processed tracker; sleep/backoff; fact/dim SQL like `warehouse.sql` |
| **Week 4** | Split `extract` / `transform` / `quality` / `load`; incremental watermark; quality gate raises before load; `ON CONFLICT` | Same module layout under `pipeline/`; quality fails closed |
| **Week 5** | Airflow + Docker; DAG tasks for dims then facts; retries; `full_reload` param | `ev_catalog_to_dw` DAG; schedule `@daily` |

### 4.3 Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| HTML structure changes | Medium if scrape-first | Prefer CSV APIs; isolate parsers behind interfaces |
| Sparse battery/range | Medium for analytics | Soft quality checks + “spec completeness” dashboard KPI |
| Catalog ≠ transactional trips | Design nuance | Model **snapshot facts** + **price-change facts**; SCD2 for price history |
| Site ToS / politeness | Medium | Use `/api/data/` + attribution; rate-limit any HTML enrichment; store license note in README |
| Small row counts (~170) | Low for DE learning | Fine for course; still demonstrate production patterns |
| Airflow resource needs | Low–medium | Reuse Week5 docker-compose pattern; keep DAG lean |

### 4.4 Overall achievability score

**9/10** — Excellent final project. Stronger than a pure HTML scrape because the source already offers attributed structured CSVs that let you focus engineering effort on warehouse design, quality, orchestration, and analytics rather than fighting the DOM.

---

## 5. Recommended data architecture (for later implementation)

### 5.1 Logical flow

```text
Nepal AutoMart /api/data/*.csv  (+ optional detail pages)
        │
        ▼
  data/raw/  (immutable daily snapshots + checksums)
        │
        ▼
  staging_* tables (Postgres)     ← full replace or append by run_id
        │
        ▼
  quality gate (fail closed)
        │
        ▼
  dimensions  ──►  facts / snapshots
        │
        ▼
  analytics views  ──►  Streamlit dashboard
```

Orchestration: Airflow DAG tasks (dims in parallel → facts → optional freshness check).

### 5.2 Suggested warehouse grain

**Natural keys**

- Vehicle: `(source_system, kind, brand, model)` → `dim_vehicle`
- Brand: `(kind, brand)` → `dim_brand`
- Date: standard `dim_date` (reuse Week3 idea)

**Facts**

1. `fact_ev_catalog_snapshot` — one row per vehicle per `as_of_date` (daily snapshot).  
   Measures: `price_lo_npr`, `price_hi_npr`, `battery_kwh`, `range_km`, `motor_kw`, derived `price_mid_npr`, `npr_per_km`, `npr_per_kwh`.
2. `fact_ev_price_change` — from `/api/data/price-changes` (variant-level when present).  
   Measures: `old_price`, `new_price`, `pct_change`.
3. Optional `fact_provincial_tax` or keep tax as dimension attributes for TCO calculators.

**Dimensions (examples)**

- `dim_brand` (distributor from brand snapshot CSVs when available)
- `dim_vehicle` (SCD2 on price band / key specs)
- `dim_vehicle_kind` (`car` / `bike`)
- `dim_date`
- `dim_province` + tax bands (from provincial tax CSV)
- `dim_source_run` / watermark & file-processing metadata (idempotency)

### 5.3 Production properties — how they will be satisfied

| Property | Implementation plan |
|---|---|
| **Modular** | `pipeline/extract.py`, `transform.py`, `quality.py`, `load.py`, `config.py` (Week4/5 layout); DAG only orchestrates |
| **Logged** | Module loggers; structured messages for row counts, skipped files, quality summary (Week3/5 style) |
| **Quality-gated** | Checks before load; `DataQualityError` halts DAG task (Week4/5 `quality.py`) |
| **Incremental** | (1) Skip unchanged daily CSV via checksum/`processed_files`; (2) load only new `price-changes` rows by `date` watermark; (3) snapshot fact inserts only for new `as_of_date` |
| **Idempotent** | Unique constraints + `ON CONFLICT` upserts; same Airflow run / re-run does not duplicate; transactions around file mark + load |

**Note on “incremental” for catalogs:** Unlike ride trips, EV catalogs change slowly. Incremental here means **change detection + price-event CDC + snapshot partitioning by date**, not “only rows with `updated_at > watermark` from OLTP.”

### 5.4 Airflow DAG sketch (Week5-aligned)

```text
extract_raw_csvs
    → stage_ev_tracker
    → [load_dim_brand, load_dim_vehicle_kind, load_dim_date, load_dim_province]
    → quality_gate_catalog
    → load_dim_vehicle
    → load_fact_catalog_snapshot
    → load_fact_price_changes   (watermarked)
```

Params: `full_reload: bool` (mirror `ride_db_to_dw`).

### 5.5 Streamlit analytics (business questions)

Suggested dashboard sections:

1. **Market overview** — counts by kind/brand; price histogram; median price.  
2. **Buyer finder** — filters: budget, min range, min battery, kind; ranked table + scatter (price vs range).  
3. **Value scores** — NPR per claimed km / per kWh; top value picks.  
4. **Brand competition** — brand share of catalog; average motor/battery.  
5. **Price movement** — from `fact_ev_price_change` (timeline / top movers).  
6. **Data health** — % models missing battery/range (trust transparency).  
7. **Tax context** — provincial EV vs ICE tax bands (buyer education).

Charts: histogram, box/violin by brand, scatter with hover, bar rankings, line for changes — Plotly or Altair via Streamlit.

---

## 6. Project structure

```text
ev_cars_pipeline/
├── PROJECT_FEASIBILITY.md
├── PIPELINE_GUIDE.md           ← step-by-step execution runbook
├── README.md
├── requirements.txt
├── .env.example
├── docker-compose.yml          ← warehouse Postgres (optional)
├── dags/ev_catalog_to_dw.py
├── pipeline/                   ← modular ETL package
├── sql/
├── dashboard/app.py
├── scripts/init_db.py
├── scripts/run_pipeline.py
├── data/raw/
└── logs/
```

Implementation status: **scaffolded and smoke-tested** (CSV + limited detail enrichment → `ev_dw`).

---

## 7. Attribution & ethics (required)

When using Nepal AutoMart datasets:

- Attribute **Nepal AutoMart** and link to the dataset page(s), especially  
  https://nepalautomart.com/data/ev-price-tracker
- Prefer `/api/data/` endpoints over aggressive HTML crawling.
- Keep request rates polite if any HTML enrichment is added.
- Document that prices/specs are **distributor-sourced catalog data**, not a purchase guarantee; range figures are manufacturer-claimed cycles (WLTP/NEDC/MIDC), not Nepal road tests (as stated by the source).

Avoid EV News Nepal (and similar) as a bulk scrape source unless written permission is obtained — their terms explicitly restrict automated collection.

---

## 8. Decision record (for the next prompt)

| Decision | Choice |
|---|---|
| Primary price extract | Official CSV: `/api/data/ev-price-tracker` (+ related `/api/data/*`) |
| Primary deep-spec extract | Nepal AutoMart **model detail pages** (variant grain + `range_cycle`) |
| HTML scrape of `/electric-vehicles` hub | Optional discovery only; prefer CSV URL list + `/new-cars/{brand}/{model}` |
| Global EV APIs (EPA, Gaia, EVDB) | Optional low-confidence fill only; never overwrite Nepal-spec fields |
| Database | PostgreSQL dimensional model + staging |
| Orchestration | Airflow (Week5 docker-compose pattern) |
| Serving | Streamlit on warehouse views |
| Build now? | **No** — wait for explicit implementation prompt |

---

## 9. Next steps (after you approve / send the build prompt)

1. Scaffold folder layout, `requirements.txt`, `.env.example`, SQL schemas.  
2. Implement extract → raw snapshot → staging → quality → dims/facts.  
3. Wire Airflow DAG with retries + `full_reload`.  
4. Build Streamlit dashboard answering the business questions above.  
5. README with attribution, runbook, and sample analytical queries.

---

## 10. Summary

This EV Nepal market project is **feasible and well-suited** as a Week1–Week5 capstone. The user’s proposed site is the right ecosystem, but the **ideal extract is not HTML scraping** — Nepal AutoMart already exposes **free, attributed, robots-allowed CSV APIs** with numeric prices and EV specs, plus a price-change feed that naturally supports incremental loads. That combination lets the project showcase a **modular, logged, quality-gated, incremental, idempotent** Airflow-orchestrated warehouse and a practical Streamlit product for buyers and market analysts in Nepal.
