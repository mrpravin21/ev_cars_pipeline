# Nepal EV Cars Pipeline

> A production-shaped data engineering pipeline for Nepal’s electric-vehicle market: extract catalog prices and deep specs from Nepal AutoMart, land them in a PostgreSQL dimensional warehouse, orchestrate with Airflow, and serve buyer/market analytics via Streamlit.

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)](https://postgresql.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-FF4B4B)](https://streamlit.io)
[![Airflow](https://img.shields.io/badge/Airflow-3.x-017CEE)](https://airflow.apache.org)
[![Docker](https://img.shields.io/badge/Docker_Compose-self--contained-2496ED)](https://docs.docker.com/compose/)

**Author:** Pravin Bhatta  
**Role:** Aspiring Data Engineer  
**Course:** Data Engineering Capstone (Week 1–5 practices)  
**Domain:** Nepal EV market — cars + bikes/scooters, ex-showroom NPR pricing, battery/range/motor specs  

**Data attribution:** [Nepal AutoMart](https://nepalautomart.com/data/ev-price-tracker) — free datasets, reuse with attribution.

---

## What It Does

1. **Extracts** structured CSV datasets from Nepal AutoMart’s official `/api/data/*` endpoints (prices, brand snapshots, provincial tax, price-change CDC)
2. **Enriches** with polite HTML detail-page scraping (`/new-cars/{brand}/{model}`) for variant specs and **range cycle** (WLTP / CLTC / NEDC / MIDC)
3. **Stages** immutable raw files + Postgres staging tables per pipeline run
4. **Transforms** into a dimensional model (dims + snapshot/CDC facts) with derived metrics (mid price, NPR/km, estimated WLTP)
5. **Quality-gates** fail-closed before warehouse loads
6. **Loads** idempotently (`ON CONFLICT` upserts, file checksums, price-change watermark)
7. **Orchestrates** via a self-contained Airflow cluster (TaskGroups, branching, trigger rules)
8. **Serves** interactive analytics for buyers and market analysts via Streamlit

---

## Architecture

```text
Nepal AutoMart
  ├─ GET /api/data/ev-price-tracker          (numeric NPR prices + thin specs)
  ├─ GET /api/data/price-changes             (CDC-style price events)
  ├─ GET /api/data/*                         (brand snapshots, tax, resale)
  └─ GET /new-cars/{brand}/{model}           (deep specs + range_cycle)
        │
        v
+------------------+     +------------------+     +------------------+
|  extract.py      |---->|  staging.py      |---->|  transform.py    |
|  CSV + HTML      |     |  run-scoped      |     |  dims/facts prep |
+------------------+     +------------------+     +------------------+
                                                        |
                                                        v
                                               +------------------+
                                               |  quality.py      |
                                               |  fail-closed     |
                                               +------------------+
                                                        |
                                                        v
                                               +------------------+
                                               |  load.py         |
                                               |  ON CONFLICT     |
                                               +------------------+
                                                        |
           +--------------------------------------------+
           v                                            v
+------------------+                          +------------------+
|  PostgreSQL 16   |  <- Docker ev_postgres   |  Airflow 3.x     |
|  staging + DW    |     host :5433           |  UI :8081        |
+------------------+                          +------------------+
           |
           v
+------------------+
|  dashboard/app   |  <- Streamlit (buyer + market views)
+------------------+
```

**Operational runbook:** see [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md)  
**Source / feasibility analysis:** see [PROJECT_FEASIBILITY.md](PROJECT_FEASIBILITY.md)

---

## Warehouse Design

### Dimensions

| Table | Grain / purpose | Notable keys |
|-------|-----------------|--------------|
| `dim_date` | Calendar keys for snapshots & price events | `date_key` PK |
| `dim_vehicle_kind` | `car` / `bike` | `kind_code` UNIQUE |
| `dim_brand` | Brand per kind (+ distributor when known) | UNIQUE `(kind_code, brand_name)` |
| `dim_vehicle` | Model identity, detail URL, body type | `natural_key` = `kind\|brand\|model` |
| `dim_province` / `dim_tax_band` | Provincial vehicle tax context | FK province → tax bands |

### Facts

| Table | Type | Grain | Measures / notes |
|-------|------|-------|------------------|
| `fact_ev_catalog_snapshot` | Snapshot fact | One row per vehicle per `as_of_date` | `price_lo/hi/mid`, battery, range, `range_cycle`, `range_km_wltp_est`, motor, NPR/km, NPR/kWh, `spec_source` |
| `fact_ev_variant_spec` | Spec fact | Variant per vehicle per day | Per-variant battery/range/cycle/charging when scraped |
| `fact_ev_price_change` | CDC fact | Price-change event | `old_price`, `new_price`, `pct_change`, watermarked loads |

### Control / staging

| Table | Purpose |
|-------|---------|
| `pipeline_runs` | Run audit (mode, status, row counts) |
| `processed_files` | Checksum-based file idempotency |
| `etl_watermark` | Price-change incremental cursor |
| `staging_*` | Run-scoped landing before quality/load |

### Analytics views (Streamlit)

- `v_latest_ev_catalog` — current catalog slice  
- `v_brand_market_summary` — brand-level aggregates  
- `v_buyer_value_picks` — NPR-per-km rankings  
- `v_price_movers` — recent price changes  
- `v_data_quality_latest` — missing battery/cycle rates  

**Why this model?** EV catalogs change slowly. Daily **snapshots** + **price-change CDC** beat a single mutable “current table,” and storing `range_km` **with** `range_cycle` (plus an explicit estimated WLTP field) keeps comparisons honest.

---

## Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Language | Python 3.10+ | ETL logic |
| HTTP / parse | `requests`, BeautifulSoup, lxml | CSV APIs + detail HTML |
| Database | PostgreSQL 16 | Staging + dimensional warehouse |
| Connector | `psycopg2` | Loads, schema bootstrap |
| Orchestration | Apache Airflow 3.x | DAG scheduling, TaskGroups, branching |
| Visualization | Streamlit + Plotly | Interactive dashboard |
| Infrastructure | Docker Compose | **Self-contained** warehouse + Airflow |
| Quality | Custom `quality.py` | Fail-closed validation |
| Config / logs | `python-dotenv`, rotating file + stdout logs | 12-factor config, Airflow-friendly logging |

---

## Project Structure

```text
ev_cars_pipeline/
├── dags/
│   └── ev_catalog_to_dw.py       # Airflow DAG (branch + TaskGroups + trigger rules)
├── pipeline/
│   ├── config.py                 # Env, endpoints, WLTP estimate factors
│   ├── logging_setup.py          # Shared logging (stdout + rotating file)
│   ├── extract.py                # CSV + detail-page extract
│   ├── staging.py                # Run-scoped staging loads
│   ├── transform.py              # Clean, enrich, derive metrics
│   ├── quality.py                # DataQualityError + checks
│   ├── load.py                   # Idempotent dim/fact upserts
│   ├── db.py                     # Connection + schema apply
│   └── etl.py                    # CLI + Airflow stage helpers
├── sql/
│   ├── schema_staging.sql
│   ├── schema_warehouse.sql
│   └── views_analytics.sql
├── dashboard/
│   └── app.py                    # Streamlit analytics
├── scripts/
│   ├── init_db.py                # Create DB + apply SQL
│   └── run_pipeline.py           # CLI entrypoint
├── data/raw/                     # Landed CSVs + cached HTML (gitignored content)
├── logs/                         # pipeline.log
├── config/ / plugins/            # Airflow mounts
├── docker-compose.yml            # ev_postgres (:5433) + Airflow UI (:8081)
├── .env.example
├── requirements.txt
├── PROJECT_FEASIBILITY.md
├── PIPELINE_GUIDE.md
└── README.md
```

---

## Production Properties

| Property | How this project satisfies it |
|----------|-------------------------------|
| **Modular** | `extract` / `staging` / `transform` / `quality` / `load`; DAG only orchestrates |
| **Logged** | Module loggers → `logs/pipeline.log` + stdout (Airflow-safe) |
| **Quality-gated** | `DataQualityError` halts load before facts |
| **Incremental** | CSV checksum reuse, HTML cache, `etl_watermark` on price changes |
| **Idempotent** | `ON CONFLICT` upserts, `processed_files`, unique natural keys / event keys |

---

## Quick Start

### Prerequisites

- Python 3.10+
- Docker Desktop / OrbStack (for warehouse + Airflow)
- Network access to `nepalautomart.com`

### 1. Clone & configure

```bash
cd ev_cars_pipeline
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env: set DB_PASSWORD, AIRFLOW_UID=$(id -u) on Linux
```

Host-side `.env` for CLI/Streamlit (matches Docker publish):

```env
DB_HOST=localhost
DB_PORT=5433
DB_NAME=ev_dw
```

### 2. Start warehouse + Airflow

```bash
docker compose up airflow-init
docker compose up -d
```

| Service | Purpose | Access |
|---------|---------|--------|
| `ev_postgres` | Warehouse `ev_dw` | `localhost:5433` |
| Airflow UI | Orchestration | http://localhost:8081 (`airflow` / `airflow`) |

> Port **5433** avoids clashing with a native Postgres on 5432.  
> Port **8081** avoids clashing with Week5 Airflow on 8080.  
> Airflow containers talk to the warehouse as `ev_postgres:5432` on the Docker network — **same DB** as CLI/Streamlit on `localhost:5433`.

### 3. Create schemas

```bash
python scripts/init_db.py
```

### 4. Run the pipeline (CLI)

**Recommended first run (limited detail scrape):**

```bash
python scripts/run_pipeline.py --details-max 15
```

**CSV-only smoke test:**

```bash
python scripts/run_pipeline.py --skip-details
```

**Full production-style run (all car detail pages):**

```bash
python scripts/run_pipeline.py
```

**Force re-download / ignore watermarks:**

```bash
python scripts/run_pipeline.py --full-reload
```

Expected shape of success logs:

```text
=== EXTRACT CSVs ...
=== EXTRACT model detail pages ===   # unless --skip-details
=== TRANSFORM ===
=== QUALITY GATE ===
Quality gate passed: ~170 catalog rows
=== LOAD DIMENSIONS ===
=== LOAD CATALOG / PRICE-CHANGE FACTS ===
ETL success run_id=N catalog=... details=... price_chg=...
```

### 5. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501

### 6. Trigger from Airflow UI

1. Open http://localhost:8081 → login `airflow` / `airflow`
2. Unpause DAG **`ev_catalog_to_dw`**
3. Trigger with params as needed:
   - `skip_details=true` → CSV-only branch
   - `details_max=10` → limited scrape on details branch
   - `full_reload=true` → ignore price-change watermark / force refresh
4. Inspect Graph: TaskGroups `extract` / `dimensions` / `facts`, skipped branch task, parallel dim/fact tasks

More detail: [PIPELINE_GUIDE.md](PIPELINE_GUIDE.md).

---

## Airflow DAG Design

```text
init_schema
  → choose_extract_path                         # @task.branch
       ├─ extract.csv_only_prepare
       └─ extract.with_details_prepare
  → coalesce_extract_batch                      # NONE_FAILED_MIN_ONE_SUCCESS
  → dimensions.{dates, brands, vehicles, tax}   # parallel TaskGroup
  → dimensions.dim_join
  → facts.{catalog, price_changes}              # parallel TaskGroup
  → facts.merge_facts
  → finalize_run → notify_success               # NONE_FAILED
  ↘ alert_on_failure                            # ONE_FAILED
```

| Pattern | Rationale |
|---------|-----------|
| One DAG + params | Matches Week5 `ride_db_to_dw` (`full_reload`), not three separate ETL DAGs |
| Branch on `skip_details` | Real cost fork: fast CSV vs slow HTML enrichment |
| TaskGroups | Clear Graph UX; parallel dim/fact work |
| Trigger rules | Join after skipped branch; alert on any failure |

---

## Data Quality

Fail-closed gates in `pipeline/quality.py` (before fact load):

| Check | Rule | Failure action |
|-------|------|----------------|
| `row_count` | Enough catalog rows (lower min when `details_max` is small) | `DataQualityError` |
| `required_natural_keys` | `natural_key` / brand / model present | `DataQualityError` |
| `valid_kind` | `kind_code ∈ {car, bike}` | `DataQualityError` |
| `prices_non_negative` | Price fields ≥ 0 when present | `DataQualityError` |
| `price_lo_le_hi` | `price_lo ≤ price_hi` | `DataQualityError` |
| `unique_natural_keys` | No duplicate models in batch | `DataQualityError` |
| `specs_plausible` | Battery / range / motor within sane bounds | `DataQualityError` |

Also:

- Blank CSV cells → `NULL` (not fake zeros)
- `range_km_wltp_est` is a **documented heuristic** (`WLTP_ESTIMATE_FACTORS` in `config.py`) — never treated as certified WLTP
- Dashboard can toggle “as published” vs “estimated WLTP” for charts

---

## Dashboard Insights

Run Streamlit against the warehouse and use the tabs below. (Add PNGs under `docs/screenshots/` later if you want README embeds like the Triwood sample.)

### 1. Market overview — price distribution & price vs range

**Questions answered:** How are Nepal EV ex-showroom prices distributed? How do price and claimed range trade off by brand?

**Insight angle:** Budget clusters (sub-30 lakh city EVs vs premium SUVs) and whether longer-range models command clear price premiums once range cycle is considered.

### 2. Buyer finder — filters on budget, range, battery

**Questions answered:** What can I buy under X lakh with at least Y km range?

**Insight angle:** Practical shortlists for Kathmandu buyers, with links back to Nepal AutoMart detail pages when enrichment ran.

### 3. Value & brands — models per brand, NPR per km

**Questions answered:** Which brands dominate the catalog? Which models look “cheap per claimed km”?

**Insight angle:** Catalog share ≠ sales share, but brand density and value scores still guide competitive scanning.

### 4. Price movers — CDC from `/api/data/price-changes`

**Questions answered:** What just got more expensive or cheaper?

**Insight angle:** Incremental watermark loads keep this tab fresh without replaying the full history every run.

### 5. Data health — missing battery / range cycle

**Questions answered:** How complete are specs after enrichment?

**Insight angle:** Transparent trust — sparse CSV fields vs detail-page fill rates are first-class metrics, not hidden.

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Official CSV APIs as price spine** | Numeric NPR, robots-allowed `/api/data/`, attributed reuse — more stable than scraping listing cards |
| **Detail pages for deep specs** | CSV lacks `range_cycle` and many variant fields; global APIs poor for Nepal China/India nameplates |
| **Never overwrite Nepal specs with global APIs** | Other-market packs differ; wrong WLTP is worse than NULL |
| **Snapshot fact + price CDC** | Catalogs change slowly; watermarks fit price events; daily snapshots enable history |
| **Staging + `pipeline_runs`** | Debuggable land-then-transform; auditable runs |
| **`ON CONFLICT` upserts** | Idempotent re-runs without truncate-and-hope |
| **Self-contained Docker Airflow** | Project runs without Week5 symlinks; UI on 8081 |
| **Estimated WLTP as separate column** | Uniform charts without lying about the published cycle |
| **CLI and Airflow share `pipeline/`** | Same code path; DAG only orchestrates |

---

## Data Scale

| Layer | Typical volume |
|-------|----------------|
| EV tracker CSV | ~170 models (~98 cars + ~75 bikes/scooters; site counts vary) |
| Price-change feed | 100+ events (grows over time) |
| Brand snapshots | Dozens of brands × kind |
| Provincial tax bands | ~90 rows |
| Detail enrichment | Up to ~98 car pages (cached under `data/raw/details/`) |
| Variant fact rows | Hundreds when multi-variant models are scraped |

**Why this size matters:** Small enough for a laptop and course demo; large enough for real Nepal buyer questions (budget bands, brand competition, tax context, spec completeness). Full detail scrape is I/O-bound (polite sleeps), not CPU-bound.

---

## DBeaver / Streamlit connection cheat sheet

| Client | Host | Port | Database |
|--------|------|------|----------|
| DBeaver (Docker warehouse) | `localhost` | **5433** | `ev_dw` |
| Streamlit / CLI | from `.env` | **5433** | `ev_dw` |
| Native Postgres leftover from early experiments | `localhost` | 5432 | maybe `ev_dw` — **different instance** |

If Airflow loaded data but Streamlit looks empty, you are almost certainly on **5432** instead of **5433**.

---

## Comparison note (course portfolio)

Relative to a typical course sample (API → star schema → Postgres → Streamlit), this project additionally emphasizes:

- Dual extract (structured CSV + HTML enrichment) with ethical attribution  
- Staging, watermarks, checksums, and idempotent loads  
- Self-contained Airflow with TaskGroups, branching, and trigger rules  
- Explicit data-quality / trust dashboards for incomplete EV specs  

---

## License & attribution

Pipeline code in this folder is part of the course repository.

**Source data:** please attribute **Nepal AutoMart** and link to  
https://nepalautomart.com/data/ev-price-tracker  
when sharing dashboards or derived datasets. Prices and specs are distributor-sourced catalog data — confirm with dealers before purchase decisions. Range figures are manufacturer-claimed cycles unless otherwise stated.

---

Built as a Week 1–5 capstone by **Pravin Bhatta**.
