# Nepal EV Cars Pipeline

> A production-shaped batch pipeline for Nepal's electric-vehicle market. It extracts prices and specs from Nepal AutoMart, loads a PostgreSQL dimensional warehouse, is orchestrated with Airflow, and serves buyer and market analytics in Streamlit.

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)](https://postgresql.org)
[![Airflow](https://img.shields.io/badge/Airflow-3.x-017CEE)](https://airflow.apache.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-FF4B4B)](https://streamlit.io)
[![Docker](https://img.shields.io/badge/Docker_Compose-self--contained-2496ED)](https://docs.docker.com/compose/)

**Author:** Pravin Bhatta · Data Engineering Capstone (Week 1–5 practices)
**Data source:** [Nepal AutoMart](https://nepalautomart.com/data/ev-price-tracker). Free datasets, reused with attribution.

---

## What it does

1. **Extracts** Nepal AutoMart's official CSV datasets (`/api/data/*`): EV prices, a price-change feed, brand snapshots and provincial tax.
2. **Enriches** car models by politely scraping their detail pages for per-variant specs and the **range test cycle** (WLTP / CLTC / NEDC / MIDC).
3. **Stages, transforms and quality-gates** the data. The gate is fail-closed: bad data never reaches the warehouse.
4. **Loads** a star schema idempotently, using `ON CONFLICT` upserts, file checksums and a price-change watermark.
5. **Orchestrates** everything with a self-contained Airflow stack (TaskGroups, branching, trigger rules).
6. **Serves** a Streamlit dashboard for buyers and analysts.

---

## Architecture

```text
Nepal AutoMart CSV APIs + model detail pages
        │
        ▼
extract ──► raw files (per day, sha256) ──► staging (per run) ──► transform
        ──► quality gate ──► dimensions ──► facts ──► analytics views ──► Streamlit

Orchestration: Airflow DAG `ev_catalog_to_dw`  (or the CLI, which uses the same code)
Runtime:       docker compose → warehouse Postgres (:5433) + Airflow UI (:8081)
```

---

## Warehouse design

| Type | Tables |
|---|---|
| Dimensions | `dim_date`, `dim_vehicle_kind`, `dim_brand`, `dim_vehicle` (natural key `kind\|brand\|model`), `dim_province`, `dim_tax_band` |
| Facts | `fact_ev_catalog_snapshot`: one row per vehicle per day<br>`fact_ev_variant_spec`: vehicle × variant × day<br>`fact_ev_price_change`: price events, unique `natural_event_key` |
| Control | `pipeline_runs` (run audit), `processed_files` (checksums), `etl_watermark` (incremental cursor), `staging_*` |
| Views | `v_latest_ev_catalog`, `v_brand_market_summary`, `v_buyer_value_picks`, `v_price_movers`, `v_data_quality_latest` |

**Key modelling choices**

- A **daily snapshot** plus a **price-change event fact**: catalogs change slowly, so snapshots keep history and the change feed drives incremental loads.
- Range is stored **as published with its cycle** (`range_km` + `range_cycle`). There's also a separate, clearly labelled `range_km_wltp_est` (CLTC ×0.82, NEDC ×0.78, MIDC ×0.88; configurable in `config.py`).
- Derived measures: `price_mid_npr`, `npr_per_km`, `npr_per_kwh`, and `spec_source` (whether specs came from the CSV or a detail page).

---

## Airflow DAG

```text
init_schema
  → choose_extract_path                         # @task.branch on skip_details
       ├─ extract.csv_only_prepare               #   seconds
       └─ extract.with_details_prepare           #   minutes (polite scraping)
  → coalesce_extract_batch                      # NONE_FAILED_MIN_ONE_SUCCESS
  → dimensions.{dates, brands, vehicles, tax}   # parallel TaskGroup
  → facts.{catalog, price_changes}              # parallel TaskGroup
  → finalize_run → notify_success               # NONE_FAILED
  ↘ alert_on_failure                            # ONE_FAILED
```

`@daily`, `catchup=False`, 2 retries. **Params:** `full_reload`, `skip_details`, `details_max`. It's one DAG with the mode as a param, the same pattern as the Week 5 `ride_db_to_dw` DAG.

---

## Incremental vs full reload

| Layer | Incremental (default) | `full_reload` |
|---|---|---|
| CSV extract | Always fetched; not rewritten if the sha256 is unchanged | Always rewritten |
| Detail pages | Reused if already cached today | Re-fetched |
| Dimensions and snapshot | Upsert | Upsert |
| Price changes | Only rows after the watermark | Watermark ignored; unique key still blocks duplicates |

New car listings are picked up by a normal incremental run; no full reload is needed.

---

## Data quality

Fail-closed checks run before any fact load and raise a `DataQualityError`. The catalog checks are: `row_count`, `required_natural_keys`, `valid_kind`, `prices_non_negative`, `price_lo_le_hi`, `unique_natural_keys` and `specs_plausible`. The price-change feed is checked for dates and event keys. **Missing specs are allowed**, because the source is sparse; they're reported on the dashboard's Data health tab instead.

---

## Quick start

**Prerequisites:** Python 3.10+, Docker (Desktop or OrbStack), and network access to `nepalautomart.com`.

```bash
cd ev_cars_pipeline
cp .env.example .env                 # set DB_PASSWORD; host side uses DB_HOST=localhost, DB_PORT=5433

docker compose up airflow-init       # first time only
docker compose up -d                 # warehouse + Airflow

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/init_db.py            # create schemas
```

**Run the pipeline**

```bash
python scripts/run_pipeline.py --skip-details   # fast, CSV only
python scripts/run_pipeline.py                  # full run with detail scraping
python scripts/run_pipeline.py --full-reload    # ignore watermark, force re-download
```

Or in Airflow: <http://localhost:8081> (`airflow` / `airflow`) → unpause `ev_catalog_to_dw` → **Trigger**.

**Dashboard**

```bash
streamlit run dashboard/app.py                  # http://localhost:8501
```

| Client | Host | Port | Database |
|---|---|---|---|
| DBeaver / CLI / Streamlit | `localhost` | **5433** | `ev_dw` |
| Airflow containers | `ev_postgres` | 5432 | `ev_dw` (same database) |

> Close Streamlit and DBeaver sessions before triggering a run. An open connection can hold locks and make `init_schema` hang.

---

## Dashboard

| Tab | What it answers |
|---|---|
| Market overview | Price distribution; price vs range by brand |
| Buyer finder | "What can I buy under X lakh with at least Y km?" |
| Value & brands | Models per brand; best NPR per km |
| Price movers | What just got cheaper or more expensive |
| Data health | How complete the specs are, and how far to trust them |

The sidebar has filters for kind, brand and budget, plus a published vs estimated WLTP range toggle.

---

## Project structure

```text
ev_cars_pipeline/
├── dags/ev_catalog_to_dw.py     # Airflow DAG
├── pipeline/                    # config, extract, staging, transform, quality, load, db, etl
├── sql/                         # staging, warehouse and view DDL
├── dashboard/app.py             # Streamlit
├── scripts/                     # init_db.py, run_pipeline.py
├── data/raw/                    # landed CSVs + cached HTML (gitignored)
├── logs/                        # pipeline.log
└── docker-compose.yml           # warehouse (:5433) + Airflow (:8081)
```

---

## Known limitations

- Models removed from the site still appear in the latest catalog view.
- The detail-page cache is per day, so the first run after a gap re-scrapes every page.
- There are no automated tests yet.

---





**Attribution:** source data © [Nepal AutoMart](https://nepalautomart.com/data/ev-price-tracker). Prices are ex-showroom, distributor-sourced catalog data; range figures are manufacturer claims under the stated cycle. Confirm with dealers before buying.
