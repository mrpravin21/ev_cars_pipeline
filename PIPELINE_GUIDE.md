# Pipeline execution guide — Nepal EV cars

Step-by-step instructions to run the full project from zero to Streamlit dashboard.

---

## 0. What the pipeline does

```text
Nepal AutoMart
  ├─ GET /api/data/ev-price-tracker          (prices + thin specs)
  ├─ GET /api/data/price-changes             (CDC-style events)
  ├─ GET /api/data/* snapshots & tax
  └─ GET /new-cars/{brand}/{model}           (deep specs + range_cycle)
        │
        ▼
  data/raw/  (immutable daily snapshots + HTML cache)
        │
        ▼
  staging_*  →  quality gate  →  dims / facts
        │
        ▼
  analytics views  →  Streamlit
```

Airflow DAG `ev_catalog_to_dw` wraps the same `pipeline.etl.run_etl()` entrypoint used by the CLI.

---

## 1. Prerequisites

- Python 3.10+ recommended
- Docker (optional, for Postgres)
- Network access to `nepalautomart.com`
- ~4GB RAM if you also run Week5 Airflow later

---

## 2. Create a virtualenv and install deps

```bash
cd ev_cars_pipeline
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

> Tip: If `apache-airflow` install is heavy for local CLI-only work, you can temporarily comment that line in `requirements.txt` and use Week5's Airflow environment for the DAG later.

---

## 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

| Variable | Meaning | Local Docker default |
|---|---|---|
| `DB_HOST` | Postgres host | `localhost` |
| `DB_PORT` | Postgres port | `5433` if using this folder's compose |
| `DB_NAME` | Database | `ev_dw` |
| `DB_USER` / `DB_PASSWORD` | Credentials | `postgres` / `postgres` |
| `DETAIL_MAX_MODELS` | Cap detail pages (0 = all cars) | `0` |
| `REQUEST_SLEEP_SEC` | Polite delay between HTTP calls | `0.6` |

---

## 4. Start the Docker stack (warehouse + Airflow)

This project is **self-contained**. From `ev_cars_pipeline/`:

```bash
docker compose up airflow-init
docker compose up -d
```

Services:

| Service | Purpose | Host access |
|---|---|---|
| `ev_postgres` | Warehouse `ev_dw` | `localhost:5433` |
| `postgres` | Airflow metadata | internal only |
| `airflow-apiserver` | Airflow UI | **http://localhost:8081** |
| scheduler / dag-processor / triggerer | orchestration | — |

Login: `airflow` / `airflow`

> Port **8081** avoids clashing with Week5 Airflow on 8080.  
> Port **5433** avoids clashing with a native Postgres on 5432.

Ensure `.env` has (see `.env.example`):

- `AIRFLOW_UID` (macOS often `501`; Linux: `id -u`)
- `FERNET_KEY`
- `DB_PASSWORD` (warehouse)
- `_PIP_ADDITIONAL_REQUIREMENTS=requests beautifulsoup4 lxml python-dotenv`

**Host CLI / Streamlit** use `DB_HOST=localhost` and `DB_PORT=5433`.  
**Airflow tasks** automatically use `DB_HOST=ev_postgres` / `DB_PORT=5432` (compose override).

### Optional: native Postgres only (no Docker warehouse)

If you prefer your existing host `ev_dw` on 5432, you can keep using CLI against it — but the compose Airflow stack expects the `ev_postgres` service. Prefer the Docker warehouse for a clean standalone project.

---

## 5. Create schemas

```bash
# Against Docker warehouse (DB_PORT=5433 in .env)
python scripts/init_db.py
```

Applies:

1. `sql/schema_staging.sql` — staging + `processed_files` + `pipeline_runs`
2. `sql/schema_warehouse.sql` — dims / facts / watermark
3. `sql/views_analytics.sql` — Streamlit views

Safe to re-run (idempotent DDL). The Airflow DAG also runs `ensure_schema` on each trigger.

---

## 6. Run the ETL

### First successful run (recommended: limited details)

Detail pages are polite + cached, but a full car scrape (~98 pages) takes a few minutes.

```bash
python scripts/run_pipeline.py --init-schema --details-max 15
```

### CSV-only (fast smoke test)

```bash
python scripts/run_pipeline.py --skip-details
```

### Full production-style run

```bash
python scripts/run_pipeline.py
```

### Force re-download / ignore watermarks

```bash
python scripts/run_pipeline.py --full-reload
```

### Equivalent module form

```bash
python -m pipeline.etl --details-max 15
```

### What “success” looks like

- Logs in terminal and `logs/pipeline.log`
- Files under `data/raw/ev_tracker/<date>/…`
- Optional HTML under `data/raw/details/<date>/…`
- Row in `pipeline_runs` with `status = success`
- Rows in `fact_ev_catalog_snapshot` for today's `as_of_date`

Verify in `psql` / DBeaver:

```sql
SELECT status, rows_catalog, rows_details, rows_price_chg
FROM pipeline_runs
ORDER BY run_id DESC
LIMIT 5;

SELECT kind_code, COUNT(*)
FROM v_latest_ev_catalog
GROUP BY 1;

SELECT brand_name, model_name, range_km, range_cycle, range_km_wltp_est, spec_source
FROM v_latest_ev_catalog
WHERE range_cycle IS NOT NULL
LIMIT 20;
```

---

## 7. Re-runs (idempotent / incremental behaviour)

| Layer | Behaviour on re-run |
|---|---|
| CSV extract | Same SHA-256 → skip rewrite; still loads snapshot for today |
| Detail HTML | Reuses `data/raw/details/...` unless `--full-reload` |
| Catalog fact | `ON CONFLICT (as_of_date, vehicle_key) DO UPDATE` |
| Price changes | Watermark `etl_watermark.price_changes_date`; only newer dates insert; `ON CONFLICT DO NOTHING` |
| Dims | Upserted by natural keys |

Running twice the same day should **not** duplicate catalog rows.

---

## 8. Launch the Streamlit dashboard

```bash
streamlit run dashboard/app.py
```

Open the URL Streamlit prints (usually http://localhost:8501).

Dashboard tabs:

1. Market overview — price histogram, price vs range  
2. Buyer finder — budget / range / battery filters  
3. Value & brands — brand counts, NPR-per-km picks  
4. Price movers — from `fact_ev_price_change`  
5. Data health — missing battery / cycle rates  

Toggle **Use estimated WLTP** to normalize CLTC/NEDC/MIDC for charts (heuristic — see `pipeline/config.py` → `WLTP_ESTIMATE_FACTORS`).

---

## 9. Trigger a run from Airflow UI (this project)

No Week5 symlink needed — Airflow runs inside `ev_cars_pipeline`.

### Which database does Airflow use vs CLI / Streamlit?

| Runner | Where it connects | Host:port you use |
|---|---|---|
| **Airflow tasks** (inside Docker) | Compose service `ev_postgres` | `ev_postgres:5432` (internal) |
| **CLI** (`scripts/run_pipeline.py`) | From your Mac via `.env` | **`localhost:5433`** |
| **Streamlit** | Same `.env` as CLI | **`localhost:5433`** |

That **is the same warehouse database** (`ev_dw`) as long as `.env` has `DB_PORT=5433` (Docker publishes `ev_postgres` there).

**Not the same** as an older CLI run against native Postgres on **`localhost:5432`**. If you loaded data on 5432 before compose, DBeaver/Streamlit on 5432 will not show what Airflow wrote on 5433.

**DBeaver:** add (or switch to) a connection:

- Host `localhost`, Port **`5433`**, Database `ev_dw`, user/password from `.env`

**Streamlit:** same — with `DB_PORT=5433` in `.env`, `streamlit run dashboard/app.py` reads the Docker warehouse. No second app needed; just point `.env` at 5433.

```bash
# confirm .env
grep DB_ .env
# should show DB_PORT=5433 when using this project's docker compose
```

### DAG design (TaskGroups + branching)

**One DAG** (`ev_catalog_to_dw`) with Week5-style branching/trigger rules:

```text
init_schema
  → choose_extract_path                    # @task.branch
       ├─ extract.csv_only_prepare         # skip_details=true
       └─ extract.with_details_prepare     # detail scrape
  → coalesce_extract_batch                 # NONE_FAILED_MIN_ONE_SUCCESS
  → dimensions.{dates, brands, vehicles, tax}   # parallel TaskGroup
  → dimensions.dim_join
  → facts.{catalog, price_changes}         # parallel TaskGroup
  → facts.merge_facts
  → finalize_run → notify_success          # NONE_FAILED
  ↘ alert_on_failure                       # ONE_FAILED
```

| Pattern | Why here |
|---|---|
| Branch on `skip_details` | Real conditional: fast CSV vs slow HTML scrape |
| `full_reload` as **param** (not a branch DAG) | Same as Week5 ride incremental vs full |
| TaskGroup dims/facts | Parallel work + clearer Graph view |
| `NONE_FAILED_MIN_ONE_SUCCESS` | Join after branch when one side is skipped |
| `ONE_FAILED` alert | Same idea as Week5 `branching_trigger_rule.py` |

1. Start the stack if needed:
   ```bash
   docker compose ps
   docker compose up -d
   ```
2. Open **http://localhost:8081** → `airflow` / `airflow`
3. Find **`ev_catalog_to_dw`** (wait/refresh after DAG edits)
4. Unpause → **Trigger** with params as needed:
   - `skip_details=true` → only `extract.csv_only_prepare` runs
   - `details_max=10` → limited scrape on details branch
   - `full_reload=true` → reload price-change history
5. In Graph view you should see **extract / dimensions / facts** groups, a skipped branch task, and parallel dim/fact tasks
6. Verify in DBeaver on **port 5433** or Streamlit with the same `.env`

```bash
docker compose logs -f airflow-scheduler
docker compose exec airflow-scheduler airflow dags list
docker compose down          # stop
docker compose down -v       # wipe Airflow meta + warehouse volumes
```

---

## 10. Quality gate (fail closed)

Before loading facts, catalog rows must pass:

- minimum row count
- required natural keys
- `kind_code ∈ {car, bike}`
- non-negative prices
- `price_lo <= price_hi`
- unique `natural_key`
- plausible battery / range / motor bounds

Failures raise `DataQualityError` and mark `pipeline_runs.status = failed`.

---

## 11. Estimated WLTP (dashboard convenience)

Stored fields:

- `range_km` + `range_cycle` — **source of truth** from Nepal AutoMart detail pages when available
- `range_km_wltp_est` — `range_km * factor[cycle]` from `WLTP_ESTIMATE_FACTORS`

Never treat estimates as certified WLTP. Factors are editable in config for reproducibility.

---

## 12. Troubleshooting

| Symptom | Fix |
|---|---|
| `connection refused` to Postgres | `docker compose up -d` and match `DB_PORT` |
| Quality fails `row_count` | Network/CSV failed — check logs; retry extract |
| Many 404 on details | Slug mismatch for some models — pipeline continues; check `failed` count in logs |
| Empty range_cycle in warehouse | Re-run without `--skip-details`; increase `--details-max` |
| Streamlit can't import / connect | Activate `.venv`, confirm `.env`, re-run ETL |
| Airflow `ModuleNotFoundError: pipeline` | Mount project root / set `PYTHONPATH=/opt/airflow` style path to package parent |

---

## 13. Suggested demo script (for course presentation)

1. Show `PROJECT_FEASIBILITY.md` source decision (CSV + detail pages).  
2. Run `--skip-details` then query `v_latest_ev_catalog` (prices only).  
3. Run `--details-max 15` and show `range_cycle` / `range_km_wltp_est` fill rate.  
4. Re-run same command — show idempotent logs (cache hits, upsert).  
5. Open Streamlit buyer finder + data health tabs.  
6. Optional: trigger Airflow DAG screenshot.

---

## Attribution

Please attribute **Nepal AutoMart** and link to  
https://nepalautomart.com/data/ev-price-tracker  
when publishing dashboards or derived datasets.
