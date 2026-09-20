-- Nepal EV warehouse — staging + control tables
-- Safe to re-run (idempotent DDL).

CREATE TABLE IF NOT EXISTS processed_files (
    filename        TEXT PRIMARY KEY,
    dataset         TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    row_count       INTEGER,
    as_of_date      DATE,
    processed_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    started_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at     TIMESTAMP,
    mode            TEXT NOT NULL CHECK (mode IN ('INCREMENTAL', 'FULL')),
    status          TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'success', 'failed')),
    rows_catalog    INTEGER,
    rows_details    INTEGER,
    rows_price_chg  INTEGER,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS staging_ev_tracker (
    run_id          BIGINT NOT NULL REFERENCES pipeline_runs(run_id),
    kind            TEXT NOT NULL,
    brand           TEXT NOT NULL,
    model           TEXT NOT NULL,
    price_lo        NUMERIC(14, 2),
    price_hi        NUMERIC(14, 2),
    battery_kwh     NUMERIC(10, 3),
    range_km        NUMERIC(10, 2),
    motor_kw        NUMERIC(10, 3),
    source_file     TEXT,
    as_of_date      DATE NOT NULL,
    loaded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_ev_tracker_run
    ON staging_ev_tracker (run_id);

CREATE TABLE IF NOT EXISTS staging_price_changes (
    run_id          BIGINT NOT NULL REFERENCES pipeline_runs(run_id),
    change_date     DATE NOT NULL,
    kind            TEXT,
    brand           TEXT,
    model           TEXT,
    variant         TEXT,
    old_price       NUMERIC(14, 2),
    new_price       NUMERIC(14, 2),
    pct_change      NUMERIC(10, 4),
    source_file     TEXT,
    loaded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_brand_snapshot (
    run_id          BIGINT NOT NULL REFERENCES pipeline_runs(run_id),
    kind            TEXT NOT NULL,
    brand           TEXT NOT NULL,
    model_count     INTEGER,
    min_price       NUMERIC(14, 2),
    median_price    NUMERIC(14, 2),
    max_price       NUMERIC(14, 2),
    distributor     TEXT,
    source_file     TEXT,
    as_of_date      DATE NOT NULL,
    loaded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_provincial_tax (
    run_id          BIGINT NOT NULL REFERENCES pipeline_runs(run_id),
    province        TEXT,
    verified        BOOLEAN,
    vehicle_type    TEXT,
    band            TEXT,
    annual_tax_npr  NUMERIC(14, 2),
    source_file     TEXT,
    as_of_date      DATE NOT NULL,
    loaded_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_model_details (
    run_id              BIGINT NOT NULL REFERENCES pipeline_runs(run_id),
    kind                TEXT NOT NULL,
    brand               TEXT NOT NULL,
    model               TEXT NOT NULL,
    detail_url          TEXT NOT NULL,
    variant_name        TEXT,
    battery_kwh         NUMERIC(10, 3),
    range_km            NUMERIC(10, 2),
    range_cycle         TEXT,
    motor_kw            NUMERIC(10, 3),
    dc_charge_kw        NUMERIC(10, 3),
    seating             INTEGER,
    body_type           TEXT,
    drivetrain          TEXT,
    price_npr           NUMERIC(14, 2),
    raw_json            JSONB,
    as_of_date          DATE NOT NULL,
    loaded_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_model_details_run
    ON staging_model_details (run_id);
