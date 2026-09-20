-- Nepal EV dimensional warehouse
-- Safe to re-run for CREATE IF NOT EXISTS objects.
-- Destructive resets belong in scripts/reset_warehouse.sql

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month           SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name      VARCHAR(10) NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    day_name        VARCHAR(10) NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_vehicle_kind (
    kind_key        SERIAL PRIMARY KEY,
    kind_code       TEXT NOT NULL UNIQUE,   -- car | bike
    kind_label      TEXT NOT NULL
);

INSERT INTO dim_vehicle_kind (kind_code, kind_label)
VALUES ('car', 'Electric car (4W)'),
       ('bike', 'Electric bike / scooter (2W)')
ON CONFLICT (kind_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_brand (
    brand_key       SERIAL PRIMARY KEY,
    kind_code       TEXT NOT NULL REFERENCES dim_vehicle_kind (kind_code),
    brand_name      TEXT NOT NULL,
    distributor     TEXT,
    source_system   TEXT NOT NULL DEFAULT 'nepalautomart',
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (kind_code, brand_name)
);

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key     SERIAL PRIMARY KEY,
    source_system   TEXT NOT NULL DEFAULT 'nepalautomart',
    kind_code       TEXT NOT NULL REFERENCES dim_vehicle_kind (kind_code),
    brand_name      TEXT NOT NULL,
    model_name      TEXT NOT NULL,
    natural_key     TEXT NOT NULL UNIQUE,  -- kind|brand|model
    detail_url      TEXT,
    body_type       TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_date DATE,
    last_seen_date  DATE,
    updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_province (
    province_key    SERIAL PRIMARY KEY,
    province_name   TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_tax_band (
    tax_band_key    SERIAL PRIMARY KEY,
    province_key    INTEGER NOT NULL REFERENCES dim_province (province_key),
    vehicle_type    TEXT NOT NULL,
    band            TEXT NOT NULL,
    annual_tax_npr  NUMERIC(14, 2),
    verified        BOOLEAN,
    UNIQUE (province_key, vehicle_type, band)
);

-- Daily catalog snapshot (model grain from CSV; enriched specs when available)
CREATE TABLE IF NOT EXISTS fact_ev_catalog_snapshot (
    snapshot_key        BIGSERIAL PRIMARY KEY,
    as_of_date          DATE NOT NULL,
    date_key            INTEGER NOT NULL REFERENCES dim_date (date_key),
    vehicle_key         INTEGER NOT NULL REFERENCES dim_vehicle (vehicle_key),
    brand_key           INTEGER NOT NULL REFERENCES dim_brand (brand_key),
    kind_code           TEXT NOT NULL,
    price_lo_npr        NUMERIC(14, 2),
    price_hi_npr        NUMERIC(14, 2),
    price_mid_npr       NUMERIC(14, 2),
    battery_kwh         NUMERIC(10, 3),
    range_km            NUMERIC(10, 2),
    range_cycle         TEXT,
    range_km_wltp_est   NUMERIC(10, 2),
    motor_kw            NUMERIC(10, 3),
    dc_charge_kw        NUMERIC(10, 3),
    seating             INTEGER,
    npr_per_km          NUMERIC(14, 4),
    npr_per_kwh         NUMERIC(14, 4),
    spec_source         TEXT NOT NULL DEFAULT 'csv',
    run_id              BIGINT,
    loaded_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (as_of_date, vehicle_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_catalog_as_of
    ON fact_ev_catalog_snapshot (as_of_date);
CREATE INDEX IF NOT EXISTS idx_fact_catalog_brand
    ON fact_ev_catalog_snapshot (brand_key);

-- Variant-level Nepal detail enrichment (optional rows)
CREATE TABLE IF NOT EXISTS fact_ev_variant_spec (
    variant_spec_key    BIGSERIAL PRIMARY KEY,
    as_of_date          DATE NOT NULL,
    vehicle_key         INTEGER NOT NULL REFERENCES dim_vehicle (vehicle_key),
    variant_name        TEXT NOT NULL,
    battery_kwh         NUMERIC(10, 3),
    range_km            NUMERIC(10, 2),
    range_cycle         TEXT,
    range_km_wltp_est   NUMERIC(10, 2),
    motor_kw            NUMERIC(10, 3),
    dc_charge_kw        NUMERIC(10, 3),
    seating             INTEGER,
    drivetrain          TEXT,
    price_npr           NUMERIC(14, 2),
    detail_url          TEXT,
    run_id              BIGINT,
    loaded_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (as_of_date, vehicle_key, variant_name)
);

CREATE TABLE IF NOT EXISTS fact_ev_price_change (
    price_change_key    BIGSERIAL PRIMARY KEY,
    change_date         DATE NOT NULL,
    date_key            INTEGER NOT NULL REFERENCES dim_date (date_key),
    vehicle_key         INTEGER REFERENCES dim_vehicle (vehicle_key),
    kind_code           TEXT,
    brand_name          TEXT,
    model_name          TEXT,
    variant_name        TEXT,
    old_price_npr       NUMERIC(14, 2),
    new_price_npr       NUMERIC(14, 2),
    pct_change          NUMERIC(10, 4),
    natural_event_key   TEXT NOT NULL UNIQUE,
    run_id              BIGINT,
    loaded_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_watermark (
    watermark_name      TEXT PRIMARY KEY,
    watermark_value     TEXT NOT NULL,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
