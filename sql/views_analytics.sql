-- Analytics views for Streamlit / ad-hoc SQL

CREATE OR REPLACE VIEW v_latest_ev_catalog AS
SELECT DISTINCT ON (f.vehicle_key)
    f.as_of_date,
    v.kind_code,
    v.brand_name,
    v.model_name,
    v.detail_url,
    f.price_lo_npr,
    f.price_hi_npr,
    f.price_mid_npr,
    f.battery_kwh,
    f.range_km,
    f.range_cycle,
    f.range_km_wltp_est,
    f.motor_kw,
    f.dc_charge_kw,
    f.seating,
    f.npr_per_km,
    f.npr_per_kwh,
    f.spec_source
FROM fact_ev_catalog_snapshot f
JOIN dim_vehicle v ON v.vehicle_key = f.vehicle_key
ORDER BY f.vehicle_key, f.as_of_date DESC;

CREATE OR REPLACE VIEW v_brand_market_summary AS
SELECT
    kind_code,
    brand_name,
    COUNT(*) AS model_count,
    ROUND(AVG(price_mid_npr)::numeric, 0) AS avg_price_mid_npr,
    ROUND(MIN(price_lo_npr)::numeric, 0) AS min_price_npr,
    ROUND(MAX(price_hi_npr)::numeric, 0) AS max_price_npr,
    ROUND(AVG(battery_kwh)::numeric, 2) AS avg_battery_kwh,
    ROUND(AVG(range_km_wltp_est)::numeric, 1) AS avg_range_wltp_est,
    ROUND(
        100.0 * AVG(CASE WHEN battery_kwh IS NULL THEN 1 ELSE 0 END),
        1
    ) AS pct_missing_battery
FROM v_latest_ev_catalog
GROUP BY kind_code, brand_name;

CREATE OR REPLACE VIEW v_data_quality_latest AS
SELECT
    kind_code,
    COUNT(*) AS models,
    SUM(CASE WHEN battery_kwh IS NULL THEN 1 ELSE 0 END) AS missing_battery,
    SUM(CASE WHEN range_km IS NULL THEN 1 ELSE 0 END) AS missing_range,
    SUM(CASE WHEN range_cycle IS NULL THEN 1 ELSE 0 END) AS missing_cycle,
    SUM(CASE WHEN motor_kw IS NULL THEN 1 ELSE 0 END) AS missing_motor,
    ROUND(
        100.0 * SUM(CASE WHEN range_cycle IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        1
    ) AS pct_with_cycle
FROM v_latest_ev_catalog
GROUP BY kind_code;

CREATE OR REPLACE VIEW v_price_movers AS
SELECT
    change_date,
    kind_code,
    brand_name,
    model_name,
    variant_name,
    old_price_npr,
    new_price_npr,
    pct_change
FROM fact_ev_price_change
ORDER BY change_date DESC, ABS(pct_change) DESC NULLS LAST;

CREATE OR REPLACE VIEW v_buyer_value_picks AS
SELECT
    kind_code,
    brand_name,
    model_name,
    price_mid_npr,
    battery_kwh,
    range_km,
    range_cycle,
    range_km_wltp_est,
    motor_kw,
    npr_per_km,
    npr_per_kwh,
    detail_url
FROM v_latest_ev_catalog
WHERE price_mid_npr IS NOT NULL
  AND COALESCE(range_km_wltp_est, range_km) IS NOT NULL
ORDER BY npr_per_km ASC NULLS LAST;
