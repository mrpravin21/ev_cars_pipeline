"""
Streamlit dashboard — Nepal EV market analytics on top of ev_dw views.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

st.set_page_config(
    page_title="Nepal EV Market",
    page_icon="⚡",
    layout="wide",
)


@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "ev_dw"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )


@st.cache_data(ttl=300)
def read_sql(sql: str) -> pd.DataFrame:
    """Load a SQL result via psycopg2 cursor (avoids pandas/SQLAlchemy warning)."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def main() -> None:
    st.title("Nepal EV Market Intelligence")
    st.caption(
        "Catalog prices from Nepal AutoMart free datasets · detail specs from model pages · "
        "estimated WLTP is heuristic only — not certified."
    )

    try:
        catalog = read_sql("SELECT * FROM v_latest_ev_catalog")
    except Exception as exc:
        st.error(
            f"Could not query warehouse. Run `scripts/init_db.py` and the ETL first.\n\n{exc}"
        )
        return

    if catalog.empty:
        st.warning("Warehouse is empty — run the pipeline first.")
        return

    # Sidebar filters
    st.sidebar.header("Filters")
    kinds = sorted(catalog["kind_code"].dropna().unique().tolist())
    kind_sel = st.sidebar.multiselect("Vehicle kind", kinds, default=kinds)
    brands = sorted(
        catalog.loc[catalog["kind_code"].isin(kind_sel), "brand_name"]
        .dropna()
        .unique()
        .tolist()
    )
    brand_sel = st.sidebar.multiselect("Brands", brands, default=brands)
    use_wltp_est = st.sidebar.toggle(
        "Use estimated WLTP for range charts",
        value=True,
        help="Applies documented conversion factors to CLTC/NEDC/MIDC. Not official WLTP.",
    )
    max_budget = st.sidebar.slider(
        "Max mid price (NPR lakh)",
        min_value=1.0,
        max_value=float(max(catalog["price_mid_npr"].fillna(0).max() / 100_000, 10)),
        value=float(max(catalog["price_mid_npr"].fillna(0).max() / 100_000, 10)),
        step=1.0,
    )

    df = catalog[
        catalog["kind_code"].isin(kind_sel) & catalog["brand_name"].isin(brand_sel)
    ].copy()
    df = df[df["price_mid_npr"].fillna(0) <= max_budget * 100_000]
    df["range_for_chart"] = (
        df["range_km_wltp_est"].fillna(df["range_km"])
        if use_wltp_est
        else df["range_km"]
    )
    df["price_lakh"] = df["price_mid_npr"] / 100_000

    # KPI row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Models", len(df))
    c2.metric("Brands", df["brand_name"].nunique())
    c3.metric(
        "Median price (lakh)",
        f"{df['price_lakh'].median():.1f}" if df["price_lakh"].notna().any() else "—",
    )
    c4.metric(
        "Median range (km)",
        f"{df['range_for_chart'].median():.0f}"
        if df["range_for_chart"].notna().any()
        else "—",
    )

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        [
            "Market overview",
            "Buyer finder",
            "Value & brands",
            "Price movers",
            "Data health",
        ]
    )

    with tab1:
        st.subheader("Price distribution")
        fig = px.histogram(
            df.dropna(subset=["price_lakh"]),
            x="price_lakh",
            color="kind_code",
            nbins=30,
            labels={"price_lakh": "Mid ex-showroom (NPR lakh)"},
        )
        st.plotly_chart(fig, width="stretch")

        st.subheader("Price vs range")
        scatter = df.dropna(subset=["price_lakh", "range_for_chart"])
        if not scatter.empty:
            fig2 = px.scatter(
                scatter,
                x="range_for_chart",
                y="price_lakh",
                color="brand_name",
                hover_data=["model_name", "range_cycle", "battery_kwh", "spec_source"],
                labels={
                    "range_for_chart": "Range km"
                    + (" (WLTP est.)" if use_wltp_est else " (as published)"),
                    "price_lakh": "Mid price (lakh)",
                },
            )
            st.plotly_chart(fig2, width="stretch")
        else:
            st.info("Not enough range data for scatter plot yet — run detail enrichment.")

    with tab2:
        st.subheader("Find EVs in budget")
        min_range = st.number_input("Minimum range (km)", min_value=0, value=200, step=10)
        min_batt = st.number_input("Minimum battery (kWh)", min_value=0.0, value=0.0, step=1.0)
        finder = df.copy()
        if min_range:
            finder = finder[finder["range_for_chart"].fillna(0) >= min_range]
        if min_batt:
            finder = finder[finder["battery_kwh"].fillna(0) >= min_batt]
        show_cols = [
            "kind_code",
            "brand_name",
            "model_name",
            "price_mid_npr",
            "price_lo_npr",
            "price_hi_npr",
            "battery_kwh",
            "range_km",
            "range_cycle",
            "range_km_wltp_est",
            "motor_kw",
            "npr_per_km",
            "spec_source",
            "detail_url",
        ]
        # Sort while price_mid_npr is still available, then display selected columns.
        display = finder.sort_values("price_mid_npr", na_position="last")
        st.dataframe(
            display[show_cols],
            width="stretch",
            hide_index=True,
        )

    with tab3:
        brand_summary = read_sql("SELECT * FROM v_brand_market_summary")
        brand_summary = brand_summary[
            brand_summary["kind_code"].isin(kind_sel)
            & brand_summary["brand_name"].isin(brand_sel)
        ]
        st.subheader("Models per brand")
        fig3 = px.bar(
            brand_summary.sort_values("model_count", ascending=False).head(20),
            x="brand_name",
            y="model_count",
            color="kind_code",
        )
        st.plotly_chart(fig3, width="stretch")

        st.subheader("Best value (lowest NPR per estimated km)")
        value = df.dropna(subset=["npr_per_km"]).sort_values("npr_per_km").head(15)
        st.dataframe(
            value[
                [
                    "brand_name",
                    "model_name",
                    "price_mid_npr",
                    "range_km",
                    "range_cycle",
                    "range_km_wltp_est",
                    "npr_per_km",
                    "npr_per_kwh",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    with tab4:
        try:
            movers = read_sql("SELECT * FROM v_price_movers LIMIT 200")
            if movers.empty:
                st.info("No price-change events loaded yet.")
            else:
                st.subheader("Recent price changes")
                st.dataframe(movers, width="stretch", hide_index=True)
                if movers["pct_change"].notna().any():
                    fig4 = px.bar(
                        movers.head(25),
                        x="model_name",
                        y="pct_change",
                        color="brand_name",
                        title="Top recent % moves",
                    )
                    st.plotly_chart(fig4, width="stretch")
        except Exception as exc:
            st.warning(f"Price movers unavailable: {exc}")

    with tab5:
        try:
            dq = read_sql("SELECT * FROM v_data_quality_latest")
            st.subheader("Spec completeness by kind")
            st.dataframe(dq, width="stretch", hide_index=True)
            missing = (
                df.assign(
                    missing_cycle=df["range_cycle"].isna(),
                    missing_battery=df["battery_kwh"].isna(),
                )
                .groupby("kind_code")[["missing_cycle", "missing_battery"]]
                .mean()
                .reset_index()
            )
            missing["missing_cycle"] *= 100
            missing["missing_battery"] *= 100
            fig5 = px.bar(
                missing.melt(id_vars="kind_code", var_name="metric", value_name="pct"),
                x="kind_code",
                y="pct",
                color="metric",
                barmode="group",
                labels={"pct": "% missing"},
            )
            st.plotly_chart(fig5, width="stretch")
        except Exception as exc:
            st.warning(f"Data health view unavailable: {exc}")

    st.markdown("---")
    st.markdown(
        "Data attributed to [Nepal AutoMart](https://nepalautomart.com/data/ev-price-tracker). "
        "Range-cycle normalization uses configurable heuristics in `pipeline/config.py`."
    )


if __name__ == "__main__":
    main()
