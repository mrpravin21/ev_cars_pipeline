"""
Airflow DAG — Nepal EV catalog → Postgres warehouse.

Week5-aligned patterns:
  - ONE DAG + params (full_reload / skip_details)
  - TaskGroup for extract / dimensions / facts
  - @task.branch for CSV-only vs detail enrichment
  - TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS after branch (skips OK)
  - TriggerRule.ONE_FAILED alert task
  - Parallel dim loads + parallel fact loads

  init_schema
       → choose_extract_path
            ├─ extract.csv_only_prepare
            └─ extract.with_details_prepare
       → coalesce_extract_batch
       → dimensions.{dates|brands|vehicles|tax}  (parallel)
       → dimensions.dim_join
       → facts.{catalog|price_changes}           (parallel)
       → facts.merge_facts
       → finalize_run → notify_success
       ↘ alert_on_failure (ONE_FAILED)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _setup() -> None:
    from pipeline.logging_setup import setup_logging

    setup_logging()


def _prepare_kwargs(context: dict, *, skip_details: bool) -> dict:
    params = context["params"]
    details_max = int(params.get("details_max") or 0)
    return {
        "full_reload": bool(params.get("full_reload")),
        "skip_details": skip_details,
        "details_max": details_max if details_max > 0 else None,
    }


with DAG(
    dag_id="ev_catalog_to_dw",
    description=(
        "Nepal EV ETL with TaskGroups, extract branching, and parallel dim/fact loads"
    ),
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "full_reload": Param(
            False,
            type="boolean",
            description=(
                "Ignore price-change watermark and force re-download "
                "(mode switch via param — not a separate DAG)"
            ),
        ),
        "skip_details": Param(
            False,
            type="boolean",
            description="Branch to CSV-only extract (no HTML detail scrape)",
        ),
        "details_max": Param(
            0,
            type="integer",
            description="Max car detail pages on the details branch (0 = all)",
        ),
    },
    tags=["ev", "nepal", "etl", "taskgroup", "branching"],
) as dag:

    @task
    def init_schema():
        from pipeline.db import ensure_schema, get_connection

        _setup()
        conn = get_connection()
        try:
            ensure_schema(conn)
        finally:
            conn.close()

    @task.branch
    def choose_extract_path(**context):
        """Week5-style branch: return task_id of the path to run."""
        if context["params"].get("skip_details"):
            return "extract.csv_only_prepare"
        return "extract.with_details_prepare"

    # ── Extract TaskGroup + branch ───────────────────────────────────────────
    with TaskGroup(group_id="extract") as extract_tg:

        @task(task_id="csv_only_prepare")
        def csv_only_prepare(**context):
            from pipeline.etl import prepare_batch

            _setup()
            return prepare_batch(**_prepare_kwargs(context, skip_details=True))

        @task(task_id="with_details_prepare")
        def with_details_prepare(**context):
            from pipeline.etl import prepare_batch

            _setup()
            return prepare_batch(**_prepare_kwargs(context, skip_details=False))

        csv_batch = csv_only_prepare()
        details_batch = with_details_prepare()

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def coalesce_extract_batch(
        csv_only_batch: dict | None = None,
        details_batch: dict | None = None,
    ):
        """Join after branch — skipped side is None; one success is enough."""
        _setup()
        batch = details_batch or csv_only_batch
        if not batch:
            raise ValueError("No extract batch produced by either branch")
        return batch

    schema = init_schema()
    branch = choose_extract_path()
    schema >> branch
    branch >> [csv_batch, details_batch]
    batch = coalesce_extract_batch(csv_batch, details_batch)

    # ── Dimensions TaskGroup (parallel) ─────────────────────────────────────
    with TaskGroup(group_id="dimensions") as dimensions_tg:

        @task
        def load_dates(b: dict):
            from pipeline.etl import load_dim_dates_batch

            _setup()
            return load_dim_dates_batch(b)

        @task
        def load_brands(b: dict):
            from pipeline.etl import load_dim_brands_batch

            _setup()
            return load_dim_brands_batch(b)

        @task
        def load_vehicles(b: dict):
            from pipeline.etl import load_dim_vehicles_batch

            _setup()
            return load_dim_vehicles_batch(b)

        @task
        def load_tax(b: dict):
            from pipeline.etl import load_dim_tax_batch

            _setup()
            return load_dim_tax_batch(b)

        @task
        def dim_join(b: dict, *_deps):
            return b

        d_dates = load_dates(batch)
        d_brands = load_brands(batch)
        d_vehicles = load_vehicles(batch)
        d_tax = load_tax(batch)
        dims_ready = dim_join(batch, d_dates, d_brands, d_vehicles, d_tax)

    # ── Facts TaskGroup (parallel after dims) ───────────────────────────────
    with TaskGroup(group_id="facts") as facts_tg:

        @task
        def load_catalog_facts(b: dict):
            from pipeline.etl import load_catalog_facts_batch

            _setup()
            return load_catalog_facts_batch(b)

        @task
        def load_price_change_facts(b: dict):
            from pipeline.etl import load_price_change_facts_batch

            _setup()
            return load_price_change_facts_batch(b)

        @task
        def merge_facts(catalog_batch: dict, price_batch: dict):
            from pipeline.etl import merge_fact_batches

            _setup()
            return merge_fact_batches(catalog_batch, price_batch)

        f_catalog = load_catalog_facts(dims_ready)
        f_prices = load_price_change_facts(dims_ready)
        facts_ready = merge_facts(f_catalog, f_prices)

    @task
    def finalize_run(b: dict):
        from pipeline.etl import finalize_batch

        _setup()
        return finalize_batch(b)

    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def notify_success(summary: dict | None = None):
        _setup()
        from pipeline.logging_setup import setup_logging

        setup_logging().info("Pipeline finished successfully: %s", summary)
        return summary

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def alert_on_failure():
        _setup()
        from pipeline.logging_setup import setup_logging

        setup_logging().error(
            "EV catalog DAG had an upstream failure — check extract/dimensions/facts logs."
        )

    summary = finalize_run(facts_ready)
    notify_success(summary)

    [
        csv_batch,
        details_batch,
        batch,
        dims_ready,
        facts_ready,
        summary,
    ] >> alert_on_failure()

    # Retain group refs for the DAG parser
    _ = (extract_tg, dimensions_tg, facts_tg)
