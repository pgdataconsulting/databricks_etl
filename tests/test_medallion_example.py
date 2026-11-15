"""The medallion example in configs/medallion, run exactly as it is shipped.

These are the pipelines the Databricks job deploys. Running them here, with
the local session catalog in place of a Unity Catalog one, keeps the example
from drifting away from the framework.
"""

from pathlib import Path

import pytest

from etl_framework.runner import run_pipeline

CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs" / "medallion"
SCHEMAS = ("bronze", "silver", "gold", "quarantine", "etl_audit", "etl_state")
TABLES = (
    "bronze.orders_raw",
    "bronze.customers_raw",
    "silver.orders",
    "quarantine.orders",
    "gold.customer_daily_revenue",
    "etl_state.watermarks",
    "etl_audit.pipeline_runs",
    "etl_audit.pipeline_steps",
    "etl_audit.pipeline_errors",
)


@pytest.fixture()
def catalog(spark):
    """A clean set of medallion schemas in the session catalog."""
    for table in TABLES:
        spark.sql(f"DROP TABLE IF EXISTS spark_catalog.{table}")
    for schema in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    yield "spark_catalog"
    for table in TABLES:
        spark.sql(f"DROP TABLE IF EXISTS spark_catalog.{table}")


def run(spark, name, catalog, **parameters):
    return run_pipeline(
        config_path=str(CONFIG_DIR / f"{name}.yaml"),
        parameters={"catalog": catalog, **parameters},
        spark=spark,
    )


def rows(result, step_name):
    (step,) = [s for s in result["steps"] if s["step_name"] == step_name]
    return step["rows_processed"]


def test_medallion_example_end_to_end(spark, catalog):
    run(spark, "seed_sample_data", catalog, business_date="2026-07-07")
    silver = run(spark, "orders_silver", catalog)
    gold = run(spark, "customer_revenue_gold", catalog)

    # Silver: 8 raw rows, one superseded version dropped, two rule-breakers
    # quarantined, five loaded.
    assert rows(silver, "latest_version_per_order") == 7
    assert rows(silver, "check_orders") == 5
    assert silver["watermark"] == {"previous": None, "new": "2026-07-07 12:45:00"}
    loaded = {r.order_id: r.status for r in spark.table(f"{catalog}.silver.orders").collect()}
    assert loaded == {
        1001: "SHIPPED",
        1002: "NEW",
        1003: "SHIPPED",
        1006: "CANCELLED",
        1007: "SHIPPED",
    }
    quarantined = spark.table(f"{catalog}.quarantine.orders").collect()
    assert sorted(r.order_id for r in quarantined) == [1004, 1005]

    # Gold: cancelled order excluded, unknown customer defaulted.
    published = {
        r.customer_id: (r.customer_name, r.country, r.order_count, r.revenue)
        for r in spark.table(f"{catalog}.gold.customer_daily_revenue").collect()
    }
    assert published == {
        1: ("Alice Novak", "PL", 2, 430.5),
        2: ("Bruno Costa", "PT", 1, 75.0),
        9: ("UNKNOWN", "??", 1, 64.0),
    }
    assert gold["lineage"]["targets"][0]["dataset"] == f"{catalog}.gold.customer_daily_revenue"

    # Audit trail: one row per audited run, one per step.
    runs = spark.table(f"{catalog}.etl_audit.pipeline_runs").collect()
    assert sorted((r.process_name, r.status) for r in runs) == [
        ("medallion_customer_revenue_gold", "succeeded"),
        ("medallion_orders_silver", "succeeded"),
    ]
    assert spark.table(f"{catalog}.etl_audit.pipeline_steps").count() == 12


def test_medallion_silver_load_is_incremental_and_idempotent(spark, catalog):
    run(spark, "seed_sample_data", catalog, business_date="2026-07-07")
    run(spark, "orders_silver", catalog)

    # Nothing changed upstream: the second run reads nothing and changes nothing.
    second = run(spark, "orders_silver", catalog)
    assert rows(second, "only_changed_orders") == 0
    assert second["watermark"]["new"] == second["watermark"]["previous"]
    assert spark.table(f"{catalog}.silver.orders").count() == 5
    assert spark.table(f"{catalog}.quarantine.orders").count() == 2

    # A new day arrives: only its rows are processed, and they are upserted.
    run(spark, "seed_sample_data", catalog, business_date="2026-07-08")
    third = run(spark, "orders_silver", catalog)
    assert rows(third, "only_changed_orders") == 8
    assert third["watermark"]["new"] == "2026-07-08 12:45:00"
    silver = spark.table(f"{catalog}.silver.orders")
    assert silver.count() == 5  # same order ids, updated in place
    assert {str(r.order_date) for r in silver.collect()} == {"2026-07-08"}
