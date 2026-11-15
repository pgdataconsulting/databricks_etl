"""Run the medallion example end to end on a local Spark session.

    python examples/quickstart.py [business_date]

Three YAML pipelines run in order (sample data -> silver -> gold), exactly as
the Databricks job runs them, but against a throwaway local warehouse under
artifacts/quickstart. Nothing outside that directory is touched.

Needs the `spark` extra and Java 17 or newer:

    pip install -e ".[spark]"
"""

import logging
import shutil
import sys
from pathlib import Path

from etl_framework import run_pipeline
from etl_framework.local import local_spark_session

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = REPO_ROOT / "configs" / "medallion"
PIPELINES = ("seed_sample_data", "orders_silver", "customer_revenue_gold")
# On Databricks these schemas are provisioned by Terraform (infra/terraform).
SCHEMAS = ("bronze", "silver", "gold", "quarantine", "etl_audit", "etl_state")
CATALOG = "spark_catalog"


def main(business_date: str = "2026-07-07") -> None:
    # The framework logs one JSON line per step to stdout, which is what a
    # job wants and too much for a demo: show warnings only.
    framework_log = logging.getLogger("etl_framework")
    framework_log.addHandler(logging.StreamHandler(sys.stderr))
    framework_log.setLevel(logging.WARNING)

    workdir = REPO_ROOT / "artifacts" / "quickstart"
    shutil.rmtree(workdir, ignore_errors=True)
    spark = local_spark_session(str(workdir / "warehouse"), app_name="etl-quickstart")
    spark.sparkContext.setLogLevel("ERROR")
    for schema in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    for name in PIPELINES:
        summary = run_pipeline(
            config_path=str(CONFIG_DIR / f"{name}.yaml"),
            parameters={"business_date": business_date, "catalog": CATALOG},
            spark=spark,
        )
        seconds = summary["metrics"]["total_duration_seconds"]
        print(f"\n== {summary['process_name']}: {summary['status']} in {seconds}s")
        for step in summary["steps"]:
            rows = step.get("rows_processed")
            print(f"   {step['step_name']:<32} {step['status']:<10} rows={rows}")

    print("\n== gold.customer_daily_revenue (published)")
    spark.table(f"{CATALOG}.gold.customer_daily_revenue").orderBy("customer_id").show()
    print("== quarantine.orders (rows that broke a rule, kept for inspection)")
    spark.table(f"{CATALOG}.quarantine.orders").select(
        "order_id", "customer_id", "order_amount", "status"
    ).orderBy("order_id").show()
    print("== etl_audit.pipeline_runs (one row per run)")
    spark.table(f"{CATALOG}.etl_audit.pipeline_runs").select(
        "process_name", "status", "start_time"
    ).orderBy("start_time").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main(*sys.argv[1:2])
