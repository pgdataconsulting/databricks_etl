"""End-to-end tests: run full YAML pipelines through run_pipeline."""

import pytest

from etl_framework.exceptions import (
    MissingParameterError,
    StepExecutionError,
    ValidationError,
)
from etl_framework.runner import run_pipeline

PIPELINE_TEMPLATE = """
spec_version: "1.0"
process_name: customer_orders_etl
parameters:
  business_date: null
  environment: "dev"
settings:
  log_row_counts: true
steps:
  - name: read_customers
    type: read
    format: parquet
    path: "{customers_path}"
    alias: customers
  - name: read_orders
    type: read
    format: parquet
    path: "{orders_path}"
    alias: orders
  - name: filter_orders
    type: filter
    input: orders
    condition: "order_date = '${{business_date}}'"
    output: filtered_orders
  - name: join_customers_orders
    type: join
    left: filtered_orders
    right: customers
    join_type: left
    condition:
      - "filtered_orders.customer_id = customers.customer_id"
    output: customer_orders
  - name: enrich
    type: expression
    input: customer_orders
    expressions:
      business_date: "to_date('${{business_date}}', 'yyyy-MM-dd')"
      customer_name: "coalesce(customer_name, 'UNKNOWN')"
    output: enriched
  - name: select_columns
    type: select
    input: enriched
    columns:
      - customer_id
      - customer_name
      - order_id
      - order_amount
      - business_date
    output: final_dataset
  - name: write_output
    type: write
    input: final_dataset
    format: parquet
    mode: overwrite
    path: "{output_path}"
"""


@pytest.fixture()
def pipeline_paths(spark, customers_df, orders_df, tmp_path):
    customers_path = str(tmp_path / "customers")
    orders_path = str(tmp_path / "orders")
    output_path = str(tmp_path / "gold")
    customers_df.write.mode("overwrite").parquet(customers_path)
    orders_df.write.mode("overwrite").parquet(orders_path)
    return customers_path, orders_path, output_path


def render_pipeline(pipeline_paths):
    customers_path, orders_path, output_path = pipeline_paths
    return PIPELINE_TEMPLATE.format(
        customers_path=customers_path,
        orders_path=orders_path,
        output_path=output_path,
    )


def test_end_to_end_pipeline(spark, pipeline_paths, tmp_path):
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(render_pipeline(pipeline_paths))

    result = run_pipeline(
        config_path=str(config_path),
        parameters={"business_date": "2026-07-07", "environment": "test"},
        spark=spark,
        run_id="run-123",
    )

    assert result["status"] == "succeeded"
    assert result["process_name"] == "customer_orders_etl"
    assert result["run_id"] == "run-123"
    assert result["environment"] == "test"
    assert [s["step_name"] for s in result["steps"]] == [
        "read_customers",
        "read_orders",
        "filter_orders",
        "join_customers_orders",
        "enrich",
        "select_columns",
        "write_output",
    ]
    filter_step = result["steps"][2]
    assert filter_step["rows_processed"] == 3

    output = spark.read.parquet(pipeline_paths[2])
    assert output.count() == 3
    unknown = output.filter("customer_name = 'UNKNOWN'").count()
    assert unknown == 1  # order 103 has no matching customer
    assert set(output.columns) == {
        "customer_id",
        "customer_name",
        "order_id",
        "order_amount",
        "business_date",
    }


def test_pipeline_from_inline_content(spark, pipeline_paths):
    result = run_pipeline(
        config_content=render_pipeline(pipeline_paths),
        parameters={"business_date": "2026-07-07"},
        spark=spark,
    )
    assert result["status"] == "succeeded"


def test_missing_parameter_fails_before_execution(spark, pipeline_paths):
    with pytest.raises(MissingParameterError, match="business_date"):
        run_pipeline(
            config_content=render_pipeline(pipeline_paths),
            parameters={},
            spark=spark,
        )


def test_invalid_config_fails_before_any_write(spark, pipeline_paths, tmp_path):
    import os

    broken = render_pipeline(pipeline_paths).replace(
        "left: filtered_orders", "left: no_such_dataframe", 1
    )
    with pytest.raises(ValidationError, match="no_such_dataframe"):
        run_pipeline(
            config_content=broken,
            parameters={"business_date": "2026-07-07"},
            spark=spark,
        )
    # Fail fast: validation errors must prevent every step, including writes.
    assert not os.path.exists(pipeline_paths[2])


def test_runtime_failure_is_wrapped_with_step_context(spark, tmp_path):
    config = """
process_name: failing_pipeline
steps:
  - name: read_missing
    type: read
    format: parquet
    path: "{path}"
    alias: source
""".format(path=str(tmp_path / "does_not_exist"))
    with pytest.raises(StepExecutionError) as exc_info:
        run_pipeline(config_content=config, spark=spark)
    message = str(exc_info.value)
    assert "failing_pipeline" in message
    assert "read_missing" in message
    assert "read" in message
