"""Tests for nested pipelines: running one configuration inside another."""

import pytest

from etl_framework.cli import validate_command
from etl_framework.config_validator import validate_config
from etl_framework.exceptions import (
    StepExecutionError,
    ValidationError,
)
from etl_framework.runner import run_pipeline

CHILD_CONFIG = """
process_name: standardize_orders
inputs: [raw_orders]
parameters:
  business_date: null
steps:
  - name: filter_business_date
    type: filter
    input: raw_orders
    condition: "order_date = '${business_date}'"
    output: filtered
  - name: enrich
    type: expression
    input: filtered
    expressions:
      order_amount: "cast(order_amount as decimal(18,2))"
    output: standardized
"""

PARENT_CONFIG = """
process_name: parent_pipeline
parameters:
  business_date: null
  environment: "dev"
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{orders_path}"
    alias: orders
  - name: standardize
    type: run_config
    config_path: child.yaml
    parameters:
      business_date: "${{business_date}}"
    inputs:
      raw_orders: orders
    outputs:
      orders_std: standardized
  - name: write_output
    type: write
    input: orders_std
    format: parquet
    mode: overwrite
    path: "{output_path}"
"""


@pytest.fixture()
def orders_path(orders_df, tmp_path):
    path = str(tmp_path / "orders")
    orders_df.write.mode("overwrite").parquet(path)
    return path


def write_configs(tmp_path, orders_path, output_path):
    parent = tmp_path / "parent.yaml"
    parent.write_text(
        PARENT_CONFIG.format(
            orders_path=orders_path,
            output_path=output_path,
        )
    )
    (tmp_path / "child.yaml").write_text(CHILD_CONFIG)
    return parent


def test_nested_pipeline_end_to_end(spark, orders_path, tmp_path):
    output_path = str(tmp_path / "gold")
    parent = write_configs(tmp_path, orders_path, output_path)

    result = run_pipeline(
        config_path=str(parent),
        parameters={"business_date": "2026-07-07", "environment": "test"},
        spark=spark,
        run_id="run-1",
    )

    assert result["status"] == "succeeded"
    assert [s["step_name"] for s in result["steps"]] == [
        "read_orders",
        "standardize",
        "write_output",
    ]
    nested_step = result["steps"][1]
    assert nested_step["step_type"] == "run_config"
    assert nested_step["inputs"] == ["orders"]
    assert nested_step["output"] == "orders_std"

    # The child run is a full pipeline run of its own, recorded in the summary.
    (child_run,) = result["nested_runs"]
    assert child_run["step_name"] == "standardize"
    assert child_run["process_name"] == "standardize_orders"
    assert child_run["run_id"] == "run-1:standardize"
    assert child_run["environment"] == "test"  # inherited from the parent
    assert child_run["status"] == "succeeded"
    assert [s["step_name"] for s in child_run["steps"]] == [
        "filter_business_date",
        "enrich",
    ]

    output = spark.read.parquet(output_path)
    assert output.count() == 3  # orders on 2026-07-07


def test_child_config_validates_standalone(orders_path, tmp_path):
    parent = write_configs(tmp_path, orders_path, str(tmp_path / "gold"))
    # Declared top-level inputs stand in for the caller-provided DataFrames,
    # so both configs pass CLI validation without a parent run.
    assert validate_command([str(parent), str(tmp_path / "child.yaml")]) == 0


def test_child_with_declared_inputs_cannot_run_standalone(spark, tmp_path):
    child = tmp_path / "child.yaml"
    child.write_text(CHILD_CONFIG)
    with pytest.raises(ValidationError, match=r"raw_orders.*was not provided"):
        run_pipeline(
            config_path=str(child),
            parameters={"business_date": "2026-07-07"},
            spark=spark,
        )


def test_circular_nested_reference_fails(spark, tmp_path):
    (tmp_path / "a.yaml").write_text("""
process_name: pipeline_a
steps:
  - name: run_b
    type: run_config
    config_path: b.yaml
""")
    (tmp_path / "b.yaml").write_text("""
process_name: pipeline_b
steps:
  - name: run_a
    type: run_config
    config_path: a.yaml
""")
    with pytest.raises(StepExecutionError, match="circular"):
        run_pipeline(config_path=str(tmp_path / "a.yaml"), spark=spark)


def test_missing_child_output_fails(spark, orders_path, tmp_path):
    parent = write_configs(tmp_path, orders_path, str(tmp_path / "gold"))
    broken = parent.read_text().replace("orders_std: standardized", "orders_std: no_such_output")
    parent.write_text(broken)
    with pytest.raises(StepExecutionError, match="did not produce"):
        run_pipeline(
            config_path=str(parent),
            parameters={"business_date": "2026-07-07"},
            spark=spark,
        )


def test_invalid_child_config_fails_parent_step(spark, orders_path, tmp_path):
    parent = write_configs(tmp_path, orders_path, str(tmp_path / "gold"))
    (tmp_path / "child.yaml").write_text("""
process_name: broken_child
inputs: [raw_orders]
steps:
  - name: bad_step
    type: no_such_type
""")
    with pytest.raises(StepExecutionError, match="could not be loaded or is invalid"):
        run_pipeline(
            config_path=str(parent),
            parameters={"business_date": "2026-07-07"},
            spark=spark,
        )


def test_run_config_outputs_are_visible_downstream():
    config = {
        "process_name": "p",
        "steps": [
            {
                "name": "nested",
                "type": "run_config",
                "config_path": "c.yaml",
                "outputs": {"a": "x", "b": "y"},
            },
            {"name": "use_b", "type": "filter", "input": "b", "condition": "1 = 1", "output": "c"},
        ],
    }
    validate_config(config)  # must not raise


def test_run_config_mappings_must_be_string_mappings():
    config = {
        "process_name": "p",
        "steps": [
            {
                "name": "nested",
                "type": "run_config",
                "config_path": "c.yaml",
                "inputs": ["not", "a", "mapping"],
                "outputs": {"a": 1},
            },
        ],
    }
    with pytest.raises(ValidationError) as exc_info:
        validate_config(config)
    message = str(exc_info.value)
    assert "'inputs' must be a mapping" in message
    assert "'outputs' must be a mapping" in message


def test_dry_run_plans_nested_step(orders_path, tmp_path):
    parent = write_configs(tmp_path, orders_path, str(tmp_path / "gold"))
    result = run_pipeline(
        config_path=str(parent),
        parameters={"business_date": "2026-07-07"},
        dry_run=True,
    )
    nested = result["plan"][1]
    assert nested["step_type"] == "run_config"
    assert nested["target"] == "child.yaml"
    assert nested["inputs"] == ["orders"]
    assert nested["output"] == "orders_std"
