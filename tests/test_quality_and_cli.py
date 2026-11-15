"""Tests for quality checks, dry-run mode, conditional steps, and the CLI."""

import os

import pytest

from etl_framework.cli import docs_command, validate_command
from etl_framework.exceptions import QualityCheckError
from etl_framework.execution_context import ExecutionContext
from etl_framework.operations.quality import QualityOperation
from etl_framework.runner import run_pipeline


@pytest.fixture()
def context(spark, orders_df):
    ctx = ExecutionContext(spark, process_name="quality_pipeline", run_id="run-q")
    ctx.register_dataframe("orders", orders_df)
    return ctx


def quality_step(checks, **extra):
    step = {
        "name": "q",
        "type": "quality",
        "input": "orders",
        "checks": checks,
        "output": "checked",
    }
    step.update(extra)
    return step


# --- quality: passing checks ----------------------------------------------------


def test_all_checks_pass(context, orders_df):
    QualityOperation().execute(
        context,
        quality_step(
            [
                {"type": "not_null", "columns": ["order_id", "customer_id"]},
                {"type": "unique", "columns": ["order_id"]},
                {"type": "expression", "condition": "order_amount > 0"},
                {"type": "row_count", "min": 1, "max": 100},
            ]
        ),
    )
    assert context.get_dataframe("checked").count() == orders_df.count()


# --- quality: on_failure modes --------------------------------------------------


def test_fail_mode_lists_all_failures(spark, context):
    df = spark.createDataFrame(
        [(1, None, 5.0), (1, "x", -2.0)],
        "order_id int, status string, amount double",
    )
    context.register_dataframe("orders", df)
    with pytest.raises(QualityCheckError) as exc_info:
        QualityOperation().execute(
            context,
            quality_step(
                [
                    {"type": "not_null", "columns": ["status"]},
                    {"type": "unique", "columns": ["order_id"]},
                    {"type": "expression", "condition": "amount >= 0", "name": "non_negative"},
                    {"type": "row_count", "min": 5},
                ]
            ),
        )
    message = str(exc_info.value)
    assert "not_null(status): 1 of 2" in message
    assert "unique(order_id): 1 duplicate" in message
    assert "non_negative: 1 of 2" in message
    assert "row_count(min=5): 2 row(s)" in message
    assert "quality_pipeline" in message


def test_warn_mode_passes_rows_through(spark, context, caplog):
    df = spark.createDataFrame([(1, None)], "order_id int, status string")
    context.register_dataframe("orders", df)
    QualityOperation().execute(
        context,
        quality_step(
            [{"type": "not_null", "columns": ["status"]}],
            on_failure="warn",
        ),
    )
    assert context.get_dataframe("checked").count() == 1
    assert any("data quality checks failed" in r.message for r in caplog.records)


def test_quarantine_mode_splits_rows(spark, context, tmp_path):
    df = spark.createDataFrame(
        [(1, "ok"), (2, None), (3, "ok")],
        "order_id int, status string",
    )
    context.register_dataframe("orders", df)
    quarantine = str(tmp_path / "quarantine")
    QualityOperation().execute(
        context,
        quality_step(
            [{"type": "not_null", "columns": ["status"]}],
            on_failure="quarantine",
            quarantine_path=quarantine,
            quarantine_format="parquet",
        ),
    )
    passed = context.get_dataframe("checked")
    assert sorted(r.order_id for r in passed.collect()) == [1, 3]
    rejected = spark.read.parquet(quarantine)
    assert [r.order_id for r in rejected.collect()] == [2]


def test_quarantine_cannot_absorb_dataset_failures(spark, context, tmp_path):
    df = spark.createDataFrame([(1,), (1,)], "order_id int")
    context.register_dataframe("orders", df)
    with pytest.raises(QualityCheckError, match="unique"):
        QualityOperation().execute(
            context,
            quality_step(
                [{"type": "unique", "columns": ["order_id"]}],
                on_failure="quarantine",
                quarantine_path=str(tmp_path / "q"),
            ),
        )


def test_accepted_values_check(spark, context):
    df = spark.createDataFrame(
        [(1, "NEW"), (2, "BOGUS"), (3, None)],
        "order_id int, status string",
    )
    context.register_dataframe("orders", df)
    with pytest.raises(QualityCheckError, match=r"accepted_values\(status\): 1 of 3"):
        QualityOperation().execute(
            context,
            quality_step(
                [{"type": "accepted_values", "column": "status", "values": ["NEW", "SHIPPED"]}],
            ),
        )


def test_quality_validation():
    errors = QualityOperation.validate(
        quality_step(
            [{"type": "nonsense"}, {"type": "not_null"}, {"type": "row_count"}],
            on_failure="explode",
        )
    )
    joined = "\n".join(errors)
    assert "unknown check type 'nonsense'" in joined
    assert "'columns' is required" in joined
    assert "'min' and/or 'max'" in joined
    assert "invalid on_failure 'explode'" in joined

    errors = QualityOperation.validate(
        quality_step(
            [{"type": "not_null", "columns": ["a"]}],
            on_failure="quarantine",
        )
    )
    assert any("quarantine_path" in e for e in errors)


# --- dry run ---------------------------------------------------------------------

DRY_RUN_CONFIG = """
process_name: dry_run_demo
parameters:
  business_date: null
steps:
  - name: read_orders
    type: read
    format: parquet
    path: /data/orders
    alias: orders
  - name: filter_orders
    type: filter
    input: orders
    condition: "order_date = '${business_date}'"
    when: "'${business_date}' != ''"
    output: filtered
  - name: write_orders
    type: write
    input: filtered
    format: delta
    mode: overwrite
    path: /data/out
"""


def test_dry_run_returns_plan_without_executing():
    # No spark fixture: dry run must not need a Spark session.
    result = run_pipeline(
        config_content=DRY_RUN_CONFIG,
        parameters={"business_date": "2026-07-07"},
        dry_run=True,
    )
    assert result["status"] == "dry_run"
    assert [p["step_name"] for p in result["plan"]] == [
        "read_orders",
        "filter_orders",
        "write_orders",
    ]
    write_plan = result["plan"][2]
    assert write_plan["inputs"] == ["filtered"]
    assert write_plan["target"] == "/data/out"
    assert result["plan"][1]["conditional"] is True
    assert not os.path.exists("/data/out")


# --- conditional steps -----------------------------------------------------------


def test_when_condition_skips_step(spark, orders_df, tmp_path):
    source = str(tmp_path / "src")
    orders_df.write.parquet(source)
    config = f"""
process_name: conditional_demo
parameters:
  environment: "dev"
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: write_prod_only
    type: write
    input: orders
    format: parquet
    mode: overwrite
    when: "'${{environment}}' = 'prod'"
    path: "{tmp_path / "prod_out"}"
"""
    result = run_pipeline(config_content=config, spark=spark, parameters={"environment": "dev"})
    assert result["status"] == "succeeded"
    skipped = result["steps"][1]
    assert skipped["status"] == "skipped"
    assert not os.path.exists(str(tmp_path / "prod_out"))

    result = run_pipeline(config_content=config, spark=spark, parameters={"environment": "prod"})
    assert result["steps"][1]["status"] == "succeeded"
    assert spark.read.parquet(str(tmp_path / "prod_out")).count() == 4


# --- CLI --------------------------------------------------------------------------

VALID_CONFIG = """
process_name: cli_demo
parameters:
  business_date: null
steps:
  - name: read_orders
    type: read
    format: parquet
    path: /data/${business_date}/orders
    alias: orders
  - name: write_orders
    type: write
    input: orders
    format: delta
    mode: overwrite
    path: /data/out
"""


def test_cli_validate_ok(tmp_path, capsys):
    config = tmp_path / "ok.yaml"
    config.write_text(VALID_CONFIG)
    assert validate_command([str(config)]) == 0
    assert "OK" in capsys.readouterr().out


def test_cli_validate_fails_with_exit_code(tmp_path, capsys):
    config = tmp_path / "bad.yaml"
    config.write_text(VALID_CONFIG.replace("input: orders", "input: nope"))
    assert validate_command([str(config)]) == 1
    output = capsys.readouterr().out
    assert "FAIL" in output
    assert "nope" in output


def test_cli_validate_repo_examples(capsys):
    assert (
        validate_command(["configs/customer_orders.yaml", "configs/customer_order_summary.yaml"])
        == 0
    )


def test_cli_docs_generates_markdown(tmp_path):
    config = tmp_path / "doc.yaml"
    config.write_text(VALID_CONFIG)
    output = tmp_path / "doc.md"
    assert docs_command(str(config), str(output)) == 0
    document = output.read_text()
    assert "# Pipeline: cli_demo" in document
    assert "| `business_date` | `(required at runtime)` |" in document
    assert "```mermaid" in document
    assert "read_orders" in document
    assert "orders((orders))" in document
