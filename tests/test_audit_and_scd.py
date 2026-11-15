"""Tests for built-in parameters, standard audit columns, merge, and SCD2."""

import pytest

from etl_framework.config_validator import validate_config
from etl_framework.exceptions import ValidationError
from etl_framework.execution_context import ExecutionContext
from etl_framework.operations.merge import MergeOperation
from etl_framework.operations.scd2 import Scd2Operation
from etl_framework.runner import run_pipeline

# --- built-in parameters -------------------------------------------------------


def test_run_id_available_as_parameter(spark, orders_df, tmp_path):
    source = str(tmp_path / "orders_src")
    orders_df.write.parquet(source)
    config = f"""
process_name: builtin_params
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: stamp
    type: add_columns
    input: orders
    columns:
      run_id: "'${{run_id}}'"
      pipeline: "'${{process_name}}'"
      started_at: "to_timestamp('${{run_started_at}}')"
    output: stamped
  - name: write
    type: write
    input: stamped
    format: parquet
    mode: overwrite
    path: "{tmp_path / "out"}"
"""
    result = run_pipeline(config_content=config, spark=spark, run_id="adf-run-42")
    assert result["status"] == "succeeded"
    row = spark.read.parquet(str(tmp_path / "out")).collect()[0]
    assert row.run_id == "adf-run-42"
    assert row.pipeline == "builtin_params"
    assert row.started_at is not None


def test_explicit_parameter_overrides_builtin(spark, orders_df, tmp_path):
    source = str(tmp_path / "orders_src")
    orders_df.write.parquet(source)
    config = f"""
process_name: builtin_override
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: stamp
    type: add_columns
    input: orders
    columns:
      run_id: "'${{run_id}}'"
    output: stamped
  - name: write
    type: write
    input: stamped
    format: parquet
    mode: overwrite
    path: "{tmp_path / "out"}"
"""
    run_pipeline(
        config_content=config,
        spark=spark,
        run_id="generated",
        parameters={"run_id": "user-supplied"},
    )
    row = spark.read.parquet(str(tmp_path / "out")).collect()[0]
    assert row.run_id == "user-supplied"


# --- standard audit columns ----------------------------------------------------

AUDIT_PIPELINE = """
process_name: audit_pipeline
parameters:
  environment: "test"
settings:
  audit_columns: {audit_columns}
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: write
    type: write
    input: orders
    format: parquet
    mode: overwrite
    path: "{target}"
"""


def test_audit_columns_true_adds_all(spark, orders_df, tmp_path):
    source = str(tmp_path / "src")
    orders_df.write.parquet(source)
    config = AUDIT_PIPELINE.format(
        audit_columns="true",
        source=source,
        target=str(tmp_path / "out"),
    )
    run_pipeline(config_content=config, spark=spark, run_id="audit-run")
    df = spark.read.parquet(str(tmp_path / "out"))
    for column in ("_run_id", "_process_name", "_environment", "_load_timestamp"):
        assert column in df.columns
    row = df.collect()[0]
    assert row._run_id == "audit-run"
    assert row._process_name == "audit_pipeline"
    assert row._environment == "test"
    assert row._load_timestamp is not None


def test_audit_columns_subset(spark, orders_df, tmp_path):
    source = str(tmp_path / "src")
    orders_df.write.parquet(source)
    config = AUDIT_PIPELINE.format(
        audit_columns="[_run_id, _load_timestamp]",
        source=source,
        target=str(tmp_path / "out"),
    )
    run_pipeline(config_content=config, spark=spark, run_id="audit-run")
    df = spark.read.parquet(str(tmp_path / "out"))
    assert "_run_id" in df.columns
    assert "_load_timestamp" in df.columns
    assert "_process_name" not in df.columns


def test_audit_columns_validation():
    config = {
        "process_name": "p",
        "settings": {"audit_columns": ["_run_id", "_bogus"]},
        "steps": [
            {"name": "r", "type": "read", "format": "parquet", "path": "/tmp/x", "alias": "a"},
        ],
    }
    with pytest.raises(ValidationError, match="_bogus"):
        validate_config(config)


# --- merge ----------------------------------------------------------------------


@pytest.fixture()
def merge_context(spark):
    return ExecutionContext(spark, process_name="merge_pipeline", run_id="run-m")


def merge_step(path, **extra):
    step = {"name": "m", "type": "merge", "input": "updates", "merge_keys": ["id"], "path": path}
    step.update(extra)
    return step


def test_merge_creates_target_then_upserts(spark, merge_context, tmp_path):
    target = str(tmp_path / "merge_target")
    initial = spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string")
    merge_context.register_dataframe("updates", initial)
    MergeOperation().execute(merge_context, merge_step(target))
    assert sorted((r.id, r.v) for r in spark.read.format("delta").load(target).collect()) == [
        (1, "a"),
        (2, "b"),
    ]

    updates = spark.createDataFrame([(2, "B"), (3, "c")], "id int, v string")
    merge_context.register_dataframe("updates", updates)
    MergeOperation().execute(merge_context, merge_step(target))
    assert sorted((r.id, r.v) for r in spark.read.format("delta").load(target).collect()) == [
        (1, "a"),
        (2, "B"),
        (3, "c"),
    ]


def test_merge_delete_condition(spark, merge_context, tmp_path):
    target = str(tmp_path / "merge_del")
    initial = spark.createDataFrame(
        [(1, "keep", False), (2, "kill", False)],
        "id int, v string, deleted boolean",
    )
    merge_context.register_dataframe("updates", initial)
    MergeOperation().execute(merge_context, merge_step(target))

    updates = spark.createDataFrame(
        [(2, "kill", True)],
        "id int, v string, deleted boolean",
    )
    merge_context.register_dataframe("updates", updates)
    MergeOperation().execute(
        merge_context,
        merge_step(target, delete_condition="s.deleted = true"),
    )
    rows = spark.read.format("delta").load(target).collect()
    assert [r.id for r in rows] == [1]


def test_merge_validation():
    errors = MergeOperation.validate({"name": "m", "type": "merge", "input": "x", "merge_keys": []})
    joined = "\n".join(errors)
    assert "merge_keys" in joined
    assert "'path' or 'table'" in joined
    errors = MergeOperation.validate(
        {
            "name": "m",
            "type": "merge",
            "input": "x",
            "merge_keys": ["id"],
            "path": "/t",
            "format": "parquet",
        }
    )
    assert any("Delta" in e for e in errors)


# --- scd2 -----------------------------------------------------------------------


@pytest.fixture()
def scd_context(spark):
    return ExecutionContext(spark, process_name="scd_pipeline", run_id="run-s")


def scd_step(path, **extra):
    step = {
        "name": "s",
        "type": "scd2",
        "input": "customers",
        "keys": ["customer_id"],
        "path": path,
    }
    step.update(extra)
    return step


def read_scd(spark, path):
    return {
        (r.customer_id, r.customer_name, r._is_current, r._effective_to is None)
        for r in spark.read.format("delta").load(path).collect()
    }


def test_scd2_initial_and_incremental_load(spark, scd_context, tmp_path):
    target = str(tmp_path / "scd_target")
    day1 = spark.createDataFrame(
        [(1, "Alice"), (2, "Bob")],
        "customer_id int, customer_name string",
    )
    scd_context.register_dataframe("customers", day1)
    Scd2Operation().execute(scd_context, scd_step(target))
    assert read_scd(spark, target) == {
        (1, "Alice", True, True),
        (2, "Bob", True, True),
    }

    # Day 2: Alice renamed, Bob unchanged, Carol new.
    day2 = spark.createDataFrame(
        [(1, "Alicia"), (2, "Bob"), (3, "Carol")],
        "customer_id int, customer_name string",
    )
    scd_context.register_dataframe("customers", day2)
    Scd2Operation().execute(scd_context, scd_step(target))
    assert read_scd(spark, target) == {
        (1, "Alice", False, False),  # closed history version
        (1, "Alicia", True, True),  # new current version
        (2, "Bob", True, True),  # untouched
        (3, "Carol", True, True),  # newly inserted
    }

    # Day 3: no changes -> nothing new, history preserved.
    scd_context.register_dataframe("customers", day2)
    Scd2Operation().execute(scd_context, scd_step(target))
    df = spark.read.format("delta").load(target)
    assert df.count() == 4
    assert df.filter("_is_current").count() == 3


def test_scd2_tracked_columns_subset(spark, scd_context, tmp_path):
    target = str(tmp_path / "scd_tracked")
    day1 = spark.createDataFrame(
        [(1, "Alice", "note-a")],
        "customer_id int, customer_name string, comment string",
    )
    scd_context.register_dataframe("customers", day1)
    Scd2Operation().execute(
        scd_context,
        scd_step(target, tracked_columns=["customer_name"]),
    )
    # Only the untracked column changes -> no new version.
    day2 = spark.createDataFrame(
        [(1, "Alice", "note-b")],
        "customer_id int, customer_name string, comment string",
    )
    scd_context.register_dataframe("customers", day2)
    Scd2Operation().execute(
        scd_context,
        scd_step(target, tracked_columns=["customer_name"]),
    )
    assert spark.read.format("delta").load(target).count() == 1


def test_scd2_effective_time_expression(spark, scd_context, tmp_path):
    target = str(tmp_path / "scd_eff")
    day1 = spark.createDataFrame([(1, "Alice")], "customer_id int, customer_name string")
    scd_context.register_dataframe("customers", day1)
    Scd2Operation().execute(
        scd_context,
        scd_step(target, effective_time="to_timestamp('2026-07-07')"),
    )
    row = spark.read.format("delta").load(target).collect()[0]
    assert str(row._effective_from).startswith("2026-07-07")


def test_scd2_validation():
    errors = Scd2Operation.validate(
        {
            "name": "s",
            "type": "scd2",
            "input": "x",
            "keys": ["id"],
            "path": "/t",
            "format": "csv",
            "tracked_columns": [],
        }
    )
    joined = "\n".join(errors)
    assert "unsupported scd2 format 'csv'" in joined
    assert "tracked_columns" in joined


# --- end to end: scd2 in a pipeline ---------------------------------------------


def test_scd2_pipeline_with_audit_columns(spark, tmp_path):
    source = str(tmp_path / "src")
    spark.createDataFrame(
        [(1, "Alice"), (2, "Bob")],
        "customer_id int, customer_name string",
    ).write.parquet(source)
    target = str(tmp_path / "scd_out")
    config = f"""
process_name: scd_e2e
settings:
  audit_columns: [_run_id]
steps:
  - name: read_customers
    type: read
    format: parquet
    path: "{source}"
    alias: customers
  - name: load_history
    type: scd2
    input: customers
    keys: [customer_id]
    path: "{target}"
"""
    result = run_pipeline(config_content=config, spark=spark, run_id="scd-run-1")
    assert result["status"] == "succeeded"
    df = spark.read.format("delta").load(target)
    assert df.filter("_is_current").count() == 2
    assert df.collect()[0]._run_id == "scd-run-1"
