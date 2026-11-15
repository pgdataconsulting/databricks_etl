"""Tests for CDC, reconciliation, dynamic partition overwrite, and metrics."""

import pytest

from etl_framework.exceptions import ReconciliationError
from etl_framework.execution_context import ExecutionContext
from etl_framework.operations.cdc import CdcOperation
from etl_framework.operations.reconcile import ReconcileOperation
from etl_framework.operations.write import WriteOperation
from etl_framework.runner import run_pipeline


@pytest.fixture()
def context(spark):
    return ExecutionContext(spark, process_name="test_pipeline", run_id="run-x")


# --- cdc -------------------------------------------------------------------------


def cdc_step(path, **extra):
    step = {
        "name": "c",
        "type": "cdc",
        "input": "changes",
        "keys": ["id"],
        "operation_column": "_op",
        "path": path,
    }
    step.update(extra)
    return step


def read_target(spark, path):
    return sorted((r.id, r.v) for r in spark.read.format("delta").load(path).collect())


def test_cdc_initial_load_excludes_deletes(spark, context, tmp_path):
    target = str(tmp_path / "cdc_init")
    changes = spark.createDataFrame(
        [(1, "a", "I"), (2, "b", "I"), (3, "gone", "D")],
        "id int, v string, _op string",
    )
    context.register_dataframe("changes", changes)
    CdcOperation().execute(context, cdc_step(target))
    df = spark.read.format("delta").load(target)
    assert read_target(spark, target) == [(1, "a"), (2, "b")]
    assert "_op" not in df.columns


def test_cdc_applies_insert_update_delete(spark, context, tmp_path):
    target = str(tmp_path / "cdc_iud")
    initial = spark.createDataFrame(
        [(1, "a", "I"), (2, "b", "I")],
        "id int, v string, _op string",
    )
    context.register_dataframe("changes", initial)
    CdcOperation().execute(context, cdc_step(target))

    feed = spark.createDataFrame(
        [(2, "B", "U"), (3, "c", "I"), (1, None, "D")],
        "id int, v string, _op string",
    )
    context.register_dataframe("changes", feed)
    CdcOperation().execute(context, cdc_step(target))
    assert read_target(spark, target) == [(2, "B"), (3, "c")]


def test_cdc_sequence_by_keeps_latest_change(spark, context, tmp_path):
    target = str(tmp_path / "cdc_seq")
    initial = spark.createDataFrame(
        [(1, "a", "I", 1)],
        "id int, v string, _op string, seq int",
    )
    context.register_dataframe("changes", initial)
    CdcOperation().execute(context, cdc_step(target, sequence_by="seq"))

    # Replayed batch: two changes for id=1; only the latest (seq=3) applies.
    feed = spark.createDataFrame(
        [(1, "stale", "U", 2), (1, "fresh", "U", 3)],
        "id int, v string, _op string, seq int",
    )
    context.register_dataframe("changes", feed)
    CdcOperation().execute(context, cdc_step(target, sequence_by="seq"))
    assert read_target(spark, target) == [(1, "fresh")]


def test_cdc_custom_operation_values(spark, context, tmp_path):
    target = str(tmp_path / "cdc_custom")
    initial = spark.createDataFrame(
        [(1, "a", "insert")],
        "id int, v string, _op string",
    )
    context.register_dataframe("changes", initial)
    values = {"insert": "insert", "update": "update", "delete": "delete"}
    CdcOperation().execute(context, cdc_step(target, operation_values=values))

    feed = spark.createDataFrame(
        [(1, None, "delete")],
        "id int, v string, _op string",
    )
    context.register_dataframe("changes", feed)
    CdcOperation().execute(context, cdc_step(target, operation_values=values))
    assert read_target(spark, target) == []


def test_cdc_validation():
    errors = CdcOperation.validate(
        {
            "name": "c",
            "type": "cdc",
            "input": "x",
            "keys": ["id"],
            "operation_column": "_op",
            "path": "/t",
            "format": "parquet",
            "operation_values": {"upsert": "X"},
        },
    )
    joined = "\n".join(errors)
    assert "Delta" in joined
    assert "unknown operation(s) upsert" in joined


# --- reconcile ---------------------------------------------------------------------


def reconcile_step(**extra):
    step = {"name": "rec", "type": "reconcile", "left": "source", "right": "loaded", "keys": ["id"]}
    step.update(extra)
    return step


def test_reconcile_passes_when_identical(spark, context):
    df = spark.createDataFrame([(1, "a", 10.0), (2, "b", 20.0)], "id int, v string, amount double")
    context.register_dataframe("source", df)
    context.register_dataframe("loaded", df)
    report = ReconcileOperation().execute(context, reconcile_step())
    assert report.count() == 0


def test_reconcile_detects_all_difference_kinds(spark, context):
    source = spark.createDataFrame(
        [(1, "a", 10.0), (2, "b", 20.0), (3, "c", 30.0)],
        "id int, v string, amount double",
    )
    loaded = spark.createDataFrame(
        [(1, "a", 10.0), (2, "b", 99.0), (4, "d", 40.0)],
        "id int, v string, amount double",
    )
    context.register_dataframe("source", source)
    context.register_dataframe("loaded", loaded)
    with pytest.raises(ReconciliationError) as exc_info:
        ReconcileOperation().execute(context, reconcile_step())
    error = exc_info.value
    assert error.counts == {"missing_in_right": 1, "missing_in_left": 1, "mismatch": 1}
    assert "missing_in_right=1" in str(error)


def test_reconcile_warn_registers_report(spark, context, caplog):
    source = spark.createDataFrame([(1, "a")], "id int, v string")
    loaded = spark.createDataFrame([(1, "CHANGED")], "id int, v string")
    context.register_dataframe("source", source)
    context.register_dataframe("loaded", loaded)
    with caplog.at_level("WARNING", logger="etl_framework"):
        ReconcileOperation().execute(
            context,
            reconcile_step(on_failure="warn", output="report"),
        )
    report = context.get_dataframe("report").collect()
    assert len(report) == 1
    assert report[0].reconcile_status == "mismatch"
    assert report[0].mismatched_columns == "v"
    assert any("reconciliation" in r.message for r in caplog.records)


def test_reconcile_tolerance_absorbs_small_numeric_diffs(spark, context):
    source = spark.createDataFrame([(1, 10.001)], "id int, amount double")
    loaded = spark.createDataFrame([(1, 10.002)], "id int, amount double")
    context.register_dataframe("source", source)
    context.register_dataframe("loaded", loaded)
    report = ReconcileOperation().execute(
        context,
        reconcile_step(tolerance=0.01),
    )
    assert report.count() == 0
    with pytest.raises(ReconciliationError):
        ReconcileOperation().execute(
            context,
            reconcile_step(tolerance=0.0001),
        )


def test_reconcile_compare_columns_subset(spark, context):
    source = spark.createDataFrame([(1, "a", "x")], "id int, v string, noise string")
    loaded = spark.createDataFrame([(1, "a", "y")], "id int, v string, noise string")
    context.register_dataframe("source", source)
    context.register_dataframe("loaded", loaded)
    report = ReconcileOperation().execute(
        context,
        reconcile_step(compare_columns=["v"]),
    )
    assert report.count() == 0


# --- dynamic partition overwrite -----------------------------------------------------


def test_overwrite_partitions_replaces_only_written_partitions(spark, context, tmp_path):
    target = str(tmp_path / "dyn")
    full = spark.createDataFrame(
        [("2026-07-06", 1), ("2026-07-06", 2), ("2026-07-07", 3)],
        "business_date string, order_id int",
    )
    context.register_dataframe("full", full)
    WriteOperation().execute(
        context,
        {
            "name": "w1",
            "type": "write",
            "input": "full",
            "format": "parquet",
            "mode": "overwrite",
            "partition_by": ["business_date"],
            "path": target,
        },
    )

    # Rerun only 2026-07-07 with different data; 07-06 must survive.
    rerun = spark.createDataFrame(
        [("2026-07-07", 99)],
        "business_date string, order_id int",
    )
    context.register_dataframe("rerun", rerun)
    WriteOperation().execute(
        context,
        {
            "name": "w2",
            "type": "write",
            "input": "rerun",
            "format": "parquet",
            "mode": "overwrite_partitions",
            "partition_by": ["business_date"],
            "path": target,
        },
    )

    result = spark.read.parquet(target)
    assert sorted(r.order_id for r in result.collect()) == [1, 2, 99]


def test_overwrite_partitions_requires_partition_by():
    errors = WriteOperation.validate(
        {
            "name": "w",
            "type": "write",
            "input": "df",
            "format": "parquet",
            "mode": "overwrite_partitions",
            "path": "/t",
        }
    )
    assert any("requires 'partition_by'" in e for e in errors)


# --- metrics ---------------------------------------------------------------------------


def test_run_summary_includes_metrics(spark, orders_df, tmp_path):
    source = str(tmp_path / "src")
    orders_df.write.parquet(source)
    config = f"""
process_name: metrics_demo
settings:
  log_row_counts: true
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: write_orders
    type: write
    input: orders
    format: parquet
    mode: overwrite
    path: "{tmp_path / "out"}"
"""
    result = run_pipeline(config_content=config, spark=spark)
    metrics = result["metrics"]
    assert metrics["steps_executed"] == 2
    assert metrics["total_duration_seconds"] > 0
    assert metrics["slowest_step"] in ("read_orders", "write_orders")
    assert metrics["total_rows_processed"] == 8  # 4 rows counted per step
    by_name = {s["step_name"]: s for s in metrics["steps"]}
    read_metrics = by_name["read_orders"]
    assert read_metrics["rows_processed"] == 4
    assert read_metrics["rows_per_second"] > 0
    assert 0 <= read_metrics["pct_of_total"] <= 100
    # Sorted slowest-first.
    durations = [s["duration_seconds"] for s in metrics["steps"]]
    assert durations == sorted(durations, reverse=True)


def test_metrics_ignore_skipped_steps(spark, orders_df, tmp_path):
    source = str(tmp_path / "src")
    orders_df.write.parquet(source)
    config = f"""
process_name: metrics_skip
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: never_runs
    type: write
    input: orders
    format: parquet
    mode: overwrite
    when: "1 = 0"
    path: "{tmp_path / "never"}"
"""
    result = run_pipeline(config_content=config, spark=spark)
    assert result["metrics"]["steps_executed"] == 1
    assert [s["step_name"] for s in result["metrics"]["steps"]] == ["read_orders"]
