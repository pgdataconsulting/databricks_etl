"""Tests for schema drift handling, the cache operation, and per-step retries."""

import pytest

from etl_framework.exceptions import StepExecutionError, ValidationError
from etl_framework.execution_context import ExecutionContext
from etl_framework.operations import OPERATION_REGISTRY, register_operation
from etl_framework.operations.base import Operation
from etl_framework.operations.cache import CacheOperation
from etl_framework.operations.merge import _AUTO_MERGE_CONF, MergeOperation
from etl_framework.operations.write import WriteOperation
from etl_framework.runner import run_pipeline


@pytest.fixture()
def context(spark, orders_df):
    ctx = ExecutionContext(spark, process_name="test_pipeline", run_id="test-run")
    ctx.register_dataframe("orders", orders_df)
    return ctx


def write_step(**overrides):
    step = {"name": "w", "type": "write", "input": "orders", "format": "delta", "mode": "append"}
    step.update(overrides)
    return step


def test_write_schema_drift_evolves_delta_target(spark, context, tmp_path):
    target = str(tmp_path / "delta_target")
    WriteOperation().execute(context, write_step(path=target))

    context.register_dataframe(
        "orders", context.get_dataframe("orders").selectExpr("*", "'web' AS channel")
    )
    # Default behaviour: a new column fails the append.
    with pytest.raises(Exception, match=r"[Ss]chema"):
        WriteOperation().execute(context, write_step(path=target))
    # With schema_drift: evolve, the target picks the column up.
    WriteOperation().execute(context, write_step(path=target, schema_drift="evolve"))
    result = spark.read.format("delta").load(target)
    assert "channel" in result.columns
    assert result.filter("channel IS NULL").count() == 4  # pre-drift rows


def test_write_schema_drift_overwrite_replaces_schema(spark, context, tmp_path):
    target = str(tmp_path / "delta_target")
    WriteOperation().execute(context, write_step(path=target, mode="overwrite"))

    context.register_dataframe(
        "orders",
        context.get_dataframe("orders").selectExpr("order_id", "order_date"),
    )
    WriteOperation().execute(
        context,
        write_step(path=target, mode="overwrite", schema_drift="overwrite"),
    )
    result = spark.read.format("delta").load(target)
    assert set(result.columns) == {"order_id", "order_date"}


def test_write_schema_drift_validation():
    on_parquet = WriteOperation.validate(
        write_step(format="parquet", path="/t", schema_drift="evolve")
    )
    wrong_mode = WriteOperation.validate(
        write_step(path="/t", mode="append", schema_drift="overwrite")
    )
    unknown = WriteOperation.validate(write_step(path="/t", schema_drift="explode"))
    assert any("requires a Delta target" in e for e in on_parquet)
    assert any("requires mode 'overwrite'" in e for e in wrong_mode)
    assert any("unsupported schema_drift" in e for e in unknown)


def test_merge_schema_evolution(spark, context, tmp_path):
    target = str(tmp_path / "merge_target")
    merge = {
        "name": "m",
        "type": "merge",
        "input": "orders",
        "merge_keys": ["order_id"],
        "path": target,
    }
    MergeOperation().execute(context, merge)

    context.register_dataframe(
        "orders",
        context.get_dataframe("orders").selectExpr("*", "'web' AS channel"),
    )
    # Without the opt-in, Delta's UPDATE SET * / INSERT * expand to the
    # *target* columns, so the drifted column is silently dropped.
    MergeOperation().execute(context, merge)
    assert "channel" not in spark.read.format("delta").load(target).columns
    MergeOperation().execute(context, dict(merge, schema_evolution=True))

    result = spark.read.format("delta").load(target)
    assert "channel" in result.columns
    assert result.filter("channel = 'web'").count() == 4
    # The session-wide evolution conf is restored after the merge.
    assert spark.conf.get(_AUTO_MERGE_CONF, None) is None


def test_cache_persists_at_requested_level(context):
    result = CacheOperation().execute(
        context,
        {
            "name": "c",
            "type": "cache",
            "input": "orders",
            "storage_level": "MEMORY_ONLY",
            "eager": True,
            "output": "cached",
        },
    )
    assert result.storageLevel.useMemory and not result.storageLevel.useDisk
    assert context.get_dataframe("cached").count() == 4
    result.unpersist()


def test_cache_local_checkpoint_truncates_lineage(context, orders_df):
    result = CacheOperation().execute(
        context,
        {
            "name": "c",
            "type": "cache",
            "input": "orders",
            "method": "local_checkpoint",
            "output": "snapshot",
        },
    )
    assert sorted(result.collect()) == sorted(orders_df.collect())
    # The plan was replaced by the materialized checkpoint.
    assert "LogicalRDD" in result._jdf.queryExecution().analyzed().toString()


def test_cache_checkpoint_with_directory(context, orders_df, tmp_path):
    checkpoint_dir = tmp_path / "chk"
    result = CacheOperation().execute(
        context,
        {
            "name": "c",
            "type": "cache",
            "input": "orders",
            "method": "checkpoint",
            "checkpoint_dir": str(checkpoint_dir),
            "output": "snapshot",
        },
    )
    assert sorted(result.collect()) == sorted(orders_df.collect())
    assert any(checkpoint_dir.rglob("*"))  # eagerly materialized to disk


def test_cache_validates_options():
    errors = CacheOperation.validate(
        {
            "name": "c",
            "input": "a",
            "output": "b",
            "method": "local_checkpoint",
            "storage_level": "MEMORY_ONLY",
            "checkpoint_dir": "/tmp/chk",
        }
    )
    message = " | ".join(errors)
    assert "'storage_level' only applies to method 'cache'" in message
    assert "'checkpoint_dir' only applies to method 'checkpoint'" in message
    unknown = CacheOperation.validate(
        {"name": "c", "input": "a", "output": "b", "storage_level": "TURBO"}
    )
    assert any("unknown storage_level" in e for e in unknown)


class FlakyOperation(Operation):
    """Test operation that fails a configurable number of times, then succeeds."""

    type_name = "flaky"
    required_fields = ("output",)
    calls = 0
    failures = 0

    def execute(self, context, step: dict):
        type(self).calls += 1
        if type(self).calls <= type(self).failures:
            raise RuntimeError("transient failure")
        df = context.spark.createDataFrame([(1,)], "id int")
        context.register_dataframe(step["output"], df)
        return df

    @classmethod
    def input_names(cls, step: dict) -> list:
        return []


@pytest.fixture()
def flaky_operation():
    register_operation(FlakyOperation)
    FlakyOperation.calls = 0
    yield FlakyOperation
    del OPERATION_REGISTRY[FlakyOperation.type_name]


FLAKY_CONFIG = """
process_name: retry_pipeline
steps:
  - name: flaky_step
    type: flaky
    output: result
    retry:
      attempts: 3
      delay_seconds: 0
"""


def test_retry_recovers_from_transient_failures(spark, flaky_operation):
    flaky_operation.failures = 2
    result = run_pipeline(config_content=FLAKY_CONFIG, spark=spark)
    assert result["status"] == "succeeded"
    assert flaky_operation.calls == 3
    assert result["steps"][0]["attempts"] == 3


def test_retry_exhaustion_fails_the_step(spark, flaky_operation):
    flaky_operation.failures = 10
    with pytest.raises(StepExecutionError, match="transient failure"):
        run_pipeline(config_content=FLAKY_CONFIG, spark=spark)
    assert flaky_operation.calls == 3  # attempts, then give up


def test_step_without_retry_runs_once(spark, flaky_operation):
    flaky_operation.failures = 1
    config = FLAKY_CONFIG.split("    retry:")[0]
    with pytest.raises(StepExecutionError):
        run_pipeline(config_content=config, spark=spark)
    assert flaky_operation.calls == 1


def test_retry_spec_is_validated(spark, flaky_operation):
    config = FLAKY_CONFIG.replace("attempts: 3", "attempts: 0")
    with pytest.raises(ValidationError, match=r"retry\.attempts"):
        run_pipeline(config_content=config, spark=spark)
