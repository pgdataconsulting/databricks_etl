"""Every way a resolved secret could leave a run, closed and kept closed.

Each test here corresponds to a path found in review: a place where the value
was reachable from the exception that reached the caller, or where masking
did damage of its own.
"""

import json
import traceback

import pytest

from etl_framework.exceptions import (
    ExecutionError,
    ExpressionExecutionError,
    QualityCheckError,
    StepExecutionError,
)
from etl_framework.operations import OPERATION_REGISTRY, Operation, register_operation
from etl_framework.runner import _run_pipeline, run_pipeline
from etl_framework.secrets import REDACTED, SecretResolver

SECRET_VALUE = "v3ry-s3cr3t-folder"


class Provider:
    def __init__(self, values, failures_before_success=0, error=None):
        self.values = values
        self.failures_left = failures_before_success
        self.error = error
        self.calls = 0

    def get(self, scope, key):
        self.calls += 1
        if self.failures_left:
            self.failures_left -= 1
            raise self.error or ConnectionError("secret store unavailable")
        return self.values[(scope, key)]


@pytest.fixture()
def provider():
    return Provider({("demo", "folder"): SECRET_VALUE})


@pytest.fixture()
def temporary_operation():
    """Register operations for one test and remove them afterwards."""
    before = dict(OPERATION_REGISTRY)
    yield register_operation
    OPERATION_REGISTRY.clear()
    OPERATION_REGISTRY.update(before)


def run(spark, config, provider, **dataframes):
    return _run_pipeline(
        config_content=config,
        spark=spark,
        secret_provider=provider,
        initial_dataframes=dataframes,
    )


def everything_reachable_from(exc) -> str:
    """What a traceback printer and an error reporter would show for `exc`."""
    parts = ["".join(traceback.format_exception(exc))]
    seen, pending = set(), [exc]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        parts.append(repr(vars(current)))
        # Local variables of every frame in the traceback (what Sentry,
        # `pytest --showlocals` or a verbose IPython traceback report).
        tb = current.__traceback__
        while tb is not None:
            parts.append(repr(tb.tb_frame.f_locals))
            tb = tb.tb_next
        pending.extend([current.__cause__, current.__context__])
    return "\n".join(parts)


def assert_clean(exc):
    assert exc.__cause__ is None
    assert exc.__context__ is None
    assert SECRET_VALUE not in everything_reachable_from(exc)


# --- failures inside a step ---------------------------------------------------


def test_spark_error_that_quotes_a_secret(spark, tmp_path, provider):
    config = f"""
process_name: read_leak
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{tmp_path}/${{secret:demo/folder}}"
    alias: orders
"""
    with pytest.raises(StepExecutionError) as excinfo:
        run_pipeline(config_content=config, spark=spark, secret_provider=provider)
    assert REDACTED in str(excinfo.value)
    assert "step 'read_orders'" in str(excinfo.value)
    assert_clean(excinfo.value)


def test_framework_error_keeps_its_type_but_not_the_value(spark, orders_df, provider):
    config = """
process_name: expression_leak
inputs: [orders]
steps:
  - name: tag_rows
    type: expression
    input: orders
    expressions:
      tag: "no_such_function('${secret:demo/folder}')"
    output: tagged
"""
    with pytest.raises(ExpressionExecutionError) as excinfo:
        run(spark, config, provider, orders=orders_df)
    error = excinfo.value
    assert f"no_such_function('{REDACTED}')" in str(error)
    # The attributes the error carries are masked too, not only its message.
    assert error.expression == f"no_such_function('{REDACTED}')"
    assert error.original is None
    assert_clean(error)


def test_quality_failure_lists_checks_without_the_value(spark, orders_df, provider):
    config = """
process_name: quality_leak
inputs: [orders]
steps:
  - name: check
    type: quality
    input: orders
    checks:
      - {type: expression, condition: "order_date = '${secret:demo/folder}'"}
    output: checked
"""
    with pytest.raises(QualityCheckError) as excinfo:
        run(spark, config, provider, orders=orders_df)
    error = excinfo.value
    assert len(error.failures) == 1
    assert REDACTED in error.failures[0]
    assert_clean(error)


def test_clean_message_raised_from_a_cause_that_quotes_the_secret(
    spark, provider, temporary_operation
):
    class Connect(Operation):
        type_name = "connect"
        required_fields = ("url",)
        produces_output = False

        def execute(self, context, step):
            try:
                raise ValueError(f"could not connect to {step['url']}")
            except ValueError as exc:
                raise RuntimeError("connection pool exhausted") from exc

    temporary_operation(Connect)
    config = """
process_name: chained_leak
steps:
  - name: connect
    type: connect
    url: "jdbc://db/${secret:demo/folder}"
"""
    with pytest.raises(StepExecutionError) as excinfo:
        run(spark, config, provider)
    assert "RuntimeError: connection pool exhausted" in str(excinfo.value)
    assert_clean(excinfo.value)


def test_failure_of_a_step_with_secrets_is_detached_even_when_nothing_is_quoted(
    spark, provider, temporary_operation
):
    # The message is harmless, but the frames of the failing operation hold the
    # resolved step, so the original exception is not chained.
    class Fail(Operation):
        type_name = "fail_quietly"
        required_fields = ("token",)
        produces_output = False

        def execute(self, context, step):
            raise RuntimeError("timed out")

    temporary_operation(Fail)
    config = """
process_name: quiet_failure
steps:
  - name: call
    type: fail_quietly
    token: "${secret:demo/folder}"
"""
    with pytest.raises(StepExecutionError) as excinfo:
        run(spark, config, provider)
    assert "RuntimeError: timed out" in str(excinfo.value)
    assert_clean(excinfo.value)


def test_failure_of_a_step_without_secrets_keeps_its_cause(spark, temporary_operation):
    # No secret involved: debugging information is not thrown away.
    class Fail(Operation):
        type_name = "fail_plainly"
        produces_output = False

        def execute(self, context, step):
            raise RuntimeError("timed out")

    temporary_operation(Fail)
    config = "process_name: plain_failure\nsteps:\n  - {name: call, type: fail_plainly}\n"
    with pytest.raises(StepExecutionError) as excinfo:
        run_pipeline(config_content=config, spark=spark)
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert excinfo.value.original is excinfo.value.__cause__
    assert "Original exception: RuntimeError: timed out" in str(excinfo.value)


# --- failures around the steps ------------------------------------------------


def test_watermark_action_that_quotes_a_secret(spark, orders_df, provider):
    # Every step succeeds lazily; the extra action that computes the new
    # watermark is what evaluates the expression and fails.
    spark.sql("DROP TABLE IF EXISTS etl_state.watermark_leak")
    config = """
process_name: watermark_leak
inputs: [orders]
settings:
  watermark:
    enabled: true
    source: tagged
    column: tag
    initial: "0"
    table: etl_state.watermark_leak
steps:
  - name: tag_rows
    type: expression
    input: orders
    expressions:
      tag: "CASE WHEN order_id > 0 THEN raise_error('${secret:demo/folder}') ELSE 'x' END"
    output: tagged
"""
    with pytest.raises(ExecutionError) as excinfo:
        run(spark, config, provider, orders=orders_df)
    assert "failed to compute the new watermark" in str(excinfo.value)
    assert REDACTED in str(excinfo.value)
    assert_clean(excinfo.value)
    # The failed run did not advance the watermark.
    assert spark.table("etl_state.watermark_leak").count() == 0
    spark.sql("DROP TABLE etl_state.watermark_leak")


def test_interrupted_run_is_not_recorded_as_succeeded(spark, caplog, temporary_operation):
    class Interrupt(Operation):
        type_name = "interrupt"
        produces_output = False

        def execute(self, context, step):
            raise KeyboardInterrupt

    temporary_operation(Interrupt)
    config = "process_name: interrupted\nsteps:\n  - {name: stop, type: interrupt}\n"
    with caplog.at_level("INFO", logger="etl_framework"), pytest.raises(KeyboardInterrupt):
        run_pipeline(config_content=config, spark=spark)
    (finished,) = [
        json.loads(r.getMessage())
        for r in caplog.records
        if r.getMessage().startswith("{") and '"pipeline_finished"' in r.getMessage()
    ]
    assert finished["status"] == "failed"
    assert finished["error"] == "the run was interrupted"


# --- the secret store ---------------------------------------------------------


def test_retry_covers_a_secret_store_that_is_briefly_unavailable(spark, orders_df, tmp_path):
    orders_df.write.parquet(str(tmp_path / SECRET_VALUE))
    provider = Provider({("demo", "folder"): SECRET_VALUE}, failures_before_success=1)
    config = f"""
process_name: retry_secret
settings:
  log_row_counts: true
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{tmp_path}/${{secret:demo/folder}}"
    alias: orders
    retry: {{attempts: 2, delay_seconds: 0}}
"""
    result = run_pipeline(config_content=config, spark=spark, secret_provider=provider)
    assert result["steps"][0]["attempts"] == 2
    assert result["steps"][0]["rows_processed"] == 4


def test_provider_error_is_reported_with_the_step(spark, tmp_path):
    provider = Provider({}, failures_before_success=5, error=PermissionError("403 from vault"))
    config = f"""
process_name: provider_error
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{tmp_path}/${{secret:demo/folder}}"
    alias: orders
"""
    with pytest.raises(StepExecutionError) as excinfo:
        run_pipeline(config_content=config, spark=spark, secret_provider=provider)
    assert "step 'read_orders'" in str(excinfo.value)
    assert "PermissionError: 403 from vault" in str(excinfo.value)


# --- masking must not damage what it does not protect -------------------------


def test_run_summary_is_never_rewritten_by_masking(spark, orders_df, tmp_path):
    # The secret's value is an ordinary word that is also part of the pipeline,
    # step and DataFrame names. Those must reach the orchestrator intact.
    orders_df.write.parquet(str(tmp_path / "sales"))
    provider = Provider({("demo", "folder"): "sales"})
    config = f"""
process_name: sales_load
steps:
  - name: read_sales
    type: read
    format: parquet
    path: "{tmp_path}/${{secret:demo/folder}}"
    alias: sales
"""
    result = run_pipeline(config_content=config, spark=spark, secret_provider=provider)
    assert result["process_name"] == "sales_load"
    assert result["steps"][0]["step_name"] == "read_sales"
    assert result["steps"][0]["output"] == "sales"
    assert result["lineage"]["sources"][0]["dataset"].endswith("${secret:demo/folder}")


def test_values_too_short_to_be_secrets_are_not_masked():
    resolver = SecretResolver(Provider({("db", "user"): "sa", ("db", "password"): "hunter2!"}))
    assert resolver.resolve("${secret:db/user}:${secret:db/password}") == "sa:hunter2!"
    text = "login failed for user sa on sales_db with password hunter2!"
    # A two-letter value would otherwise turn `sales_db` into `***les_db`
    # and make unrelated errors look like leaks.
    assert resolver.redact(text) == f"login failed for user sa on sales_db with password {REDACTED}"
    assert not resolver.leaks("unrelated failure in sales_db")
    assert resolver.leaks(RuntimeError("bad password hunter2!"))
