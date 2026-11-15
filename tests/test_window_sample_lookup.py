"""Tests for the window, sample, and lookup operations."""

import pytest

from etl_framework.config_validator import validate_config
from etl_framework.exceptions import ExpressionExecutionError, ValidationError
from etl_framework.execution_context import ExecutionContext
from etl_framework.operations.lookup import LookupOperation
from etl_framework.operations.sample import SampleOperation
from etl_framework.operations.window import WindowOperation


@pytest.fixture()
def context(spark, customers_df, orders_df):
    ctx = ExecutionContext(spark, process_name="test_pipeline", run_id="test-run")
    ctx.register_dataframe("customers", customers_df)
    ctx.register_dataframe("orders", orders_df)
    return ctx


def test_window_running_total_and_rank(context):
    WindowOperation().execute(
        context,
        {
            "name": "w",
            "type": "window",
            "input": "orders",
            "partition_by": ["customer_id"],
            "order_by": ["order_date"],
            "columns": {
                "running_total": "sum(order_amount)",
                "order_seq": "row_number()",
                "prev_amount": "lag(order_amount, 1)",
            },
            "output": "windowed",
        },
    )
    rows = {r.order_id: r for r in context.get_dataframe("windowed").collect()}
    # Customer 1: order 101 on 07-06 (75.0), then order 100 on 07-07 (50.0).
    assert rows[101].order_seq == 1 and rows[101].running_total == 75.0
    assert rows[100].order_seq == 2 and rows[100].running_total == 125.0
    assert rows[100].prev_amount == 75.0
    assert rows[101].prev_amount is None


def test_window_grand_total_without_partition(context):
    WindowOperation().execute(
        context,
        {
            "name": "w",
            "type": "window",
            "input": "orders",
            "columns": {"pct_of_total": "order_amount / sum(order_amount)"},
            "output": "windowed",
        },
    )
    rows = context.get_dataframe("windowed").collect()
    assert sum(r.pct_of_total for r in rows) == pytest.approx(1.0)


def test_window_bad_expression_is_wrapped(context):
    with pytest.raises(ExpressionExecutionError, match="no_such_column"):
        WindowOperation().execute(
            context,
            {
                "name": "w",
                "type": "window",
                "input": "orders",
                "partition_by": ["customer_id"],
                "columns": {"x": "sum(no_such_column)"},
                "output": "windowed",
            },
        ).collect()


def test_sample_rows(context):
    SampleOperation().execute(
        context,
        {"name": "s", "type": "sample", "input": "orders", "rows": 2, "output": "sampled"},
    )
    assert context.get_dataframe("sampled").count() == 2


def test_sample_fraction_with_seed_is_deterministic(context, orders_df):
    step = {
        "name": "s",
        "type": "sample",
        "input": "orders",
        "fraction": 0.5,
        "seed": 42,
        "output": "sampled",
    }
    first = SampleOperation().execute(context, step).collect()
    step = dict(step, output="sampled_again")
    second = SampleOperation().execute(context, step).collect()
    assert first == second
    assert len(first) <= orders_df.count()


def test_sample_requires_exactly_one_mode():
    both = SampleOperation.validate(
        {"name": "s", "input": "a", "output": "b", "fraction": 0.5, "rows": 3}
    )
    neither = SampleOperation.validate({"name": "s", "input": "a", "output": "b"})
    bad = SampleOperation.validate({"name": "s", "input": "a", "output": "b", "fraction": 1.5})
    assert any("exactly one" in e for e in both)
    assert any("exactly one" in e for e in neither)
    assert any("'fraction' must be" in e for e in bad)


def test_lookup_with_defaults(context):
    LookupOperation().execute(
        context,
        {
            "name": "l",
            "type": "lookup",
            "input": "orders",
            "lookup": "customers",
            "on": {"customer_id": "customer_id"},
            "select": ["customer_name as name"],
            "defaults": {"name": "UNKNOWN"},
            "output": "enriched",
        },
    )
    df = context.get_dataframe("enriched")
    assert set(df.columns) == {
        "order_id",
        "customer_id",
        "order_date",
        "order_amount",
        "name",
    }
    rows = {r.order_id: r.name for r in df.collect()}
    assert rows[100] == "Alice"
    assert rows[103] == "UNKNOWN"  # customer 9 is not in the dimension


def test_lookup_defaults_to_all_non_key_columns(context):
    LookupOperation().execute(
        context,
        {
            "name": "l",
            "type": "lookup",
            "input": "orders",
            "lookup": "customers",
            "on": {"customer_id": "customer_id"},
            "output": "enriched",
        },
    )
    df = context.get_dataframe("enriched")
    assert {"customer_name", "email"} <= set(df.columns)
    assert df.count() == 4  # left join: unmatched orders survive


def test_lookup_validates_mappings():
    errors = LookupOperation.validate(
        {
            "name": "l",
            "input": "a",
            "lookup": "d",
            "output": "out",
            "on": ["not", "a", "mapping"],
            "select": "not a list",
            "defaults": "not a mapping",
        }
    )
    message = " | ".join(errors)
    assert "'on' must be a mapping" in message
    assert "'select' must be a list" in message
    assert "'defaults' must be a mapping" in message


def test_lookup_and_window_validate_in_config():
    config = {
        "process_name": "p",
        "steps": [
            {"name": "r1", "type": "read", "format": "delta", "path": "/a", "alias": "facts"},
            {"name": "r2", "type": "read", "format": "delta", "path": "/d", "alias": "dim"},
            {
                "name": "l",
                "type": "lookup",
                "input": "facts",
                "lookup": "dim",
                "on": {"k": "k"},
                "output": "enriched",
            },
            {
                "name": "w",
                "type": "window",
                "input": "enriched",
                "partition_by": ["k"],
                "columns": "not a mapping",
                "output": "final",
            },
        ],
    }
    with pytest.raises(ValidationError, match="'columns' must be a mapping"):
        validate_config(config)
