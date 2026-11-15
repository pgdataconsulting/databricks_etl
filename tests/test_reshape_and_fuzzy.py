"""Tests for the union, pivot, unpivot, and fuzzy_match operations."""

import pytest

from etl_framework.config_validator import validate_config
from etl_framework.exceptions import ValidationError
from etl_framework.execution_context import ExecutionContext
from etl_framework.operations.fuzzy_match import FuzzyMatchOperation
from etl_framework.operations.pivot import PivotOperation
from etl_framework.operations.union import UnionOperation
from etl_framework.operations.unpivot import UnpivotOperation


@pytest.fixture()
def context(spark, orders_df):
    ctx = ExecutionContext(spark, process_name="test_pipeline", run_id="test-run")
    ctx.register_dataframe("orders", orders_df)
    return ctx


def test_union_by_name(spark, context):
    context.register_dataframe(
        "batch_a",
        spark.createDataFrame([(1, "x")], "id int, label string"),
    )
    context.register_dataframe(
        "batch_b",
        # Same columns, different order: union must match by name.
        spark.createDataFrame([("y", 2), ("x", 1)], "label string, id int"),
    )
    UnionOperation().execute(
        context,
        {
            "name": "u",
            "type": "union",
            "inputs": ["batch_a", "batch_b"],
            "distinct": True,
            "output": "combined",
        },
    )
    rows = context.get_dataframe("combined").collect()
    assert {(r.id, r.label) for r in rows} == {(1, "x"), (2, "y")}


def test_union_allow_missing_columns(spark, context):
    context.register_dataframe(
        "with_extra",
        spark.createDataFrame([(1, "x")], "id int, extra string"),
    )
    context.register_dataframe(
        "without_extra",
        spark.createDataFrame([(2,)], "id int"),
    )
    UnionOperation().execute(
        context,
        {
            "name": "u",
            "type": "union",
            "inputs": ["with_extra", "without_extra"],
            "allow_missing_columns": True,
            "output": "combined",
        },
    )
    rows = {(r.id, r.extra) for r in context.get_dataframe("combined").collect()}
    assert rows == {(1, "x"), (2, None)}


def test_union_requires_two_inputs():
    config = {
        "process_name": "p",
        "steps": [
            {"name": "u", "type": "union", "inputs": ["only_one"], "output": "out"},
        ],
    }
    with pytest.raises(ValidationError, match="at least two"):
        validate_config(config)


def test_pivot_with_pinned_values(context):
    PivotOperation().execute(
        context,
        {
            "name": "p",
            "type": "pivot",
            "input": "orders",
            "group_by": ["customer_id"],
            "pivot_column": "order_date",
            "values": ["2026-07-06", "2026-07-07"],
            "aggregations": {"total": "sum(order_amount)"},
            "output": "pivoted",
        },
    )
    df = context.get_dataframe("pivoted")
    assert set(df.columns) == {"customer_id", "2026-07-06", "2026-07-07"}
    row = {r.customer_id: r for r in df.collect()}[1]
    assert row["2026-07-06"] == 75.0
    assert row["2026-07-07"] == 50.0


def test_pivot_multiple_aggregations(context):
    PivotOperation().execute(
        context,
        {
            "name": "p",
            "type": "pivot",
            "input": "orders",
            "group_by": ["customer_id"],
            "pivot_column": "order_date",
            "values": ["2026-07-07"],
            "aggregations": {"total": "sum(order_amount)", "orders": "count(*)"},
            "output": "pivoted",
        },
    )
    df = context.get_dataframe("pivoted")
    assert set(df.columns) == {"customer_id", "2026-07-07_total", "2026-07-07_orders"}


def test_unpivot_wide_to_long(spark, context):
    context.register_dataframe(
        "monthly",
        spark.createDataFrame(
            [(1, 10.0, 20.0)],
            "customer_id int, jan double, feb double",
        ),
    )
    UnpivotOperation().execute(
        context,
        {
            "name": "u",
            "type": "unpivot",
            "input": "monthly",
            "id_columns": ["customer_id"],
            "key_column": "month",
            "value_column": "amount",
            "output": "long",
        },
    )
    rows = context.get_dataframe("long").collect()
    assert {(r.customer_id, r.month, r.amount) for r in rows} == {
        (1, "jan", 10.0),
        (1, "feb", 20.0),
    }
    # value_columns defaulted to every non-id column.
    assert set(context.get_dataframe("long").columns) == {
        "customer_id",
        "month",
        "amount",
    }


@pytest.fixture()
def fuzzy_context(spark):
    ctx = ExecutionContext(spark, process_name="test_pipeline", run_id="test-run")
    ctx.register_dataframe(
        "crm",
        spark.createDataFrame(
            [
                (1, "Acme Corporation", "US"),
                (2, "Globex", "US"),
                (3, "Initech", "DE"),
            ],
            "crm_id int, company string, country string",
        ),
    )
    ctx.register_dataframe(
        "billing",
        spark.createDataFrame(
            [
                (10, "ACME Corporation ", "US"),  # case/whitespace noise
                (11, "Acme Corp", "US"),  # weaker variant of the same name
                (12, "Globex Inc", "US"),
                (13, "Umbrella", "US"),
            ],
            "billing_id int, account_name string, country string",
        ),
    )
    return ctx


def test_fuzzy_match_keeps_best_match_per_left_row(fuzzy_context):
    FuzzyMatchOperation().execute(
        fuzzy_context,
        {
            "name": "f",
            "type": "fuzzy_match",
            "left": "crm",
            "right": "billing",
            "left_on": "company",
            "right_on": "account_name",
            "threshold": 0.6,
            "block_on": ["crm.country = billing.country"],
            "output": "matched",
        },
    )
    rows = {r.crm_id: r for r in fuzzy_context.get_dataframe("matched").collect()}
    # Acme matches two candidates above the threshold; only the best is kept.
    assert rows[1].billing_id == 10
    assert rows[1]._match_score == 1.0  # identical after lower/trim normalization
    assert rows[2].billing_id == 12
    # Initech has no candidate in its country block: dropped by the inner join.
    assert 3 not in rows
    # Blocking + keep_left column policy: one country column, the left one.
    assert list(rows[1].asDict()).count("country") == 1


def test_fuzzy_match_left_join_and_keep_all(fuzzy_context):
    FuzzyMatchOperation().execute(
        fuzzy_context,
        {
            "name": "f",
            "type": "fuzzy_match",
            "left": "crm",
            "right": "billing",
            "left_on": "company",
            "right_on": "account_name",
            # "Acme Corp" vs "Acme Corporation" scores 1 - 7/16 = 0.5625
            "threshold": 0.5,
            "block_on": ["crm.country = billing.country"],
            "keep": "all",
            "join_type": "left",
            "output": "matched",
        },
    )
    rows = fuzzy_context.get_dataframe("matched").collect()
    acme_matches = {r.billing_id for r in rows if r.crm_id == 1}
    assert acme_matches == {10, 11}
    (unmatched,) = [r for r in rows if r.crm_id == 3]
    assert unmatched.billing_id is None
    assert unmatched._match_score is None


def test_fuzzy_match_soundex(fuzzy_context):
    FuzzyMatchOperation().execute(
        fuzzy_context,
        {
            "name": "f",
            "type": "fuzzy_match",
            "left": "crm",
            "right": "billing",
            "left_on": "company",
            "right_on": "account_name",
            "method": "soundex",
            "block_on": ["crm.country = billing.country"],
            "output": "matched",
        },
    )
    rows = {r.crm_id: r for r in fuzzy_context.get_dataframe("matched").collect()}
    assert rows[1].billing_id == 10  # ACME Corporation sounds like Acme Corporation


def test_fuzzy_match_requires_blocking_or_opt_out():
    step = {
        "name": "f",
        "type": "fuzzy_match",
        "left": "a",
        "right": "b",
        "left_on": "x",
        "right_on": "y",
        "output": "out",
    }
    config = {
        "process_name": "p",
        "steps": [
            {"name": "ra", "type": "read", "format": "delta", "path": "/a", "alias": "a"},
            {"name": "rb", "type": "read", "format": "delta", "path": "/b", "alias": "b"},
            step,
        ],
    }
    with pytest.raises(ValidationError, match="allow_unblocked"):
        validate_config(config)
    step["allow_unblocked"] = True
    validate_config(config)  # must not raise


def test_fuzzy_match_validates_options():
    errors = FuzzyMatchOperation.validate(
        {
            "name": "f",
            "left": "a",
            "right": "b",
            "left_on": "x",
            "right_on": "y",
            "output": "out",
            "method": "jaro",
            "threshold": 2,
            "keep": "worst",
            "join_type": "right",
            "duplicate_columns": "drop_all",
            "block_on": "not a list",
        }
    )
    message = " | ".join(errors)
    assert "unsupported method 'jaro'" in message
    assert "'threshold' must be a number between 0 and 1" in message
    assert "invalid keep value" in message
    assert "invalid join_type" in message
    assert "invalid duplicate_columns" in message
    assert "'block_on' must be a list" in message
