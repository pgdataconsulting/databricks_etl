import pytest

from etl_framework.exceptions import ExpressionExecutionError, MissingInputError
from etl_framework.execution_context import ExecutionContext
from etl_framework.operations import OPERATION_REGISTRY, register_operation
from etl_framework.operations.aggregate import AggregateOperation
from etl_framework.operations.base import Operation
from etl_framework.operations.deduplicate import DeduplicateOperation
from etl_framework.operations.expression import AddColumnsOperation, ExpressionOperation
from etl_framework.operations.filter import FilterOperation
from etl_framework.operations.join import JoinOperation
from etl_framework.operations.read import ReadOperation
from etl_framework.operations.select import SelectOperation
from etl_framework.operations.sql import SqlOperation
from etl_framework.operations.write import WriteOperation


@pytest.fixture()
def context(spark, customers_df, orders_df):
    ctx = ExecutionContext(spark, process_name="test_pipeline", run_id="test-run")
    ctx.register_dataframe("customers", customers_df)
    ctx.register_dataframe("orders", orders_df)
    return ctx


def test_missing_input_raises(context):
    with pytest.raises(MissingInputError, match="nope"):
        FilterOperation().execute(
            context,
            {"name": "f", "type": "filter", "input": "nope", "condition": "1 = 1", "output": "out"},
        )


def test_read_and_write_roundtrip(spark, context, orders_df, tmp_path):
    target = str(tmp_path / "orders_parquet")
    WriteOperation().execute(
        context,
        {
            "name": "w",
            "type": "write",
            "input": "orders",
            "format": "parquet",
            "mode": "overwrite",
            "path": target,
            "partition_by": ["order_date"],
        },
    )
    ReadOperation().execute(
        context,
        {"name": "r", "type": "read", "format": "parquet", "path": target, "alias": "orders_back"},
    )
    assert context.get_dataframe("orders_back").count() == orders_df.count()


def test_read_csv_with_schema_and_options(spark, context, tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("id,name\n1,Alice\n2,Bob\n")
    ReadOperation().execute(
        context,
        {
            "name": "r",
            "type": "read",
            "format": "csv",
            "path": str(path),
            "alias": "csv_data",
            "schema": "id int, name string",
            "options": {"header": "true"},
        },
    )
    rows = context.get_dataframe("csv_data").collect()
    assert {(r.id, r.name) for r in rows} == {(1, "Alice"), (2, "Bob")}


def test_select_with_rename(context):
    SelectOperation().execute(
        context,
        {
            "name": "s",
            "type": "select",
            "input": "customers",
            "columns": ["customer_id", "customer_name as name"],
            "output": "selected",
        },
    )
    df = context.get_dataframe("selected")
    assert df.columns == ["customer_id", "name"]


def test_filter(context):
    FilterOperation().execute(
        context,
        {
            "name": "f",
            "type": "filter",
            "input": "orders",
            "condition": "order_date = '2026-07-07'",
            "output": "filtered",
        },
    )
    assert context.get_dataframe("filtered").count() == 3


def test_join_left(context):
    JoinOperation().execute(
        context,
        {
            "name": "j",
            "type": "join",
            "left": "orders",
            "right": "customers",
            "join_type": "left",
            "condition": ["orders.customer_id = customers.customer_id"],
            "output": "joined",
        },
    )
    df = context.get_dataframe("joined")
    assert df.count() == 4
    unmatched = df.filter("customer_name is null").count()
    assert unmatched == 1  # order 103 references unknown customer 9


def test_join_anti(context):
    JoinOperation().execute(
        context,
        {
            "name": "j",
            "type": "join",
            "left": "orders",
            "right": "customers",
            "join_type": "anti",
            "condition": ["orders.customer_id = customers.customer_id"],
            "output": "orphans",
        },
    )
    rows = context.get_dataframe("orphans").collect()
    assert [r.order_id for r in rows] == [103]


def test_cross_join(context):
    JoinOperation().execute(
        context,
        {
            "name": "j",
            "type": "join",
            "left": "orders",
            "right": "customers",
            "join_type": "cross",
            "allow_cross": True,
            "output": "crossed",
        },
    )
    assert context.get_dataframe("crossed").count() == 12


def test_aggregate(context):
    AggregateOperation().execute(
        context,
        {
            "name": "a",
            "type": "aggregate",
            "input": "orders",
            "group_by": ["customer_id"],
            "aggregations": {"total_amount": "sum(order_amount)", "order_count": "count(*)"},
            "output": "summary",
        },
    )
    rows = {
        r.customer_id: (r.total_amount, r.order_count)
        for r in context.get_dataframe("summary").collect()
    }
    assert rows[1] == (125.0, 2)
    assert rows[2] == (20.0, 1)


def test_aggregate_without_group_by(context):
    AggregateOperation().execute(
        context,
        {
            "name": "a",
            "type": "aggregate",
            "input": "orders",
            "aggregations": {"order_count": "count(*)"},
            "output": "totals",
        },
    )
    assert context.get_dataframe("totals").collect()[0].order_count == 4


def test_expression_creates_and_overwrites_columns(context):
    ExpressionOperation().execute(
        context,
        {
            "name": "e",
            "type": "expression",
            "input": "customers",
            "expressions": {
                "email": "lower(email)",
                "name_length": "length(customer_name)",
                "is_long_name": "case when length(customer_name) > 4 then true else false end",
                "customer_id": "cast(customer_id as string)",
            },
            "output": "enriched",
        },
    )
    df = context.get_dataframe("enriched")
    row = df.filter("customer_name = 'Alice'").collect()[0]
    assert row.name_length == 5
    assert row.is_long_name is True
    assert df.schema["customer_id"].dataType.simpleString() == "string"


def test_expression_error_includes_context(context):
    with pytest.raises(ExpressionExecutionError) as exc_info:
        ExpressionOperation().execute(
            context,
            {
                "name": "bad_step",
                "type": "expression",
                "input": "customers",
                "expressions": {"broken": "not_a_function(("},
                "output": "x",
            },
        )
    message = str(exc_info.value)
    assert "test_pipeline" in message
    assert "bad_step" in message
    assert "customers" in message
    assert "broken" in message
    assert "not_a_function((" in message


def test_add_columns(context):
    AddColumnsOperation().execute(
        context,
        {
            "name": "ac",
            "type": "add_columns",
            "input": "orders",
            "columns": {"business_date": "'2026-07-07'", "net_amount": "order_amount * 0.9"},
            "output": "orders_enriched",
        },
    )
    row = context.get_dataframe("orders_enriched").collect()[0]
    assert row.business_date == "2026-07-07"


def test_deduplicate_with_order_by(spark, context):
    df = spark.createDataFrame(
        [(1, "old", "2026-01-01"), (1, "new", "2026-06-01"), (2, "only", "2026-03-01")],
        "customer_id int, status string, updated_at string",
    )
    context.register_dataframe("source_df", df)
    DeduplicateOperation().execute(
        context,
        {
            "name": "d",
            "type": "deduplicate",
            "input": "source_df",
            "partition_by": ["customer_id"],
            "order_by": ["updated_at desc"],
            "output": "deduplicated",
        },
    )
    rows = {r.customer_id: r.status for r in context.get_dataframe("deduplicated").collect()}
    assert rows == {1: "new", 2: "only"}
    assert "__etl_framework_row_number" not in context.get_dataframe("deduplicated").columns


def test_deduplicate_without_order_by(spark, context):
    df = spark.createDataFrame(
        [(1, "a"), (1, "a"), (2, "b")],
        "customer_id int, status string",
    )
    context.register_dataframe("dupes", df)
    DeduplicateOperation().execute(
        context,
        {
            "name": "d",
            "type": "deduplicate",
            "input": "dupes",
            "partition_by": ["customer_id"],
            "output": "unique_rows",
        },
    )
    assert context.get_dataframe("unique_rows").count() == 2


def test_sql_operation(context):
    SqlOperation().execute(
        context,
        {
            "name": "q",
            "type": "sql",
            "query": "SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id",
            "output": "counts",
        },
    )
    rows = {r.customer_id: r.order_count for r in context.get_dataframe("counts").collect()}
    assert rows[1] == 2


def test_registry_is_extensible():
    class NoopOperation(Operation):
        type_name = "noop"

        def execute(self, context, step):
            return None

    register_operation(NoopOperation)
    try:
        assert OPERATION_REGISTRY["noop"] is NoopOperation
    finally:
        del OPERATION_REGISTRY["noop"]
