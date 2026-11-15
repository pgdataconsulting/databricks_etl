import pytest

from etl_framework.config_validator import validate_config
from etl_framework.exceptions import (
    SchemaContractError,
    SchemaEnforcementError,
    ValidationError,
)
from etl_framework.execution_context import ExecutionContext
from etl_framework.operations.read import ReadOperation
from etl_framework.operations.write import WriteOperation
from etl_framework.runner import run_pipeline
from etl_framework.schema_registry import SchemaRegistry, normalize_type

ORDERS_SCHEMA = """
dataset: silver.orders
columns:
  - {name: order_id, type: int, nullable: false}
  - {name: customer_id, type: int, nullable: true}
  - {name: order_date, type: string, nullable: true}
  - {name: order_amount, type: double, nullable: true}
"""


@pytest.fixture()
def schemas_dir(tmp_path):
    schemas = tmp_path / "schemas"
    (schemas / "silver").mkdir(parents=True)
    (schemas / "silver" / "orders.yaml").write_text(ORDERS_SCHEMA)
    return schemas


@pytest.fixture()
def registry(schemas_dir):
    return SchemaRegistry(str(schemas_dir))


@pytest.fixture()
def context(spark, registry, orders_df):
    ctx = ExecutionContext(
        spark,
        process_name="schema_pipeline",
        run_id="run-1",
        schema_registry=registry,
    )
    ctx.register_dataframe("orders", orders_df)
    return ctx


# --- registry / contract unit tests -----------------------------------------


def test_normalize_type_aliases():
    assert normalize_type("Integer") == "int"
    assert normalize_type("LONG") == "bigint"
    assert normalize_type("decimal(18, 2)") == "decimal(18,2)"


def test_registry_loads_and_caches(registry):
    contract = registry.get("silver/orders")
    assert contract.dataset == "silver.orders"
    assert [c["name"] for c in contract.columns] == [
        "order_id",
        "customer_id",
        "order_date",
        "order_amount",
    ]
    assert registry.get("silver/orders") is contract


def test_registry_missing_file(registry):
    with pytest.raises(SchemaContractError, match="not found"):
        registry.get("silver/no_such_dataset")


def test_registry_invalid_schema_file(schemas_dir):
    (schemas_dir / "bad.yaml").write_text("dataset: bad\ncolumns: []\n")
    with pytest.raises(SchemaContractError, match="non-empty 'columns'"):
        SchemaRegistry(str(schemas_dir)).get("bad")


def test_registry_invalid_column(schemas_dir):
    (schemas_dir / "bad_col.yaml").write_text("columns:\n  - {name: 'not a name!', type: int}\n")
    with pytest.raises(SchemaContractError, match="invalid 'name'"):
        SchemaRegistry(str(schemas_dir)).get("bad_col")


def test_contract_validates_matching_dataframe(registry, orders_df):
    assert registry.get("silver/orders").validate_dataframe(orders_df) == []


def test_contract_reports_missing_column(registry, orders_df):
    problems = registry.get("silver/orders").validate_dataframe(orders_df.drop("order_amount"))
    assert problems == ["missing column 'order_amount' (expected type double)"]


def test_contract_reports_type_mismatch(registry, orders_df):
    df = orders_df.selectExpr(
        "cast(order_id as string) as order_id",
        "customer_id",
        "order_date",
        "order_amount",
    )
    problems = registry.get("silver/orders").validate_dataframe(df)
    assert problems == ["column 'order_id' has type string, expected int"]


def test_contract_extra_columns_permissive_vs_strict(registry, orders_df):
    df = orders_df.selectExpr("*", "1 as surprise_column")
    contract = registry.get("silver/orders")
    assert contract.validate_dataframe(df) == []
    problems = contract.validate_dataframe(df, strict=True)
    assert problems == ["unexpected extra column(s) not in contract: surprise_column"]


def test_contract_nullability_opt_in(registry, orders_df):
    contract = registry.get("silver/orders")
    # order_id is declared nullable: false but Spark reports it nullable.
    assert contract.validate_dataframe(orders_df, enforce_nullability=False) == []
    problems = contract.validate_dataframe(orders_df, enforce_nullability=True)
    assert problems == ["column 'order_id' must not be nullable"]


# --- operation-level enforcement ---------------------------------------------


def test_read_csv_applies_contract_schema(context, tmp_path):
    path = tmp_path / "orders.csv"
    path.write_text("order_id,customer_id,order_date,order_amount\n1,10,2026-07-07,99.5\n")
    ReadOperation().execute(
        context,
        {
            "name": "r",
            "type": "read",
            "format": "csv",
            "path": str(path),
            "schema_ref": "silver/orders",
            "alias": "csv_orders",
            "options": {"header": "true"},
        },
    )
    df = context.get_dataframe("csv_orders")
    assert df.schema["order_id"].dataType.simpleString() == "int"
    assert df.schema["order_amount"].dataType.simpleString() == "double"


def test_read_parquet_enforces_contract(context, orders_df, tmp_path):
    bad = orders_df.selectExpr(
        "cast(order_id as string) as order_id",
        "customer_id",
        "order_date",
        "order_amount",
    )
    path = str(tmp_path / "bad_orders")
    bad.write.parquet(path)
    with pytest.raises(SchemaEnforcementError) as exc_info:
        ReadOperation().execute(
            context,
            {
                "name": "read_bad",
                "type": "read",
                "format": "parquet",
                "path": path,
                "schema_ref": "silver/orders",
                "alias": "bad_orders",
            },
        )
    message = str(exc_info.value)
    assert "schema_pipeline" in message
    assert "read_bad" in message
    assert "silver/orders" in message
    assert "order_id" in message


def test_write_enforces_contract_before_writing(context, tmp_path):
    import os

    target = str(tmp_path / "should_not_exist")
    context.register_dataframe(
        "incomplete_orders", context.get_dataframe("orders").drop("order_amount")
    )
    with pytest.raises(SchemaEnforcementError, match="order_amount"):
        WriteOperation().execute(
            context,
            {
                "name": "w",
                "type": "write",
                "input": "incomplete_orders",
                "format": "parquet",
                "mode": "overwrite",
                "path": target,
                "schema_ref": "silver/orders",
            },
        )
    assert not os.path.exists(target)


def test_missing_registry_fails(spark, orders_df):
    ctx = ExecutionContext(spark, process_name="p", run_id="r")
    ctx.register_dataframe("orders", orders_df)
    with pytest.raises(SchemaContractError, match="no schema base path"):
        WriteOperation().execute(
            ctx,
            {
                "name": "w",
                "type": "write",
                "input": "orders",
                "format": "parquet",
                "mode": "overwrite",
                "path": "/tmp/x",
                "schema_ref": "silver/orders",
            },
        )


# --- validation-time fail fast ------------------------------------------------


def config_with_ref(ref):
    return {
        "process_name": "demo",
        "steps": [
            {
                "name": "read_orders",
                "type": "read",
                "format": "parquet",
                "path": "/tmp/orders",
                "schema_ref": ref,
                "alias": "orders",
            },
        ],
    }


def test_validator_fails_on_missing_schema_file(registry):
    with pytest.raises(ValidationError, match="not found"):
        validate_config(config_with_ref("silver/nope"), schema_registry=registry)


def test_validator_fails_on_ref_without_base_path():
    with pytest.raises(ValidationError, match="no schema base path"):
        validate_config(config_with_ref("silver/orders"), schema_registry=None)


def test_validator_accepts_valid_ref(registry):
    validate_config(config_with_ref("silver/orders"), schema_registry=registry)


# --- end to end ----------------------------------------------------------------

PIPELINE = """
process_name: schema_e2e
parameters:
  business_date: null
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{orders_path}"
    schema_ref: silver/orders
    alias: orders
  - name: filter_orders
    type: filter
    input: orders
    condition: "order_date = '${{business_date}}'"
    output: filtered
  - name: write_orders
    type: write
    input: filtered
    format: parquet
    mode: overwrite
    schema_ref: silver/orders
    path: "{output_path}"
"""


def test_end_to_end_with_auto_detected_schemas(spark, orders_df, schemas_dir, tmp_path):
    # Conventional layout: configs/ and schemas/ side by side.
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir()
    orders_path = str(tmp_path / "orders_src")
    orders_df.write.parquet(orders_path)
    config_file = configs_dir / "pipeline.yaml"
    config_file.write_text(
        PIPELINE.format(
            orders_path=orders_path,
            output_path=str(tmp_path / "out"),
        )
    )

    result = run_pipeline(
        config_path=str(config_file),
        parameters={"business_date": "2026-07-07"},
        spark=spark,
    )
    assert result["status"] == "succeeded"
    assert spark.read.parquet(str(tmp_path / "out")).count() == 3


def test_end_to_end_fails_fast_on_missing_schema(spark, orders_df, schemas_dir, tmp_path):
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir()
    orders_path = str(tmp_path / "orders_src")
    orders_df.write.parquet(orders_path)
    config_file = configs_dir / "pipeline.yaml"
    config_file.write_text(
        PIPELINE.format(orders_path=orders_path, output_path=str(tmp_path / "out")).replace(
            "schema_ref: silver/orders", "schema_ref: silver/missing", 1
        )
    )

    with pytest.raises(ValidationError, match="silver/missing"):
        run_pipeline(
            config_path=str(config_file),
            parameters={"business_date": "2026-07-07"},
            spark=spark,
        )
    import os

    assert not os.path.exists(str(tmp_path / "out"))
