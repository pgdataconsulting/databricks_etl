"""Tests for incremental watermark tracking."""

import pytest

from etl_framework.exceptions import ValidationError
from etl_framework.runner import run_pipeline

CONFIG_TEMPLATE = """
process_name: incremental_orders
settings:
  log_row_counts: true
  watermark:
    enabled: true
    source: new_orders
    column: order_date
    initial: "1900-01-01"
    table: etl_state.orders_watermark
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source_path}"
    alias: orders
  - name: incremental_filter
    type: filter
    input: orders
    condition: "order_date > '${{watermark}}'"
    output: new_orders
  - name: write_output
    type: write
    input: new_orders
    format: parquet
    mode: overwrite
    path: "{target_path}"
"""


def rows_processed(result, step_name):
    (entry,) = [s for s in result["steps"] if s["step_name"] == step_name]
    return entry["rows_processed"]


def test_watermark_incremental_runs(spark, orders_df, tmp_path):
    source_path = str(tmp_path / "orders")
    orders_df.write.mode("overwrite").parquet(source_path)
    config = CONFIG_TEMPLATE.format(
        source_path=source_path,
        target_path=str(tmp_path / "gold"),
    )
    spark.sql("DROP TABLE IF EXISTS etl_state.orders_watermark")

    # First run: no stored watermark, everything after `initial` is new.
    first = run_pipeline(config_content=config, spark=spark)
    assert rows_processed(first, "incremental_filter") == 4
    assert first["watermark"] == {"previous": None, "new": "2026-07-07"}
    stored = spark.table("etl_state.orders_watermark").collect()
    assert [(r.process_name, r.watermark) for r in stored] == [
        ("incremental_orders", "2026-07-07"),
    ]

    # Second run with one late-arriving order: only the new row is processed.
    spark.createDataFrame(
        [(104, 2, "2026-07-08", 42.0)],
        orders_df.schema,
    ).write.mode("append").parquet(source_path)
    second = run_pipeline(config_content=config, spark=spark)
    assert rows_processed(second, "incremental_filter") == 1
    assert second["watermark"] == {"previous": "2026-07-07", "new": "2026-07-08"}

    # Third run with no new data: empty increment, watermark unchanged.
    third = run_pipeline(config_content=config, spark=spark)
    assert rows_processed(third, "incremental_filter") == 0
    assert third["watermark"] == {"previous": "2026-07-08", "new": "2026-07-08"}
    stored = spark.table("etl_state.orders_watermark").collect()
    assert [r.watermark for r in stored] == ["2026-07-08"]


def test_watermark_dry_run_uses_initial_and_no_state(orders_df, tmp_path):
    config = CONFIG_TEMPLATE.format(
        source_path=str(tmp_path / "orders"),
        target_path=str(tmp_path / "gold"),
    ).replace("etl_state.orders_watermark", "etl_state.never_created")
    result = run_pipeline(config_content=config, dry_run=True)
    # Dry runs resolve ${watermark} from `initial` without any Spark session,
    # so the state table is never touched.
    assert result["parameters"]["watermark"] == "1900-01-01"


def test_watermark_settings_are_validated(spark):
    config = """
process_name: broken_watermark
settings:
  watermark:
    enabled: true
steps:
  - name: read_orders
    type: read
    format: parquet
    path: /tmp/x
    alias: orders
"""
    with pytest.raises(ValidationError) as exc_info:
        run_pipeline(config_content=config, spark=spark)
    message = str(exc_info.value)
    for field in ("source", "column", "initial"):
        assert f"settings.watermark.{field} is required" in message


def test_watermark_source_must_be_a_step_output():
    from etl_framework.config_validator import validate_config

    config = {
        "process_name": "p",
        "settings": {
            "watermark": {
                "enabled": True,
                "source": "no_such_df",
                "column": "updated_at",
                "initial": "0",
            }
        },
        "steps": [
            {"name": "r", "type": "read", "format": "parquet", "path": "/tmp/a", "alias": "orders"},
        ],
    }
    with pytest.raises(ValidationError, match="no_such_df"):
        validate_config(config)
