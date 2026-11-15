# Pipeline: medallion_orders_silver

Spec version: `1.0`

Source configuration: `configs/medallion/orders_silver.yaml`

## Parameters

| Name | Default |
| --- | --- |
| `catalog` | `spark_catalog` |
| `environment` | `dev` |

## Settings

- `log_row_counts`: `true`
- `audit_columns`: `["_run_id", "_load_timestamp"]`
- `audit`: `{"enabled": true, "schema": "${catalog}.etl_audit"}`
- `watermark`: `{"enabled": true, "source": "new_orders", "column": "updated_at", "initial": "1900-01-01 00:00:00", "table": "${catalog}.etl_state.watermarks"}`

## Steps

| # | Step | Type | Inputs | Output | Details |
| --- | --- | --- | --- | --- | --- |
| 1 | `read_bronze_orders` | read |  | orders_raw | `format: table`<br>`table: ${catalog}.bronze.orders_raw` |
| 2 | `only_changed_orders` | filter | orders_raw | new_orders | `condition: updated_at > '${watermark}'` |
| 3 | `latest_version_per_order` | deduplicate | new_orders | latest_orders |  |
| 4 | `check_orders` | quality | latest_orders | clean_orders | `on_failure: quarantine`<br>`quarantine_table: ${catalog}.quarantine.orders` |
| 5 | `upsert_silver_orders` | merge | clean_orders |  | `table: ${catalog}.silver.orders`<br>`schema_ref: medallion/silver_orders` |

## Lineage

```mermaid
flowchart TD
    step0["read_bronze_orders<br><i>read</i>"]
    step0 --> orders_raw((orders_raw))
    step1["only_changed_orders<br><i>filter</i>"]
    orders_raw((orders_raw)) --> step1
    step1 --> new_orders((new_orders))
    step2["latest_version_per_order<br><i>deduplicate</i>"]
    new_orders((new_orders)) --> step2
    step2 --> latest_orders((latest_orders))
    step3["check_orders<br><i>quality</i>"]
    latest_orders((latest_orders)) --> step3
    step3 --> clean_orders((clean_orders))
    step4["upsert_silver_orders<br><i>merge</i>"]
    clean_orders((clean_orders)) --> step4
    step4 --> sink4[("${catalog}.silver.orders")]
```
