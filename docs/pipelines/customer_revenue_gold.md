# Pipeline: medallion_customer_revenue_gold

Spec version: `1.0`

Source configuration: `configs/medallion/customer_revenue_gold.yaml`

## Parameters

| Name | Default |
| --- | --- |
| `catalog` | `spark_catalog` |
| `environment` | `dev` |

## Settings

- `log_row_counts`: `true`
- `audit`: `{"enabled": true, "schema": "${catalog}.etl_audit"}`

## Steps

| # | Step | Type | Inputs | Output | Details |
| --- | --- | --- | --- | --- | --- |
| 1 | `read_silver_orders` | read |  | orders | `format: table`<br>`table: ${catalog}.silver.orders`<br>`schema_ref: medallion/silver_orders` |
| 2 | `read_customers` | read |  | customers | `format: table`<br>`table: ${catalog}.bronze.customers_raw` |
| 3 | `drop_cancelled_orders` | filter | orders | active_orders | `condition: status <> 'CANCELLED'` |
| 4 | `add_customer_attributes` | lookup | active_orders, customers | orders_enriched |  |
| 5 | `revenue_per_customer_and_day` | aggregate | orders_enriched | daily_revenue |  |
| 6 | `check_daily_revenue` | quality | daily_revenue | checked_daily_revenue | `on_failure: fail` |
| 7 | `publish_gold` | write | checked_daily_revenue |  | `format: table`<br>`mode: overwrite`<br>`table: ${catalog}.gold.customer_daily_revenue`<br>`schema_ref: medallion/gold_customer_daily_revenue` |

## Lineage

```mermaid
flowchart TD
    step0["read_silver_orders<br><i>read</i>"]
    step0 --> orders((orders))
    step1["read_customers<br><i>read</i>"]
    step1 --> customers((customers))
    step2["drop_cancelled_orders<br><i>filter</i>"]
    orders((orders)) --> step2
    step2 --> active_orders((active_orders))
    step3["add_customer_attributes<br><i>lookup</i>"]
    active_orders((active_orders)) --> step3
    customers((customers)) --> step3
    step3 --> orders_enriched((orders_enriched))
    step4["revenue_per_customer_and_day<br><i>aggregate</i>"]
    orders_enriched((orders_enriched)) --> step4
    step4 --> daily_revenue((daily_revenue))
    step5["check_daily_revenue<br><i>quality</i>"]
    daily_revenue((daily_revenue)) --> step5
    step5 --> checked_daily_revenue((checked_daily_revenue))
    step6["publish_gold<br><i>write</i>"]
    checked_daily_revenue((checked_daily_revenue)) --> step6
    step6 --> sink6[("${catalog}.gold.customer_daily_revenue")]
```
