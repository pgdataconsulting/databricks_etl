# Pipeline: customer_order_summary

Spec version: `1.0`

Source configuration: `configs/customer_order_summary.yaml`

## Parameters

| Name | Default |
| --- | --- |
| `business_date` | `(required at runtime)` |

## Steps

| # | Step | Type | Inputs | Output | Details |
| --- | --- | --- | --- | --- | --- |
| 1 | `read_orders` | read |  | orders | `format: parquet`<br>`path: abfss://datalake/silver/orders` |
| 2 | `deduplicate_orders` | deduplicate | orders | unique_orders |  |
| 3 | `add_audit_columns` | add_columns | unique_orders | orders_enriched |  |
| 4 | `summarize_orders` | aggregate | orders_enriched | customer_summary |  |
| 5 | `write_summary` | write | customer_summary |  | `format: table`<br>`mode: overwrite`<br>`table: gold.customer_order_summary` |

## Lineage

```mermaid
flowchart TD
    step0["read_orders<br><i>read</i>"]
    step0 --> orders((orders))
    step1["deduplicate_orders<br><i>deduplicate</i>"]
    orders((orders)) --> step1
    step1 --> unique_orders((unique_orders))
    step2["add_audit_columns<br><i>add_columns</i>"]
    unique_orders((unique_orders)) --> step2
    step2 --> orders_enriched((orders_enriched))
    step3["summarize_orders<br><i>aggregate</i>"]
    orders_enriched((orders_enriched)) --> step3
    step3 --> customer_summary((customer_summary))
    step4["write_summary<br><i>write</i>"]
    customer_summary((customer_summary)) --> step4
    step4 --> sink4[("gold.customer_order_summary")]
```
