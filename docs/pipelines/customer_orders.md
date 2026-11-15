# Pipeline: customer_orders_etl

Spec version: `1.0`

Source configuration: `configs/customer_orders.yaml`

## Parameters

| Name | Default |
| --- | --- |
| `business_date` | `(required at runtime)` |
| `environment` | `dev` |

## Settings

- `log_row_counts`: `true`

## Steps

| # | Step | Type | Inputs | Output | Details |
| --- | --- | --- | --- | --- | --- |
| 1 | `read_customers` | read |  | customers | `format: delta`<br>`path: abfss://datalake/silver/customers`<br>`schema_ref: silver/customers` |
| 2 | `read_orders` | read |  | orders | `format: delta`<br>`path: abfss://datalake/silver/orders`<br>`schema_ref: silver/orders` |
| 3 | `filter_orders` | filter | orders | filtered_orders | `condition: order_date = '${business_date}'` |
| 4 | `join_customers_orders` | join | filtered_orders, customers | customer_orders | `condition: ['filtered_orders.customer_id = customers.customer_id']`<br>`join_type: left` |
| 5 | `enrich_orders` | expression | customer_orders | enriched_orders |  |
| 6 | `select_columns` | select | enriched_orders | final_dataset |  |
| 7 | `check_quality` | quality | final_dataset | checked_dataset | `on_failure: fail` |
| 8 | `write_output` | write | checked_dataset |  | `format: delta`<br>`mode: overwrite`<br>`path: abfss://datalake/gold/customer_orders`<br>`schema_ref: gold/customer_orders` |

## Lineage

```mermaid
flowchart TD
    step0["read_customers<br><i>read</i>"]
    step0 --> customers((customers))
    step1["read_orders<br><i>read</i>"]
    step1 --> orders((orders))
    step2["filter_orders<br><i>filter</i>"]
    orders((orders)) --> step2
    step2 --> filtered_orders((filtered_orders))
    step3["join_customers_orders<br><i>join</i>"]
    filtered_orders((filtered_orders)) --> step3
    customers((customers)) --> step3
    step3 --> customer_orders((customer_orders))
    step4["enrich_orders<br><i>expression</i>"]
    customer_orders((customer_orders)) --> step4
    step4 --> enriched_orders((enriched_orders))
    step5["select_columns<br><i>select</i>"]
    enriched_orders((enriched_orders)) --> step5
    step5 --> final_dataset((final_dataset))
    step6["check_quality<br><i>quality</i>"]
    final_dataset((final_dataset)) --> step6
    step6 --> checked_dataset((checked_dataset))
    step7["write_output<br><i>write</i>"]
    checked_dataset((checked_dataset)) --> step7
    step7 --> sink7[("abfss://datalake/gold/customer_orders")]
```
