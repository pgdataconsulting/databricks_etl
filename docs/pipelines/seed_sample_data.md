# Pipeline: medallion_seed_sample_data

Spec version: `1.0`

Source configuration: `configs/medallion/seed_sample_data.yaml`

## Parameters

| Name | Default |
| --- | --- |
| `catalog` | `spark_catalog` |
| `business_date` | `(required at runtime)` |

## Settings

- `allow_sql`: `true`
- `log_row_counts`: `true`

## Steps

| # | Step | Type | Inputs | Output | Details |
| --- | --- | --- | --- | --- | --- |
| 1 | `generate_orders` | sql |  | orders_raw |  |
| 2 | `write_orders` | write | orders_raw |  | `format: table`<br>`mode: overwrite`<br>`table: ${catalog}.bronze.orders_raw` |
| 3 | `generate_customers` | sql |  | customers_raw |  |
| 4 | `write_customers` | write | customers_raw |  | `format: table`<br>`mode: overwrite`<br>`table: ${catalog}.bronze.customers_raw` |

## Lineage

```mermaid
flowchart TD
    step0["generate_orders<br><i>sql</i>"]
    step0 --> orders_raw((orders_raw))
    step1["write_orders<br><i>write</i>"]
    orders_raw((orders_raw)) --> step1
    step1 --> sink1[("${catalog}.bronze.orders_raw")]
    step2["generate_customers<br><i>sql</i>"]
    step2 --> customers_raw((customers_raw))
    step3["write_customers<br><i>write</i>"]
    customers_raw((customers_raw)) --> step3
    step3 --> sink3[("${catalog}.bronze.customers_raw")]
```
