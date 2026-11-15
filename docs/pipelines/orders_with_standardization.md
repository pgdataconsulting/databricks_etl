# Pipeline: orders_with_standardization

Spec version: `1.0`

Source configuration: `configs/orders_with_standardization.yaml`

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
| 1 | `read_orders` | read |  | orders | `format: delta`<br>`path: abfss://datalake/silver/orders`<br>`schema_ref: silver/orders` |
| 2 | `standardize` | run_config | orders | orders_std | `config_path: standardize_orders.yaml` |
| 3 | `write_output` | write | orders_std |  | `format: delta`<br>`mode: overwrite`<br>`path: abfss://datalake/gold/orders_standardized` |

## Lineage

```mermaid
flowchart TD
    step0["read_orders<br><i>read</i>"]
    step0 --> orders((orders))
    step1["standardize<br><i>run_config</i>"]
    orders((orders)) --> step1
    step1 --> orders_std((orders_std))
    step2["write_output<br><i>write</i>"]
    orders_std((orders_std)) --> step2
    step2 --> sink2[("abfss://datalake/gold/orders_standardized")]
```
