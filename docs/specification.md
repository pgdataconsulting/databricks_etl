Specification: Declarative YAML-Based ETL Framework for Databricks

1. Overview

The goal of the application is to provide a declarative framework for defining ETL and data transformation processes in YAML configuration files. Instead of writing custom PySpark code for every pipeline, users will define processing steps such as read, select, filter, join, aggregate, write, and other transformations in YAML.

At runtime, the application will parse the YAML configuration, validate it, translate it into executable PySpark logic, and run it on Databricks. The execution can be orchestrated by Azure Data Factory, which will trigger Databricks notebooks and pass the required YAML configuration or configuration path as a runtime parameter.

2. Main Objective

The application should allow data teams to build reusable, maintainable, and standardized ETL processes by separating business logic from execution code.

The YAML file should describe what the pipeline should do, while the framework should handle how it is executed in PySpark.

3. Target Architecture

The proposed architecture consists of the following components:

1. YAML configuration files stored in Git, Azure DevOps, DBFS, ADLS, or Databricks Workspace.
2. A generic Databricks notebook deployed to a Databricks workspace.
3. A custom Python/PySpark library installed on the Databricks cluster.
4. Azure Data Factory used as the orchestration layer.
5. Databricks clusters or job clusters used for distributed execution.
6. Delta Lake or another supported storage layer used as source and target.

The runtime flow should look as follows:

ADF triggers a Databricks notebook → the notebook receives the YAML configuration path or content → the framework loads and validates the YAML → the YAML is translated into PySpark transformations → the process is executed on Databricks → results are written to the configured target → execution status and logs are returned to ADF.

4. Core Concept

Each ETL process will be defined as a YAML file containing a sequence of operations. Each operation represents a logical step in the pipeline.

Example operations:

process_name: customer_orders_etl
parameters:
  business_date: "2026-07-07"
  environment: "dev"
steps:
  - name: read_customers
    type: read
    format: delta
    path: abfss://datalake/silver/customers
    alias: customers
  - name: read_orders
    type: read
    format: delta
    path: abfss://datalake/silver/orders
    alias: orders
  - name: filter_orders
    type: filter
    input: orders
    condition: "order_date = '${business_date}'"
    output: filtered_orders
  - name: join_customers_orders
    type: join
    left: filtered_orders
    right: customers
    join_type: left
    condition:
      - "filtered_orders.customer_id = customers.customer_id"
    output: customer_orders
  - name: select_columns
    type: select
    input: customer_orders
    columns:
      - customer_id
      - customer_name
      - order_id
      - order_date
      - order_amount
    output: final_dataset
  - name: write_output
    type: write
    input: final_dataset
    format: delta
    mode: overwrite
    path: abfss://datalake/gold/customer_orders

5. Supported Operations

The first version of the framework should support the following operation types:

Read

The read operation loads data from a configured source.

Supported source types:

* Delta Lake
* Parquet
* CSV
* JSON
* JDBC
* Databricks tables
* Unity Catalog tables

Required fields:

type: read
format: delta
path: ...
alias: ...

Optional fields:

schema: ...
options: ...

Write

The write operation saves the result of a transformation.

Supported targets:

* Delta Lake
* Parquet
* CSV
* Databricks table
* Unity Catalog table
* JDBC target

Example:

type: write
input: final_dataset
format: delta
mode: overwrite
path: ...
partition_by:
  - business_date

Select

The select operation chooses or renames columns.

Example:

type: select
input: source_df
columns:
  - customer_id
  - "customer_name as name"
  - order_amount
output: selected_df

Filter

The filter operation applies a condition to a DataFrame.

Example:

type: filter
input: orders
condition: "order_amount > 0"
output: valid_orders

Join

The join operation combines two DataFrames.

Example:

type: join
left: orders
right: customers
join_type: left
condition:
  - "orders.customer_id = customers.customer_id"
output: orders_with_customers

Supported join types:

* inner
* left
* right
* full
* semi
* anti
* cross, if explicitly enabled

Aggregate

The aggregate operation groups data and calculates metrics.

Example:

type: aggregate
input: orders
group_by:
  - customer_id
aggregations:
  total_amount: "sum(order_amount)"
  order_count: "count(*)"
output: customer_summary

Add Calculated Columns

The add_columns operation adds new calculated columns.

Expression Transformation

The framework should support an expression transformation that allows users to define new columns or overwrite existing columns using Spark SQL-compatible expressions.

This transformation is intended for row-level calculations, data cleansing, formatting, type casting, conditional logic, and usage of built-in Spark SQL functions.

Example:

- name: enrich_customer_data
  type: expression
  input: customers
  expressions:
    full_name: "concat(first_name, ' ', last_name)"
    email_lower: "lower(email)"
    birth_year: "year(birth_date)"
    is_adult: "case when age >= 18 then true else false end"
    load_timestamp: "current_timestamp()"
    business_date: "to_date('${business_date}', 'yyyy-MM-dd')"
  output: customers_enriched

The framework should translate every expression into PySpark column logic using Spark SQL functions, for example through pyspark.sql.functions.expr.

Conceptually:

from pyspark.sql.functions import expr
df = input_df
for column_name, sql_expression in expressions.items():
    df = df.withColumn(column_name, expr(sql_expression))

The expression transformation should support:

* Creating new columns.
* Overwriting existing columns.
* Using Spark SQL functions such as lower, upper, concat, coalesce, cast, to_date, date_format, current_timestamp, regexp_replace, substring, trim, round, etc.
* Conditional logic using case when.
* Runtime parameters injected into expressions.
* Explicit type casting.
* Null handling.
* String, numeric, date, timestamp, and boolean expressions.

Example with casting and null handling:

- name: normalize_orders
  type: expression
  input: raw_orders
  expressions:
    order_id: "cast(order_id as string)"
    order_amount: "cast(order_amount as decimal(18,2))"
    order_date: "to_date(order_date, 'yyyy-MM-dd')"
    customer_id: "coalesce(customer_id, 'UNKNOWN')"
    is_valid_order: "case when order_amount > 0 then true else false end"
  output: normalized_orders

The framework should validate that:

* input exists in the execution context.
* expressions is not empty.
* Each expression has a valid output column name.
* Expressions can be parsed by Spark.
* Runtime parameters referenced in expressions are provided.
* The transformation output is registered in the execution context.

If an expression fails, the error message should include:

* Pipeline name
* Step name
* Input DataFrame
* Column being calculated
* Failed Spark SQL expression
* Original Spark exception

Example:

type: add_columns
input: orders
columns:
  business_date: "'${business_date}'"
  load_timestamp: "current_timestamp()"
  net_amount: "amount - tax_amount"
output: orders_enriched

Deduplicate

The deduplicate operation removes duplicates using either simple column-based deduplication or window-based logic.

Example:

type: deduplicate
input: source_df
partition_by:
  - customer_id
order_by:
  - "updated_at desc"
output: deduplicated_df

This should be translated into PySpark logic using ROW_NUMBER() OVER (...).

Custom SQL Expression

The framework may support SQL-style expressions for advanced users.

Example:

type: sql
query: |
  SELECT customer_id, COUNT(*) AS order_count
  FROM orders
  GROUP BY customer_id
output: customer_order_count

This should be optional and controlled, because the main goal is declarative configuration, not free-form SQL everywhere.

6. Runtime Execution Model

The framework should expose a generic execution entry point, for example:

run_pipeline(config_path: str, parameters: dict)

The Databricks notebook should be generic and should not contain pipeline-specific business logic.

Example notebook flow:

from etl_framework.runner import run_pipeline
config_path = dbutils.widgets.get("config_path")
business_date = dbutils.widgets.get("business_date")
environment = dbutils.widgets.get("environment")
run_pipeline(
    config_path=config_path,
    parameters={
        "business_date": business_date,
        "environment": environment
    }
)

The YAML file is loaded at runtime, parsed, validated, and executed step by step.

7. Parameterization

The framework should support runtime parameters passed from Azure Data Factory or Databricks Jobs.

Example YAML parameter usage:

parameters:
  business_date: null
steps:
  - name: filter_by_business_date
    type: filter
    input: source
    condition: "business_date = '${business_date}'"
    output: filtered_source

ADF should be able to pass values such as:

{
  "config_path": "/configs/customer_orders.yaml",
  "business_date": "2026-07-07",
  "environment": "prod"
}

8. Validation

Before execution, the framework should validate the YAML file.

Validation should include:

* Required fields are present.
* Step names are unique.
* Input DataFrames reference existing previous outputs.
* Output aliases are unique or intentionally overwritten.
* Operation type is supported.
* Join conditions are valid.
* Write target is defined.
* Required parameters are provided.
* Unsupported operations fail fast with a clear error message.

Invalid YAML should stop the process before any write operation is executed.

9. Logging and Monitoring

The framework should provide structured logs for every execution.

Logs should include:

* Process name
* Run ID
* Environment
* Config path
* Start time
* End time
* Step name
* Step type
* Input DataFrame names
* Output DataFrame name
* Number of rows processed, where possible
* Execution status
* Error message and stack trace in case of failure

Logs should be visible in Databricks and returned to Azure Data Factory where possible.

Optional future extension: write execution metadata to a Delta audit table.

Example audit table:

etl_audit.pipeline_runs
etl_audit.pipeline_steps
etl_audit.pipeline_errors

10. Error Handling

The framework should fail fast for configuration errors and provide readable messages.

Examples:

* Missing input DataFrame
* Unsupported operation type
* Invalid join condition
* Missing runtime parameter
* Write target not configured
* Schema mismatch
* Permission issue on source or target

Each error should include:

* Pipeline name
* Step name
* Step type
* Error description
* Original exception details

11. Deployment Model

The framework should be packaged as a Python library and deployed to Databricks clusters.

Possible deployment options:

* Wheel package installed on Databricks cluster
* Databricks Asset Bundles
* Unity Catalog volumes
* DBFS library installation
* CI/CD deployment from Azure DevOps

The generic notebook should also be deployed to the Databricks workspace and reused by multiple pipelines.

Only the YAML configuration should change between processes.

12. Azure Data Factory Integration

ADF should orchestrate the execution by triggering the Databricks notebook.

ADF should pass parameters such as:

* config_path
* business_date
* environment
* run_id
* optional process-specific parameters

ADF should be responsible for:

* Scheduling
* Dependency management
* Retry policy
* Environment-specific parameter injection
* Monitoring pipeline success or failure

Databricks should be responsible for:

* Reading the YAML
* Executing PySpark transformations
* Writing data
* Returning status and errors

13. Example ADF to Databricks Runtime Flow

1. ADF pipeline starts.
2. ADF calculates or receives business_date.
3. ADF triggers Databricks notebook.
4. Notebook receives config_path and runtime parameters.
5. Framework loads YAML configuration.
6. Framework validates configuration.
7. Framework executes each YAML step as PySpark.
8. Output is written to the configured target.
9. Execution metadata is logged.
10. Databricks returns success or failure to ADF.

14. Design Principles

The framework should follow these principles:

* Declarative over imperative.
* Configuration separated from execution logic.
* Reusable generic notebook.
* Reusable operation handlers.
* Clear validation before execution.
* Fail fast on invalid configuration.
* Minimal custom code per pipeline.
* Environment-agnostic configuration where possible.
* Support for CI/CD.
* Testable transformation logic.
* Extensible operation model.

15. Internal Framework Structure

Suggested Python package structure:

etl_framework/
  __init__.py
  runner.py
  config_loader.py
  config_validator.py
  parameter_resolver.py
  execution_context.py
  operations/
    __init__.py
    base.py
    read.py
    write.py
    select.py
    filter.py
    join.py
    aggregate.py
    add_columns.py
    deduplicate.py
    sql.py
  logging/
    audit_logger.py
    step_logger.py
  exceptions/
    config_errors.py
    execution_errors.py

The runner should be responsible for orchestration, while every operation type should be implemented as a separate handler.

Example concept:

class JoinOperation:
    def execute(self, context, step_config):
        left_df = context.get_dataframe(step_config["left"])
        right_df = context.get_dataframe(step_config["right"])
        result_df = left_df.join(
            right_df,
            on=build_join_condition(step_config["condition"]),
            how=step_config.get("join_type", "inner")
        )
        context.register_dataframe(step_config["output"], result_df)

16. Execution Context

The framework should maintain an execution context during runtime.

The context should store:

* Spark session
* Runtime parameters
* Loaded DataFrames
* Output DataFrames
* Current step metadata
* Audit information
* Environment configuration

Example:

context.dataframes["customers"] = customers_df
context.parameters["business_date"] = "2026-07-07"

Every step should read from and write to this context.

17. Security and Access

The framework should not store secrets directly in YAML files.

Secrets should be handled through:

* Databricks Secrets
* Azure Key Vault
* Managed Identity
* Service principals
* Unity Catalog permissions

YAML files should contain logical references, not raw passwords or tokens.

Bad example:

password: "my-password"

Preferred example:

secret_scope: data-platform-secrets
secret_key: source-db-password

18. Testing Strategy

The framework should support automated testing.

Recommended test levels:

* Unit tests for every operation handler.
* YAML validation tests.
* Integration tests on sample Databricks data.
* End-to-end tests triggered through ADF or Databricks Jobs.
* Regression tests for existing YAML pipelines.

Each YAML process should be testable without modifying the core framework code.

19. Versioning

Both the framework and YAML specification should be versioned.

Example:

spec_version: "1.0"
process_name: customer_orders_etl

This allows future changes to the YAML schema without breaking existing pipelines.

20. Future Extensions

Possible future features:

* Data quality checks
* Schema enforcement
* Automatic lineage extraction
* Conditional steps
* Looping over datasets
* Reusable YAML templates
* Environment-specific overrides
* Dynamic partition overwrite
* CDC support
* SCD Type 1 and Type 2 support
* Integration with Unity Catalog lineage
* Integration with OpenLineage
* Automatic documentation generation from YAML
* Dry-run mode
* Cost and performance metrics
* Row-level reconciliation checks

21. Expected Benefits

The application should provide the following benefits:

* Faster ETL development.
* Reduced amount of repetitive PySpark code.
* Better standardization across pipelines.
* Easier maintenance.
* Better readability of business logic.
* Easier onboarding for new developers.
* Separation between platform code and process configuration.
* Improved governance through validation, logging, and versioning.
* Easier migration from legacy ETL tools to Databricks.

22. Summary

The application will act as a declarative ETL execution framework for Databricks. Users will define ETL logic in YAML files, while the framework will translate those definitions into PySpark operations at runtime.

Azure Data Factory can be used as the orchestration layer, passing configuration and runtime parameters to a generic Databricks notebook. The notebook will load the framework libraries installed on the cluster, execute the YAML-defined process, and return execution status back to ADF.

This approach enables scalable, reusable, and standardized ETL development while reducing the need to write custom PySpark code for every pipeline.
