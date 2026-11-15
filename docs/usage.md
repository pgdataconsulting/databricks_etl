# Using the ETL framework

This framework implements the [design specification](specification.md):
ETL pipelines are defined in YAML and executed as PySpark on Databricks.

## Installation

```bash
pip install -e .              # validate and document configs; no Spark needed
pip install -e ".[spark]"     # also run pipelines locally (needs Java 17+)
pip install -e ".[dev]"       # everything for working on the framework
```

PySpark and Delta Lake are not install requirements, because the Databricks
runtime provides them. On Databricks the framework is installed as a wheel;
the Asset Bundle in this repository builds and attaches it to the jobs
(`databricks bundle deploy`), see the [deployment guide](deployment.md).

## Running a pipeline

```python
from etl_framework.runner import run_pipeline

result = run_pipeline(
    config_path="/Volumes/main/configs/customer_orders.yaml",
    parameters={"business_date": "2026-07-07", "environment": "prod"},
)
```

`run_pipeline` loads the YAML (local, `dbfs:/`, Workspace, or Volume paths),
merges runtime parameters with the YAML `parameters` defaults, validates the
configuration (failing fast before anything is written), executes each step,
and returns a JSON-serializable run summary. YAML can also be passed inline via
`config_content=` instead of `config_path=`.

The same thing from the command line, which is also what a Databricks job
runs (a `python_wheel_task` with these arguments):

```bash
etl-framework run configs/customer_orders.yaml \
    --param business_date=2026-07-07 --param environment=prod \
    --run-id "$ORCHESTRATOR_RUN_ID"
```

`run` prints the run summary to stdout as one JSON document (or writes it
with `--output`); log lines go to stderr, so stdout can be piped to a parser.
Its exit code is 0 when the pipeline succeeded, 1 when a step failed, and 2
when the configuration or the command line was rejected, in which case
nothing was executed and no state was touched. `--params-json` takes
parameters as a JSON object, which keeps their types; `--param` values are
strings and take precedence.

`run` uses the active Spark session. Off Databricks, `--local [WAREHOUSE_DIR]`
runs on a local session with Delta Lake configured (needs the `spark` extra
and Java); table metadata lives only as long as the process, so use it for
path-based pipelines or see [Local development](#local-development).

When Azure Data Factory is the orchestrator, deploy `notebooks/run_pipeline.py`
as the single generic notebook and trigger it with the `config_path`,
`business_date`, `environment`, `run_id`, and optional `extra_parameters`
widget values. The notebook returns the run summary to ADF through
`dbutils.notebook.exit`.

## Pipeline structure

```yaml
spec_version: "1.0"
process_name: my_pipeline
parameters:            # defaults; runtime values override, null = required
  business_date: null
settings:
  log_row_counts: true # count rows per step (extra Spark action per step)
  allow_sql: false     # opt-in gate for free-form `sql` steps
  audit:               # optional Delta audit trail
    enabled: false
    schema: etl_audit
steps:
  - name: ...
    type: read | write | select | filter | join | aggregate | ...
    ...
```

Strings in `steps` and in `settings` may reference runtime parameters as
`${name}`. In `settings` that is what lets one file serve every environment:

```yaml
parameters:
  catalog: etl_dev
settings:
  audit:
    enabled: true
    schema: ${catalog}.etl_audit
  watermark:
    enabled: true
    table: ${catalog}.etl_state.watermarks
    ...
```

A string that is exactly one placeholder keeps the parameter's type; inside a
longer string the value is inserted as text. The switches among the settings
(`log_row_counts`, `allow_sql`, `enforce_nullability`, `audit.enabled`,
`watermark.enabled`, `lineage.enabled`, and `audit_columns`) also accept the
words `true` and `false`, which is what a command-line or job parameter
delivers; anything else is rejected by validation. The one setting that
cannot reference a parameter is `template_base_path`, because templates are
expanded before parameters are resolved.

**A note on YAML.** YAML reads the bare words `yes`, `no`, `on` and `off` as
booleans. As a field name the framework keeps them as written (so the `on:`
of a `lookup` step works), but as a *value* they are booleans: quote them
when you mean text, for example `values: ["NO", "SE", "DK"]` for country
codes.

### Built-in parameters

Besides the parameters passed at runtime, every run automatically provides
`${run_id}`, `${process_name}`, and `${run_started_at}` (UTC ISO timestamp).
Explicitly passed parameters with the same names take precedence.

### Standard audit columns

Set `settings.audit_columns` to stamp every sink's rows (write, merge, scd2)
with standard lineage columns:

```yaml
settings:
  audit_columns: true                    # all of the columns below
  # audit_columns: [_run_id, _load_timestamp]   # or a subset
```

Available columns: `_run_id`, `_process_name`, `_environment`,
`_load_timestamp`. They are appended just before the sink executes, so
schema contracts on write steps see (and must include, when strict) these
columns. Unknown names fail validation.

## Schema contracts

Dataset schemas live as versioned YAML files in a `schemas/` directory kept in
Git next to `configs/`, one file per dataset:

```yaml
# schemas/silver/orders.yaml
dataset: silver.orders
strict: false                    # optional: also fail on extra columns
columns:
  - {name: order_id, type: int, nullable: false}
  - {name: customer_id, type: int, nullable: true}
  - {name: order_amount, type: double, nullable: true}
```

Read and write steps reference a contract with `schema_ref` (the file path
under the schema directory, extension optional):

```yaml
- name: read_orders
  type: read
  format: delta
  path: abfss://datalake/silver/orders
  schema_ref: silver/orders
  alias: orders
```

Enforcement happens in three places:

1. **Validation time** — every `schema_ref` must resolve to an existing, valid
   schema file or the run fails before any step executes.
2. **On read** — for CSV/JSON the contract is *applied* as the read schema;
   for self-describing sources (Delta, Parquet, tables, JDBC) the actual
   schema is *compared* against the contract and the step fails on mismatch.
3. **Before write** — the output DataFrame is compared against the target's
   contract before the write starts, so a drifted pipeline never touches the
   target.

Mismatches raise `SchemaEnforcementError` listing every problem (missing
columns, type mismatches, and — when strict — unexpected extra columns).
Column matching is case-insensitive and order-independent. Per-step
`strict: true|false` overrides the contract's own `strict` flag. Nullability
is only checked when `settings.enforce_nullability: true`, because Spark
frequently reports readable sources as nullable regardless of the data.

The schema directory is located in this order: the `schema_base_path`
argument to `run_pipeline`, then `settings.schema_base_path` in the YAML,
then the nearest `schemas` directory at or above the config file, up to two
levels up (the conventional `configs/` + `schemas/` repo layout, with configs
optionally grouped one level deep). `templates/` is found the same way.

## Supported operations

| Type | Purpose | Key fields |
| --- | --- | --- |
| `read` | Load a source into the context | `format` (delta/parquet/csv/json/jdbc/table/unity_catalog), `path` or `table`, `alias`, optional `schema`, `schema_ref`, `options` |
| `write` | Save a DataFrame | `input`, `format`, `mode`, `path` or `table`, optional `partition_by`, `schema_ref`, `schema_drift`, `options` |
| `select` | Choose/rename columns | `input`, `columns` (supports `"col as name"`), `output` |
| `filter` | Apply a SQL condition | `input`, `condition`, `output` |
| `join` | Combine two DataFrames | `left`, `right`, `join_type` (inner/left/right/full/semi/anti/cross), `condition` (list), `output` |
| `aggregate` | Group and aggregate | `input`, `group_by`, `aggregations` (name → SQL expr), `output` |
| `expression` | Add/overwrite columns with Spark SQL | `input`, `expressions` (name → SQL expr), `output` |
| `add_columns` | Same as `expression` with a `columns` mapping | `input`, `columns`, `output` |
| `deduplicate` | Remove duplicates | `input`, `partition_by`, optional `order_by` (`"col desc"`), `output` |
| `union` | Stack DataFrames by column name | `inputs` (2+), optional `distinct`, `allow_missing_columns`, `output` |
| `window` | Analytic columns over a window | `input`, `columns` (name → SQL expr), optional `partition_by`, `order_by`, `frame`, `output` |
| `sample` | Random fraction or first N rows | `input`, `fraction` (with optional `seed`) or `rows`, `output` |
| `cache` | Persist/checkpoint a reused DataFrame | `input`, optional `method` (cache/checkpoint/local_checkpoint), `storage_level`, `eager`, `checkpoint_dir`, `output` |
| `lookup` | Broadcast-enrich from a small dimension | `input`, `lookup`, `on` (input col → lookup col), optional `select`, `defaults`, `output` |
| `pivot` | Turn one column's values into columns | `input`, `pivot_column`, `aggregations`, optional `group_by`, `values`, `output` |
| `unpivot` | Melt columns into key/value rows | `input`, `id_columns`, optional `value_columns`, `key_column`, `value_column`, `output` |
| `fuzzy_match` | Join on approximate string equality | `left`, `right`, `left_on`, `right_on`, optional `method`, `threshold`, `block_on`, `keep`, `join_type`, `output` |
| `quality` | Data quality checks | `input`, `checks`, `output`, optional `on_failure` (fail/warn/quarantine), `quarantine_path` |
| `merge` | Upsert into a Delta target (`MERGE INTO`) | `input`, `merge_keys`, `path` or `table`, optional `update_condition`, `delete_condition`, `schema_evolution`, `schema_ref` |
| `scd2` | Maintain SCD Type 2 history | `input`, `keys`, `path` or `table`, optional `tracked_columns`, `effective_time`, `schema_ref` |
| `cdc` | Apply a change feed (I/U/D) to a Delta target | `input`, `keys`, `operation_column`, `path` or `table`, optional `operation_values`, `sequence_by` |
| `reconcile` | Row-level comparison of two DataFrames | `left`, `right`, `keys`, optional `compare_columns`, `tolerance`, `on_failure` (fail/warn), `output` |
| `sql` | Free-form SQL over registered DataFrames | `query`, `output`; requires `settings.allow_sql: true` |
| `run_config` | Run another YAML config as a nested pipeline | `config_path`, optional `parameters`, `inputs` (child ← parent), `outputs` (parent ← child) |

`merge` updates matched rows (all columns), inserts unmatched rows, and
optionally deletes matched rows via `delete_condition`; the target is created
from the input on first run. Delta targets only.

`cdc` applies change records to a Delta target via `MERGE INTO`: deletes
remove the row, updates and inserts upsert the business columns (everything
except `operation_column`). With `sequence_by`, only the latest change per
key is applied, so replayed or batched feeds collapse correctly; without it,
the feed must contain at most one change per key. A missing target is created
from the non-delete changes. `operation_values` remaps the indicator values
(defaults: `I`/`U`/`D`).

`reconcile` full-outer-joins `left` against `right` on the keys and
classifies each differing row as `missing_in_right`, `missing_in_left`, or
`mismatch`. `fail` raises with per-category counts; `warn` logs and
continues. When `output` is set, a per-row difference report (keys +
`reconcile_status` + `mismatched_columns`) is registered for downstream use,
e.g. writing it to a reconciliation table. `tolerance` absorbs small
differences on numeric columns.

`scd2` owns the versioning columns `_effective_from`, `_effective_to`, and
`_is_current`: input rows whose `tracked_columns` (default: all non-key
columns) differ from the target's current version close the old version and
insert a new current one; unchanged rows and history are untouched.
`effective_time` accepts a SQL expression (default `current_timestamp()`),
e.g. `"to_timestamp('${business_date}')"` for deterministic reruns. A
`schema_ref` on merge/scd2 validates the *input* (business columns); the SCD
columns are added by the framework afterwards.

`union` combines two or more DataFrames by column *name* (not position);
`allow_missing_columns: true` fills columns absent on some inputs with nulls,
and `distinct: true` drops duplicate rows (UNION vs UNION ALL semantics).

`window` adds analytic columns without collapsing rows: each `columns` entry
is a SQL expression evaluated `OVER` the window built from `partition_by` /
`order_by` (plus an optional `frame` clause), covering ranking
(`row_number()`, `rank()`), navigation (`lag(col, 1)`, `lead(col, 1)`), and
running aggregates (`sum(col)`, `avg(col)`). With `order_by`, Spark's default
frame runs from the partition start to the current row — running totals work
out of the box. Both lists may be omitted for grand-total windows, e.g.
`pct_of_total: "order_amount / sum(order_amount)"`.

`lookup` is a specialized left join for dimension enrichment: the `lookup`
DataFrame is broadcast (no shuffle of the large input), only the `select`ed
columns are appended (`"col as name"` renames; default: all non-key columns),
and misses fall back to `defaults` instead of nulls. Lookup keys should be
unique per `on` combination — duplicates multiply rows exactly like a join.

`sample` builds development subsets and profiling slices: `fraction: 0.01`
draws a random ~1% sample (`seed` makes it reproducible), `rows: 1000` takes
the first thousand rows (cheap, but not random).

`cache` avoids recomputing an intermediate DataFrame consumed by several
later steps (Spark otherwise re-evaluates the full lineage per sink).
`method: cache` persists at `storage_level` (default `MEMORY_AND_DISK`;
`eager: true` materializes immediately instead of at first use);
`checkpoint` / `local_checkpoint` materialize *and* truncate the lineage,
which also keeps very deep plans manageable. `checkpoint` writes to
`checkpoint_dir` (or the cluster's configured directory) and survives
executor loss; `local_checkpoint` needs no directory but is not
fault-tolerant.

`pivot` groups by `group_by`, turns the distinct values of `pivot_column`
into columns, and fills them with the `aggregations` (name → SQL expression,
like `aggregate`). Declare the expected `values` explicitly where possible:
it keeps the output schema stable as data changes and skips the extra Spark
job that otherwise collects distinct values. With several aggregations the
columns are named `<value>_<aggregation>`. `unpivot` is the inverse (wide to
long): every `value_columns` entry (default: all non-`id_columns`) becomes a
row of `key_column` / `value_column`; the melted columns must share a common
type, so cast them first when they are mixed.

### Fuzzy matching

`fuzzy_match` joins two DataFrames on *approximate* string equality — the
typical "same customer, differently spelled" reconciliation between systems:

```yaml
- name: match_accounts
  type: fuzzy_match
  left: crm_customers
  right: billing_accounts
  left_on: company_name
  right_on: account_name
  method: levenshtein          # levenshtein (default) | soundex
  threshold: 0.85              # minimum similarity, 0..1 (levenshtein only)
  block_on:                    # equality pre-filters, join-condition syntax
    - "crm_customers.country = billing_accounts.country"
  keep: best                   # best (default) | all
  join_type: inner             # inner (default) | left
  output: matched_accounts
```

- **Methods** — `levenshtein` keeps pairs whose normalized edit-distance
  similarity (1 − distance ÷ longer length) reaches `threshold`; `soundex`
  keeps phonetically equal values. Both sides are lowercased and trimmed
  before comparison unless `normalize: false`.
- **Score** — every match carries its similarity in `_match_score` (rename
  via `score_column`), so match quality can be checked downstream — e.g. a
  `quality` step asserting `_match_score >= 0.9`. It is computed for soundex
  matches too, which is how `keep: best` ranks them.
- **Keep** — `best` keeps only the highest-scoring right-side candidate per
  left row; `all` keeps every pair that passed. `join_type: left` also keeps
  unmatched left rows with null right columns and a null score.
- **Blocking** — fuzzy comparison cannot use a hash join, so an
  unconstrained match compares every left row against every right row.
  `block_on` equality conditions (country, postcode, first letter, ...)
  bound the search space; omitting them requires an explicit
  `allow_unblocked: true` (fine for small dimensions, expensive at scale).
- Shared column names keep the left copy, like `join`
  (`duplicate_columns: keep_both` to keep both).

### Environment-specific overrides

A single YAML file can carry per-environment differences for parameter
defaults and settings — steps are never overridden, so the pipeline logic
stays identical everywhere:

```yaml
parameters:
  environment: "dev"
  target_path: "abfss://dev-lake/gold/orders"
environments:
  prod:
    parameters:
      target_path: "abfss://prod-lake/gold/orders"
    settings:
      audit: {enabled: true}
```

The active environment comes from the `environment` runtime parameter
(falling back to the YAML default). Precedence, lowest to highest: YAML
defaults → environment overrides → explicit runtime parameters. Environments
without an entry use the base configuration unchanged.

### Reusable step templates

Templates are parameterized step sequences kept in a `templates/` directory
next to `configs/` (or set `settings.template_base_path` / the
`template_base_path` argument):

```yaml
# templates/ingest_with_quality.yaml
template_name: ingest_with_quality
template_parameters: [source_path, alias]
steps:
  - name: read_${alias}
    type: read
    format: delta
    path: ${source_path}
    alias: ${alias}
  - name: check_${alias}
    type: quality
    input: ${alias}
    checks:
      - {type: row_count, min: 1}
    output: ${alias}_checked
```

Pipelines expand a template in place of a step entry:

```yaml
steps:
  - use_template: ingest_with_quality
    with:
      source_path: abfss://datalake/silver/orders
      alias: orders
  - name: next_step
    input: orders_checked
    ...
```

Only the placeholders named in `with` are substituted at expansion time —
other `${...}` references (e.g. `${business_date}`) are left for runtime
resolution, so templates can use runtime parameters too. A template can be
used many times with different arguments; missing template parameters, missing
template files, and templates including other templates all fail fast. The
CLI `validate` and `docs` commands expand templates the same way.

### Nested pipelines

A `run_config` step executes another YAML configuration as a nested pipeline,
so one config can be run within another. Where templates splice *steps* into
the calling pipeline, a nested config is a complete pipeline run of its own —
loaded, template-expanded, environment-resolved, validated, and executed with
the parent's Spark session — that the parent treats as a single step:

```yaml
# configs/customer_orders.yaml (parent)
steps:
  - name: read_orders
    type: read
    format: delta
    path: abfss://datalake/silver/orders
    alias: orders
  - name: standardize
    type: run_config
    config_path: shared/standardize_orders.yaml  # relative to this config
    parameters:
      business_date: "${business_date}"
    inputs:                 # child alias ← parent DataFrame
      raw_orders: orders
    outputs:                # parent alias ← child DataFrame
      orders_std: standardized
  - name: write_output
    type: write
    input: orders_std
    ...
```

```yaml
# configs/shared/standardize_orders.yaml (child)
process_name: standardize_orders
inputs: [raw_orders]        # DataFrames the caller must provide
parameters:
  business_date: null
steps:
  - name: filter_business_date
    type: filter
    input: raw_orders
    condition: "order_date = '${business_date}'"
    output: filtered
  - ...
    output: standardized
```

Semantics:

- **Parameters** — the child sees only what the step's `parameters` mapping
  passes (values may reference parent parameters via `${...}`), plus the
  parent's `environment` unless explicitly overridden, so environment
  overrides stay consistent across the whole run. The child's own defaults
  and `environments` block apply as usual.
- **DataFrames** — `inputs` hands parent DataFrames to the child under child
  aliases; `outputs` registers child DataFrames back into the parent. Both
  are optional: a child without them is a self-contained sub-process reading
  and writing its own sources. A child that consumes caller-provided
  DataFrames should declare them in a top-level `inputs:` list so it still
  passes standalone CLI validation; running such a config directly (not
  nested) fails fast with a clear error.
- **Paths** — a relative `config_path` is resolved against the parent
  config's directory. The child locates its own `schemas/` and `templates/`
  directories the usual way.
- **Observability** — the child runs under run id
  `<parent_run_id>:<step_name>` with its own step logs, audit records, and
  lineage; its full run summary is included in the parent summary under
  `nested_runs`. With a single `outputs` entry, `log_row_counts` counts the
  exported DataFrame for the parent step.
- **Safety** — circular references between configs and nesting deeper than
  10 levels fail fast. The child validates itself before executing any of
  its steps; parent steps before the `run_config` step will already have run
  by then, so validate child configs in CI
  (`python -m etl_framework validate configs/**/*.yaml`) to catch problems
  earlier.

### Lineage

Dataset-level lineage (sources read, targets written, and the DataFrame flow
between steps) is derived from the configuration and always included in the
run summary and dry-run output. Optionally, OpenLineage RunEvents (START and
COMPLETE/FAIL) can be emitted to any OpenLineage-compatible backend such as
Marquez — no extra client dependency needed:

```yaml
settings:
  lineage:
    enabled: true
    transport: http        # http | console (default: console)
    url: https://marquez.example.com/api/v1/lineage
    api_key: ${secret:observability/marquez-token}   # optional bearer token
    namespace: datalake
```

Lineage emission failures are logged and never fail the run. On Databricks,
reads and writes through Unity Catalog tables additionally get column-level
lineage captured natively by Unity Catalog, with no framework involvement.

### Data quality checks

```yaml
- name: check_orders
  type: quality
  input: orders
  on_failure: fail            # fail (default) | warn | quarantine
  checks:
    - {type: not_null, columns: [order_id, customer_id]}
    - {type: unique, columns: [order_id]}
    - {type: accepted_values, column: status, values: [NEW, SHIPPED]}
    - {type: expression, condition: "order_amount >= 0", name: non_negative}
    - {type: row_count, min: 1}
  output: checked_orders
```

`fail` raises a `QualityCheckError` listing every failed check with violation
counts. `warn` logs the failures and passes all rows through. `quarantine`
writes rows violating row-level checks to `quarantine_table` or
`quarantine_path` (`quarantine_format`, default delta; `quarantine_mode`,
default append) and passes only clean rows downstream; dataset-level failures
(`unique`, `row_count`) still fail the step because they cannot be attributed
to individual rows. On Unity Catalog use `quarantine_table`:

```yaml
  on_failure: quarantine
  quarantine_table: ${catalog}.quarantine.orders
```

### Schema drift on writes

By default a write fails when the input carries columns the Delta target
does not have — drift must be an explicit decision, not an accident.
`schema_drift` makes the decision declarative:

```yaml
- name: write_output
  type: write
  input: final_dataset
  format: delta
  mode: append
  schema_drift: evolve      # fail (default) | evolve | overwrite
  path: abfss://datalake/gold/orders
```

`evolve` adds new input columns to the target in place (Delta `mergeSchema`;
existing rows read them as null). `overwrite` replaces the target schema
entirely and requires `mode: overwrite` (Delta `overwriteSchema`) — the
rebuild-the-table case. Both apply to Delta targets only, and a `schema_ref`
contract is still enforced first, so a contract-violating drift never reaches
the target. The same opt-in exists on `merge` as `schema_evolution: true`;
without it, Delta's `UPDATE SET * / INSERT *` silently ignores extra input
columns.

### Per-step retries

Any step may declare a `retry:` block for transient failures — flaky JDBC
sources, storage throttling, cloud hiccups:

```yaml
- name: read_erp
  type: read
  format: jdbc
  options: {...}
  alias: erp_orders
  retry:
    attempts: 3          # total attempts, including the first
    delay_seconds: 10    # wait before the first retry
    backoff: 2           # delay multiplier per retry (default 2)
```

Each retry re-executes the whole step and is logged as a structured
`step_retrying` warning; the step's entry in the run summary records how many
`attempts` it took. Only the final failure raises. Two things to keep in
mind: Spark is lazy, so a step's failure can surface in the downstream step
that first evaluates it (enable `settings.log_row_counts` to force evaluation
inside the owning step and make its retries effective), and retrying a step
with side effects replays them — pair retries on sinks with idempotent modes
(`merge`, `overwrite`, `overwrite_partitions`), not blind `append`.

### Conditional steps

Any step may declare a `when:` condition — a SQL boolean expression evaluated
before the step runs (typically over runtime parameters). Skipped steps are
recorded with status `skipped` in logs and the run summary. Note that a
skipped step registers no output, so downstream steps consuming it should be
guarded by the same condition.

```yaml
- name: write_prod_only
  type: write
  when: "'${environment}' = 'prod'"
  ...
```

### Dry-run mode

`run_pipeline(..., dry_run=True)` loads, resolves, and validates the
configuration, then returns the execution plan (steps, inputs, outputs,
targets, contracts) without creating a Spark session or touching any data.

### Command line interface

`validate` and `docs` need neither PySpark nor a Databricks connection, so
they work in CI and pre-commit hooks with only the package installed:

```bash
# Validate configurations (exit code 1 on failure); unset parameters
# get placeholders, schema contracts are resolved and checked.
etl-framework validate configs/*.yaml

# Generate Markdown documentation (parameters, steps, mermaid lineage
# diagram) from a pipeline configuration.
etl-framework docs configs/customer_orders.yaml -o docs/pipelines/customer_orders.md

# Run a pipeline, or only print its plan.
etl-framework run configs/customer_orders.yaml --param business_date=2026-07-07 --dry-run
```

`python -m etl_framework` is the same command. This repository's own
[pre-commit configuration](../.pre-commit-config.yaml) and
[CI workflow](../.github/workflows/ci.yml) show both gates in use.

### Performance metrics

Every successful run summary includes a `metrics` section — total duration,
per-step durations with percentage of total (sorted slowest-first), the
slowest step, and rows/second where `settings.log_row_counts` is enabled.
The same payload is logged as a structured `pipeline_metrics` event. On job
clusters wall-clock time is the primary cost driver (DBUs), so the breakdown
shows where the money goes.

### Dynamic partition overwrite

`mode: overwrite_partitions` on a write step replaces only the partitions
present in the input and leaves all other partitions untouched — the natural
mode for idempotent daily reruns into a date-partitioned target. Requires
`partition_by`.

```yaml
- name: write_snapshot
  type: write
  input: daily_orders
  format: delta
  mode: overwrite_partitions
  partition_by: [business_date]
  path: abfss://datalake/gold/orders
```

### Incremental loads with watermarks

The framework can persist a high-water mark per pipeline, so each run only
processes what arrived since the last successful run — no hand-rolled state
tables or ADF variables:

```yaml
process_name: incremental_orders
settings:
  watermark:
    enabled: true
    source: new_orders            # DataFrame whose maximum drives the state
    column: updated_at            # column to take the maximum of
    initial: "1900-01-01"         # used before the first run
    table: etl_state.watermarks   # Delta state table (default, auto-created)
steps:
  - name: read_orders
    type: read
    ...
    alias: orders
  - name: incremental_filter
    type: filter
    input: orders
    condition: "updated_at > '${watermark}'"
    output: new_orders
  - name: upsert
    type: merge
    input: new_orders
    ...
```

At run start the stored value (or `initial` on the first run) is injected as
the `${watermark}` runtime parameter (rename via `settings.watermark.
parameter`). After every step succeeds, the maximum of `column` in `source`
is persisted and reported in the run summary under `watermark`
(`{previous, new}`). The semantics are deliberately conservative:

- A **failed run never advances** the watermark, so the next run picks the
  same window up again — pair watermarking with idempotent sinks (`merge`,
  `scd2`, `mode: overwrite_partitions`) so replays are safe.
- An **empty increment** (no rows in `source`) leaves the stored value
  untouched; nothing is ever skipped.
- Watermark state is correctness-relevant, so failures reading or writing it
  **fail the run** (unlike audit logging, which never does).
- Dry runs resolve `${watermark}` from `initial` and never touch the state
  table. Values are stored as strings; ISO timestamps and dates compare
  correctly as text, which is why `updated_at > '${watermark}'` works.

Notes:

- Cross joins must be explicitly enabled per step with `allow_cross: true`.
- After a join, columns that exist on both sides keep the left copy by
  default; set `duplicate_columns: keep_both` on the join step to keep both.
- Re-registering an existing output name requires `allow_overwrite: true`.
- New operation types can be plugged in, see
  [Operation plugins](#operation-plugins).

## Validation and errors

Before execution the framework verifies required fields, unique step names,
supported operation types, join types and conditions, write targets, input
references against previous step outputs, and that every `${parameter}` has a
value. All problems are reported together in a single `ValidationError`, and
nothing executes until the configuration is valid. Runtime failures are
wrapped in `StepExecutionError` / `ExpressionExecutionError` carrying the
pipeline name, step name, step type, inputs, and the original Spark exception.

## Secrets

Credentials never go into YAML. A pipeline carries a reference:

```yaml
- name: read_source_orders
  type: read
  format: jdbc
  options:
    url: jdbc:sqlserver://sales-db.example.com:1433;database=sales
    dbtable: dbo.orders
    user: etl_reader
    password: ${secret:source-systems/sales-db-password}
  alias: source_orders
```

`${secret:<scope>/<key>}` is resolved when the step that uses it executes,
not before. That has three consequences:

- Validation, `--dry-run` and the generated docs never touch a secret and
  need no access to one.
- The value never reaches the run summary, lineage, audit tables or the
  step's log lines; they all carry the reference.
- Whatever was resolved is masked wherever text the framework did not write
  is reported: log lines and errors. Spark errors often quote connection
  options, so a failure of a step that used a secret (or any failure that
  quotes one) is raised with its message masked and nothing attached: no
  chained exception and no traceback of the failing step, because both can
  hold the value. You get the error's type and message, which is what the
  audit table and the orchestrator get as well.

References are resolved in steps, and in `settings.lineage.url` and
`api_key`. Anywhere else (another setting, a `when` condition) a reference
is rejected by validation rather than used as literal text.

Where the value comes from depends on where the pipeline runs:

| Environment | Provider | Lookup |
| --- | --- | --- |
| Databricks compute | Databricks secret scope (optionally backed by Azure Key Vault) | `dbutils.secrets.get(scope, key)` |
| Anywhere else | Environment variables | `${secret:source-systems/sales-db-password}` reads `ETL_SECRET_SOURCE_SYSTEMS_SALES_DB_PASSWORD` |

Set `ETL_SECRET_PROVIDER=databricks` or `=env` to force one. For a vault the
platform team already operates, pass any object with a `get(scope, key)`
method:

```python
run_pipeline(config_path=..., secret_provider=MyVaultClient())
```

A reference passed to a nested pipeline (`run_config` `parameters`) stays a
reference and is resolved by the child step that uses it. Besides steps, the
`url` and `api_key` of `settings.lineage` accept references.

Do not pass credentials as runtime parameters: parameters are part of the run
summary. As a safety net, parameters whose names look like credentials
(`password`, `token`, `api_key`, `secret`, ...) are masked there.

## Operation plugins

An operation that is specific to your organization does not have to be added
to the framework. Put it in your own package:

```python
# acme_etl_plugins/mask_pii.py
from etl_framework.operations import Operation


class MaskPiiOperation(Operation):
    type_name = "mask_pii"
    required_fields = ("input", "columns", "output")

    def execute(self, context, step):
        from pyspark.sql.functions import sha2

        df = context.get_dataframe(step["input"])
        for column in step["columns"]:
            df = df.withColumn(column, sha2(df[column].cast("string"), 256))
        context.register_dataframe(step["output"], df)
        return df
```

and declare it as an entry point in that package's `pyproject.toml`:

```toml
[project.entry-points."etl_framework.operations"]
mask_pii = "acme_etl_plugins.mask_pii:MaskPiiOperation"
```

Installing the package next to the framework makes `type: mask_pii` available
to every pipeline, including validation and the generated docs. A plugin that
fails to import is skipped with a warning (pipelines that use it then fail
validation with an "unsupported operation type" error), and a plugin cannot
replace a built-in operation. `register_operation(MaskPiiOperation)` does the
same thing from code.

## Local development

`etl_framework.local.local_spark_session(warehouse_dir)` returns a local Spark
session with Delta Lake configured, the one the test suite and
`examples/quickstart.py` use. Three-part table names work against it with the
catalog `spark_catalog`, which is how the medallion example runs unchanged on
a laptop and on Unity Catalog.

## Example configurations

- [`configs/customer_orders.yaml`](../configs/customer_orders.yaml) — read,
  filter, join, expression, select, write.
- [`configs/customer_order_summary.yaml`](../configs/customer_order_summary.yaml)
  — deduplicate, add_columns, aggregate, write to a table.
- [`configs/orders_with_standardization.yaml`](../configs/orders_with_standardization.yaml)
  — runs [`configs/standardize_orders.yaml`](../configs/standardize_orders.yaml)
  as a nested pipeline via `run_config`.
- [`configs/medallion/`](../configs/medallion) — a complete incremental flow on
  Unity Catalog table names: sample data, then bronze to silver (watermark,
  deduplicate, quarantine, merge), then silver to gold (lookup, aggregate,
  strict contract). `make demo` runs it locally; the Asset Bundle deploys it.
