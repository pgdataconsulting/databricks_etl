# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). The version covers
the Python API, the command line, and the YAML specification (`spec_version`).

## [1.0.0] - 2026-11-15

First public release.

### Added

- **Secret references.** `${secret:scope/key}` in steps and lineage settings,
  resolved only when the step that uses them runs. Values never reach the
  dry-run plan, lineage, run summary or audit tables, and are masked in logs
  and in errors that echo them. Providers: Databricks secret scopes,
  environment variables, or any object with `get(scope, key)`.
- **`etl-framework run`.** Runs a pipeline from the command line and is the
  entry point of the Databricks job (`python_wheel_task`). Exit code 1 means a
  step failed, 2 means the configuration or the command line was rejected
  before anything ran. The summary goes to stdout as one JSON document and
  log lines to stderr; `--local` runs on a local Delta-enabled session.
- **Operation plugins.** Installed packages can contribute operation types
  through the `etl_framework.operations` entry-point group.
- **Parameterized settings.** `${parameter}` placeholders are resolved in
  `settings`, so the audit schema and the watermark table can be
  catalog-qualified per environment. Switches accept the words `true` and
  `false`, which is what a job parameter delivers.
- **Quarantine to a table.** `quarantine_table` on `quality` steps, alongside
  `quarantine_path`.
- **Medallion example** (`configs/medallion`) that runs locally
  (`make demo`) and is deployed unchanged by the bundle.
- **Deployment.** A Databricks Asset Bundle for the jobs, Terraform for the
  Azure platform, Unity Catalog, identities and grants, and GitHub Actions
  for CI, release and deployment with OIDC federation (no stored secrets).
- `etl_framework.local.local_spark_session` for local development and tests.
- Type information (`py.typed`), `--version`, console scripts, and the `spark`
  and `dev` extras.

### Changed

- Python 3.10 or newer is required.
- PySpark is imported only by the code that executes steps. Validating and
  documenting configurations works with just the package installed.
- The audit and watermark schemas are created only when they do not exist,
  so no DDL is issued where they are provisioned ahead of time.
- `schemas/` and `templates/` are found up to two levels above the
  configuration file, so configurations can be grouped in subdirectories.
- A configuration with watermarking is validated completely before the
  watermark table is read, so a rejected configuration touches no state.
- An interrupted run (Ctrl-C, a cancelled job) is logged and audited as
  failed instead of succeeded.
- Parameters whose names look like credentials are masked in run summaries.
- Generated pipeline documentation renders settings as YAML values.

### Fixed

- The `on:` field of a `lookup` step could not be written in a YAML file:
  YAML 1.1 reads the bare word `on` as a boolean. Mapping keys now keep
  their text.
- A pipeline without a `process_name` and with watermarking enabled read
  (and could create) the watermark table before being rejected.
- Spark started its Python workers with a different interpreter than the
  tests when run inside a virtualenv.

## [0.1.0]

Development version, not published: the declarative engine with 23 operation
types, schema contracts, data quality checks, templates, environment
overrides, lineage, audit logging, nested pipelines and watermarks.

[1.0.0]: https://github.com/pgdataconsulting/databricks_etl/releases/tag/v1.0.0
