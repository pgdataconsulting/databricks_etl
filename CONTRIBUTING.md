# Contributing

Bug reports, fixes and new operations are welcome. This page covers the setup
and what a change needs before it can be merged.

## Setup

You need Python 3.10 or newer and, for the tests, Java 17 or newer (the tests
run real Spark with Delta Lake; nothing is mocked).

```bash
python -m venv .venv && source .venv/bin/activate
make install        # the package with development dependencies
make hooks          # optional: lint and config validation before each commit
make check          # lint, types, config validation, tests
```

`make help` lists every task. `make demo` runs the medallion example end to
end on a local Spark session.

## What a change needs

- **Tests.** New behaviour comes with a test that fails without it. Operations
  are tested against a real local Spark session through the `spark` fixture.
- **`make check` passes.** CI runs the same checks, plus the tests on several
  Python and Spark versions.
- **Docs.** User-visible behaviour is described in
  [docs/usage.md](docs/usage.md), and an entry goes under a new heading at the
  top of [CHANGELOG.md](CHANGELOG.md).
- **Regenerated pipeline docs** if you touched a file under `configs/`:
  `make pipeline-docs`. CI fails when they are stale.

## Adding an operation

1. Subclass `etl_framework.operations.base.Operation`: set `type_name` and
   `required_fields`, implement `execute`, and override `validate` to reject
   bad configuration before anything runs. Import PySpark inside `execute`,
   not at module level, so configurations still validate without Spark.
2. Register it in `etl_framework/operations/__init__.py`.
3. Add tests for both validation and execution, and a row in the operations
   table in `docs/usage.md`.

An operation that is specific to one organization does not have to live in
this repository: ship it in your own package and declare it under the
`etl_framework.operations` entry-point group (see the user guide).

## Infrastructure and deployment files

- Terraform: `make tf-check` (formatting and `terraform validate`; no cloud
  credentials needed).
- Asset Bundle: `make bundle-schema` checks `databricks.yml` and
  `resources/` against the schema of the installed Databricks CLI.

## Pull requests

Branch from `main`, keep a pull request to one concern, and describe what
changed and how you checked it. Commit messages are written in the imperative
("Add …", "Fix …") with the reasoning in the body when it is not obvious.

## Security issues

Do not open a public issue for a vulnerability. See [SECURITY.md](SECURITY.md).
