"""Command line interface: validate, document, and run pipeline configurations.

``validate`` and ``docs`` need neither PySpark nor a Databricks connection,
so they are usable in CI and pre-commit hooks with only the package itself
installed::

    etl-framework validate configs/*.yaml
    etl-framework docs configs/customer_orders.yaml -o docs/pipelines/customer_orders.md

``run`` executes a pipeline and is the entry point of the Databricks job
(a ``python_wheel_task``)::

    etl-framework run configs/customer_orders.yaml --param business_date=2026-07-07

It uses the active Spark session, which on Databricks is the cluster's. Off
Databricks, add ``--local`` to run on a local session with Delta Lake
configured (needs the ``spark`` extra and Java).

The run summary is printed to stdout as one JSON document (or written to
``--output``); log lines go to stderr. Exit codes of ``run``: 0 the pipeline
succeeded, 1 a step failed, 2 the configuration or the command line was
rejected before anything executed.

The same commands are available as ``python -m etl_framework``.
"""

import argparse
import json
import sys
from typing import NoReturn

from etl_framework import __version__
from etl_framework.config_loader import load_config
from etl_framework.config_validator import validate_config
from etl_framework.environment_overrides import (
    apply_environment_overrides,
    select_environment,
)
from etl_framework.exceptions import ConfigError, ExecutionError
from etl_framework.logging import log_to_stderr
from etl_framework.operations import OPERATION_REGISTRY
from etl_framework.parameter_resolver import (
    collect_parameter_references,
    merge_parameters,
    resolve_config,
)
from etl_framework.runner import (
    _resolve_schema_base_path,
    _resolve_template_base_path,
    run_pipeline,
)
from etl_framework.schema_registry import SchemaRegistry
from etl_framework.settings import VALIDATION_PLACEHOLDER
from etl_framework.template_loader import expand_templates

# Kept under its old name for callers that imported it from here.
PLACEHOLDER = VALIDATION_PLACEHOLDER


def _usage_error(message: str) -> NoReturn:
    """Reject the command line the way argparse does: message on stderr, exit 2."""
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(2)


def _parse_params(pairs: list) -> dict:
    parameters = {}
    for pair in pairs or []:
        if "=" not in pair:
            _usage_error(f"invalid --param '{pair}', expected name=value")
        name, value = pair.split("=", 1)
        parameters[name] = value
    return parameters


def validate_command(
    config_paths: list, schema_base_path: str | None = None, params: dict | None = None
) -> int:
    failed = 0
    for config_path in config_paths:
        try:
            config = load_config(config_path=config_path)
            template_base = _resolve_template_base_path(
                None, config.get("settings") or {}, config_path
            )
            config = expand_templates(config, template_base)
            config = apply_environment_overrides(config, select_environment(config, params or {}))
            # A parameter with no value yet (required at runtime, not passed
            # here) gets a placeholder. Declared defaults are used as they are.
            parameters = dict(params or {})
            known = merge_parameters(config.get("parameters") or {}, parameters)
            referenced = collect_parameter_references(config.get("steps") or [])
            referenced |= collect_parameter_references(config.get("settings") or {})
            for name in referenced:
                if known.get(name) is None:
                    parameters[name] = VALIDATION_PLACEHOLDER
            resolved, _ = resolve_config(config, parameters)

            settings = resolved.get("settings") or {}
            base = _resolve_schema_base_path(schema_base_path, settings, config_path)
            registry = SchemaRegistry(base) if base else None
            validate_config(resolved, schema_registry=registry)
        except ConfigError as exc:
            failed += 1
            print(f"FAIL {config_path}\n{exc}\n")
        else:
            print(f"OK   {config_path}")
    if failed:
        print(f"{failed} of {len(config_paths)} configuration(s) invalid")
    return 1 if failed else 0


def _render_value(value) -> str:
    """Render a setting the way it is written in YAML flow style, not as a Python repr."""
    return value if isinstance(value, str) else json.dumps(value)


def _step_details(step: dict) -> str:
    details = []
    for field in (
        "format",
        "mode",
        "path",
        "table",
        "config_path",
        "schema_ref",
        "condition",
        "join_type",
        "when",
        "on_failure",
        "quarantine_path",
        "quarantine_table",
    ):
        if step.get(field):
            details.append(f"`{field}: {step[field]}`")
    return "<br>".join(details)


def _mermaid_diagram(steps: list) -> str:
    lines = ["```mermaid", "flowchart TD"]
    for index, step in enumerate(steps):
        operation_class = OPERATION_REGISTRY.get(step.get("type"))
        if operation_class is None:
            continue
        node = f"step{index}"
        lines.append(f'    {node}["{step.get("name")}<br><i>{step.get("type")}</i>"]')
        for input_name in operation_class.input_names(step):
            lines.append(f"    {input_name}(({input_name})) --> {node}")
        outputs = operation_class.output_names(step)
        for output in outputs:
            lines.append(f"    {node} --> {output}(({output}))")
        target = step.get("path") or step.get("table")
        if not outputs and target:
            lines.append(f'    {node} --> sink{index}[("{target}")]')
    lines.append("```")
    return "\n".join(lines)


def docs_command(config_path: str, output_path: str | None = None) -> int:
    config = load_config(config_path=config_path)
    template_base = _resolve_template_base_path(None, config.get("settings") or {}, config_path)
    config = expand_templates(config, template_base)
    steps = config.get("steps") or []

    lines = [f"# Pipeline: {config.get('process_name', '(unnamed)')}", ""]
    if config.get("spec_version"):
        lines.append(f"Spec version: `{config['spec_version']}`")
        lines.append("")
    lines.append(f"Source configuration: `{config_path}`")
    lines.append("")

    parameters = config.get("parameters") or {}
    if parameters:
        lines += ["## Parameters", "", "| Name | Default |", "| --- | --- |"]
        for name, default in parameters.items():
            rendered = "`(required at runtime)`" if default is None else f"`{default}`"
            lines.append(f"| `{name}` | {rendered} |")
        lines.append("")

    settings = config.get("settings") or {}
    if settings:
        lines += ["## Settings", ""]
        lines += [f"- `{key}`: `{_render_value(value)}`" for key, value in settings.items()]
        lines.append("")

    lines += [
        "## Steps",
        "",
        "| # | Step | Type | Inputs | Output | Details |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for index, step in enumerate(steps, start=1):
        operation_class = OPERATION_REGISTRY.get(step.get("type"))
        inputs = ", ".join(operation_class.input_names(step)) if operation_class else "?"
        output = ", ".join(operation_class.output_names(step)) if operation_class else "?"
        lines.append(
            f"| {index} | `{step.get('name')}` | {step.get('type')} "
            f"| {inputs} | {output} | {_step_details(step)} |"
        )
    lines += ["", "## Lineage", "", _mermaid_diagram(steps), ""]

    document = "\n".join(lines)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(document)
        print(f"wrote {output_path}")
    else:
        print(document)
    return 0


def _load_params_json(text: str) -> dict:
    if not text:
        return {}
    try:
        parameters = json.loads(text)
    except ValueError as exc:
        _usage_error(f"invalid --params-json: {exc}")
    if not isinstance(parameters, dict):
        _usage_error("invalid --params-json: expected a JSON object")
    return parameters


def run_command(
    config_path: str,
    parameters: dict,
    run_id: str | None = None,
    schema_base_path: str | None = None,
    template_base_path: str | None = None,
    dry_run: bool = False,
    output_path: str | None = None,
    local_warehouse: str | None = None,
) -> int:
    try:
        spark = None
        if local_warehouse and not dry_run:
            from etl_framework.local import local_spark_session

            spark = local_spark_session(local_warehouse)
        with log_to_stderr():
            summary = run_pipeline(
                config_path=config_path,
                parameters=parameters,
                spark=spark,
                run_id=run_id,
                schema_base_path=schema_base_path,
                template_base_path=template_base_path,
                dry_run=dry_run,
            )
    except ConfigError as exc:
        print(f"Configuration rejected, nothing was executed.\n{exc}", file=sys.stderr)
        return 2
    except ExecutionError as exc:
        print(f"Pipeline failed.\n{exc}", file=sys.stderr)
        return 1

    document = json.dumps(summary, indent=2, default=str)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(document + "\n")
        print(f"{summary['process_name']}: {summary['status']} (summary written to {output_path})")
    else:
        print(document)
    return 0


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="etl-framework",
        description="Validate, document, and run YAML pipeline configurations.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run", help="Run a pipeline (exit code 1: a step failed, 2: invalid config)"
    )
    run_parser.add_argument("config", help="YAML configuration file")
    run_parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Runtime parameter (repeatable)",
    )
    run_parser.add_argument(
        "--params-json",
        default=None,
        metavar="JSON",
        help="Runtime parameters as a JSON object; --param values take precedence",
    )
    run_parser.add_argument(
        "--run-id", default=None, help="Correlation id, e.g. the orchestrator's run id"
    )
    run_parser.add_argument(
        "--schema-base-path", default=None, help="Directory containing schema contracts"
    )
    run_parser.add_argument(
        "--template-base-path", default=None, help="Directory containing step templates"
    )
    run_parser.add_argument(
        "--dry-run", action="store_true", help="Validate and print the plan without executing"
    )
    run_parser.add_argument(
        "-o", "--output", default=None, help="Write the run summary to a file (default: stdout)"
    )
    run_parser.add_argument(
        "--local",
        nargs="?",
        const="spark-warehouse",
        default=None,
        metavar="WAREHOUSE_DIR",
        help="Run off Databricks, on a local Spark session with Delta Lake; managed "
        "tables are stored in WAREHOUSE_DIR (default: ./spark-warehouse)",
    )

    validate_parser = subparsers.add_parser(
        "validate", help="Validate configurations (fails with exit code 1)"
    )
    validate_parser.add_argument("configs", nargs="+", help="YAML configuration files")
    validate_parser.add_argument(
        "--schema-base-path", default=None, help="Directory containing schema contracts"
    )
    validate_parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Runtime parameter (repeatable); unset parameters get placeholders",
    )

    docs_parser = subparsers.add_parser(
        "docs", help="Generate Markdown documentation for a configuration"
    )
    docs_parser.add_argument("config", help="YAML configuration file")
    docs_parser.add_argument("-o", "--output", default=None, help="Output file (default: stdout)")

    args = parser.parse_args(argv)
    if args.command == "run":
        parameters = _load_params_json(args.params_json)
        parameters.update(_parse_params(args.param))
        return run_command(
            args.config,
            parameters,
            run_id=args.run_id,
            schema_base_path=args.schema_base_path,
            template_base_path=args.template_base_path,
            dry_run=args.dry_run,
            output_path=args.output,
            local_warehouse=args.local,
        )
    if args.command == "validate":
        return validate_command(args.configs, args.schema_base_path, _parse_params(args.param))
    return docs_command(args.config, args.output)


def entrypoint() -> None:
    """Console-script and Databricks ``python_wheel_task`` entry point.

    A wheel task calls this function directly rather than through a shell, so
    a failure has to be raised: returning a non-zero code would be ignored.
    """
    code = main()
    if code:
        sys.exit(code)


if __name__ == "__main__":
    entrypoint()
