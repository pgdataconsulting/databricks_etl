"""Pipeline runner: the generic execution entry point.

``run_pipeline`` loads the YAML configuration, resolves runtime parameters,
validates the configuration, and executes each step as PySpark. It returns a
serializable execution summary that a Databricks notebook can hand back to
Azure Data Factory via ``dbutils.notebook.exit``.
"""

import logging
import os
import time
import uuid
from datetime import datetime, timezone

from etl_framework.config_loader import _to_local_path, load_config
from etl_framework.config_validator import validate_config
from etl_framework.environment_overrides import (
    apply_environment_overrides,
    select_environment,
)
from etl_framework.exceptions import (
    ConfigLoadError,
    ExecutionError,
    SecretResolutionError,
    StepExecutionError,
    ValidationError,
)
from etl_framework.execution_context import ExecutionContext
from etl_framework.lineage import OpenLineageEmitter, extract_lineage
from etl_framework.logging import AuditLogger, StepLogger
from etl_framework.operations import OPERATION_REGISTRY
from etl_framework.parameter_resolver import resolve_config, resolve_settings_block
from etl_framework.schema_registry import SchemaRegistry
from etl_framework.secrets import SecretResolver, contains_secret_reference, mask_parameters
from etl_framework.template_loader import expand_templates
from etl_framework.watermark import (
    DEFAULT_PARAMETER,
    WatermarkStore,
    validate_watermark_settings,
)

# Hard ceiling for run_config nesting: deep chains are almost certainly a
# configuration mistake, and inline configs cannot be cycle-checked by path.
MAX_NESTED_DEPTH = 10

# How far above the config file to look for the schemas/ and templates/
# directories: the config's own directory plus this many parents. Two covers
# configs grouped one level deep (configs/<area>/x.yaml next to schemas/)
# without wandering off into unrelated folders higher up.
MAX_BASE_PATH_PARENTS = 2


class _RedactingFilter(logging.Filter):
    """Masks resolved secret values in every record of the framework logger."""

    def __init__(self, secrets: SecretResolver):
        super().__init__()
        self._secrets = secrets

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = self._secrets.redact(message)
        if redacted != message:
            record.msg, record.args = redacted, ()
        return True


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _check_nesting(config_path, config_stack: tuple) -> str:
    """Guard nested runs against cycles and runaway depth.

    Returns the entry to record for this pipeline on the config stack.
    """
    local_path = os.path.abspath(_to_local_path(config_path)) if config_path else None
    if local_path and local_path in config_stack:
        chain = " -> ".join((*config_stack, local_path))
        raise ConfigLoadError(config_path, f"circular nested pipeline reference: {chain}")
    if len(config_stack) > MAX_NESTED_DEPTH:
        raise ConfigLoadError(
            config_path or "<inline>",
            f"nested pipelines exceed the maximum depth of {MAX_NESTED_DEPTH}",
        )
    return local_path or "<inline>"


def _resolve_base_path(explicit, settings_value, config_path, directory_name):
    """Locate an auxiliary directory (schemas/, templates/) for a config.

    Precedence: explicit run_pipeline argument, then the settings value, then
    the nearest conventionally-named directory at or above the config file
    (the repo layout of configs/, schemas/, and templates/ side by side, with
    configs optionally grouped into subdirectories).
    """
    if explicit:
        return _to_local_path(explicit)
    if settings_value:
        return _to_local_path(settings_value)
    if config_path:
        current = os.path.dirname(os.path.abspath(_to_local_path(config_path)))
        for _ in range(MAX_BASE_PATH_PARENTS + 1):
            candidate = os.path.join(current, directory_name)
            if os.path.isdir(candidate):
                return candidate
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent
    return None


def _resolve_schema_base_path(schema_base_path, settings: dict, config_path):
    return _resolve_base_path(
        schema_base_path, settings.get("schema_base_path"), config_path, "schemas"
    )


def _resolve_template_base_path(template_base_path, settings: dict, config_path):
    return _resolve_base_path(
        template_base_path, settings.get("template_base_path"), config_path, "templates"
    )


def _get_spark(spark):
    if spark is not None:
        return spark
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def _build_metrics(step_metadata: list, total_duration: float) -> dict:
    """Per-step and run-level performance metrics for the run summary.

    Step duration is the primary cost driver on job clusters (DBUs scale with
    wall-clock time), so the breakdown highlights where the time went.
    """
    steps = []
    for entry in step_metadata:
        if entry.get("status") != "succeeded":
            continue
        duration = entry.get("duration_seconds") or 0.0
        rows = entry.get("rows_processed")
        steps.append(
            {
                "step_name": entry["step_name"],
                "step_type": entry["step_type"],
                "duration_seconds": duration,
                "pct_of_total": round(100.0 * duration / total_duration, 1)
                if total_duration
                else 0.0,
                "rows_processed": rows,
                "rows_per_second": round(rows / duration, 1)
                if rows is not None and duration
                else None,
            }
        )
    steps.sort(key=lambda s: s["duration_seconds"], reverse=True)
    rows_counted = [s["rows_processed"] for s in steps if s["rows_processed"] is not None]
    return {
        "total_duration_seconds": round(total_duration, 3),
        "steps_executed": len(steps),
        "slowest_step": steps[0]["step_name"] if steps else None,
        "total_rows_processed": sum(rows_counted) if rows_counted else None,
        "steps": steps,
    }


def _build_plan(resolved_config: dict) -> list:
    """Describe every step without executing anything (dry-run output)."""
    plan = []
    for step in resolved_config["steps"]:
        operation_class = OPERATION_REGISTRY[step["type"]]
        plan.append(
            {
                "step_name": step["name"],
                "step_type": step["type"],
                "inputs": operation_class.input_names(step),
                "output": operation_class.output_name(step),
                "target": step.get("path") or step.get("table") or step.get("config_path"),
                "schema_ref": step.get("schema_ref"),
                "conditional": "when" in step,
            }
        )
    return plan


def _advance_watermark(context, store, settings_block: dict, process_name: str):
    """Persist the new high-water mark once every step has succeeded.

    Returns the new value, or None when the increment was empty (the stored
    watermark is left untouched so nothing is skipped next run).
    """
    if store is None:
        return None
    source, column = settings_block["source"], settings_block["column"]
    if not context.has_dataframe(source):
        raise ExecutionError(
            f"Pipeline '{process_name}': watermark source DataFrame '{source}' "
            f"was never registered (was its step skipped?), so the watermark "
            f"cannot be advanced"
        )
    try:
        row = (
            context.get_dataframe(source)
            .selectExpr(f"CAST(MAX({column}) AS STRING) AS value")
            .collect()[0]
        )
    except Exception as exc:
        # This is an extra Spark action over the source DataFrame, so it can
        # fail where the steps themselves, being lazy, did not.
        raise ExecutionError(
            f"Pipeline '{process_name}': failed to compute the new watermark "
            f"(MAX({column}) of '{source}'): {type(exc).__name__}: {exc}"
        ) from exc
    if row.value is not None:
        store.set(process_name, row.value)
    return row.value


def _should_run(context, step: dict) -> bool:
    """Evaluate an optional `when:` condition (a SQL boolean expression)."""
    condition = step.get("when")
    if condition is None:
        return True
    try:
        row = context.spark.sql(f"SELECT CAST(({condition}) AS BOOLEAN) AS result").collect()[0]
    except Exception as exc:
        raise StepExecutionError(
            process_name=context.process_name,
            step_name=step["name"],
            step_type=step["type"],
            description=f"failed to evaluate 'when' condition: {condition}",
            original=exc,
        ) from exc
    return bool(row.result)


def _step_failure(context, step: dict, exc: Exception, uses_secrets: bool) -> Exception:
    """Decide what a failed step raises.

    A failure is sensitive when the step ran with resolved secrets, or when a
    resolved value shows up anywhere in the exception chain (Spark is lazy: a
    later step can fail and quote the connection options of an earlier one).
    A sensitive failure keeps the error's type and message, masked, and
    nothing else: no chained exception and no traceback, because the chain
    can quote the value and the traceback's frames hold the resolved step.
    """
    secrets = context.secrets
    sensitive = uses_secrets or secrets.leaks(exc)

    # Errors the operations raise already name the pipeline and the step.
    names_the_step = isinstance(exc, ExecutionError) and not (
        isinstance(exc, SecretResolutionError) or type(exc) is ExecutionError
    )
    if names_the_step:
        if sensitive:
            secrets.scrub_exception(exc)
        return exc

    if isinstance(exc, ExecutionError):
        description, original = str(exc), None
    elif sensitive:
        description = f"unexpected failure while executing the step: {type(exc).__name__}: {exc}"
        original = None
    else:
        description, original = "unexpected failure while executing the step", exc
    failure = StepExecutionError(
        process_name=context.process_name,
        step_name=step["name"],
        step_type=step["type"],
        description=secrets.redact(description),
        original=original,
    )
    if not sensitive:
        failure.__cause__ = exc
    return failure


def _execute_step(context, step, operation_class, step_logger, audit_logger, count_rows: bool):
    operation = operation_class()
    inputs = operation_class.input_names(step)
    output = operation_class.output_name(step)
    context.current_step = step
    step_logger.step_started(step["name"], step["type"], inputs)
    start_time = _utc_now()
    started = time.monotonic()

    retry_spec = step.get("retry") or {}
    max_attempts = int(retry_spec.get("attempts", 1))
    delay = float(retry_spec.get("delay_seconds", 0))
    backoff = float(retry_spec.get("backoff", 2))

    uses_secrets = operation_class.resolves_secrets and contains_secret_reference(step)
    attempt = 0
    failure = None
    runnable_step = None
    try:
        while True:
            attempt += 1
            try:
                # Secret references are resolved here and nowhere earlier, so
                # the values exist only in the step handed to the operation.
                # Everything reported about the step (logs, audit, summary)
                # uses `step`, which still holds the references. Resolving
                # inside the attempt lets `retry` cover a secret store that is
                # briefly unavailable.
                runnable_step = context.secrets.resolve(step) if uses_secrets else step
                result_df = operation.execute(context, runnable_step)
                rows_processed = None
                if count_rows and result_df is not None:
                    # count() forces evaluation, so lazy Spark errors surface
                    # here inside the step that caused them.
                    rows_processed = result_df.count()
                break
            except Exception as exc:  # noqa: BLE001 - re-raised below as `failure`
                if attempt >= max_attempts:
                    failure = _step_failure(context, step, exc, uses_secrets)
                    break
                step_logger.step_retrying(
                    step["name"],
                    step["type"],
                    attempt,
                    max_attempts,
                    delay,
                    str(exc),
                )
                if delay > 0:
                    time.sleep(delay)
                delay *= backoff
        if failure is not None:
            # Raised outside the handler, so the exception that was being
            # handled does not become the context of the one that leaves.
            raise failure
    finally:
        context.current_step = None
        # This frame stays reachable from a traceback, and tools that report
        # frame locals would show the resolved step. Do not leave it here.
        runnable_step = None

    duration = time.monotonic() - started
    step_logger.step_finished(
        step["name"],
        step["type"],
        inputs,
        output,
        status="succeeded",
        duration_seconds=duration,
        rows_processed=rows_processed,
    )
    step_metadata = {
        "step_name": step["name"],
        "step_type": step["type"],
        "inputs": inputs,
        "output": output,
        "status": "succeeded",
        "start_time": start_time,
        "end_time": _utc_now(),
        "duration_seconds": round(duration, 3),
        "rows_processed": rows_processed,
        "attempts": attempt,
    }
    context.audit["steps"].append(step_metadata)
    audit_logger.record_step(
        process_name=context.process_name,
        run_id=context.run_id,
        inputs=",".join(inputs),
        error=None,
        **{k: v for k, v in step_metadata.items() if k not in ("inputs", "attempts")},
    )


def _resolve_and_validate(
    config: dict,
    parameters: dict,
    schema_base_path,
    config_path,
    initial_dataframes,
    require_inputs: bool,
) -> tuple:
    """Resolve parameters and validate; nothing here touches Spark or state.

    Returns ``(resolved_config, merged_parameters, schema_registry)`` and
    raises a ``ConfigError`` for anything that makes the configuration
    unusable.
    """
    resolved_config, merged_parameters = resolve_config(config, parameters)
    settings = resolved_config.get("settings") or {}
    schema_base = _resolve_schema_base_path(schema_base_path, settings, config_path)
    schema_registry = SchemaRegistry(schema_base) if schema_base else None
    validate_config(
        resolved_config,
        schema_registry=schema_registry,
        initial_inputs=initial_dataframes,
    )
    if require_inputs:
        missing_inputs = [
            name
            for name in (resolved_config.get("inputs") or [])
            if name not in (initial_dataframes or {})
        ]
        if missing_inputs:
            raise ValidationError(
                resolved_config["process_name"],
                [
                    f"declared input DataFrame '{name}' was not provided. This "
                    f"pipeline is meant to run as a nested 'run_config' step whose "
                    f"'inputs' mapping supplies it"
                    for name in missing_inputs
                ],
            )
    return resolved_config, merged_parameters, schema_registry


def _run_failure(secrets: SecretResolver, exc: Exception) -> Exception:
    """What a failed run raises: the error itself, unless it would carry a secret.

    Step failures are already safe when they get here. This covers what
    happens around the steps: a ``when`` condition, the watermark action.
    """
    if not secrets.leaks(exc):
        return exc
    if isinstance(exc, ExecutionError):
        secrets.scrub_exception(exc)
        return exc
    return ExecutionError(secrets.redact(f"{type(exc).__name__}: {exc}"))


def run_pipeline(
    config_path: str | None = None,
    parameters: dict | None = None,
    config_content: str | None = None,
    spark=None,
    run_id: str | None = None,
    schema_base_path: str | None = None,
    template_base_path: str | None = None,
    dry_run: bool = False,
    secret_provider=None,
) -> dict:
    """Load, validate, and execute a YAML-defined pipeline.

    Args:
        config_path: Location of the YAML configuration (local, ``dbfs:/``,
            Workspace, or Volume path).
        parameters: Runtime parameters (e.g. from ADF or Databricks widgets).
            They override defaults declared in the YAML ``parameters`` block.
        config_content: Raw YAML text, as an alternative to ``config_path``.
        spark: An existing SparkSession. Defaults to the active session.
        run_id: External correlation id (e.g. the ADF run id). Auto-generated
            when omitted.
        schema_base_path: Directory containing schema contract files used by
            'schema_ref' step fields. Defaults to settings.schema_base_path,
            or the nearest 'schemas' directory at or above the config file.
        template_base_path: Directory containing step templates used by
            'use_template' entries. Defaults to settings.template_base_path,
            or the nearest 'templates' directory at or above the config file.
        dry_run: Load, resolve, and validate the configuration, then return
            the execution plan without creating a Spark session or executing
            any step. Nothing is read or written, and no secret is resolved.
        secret_provider: Where ``${secret:scope/key}`` references are
            resolved: any object with a ``get(scope, key)`` method. Defaults
            to Databricks secret scopes on Databricks compute and to
            environment variables elsewhere.

    Returns:
        A JSON-serializable summary of the run, including per-step metadata.

    Raises:
        ConfigError: The configuration could not be loaded, is invalid, or
            references missing parameters. Raised before any step executes
            and before any state (the watermark table) is read or created.
        ExecutionError: A step failed at runtime. If the failure involved a
            resolved secret, the error carries the masked message only: no
            chained exception and no traceback of the failing step.
    """
    summary, _ = _run_pipeline(
        config_path=config_path,
        parameters=parameters,
        config_content=config_content,
        spark=spark,
        run_id=run_id,
        schema_base_path=schema_base_path,
        template_base_path=template_base_path,
        dry_run=dry_run,
        secret_provider=secret_provider,
    )
    return summary


def _run_pipeline(
    config_path: str | None = None,
    parameters: dict | None = None,
    config_content: str | None = None,
    spark=None,
    run_id: str | None = None,
    schema_base_path: str | None = None,
    template_base_path: str | None = None,
    dry_run: bool = False,
    initial_dataframes: dict | None = None,
    config_stack: tuple = (),
    secret_provider=None,
    secret_resolver: SecretResolver | None = None,
) -> tuple:
    """Implementation of :func:`run_pipeline`; returns ``(summary, context)``.

    The extra arguments serve nested runs started by ``run_config`` steps:
    ``initial_dataframes`` are registered in the context before the first
    step (the DataFrames the parent hands over), ``config_stack`` is the
    chain of configs already running, used to detect circular nesting, and
    ``secret_resolver`` is the parent's resolver, shared so that a secret
    resolved anywhere in the run is masked everywhere. The context is
    returned so the parent can collect the child's DataFrames (``None`` for
    dry runs, which never build a context).
    """
    parameters = dict(parameters or {})
    run_id = run_id or str(uuid.uuid4())
    start_time = _utc_now()

    stack_entry = _check_nesting(config_path, config_stack)
    config = load_config(config_path=config_path, config_content=config_content)

    resolved_template_base = _resolve_template_base_path(
        template_base_path, config.get("settings") or {}, config_path
    )
    config = expand_templates(config, resolved_template_base)
    config = apply_environment_overrides(config, select_environment(config, parameters))

    # Built-in parameters, always available as ${run_id}, ${process_name},
    # and ${run_started_at} in steps. Explicit runtime parameters win.
    for name, value in {
        "run_id": run_id,
        "process_name": config.get("process_name"),
        "run_started_at": start_time,
    }.items():
        parameters.setdefault(name, value)

    # Incremental watermark: the stored value (or `initial` on the first run
    # and in dry runs, which never touch state) becomes a runtime parameter
    # before resolution, so steps can filter with it.
    watermark_settings = (config.get("settings") or {}).get("watermark") or {}
    if isinstance(watermark_settings, dict):
        # Resolved ahead of the rest of the configuration: whether it is
        # enabled and which state table it uses may be parameterized, and its
        # value decides the ${watermark} parameter the steps are resolved with.
        watermark_settings = resolve_settings_block(config, parameters, "watermark")
    else:
        watermark_settings = {}  # reported by validate_config below
    watermark_enabled = watermark_settings.get("enabled") is True
    watermark_store, previous_watermark = None, None
    if watermark_enabled:
        problems = validate_watermark_settings({"watermark": watermark_settings})
        declared_name = config.get("process_name") or ""
        if not declared_name:
            # The name keys the stored watermark.
            problems.append("top-level field 'process_name' is required")
        if problems:
            raise ValidationError(declared_name or "<unknown>", problems)
        watermark_parameter = watermark_settings.get("parameter", DEFAULT_PARAMETER)
        if dry_run:
            parameters.setdefault(watermark_parameter, watermark_settings["initial"])
        else:
            # Reading the stored watermark creates the state table on first
            # use. Validate the whole configuration first, with the initial
            # value standing in for the stored one, so that a configuration
            # that is going to be rejected never gets that far.
            provisional = dict(parameters)
            provisional.setdefault(watermark_parameter, watermark_settings["initial"])
            _resolve_and_validate(
                config,
                provisional,
                schema_base_path,
                config_path,
                initial_dataframes,
                require_inputs=True,
            )
            spark = _get_spark(spark)
            watermark_store = WatermarkStore(spark, watermark_settings)
            previous_watermark = watermark_store.get(declared_name)
            parameters.setdefault(
                watermark_parameter,
                previous_watermark
                if previous_watermark is not None
                else watermark_settings["initial"],
            )

    # Inputs declared at the top level must be provided by the caller (a
    # parent run_config step); dry runs and CLI validation skip that check so
    # nested fragments can still be validated standalone.
    resolved_config, merged_parameters, schema_registry = _resolve_and_validate(
        config,
        parameters,
        schema_base_path,
        config_path,
        initial_dataframes,
        require_inputs=not dry_run,
    )
    settings = resolved_config.get("settings") or {}

    process_name = resolved_config["process_name"]
    environment = merged_parameters.get("environment")
    audit_settings = settings.get("audit") or {}

    lineage = extract_lineage(resolved_config)

    if dry_run:
        return {
            "process_name": process_name,
            "run_id": run_id,
            "environment": environment,
            "config_path": config_path,
            "status": "dry_run",
            "parameters": mask_parameters(merged_parameters),
            "plan": _build_plan(resolved_config),
            "lineage": lineage,
        }, None

    spark = _get_spark(spark)
    # A nested run shares its parent's resolver; the outermost run owns it
    # and installs the log filter that masks whatever gets resolved.
    owns_secrets = secret_resolver is None
    secrets = secret_resolver or SecretResolver(provider=secret_provider, spark=spark)
    framework_logger = logging.getLogger("etl_framework")
    redacting_filter = _RedactingFilter(secrets) if owns_secrets else None

    lineage_emitter = OpenLineageEmitter(settings.get("lineage"), process_name, run_id, secrets)
    context = ExecutionContext(
        spark=spark,
        process_name=process_name,
        parameters=merged_parameters,
        run_id=run_id,
        environment=environment,
        schema_registry=schema_registry,
        settings=settings,
        config_path=config_path,
        config_stack=(*config_stack, stack_entry),
        secret_resolver=secrets,
    )
    for name, df in (initial_dataframes or {}).items():
        context.register_dataframe(name, df)
    step_logger = StepLogger(process_name, run_id, environment, config_path)
    audit_logger = AuditLogger(
        spark,
        enabled=bool(audit_settings.get("enabled")),
        schema=audit_settings.get("schema", "etl_audit"),
    )
    count_rows = bool(settings.get("log_row_counts"))

    if redacting_filter is not None:
        framework_logger.addFilter(redacting_filter)
    status, error_message = "succeeded", None
    run_started_monotonic = time.monotonic()

    failed_step, new_watermark = {}, None
    failure = None
    try:
        step_logger.run_started()
        lineage_emitter.emit("START", lineage)
        for step in resolved_config["steps"]:
            failed_step = step
            operation_class = OPERATION_REGISTRY[step["type"]]
            if not _should_run(context, step):
                step_logger.step_finished(
                    step["name"],
                    step["type"],
                    operation_class.input_names(step),
                    operation_class.output_name(step),
                    status="skipped",
                    duration_seconds=0.0,
                )
                context.audit["steps"].append(
                    {
                        "step_name": step["name"],
                        "step_type": step["type"],
                        "status": "skipped",
                        "when": step.get("when"),
                    }
                )
                continue
            _execute_step(context, step, operation_class, step_logger, audit_logger, count_rows)
        failed_step = {}
        new_watermark = _advance_watermark(
            context, watermark_store, watermark_settings, process_name
        )
    except Exception as exc:  # noqa: BLE001 - re-raised below as `failure`
        failure = _run_failure(secrets, exc)
        status, error_message = "failed", str(failure)
        audit_logger.record_error(
            process_name=process_name,
            run_id=run_id,
            step_name=failed_step.get("name"),
            step_type=failed_step.get("type"),
            error_type=type(failure).__name__,
            error=error_message,
            occurred_at=_utc_now(),
        )
    except BaseException:
        # Interrupted (Ctrl-C, a cancelled job run). Not a success.
        status, error_message = "failed", "the run was interrupted"
        raise
    finally:
        end_time = _utc_now()
        step_logger.run_finished(status, start_time, end_time, error_message)
        lineage_emitter.emit(
            "COMPLETE" if status == "succeeded" else "FAIL",
            lineage,
            error=error_message,
        )
        audit_logger.record_run(
            process_name=process_name,
            run_id=run_id,
            environment=environment,
            config_path=config_path,
            status=status,
            start_time=start_time,
            end_time=end_time,
            error=error_message,
        )
        if redacting_filter is not None:
            framework_logger.removeFilter(redacting_filter)

    if failure is not None:
        # Raised here rather than re-raised in the handler, so a failure that
        # had to be replaced by a masked one does not drag the original along
        # as its context.
        raise failure

    metrics = _build_metrics(context.audit["steps"], time.monotonic() - run_started_monotonic)
    step_logger.run_metrics(metrics)

    summary = {
        "process_name": process_name,
        "run_id": run_id,
        "environment": environment,
        "config_path": config_path,
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "parameters": mask_parameters(merged_parameters),
        "steps": context.audit["steps"],
        "lineage": lineage,
        "metrics": metrics,
    }
    if context.audit.get("nested_runs"):
        summary["nested_runs"] = context.audit["nested_runs"]
    if watermark_enabled:
        summary["watermark"] = {
            "previous": previous_watermark,
            "new": new_watermark if new_watermark is not None else previous_watermark,
        }
    # The summary is built from the configuration as written, so it carries
    # secret references, never values. It is deliberately not passed through
    # the masking used for logs and errors: that replaces text, and a pipeline
    # or table name must reach the orchestrator exactly as it is.
    return summary, context
