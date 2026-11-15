"""Validates a pipeline configuration before any step is executed.

Collects every problem it can find and raises a single
:class:`~etl_framework.exceptions.ValidationError`, so invalid YAML stops the
process before any write operation runs.
"""

from etl_framework import SUPPORTED_SPEC_VERSIONS
from etl_framework.environment_overrides import validate_environments_block
from etl_framework.exceptions import SchemaContractError, ValidationError
from etl_framework.operations import get_operation_class, supported_operation_types
from etl_framework.operations.base import AUDIT_COLUMN_BUILDERS
from etl_framework.secrets import contains_secret_reference, find_malformed_references
from etl_framework.settings import VALIDATION_PLACEHOLDER, boolean_setting_errors
from etl_framework.watermark import validate_watermark_settings

# The only settings in which a ${secret:...} reference is resolved.
_SECRET_AWARE_LINEAGE_SETTINGS = ("url", "api_key")


def _malformed_secret_errors(obj) -> list:
    return [
        f"malformed secret reference '{reference}'. Expected ${{secret:<scope>/<key>}}"
        for reference in find_malformed_references(obj)
    ]


def _unresolvable_secret_errors(settings: dict) -> list:
    """Secret references in settings that nothing would ever resolve.

    Only ``settings.lineage.url`` and ``api_key`` are resolved (when an event
    is sent). Anywhere else the reference would be used as literal text, for
    example as the name of the audit schema.
    """
    errors = []
    for name, value in settings.items():
        if name == "lineage" and isinstance(value, dict):
            value = {k: v for k, v in value.items() if k not in _SECRET_AWARE_LINEAGE_SETTINGS}
        if contains_secret_reference(value):
            errors.append(
                f"settings.{name} contains a secret reference, which is not resolved "
                f"there. Secret references work in steps and in settings.lineage.url "
                f"and settings.lineage.api_key"
            )
    return errors


def _validate_settings(settings: dict) -> list:
    errors = validate_watermark_settings(settings)
    errors.extend(f"settings: {e}" for e in _malformed_secret_errors(settings))
    errors.extend(_unresolvable_secret_errors(settings))
    errors.extend(boolean_setting_errors(settings))
    audit_columns = settings.get("audit_columns")
    if audit_columns in (None, True, False) or audit_columns == VALIDATION_PLACEHOLDER:
        return errors
    allowed = ", ".join(AUDIT_COLUMN_BUILDERS)
    if not isinstance(audit_columns, list):
        errors.append(f"settings.audit_columns must be true, false, or a list of: {allowed}")
        return errors
    unknown = [c for c in audit_columns if c not in AUDIT_COLUMN_BUILDERS]
    if unknown:
        errors.append(
            f"settings.audit_columns contains unknown column(s) "
            f"{', '.join(map(str, unknown))}. Available columns: {allowed}"
        )
    return errors


def _validate_schema_ref(step: dict, schema_registry) -> list:
    schema_ref = step.get("schema_ref")
    if not schema_ref:
        return []
    if schema_registry is None:
        return [
            f"schema_ref '{schema_ref}' is used but no schema base path is "
            f"configured (pass schema_base_path to run_pipeline, set "
            f"settings.schema_base_path, or keep a 'schemas' directory next "
            f"to the config file)"
        ]
    try:
        schema_registry.get(schema_ref)
    except SchemaContractError as exc:
        return [str(exc)]
    return []


def validate_config(config: dict, schema_registry=None, initial_inputs=None) -> None:
    """Validate a fully parameter-resolved configuration. Raises on failure.

    ``initial_inputs`` names DataFrames registered in the context before the
    first step runs — a nested pipeline receives the DataFrames its parent
    hands over, so its steps may consume them without a producing step.
    """
    errors = []
    process_name = config.get("process_name") or "<unknown>"

    if not config.get("process_name"):
        errors.append("top-level field 'process_name' is required")

    spec_version = config.get("spec_version")
    if spec_version is not None and str(spec_version) not in SUPPORTED_SPEC_VERSIONS:
        errors.append(
            f"unsupported spec_version '{spec_version}'. "
            f"Supported versions: {', '.join(SUPPORTED_SPEC_VERSIONS)}"
        )

    steps = config.get("steps")
    if not isinstance(steps, list) or not steps:
        errors.append("top-level field 'steps' must be a non-empty list")
        raise ValidationError(process_name, errors)

    settings = config.get("settings") or {}
    errors.extend(_validate_settings(settings))
    errors.extend(validate_environments_block(config))

    # A config meant to run nested (via a run_config step) can declare the
    # DataFrames its caller must hand over, so it also validates standalone.
    declared_inputs = config.get("inputs") or []
    if not isinstance(declared_inputs, list) or not all(
        isinstance(name, str) and name for name in declared_inputs
    ):
        errors.append("top-level 'inputs' must be a list of DataFrame names")
        declared_inputs = []

    seen_names = set()
    available_outputs = set(declared_inputs) | set(initial_inputs or ())

    for index, step in enumerate(steps):
        position = f"step #{index + 1}"
        if not isinstance(step, dict):
            errors.append(f"{position}: each step must be a mapping")
            continue

        name = step.get("name")
        if not name:
            errors.append(f"{position}: required field 'name' is missing")
            name = position
        elif name in seen_names:
            errors.append(f"{position} ('{name}'): step names must be unique")
        seen_names.add(name)
        label = f"step '{name}'"

        step_type = step.get("type")
        if not step_type:
            errors.append(f"{label}: required field 'type' is missing")
            continue

        operation_class = get_operation_class(step_type)
        if operation_class is None:
            errors.append(
                f"{label}: unsupported operation type '{step_type}'. "
                f"Supported types: {', '.join(supported_operation_types())}"
            )
            continue

        if step_type == "sql" and not settings.get("allow_sql"):
            errors.append(
                f"{label}: 'sql' steps are disabled. Enable them explicitly "
                f"with 'settings.allow_sql: true'"
            )

        errors.extend(f"{label}: {e}" for e in operation_class.validate(step))
        errors.extend(f"{label}: {e}" for e in _validate_schema_ref(step, schema_registry))
        errors.extend(f"{label}: {e}" for e in _malformed_secret_errors(step))
        if contains_secret_reference(step.get("when")):
            errors.append(
                f"{label}: 'when' cannot use a secret reference. The condition is "
                f"evaluated before the step's secrets are resolved"
            )

        for input_name in operation_class.input_names(step):
            if input_name not in available_outputs:
                errors.append(
                    f"{label}: input DataFrame '{input_name}' does not reference "
                    f"the output of any previous step"
                )

        for output in operation_class.output_names(step):
            if output in available_outputs and not step.get("allow_overwrite"):
                errors.append(
                    f"{label}: output '{output}' would overwrite an existing "
                    f"DataFrame. Set 'allow_overwrite: true' if intentional"
                )
            available_outputs.add(output)

    watermark = settings.get("watermark")
    if isinstance(watermark, dict) and watermark.get("enabled"):
        source = watermark.get("source")
        if source and source not in available_outputs:
            errors.append(
                f"settings.watermark.source '{source}' does not reference the output of any step"
            )

    if errors:
        raise ValidationError(process_name, errors)
