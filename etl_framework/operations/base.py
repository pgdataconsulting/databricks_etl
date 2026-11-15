"""Base class for all operation handlers.

Each YAML step type is implemented by one Operation subclass. Subclasses
declare the fields they require for validation, implement ``execute`` to run
the PySpark logic, and describe their inputs/outputs so the validator can
check DataFrame references before execution.
"""

from abc import ABC, abstractmethod

from etl_framework.exceptions import SchemaContractError, SchemaEnforcementError

# Standard audit columns appended to every sink's input when the pipeline
# sets `settings.audit_columns` (true = all of them, or a list of names).
AUDIT_COLUMN_BUILDERS = {
    "_run_id": lambda context: _lit_string(context.run_id),
    "_process_name": lambda context: _lit_string(context.process_name),
    "_environment": lambda context: _lit_string(context.environment),
    "_load_timestamp": lambda context: _current_timestamp(),
}


def _lit_string(value):
    from pyspark.sql.functions import lit

    return lit(value).cast("string")


def _current_timestamp():
    from pyspark.sql.functions import current_timestamp

    return current_timestamp()


def resolve_audit_column_names(audit_columns_setting) -> list:
    """Normalize the settings.audit_columns value into a list of column names."""
    if audit_columns_setting is True:
        return list(AUDIT_COLUMN_BUILDERS)
    if isinstance(audit_columns_setting, list):
        return list(audit_columns_setting)
    return []


def _validate_retry_spec(step: dict) -> list:
    """Validate the optional per-step `retry:` block (any operation type)."""
    retry = step.get("retry")
    if retry is None:
        return []
    if not isinstance(retry, dict):
        return ["'retry' must be a mapping (attempts, delay_seconds, backoff)"]
    errors = []
    attempts = retry.get("attempts")
    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 1:
        errors.append("'retry.attempts' must be a positive integer")
    for field, minimum in (("delay_seconds", 0), ("backoff", 1)):
        value = retry.get(field)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, (int, float)) or value < minimum
        ):
            errors.append(f"'retry.{field}' must be a number >= {minimum}")
    return errors


class Operation(ABC):
    # The YAML `type` value handled by this operation.
    type_name: str | None = None
    # Fields that must be present and non-empty on the step.
    required_fields: tuple[str, ...] = ()
    # Whether the step registers an output DataFrame in the context.
    produces_output = True
    # Whether ${secret:scope/key} references in the step are replaced with
    # their values before `execute` runs. Operations that only pass their
    # fields on (e.g. to a nested pipeline) leave them as references.
    resolves_secrets = True

    @abstractmethod
    def execute(self, context, step: dict):
        """Run the operation. Reads inputs from and registers outputs into the context."""

    @classmethod
    def validate(cls, step: dict) -> list:
        """Return a list of human-readable validation errors for this step."""
        errors = _validate_retry_spec(step)
        for field in cls.required_fields:
            value = step.get(field)
            if value is None or (isinstance(value, (str, list, dict)) and not value):
                errors.append(f"required field '{field}' is missing or empty")
        return errors

    @classmethod
    def input_names(cls, step: dict) -> list:
        """Names of context DataFrames this step consumes."""
        return [step["input"]] if step.get("input") else []

    @classmethod
    def output_name(cls, step: dict):
        """Name of the context DataFrame this step produces, if any."""
        return step.get("output") if cls.produces_output else None

    @classmethod
    def output_names(cls, step: dict) -> list:
        """Names of every context DataFrame this step produces.

        Most operations produce at most one output; steps that can register
        several (e.g. ``run_config``) override this.
        """
        output = cls.output_name(step)
        return [output] if output else []

    def apply_audit_columns(self, context, df):
        """Append the configured standard audit columns to a sink's input."""
        for name in resolve_audit_column_names(context.settings.get("audit_columns")):
            df = df.withColumn(name, AUDIT_COLUMN_BUILDERS[name](context))
        return df

    def get_schema_contract(self, context, step: dict):
        """Load the step's ``schema_ref`` contract, if declared."""
        schema_ref = step.get("schema_ref")
        if not schema_ref:
            return None
        if context.schema_registry is None:
            raise SchemaContractError(
                schema_ref,
                "no schema base path is configured (pass schema_base_path to "
                "run_pipeline or set settings.schema_base_path)",
            )
        return context.schema_registry.get(schema_ref)

    def enforce_schema_contract(self, context, step: dict, df, contract) -> None:
        """Fail the step if the DataFrame does not satisfy the contract."""
        problems = contract.validate_dataframe(
            df,
            strict=step.get("strict"),
            enforce_nullability=bool(context.settings.get("enforce_nullability")),
        )
        if problems:
            raise SchemaEnforcementError(
                process_name=context.process_name,
                step_name=step.get("name", "<unnamed>"),
                step_type=str(self.type_name),
                schema_ref=contract.ref,
                problems=problems,
            )
