"""Errors raised while executing pipeline steps.

Every execution error carries the pipeline name, step name, and step type so
failures surfaced in Databricks or Azure Data Factory point directly at the
offending YAML step.
"""


class ExecutionError(Exception):
    """Base class for all runtime execution errors."""


class SecretResolutionError(ExecutionError):
    """A ``${secret:scope/key}`` reference could not be resolved.

    The message names the scope and key only; it never contains a value.
    """

    def __init__(self, scope: str, key: str, reason: str):
        self.scope = scope
        self.key = key
        self.reason = reason
        super().__init__(f"Secret '{scope}/{key}' could not be resolved: {reason}")


class UnsupportedOperationError(ExecutionError):
    """A step declared an operation type the framework does not know."""

    def __init__(self, process_name: str, step_name: str, step_type: str, supported: list):
        self.process_name = process_name
        self.step_name = step_name
        self.step_type = step_type
        super().__init__(
            f"Pipeline '{process_name}', step '{step_name}': unsupported operation "
            f"type '{step_type}'. Supported types: {', '.join(sorted(supported))}"
        )


class MissingInputError(ExecutionError):
    """A step referenced a DataFrame that is not in the execution context."""

    def __init__(self, process_name: str, step_name: str, missing_input: str, available: list):
        self.process_name = process_name
        self.step_name = step_name
        self.missing_input = missing_input
        available = sorted(available)
        super().__init__(
            f"Pipeline '{process_name}', step '{step_name}': input DataFrame "
            f"'{missing_input}' does not exist in the execution context. "
            f"Available DataFrames: {', '.join(available) if available else '(none)'}"
        )


class StepExecutionError(ExecutionError):
    """A step failed while executing its PySpark logic."""

    def __init__(
        self,
        process_name: str,
        step_name: str,
        step_type: str,
        description: str,
        original: Exception | None = None,
    ):
        self.process_name = process_name
        self.step_name = step_name
        self.step_type = step_type
        self.original = original
        message = (
            f"Pipeline '{process_name}', step '{step_name}' (type '{step_type}') "
            f"failed: {description}"
        )
        if original is not None:
            message += f"\nOriginal exception: {type(original).__name__}: {original}"
        super().__init__(message)


class SchemaEnforcementError(StepExecutionError):
    """A DataFrame did not match its schema contract.

    Raised on read after loading the source, or on write before anything is
    written to the target.
    """

    def __init__(
        self, process_name: str, step_name: str, step_type: str, schema_ref: str, problems: list
    ):
        self.schema_ref = schema_ref
        self.problems = list(problems)
        details = "\n".join(f"  - {p}" for p in self.problems)
        description = (
            f"schema enforcement failed against contract '{schema_ref}' "
            f"({len(self.problems)} problem(s)):\n{details}"
        )
        super().__init__(process_name, step_name, step_type, description)


class QualityCheckError(StepExecutionError):
    """One or more data quality checks failed with on_failure: fail."""

    def __init__(
        self, process_name: str, step_name: str, step_type: str, input_name: str, failures: list
    ):
        self.input_name = input_name
        self.failures = list(failures)
        details = "\n".join(f"  - {f}" for f in self.failures)
        description = (
            f"data quality checks failed on input '{input_name}' "
            f"({len(self.failures)} failed check(s)):\n{details}"
        )
        super().__init__(process_name, step_name, step_type, description)


class ReconciliationError(StepExecutionError):
    """A row-level reconciliation between two DataFrames found differences."""

    def __init__(
        self, process_name: str, step_name: str, step_type: str, left: str, right: str, counts: dict
    ):
        self.left = left
        self.right = right
        self.counts = dict(counts)
        details = ", ".join(f"{key}={value}" for key, value in counts.items() if value)
        description = f"reconciliation of '{left}' against '{right}' found differences: {details}"
        super().__init__(process_name, step_name, step_type, description)


class ExpressionExecutionError(StepExecutionError):
    """A Spark SQL expression could not be evaluated.

    Includes the input DataFrame, the column being calculated, and the failed
    expression, as required by the specification.
    """

    def __init__(
        self,
        process_name: str,
        step_name: str,
        step_type: str,
        input_name: str,
        column_name: str,
        expression: str,
        original: Exception,
    ):
        self.input_name = input_name
        self.column_name = column_name
        self.expression = expression
        description = (
            f"failed to evaluate expression for column '{column_name}' on input "
            f"'{input_name}'. Expression: {expression}"
        )
        super().__init__(process_name, step_name, step_type, description, original)
