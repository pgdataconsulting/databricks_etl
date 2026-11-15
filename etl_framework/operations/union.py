"""Union operation: stacks two or more context DataFrames by column name.

Columns are matched by name, not position. With ``allow_missing_columns:
true``, columns present on only some inputs are filled with nulls on the
others; without it, mismatched schemas fail the step. ``distinct: true``
drops duplicate rows from the combined result (UNION vs UNION ALL).
"""

from etl_framework.operations.base import Operation


class UnionOperation(Operation):
    type_name = "union"
    required_fields = ("inputs", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        inputs = step.get("inputs")
        if inputs is None:
            return errors
        if not isinstance(inputs, list) or not all(
            isinstance(name, str) and name for name in inputs
        ):
            errors.append("'inputs' must be a list of DataFrame names")
        elif len(inputs) < 2:
            errors.append("'inputs' must name at least two DataFrames")
        return errors

    @classmethod
    def input_names(cls, step: dict) -> list:
        inputs = step.get("inputs")
        return list(inputs) if isinstance(inputs, list) else []

    def execute(self, context, step: dict):
        allow_missing = bool(step.get("allow_missing_columns"))
        result, *rest = [context.get_dataframe(name) for name in step["inputs"]]
        for df in rest:
            result = result.unionByName(df, allowMissingColumns=allow_missing)
        if step.get("distinct"):
            result = result.dropDuplicates()
        context.register_dataframe(step["output"], result)
        return result
