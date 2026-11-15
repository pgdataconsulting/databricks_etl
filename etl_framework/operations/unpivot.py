"""Unpivot operation: melts columns into key/value rows (wide to long).

Every column named in ``value_columns`` (default: all columns not listed in
``id_columns``) becomes one output row of ``key_column`` (the column's name)
and ``value_column`` (its value), repeated for each input row. Spark requires
the unpivoted columns to share a common type — cast them first (e.g. with an
``expression`` step) when they are mixed.
"""

from etl_framework.operations.base import Operation


class UnpivotOperation(Operation):
    type_name = "unpivot"
    required_fields = ("input", "id_columns", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        for field in ("id_columns", "value_columns"):
            value = step.get(field)
            if value is not None and not isinstance(value, list):
                errors.append(f"'{field}' must be a list of column names")
        return errors

    def execute(self, context, step: dict):
        df = context.get_dataframe(step["input"])
        id_columns = step["id_columns"]
        value_columns = step.get("value_columns") or [
            column for column in df.columns if column not in id_columns
        ]
        result = df.unpivot(
            ids=id_columns,
            values=value_columns,
            variableColumnName=step.get("key_column", "key"),
            valueColumnName=step.get("value_column", "value"),
        )
        context.register_dataframe(step["output"], result)
        return result
