"""Pivot operation: turns the values of one column into columns.

Rows are grouped by ``group_by``, the distinct values of ``pivot_column``
become columns, and each ``aggregations`` entry fills them. Declaring the
expected ``values`` explicitly is recommended: it keeps the output schema
stable when the data changes and avoids the extra Spark job that otherwise
scans for distinct values.
"""

from etl_framework.operations.base import Operation


class PivotOperation(Operation):
    type_name = "pivot"
    required_fields = ("input", "pivot_column", "aggregations", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        aggregations = step.get("aggregations")
        if aggregations is not None and not isinstance(aggregations, dict):
            errors.append("'aggregations' must be a mapping of output column to SQL expression")
        values = step.get("values")
        if values is not None and not isinstance(values, list):
            errors.append("'values' must be a list of pivot column values")
        return errors

    def execute(self, context, step: dict):
        from pyspark.sql.functions import expr

        df = context.get_dataframe(step["input"])
        grouped = df.groupBy(*(step.get("group_by") or []))
        values = step.get("values")
        pivoted = grouped.pivot(step["pivot_column"], values)
        result = pivoted.agg(
            *[expr(expression).alias(name) for name, expression in step["aggregations"].items()]
        )
        context.register_dataframe(step["output"], result)
        return result
