"""Aggregate operation: groups data and calculates SQL aggregation metrics."""

from etl_framework.operations.base import Operation


class AggregateOperation(Operation):
    type_name = "aggregate"
    required_fields = ("input", "aggregations", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        aggregations = step.get("aggregations")
        if aggregations is not None and not isinstance(aggregations, dict):
            errors.append("'aggregations' must be a mapping of output column to SQL expression")
        return errors

    def execute(self, context, step: dict):
        from pyspark.sql.functions import expr

        df = context.get_dataframe(step["input"])
        aggregations = [
            expr(expression).alias(name) for name, expression in step["aggregations"].items()
        ]
        group_by = step.get("group_by") or []
        grouped = df.groupBy(*group_by) if group_by else df
        result = grouped.agg(*aggregations)
        context.register_dataframe(step["output"], result)
        return result
