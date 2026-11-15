"""Filter operation: applies a SQL condition to a DataFrame."""

from etl_framework.operations.base import Operation


class FilterOperation(Operation):
    type_name = "filter"
    required_fields = ("input", "condition", "output")

    def execute(self, context, step: dict):
        df = context.get_dataframe(step["input"])
        result = df.filter(step["condition"])
        context.register_dataframe(step["output"], result)
        return result
