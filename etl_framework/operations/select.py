"""Select operation: chooses or renames columns using SQL-style expressions."""

from etl_framework.operations.base import Operation


class SelectOperation(Operation):
    type_name = "select"
    required_fields = ("input", "columns", "output")

    def execute(self, context, step: dict):
        df = context.get_dataframe(step["input"])
        # selectExpr supports both plain names and "customer_name as name".
        result = df.selectExpr(*[str(c) for c in step["columns"]])
        context.register_dataframe(step["output"], result)
        return result
