"""SQL operation: runs a free-form SQL query against registered DataFrames.

Every DataFrame in the execution context is exposed as a temporary view, so
queries can reference prior step outputs by name. This operation is optional
and controlled: it must be enabled per pipeline with ``settings.allow_sql``,
because the framework favors declarative steps over free-form SQL.
"""

from etl_framework.operations.base import Operation


class SqlOperation(Operation):
    type_name = "sql"
    required_fields = ("query", "output")

    @classmethod
    def input_names(cls, step: dict) -> list:
        # SQL queries may reference any registered DataFrame; references are
        # resolved by Spark at execution time rather than by the validator.
        return []

    def execute(self, context, step: dict):
        for name, df in context.dataframes.items():
            df.createOrReplaceTempView(name)
        result = context.spark.sql(step["query"])
        context.register_dataframe(step["output"], result)
        return result
