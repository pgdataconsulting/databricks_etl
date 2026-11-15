"""Expression operation: adds or overwrites columns using Spark SQL expressions.

Every expression is translated into PySpark column logic via
``pyspark.sql.functions.expr``, supporting built-in Spark SQL functions,
``case when`` conditional logic, casting, null handling, and runtime
parameters (already substituted by the parameter resolver).
"""

import re

from etl_framework.exceptions import ExpressionExecutionError
from etl_framework.operations.base import Operation

_VALID_COLUMN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ExpressionOperation(Operation):
    type_name = "expression"
    required_fields = ("input", "expressions", "output")
    expressions_field = "expressions"

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        expressions = step.get(cls.expressions_field)
        if expressions is not None:
            if not isinstance(expressions, dict):
                errors.append(
                    f"'{cls.expressions_field}' must be a mapping of column name to SQL expression"
                )
            else:
                for column_name, expression in expressions.items():
                    if not _VALID_COLUMN_NAME.match(str(column_name)):
                        errors.append(f"invalid output column name '{column_name}'")
                    if not expression or not isinstance(expression, str):
                        errors.append(
                            f"expression for column '{column_name}' must be a non-empty SQL string"
                        )
        return errors

    def execute(self, context, step: dict):
        from pyspark.sql.functions import expr

        input_name = step["input"]
        df = context.get_dataframe(input_name)
        for column_name, sql_expression in step[self.expressions_field].items():
            try:
                df = df.withColumn(column_name, expr(sql_expression))
            except Exception as exc:
                raise ExpressionExecutionError(
                    process_name=context.process_name,
                    step_name=step.get("name", "<unnamed>"),
                    step_type=self.type_name,
                    input_name=input_name,
                    column_name=column_name,
                    expression=sql_expression,
                    original=exc,
                ) from exc
        context.register_dataframe(step["output"], df)
        return df


class AddColumnsOperation(ExpressionOperation):
    """add_columns is the same transformation with a `columns` mapping."""

    type_name = "add_columns"
    required_fields = ("input", "columns", "output")
    expressions_field = "columns"
