"""Window operation: adds analytic columns computed over a window.

Each ``columns`` entry is a SQL expression evaluated ``OVER`` the window
defined by ``partition_by`` / ``order_by`` (and an optional ``frame``
clause), which covers ranking (``row_number()``, ``rank()``,
``dense_rank()``), navigation (``lag(col, 1)``, ``lead(col, 1)``), and
running aggregates (``sum(col)``, ``avg(col)``, ...). Note Spark's default
frame: with ``order_by``, aggregates run from the partition start to the
current row (running totals); without it they cover the whole partition.
Both lists may be omitted for grand-total windows, e.g.
``order_amount / sum(order_amount)`` over the entire DataFrame.
"""

from etl_framework.exceptions import ExpressionExecutionError
from etl_framework.operations.base import Operation


class WindowOperation(Operation):
    type_name = "window"
    required_fields = ("input", "columns", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        columns = step.get("columns")
        if columns is not None and not isinstance(columns, dict):
            errors.append("'columns' must be a mapping of output column to SQL expression")
        for field in ("partition_by", "order_by"):
            value = step.get(field)
            if value is not None and not isinstance(value, list):
                errors.append(f"'{field}' must be a list of columns")
        return errors

    @staticmethod
    def _over_clause(step: dict) -> str:
        parts = []
        if step.get("partition_by"):
            parts.append("PARTITION BY " + ", ".join(step["partition_by"]))
        if step.get("order_by"):
            parts.append("ORDER BY " + ", ".join(step["order_by"]))
        if step.get("frame"):
            parts.append(step["frame"])
        return "OVER (" + " ".join(parts) + ")"

    def execute(self, context, step: dict):
        from pyspark.sql.functions import expr

        df = context.get_dataframe(step["input"])
        over = self._over_clause(step)
        for name, expression in step["columns"].items():
            windowed = f"{expression} {over}"
            try:
                df = df.withColumn(name, expr(windowed))
            except Exception as exc:
                raise ExpressionExecutionError(
                    process_name=context.process_name,
                    step_name=step.get("name", "<unnamed>"),
                    step_type=self.type_name,
                    input_name=step["input"],
                    column_name=name,
                    expression=windowed,
                    original=exc,
                ) from exc
        context.register_dataframe(step["output"], df)
        return df
