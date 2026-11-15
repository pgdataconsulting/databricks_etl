"""Deduplicate operation: removes duplicate rows.

With ``order_by``, keeps the first row per partition using
``ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)``. Without it, falls back
to simple column-based deduplication.
"""

from etl_framework.operations.base import Operation

_ROW_NUMBER_COLUMN = "__etl_framework_row_number"


def _parse_order_by(order_spec: str):
    """Parse "updated_at desc" / "created_at" into an ordered Column."""
    from pyspark.sql.functions import col

    parts = str(order_spec).split()
    column = col(parts[0])
    direction = parts[1].lower() if len(parts) > 1 else "asc"
    return column.desc() if direction == "desc" else column.asc()


class DeduplicateOperation(Operation):
    type_name = "deduplicate"
    required_fields = ("input", "partition_by", "output")

    def execute(self, context, step: dict):
        from pyspark.sql.functions import col, row_number
        from pyspark.sql.window import Window

        df = context.get_dataframe(step["input"])
        partition_by = step["partition_by"]
        order_by = step.get("order_by") or []

        if order_by:
            window = Window.partitionBy(*partition_by).orderBy(
                *[_parse_order_by(o) for o in order_by]
            )
            result = (
                df.withColumn(_ROW_NUMBER_COLUMN, row_number().over(window))
                .filter(col(_ROW_NUMBER_COLUMN) == 1)
                .drop(_ROW_NUMBER_COLUMN)
            )
        else:
            result = df.dropDuplicates(subset=list(partition_by))

        context.register_dataframe(step["output"], result)
        return result
