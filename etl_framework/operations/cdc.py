"""CDC operation: applies a change feed (inserts/updates/deletes) to a Delta target.

The input DataFrame carries change records with an operation indicator column
and, ideally, a sequence column for ordering replayed changes::

    - name: apply_order_changes
      type: cdc
      input: order_changes
      keys: [order_id]
      operation_column: _op
      operation_values: {insert: I, update: U, delete: D}   # defaults shown
      sequence_by: _commit_timestamp    # keep only the latest change per key
      path: abfss://datalake/silver/orders

Semantics:

* With ``sequence_by``, only the latest change per key (highest sequence
  value) is applied, so replayed or batched feeds collapse correctly.
  Without it, the feed must contain at most one change per key — Delta
  rejects merges with multiple source rows per key.
* Deletes remove the row; updates and inserts upsert the business columns
  (every input column except the operation column).
* A missing target is created from the non-delete changes.

Delta targets only, like the merge operation.
"""

import uuid

from etl_framework.operations.base import Operation

DEFAULT_OPERATION_VALUES = {"insert": "I", "update": "U", "delete": "D"}


class CdcOperation(Operation):
    type_name = "cdc"
    required_fields = ("input", "keys", "operation_column")
    produces_output = False

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        if not (step.get("path") or step.get("table")):
            errors.append("cdc requires a 'path' or 'table' target")
        keys = step.get("keys")
        if keys is not None and not isinstance(keys, list):
            errors.append("'keys' must be a list of business key columns")
        operation_values = step.get("operation_values")
        if operation_values is not None:
            if not isinstance(operation_values, dict):
                errors.append("'operation_values' must be a mapping")
            else:
                unknown = [k for k in operation_values if k not in DEFAULT_OPERATION_VALUES]
                if unknown:
                    errors.append(
                        f"'operation_values' contains unknown operation(s) "
                        f"{', '.join(unknown)}. Supported: insert, update, delete"
                    )
        fmt = step.get("format", "delta")
        if fmt != "delta":
            errors.append(f"cdc only supports Delta targets, got format '{fmt}'")
        return errors

    def _target_exists(self, context, step: dict) -> bool:
        if step.get("table"):
            return context.spark.catalog.tableExists(step["table"])
        try:
            # Resolving the schema is what makes Spark look at the path.
            _ = context.spark.read.format("delta").load(step["path"]).schema
            return True
        except Exception:  # noqa: BLE001 - a real access problem resurfaces on write
            return False

    @staticmethod
    def _latest_change_per_key(df, keys: list, sequence_by: str):
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        window = Window.partitionBy(*keys).orderBy(F.col(sequence_by).desc())
        return (
            df.withColumn("__cdc_row_number", F.row_number().over(window))
            .filter(F.col("__cdc_row_number") == 1)
            .drop("__cdc_row_number")
        )

    def execute(self, context, step: dict):
        from pyspark.sql import functions as F

        df = context.get_dataframe(step["input"])
        keys = step["keys"]
        operation_column = step["operation_column"]
        operation_values = {
            **DEFAULT_OPERATION_VALUES,
            **(step.get("operation_values") or {}),
        }
        if step.get("sequence_by"):
            df = self._latest_change_per_key(df, keys, step["sequence_by"])

        business_columns = [c for c in df.columns if c != operation_column]

        if not self._target_exists(context, step):
            initial = df.filter(
                F.col(operation_column) != F.lit(operation_values["delete"])
            ).select(*business_columns)
            writer = initial.write.format("delta")
            if step.get("table"):
                writer.saveAsTable(step["table"])
            else:
                writer.save(step["path"])
            return df

        target_sql = step["table"] if step.get("table") else f"delta.`{step['path']}`"
        source_view = f"__etl_cdc_source_{uuid.uuid4().hex}"
        df.createOrReplaceTempView(source_view)
        try:
            on_clause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
            delete_value = operation_values["delete"]
            update_set = ", ".join(f"t.`{c}` = s.`{c}`" for c in business_columns)
            insert_columns = ", ".join(f"`{c}`" for c in business_columns)
            insert_values = ", ".join(f"s.`{c}`" for c in business_columns)
            context.spark.sql(
                f"MERGE INTO {target_sql} t USING {source_view} s ON {on_clause} "
                f"WHEN MATCHED AND s.`{operation_column}` = '{delete_value}' THEN DELETE "
                f"WHEN MATCHED THEN UPDATE SET {update_set} "
                f"WHEN NOT MATCHED AND s.`{operation_column}` <> '{delete_value}' "
                f"THEN INSERT ({insert_columns}) VALUES ({insert_values})"
            )
        finally:
            context.spark.catalog.dropTempView(source_view)
        return df
