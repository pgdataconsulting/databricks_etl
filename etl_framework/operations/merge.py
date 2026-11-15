"""Merge operation: upserts a DataFrame into a Delta target.

Translated into a Delta ``MERGE INTO`` keyed on ``merge_keys``: matched rows
are updated (all columns), unmatched rows are inserted, and an optional
``delete_condition`` deletes matched rows instead. If the target does not
exist yet it is created from the input.

The input and the target must have the same columns (``UPDATE SET *`` /
``INSERT *`` semantics) unless ``schema_evolution: true``, which lets new
input columns evolve the target schema during the merge. Delta targets only —
this is the incremental alternative to a plain ``write`` for mutable silver
datasets.
"""

import uuid
from contextlib import contextmanager

from etl_framework.operations.base import Operation

_AUTO_MERGE_CONF = "spark.databricks.delta.schema.autoMerge.enabled"


@contextmanager
def _schema_evolution(spark, enabled: bool):
    """Temporarily allow Delta MERGE schema evolution (a session-wide conf)."""
    if not enabled:
        yield
        return
    previous = spark.conf.get(_AUTO_MERGE_CONF, None)
    spark.conf.set(_AUTO_MERGE_CONF, "true")
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(_AUTO_MERGE_CONF)
        else:
            spark.conf.set(_AUTO_MERGE_CONF, previous)


class MergeOperation(Operation):
    type_name = "merge"
    required_fields = ("input", "merge_keys")
    produces_output = False

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        if not (step.get("path") or step.get("table")):
            errors.append("merge requires a 'path' or 'table' target")
        merge_keys = step.get("merge_keys")
        if merge_keys is not None and not isinstance(merge_keys, list):
            errors.append("'merge_keys' must be a list of column names")
        fmt = step.get("format", "delta")
        if fmt != "delta":
            errors.append(f"merge only supports Delta targets, got format '{fmt}'")
        return errors

    def _target_exists(self, context, step: dict) -> bool:
        if step.get("table"):
            return context.spark.catalog.tableExists(step["table"])
        try:
            # Resolving the schema is what makes Spark look at the path.
            _ = context.spark.read.format("delta").load(step["path"]).schema
            return True
        except Exception:  # noqa: BLE001 - a real access problem resurfaces on create
            return False

    def execute(self, context, step: dict):
        df = context.get_dataframe(step["input"])
        df = self.apply_audit_columns(context, df)
        contract = self.get_schema_contract(context, step)
        if contract is not None:
            self.enforce_schema_contract(context, step, df, contract)

        spark = context.spark
        if not self._target_exists(context, step):
            writer = df.write.format("delta")
            if step.get("table"):
                writer.saveAsTable(step["table"])
            else:
                writer.save(step["path"])
            return df

        target_sql = step["table"] if step.get("table") else f"delta.`{step['path']}`"
        source_view = f"__etl_merge_source_{uuid.uuid4().hex}"
        df.createOrReplaceTempView(source_view)
        try:
            on_clause = " AND ".join(f"t.`{key}` = s.`{key}`" for key in step["merge_keys"])
            clauses = []
            if step.get("delete_condition"):
                clauses.append(f"WHEN MATCHED AND ({step['delete_condition']}) THEN DELETE")
            update_condition = step.get("update_condition")
            matched = "WHEN MATCHED"
            if update_condition:
                matched += f" AND ({update_condition})"
            clauses.append(f"{matched} THEN UPDATE SET *")
            clauses.append("WHEN NOT MATCHED THEN INSERT *")
            with _schema_evolution(spark, bool(step.get("schema_evolution"))):
                spark.sql(
                    f"MERGE INTO {target_sql} t USING {source_view} s "
                    f"ON {on_clause} " + " ".join(clauses)
                )
        finally:
            spark.catalog.dropTempView(source_view)
        return df
