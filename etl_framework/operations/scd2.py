"""SCD Type 2 operation: maintains full change history in the target.

The framework owns the versioning columns:

* ``_effective_from`` (timestamp) — when this version became active
* ``_effective_to``   (timestamp) — when it was superseded (null while current)
* ``_is_current``     (boolean)   — convenience flag for the active version

On each run, input rows are compared to the target's current versions on the
business ``keys``: rows whose ``tracked_columns`` changed (or whose key is
new) close the existing current version and insert a new current one;
unchanged rows and existing history are left untouched.

Example::

    - name: load_customers_scd2
      type: scd2
      input: customers_clean
      keys: [customer_id]
      tracked_columns: [customer_name, email]   # default: all non-key columns
      effective_time: "to_timestamp('${business_date}')"  # default: current_timestamp()
      format: delta
      path: abfss://datalake/silver/customers_history

The target is rewritten as (history + unchanged current + closed versions +
new versions); the result is materialized with localCheckpoint() first so the
target being read can be overwritten safely.
"""

from functools import reduce

from etl_framework.operations.base import Operation

EFFECTIVE_FROM = "_effective_from"
EFFECTIVE_TO = "_effective_to"
IS_CURRENT = "_is_current"

SUPPORTED_FORMATS = ("delta", "parquet")


class Scd2Operation(Operation):
    type_name = "scd2"
    required_fields = ("input", "keys")
    produces_output = False

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        if not (step.get("path") or step.get("table")):
            errors.append("scd2 requires a 'path' or 'table' target")
        keys = step.get("keys")
        if keys is not None and not isinstance(keys, list):
            errors.append("'keys' must be a list of business key columns")
        tracked = step.get("tracked_columns")
        if tracked is not None and (not isinstance(tracked, list) or not tracked):
            errors.append("'tracked_columns' must be a non-empty list when provided")
        fmt = step.get("format", "delta")
        if fmt not in SUPPORTED_FORMATS:
            errors.append(
                f"unsupported scd2 format '{fmt}'. "
                f"Supported formats: {', '.join(SUPPORTED_FORMATS)}"
            )
        return errors

    def _read_target(self, context, step: dict):
        if step.get("table"):
            if not context.spark.catalog.tableExists(step["table"]):
                return None
            return context.spark.read.table(step["table"])
        try:
            df = context.spark.read.format(step.get("format", "delta")).load(step["path"])
            # Resolving the schema is what makes Spark look at the path.
            _ = df.schema
            return df
        except Exception:  # noqa: BLE001 - a real access problem resurfaces on write
            return None

    def execute(self, context, step: dict):
        from pyspark.sql import functions as F

        df = context.get_dataframe(step["input"])
        df = self.apply_audit_columns(context, df)
        contract = self.get_schema_contract(context, step)
        if contract is not None:
            # The contract describes the business columns of the input; the
            # SCD versioning columns are added by the framework afterwards.
            self.enforce_schema_contract(context, step, df, contract)

        keys = step["keys"]
        effective = F.expr(step.get("effective_time") or "current_timestamp()").cast("timestamp")

        def as_new_version(input_df):
            return (
                input_df.withColumn(EFFECTIVE_FROM, effective)
                .withColumn(EFFECTIVE_TO, F.lit(None).cast("timestamp"))
                .withColumn(IS_CURRENT, F.lit(True))
            )

        target_df = self._read_target(context, step)
        if target_df is None:
            result = as_new_version(df)
        else:
            tracked = step.get("tracked_columns") or [c for c in df.columns if c not in keys]
            current = target_df.filter(F.col(IS_CURRENT))
            history = target_df.filter(~F.col(IS_CURRENT))

            probe = current.select(
                *keys, *[F.col(c).alias(f"__target_{c}") for c in tracked]
            ).withColumn("__target_exists", F.lit(True))
            joined = df.join(probe, on=keys, how="left")
            differences = [~F.col(c).eqNullSafe(F.col(f"__target_{c}")) for c in tracked]
            is_changed = F.col("__target_exists").isNull()
            if differences:
                is_changed = is_changed | reduce(lambda a, b: a | b, differences)
            changed = joined.filter(is_changed).select(*df.columns)
            changed_keys = changed.select(*keys).distinct()

            closed = (
                current.join(changed_keys, on=keys, how="left_semi")
                .withColumn(EFFECTIVE_TO, effective)
                .withColumn(IS_CURRENT, F.lit(False))
            )
            unchanged = current.join(changed_keys, on=keys, how="left_anti")
            result = (
                history.unionByName(unchanged)
                .unionByName(closed)
                .unionByName(as_new_version(changed))
            )

        # Materialize before overwriting the location the result was read from.
        result = result.localCheckpoint()
        writer = result.write.mode("overwrite").format(step.get("format", "delta"))
        if step.get("table"):
            writer.saveAsTable(step["table"])
        else:
            writer.save(step["path"])
        return result
