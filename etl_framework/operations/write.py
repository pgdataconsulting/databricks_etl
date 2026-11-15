"""Write operation: saves a context DataFrame to the configured target."""

from etl_framework.operations.base import Operation

FILE_FORMATS = ("delta", "parquet", "csv", "json")
TABLE_FORMATS = ("table", "unity_catalog")
SUPPORTED_FORMATS = FILE_FORMATS + TABLE_FORMATS + ("jdbc",)
SUPPORTED_MODES = (
    "append",
    "overwrite",
    "overwrite_partitions",
    "error",
    "errorifexists",
    "ignore",
)
# How to handle input columns the target does not have (Delta targets only):
# fail the write (default), evolve the target schema in place, or replace the
# target schema entirely on a full overwrite.
SUPPORTED_SCHEMA_DRIFT = ("fail", "evolve", "overwrite")
_DELTA_FORMATS = ("delta", *TABLE_FORMATS)


class WriteOperation(Operation):
    type_name = "write"
    required_fields = ("input", "format")
    produces_output = False

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        fmt = step.get("format")
        if fmt and fmt not in SUPPORTED_FORMATS:
            errors.append(
                f"unsupported write format '{fmt}'. "
                f"Supported formats: {', '.join(SUPPORTED_FORMATS)}"
            )
        if fmt in FILE_FORMATS and not (step.get("path") or step.get("table")):
            errors.append(f"write with format '{fmt}' requires a 'path' or 'table' target")
        if fmt in TABLE_FORMATS and not step.get("table"):
            errors.append(f"write with format '{fmt}' requires 'table'")
        if fmt == "jdbc" and not step.get("options"):
            errors.append("write with format 'jdbc' requires 'options' (e.g. url, dbtable)")
        mode = step.get("mode")
        if mode and mode not in SUPPORTED_MODES:
            errors.append(
                f"unsupported write mode '{mode}'. Supported modes: {', '.join(SUPPORTED_MODES)}"
            )
        if mode == "overwrite_partitions" and not step.get("partition_by"):
            errors.append("mode 'overwrite_partitions' requires 'partition_by'")
        drift = step.get("schema_drift", "fail")
        if drift not in SUPPORTED_SCHEMA_DRIFT:
            errors.append(
                f"unsupported schema_drift '{drift}'. "
                f"Supported values: {', '.join(SUPPORTED_SCHEMA_DRIFT)}"
            )
        elif drift != "fail":
            if fmt and fmt not in _DELTA_FORMATS:
                errors.append(f"schema_drift '{drift}' requires a Delta target, got format '{fmt}'")
            if drift == "overwrite" and mode != "overwrite":
                errors.append("schema_drift 'overwrite' requires mode 'overwrite'")
        return errors

    def execute(self, context, step: dict):
        df = context.get_dataframe(step["input"])
        df = self.apply_audit_columns(context, df)
        contract = self.get_schema_contract(context, step)
        if contract is not None:
            # Enforced before the write starts so a drifted pipeline never
            # touches the target.
            self.enforce_schema_contract(context, step, df, contract)

        fmt = step["format"]
        mode = step.get("mode", "error")
        writer = df.write
        if mode == "overwrite_partitions":
            # Dynamic partition overwrite: only the partitions present in the
            # input are replaced; all other partitions are left untouched.
            writer = writer.mode("overwrite").option("partitionOverwriteMode", "dynamic")
        else:
            writer = writer.mode(mode)
        drift = step.get("schema_drift", "fail")
        if drift == "evolve":
            writer = writer.option("mergeSchema", "true")
        elif drift == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        if step.get("partition_by"):
            writer = writer.partitionBy(*step["partition_by"])
        if step.get("options"):
            writer = writer.options(**step["options"])

        if fmt in TABLE_FORMATS:
            writer.saveAsTable(step["table"])
        elif fmt == "jdbc":
            writer.format("jdbc").save()
        elif step.get("table"):
            writer.format(fmt).saveAsTable(step["table"])
        else:
            writer.format(fmt).save(step["path"])
        return df
