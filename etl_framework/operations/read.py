"""Read operation: loads data from a configured source into the context."""

from etl_framework.operations.base import Operation

FILE_FORMATS = ("delta", "parquet", "csv", "json")
TABLE_FORMATS = ("table", "unity_catalog")
SUPPORTED_FORMATS = FILE_FORMATS + TABLE_FORMATS + ("jdbc",)


class ReadOperation(Operation):
    type_name = "read"
    required_fields = ("format", "alias")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        fmt = step.get("format")
        if fmt and fmt not in SUPPORTED_FORMATS:
            errors.append(
                f"unsupported read format '{fmt}'. "
                f"Supported formats: {', '.join(SUPPORTED_FORMATS)}"
            )
        if fmt in FILE_FORMATS and not (step.get("path") or step.get("table")):
            errors.append(f"read with format '{fmt}' requires 'path' (or 'table')")
        if fmt in TABLE_FORMATS and not step.get("table"):
            errors.append(f"read with format '{fmt}' requires 'table'")
        if fmt == "jdbc" and not step.get("options"):
            errors.append("read with format 'jdbc' requires 'options' (e.g. url, dbtable)")
        return errors

    @classmethod
    def input_names(cls, step: dict) -> list:
        return []

    @classmethod
    def output_name(cls, step: dict):
        return step.get("alias")

    def execute(self, context, step: dict):
        fmt = step["format"]
        options = step.get("options") or {}
        contract = self.get_schema_contract(context, step)
        # For schema-less formats the contract is applied as the read schema;
        # for self-describing sources it is enforced against what was read.
        apply_contract = contract is not None and fmt in ("csv", "json")

        if fmt in TABLE_FORMATS or (fmt == "delta" and step.get("table")):
            df = context.spark.read.options(**options).table(step["table"])
        elif fmt == "jdbc":
            df = context.spark.read.format("jdbc").options(**options).load()
        else:
            reader = context.spark.read.format(fmt).options(**options)
            if apply_contract:
                reader = reader.schema(contract.to_ddl())
            elif step.get("schema"):
                reader = reader.schema(step["schema"])
            df = reader.load(step["path"])

        if contract is not None and not apply_contract:
            self.enforce_schema_contract(context, step, df, contract)

        context.register_dataframe(step["alias"], df)
        return df
