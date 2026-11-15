"""Join operation: combines two context DataFrames on SQL conditions."""

from etl_framework.operations.base import Operation

SUPPORTED_JOIN_TYPES = ("inner", "left", "right", "full", "semi", "anti", "cross")

# Map user-facing join types to the names Spark expects.
_SPARK_JOIN_TYPES = {
    "inner": "inner",
    "left": "left",
    "right": "right",
    "full": "full",
    "semi": "left_semi",
    "anti": "left_anti",
    "cross": "cross",
}


class JoinOperation(Operation):
    """Joins two DataFrames.

    When both sides share column names, the left side's copy is kept and the
    right side's duplicates are dropped, so downstream steps can reference the
    columns unqualified. Set ``duplicate_columns: keep_both`` to keep both.
    """

    type_name = "join"
    required_fields = ("left", "right", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        join_type = step.get("join_type", "inner")
        if join_type not in SUPPORTED_JOIN_TYPES:
            errors.append(
                f"unsupported join_type '{join_type}'. "
                f"Supported types: {', '.join(SUPPORTED_JOIN_TYPES)}"
            )
        duplicate_columns = step.get("duplicate_columns", "keep_left")
        if duplicate_columns not in ("keep_left", "keep_both"):
            errors.append(
                f"invalid duplicate_columns '{duplicate_columns}'. "
                f"Supported values: keep_left, keep_both"
            )
        if join_type == "cross":
            if not step.get("allow_cross"):
                errors.append("cross joins must be explicitly enabled with 'allow_cross: true'")
        else:
            condition = step.get("condition")
            if not condition:
                errors.append("required field 'condition' is missing or empty")
            elif not isinstance(condition, list):
                errors.append("'condition' must be a list of SQL join conditions")
        return errors

    @classmethod
    def input_names(cls, step: dict) -> list:
        return [name for name in (step.get("left"), step.get("right")) if name]

    def execute(self, context, step: dict):
        from pyspark.sql.functions import expr

        left_name, right_name = step["left"], step["right"]
        # Alias both sides with their context names so conditions can use
        # qualified references like "orders.customer_id = customers.customer_id".
        left_df = context.get_dataframe(left_name).alias(left_name)
        right_df = context.get_dataframe(right_name).alias(right_name)
        join_type = _SPARK_JOIN_TYPES[step.get("join_type", "inner")]

        if join_type == "cross":
            result = left_df.crossJoin(right_df)
        else:
            condition = expr(" AND ".join(f"({c})" for c in step["condition"]))
            result = left_df.join(right_df, on=condition, how=join_type)

        keep_left = step.get("duplicate_columns", "keep_left") == "keep_left"
        # Semi/anti joins only return left-side columns; nothing to drop.
        if keep_left and join_type not in ("left_semi", "left_anti"):
            for column in set(left_df.columns) & set(right_df.columns):
                result = result.drop(right_df[column])

        context.register_dataframe(step["output"], result)
        return result
