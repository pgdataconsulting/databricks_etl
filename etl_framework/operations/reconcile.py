"""Reconcile operation: row-level comparison of two DataFrames.

Verifies that a load actually delivered what the source promised — e.g.
compare the silver output against the bronze input, or a migrated table
against its legacy counterpart::

    - name: reconcile_orders
      type: reconcile
      left: source_orders
      right: loaded_orders
      keys: [order_id]
      compare_columns: [order_amount, status]  # default: all shared non-key columns
      tolerance: 0.01          # optional, applies to numeric columns
      on_failure: fail         # fail (default) | warn
      output: reconciliation_report   # optional per-row difference report

The two sides are full-outer-joined on the keys and classified per row:
``missing_in_right`` (in left only), ``missing_in_left`` (in right only), or
``mismatch`` (present in both but differing on a compared column). Matching
rows are excluded from the report.

``fail`` raises :class:`ReconciliationError` with the per-category counts;
``warn`` logs them and continues. Either way, when ``output`` is set a report
DataFrame (key columns + ``reconcile_status`` + ``mismatched_columns``) is
registered for downstream use — e.g. writing it to a reconciliation table.
"""

import logging
from functools import reduce

from etl_framework.exceptions import ReconciliationError
from etl_framework.operations.base import Operation

_logger = logging.getLogger("etl_framework")

ON_FAILURE_MODES = ("fail", "warn")


class ReconcileOperation(Operation):
    type_name = "reconcile"
    required_fields = ("left", "right", "keys")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        keys = step.get("keys")
        if keys is not None and not isinstance(keys, list):
            errors.append("'keys' must be a list of key columns")
        on_failure = step.get("on_failure", "fail")
        if on_failure not in ON_FAILURE_MODES:
            errors.append(
                f"invalid on_failure '{on_failure}'. Supported modes: {', '.join(ON_FAILURE_MODES)}"
            )
        compare_columns = step.get("compare_columns")
        if compare_columns is not None and (
            not isinstance(compare_columns, list) or not compare_columns
        ):
            errors.append("'compare_columns' must be a non-empty list when provided")
        return errors

    @classmethod
    def input_names(cls, step: dict) -> list:
        return [name for name in (step.get("left"), step.get("right")) if name]

    @staticmethod
    def _columns_equal(left_col, right_col, data_type, tolerance):
        """Null-safe equality, with optional tolerance for numeric columns."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import NumericType

        if tolerance is not None and isinstance(data_type, NumericType):
            both_null = left_col.isNull() & right_col.isNull()
            within = (
                left_col.isNotNull()
                & right_col.isNotNull()
                & (F.abs(left_col - right_col) <= F.lit(tolerance))
            )
            return both_null | within
        return left_col.eqNullSafe(right_col)

    def execute(self, context, step: dict):
        from pyspark.sql import functions as F

        left_name, right_name = step["left"], step["right"]
        keys = step["keys"]
        left = context.get_dataframe(left_name).withColumn("__left_exists", F.lit(True))
        right = context.get_dataframe(right_name).withColumn("__right_exists", F.lit(True))
        compare_columns = step.get("compare_columns") or [
            c
            for c in left.columns
            if c in right.columns and c not in keys and not c.startswith("__")
        ]
        tolerance = step.get("tolerance")
        left_types = {f.name: f.dataType for f in left.schema.fields}

        join_condition = reduce(
            lambda a, b: a & b,
            [left[k].eqNullSafe(right[k]) for k in keys],
        )
        joined = left.alias("l").join(right.alias("r"), on=join_condition, how="full_outer")

        mismatch_flags = [
            (
                column,
                ~self._columns_equal(
                    F.col(f"l.`{column}`"),
                    F.col(f"r.`{column}`"),
                    left_types[column],
                    tolerance,
                ),
            )
            for column in compare_columns
        ]
        any_mismatch = reduce(
            lambda a, b: a | b, [flag for _, flag in mismatch_flags], F.lit(False)
        )
        status = (
            F.when(F.col("r.__right_exists").isNull(), F.lit("missing_in_right"))
            .when(F.col("l.__left_exists").isNull(), F.lit("missing_in_left"))
            .when(any_mismatch, F.lit("mismatch"))
        )

        report = (
            joined.withColumn("reconcile_status", status)
            .filter(F.col("reconcile_status").isNotNull())
            .select(
                *[F.coalesce(F.col(f"l.`{k}`"), F.col(f"r.`{k}`")).alias(k) for k in keys],
                F.col("reconcile_status"),
                F.concat_ws(
                    ",",
                    *[
                        F.when(
                            flag
                            & F.col("l.__left_exists").isNotNull()
                            & F.col("r.__right_exists").isNotNull(),
                            F.lit(column),
                        )
                        for column, flag in mismatch_flags
                    ],
                ).alias("mismatched_columns"),
            )
        )

        counts = {"missing_in_right": 0, "missing_in_left": 0, "mismatch": 0}
        for row in report.groupBy("reconcile_status").count().collect():
            counts[row["reconcile_status"]] = row["count"]

        if step.get("output"):
            context.register_dataframe(step["output"], report)

        if any(counts.values()):
            if step.get("on_failure", "fail") == "fail":
                raise ReconciliationError(
                    process_name=context.process_name,
                    step_name=step.get("name", "<unnamed>"),
                    step_type=self.type_name,
                    left=left_name,
                    right=right_name,
                    counts=counts,
                )
            _logger.warning(
                "Pipeline '%s', step '%s': reconciliation of '%s' against '%s' "
                "found differences: %s",
                context.process_name,
                step.get("name"),
                left_name,
                right_name,
                ", ".join(f"{k}={v}" for k, v in counts.items() if v),
            )
        return report
