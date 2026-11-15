"""Quality operation: declarative data quality checks on a DataFrame.

Example::

    - name: check_orders
      type: quality
      input: orders
      on_failure: fail            # fail (default) | warn | quarantine
      quarantine_path: abfss://datalake/quarantine/orders   # quarantine only
      checks:
        - {type: not_null, columns: [order_id, customer_id]}
        - {type: unique, columns: [order_id]}
        - {type: accepted_values, column: status, values: [NEW, SHIPPED]}
        - {type: expression, condition: "order_amount >= 0"}
        - {type: row_count, min: 1}
      output: checked_orders

Row-level checks (``not_null``, ``accepted_values``, ``expression``) identify
individual violating rows; dataset-level checks (``unique``, ``row_count``)
judge the DataFrame as a whole.

``on_failure`` semantics:

* ``fail`` — raise :class:`QualityCheckError` listing every failed check.
* ``warn`` — log the failures and pass all rows through unchanged.
* ``quarantine`` — write rows violating row-level checks to
  ``quarantine_path`` or ``quarantine_table`` (format ``quarantine_format``,
  default delta; a table is the natural choice on Unity Catalog) and pass
  only clean rows downstream. Dataset-level check failures still fail the
  step, since they cannot be attributed to individual rows.
"""

import logging
from functools import reduce

from etl_framework.exceptions import QualityCheckError
from etl_framework.operations.base import Operation

_logger = logging.getLogger("etl_framework")

ROW_LEVEL_TYPES = ("not_null", "accepted_values", "expression")
DATASET_LEVEL_TYPES = ("unique", "row_count")
CHECK_TYPES = ROW_LEVEL_TYPES + DATASET_LEVEL_TYPES
ON_FAILURE_MODES = ("fail", "warn", "quarantine")


def _check_label(check: dict) -> str:
    check_type = check.get("type")
    if check_type == "not_null":
        return f"not_null({', '.join(check.get('columns') or [])})"
    if check_type == "unique":
        return f"unique({', '.join(check.get('columns') or [])})"
    if check_type == "accepted_values":
        return f"accepted_values({check.get('column')})"
    if check_type == "expression":
        return check.get("name") or f"expression({check.get('condition')})"
    if check_type == "row_count":
        bounds = [f"{bound}={check[bound]}" for bound in ("min", "max") if bound in check]
        return f"row_count({', '.join(bounds)})"
    return str(check_type)


def _violation_condition(check: dict):
    """Boolean Column that is true for rows VIOLATING a row-level check."""
    from pyspark.sql import functions as F

    check_type = check["type"]
    if check_type == "not_null":
        conditions = [F.col(c).isNull() for c in check["columns"]]
        return reduce(lambda a, b: a | b, conditions)
    if check_type == "accepted_values":
        column = F.col(check["column"])
        return column.isNotNull() & ~column.isin(list(check["values"]))
    # expression: rows where the condition is not satisfied (null counts as
    # a violation — an unknown outcome is not a pass).
    condition = F.expr(check["condition"])
    return ~condition.eqNullSafe(F.lit(True))


class QualityOperation(Operation):
    type_name = "quality"
    required_fields = ("input", "checks", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        on_failure = step.get("on_failure", "fail")
        if on_failure not in ON_FAILURE_MODES:
            errors.append(
                f"invalid on_failure '{on_failure}'. Supported modes: {', '.join(ON_FAILURE_MODES)}"
            )
        targets = [f for f in ("quarantine_path", "quarantine_table") if step.get(f)]
        if on_failure == "quarantine" and not targets:
            errors.append("on_failure: quarantine requires 'quarantine_path' or 'quarantine_table'")
        if len(targets) > 1:
            errors.append("set only one of 'quarantine_path' and 'quarantine_table'")

        checks = step.get("checks")
        if not isinstance(checks, list):
            return errors
        for index, check in enumerate(checks):
            position = f"checks[{index}]"
            if not isinstance(check, dict):
                errors.append(f"{position} must be a mapping")
                continue
            check_type = check.get("type")
            if check_type not in CHECK_TYPES:
                errors.append(
                    f"{position}: unknown check type '{check_type}'. "
                    f"Supported types: {', '.join(CHECK_TYPES)}"
                )
                continue
            if check_type in ("not_null", "unique") and not check.get("columns"):
                errors.append(f"{position} ({check_type}): 'columns' is required")
            if check_type == "accepted_values" and (
                not check.get("column") or not check.get("values")
            ):
                errors.append(f"{position} (accepted_values): 'column' and 'values' are required")
            if check_type == "expression" and not check.get("condition"):
                errors.append(f"{position} (expression): 'condition' is required")
            if check_type == "row_count" and not ("min" in check or "max" in check):
                errors.append(f"{position} (row_count): 'min' and/or 'max' is required")
        return errors

    def _evaluate(self, df, checks: list):
        """Run all checks; returns (failures, row_level_violation_condition)."""
        from pyspark.sql import functions as F

        row_checks = [c for c in checks if c["type"] in ROW_LEVEL_TYPES]
        failures = []

        # One aggregation pass: total rows plus violation count per row check.
        aggregations = [F.count(F.lit(1)).alias("__total")]
        for index, check in enumerate(row_checks):
            aggregations.append(
                F.sum(_violation_condition(check).cast("long")).alias(f"__violations_{index}")
            )
        counts = df.agg(*aggregations).collect()[0]
        total_rows = counts["__total"]

        for index, check in enumerate(row_checks):
            violations = counts[f"__violations_{index}"] or 0
            if violations:
                failures.append(
                    f"{_check_label(check)}: {violations} of {total_rows} row(s) violate the check"
                )

        dataset_failures = []
        for check in (c for c in checks if c["type"] in DATASET_LEVEL_TYPES):
            if check["type"] == "unique":
                distinct = df.select(*check["columns"]).distinct().count()
                if distinct != total_rows:
                    dataset_failures.append(
                        f"{_check_label(check)}: {total_rows - distinct} duplicate row(s) "
                        f"across {total_rows} row(s)"
                    )
            else:  # row_count
                minimum, maximum = check.get("min"), check.get("max")
                if minimum is not None and total_rows < minimum:
                    dataset_failures.append(
                        f"{_check_label(check)}: {total_rows} row(s), expected at least {minimum}"
                    )
                if maximum is not None and total_rows > maximum:
                    dataset_failures.append(
                        f"{_check_label(check)}: {total_rows} row(s), expected at most {maximum}"
                    )

        row_violation = None
        if row_checks:
            row_violation = reduce(
                lambda a, b: a | b, (_violation_condition(c) for c in row_checks)
            )
        return failures, dataset_failures, row_violation

    def execute(self, context, step: dict):
        df = context.get_dataframe(step["input"])
        on_failure = step.get("on_failure", "fail")
        row_failures, dataset_failures, row_violation = self._evaluate(df, step["checks"])
        all_failures = row_failures + dataset_failures
        result = df

        if all_failures:
            if on_failure == "fail" or (on_failure == "quarantine" and dataset_failures):
                # Dataset-level failures cannot be quarantined row by row.
                raise QualityCheckError(
                    process_name=context.process_name,
                    step_name=step.get("name", "<unnamed>"),
                    step_type=self.type_name,
                    input_name=step["input"],
                    failures=all_failures if on_failure == "fail" else dataset_failures,
                )
            if on_failure == "warn":
                _logger.warning(
                    "Pipeline '%s', step '%s': data quality checks failed (passing "
                    "rows through): %s",
                    context.process_name,
                    step.get("name"),
                    "; ".join(all_failures),
                )
            elif row_failures:  # quarantine
                rejected = df.filter(row_violation)
                writer = rejected.write.mode(step.get("quarantine_mode", "append"))
                writer = writer.format(step.get("quarantine_format", "delta"))
                target = step.get("quarantine_table") or step["quarantine_path"]
                if step.get("quarantine_table"):
                    writer.saveAsTable(target)
                else:
                    writer.save(target)
                _logger.warning(
                    "Pipeline '%s', step '%s': quarantined rows failing %d check(s) to %s",
                    context.process_name,
                    step.get("name"),
                    len(row_failures),
                    target,
                )
                result = df.filter(~row_violation)

        context.register_dataframe(step["output"], result)
        return result
