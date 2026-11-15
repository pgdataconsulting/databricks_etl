"""Lookup operation: enriches rows from a small reference DataFrame.

A specialized left join for the dimension-lookup pattern: the reference is
broadcast to every executor (no shuffle of the large input), only the chosen
columns are appended, and misses can fall back to defaults instead of nulls::

    - name: add_country
      type: lookup
      input: orders
      lookup: country_dim
      on:
        country_code: code          # input column -> lookup column
      select:                       # columns to append (optional rename)
        - country_name
        - "region as country_region"
      defaults:
        country_name: "UNKNOWN"
      output: orders_with_country

Without ``select``, every non-key lookup column is appended. The lookup keys
should be unique per combination of ``on`` columns — duplicate keys multiply
input rows, exactly like a join would (add a ``deduplicate`` step first).
"""

import re

from etl_framework.operations.base import Operation

_ALIAS_PATTERN = re.compile(r"\s+as\s+", re.IGNORECASE)
_PREFIX = "__etl_framework_lookup_"


def _parse_select(entries: list) -> list:
    """Split "col" / "col as name" entries into (source, target) pairs."""
    pairs = []
    for entry in entries:
        parts = _ALIAS_PATTERN.split(str(entry).strip(), maxsplit=1)
        pairs.append((parts[0], parts[1] if len(parts) > 1 else parts[0]))
    return pairs


class LookupOperation(Operation):
    type_name = "lookup"
    required_fields = ("input", "lookup", "on", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        mapping = step.get("on")
        if mapping is not None and (
            not isinstance(mapping, dict)
            or not all(
                isinstance(key, str) and key and isinstance(value, str) and value
                for key, value in mapping.items()
            )
        ):
            errors.append("'on' must be a mapping of input column -> lookup column")
        select = step.get("select")
        if select is not None and not isinstance(select, list):
            errors.append("'select' must be a list of lookup columns (supports 'col as name')")
        defaults = step.get("defaults")
        if defaults is not None and not isinstance(defaults, dict):
            errors.append("'defaults' must be a mapping of output column to fallback value")
        return errors

    @classmethod
    def input_names(cls, step: dict) -> list:
        return [name for name in (step.get("input"), step.get("lookup")) if name]

    def execute(self, context, step: dict):
        from pyspark.sql.functions import broadcast, coalesce, col, lit

        input_df = context.get_dataframe(step["input"])
        lookup_df = context.get_dataframe(step["lookup"])
        on = step["on"]

        select = step.get("select")
        if select is None:
            select = [c for c in lookup_df.columns if c not in on.values()]
        pairs = _parse_select(select)

        # Prefix every lookup column so nothing collides with input columns,
        # then join, project the requested columns, and drop the scaffolding.
        needed = list(dict.fromkeys(list(on.values()) + [source for source, _ in pairs]))
        reference = lookup_df.select(*[col(name).alias(_PREFIX + name) for name in needed])
        condition = None
        for input_column, lookup_column in on.items():
            clause = input_df[input_column] == reference[_PREFIX + lookup_column]
            condition = clause if condition is None else condition & clause
        result = input_df.join(broadcast(reference), on=condition, how="left")

        defaults = step.get("defaults") or {}
        for source, target in pairs:
            column = col(_PREFIX + source)
            if target in defaults:
                column = coalesce(column, lit(defaults[target]))
            result = result.withColumn(target, column)
        result = result.drop(*[_PREFIX + name for name in needed])

        context.register_dataframe(step["output"], result)
        return result
