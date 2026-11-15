"""Sample operation: takes a random fraction or a fixed number of rows.

Useful for building development subsets, smoke-test pipelines, or profiling
runs over a slice of production data. ``fraction`` draws a random sample
(approximate row count; pass ``seed`` for reproducibility); ``rows`` takes
the first N rows (cheap, but not random).
"""

from etl_framework.operations.base import Operation


class SampleOperation(Operation):
    type_name = "sample"
    required_fields = ("input", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        fraction, rows = step.get("fraction"), step.get("rows")
        if (fraction is None) == (rows is None):
            errors.append("provide exactly one of 'fraction' or 'rows'")
        if fraction is not None and (
            not isinstance(fraction, (int, float)) or not 0 < fraction <= 1
        ):
            errors.append("'fraction' must be a number between 0 (exclusive) and 1")
        if rows is not None and (not isinstance(rows, int) or rows <= 0):
            errors.append("'rows' must be a positive integer")
        return errors

    def execute(self, context, step: dict):
        df = context.get_dataframe(step["input"])
        if step.get("rows") is not None:
            result = df.limit(step["rows"])
        else:
            result = df.sample(fraction=step["fraction"], seed=step.get("seed"))
        context.register_dataframe(step["output"], result)
        return result
