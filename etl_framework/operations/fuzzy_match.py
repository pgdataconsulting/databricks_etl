"""Fuzzy-match operation: joins two DataFrames on approximate string equality.

Matches ``left_on`` against ``right_on`` using either normalized Levenshtein
similarity (``method: levenshtein``, kept when the similarity reaches
``threshold``) or phonetic equality (``method: soundex``). Both sides are
lowercased and trimmed before comparing unless ``normalize: false``.

Every surviving pair carries a similarity score column (``score_column``,
default ``_match_score``) so downstream steps can inspect match quality; with
``keep: best`` (the default) only the highest-scoring right-side match per
left row is kept, while ``keep: all`` retains every pair above the threshold.

Fuzzy comparison cannot use a hash join, so an unconstrained match compares
every left row with every right row. Provide ``block_on`` equality conditions
(e.g. same country or postcode) to bound the search space, or acknowledge the
full comparison explicitly with ``allow_unblocked: true``.
"""

from etl_framework.operations.base import Operation

SUPPORTED_METHODS = ("levenshtein", "soundex")

_LEFT_ID_COLUMN = "__etl_framework_fuzzy_left_id"
_RANK_COLUMN = "__etl_framework_fuzzy_rank"
DEFAULT_SCORE_COLUMN = "_match_score"
DEFAULT_THRESHOLD = 0.85


class FuzzyMatchOperation(Operation):
    type_name = "fuzzy_match"
    required_fields = ("left", "right", "left_on", "right_on", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        method = step.get("method", "levenshtein")
        if method not in SUPPORTED_METHODS:
            errors.append(
                f"unsupported method '{method}'. Supported methods: {', '.join(SUPPORTED_METHODS)}"
            )
        threshold = step.get("threshold", DEFAULT_THRESHOLD)
        if not isinstance(threshold, (int, float)) or not 0 <= threshold <= 1:
            errors.append("'threshold' must be a number between 0 and 1")
        if step.get("keep", "best") not in ("best", "all"):
            errors.append("invalid keep value. Supported values: best, all")
        if step.get("join_type", "inner") not in ("inner", "left"):
            errors.append("invalid join_type. Supported values: inner, left")
        if step.get("duplicate_columns", "keep_left") not in ("keep_left", "keep_both"):
            errors.append("invalid duplicate_columns. Supported values: keep_left, keep_both")
        block_on = step.get("block_on")
        if block_on is not None and not isinstance(block_on, list):
            errors.append("'block_on' must be a list of SQL equality conditions")
        if not block_on and not step.get("allow_unblocked"):
            errors.append(
                "fuzzy matching without 'block_on' compares every left row "
                "with every right row. Add blocking conditions or enable it "
                "explicitly with 'allow_unblocked: true'"
            )
        return errors

    @classmethod
    def input_names(cls, step: dict) -> list:
        return [name for name in (step.get("left"), step.get("right")) if name]

    @staticmethod
    def _comparable(qualified_column: str, normalize: bool) -> str:
        return f"lower(trim({qualified_column}))" if normalize else qualified_column

    def execute(self, context, step: dict):
        from pyspark.sql.functions import (
            col,
            expr,
            monotonically_increasing_id,
            row_number,
        )
        from pyspark.sql.window import Window

        left_name, right_name = step["left"], step["right"]
        # Tag every left row so best-match ranking and duplicate-free left
        # joins survive left-side duplicates, then alias both sides so the
        # score and block_on conditions can use qualified references.
        left_df = (
            context.get_dataframe(left_name)
            .withColumn(_LEFT_ID_COLUMN, monotonically_increasing_id())
            .alias(left_name)
        )
        right_df = context.get_dataframe(right_name).alias(right_name)

        normalize = step.get("normalize", True)
        left_value = self._comparable(f"{left_name}.{step['left_on']}", normalize)
        right_value = self._comparable(f"{right_name}.{step['right_on']}", normalize)

        # Normalized Levenshtein similarity in [0, 1]; also computed for
        # soundex matches so 'keep: best' can rank them.
        score_sql = (
            f"1.0 - levenshtein({left_value}, {right_value}) / "
            f"greatest(length({left_value}), length({right_value}), 1)"
        )
        if step.get("method", "levenshtein") == "soundex":
            match_sql = f"soundex({left_value}) = soundex({right_value})"
        else:
            match_sql = f"({score_sql}) >= {step.get('threshold', DEFAULT_THRESHOLD)}"

        conditions = [*(step.get("block_on") or []), match_sql]
        condition = expr(" AND ".join(f"({c})" for c in conditions))
        join_type = step.get("join_type", "inner")
        result = left_df.join(right_df, on=condition, how=join_type)
        # Unmatched rows of a left join get a null score.
        score_column = step.get("score_column", DEFAULT_SCORE_COLUMN)
        result = result.withColumn(
            score_column,
            expr(f"CASE WHEN {right_name}.{step['right_on']} IS NOT NULL THEN {score_sql} END"),
        )

        if step.get("keep", "best") == "best":
            window = Window.partitionBy(_LEFT_ID_COLUMN).orderBy(
                col(score_column).desc_nulls_last()
            )
            result = (
                result.withColumn(_RANK_COLUMN, row_number().over(window))
                .filter(col(_RANK_COLUMN) == 1)
                .drop(_RANK_COLUMN)
            )

        keep_left = step.get("duplicate_columns", "keep_left") == "keep_left"
        if keep_left:
            for column in set(left_df.columns) & set(right_df.columns):
                result = result.drop(right_df[column])
        result = result.drop(_LEFT_ID_COLUMN)

        context.register_dataframe(step["output"], result)
        return result
