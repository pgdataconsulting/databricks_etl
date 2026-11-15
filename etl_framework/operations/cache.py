"""Cache operation: persists or checkpoints a DataFrame reused downstream.

Spark recomputes a DataFrame's full lineage for every action, so an
intermediate result consumed by several later steps (or feeding multiple
sinks) is otherwise computed repeatedly. ``method: cache`` persists it at a
configurable storage level; ``checkpoint`` / ``local_checkpoint`` materialize
it *and* truncate the lineage, which also keeps very deep plans (long
pipelines, iterative logic) from overwhelming the optimizer::

    - name: cache_enriched
      type: cache
      input: enriched
      method: cache                    # cache (default) | checkpoint | local_checkpoint
      storage_level: MEMORY_AND_DISK   # cache only (default MEMORY_AND_DISK)
      eager: true                      # cache only: materialize now, not at first use
      output: enriched_cached

``checkpoint`` writes to the directory set via ``checkpoint_dir`` (or one
configured on the cluster) and survives executor loss; ``local_checkpoint``
uses executor-local storage — faster, no directory needed, but not
fault-tolerant. Checkpoints always materialize eagerly.
"""

from etl_framework.operations.base import Operation

SUPPORTED_METHODS = ("cache", "checkpoint", "local_checkpoint")
DEFAULT_STORAGE_LEVEL = "MEMORY_AND_DISK"


def _storage_level(name: str):
    from pyspark import StorageLevel

    level = getattr(StorageLevel, str(name), None)
    return level if isinstance(level, StorageLevel) else None


def _is_known_storage_level(name: str) -> bool:
    """Check a storage level name against PySpark's own list.

    Configurations are also validated where PySpark is not installed (CI
    gates, pre-commit); there the name is accepted and checked at execution.
    """
    try:
        return _storage_level(name) is not None
    except ImportError:
        return True


class CacheOperation(Operation):
    type_name = "cache"
    required_fields = ("input", "output")

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        method = step.get("method", "cache")
        if method not in SUPPORTED_METHODS:
            errors.append(
                f"unsupported method '{method}'. Supported methods: {', '.join(SUPPORTED_METHODS)}"
            )
        level = step.get("storage_level")
        if level is not None:
            if method != "cache":
                errors.append("'storage_level' only applies to method 'cache'")
            elif not _is_known_storage_level(level):
                errors.append(
                    f"unknown storage_level '{level}' "
                    f"(e.g. MEMORY_AND_DISK, MEMORY_ONLY, DISK_ONLY)"
                )
        if step.get("checkpoint_dir") and method != "checkpoint":
            errors.append("'checkpoint_dir' only applies to method 'checkpoint'")
        return errors

    def execute(self, context, step: dict):
        df = context.get_dataframe(step["input"])
        method = step.get("method", "cache")

        if method == "cache":
            result = df.persist(_storage_level(step.get("storage_level", DEFAULT_STORAGE_LEVEL)))
            if step.get("eager"):
                result.count()
        elif method == "checkpoint":
            if step.get("checkpoint_dir"):
                context.spark.sparkContext.setCheckpointDir(step["checkpoint_dir"])
            result = df.checkpoint(eager=True)
        else:
            result = df.localCheckpoint(eager=True)

        context.register_dataframe(step["output"], result)
        return result
