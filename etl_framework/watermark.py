"""Incremental-load watermarks: persisted high-water-mark state per pipeline.

Enabled through pipeline settings::

    settings:
      watermark:
        enabled: true
        source: new_orders          # DataFrame whose maximum drives the state
        column: updated_at          # column to take the maximum of
        initial: "1900-01-01"       # value used before the first run
        table: etl_state.watermarks # Delta state table (default, auto-created)
        parameter: watermark        # exposed as ${watermark} (default)

At run start the last stored value (or ``initial``) is injected as a runtime
parameter so steps can filter incrementally, e.g.
``condition: "updated_at > '${watermark}'"``. After a successful run the
maximum of ``column`` in the ``source`` DataFrame is persisted; an empty
increment leaves the previous watermark untouched. Failed runs never advance
the watermark, so the next run picks the same window up again — pair this
with idempotent sinks (``merge``, ``scd2``, ``overwrite_partitions``).

Unlike audit logging, watermark state is correctness-relevant, so failures
reading or writing it fail the run instead of being swallowed.
"""

from etl_framework.catalog import ensure_schema
from etl_framework.exceptions import ExecutionError

DEFAULT_TABLE = "etl_state.watermarks"
DEFAULT_PARAMETER = "watermark"


def validate_watermark_settings(settings: dict) -> list:
    """Structural validation of the settings.watermark block."""
    block = settings.get("watermark")
    if block is None:
        return []
    if not isinstance(block, dict):
        return ["settings.watermark must be a mapping"]
    if not block.get("enabled"):
        return []
    return [
        f"settings.watermark.{field} is required when watermarking is enabled"
        for field in ("source", "column", "initial")
        if not block.get(field)
    ]


def _sql_literal(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


class WatermarkStore:
    """Reads and upserts one watermark row per process in a Delta table."""

    def __init__(self, spark, settings_block: dict):
        self.spark = spark
        self.table = settings_block.get("table", DEFAULT_TABLE)

    def _ensure_table(self) -> None:
        if "." in self.table:
            schema_name = self.table.rsplit(".", 1)[0]
            ensure_schema(self.spark, schema_name)
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.table} "
            f"(process_name STRING, watermark STRING, updated_at TIMESTAMP) "
            f"USING delta"
        )

    def get(self, process_name: str):
        """Return the stored watermark for a process, or None on first run."""
        try:
            self._ensure_table()
            rows = (
                self.spark.table(self.table)
                .filter(f"process_name = {_sql_literal(process_name)}")
                .select("watermark")
                .collect()
            )
        except Exception as exc:
            raise ExecutionError(
                f"Pipeline '{process_name}': failed to read the watermark from "
                f"'{self.table}': {exc}"
            ) from exc
        return rows[0].watermark if rows else None

    def set(self, process_name: str, value: str) -> None:
        """Upsert the watermark for a process."""
        try:
            self._ensure_table()
            self.spark.sql(
                f"MERGE INTO {self.table} t USING (SELECT "
                f"{_sql_literal(process_name)} AS process_name, "
                f"{_sql_literal(value)} AS watermark) s "
                f"ON t.process_name = s.process_name "
                f"WHEN MATCHED THEN UPDATE SET t.watermark = s.watermark, "
                f"t.updated_at = current_timestamp() "
                f"WHEN NOT MATCHED THEN INSERT (process_name, watermark, updated_at) "
                f"VALUES (s.process_name, s.watermark, current_timestamp())"
            )
        except Exception as exc:
            raise ExecutionError(
                f"Pipeline '{process_name}': failed to persist watermark "
                f"'{value}' to '{self.table}': {exc}"
            ) from exc
