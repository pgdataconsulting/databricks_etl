"""Optional Delta audit trail for pipeline executions.

When enabled via pipeline settings::

    settings:
      audit:
        enabled: true
        schema: etl_audit   # target schema (may be catalog-qualified), created if missing

the framework appends one row per run to ``<schema>.pipeline_runs``, one row
per step to ``<schema>.pipeline_steps``, and one row per failure to
``<schema>.pipeline_errors``. Audit failures are logged but never fail the
pipeline itself.
"""

import logging

from etl_framework.catalog import ensure_schema

_logger = logging.getLogger("etl_framework")

_RUNS_SCHEMA = (
    "process_name string, run_id string, environment string, config_path string, "
    "status string, start_time string, end_time string, error string"
)
_STEPS_SCHEMA = (
    "process_name string, run_id string, step_name string, step_type string, "
    "inputs string, output string, status string, start_time string, "
    "end_time string, duration_seconds double, rows_processed long, error string"
)
_ERRORS_SCHEMA = (
    "process_name string, run_id string, step_name string, step_type string, "
    "error_type string, error string, occurred_at string"
)


class AuditLogger:
    def __init__(self, spark, enabled: bool = False, schema: str = "etl_audit"):
        self.spark = spark
        self.enabled = enabled
        self.schema = schema
        self._schema_ready = False

    def _append(self, table: str, ddl_schema: str, row: dict) -> None:
        if not self.enabled:
            return
        try:
            if not self._schema_ready:
                ensure_schema(self.spark, self.schema)
                self._schema_ready = True
            df = self.spark.createDataFrame([row], schema=ddl_schema)
            df.write.mode("append").format("delta").saveAsTable(f"{self.schema}.{table}")
        except Exception as exc:  # noqa: BLE001 - audit must never fail the run
            _logger.warning("Audit logging to %s.%s failed: %s", self.schema, table, exc)

    def record_run(self, **row) -> None:
        self._append("pipeline_runs", _RUNS_SCHEMA, row)

    def record_step(self, **row) -> None:
        self._append("pipeline_steps", _STEPS_SCHEMA, row)

    def record_error(self, **row) -> None:
        self._append("pipeline_errors", _ERRORS_SCHEMA, row)
