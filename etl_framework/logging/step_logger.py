"""Structured logging for pipeline and step execution.

Emits one structured (JSON) log line per event so logs are searchable in the
Databricks driver log and can be surfaced back to Azure Data Factory.
"""

import json
import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timezone

_LOGGER_NAME = "etl_framework"

# Which of sys.stdout / sys.stderr the framework's log lines go to.
_stream_name = "stdout"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class _CurrentStreamHandler(logging.StreamHandler):
    """Writes to whatever ``sys.stdout`` (or ``sys.stderr``) is at emit time.

    A plain StreamHandler keeps the stream object it was created with, which
    goes stale when the stream is replaced later, and cannot be redirected.
    """

    def __init__(self):
        super().__init__()

    @property
    def stream(self):
        return getattr(sys, _stream_name)

    @stream.setter
    def stream(self, value):
        """Ignored: the stream is looked up on every emit."""


@contextmanager
def log_to_stderr():
    """Send the framework's log lines to stderr for the duration of the block.

    The command line prints the run summary to stdout; this keeps stdout
    parseable as a single JSON document.
    """
    global _stream_name
    previous, _stream_name = _stream_name, "stderr"
    try:
        yield
    finally:
        _stream_name = previous


def _build_logger() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = _CurrentStreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class StepLogger:
    """Logs structured run/step lifecycle events for one pipeline run."""

    def __init__(
        self,
        process_name: str,
        run_id: str,
        environment: str | None = None,
        config_path: str | None = None,
    ):
        self.process_name = process_name
        self.run_id = run_id
        self.environment = environment
        self.config_path = config_path
        self._logger = _build_logger()

    def _log(self, level: int, event: str, **fields) -> None:
        record = {
            "event": event,
            "process_name": self.process_name,
            "run_id": self.run_id,
            "environment": self.environment,
            "timestamp": _utc_now(),
        }
        record.update({k: v for k, v in fields.items() if v is not None})
        self._logger.log(level, json.dumps(record, default=str))

    def run_started(self) -> None:
        self._log(logging.INFO, "pipeline_started", config_path=self.config_path)

    def run_finished(
        self, status: str, start_time: str, end_time: str, error: str | None = None
    ) -> None:
        self._log(
            logging.INFO if status == "succeeded" else logging.ERROR,
            "pipeline_finished",
            status=status,
            start_time=start_time,
            end_time=end_time,
            error=error,
        )

    def run_metrics(self, metrics: dict) -> None:
        self._log(logging.INFO, "pipeline_metrics", **metrics)

    def step_started(self, step_name: str, step_type: str, inputs: list) -> None:
        self._log(
            logging.INFO, "step_started", step_name=step_name, step_type=step_type, inputs=inputs
        )

    def step_retrying(
        self,
        step_name: str,
        step_type: str,
        attempt: int,
        max_attempts: int,
        delay_seconds: float,
        error: str,
    ) -> None:
        self._log(
            logging.WARNING,
            "step_retrying",
            step_name=step_name,
            step_type=step_type,
            attempt=attempt,
            max_attempts=max_attempts,
            retry_delay_seconds=delay_seconds,
            error=error,
        )

    def step_finished(
        self,
        step_name: str,
        step_type: str,
        inputs: list,
        output: str,
        status: str,
        duration_seconds: float,
        rows_processed: int | None = None,
        error: str | None = None,
    ) -> None:
        self._log(
            logging.INFO if status == "succeeded" else logging.ERROR,
            "step_finished",
            step_name=step_name,
            step_type=step_type,
            inputs=inputs,
            output=output,
            status=status,
            duration_seconds=round(duration_seconds, 3),
            rows_processed=rows_processed,
            error=error,
        )
