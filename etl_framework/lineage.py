"""Pipeline lineage: extraction from the config and OpenLineage emission.

Because pipelines are fully declarative, complete dataset-level lineage is
known before execution: which external sources are read, how DataFrames flow
between steps, and which external targets are written. That lineage is:

* always included in the run summary returned to the orchestrator, and
* optionally emitted as OpenLineage RunEvents (START and COMPLETE/FAIL) to
  any OpenLineage-compatible backend (Marquez, Databricks, astronomer, ...)::

      settings:
        lineage:
          enabled: true
          transport: http          # http | console (default: console)
          url: https://marquez.example.com/api/v1/lineage
          api_key: ${secret:observability/marquez-token}   # optional bearer token
          namespace: datalake      # OpenLineage job namespace

  Events are built by hand against the OpenLineage spec, so no extra client
  dependency is needed; lineage failures are logged and never fail the run.

On Databricks, reads and writes through Unity Catalog tables additionally get
column-level lineage captured natively by Unity Catalog — no framework
involvement required.
"""

import json
import logging
import urllib.request
from datetime import datetime, timezone
from typing import Any

from etl_framework.operations import OPERATION_REGISTRY

_logger = logging.getLogger("etl_framework")

_PRODUCER = "https://github.com/pgdataconsulting/databricks_etl"
_SCHEMA_URL = "https://openlineage.io/spec/1-0-5/OpenLineage.json"

# Step types that touch external storage rather than in-context DataFrames.
_SOURCE_TYPES = ("read",)
_SINK_TYPES = ("write", "merge", "scd2")


def _dataset_of(step: dict) -> dict:
    return {
        "dataset": step.get("path") or step.get("table"),
        "kind": "table" if step.get("table") else "path",
        "format": step.get("format", "delta"),
        "step": step.get("name"),
        "schema_ref": step.get("schema_ref"),
    }


def extract_lineage(resolved_config: dict) -> dict:
    """Dataset-level lineage derived from a resolved configuration."""
    sources, targets, flow = [], [], []
    for step in resolved_config.get("steps") or []:
        operation_class = OPERATION_REGISTRY.get(step.get("type"))
        if operation_class is None:
            continue
        if step["type"] in _SOURCE_TYPES:
            sources.append(_dataset_of(step))
        elif step["type"] in _SINK_TYPES:
            targets.append(_dataset_of(step))
        flow.append(
            {
                "step": step.get("name"),
                "type": step.get("type"),
                "inputs": operation_class.input_names(step),
                "output": operation_class.output_name(step),
            }
        )
    return {"sources": sources, "targets": targets, "flow": flow}


def _openlineage_dataset(entry: dict, namespace: str) -> dict:
    name = entry["dataset"]
    if entry["kind"] == "path" and name and "://" in name:
        scheme, _, rest = name.partition("://")
        authority, _, path = rest.partition("/")
        return {"namespace": f"{scheme}://{authority}", "name": "/" + path}
    return {"namespace": namespace, "name": name}


class OpenLineageEmitter:
    """Emits OpenLineage RunEvents for a pipeline run."""

    def __init__(self, lineage_settings: dict | None, process_name: str, run_id: str, secrets=None):
        settings = lineage_settings or {}
        self.enabled = bool(settings.get("enabled"))
        self.transport = settings.get("transport", "console")
        # `url` and `api_key` may be ${secret:scope/key} references; they are
        # resolved only when an event is actually sent.
        self.url = settings.get("url")
        self.api_key = settings.get("api_key")
        self.namespace = settings.get("namespace", "default")
        self.process_name = process_name
        self.run_id = run_id
        self._secrets = secrets

    def _resolve(self, value):
        return self._secrets.resolve(value) if self._secrets is not None else value

    def _event(self, event_type: str, lineage: dict, error: str | None = None) -> dict:
        event: dict[str, Any] = {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "producer": _PRODUCER,
            "schemaURL": _SCHEMA_URL,
            "run": {"runId": self.run_id, "facets": {}},
            "job": {"namespace": self.namespace, "name": self.process_name},
            "inputs": [_openlineage_dataset(s, self.namespace) for s in lineage["sources"]],
            "outputs": [_openlineage_dataset(t, self.namespace) for t in lineage["targets"]],
        }
        if error:
            event["run"]["facets"]["errorMessage"] = {
                "_producer": _PRODUCER,
                "_schemaURL": _SCHEMA_URL,
                "message": error,
                "programmingLanguage": "python",
            }
        return event

    def _send(self, event: dict) -> None:
        payload = json.dumps(event)
        if self.transport == "http":
            request = urllib.request.Request(
                self._resolve(self.url),
                data=payload.encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            if self.api_key:
                request.add_header("Authorization", f"Bearer {self._resolve(self.api_key)}")
            urllib.request.urlopen(request, timeout=10).read()
        else:
            _logger.info("OpenLineage event: %s", payload)

    def emit(self, event_type: str, lineage: dict, error: str | None = None) -> None:
        """Emit one event; lineage problems never fail the pipeline."""
        if not self.enabled:
            return
        try:
            self._send(self._event(event_type, lineage, error))
        except Exception as exc:  # noqa: BLE001 - lineage must never fail the run
            _logger.warning("Failed to emit OpenLineage %s event: %s", event_type, exc)
