"""Resolves ``${parameter}`` placeholders in a pipeline configuration.

Parameters can be declared with defaults in the YAML ``parameters`` block and
overridden (or supplied) at runtime by Azure Data Factory or Databricks Jobs.
Every placeholder must resolve to a non-null value before execution starts.

``${secret:scope/key}`` references are a different mechanism and are left
untouched here: they are resolved when a step executes (see
:mod:`etl_framework.secrets`).
"""

import re

from etl_framework.exceptions import MissingParameterError
from etl_framework.settings import coerce_boolean_settings

PARAM_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def merge_parameters(defaults: dict, runtime: dict) -> dict:
    """Merge YAML parameter defaults with runtime parameters.

    Runtime values win. A runtime value of ``None`` is treated as "not
    provided" so it never masks a YAML default.
    """
    merged = dict(defaults or {})
    for key, value in (runtime or {}).items():
        if value is not None:
            merged[key] = value
    return merged


def collect_parameter_references(obj) -> set:
    """Return every parameter name referenced via ``${name}`` in the object tree."""
    refs = set()
    if isinstance(obj, str):
        refs.update(PARAM_PATTERN.findall(obj))
    elif isinstance(obj, dict):
        for key, value in obj.items():
            refs.update(collect_parameter_references(key))
            refs.update(collect_parameter_references(value))
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            refs.update(collect_parameter_references(item))
    return refs


def check_required_parameters(config: dict, parameters: dict) -> None:
    """Fail fast if any referenced parameter is missing or null."""
    referenced = collect_parameter_references(config.get("steps") or [])
    referenced |= collect_parameter_references(config.get("settings") or {})
    missing = [name for name in referenced if name not in parameters or parameters[name] is None]
    if missing:
        process_name = config.get("process_name", "<unknown>")
        raise MissingParameterError(process_name, missing)


def resolve_parameters(obj, parameters: dict):
    """Substitute every ``${name}`` placeholder with its parameter value.

    A string consisting solely of one placeholder keeps the parameter's native
    type; placeholders embedded in longer strings are interpolated as text.
    """
    if isinstance(obj, str):
        full_match = PARAM_PATTERN.fullmatch(obj)
        if full_match:
            return parameters[full_match.group(1)]
        return PARAM_PATTERN.sub(lambda m: str(parameters[m.group(1)]), obj)
    if isinstance(obj, dict):
        return {key: resolve_parameters(value, parameters) for key, value in obj.items()}
    if isinstance(obj, list):
        return [resolve_parameters(item, parameters) for item in obj]
    return obj


def resolve_settings_block(config: dict, runtime_parameters: dict, block: str) -> dict:
    """Resolve one ``settings`` block ahead of the full configuration.

    The runner needs the watermark settings before it can supply the
    ``${watermark}`` parameter the steps depend on, so that block is resolved
    on its own, against the parameters known at that point.
    """
    merged = merge_parameters(config.get("parameters") or {}, runtime_parameters or {})
    value = (config.get("settings") or {}).get(block) or {}
    missing = [name for name in collect_parameter_references(value) if merged.get(name) is None]
    if missing:
        raise MissingParameterError(config.get("process_name", "<unknown>"), missing)
    resolved = resolve_parameters(value, merged)
    return coerce_boolean_settings({block: resolved})[block]


def resolve_config(config: dict, runtime_parameters: dict) -> tuple:
    """Merge, verify, and substitute parameters for a full configuration.

    Returns ``(resolved_config, merged_parameters)``. The ``steps`` and
    ``settings`` sections are substituted; the ``parameters`` block itself is
    left as the record of declared defaults. Boolean settings given as the
    words "true"/"false" (what a command-line parameter delivers) become
    booleans.
    """
    merged = merge_parameters(config.get("parameters") or {}, runtime_parameters or {})
    check_required_parameters(config, merged)
    resolved = dict(config)
    resolved["steps"] = resolve_parameters(config.get("steps") or [], merged)
    if config.get("settings"):
        resolved["settings"] = coerce_boolean_settings(
            resolve_parameters(config["settings"], merged)
        )
    return resolved, merged
