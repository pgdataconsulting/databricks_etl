"""Environment-specific configuration overrides.

Pipelines can declare per-environment overrides for parameter defaults and
settings, keeping a single YAML file valid for dev/test/prod::

    parameters:
      environment: "dev"
      target_path: "abfss://dev-lake/gold/orders"
    settings:
      log_row_counts: true
    environments:
      prod:
        parameters:
          target_path: "abfss://prod-lake/gold/orders"
        settings:
          log_row_counts: false
          audit: {enabled: true}

The active environment is chosen by the ``environment`` runtime parameter
(falling back to the YAML default). Precedence, lowest to highest: YAML
``parameters`` defaults → environment overrides → explicit runtime
parameters. Steps themselves are never overridden — environment differences
must flow through parameters, keeping the pipeline logic identical everywhere.
"""

OVERRIDABLE_SECTIONS = ("parameters", "settings")


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def validate_environments_block(config: dict) -> list:
    """Return validation errors for the optional top-level environments block."""
    environments = config.get("environments")
    if environments is None:
        return []
    if not isinstance(environments, dict):
        return ["top-level 'environments' must be a mapping of environment name to overrides"]
    errors = []
    for name, overrides in environments.items():
        if not isinstance(overrides, dict):
            errors.append(f"environments.{name} must be a mapping")
            continue
        unknown = [k for k in overrides if k not in OVERRIDABLE_SECTIONS]
        if unknown:
            errors.append(
                f"environments.{name} contains unsupported section(s) "
                f"{', '.join(unknown)}. Only {' and '.join(OVERRIDABLE_SECTIONS)} "
                f"can be overridden per environment"
            )
        for section in OVERRIDABLE_SECTIONS:
            if section in overrides and not isinstance(overrides[section], dict):
                errors.append(f"environments.{name}.{section} must be a mapping")
    return errors


def select_environment(config: dict, runtime_parameters: dict):
    """The active environment name: runtime parameter, then YAML default."""
    runtime_value = (runtime_parameters or {}).get("environment")
    if runtime_value is not None:
        return runtime_value
    return (config.get("parameters") or {}).get("environment")


def apply_environment_overrides(config: dict, environment) -> dict:
    """Merge the active environment's overrides into parameters and settings.

    Environments without an entry in the block get the base configuration
    unchanged, so only environments that differ need to be listed.
    """
    environments = config.get("environments") or {}
    overrides = environments.get(environment) if environment else None
    if not overrides:
        return config
    merged = dict(config)
    for section in OVERRIDABLE_SECTIONS:
        if overrides.get(section):
            merged[section] = _deep_merge(config.get(section) or {}, overrides[section])
    return merged
