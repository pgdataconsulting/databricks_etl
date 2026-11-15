"""Errors raised while loading, resolving, or validating a pipeline configuration.

All of these fail fast: they are raised before any step is executed, so an
invalid configuration can never trigger a write.
"""


class ConfigError(Exception):
    """Base class for all configuration-related errors."""


class ConfigLoadError(ConfigError):
    """The YAML file could not be found, read, or parsed."""

    def __init__(self, config_path: str, reason: str):
        self.config_path = config_path
        self.reason = reason
        super().__init__(f"Failed to load configuration '{config_path}': {reason}")


class ValidationError(ConfigError):
    """The configuration is structurally invalid.

    Collects every validation problem found so users can fix them all at once
    instead of replaying the pipeline error by error.
    """

    def __init__(self, process_name: str, errors: list):
        self.process_name = process_name
        self.errors = list(errors)
        details = "\n".join(f"  - {e}" for e in self.errors)
        super().__init__(
            f"Invalid configuration for pipeline '{process_name}' "
            f"({len(self.errors)} error(s)):\n{details}"
        )


class SchemaContractError(ConfigError):
    """A referenced schema contract file is missing or invalid."""

    def __init__(self, schema_ref: str, reason: str):
        self.schema_ref = schema_ref
        self.reason = reason
        super().__init__(f"Schema contract '{schema_ref}': {reason}")


class TemplateError(ConfigError):
    """A referenced step template is missing, invalid, or misused."""

    def __init__(self, template_name: str, reason: str):
        self.template_name = template_name
        self.reason = reason
        super().__init__(f"Template '{template_name}': {reason}")


class MissingParameterError(ConfigError):
    """A ${parameter} referenced in the configuration was not provided."""

    def __init__(self, process_name: str, missing: list):
        self.process_name = process_name
        self.missing = sorted(set(missing))
        super().__init__(
            f"Pipeline '{process_name}' references parameters that were not "
            f"provided at runtime: {', '.join(self.missing)}"
        )
