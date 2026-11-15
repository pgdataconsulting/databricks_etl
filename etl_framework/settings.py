"""What the framework knows about the ``settings`` block of a configuration.

Settings may reference runtime parameters (``enabled: ${incremental}``). A
parameter that arrives from a command line or a job is a string, and the
string ``"false"`` is truthy, so the boolean settings are normalized here
after substitution and before anything reads them.
"""

# Stand-in for a parameter that has no value during `etl-framework validate`.
# Structural validation does not depend on real parameter values, and checks
# that would need one (is this setting a boolean?) are skipped for it.
VALIDATION_PLACEHOLDER = "__validation_placeholder__"

# Settings that are switches, as paths into the settings block.
BOOLEAN_SETTINGS = (
    ("log_row_counts",),
    ("allow_sql",),
    ("enforce_nullability",),
    ("audit", "enabled"),
    ("watermark", "enabled"),
    ("lineage", "enabled"),
)

_BOOLEAN_WORDS = {"true": True, "false": False}


def _as_boolean(value):
    """Turn the words true/false into booleans; leave anything else alone."""
    if isinstance(value, str):
        return _BOOLEAN_WORDS.get(value.strip().lower(), value)
    return value


def get_setting(settings: dict, path: tuple):
    """Read a nested setting; None when any level is missing or not a mapping."""
    value: object = settings
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _coerce_at(mapping: dict, path: tuple) -> None:
    key, rest = path[0], path[1:]
    if key not in mapping:
        return
    if not rest:
        mapping[key] = _as_boolean(mapping[key])
    elif isinstance(mapping[key], dict):
        mapping[key] = dict(mapping[key])  # copy, so the caller's settings stay untouched
        _coerce_at(mapping[key], rest)


def coerce_boolean_settings(settings):
    """Return the settings with "true"/"false" strings turned into booleans.

    Only the known switches (and ``audit_columns``, which is a switch or a
    list) are touched. A value that is neither a boolean nor one of the two
    words is left as it is and reported by validation.
    """
    if not isinstance(settings, dict):
        return settings
    coerced = dict(settings)
    for path in BOOLEAN_SETTINGS:
        _coerce_at(coerced, path)
    _coerce_at(coerced, ("audit_columns",))
    return coerced


def boolean_setting_errors(settings: dict) -> list:
    """Validation errors for switches that are not booleans."""
    errors = []
    for path in BOOLEAN_SETTINGS:
        value = get_setting(settings, path)
        if value is None or isinstance(value, bool) or value == VALIDATION_PLACEHOLDER:
            continue
        errors.append(f"settings.{'.'.join(path)} must be true or false, got {value!r}")
    return errors
