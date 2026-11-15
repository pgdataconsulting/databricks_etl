"""Secret references: credentials stay out of YAML, logs, and run summaries.

A pipeline never carries a credential. It carries a *reference* to one::

    - name: read_source_orders
      type: read
      format: jdbc
      options:
        url: jdbc:sqlserver://sales-db.example.com:1433;database=sales
        dbtable: dbo.orders
        user: etl_reader
        password: ${secret:source-systems/sales-db-password}
      alias: source_orders

``${secret:<scope>/<key>}`` is resolved at the last possible moment — when
the step that uses it executes — so the value never reaches the validated
configuration, the dry-run plan, lineage, the run summary, or the audit
tables. Any value that has been resolved is also masked wherever the
framework reports text it did not write itself (Spark error messages
frequently echo connection options).

Where the value comes from is pluggable. Two providers ship with the
framework:

* :class:`DatabricksSecretProvider` reads a Databricks secret scope (which can
  be backed by Azure Key Vault). Selected automatically on Databricks compute.
* :class:`EnvironmentSecretProvider` reads environment variables, for local
  runs and CI: ``${secret:source-systems/sales-db-password}`` is looked up as
  ``ETL_SECRET_SOURCE_SYSTEMS_SALES_DB_PASSWORD``.

Any object with a ``get(scope, key) -> str`` method can be passed to
``run_pipeline(secret_provider=...)`` instead, for example a client for a
vault the platform team already operates.
"""

import json
import os
import re
from importlib.util import find_spec

from etl_framework.exceptions import ExecutionError, SecretResolutionError

# Scope and key names follow the Databricks rules: letters, digits, dashes,
# underscores, periods and "@".
_NAME = r"[A-Za-z0-9_.@-]+"
SECRET_PATTERN = re.compile(r"\$\{secret:(" + _NAME + r")/(" + _NAME + r")\}")
# Anything that starts like a secret reference; used to report malformed ones.
_SECRET_START = re.compile(r"\$\{secret:[^}]*\}?")

REDACTED = "***"
PROVIDER_ENV_VAR = "ETL_SECRET_PROVIDER"
ENV_PREFIX = "ETL_SECRET_"

# Masking replaces a resolved value wherever its text occurs. A value of one
# to three characters occurs inside ordinary words, so masking it would
# corrupt log lines and flag unrelated errors as leaks; such values are
# resolved but not masked. Keep only real credentials behind secret
# references: a user name is a parameter, not a secret.
MIN_MASKED_LENGTH = 4

# Parameters whose names suggest a credential are masked in the `parameters`
# block of run summaries. That is all: a parameter is substituted into the
# configuration as text and shows up wherever it was used (plan, lineage).
# Credentials belong in ${secret:...} references; this is only a safety net.
_SENSITIVE_NAME = re.compile(
    r"(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|credential"
    r"|private[_-]?key|connection[_-]?string)",
    re.IGNORECASE,
)


def is_sensitive_name(name) -> bool:
    """Whether a parameter name looks like it holds a credential."""
    return bool(_SENSITIVE_NAME.search(str(name)))


def mask_parameters(parameters: dict) -> dict:
    """Stringify parameters for reporting, masking credential-like names."""
    return {
        name: REDACTED if is_sensitive_name(name) and value is not None else str(value)
        for name, value in parameters.items()
    }


def find_malformed_references(obj) -> list:
    """Return every ``${secret:...}`` occurrence that is not a valid reference."""
    malformed = []
    if isinstance(obj, str):
        for match in _SECRET_START.finditer(obj):
            if not SECRET_PATTERN.fullmatch(match.group(0)):
                malformed.append(match.group(0))
    elif isinstance(obj, dict):
        for key, value in obj.items():
            malformed.extend(find_malformed_references(key))
            malformed.extend(find_malformed_references(value))
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            malformed.extend(find_malformed_references(item))
    return malformed


def contains_secret_reference(obj) -> bool:
    """Whether the object tree contains at least one secret reference."""
    if isinstance(obj, str):
        return bool(SECRET_PATTERN.search(obj))
    if isinstance(obj, dict):
        return any(contains_secret_reference(v) for v in obj.values())
    if isinstance(obj, (list, tuple)):
        return any(contains_secret_reference(item) for item in obj)
    return False


def environment_variable_name(scope: str, key: str) -> str:
    """The environment variable an ``EnvironmentSecretProvider`` reads."""
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", f"{scope}_{key}").strip("_")
    return ENV_PREFIX + normalized.upper()


class EnvironmentSecretProvider:
    """Resolves secrets from environment variables (local runs and CI)."""

    def __init__(self, environ=None):
        self._environ = os.environ if environ is None else environ

    def get(self, scope: str, key: str) -> str:
        variable = environment_variable_name(scope, key)
        value = self._environ.get(variable)
        if value is None:
            raise SecretResolutionError(scope, key, f"environment variable '{variable}' is not set")
        return value


class DatabricksSecretProvider:
    """Resolves secrets from a Databricks secret scope through ``dbutils``."""

    def __init__(self, spark=None, dbutils=None):
        self._spark = spark
        self._dbutils = dbutils

    def _get_dbutils(self):
        if self._dbutils is None:
            try:
                from databricks.sdk.runtime import dbutils
            except ImportError:
                from pyspark.dbutils import DBUtils

                dbutils = DBUtils(self._spark)
            self._dbutils = dbutils
        return self._dbutils

    def get(self, scope: str, key: str) -> str:
        try:
            return self._get_dbutils().secrets.get(scope=scope, key=key)
        except Exception as exc:
            # The provider's own message never contains the value, only names.
            raise SecretResolutionError(scope, key, f"{type(exc).__name__}: {exc}") from exc


def on_databricks() -> bool:
    """Whether the process runs on Databricks compute.

    The ``dbruntime`` module exists only there; it is the signal the
    Databricks SDK itself uses. The runtime version variable that classic
    clusters set is honoured as well.
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True
    return find_spec("dbruntime") is not None


def default_secret_provider(spark=None):
    """Pick the provider for the current environment.

    ``ETL_SECRET_PROVIDER=databricks|env`` forces the choice; otherwise
    Databricks compute uses secret scopes and everything else uses
    environment variables.
    """
    choice = (os.environ.get(PROVIDER_ENV_VAR) or "").strip().lower()
    if choice not in ("", "databricks", "env"):
        raise ExecutionError(f"{PROVIDER_ENV_VAR} must be 'databricks' or 'env', got '{choice}'")
    if choice == "databricks" or (not choice and on_databricks()):
        return DatabricksSecretProvider(spark)
    return EnvironmentSecretProvider()


def _chain_texts(exc: BaseException) -> list:
    """The message of an exception and of every exception chained to it."""
    texts: list[str] = []
    seen: set[int] = set()
    pending: list[BaseException | None] = [exc]
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        texts.append(str(current))
        texts.extend(str(value) for value in vars(current).values() if isinstance(value, str))
        pending.extend([current.__cause__, current.__context__])
    return texts


class SecretResolver:
    """Resolves secret references on demand and masks what it resolved.

    One resolver is shared by a pipeline run and every pipeline nested inside
    it, so a value resolved anywhere in the run is masked everywhere.
    """

    def __init__(self, provider=None, spark=None):
        self._provider = provider
        self._spark = spark
        self._cache: dict[tuple[str, str], str] = {}

    def _lookup(self, scope: str, key: str) -> str:
        if (scope, key) not in self._cache:
            if self._provider is None:
                self._provider = default_secret_provider(self._spark)
            value = self._provider.get(scope, key)
            if value is None:
                raise SecretResolutionError(scope, key, "the provider returned no value")
            self._cache[(scope, key)] = str(value)
        return self._cache[(scope, key)]

    def resolve(self, obj):
        """Return a copy of the object tree with secret references replaced."""
        if isinstance(obj, str):
            return SECRET_PATTERN.sub(lambda m: self._lookup(m.group(1), m.group(2)), obj)
        if isinstance(obj, dict):
            return {key: self.resolve(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [self.resolve(item) for item in obj]
        return obj

    def _values(self) -> list:
        # Each value is masked both as-is and as it appears inside a JSON
        # string (the structured log lines), longest first so a secret that
        # contains another is masked whole.
        values = set()
        for value in self._cache.values():
            if len(value) >= MIN_MASKED_LENGTH:
                values.add(value)
                values.add(json.dumps(value)[1:-1])
        return sorted(values, key=len, reverse=True)

    def scrub_exception(self, exc: BaseException) -> BaseException:
        """Mask resolved values in an exception before it leaves the framework.

        Rewrites the exception in place and returns it. The message and every
        attribute the exception carries (the failing expression, the list of
        failed checks) are masked. What cannot be masked is dropped: the
        chained cause and context, which are usually the Spark error that
        echoed the value, and the traceback, whose frames hold the resolved
        step as a local variable.
        """
        exc.args = (self.redact(str(exc)),)
        for name, value in list(vars(exc).items()):
            if isinstance(value, BaseException):
                setattr(exc, name, None)
            elif isinstance(value, (str, list, tuple, dict)):
                setattr(exc, name, self.redact(value))
        exc.__cause__ = None
        exc.__context__ = None
        exc.__suppress_context__ = True
        exc.__traceback__ = None
        return exc

    def leaks(self, obj) -> bool:
        """Whether a resolved value occurs in a text or anywhere in an exception.

        For an exception the whole chain is inspected, not only its own
        message: a clean message can still be raised from a cause that quotes
        the value, and a traceback prints the chain.
        """
        values = self._values()
        if not values:
            return False
        texts = _chain_texts(obj) if isinstance(obj, BaseException) else [str(obj)]
        return any(value in text for text in texts for value in values)

    def redact(self, obj):
        """Mask every resolved value in a string or a JSON-like structure."""
        values = self._values()
        if not values:
            return obj
        return self._redact(obj, values)

    def _redact(self, obj, values: list):
        if isinstance(obj, str):
            for value in values:
                obj = obj.replace(value, REDACTED)
            return obj
        if isinstance(obj, dict):
            return {key: self._redact(value, values) for key, value in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._redact(item, values) for item in obj]
        return obj
