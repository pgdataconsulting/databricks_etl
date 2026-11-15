"""Execution context shared by every step of a pipeline run.

The context stores the Spark session, runtime parameters, every registered
DataFrame, metadata about the step currently executing, and audit information.
Every operation reads its inputs from the context and registers its output
back into it.
"""

from typing import Any

from etl_framework.exceptions import MissingInputError
from etl_framework.secrets import SecretResolver


class ExecutionContext:
    def __init__(
        self,
        spark,
        process_name: str,
        parameters: dict | None = None,
        run_id: str | None = None,
        environment: str | None = None,
        schema_registry=None,
        settings: dict | None = None,
        config_path: str | None = None,
        config_stack: tuple = (),
        secret_resolver: SecretResolver | None = None,
    ):
        self.spark = spark
        # Resolves ${secret:scope/key} references and masks resolved values.
        self.secrets = secret_resolver or SecretResolver(spark=spark)
        self.process_name = process_name
        self.parameters = dict(parameters or {})
        self.run_id = run_id
        self.environment = environment
        self.schema_registry = schema_registry
        self.settings = dict(settings or {})
        # Where this pipeline's config lives (for resolving relative nested
        # config paths) and the chain of configs currently running, root
        # first — used by run_config to detect circular nesting.
        self.config_path = config_path
        self.config_stack = tuple(config_stack)
        self.dataframes: dict[str, Any] = {}
        self.current_step: dict | None = None
        self.audit: dict[str, list] = {"steps": []}

    def register_dataframe(self, name: str, df) -> None:
        self.dataframes[name] = df

    def get_dataframe(self, name: str):
        if name not in self.dataframes:
            step_name = (self.current_step or {}).get("name", "<unknown>")
            raise MissingInputError(self.process_name, step_name, name, list(self.dataframes))
        return self.dataframes[name]

    def has_dataframe(self, name: str) -> bool:
        return name in self.dataframes
