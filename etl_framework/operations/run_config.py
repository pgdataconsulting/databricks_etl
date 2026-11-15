"""Run-config operation: executes another YAML configuration as one step.

This makes processes composable: a child configuration is a complete pipeline
of its own (loaded, template-expanded, environment-resolved, validated, and
executed with the parent's Spark session), and the parent treats the whole
run as a single step. Parent DataFrames can be handed to the child through
``inputs`` and child DataFrames handed back through ``outputs``, so a config
can either be a self-contained sub-process or a reusable transformation over
the parent's data::

    - name: standardize_orders
      type: run_config
      config_path: shared/standardize.yaml   # relative to the parent config
      parameters:
        business_date: "${business_date}"
      inputs:                # child alias <- parent DataFrame
        raw: orders
      outputs:               # parent alias <- child DataFrame
        orders_clean: standardized
"""

import os

from etl_framework.config_loader import _to_local_path
from etl_framework.exceptions import ConfigError, StepExecutionError
from etl_framework.operations.base import Operation


class RunConfigOperation(Operation):
    type_name = "run_config"
    required_fields = ("config_path",)
    # A secret reference handed to the child as a parameter stays a reference
    # and is resolved by the child step that finally uses it, so the value
    # never appears among the child's parameters.
    resolves_secrets = False

    @classmethod
    def validate(cls, step: dict) -> list:
        errors = super().validate(step)
        for field, key_role, value_role in (
            ("inputs", "child alias", "parent DataFrame name"),
            ("outputs", "parent alias", "child DataFrame name"),
        ):
            mapping = step.get(field)
            if mapping is None:
                continue
            if not isinstance(mapping, dict) or not all(
                isinstance(key, str) and key and isinstance(value, str) and value
                for key, value in mapping.items()
            ):
                errors.append(f"'{field}' must be a mapping of {key_role} -> {value_role}")
        parameters = step.get("parameters")
        if parameters is not None and not isinstance(parameters, dict):
            errors.append("'parameters' must be a mapping of child parameter values")
        return errors

    @classmethod
    def input_names(cls, step: dict) -> list:
        inputs = step.get("inputs")
        return list(inputs.values()) if isinstance(inputs, dict) else []

    @classmethod
    def output_name(cls, step: dict):
        names = cls.output_names(step)
        return names[0] if len(names) == 1 else None

    @classmethod
    def output_names(cls, step: dict) -> list:
        outputs = step.get("outputs")
        return list(outputs) if isinstance(outputs, dict) else []

    @staticmethod
    def _resolve_child_path(context, config_path: str) -> str:
        """Resolve a relative child path against the parent config's directory."""
        if config_path.startswith("dbfs:/") or os.path.isabs(config_path):
            return config_path
        if context.config_path:
            parent_dir = os.path.dirname(os.path.abspath(_to_local_path(context.config_path)))
            return os.path.join(parent_dir, config_path)
        return config_path

    def execute(self, context, step: dict):
        # Imported lazily: the runner imports the operation registry at module
        # load, so a top-level import here would be circular.
        from etl_framework.runner import _run_pipeline

        # Resolve the parent-side inputs first so a missing DataFrame fails
        # with the usual MissingInputError before the child run starts.
        initial_dataframes = {
            child_name: context.get_dataframe(parent_name)
            for child_name, parent_name in (step.get("inputs") or {}).items()
        }

        parameters = dict(step.get("parameters") or {})
        if context.environment is not None:
            parameters.setdefault("environment", context.environment)

        try:
            summary, child_context = _run_pipeline(
                config_path=self._resolve_child_path(context, step["config_path"]),
                parameters=parameters,
                spark=context.spark,
                run_id=f"{context.run_id}:{step['name']}",
                initial_dataframes=initial_dataframes,
                config_stack=context.config_stack,
                secret_resolver=context.secrets,
            )
        except ConfigError as exc:
            raise StepExecutionError(
                process_name=context.process_name,
                step_name=step["name"],
                step_type=self.type_name,
                description=(
                    f"nested configuration '{step['config_path']}' could not "
                    f"be loaded or is invalid"
                ),
                original=exc,
            ) from exc

        context.audit.setdefault("nested_runs", []).append({"step_name": step["name"], **summary})

        outputs = step.get("outputs") or {}
        exported = None
        for parent_name, child_name in outputs.items():
            if not child_context.has_dataframe(child_name):
                available = ", ".join(sorted(child_context.dataframes)) or "(none)"
                raise StepExecutionError(
                    process_name=context.process_name,
                    step_name=step["name"],
                    step_type=self.type_name,
                    description=(
                        f"nested pipeline '{summary['process_name']}' did not "
                        f"produce output DataFrame '{child_name}' (was its step "
                        f"skipped?). Available DataFrames: {available}"
                    ),
                )
            exported = child_context.get_dataframe(child_name)
            context.register_dataframe(parent_name, exported)
        # With a single exported DataFrame, return it so row counting and
        # metrics apply to it; with several there is no single result.
        return exported if len(outputs) == 1 else None
