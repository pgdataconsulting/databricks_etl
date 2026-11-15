"""Reusable YAML step templates.

A template is a YAML file containing a parameterized sequence of steps::

    # templates/ingest_delta.yaml
    template_name: ingest_delta
    template_parameters: [source_path, alias]
    steps:
      - name: read_${alias}
        type: read
        format: delta
        path: ${source_path}
        alias: ${alias}
      - name: check_${alias}
        type: quality
        input: ${alias}
        checks:
          - {type: row_count, min: 1}
        output: ${alias}_checked

Pipelines expand a template in place of a step entry::

    steps:
      - use_template: ingest_delta
        with:
          source_path: abfss://datalake/silver/orders
          alias: orders
      - name: next_step
        input: orders_checked
        ...

Only the placeholders named in ``with`` (which must cover every declared
``template_parameter``) are substituted at expansion time; other ``${...}``
references are left for runtime parameter resolution, so templates can use
runtime parameters like ``${business_date}`` too. Templates cannot include
other templates.

Template files live in the nearest ``templates/`` directory at or above the
config file, or wherever ``settings.template_base_path`` / the
``template_base_path`` argument points — the same convention as schemas.
Templates are expanded before parameters are resolved, so
``settings.template_base_path`` must be a literal path.
"""

import os
import re

import yaml

from etl_framework.config_loader import load_yaml
from etl_framework.exceptions import TemplateError


def _substitute(obj, arguments: dict):
    """Replace ${name} placeholders for the given argument names only."""
    if isinstance(obj, str):
        names = "|".join(re.escape(name) for name in arguments)
        pattern = re.compile(r"\$\{(" + names + r")\}")
        full_match = pattern.fullmatch(obj)
        if full_match:
            return arguments[full_match.group(1)]
        return pattern.sub(lambda m: str(arguments[m.group(1)]), obj)
    if isinstance(obj, dict):
        return {key: _substitute(value, arguments) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_substitute(item, arguments) for item in obj]
    return obj


class TemplateLoader:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self._cache: dict[str, dict] = {}

    def _load(self, template_name: str) -> dict:
        if template_name in self._cache:
            return self._cache[template_name]
        tried = []
        for suffix in (".yaml", ".yml"):
            path = os.path.join(self.base_path, template_name + suffix)
            tried.append(path)
            if os.path.isfile(path):
                break
        else:
            raise TemplateError(
                template_name, f"template file not found (tried: {', '.join(tried)})"
            )
        try:
            with open(path, encoding="utf-8") as handle:
                template = load_yaml(handle)
        except (OSError, yaml.YAMLError) as exc:
            raise TemplateError(template_name, f"could not load '{path}': {exc}") from exc

        if (
            not isinstance(template, dict)
            or not isinstance(template.get("steps"), list)
            or not template["steps"]
        ):
            raise TemplateError(
                template_name, "template must be a mapping with a non-empty 'steps' list"
            )
        for step in template["steps"]:
            if isinstance(step, dict) and step.get("use_template"):
                raise TemplateError(template_name, "templates cannot include other templates")
        self._cache[template_name] = template
        return template

    def expand(self, template_name: str, arguments: dict) -> list:
        template = self._load(template_name)
        declared = template.get("template_parameters") or []
        missing = [name for name in declared if name not in arguments]
        if missing:
            raise TemplateError(
                template_name,
                f"missing template parameter(s): {', '.join(sorted(missing))}",
            )
        return [_substitute(step, arguments) for step in template["steps"]]


def expand_templates(config: dict, template_base_path) -> dict:
    """Replace every `use_template` step entry with the template's steps."""
    steps = config.get("steps")
    if not isinstance(steps, list):
        return config
    if not any(isinstance(s, dict) and s.get("use_template") for s in steps):
        return config
    if not template_base_path:
        raise TemplateError(
            "<unknown>",
            "use_template is used but no template base path is configured "
            "(pass template_base_path to run_pipeline, set "
            "settings.template_base_path, or keep a 'templates' directory "
            "next to the config file)",
        )

    if "${" in str(template_base_path):
        raise TemplateError(
            "<unknown>",
            f"settings.template_base_path is '{template_base_path}', but it cannot "
            f"reference parameters: templates are expanded before parameters are "
            f"resolved. Use a literal path, or pass template_base_path to run_pipeline",
        )

    loader = TemplateLoader(template_base_path)
    expanded_steps = []
    for step in steps:
        if isinstance(step, dict) and step.get("use_template"):
            expanded_steps.extend(loader.expand(step["use_template"], step.get("with") or {}))
        else:
            expanded_steps.append(step)
    expanded = dict(config)
    expanded["steps"] = expanded_steps
    return expanded
