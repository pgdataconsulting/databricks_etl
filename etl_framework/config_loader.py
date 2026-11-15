"""Loads pipeline configurations from YAML files or inline YAML content.

Supports local paths, DBFS paths (``dbfs:/...`` is translated to ``/dbfs/...``
so plain Python file IO works on Databricks), and Databricks Workspace files,
which are mounted on the driver filesystem.
"""

import os

import yaml

from etl_framework.exceptions import ConfigLoadError


class _ConfigLoader(yaml.SafeLoader):
    """SafeLoader that keeps mapping keys exactly as they are written.

    YAML 1.1 reads the plain words ``on``, ``off``, ``yes`` and ``no`` as
    booleans. For a mapping *key* that is never what a pipeline means: the
    ``on:`` field of a ``lookup`` step would arrive as the key ``True`` and
    the step would be rejected for a missing ``on``. Values are left alone,
    so ``enabled: yes`` still means true.
    """

    _BOOL_TAG = "tag:yaml.org,2002:bool"
    _STR_TAG = "tag:yaml.org,2002:str"

    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            # Bring `<<` merge keys in first, so their keys are covered too.
            self.flatten_mapping(node)
        for key_node, _ in node.value:
            if isinstance(key_node, yaml.ScalarNode) and key_node.tag == self._BOOL_TAG:
                key_node.tag = self._STR_TAG
        return super().construct_mapping(node, deep=deep)


def load_yaml(text):
    """Parse YAML the way every framework file (config, template, schema) is read.

    The loader is a ``SafeLoader`` subclass, so it constructs plain data only.
    """
    return yaml.load(text, Loader=_ConfigLoader)


def _to_local_path(config_path: str) -> str:
    if config_path.startswith("dbfs:/"):
        return "/dbfs/" + config_path[len("dbfs:/") :].lstrip("/")
    return config_path


def load_config(config_path: str | None = None, config_content: str | None = None) -> dict:
    """Load a pipeline configuration.

    Exactly one of ``config_path`` (a YAML file location) or ``config_content``
    (raw YAML text, e.g. passed directly from ADF) must be provided.
    """
    if (config_path is None) == (config_content is None):
        raise ConfigLoadError(
            config_path or "<inline>",
            "provide exactly one of config_path or config_content",
        )

    if config_path is not None:
        local_path = _to_local_path(config_path)
        if not os.path.isfile(local_path):
            raise ConfigLoadError(config_path, "file does not exist")
        try:
            with open(local_path, encoding="utf-8") as handle:
                raw = handle.read()
        except OSError as exc:
            raise ConfigLoadError(config_path, f"could not read file: {exc}") from exc
    else:
        raw = config_content or ""
        config_path = "<inline>"

    try:
        config = load_yaml(raw)
    except yaml.YAMLError as exc:
        raise ConfigLoadError(config_path, f"invalid YAML: {exc}") from exc

    if not isinstance(config, dict):
        raise ConfigLoadError(
            config_path,
            f"top level of the configuration must be a mapping, got {type(config).__name__}",
        )
    return config
