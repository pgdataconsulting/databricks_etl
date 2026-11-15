"""Operation registry: maps YAML step types to their handler classes.

New operation types plug in without modifying the framework, in either of
two ways:

* call :func:`register_operation` with an :class:`Operation` subclass, or
* ship the subclass in any installed package and declare it under the
  ``etl_framework.operations`` entry-point group, in which case installing
  the package is enough for the type to be available::

      [project.entry-points."etl_framework.operations"]
      mask_pii = "acme_etl_plugins.mask_pii:MaskPiiOperation"
"""

import logging
from importlib.metadata import entry_points

from etl_framework.operations.aggregate import AggregateOperation
from etl_framework.operations.base import Operation
from etl_framework.operations.cache import CacheOperation
from etl_framework.operations.cdc import CdcOperation
from etl_framework.operations.deduplicate import DeduplicateOperation
from etl_framework.operations.expression import AddColumnsOperation, ExpressionOperation
from etl_framework.operations.filter import FilterOperation
from etl_framework.operations.fuzzy_match import FuzzyMatchOperation
from etl_framework.operations.join import JoinOperation
from etl_framework.operations.lookup import LookupOperation
from etl_framework.operations.merge import MergeOperation
from etl_framework.operations.pivot import PivotOperation
from etl_framework.operations.quality import QualityOperation
from etl_framework.operations.read import ReadOperation
from etl_framework.operations.reconcile import ReconcileOperation
from etl_framework.operations.run_config import RunConfigOperation
from etl_framework.operations.sample import SampleOperation
from etl_framework.operations.scd2 import Scd2Operation
from etl_framework.operations.select import SelectOperation
from etl_framework.operations.sql import SqlOperation
from etl_framework.operations.union import UnionOperation
from etl_framework.operations.unpivot import UnpivotOperation
from etl_framework.operations.window import WindowOperation
from etl_framework.operations.write import WriteOperation

PLUGIN_ENTRY_POINT_GROUP = "etl_framework.operations"

_logger = logging.getLogger("etl_framework")

OPERATION_REGISTRY: dict[str, type[Operation]] = {}
_BUILT_IN_TYPES: set[str] = set()


def register_operation(operation_class) -> None:
    """Register an Operation subclass under its ``type_name``."""
    if not operation_class.type_name:
        raise ValueError(f"{operation_class.__name__} must define a type_name")
    OPERATION_REGISTRY[operation_class.type_name] = operation_class


def load_plugin_operations(group: str = PLUGIN_ENTRY_POINT_GROUP) -> list:
    """Register operations that installed packages declare as entry points.

    Returns the type names that were registered. A plugin that fails to load,
    is not an Operation subclass, or tries to replace a built-in operation is
    skipped with a warning: one broken plugin must not take down pipelines
    that never use it, and a pipeline that does use it then fails validation
    with an "unsupported operation type" error naming the step.
    """
    registered: list[str] = []
    for entry_point in entry_points(group=group):
        try:
            operation_class = entry_point.load()
            if not (isinstance(operation_class, type) and issubclass(operation_class, Operation)):
                raise TypeError("the entry point is not an Operation subclass")
            if not operation_class.type_name:
                raise ValueError("the operation does not define a type_name")
            if operation_class.type_name in _BUILT_IN_TYPES:
                raise ValueError(
                    f"'{operation_class.type_name}' is a built-in operation "
                    f"and cannot be replaced by a plugin"
                )
        except Exception as exc:  # noqa: BLE001 - see docstring
            _logger.warning(
                "Skipping operation plugin '%s' (%s): %s",
                entry_point.name,
                entry_point.value,
                exc,
            )
            continue
        register_operation(operation_class)
        registered.append(operation_class.type_name)
    return registered


def get_operation_class(step_type: str):
    return OPERATION_REGISTRY.get(step_type)


def supported_operation_types() -> list:
    return sorted(OPERATION_REGISTRY)


for _operation in (
    ReadOperation,
    WriteOperation,
    SelectOperation,
    FilterOperation,
    JoinOperation,
    AggregateOperation,
    ExpressionOperation,
    AddColumnsOperation,
    DeduplicateOperation,
    SqlOperation,
    MergeOperation,
    Scd2Operation,
    QualityOperation,
    CdcOperation,
    ReconcileOperation,
    RunConfigOperation,
    UnionOperation,
    PivotOperation,
    UnpivotOperation,
    FuzzyMatchOperation,
    WindowOperation,
    SampleOperation,
    LookupOperation,
    CacheOperation,
):
    register_operation(_operation)

_BUILT_IN_TYPES.update(OPERATION_REGISTRY)
load_plugin_operations()

__all__ = [
    "OPERATION_REGISTRY",
    "PLUGIN_ENTRY_POINT_GROUP",
    "Operation",
    "get_operation_class",
    "load_plugin_operations",
    "register_operation",
    "supported_operation_types",
]
