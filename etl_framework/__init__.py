"""Declarative YAML-based ETL framework for Databricks.

Users define ETL pipelines in YAML configuration files. At runtime the
framework parses the YAML, validates it, translates it into PySpark
operations, and executes it step by step on a Spark cluster.
"""

__version__ = "1.0.0"

SUPPORTED_SPEC_VERSIONS = ("1.0",)

from etl_framework.runner import run_pipeline  # noqa: E402

__all__ = ["SUPPORTED_SPEC_VERSIONS", "__version__", "run_pipeline"]
