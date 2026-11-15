"""A local, Delta-enabled Spark session for development and tests.

On Databricks the session already exists and is configured by the platform.
Off Databricks, Delta Lake has to be wired in explicitly; this helper does
that the same way for the test suite, the quickstart, and anyone developing
pipelines or operation plugins on a laptop. It needs the ``spark`` extra
(``pip install "databricks-yaml-etl[spark]"``) and a Java runtime.
"""

import os
import sys


def local_spark_session(
    warehouse_dir: str, app_name: str = "etl-framework-local", master: str = "local[2]"
):
    """Create (or return the active) local Spark session with Delta Lake.

    ``warehouse_dir`` is where managed tables are stored. Table metadata is
    kept in memory, so tables are visible for the lifetime of the session.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    # Spark starts its Python workers with whatever `python3` is first on
    # PATH, which fails when that is a different minor version than this
    # interpreter (e.g. inside a virtualenv). Pin both to this interpreter.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # Delta keeps table state in 50 partitions by default, which is
        # pure overhead for the small tables of a local run.
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
