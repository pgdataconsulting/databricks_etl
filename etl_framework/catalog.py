"""Small helpers for the framework's own tables (audit trail, watermarks)."""


def ensure_schema(spark, schema_name: str) -> None:
    """Create a schema only when it does not exist yet.

    The framework's audit and state schemas are normally provisioned ahead of
    time by whoever owns the catalog, and the identity a job runs as then has
    no privilege to create schemas. Checking first means no DDL is issued in
    that case; on a fresh local warehouse the schema is simply created.
    """
    try:
        if spark.catalog.databaseExists(schema_name):
            return
    except Exception:  # noqa: BLE001 - the CREATE below reports the real problem
        pass
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
