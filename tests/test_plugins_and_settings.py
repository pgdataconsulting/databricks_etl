"""Operation plugins, parameterized settings, and base-directory discovery."""

import pytest

import etl_framework.operations as operations
from etl_framework.cli import validate_command
from etl_framework.exceptions import MissingParameterError
from etl_framework.operations import (
    OPERATION_REGISTRY,
    Operation,
    load_plugin_operations,
)
from etl_framework.operations.filter import FilterOperation
from etl_framework.parameter_resolver import resolve_config
from etl_framework.runner import MAX_BASE_PATH_PARENTS, _resolve_base_path, run_pipeline

# --- operation plugins -------------------------------------------------------


class UppercaseOperation(Operation):
    """A third-party operation, as a plugin package would define it."""

    type_name = "uppercase"
    required_fields = ("input", "column", "output")

    def execute(self, context, step):
        from pyspark.sql.functions import col, upper

        df = context.get_dataframe(step["input"])
        result = df.withColumn(step["column"], upper(col(step["column"])))
        context.register_dataframe(step["output"], result)
        return result


class FakeEntryPoint:
    def __init__(self, name, target):
        self.name = name
        self.value = f"plugin_package:{name}"
        self._target = target

    def load(self):
        if isinstance(self._target, Exception):
            raise self._target
        return self._target


@pytest.fixture()
def plugin_entry_points(monkeypatch):
    """Install fake entry points and undo any registration afterwards."""
    before = dict(OPERATION_REGISTRY)

    def install(*entry_points):
        monkeypatch.setattr(operations, "entry_points", lambda group: list(entry_points))

    yield install
    OPERATION_REGISTRY.clear()
    OPERATION_REGISTRY.update(before)


def test_plugin_operation_is_registered_and_runs(spark, customers_df, plugin_entry_points):
    plugin_entry_points(FakeEntryPoint("uppercase", UppercaseOperation))
    assert load_plugin_operations() == ["uppercase"]

    result, context = _run_with_input(
        spark,
        customers_df,
        """
process_name: plugin_demo
inputs: [customers]
steps:
  - name: shout
    type: uppercase
    input: customers
    column: customer_name
    output: shouted
""",
    )
    assert result["steps"][0]["step_type"] == "uppercase"
    names = sorted(row.customer_name for row in context.get_dataframe("shouted").collect())
    assert names == ["ALICE", "BOB", "CAROL"]


def _run_with_input(spark, df, config):
    from etl_framework.runner import _run_pipeline

    return _run_pipeline(
        config_content=config,
        spark=spark,
        initial_dataframes={"customers": df},
    )


def test_broken_plugins_are_skipped_with_a_warning(plugin_entry_points, caplog):
    class ReplacesFilter(Operation):
        type_name = "filter"

        def execute(self, context, step):
            raise AssertionError("must never run")

    class Unnamed(Operation):
        def execute(self, context, step):
            raise AssertionError("must never run")

    plugin_entry_points(
        FakeEntryPoint("import_error", ImportError("No module named 'missing_dependency'")),
        FakeEntryPoint("not_an_operation", dict),
        FakeEntryPoint("replaces_builtin", ReplacesFilter),
        FakeEntryPoint("unnamed", Unnamed),
    )
    with caplog.at_level("WARNING", logger="etl_framework"):
        assert load_plugin_operations() == []

    assert OPERATION_REGISTRY["filter"] is FilterOperation
    warnings = [record.getMessage() for record in caplog.records]
    assert len(warnings) == 4
    assert any("missing_dependency" in w for w in warnings)
    assert any("not an Operation subclass" in w for w in warnings)
    assert any("built-in operation" in w for w in warnings)
    assert any("type_name" in w for w in warnings)


# --- parameterized settings --------------------------------------------------


def test_settings_placeholders_are_resolved():
    config = {
        "process_name": "p",
        "parameters": {"catalog": "etl_dev"},
        "settings": {
            "audit": {"enabled": True, "schema": "${catalog}.etl_audit"},
            "log_row_counts": True,
        },
        "steps": [{"name": "s", "type": "read", "table": "${catalog}.silver.orders"}],
    }
    resolved, _ = resolve_config(config, {"catalog": "etl_prod"})
    assert resolved["settings"]["audit"]["schema"] == "etl_prod.etl_audit"
    assert resolved["settings"]["log_row_counts"] is True
    assert resolved["steps"][0]["table"] == "etl_prod.silver.orders"
    # The input configuration is not modified.
    assert config["settings"]["audit"]["schema"] == "${catalog}.etl_audit"


def test_missing_parameter_in_settings_fails_fast():
    config = {
        "process_name": "p",
        "settings": {"audit": {"schema": "${catalog}.etl_audit"}},
        "steps": [{"name": "s", "type": "read"}],
    }
    with pytest.raises(MissingParameterError, match="catalog"):
        resolve_config(config, {})


def test_watermark_table_can_be_parameterized(spark, orders_df, tmp_path):
    source = str(tmp_path / "orders")
    orders_df.write.parquet(source)
    config = f"""
process_name: parameterized_watermark
parameters:
  state_schema: null
settings:
  watermark:
    enabled: true
    source: new_orders
    column: order_date
    initial: "1900-01-01"
    table: "${{state_schema}}.watermarks"
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: incremental_filter
    type: filter
    input: orders
    condition: "order_date > '${{watermark}}'"
    output: new_orders
"""
    spark.sql("DROP TABLE IF EXISTS etl_state_param.watermarks")
    result = run_pipeline(
        config_content=config,
        spark=spark,
        parameters={"state_schema": "etl_state_param"},
    )
    assert result["watermark"] == {"previous": None, "new": "2026-07-07"}
    stored = spark.table("etl_state_param.watermarks").collect()
    assert [(row.process_name, row.watermark) for row in stored] == [
        ("parameterized_watermark", "2026-07-07"),
    ]


def test_cli_validate_accepts_parameterized_settings(tmp_path, capsys):
    config = tmp_path / "settings.yaml"
    config.write_text("""
process_name: parameterized_settings
parameters:
  catalog: null
settings:
  audit: {enabled: true, schema: "${catalog}.etl_audit"}
steps:
  - name: read_orders
    type: read
    format: table
    table: "${catalog}.silver.orders"
    alias: orders
""")
    assert validate_command([str(config)]) == 0
    assert "OK" in capsys.readouterr().out


# --- schemas/ and templates/ discovery ---------------------------------------


def test_base_directory_is_found_above_grouped_configs(tmp_path):
    (tmp_path / "schemas").mkdir()
    config = tmp_path / "configs" / "medallion" / "silver.yaml"
    config.parent.mkdir(parents=True)
    config.write_text("process_name: p\n")
    assert _resolve_base_path(None, None, str(config), "schemas") == str(tmp_path / "schemas")
    assert _resolve_base_path(None, None, str(config), "templates") is None


def test_nearest_base_directory_wins(tmp_path):
    (tmp_path / "schemas").mkdir()
    nearest = tmp_path / "configs" / "schemas"
    nearest.mkdir(parents=True)
    config = tmp_path / "configs" / "pipeline.yaml"
    config.write_text("process_name: p\n")
    assert _resolve_base_path(None, None, str(config), "schemas") == str(nearest)


def test_explicit_and_settings_paths_take_precedence(tmp_path):
    (tmp_path / "schemas").mkdir()
    config = tmp_path / "pipeline.yaml"
    config.write_text("process_name: p\n")
    assert _resolve_base_path("/explicit", "/settings", str(config), "schemas") == "/explicit"
    assert _resolve_base_path(None, "/settings", str(config), "schemas") == "/settings"


def test_base_directory_search_is_bounded(tmp_path):
    (tmp_path / "schemas").mkdir()
    deep = tmp_path.joinpath(*[f"level{i}" for i in range(MAX_BASE_PATH_PARENTS + 1)])
    deep.mkdir(parents=True)
    config = deep / "pipeline.yaml"
    config.write_text("process_name: p\n")
    assert _resolve_base_path(None, None, str(config), "schemas") is None


# --- YAML keys, quarantine tables, framework schemas -------------------------


def test_yaml_keys_that_look_like_booleans_stay_keys(spark, customers_df, orders_df):
    # `on:` is the documented field of a lookup step; YAML 1.1 would read the
    # bare word as the boolean true and the step would lose its `on`.
    from etl_framework.config_loader import load_config
    from etl_framework.runner import _run_pipeline

    config = """
process_name: yaml_on_key
inputs: [orders, customers]
settings:
  log_row_counts: true
steps:
  - name: add_customer
    type: lookup
    input: orders
    lookup: customers
    on:
      customer_id: customer_id
    select: [customer_name]
    defaults: {customer_name: UNKNOWN}
    output: enriched
"""
    step = load_config(config_content=config)["steps"][0]
    assert step["on"] == {"customer_id": "customer_id"}
    assert True not in step

    result, context = _run_pipeline(
        config_content=config,
        spark=spark,
        initial_dataframes={"orders": orders_df, "customers": customers_df},
    )
    assert result["steps"][0]["rows_processed"] == 4
    names = {row.customer_name for row in context.get_dataframe("enriched").collect()}
    assert names == {"Alice", "Bob", "UNKNOWN"}


def test_yaml_boolean_values_are_unchanged():
    from etl_framework.config_loader import load_yaml

    assert load_yaml("enabled: yes\nflag: off\non: 1\n") == {
        "enabled": True,
        "flag": False,
        "on": 1,
    }


def test_quarantine_target_validation():
    from etl_framework.operations.quality import QualityOperation

    base = {
        "name": "q",
        "type": "quality",
        "input": "a",
        "output": "b",
        "on_failure": "quarantine",
        "checks": [{"type": "not_null", "columns": ["id"]}],
    }
    assert any(
        "quarantine_path" in e and "quarantine_table" in e for e in QualityOperation.validate(base)
    )
    assert QualityOperation.validate({**base, "quarantine_table": "c.q.rejects"}) == []
    assert QualityOperation.validate({**base, "quarantine_path": "/q"}) == []
    assert any(
        "only one of" in e
        for e in QualityOperation.validate(
            {**base, "quarantine_path": "/q", "quarantine_table": "c.q.rejects"}
        )
    )


def test_ensure_schema_issues_no_ddl_for_an_existing_schema():
    from etl_framework.catalog import ensure_schema

    class FakeCatalog:
        def __init__(self, exists):
            self.exists = exists

        def databaseExists(self, name):
            if isinstance(self.exists, Exception):
                raise self.exists
            return self.exists

    class FakeSpark:
        def __init__(self, exists):
            self.catalog = FakeCatalog(exists)
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)

    existing = FakeSpark(True)
    ensure_schema(existing, "etl_prod.etl_audit")
    assert existing.statements == []

    missing = FakeSpark(False)
    ensure_schema(missing, "etl_audit")
    assert missing.statements == ["CREATE SCHEMA IF NOT EXISTS etl_audit"]

    # If the existence check itself is not possible, fall back to the DDL.
    unknown = FakeSpark(RuntimeError("catalog API unavailable"))
    ensure_schema(unknown, "etl_audit")
    assert unknown.statements == ["CREATE SCHEMA IF NOT EXISTS etl_audit"]


# --- settings that are parameters --------------------------------------------

SWITCHED_PIPELINE = """
process_name: {name}
parameters:
  incremental: false
  count_rows: false
  audit_on: false
settings:
  log_row_counts: ${{count_rows}}
  audit:
    enabled: ${{audit_on}}
    schema: {name}_audit
  watermark:
    enabled: ${{incremental}}
    source: orders
    column: order_date
    initial: "1900-01-01"
    table: etl_state.{name}
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
"""


@pytest.mark.parametrize("off", [False, "false", "FALSE"])
def test_switches_given_as_parameters_can_be_off(spark, orders_df, tmp_path, off):
    # A parameter from a command line or a job arrives as the string "false",
    # which is truthy. The switch must still be off.
    orders_df.write.parquet(str(tmp_path / "orders"))
    name = "switched_off"
    spark.sql(f"DROP TABLE IF EXISTS etl_state.{name}")
    spark.sql(f"DROP SCHEMA IF EXISTS {name}_audit CASCADE")
    result = run_pipeline(
        config_content=SWITCHED_PIPELINE.format(name=name, source=tmp_path / "orders"),
        spark=spark,
        parameters={"incremental": off, "count_rows": off, "audit_on": off},
    )
    assert "watermark" not in result
    assert result["steps"][0]["rows_processed"] is None
    assert not spark.catalog.tableExists(f"etl_state.{name}")
    assert not spark.catalog.databaseExists(f"{name}_audit")


def test_switches_given_as_parameters_can_be_on(spark, orders_df, tmp_path):
    orders_df.write.parquet(str(tmp_path / "orders"))
    name = "switched_on"
    spark.sql(f"DROP TABLE IF EXISTS etl_state.{name}")
    result = run_pipeline(
        config_content=SWITCHED_PIPELINE.format(name=name, source=tmp_path / "orders"),
        spark=spark,
        parameters={"incremental": "true", "count_rows": "true", "audit_on": "true"},
    )
    assert result["watermark"] == {"previous": None, "new": "2026-07-07"}
    assert result["steps"][0]["rows_processed"] == 4
    assert spark.table(f"{name}_audit.pipeline_runs").count() == 1
    spark.sql(f"DROP TABLE etl_state.{name}")
    spark.sql(f"DROP SCHEMA {name}_audit CASCADE")


def test_a_switch_that_is_not_a_boolean_is_rejected(tmp_path):
    from etl_framework.exceptions import ValidationError

    config = SWITCHED_PIPELINE.format(name="bad_switch", source=tmp_path)
    with pytest.raises(ValidationError) as excinfo:
        run_pipeline(config_content=config, parameters={"count_rows": "sometimes"}, dry_run=True)
    assert "settings.log_row_counts must be true or false, got 'sometimes'" in str(excinfo.value)


def test_enabled_watermark_with_missing_fields_is_a_validation_error():
    from etl_framework.exceptions import ValidationError

    config = """
process_name: incomplete_watermark
parameters:
  incremental: true
settings:
  watermark:
    enabled: ${incremental}
steps:
  - {name: read_orders, type: read, format: parquet, path: /data/orders, alias: orders}
"""
    with pytest.raises(ValidationError, match=r"settings\.watermark\.initial is required"):
        run_pipeline(config_content=config, dry_run=True)


def test_rejected_configuration_does_not_create_the_watermark_table(spark, tmp_path):
    # "Nothing was executed" has to be true: the state table is created on
    # first read, so validation must come before it.
    spark.sql("DROP TABLE IF EXISTS etl_state.never_touched")
    config = f"""
process_name: rejected_before_state
parameters:
  business_date: null
settings:
  watermark:
    enabled: true
    source: orders
    column: order_date
    initial: "1900-01-01"
    table: etl_state.never_touched
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{tmp_path}/${{business_date}}"
    alias: orders
"""
    with pytest.raises(MissingParameterError, match="business_date"):
        run_pipeline(config_content=config, spark=spark)
    assert not spark.catalog.tableExists("etl_state.never_touched")

    invalid = config.replace("type: read", "type: no_such_operation")
    with pytest.raises(Exception, match="unsupported operation type"):
        run_pipeline(config_content=invalid, spark=spark, parameters={"business_date": "x"})
    assert not spark.catalog.tableExists("etl_state.never_touched")


def test_template_base_path_cannot_be_parameterized(tmp_path):
    from etl_framework.exceptions import TemplateError

    config = """
process_name: templated
parameters:
  root: /somewhere
settings:
  template_base_path: ${root}/templates
steps:
  - use_template: ingest
    with: {alias: orders}
"""
    with pytest.raises(TemplateError, match="cannot reference parameters"):
        run_pipeline(config_content=config, dry_run=True)


def test_cli_validate_uses_declared_defaults_not_placeholders(tmp_path, capsys):
    # `validate` used to put a placeholder in every referenced parameter, which
    # overrode defaults and rejected configurations that run fine.
    contracts = tmp_path / "contracts"
    contracts.mkdir()
    (contracts / "orders.yaml").write_text(
        "dataset: orders\ncolumns:\n  - {name: order_id, type: int}\n"
    )
    config = tmp_path / "defaults.yaml"
    config.write_text(f"""
process_name: validate_defaults
parameters:
  contracts_dir: "{contracts}"
  stamp: [_run_id]
  catalog: null
settings:
  schema_base_path: ${{contracts_dir}}
  audit_columns: ${{stamp}}
  log_row_counts: ${{count_rows}}
  audit: {{enabled: true, schema: "${{catalog}}.etl_audit"}}
steps:
  - name: read_orders
    type: read
    format: table
    table: "${{catalog}}.silver.orders"
    schema_ref: orders
    alias: orders
""")
    assert validate_command([str(config)]) == 0, capsys.readouterr().out


# --- secret references nothing would resolve, YAML merge keys -----------------


def test_secret_references_are_rejected_where_they_are_never_resolved():
    from etl_framework.config_validator import validate_config
    from etl_framework.exceptions import ValidationError

    config = {
        "process_name": "p",
        "settings": {
            "audit": {"enabled": True, "schema": "${secret:demo/schema}"},
            "lineage": {"enabled": True, "api_key": "${secret:demo/token}"},
        },
        "steps": [
            {
                "name": "read",
                "type": "read",
                "format": "parquet",
                "path": "/data/${secret:demo/folder}",
                "alias": "src",
                "when": "'${secret:demo/flag}' = 'yes'",
            }
        ],
    }
    with pytest.raises(ValidationError) as excinfo:
        validate_config(config)
    assert excinfo.value.errors == [
        "settings.audit contains a secret reference, which is not resolved there. "
        "Secret references work in steps and in settings.lineage.url and "
        "settings.lineage.api_key",
        "step 'read': 'when' cannot use a secret reference. The condition is "
        "evaluated before the step's secrets are resolved",
    ]


def test_boolean_like_keys_from_a_yaml_merge_stay_keys():
    from etl_framework.config_loader import load_yaml

    loaded = load_yaml("""
defaults: &defaults
  on: {customer_id: customer_id}
anchored:
  <<: *defaults
  input: orders
inline:
  <<: {on: x}
  input: orders
""")
    assert loaded["anchored"] == {"on": {"customer_id": "customer_id"}, "input": "orders"}
    assert loaded["inline"] == {"on": "x", "input": "orders"}


def test_a_switched_off_watermark_needs_none_of_its_other_fields():
    config = """
process_name: optional_watermark
parameters:
  incremental: false
settings:
  watermark:
    enabled: ${incremental}
steps:
  - {name: read_orders, type: read, format: parquet, path: /data/orders, alias: orders}
"""
    result = run_pipeline(config_content=config, dry_run=True)
    assert result["status"] == "dry_run"
