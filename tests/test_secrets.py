"""Secret references: resolved late, never reported, masked when echoed."""

import json

import pytest

from etl_framework.config_validator import validate_config
from etl_framework.exceptions import (
    ExecutionError,
    SecretResolutionError,
    StepExecutionError,
    ValidationError,
)
from etl_framework.lineage import OpenLineageEmitter, extract_lineage
from etl_framework.runner import run_pipeline
from etl_framework.secrets import (
    REDACTED,
    DatabricksSecretProvider,
    EnvironmentSecretProvider,
    SecretResolver,
    contains_secret_reference,
    default_secret_provider,
    environment_variable_name,
    find_malformed_references,
    mask_parameters,
)

SECRET_VALUE = "v3ry-s3cr3t-folder"


class RecordingProvider:
    """A provider backed by a dict that records every lookup."""

    def __init__(self, values):
        self.values = values
        self.calls = []

    def get(self, scope, key):
        self.calls.append((scope, key))
        if (scope, key) not in self.values:
            raise SecretResolutionError(scope, key, "not found")
        return self.values[(scope, key)]


class ForbiddenProvider:
    def get(self, scope, key):
        raise AssertionError("no secret may be resolved here")


@pytest.fixture()
def provider():
    return RecordingProvider({("demo", "folder"): SECRET_VALUE})


# --- providers ---------------------------------------------------------------


def test_environment_variable_name_is_normalized():
    assert (
        environment_variable_name("source-systems", "sales-db.password")
        == "ETL_SECRET_SOURCE_SYSTEMS_SALES_DB_PASSWORD"
    )


def test_environment_provider_reads_the_variable():
    provider = EnvironmentSecretProvider({"ETL_SECRET_DEMO_FOLDER": "x"})
    assert provider.get("demo", "folder") == "x"


def test_environment_provider_names_the_missing_variable():
    with pytest.raises(SecretResolutionError) as excinfo:
        EnvironmentSecretProvider({}).get("demo", "folder")
    assert "ETL_SECRET_DEMO_FOLDER" in str(excinfo.value)


def test_databricks_provider_reads_through_dbutils():
    class FakeSecrets:
        def get(self, scope, key):
            return f"{scope}:{key}"

    class FakeDbutils:
        secrets = FakeSecrets()

    provider = DatabricksSecretProvider(dbutils=FakeDbutils())
    assert provider.get("demo", "folder") == "demo:folder"


def test_databricks_provider_wraps_failures():
    class FailingSecrets:
        def get(self, scope, key):
            raise RuntimeError("Secret does not exist")

    class FakeDbutils:
        secrets = FailingSecrets()

    with pytest.raises(SecretResolutionError) as excinfo:
        DatabricksSecretProvider(dbutils=FakeDbutils()).get("demo", "folder")
    assert "demo/folder" in str(excinfo.value)
    assert "Secret does not exist" in str(excinfo.value)


@pytest.mark.parametrize(
    ("environment", "expected"),
    [
        ({}, EnvironmentSecretProvider),
        ({"DATABRICKS_RUNTIME_VERSION": "16.4"}, DatabricksSecretProvider),
        (
            {"DATABRICKS_RUNTIME_VERSION": "16.4", "ETL_SECRET_PROVIDER": "env"},
            EnvironmentSecretProvider,
        ),
        ({"ETL_SECRET_PROVIDER": "databricks"}, DatabricksSecretProvider),
    ],
)
def test_default_provider_follows_the_environment(monkeypatch, environment, expected):
    for name in ("DATABRICKS_RUNTIME_VERSION", "ETL_SECRET_PROVIDER"):
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    assert isinstance(default_secret_provider(), expected)


def test_databricks_compute_is_recognized_by_its_runtime_module(monkeypatch):
    # Serverless compute does not have to set the runtime version variable;
    # the `dbruntime` module is what marks Databricks compute.
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("ETL_SECRET_PROVIDER", raising=False)
    monkeypatch.setattr(
        "etl_framework.secrets.find_spec", lambda name: object() if name == "dbruntime" else None
    )
    assert isinstance(default_secret_provider(), DatabricksSecretProvider)


def test_default_provider_rejects_an_unknown_choice(monkeypatch):
    monkeypatch.setenv("ETL_SECRET_PROVIDER", "vault")
    with pytest.raises(ExecutionError, match="ETL_SECRET_PROVIDER"):
        default_secret_provider()


# --- resolver ----------------------------------------------------------------


def test_resolver_replaces_references_in_nested_structures(provider):
    resolver = SecretResolver(provider)
    step = {
        "options": {"password": "${secret:demo/folder}", "port": 1433},
        "paths": ["/data/${secret:demo/folder}/in", "plain"],
    }
    assert resolver.resolve(step) == {
        "options": {"password": SECRET_VALUE, "port": 1433},
        "paths": [f"/data/{SECRET_VALUE}/in", "plain"],
    }
    # The input is left untouched and the provider is asked once per secret.
    assert step["options"]["password"] == "${secret:demo/folder}"
    assert provider.calls == [("demo", "folder")]


def test_resolver_masks_only_what_it_resolved(provider):
    resolver = SecretResolver(provider)
    text = f"connection to {SECRET_VALUE} refused"
    assert resolver.redact(text) == text  # nothing resolved yet
    assert not resolver.leaks(text)

    resolver.resolve("${secret:demo/folder}")
    assert resolver.leaks(text)
    assert resolver.redact(text) == f"connection to {REDACTED} refused"
    assert resolver.redact({"a": [text], "n": 1}) == {
        "a": [f"connection to {REDACTED} refused"],
        "n": 1,
    }


def test_resolver_masks_the_json_escaped_form():
    value = 'pa"ss\\word'
    resolver = SecretResolver(RecordingProvider({("s", "k"): value}))
    resolver.resolve("${secret:s/k}")
    log_line = json.dumps({"error": f"login failed for {value}"})
    assert value not in resolver.redact(log_line)
    assert json.dumps(value)[1:-1] not in resolver.redact(log_line)


def test_malformed_references_are_found():
    assert find_malformed_references(
        {"a": "${secret:only-scope}", "b": ["${secret:ok/key}", "${secret:bad key/x}"]}
    ) == ["${secret:only-scope}", "${secret:bad key/x}"]
    assert contains_secret_reference({"a": ["x", "${secret:ok/key}"]})
    assert not contains_secret_reference({"a": "${parameter}"})


def test_validator_rejects_malformed_references():
    config = {
        "process_name": "p",
        "settings": {"lineage": {"api_key": "${secret:missing-key}"}},
        "steps": [
            {
                "name": "read",
                "type": "read",
                "format": "jdbc",
                "alias": "src",
                "options": {"url": "jdbc:x", "password": "${secret:scope-only}"},
            }
        ],
    }
    with pytest.raises(ValidationError) as excinfo:
        validate_config(config)
    message = str(excinfo.value)
    assert "step 'read': malformed secret reference '${secret:scope-only}'" in message
    assert "settings: malformed secret reference '${secret:missing-key}'" in message


def test_mask_parameters_hides_credential_like_names():
    assert mask_parameters(
        {
            "business_date": "2026-07-07",
            "db_password": "hunter2",
            "API_KEY": "abc",
            "retries": 3,
            "token": None,
        }
    ) == {
        "business_date": "2026-07-07",
        "db_password": REDACTED,
        "API_KEY": REDACTED,
        "retries": "3",
        "token": "None",
    }


# --- end to end --------------------------------------------------------------

PIPELINE = """
process_name: secret_demo
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{base}/${{secret:demo/folder}}"
    alias: orders
  - name: write_orders
    type: write
    input: orders
    format: parquet
    mode: overwrite
    path: "{base}/out"
"""


def test_secret_is_resolved_for_the_step_and_kept_out_of_everything_else(
    spark, orders_df, tmp_path, provider, caplog
):
    orders_df.write.parquet(str(tmp_path / SECRET_VALUE))
    with caplog.at_level("INFO", logger="etl_framework"):
        result = run_pipeline(
            config_content=PIPELINE.format(base=tmp_path),
            spark=spark,
            secret_provider=provider,
        )

    assert result["status"] == "succeeded"
    assert spark.read.parquet(str(tmp_path / "out")).count() == 4
    # The summary (lineage included) carries the reference, never the value.
    assert SECRET_VALUE not in json.dumps(result)
    assert result["lineage"]["sources"][0]["dataset"].endswith("${secret:demo/folder}")
    assert all(SECRET_VALUE not in record.getMessage() for record in caplog.records)


def test_dry_run_never_resolves_secrets(tmp_path):
    result = run_pipeline(
        config_content=PIPELINE.format(base=tmp_path),
        dry_run=True,
        secret_provider=ForbiddenProvider(),
    )
    assert result["status"] == "dry_run"
    assert result["plan"][0]["target"].endswith("${secret:demo/folder}")


def test_failure_that_echoes_a_secret_is_masked(spark, tmp_path, provider, caplog):
    # Nothing exists at the resolved path, so Spark fails and names the path.
    with (
        caplog.at_level("INFO", logger="etl_framework"),
        pytest.raises(StepExecutionError) as excinfo,
    ):
        run_pipeline(
            config_content=PIPELINE.format(base=tmp_path),
            spark=spark,
            secret_provider=provider,
        )

    message = str(excinfo.value)
    assert SECRET_VALUE not in message
    assert REDACTED in message
    assert "step 'read_orders'" in message
    # The Spark exception that echoed the value is not attached to the error.
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None
    assert all(SECRET_VALUE not in record.getMessage() for record in caplog.records)
    assert any("pipeline_finished" in record.getMessage() for record in caplog.records)


def test_unresolvable_secret_fails_the_step_by_name(spark, tmp_path):
    with pytest.raises(StepExecutionError) as excinfo:
        run_pipeline(
            config_content=PIPELINE.format(base=tmp_path),
            spark=spark,
            secret_provider=RecordingProvider({}),
        )
    assert "step 'read_orders'" in str(excinfo.value)
    assert "Secret 'demo/folder' could not be resolved" in str(excinfo.value)


def test_secret_reference_passed_to_a_nested_pipeline_stays_a_reference(
    spark, orders_df, tmp_path, provider
):
    orders_df.write.parquet(str(tmp_path / SECRET_VALUE))
    (tmp_path / "child.yaml").write_text(f"""
process_name: child
parameters:
  folder: null
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{tmp_path}/${{folder}}"
    alias: orders
""")
    (tmp_path / "parent.yaml").write_text("""
process_name: parent
settings:
  log_row_counts: true
steps:
  - name: load
    type: run_config
    config_path: child.yaml
    parameters:
      folder: "${secret:demo/folder}"
    outputs:
      orders: orders
""")
    result = run_pipeline(
        config_path=str(tmp_path / "parent.yaml"),
        spark=spark,
        secret_provider=provider,
    )
    assert result["steps"][0]["rows_processed"] == 4
    child = result["nested_runs"][0]
    assert child["parameters"]["folder"] == "${secret:demo/folder}"
    assert SECRET_VALUE not in json.dumps(result)


def test_lineage_api_key_can_be_a_secret_reference(monkeypatch, provider):
    sent = []

    def fake_urlopen(request, timeout=None):
        sent.append(request)

        class Response:
            def read(self):
                return b"{}"

        return Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    emitter = OpenLineageEmitter(
        {
            "enabled": True,
            "transport": "http",
            "url": "https://lineage.example.com/api",
            "api_key": "${secret:demo/folder}",
        },
        process_name="p",
        run_id="r",
        secrets=SecretResolver(provider),
    )
    emitter.emit("START", extract_lineage({"steps": []}))
    assert sent[0].get_header("Authorization") == f"Bearer {SECRET_VALUE}"


def test_framework_error_that_quotes_a_secret_is_scrubbed(spark, orders_df, provider):
    # An operation's own error quotes the failing expression, which here
    # contains a resolved secret. The error keeps its type and step context
    # but loses the value and the Spark exception that came with it.
    from etl_framework.exceptions import ExpressionExecutionError
    from etl_framework.runner import _run_pipeline

    config = """
process_name: scrub_demo
inputs: [orders]
steps:
  - name: tag_rows
    type: expression
    input: orders
    expressions:
      tag: "no_such_function('${secret:demo/folder}')"
    output: tagged
"""
    with pytest.raises(ExpressionExecutionError) as excinfo:
        _run_pipeline(
            config_content=config,
            spark=spark,
            secret_provider=provider,
            initial_dataframes={"orders": orders_df},
        )
    message = str(excinfo.value)
    assert SECRET_VALUE not in message
    assert f"no_such_function('{REDACTED}')" in message
    assert "step 'tag_rows'" in message
    assert excinfo.value.original is None
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None
