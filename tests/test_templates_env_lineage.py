"""Tests for environment overrides, YAML templates, and lineage."""

import json

import pytest

from etl_framework.cli import validate_command
from etl_framework.environment_overrides import (
    apply_environment_overrides,
    select_environment,
)
from etl_framework.exceptions import TemplateError, ValidationError
from etl_framework.lineage import OpenLineageEmitter, extract_lineage
from etl_framework.runner import run_pipeline
from etl_framework.template_loader import TemplateLoader, expand_templates

TEMPLATE = """
template_name: ingest
template_parameters: [source_path, alias]
steps:
  - name: read_${alias}
    type: read
    format: parquet
    path: ${source_path}
    alias: ${alias}
  - name: check_${alias}
    type: quality
    input: ${alias}
    checks:
      - {type: row_count, min: 1}
    output: ${alias}_checked
"""


@pytest.fixture()
def templates_dir(tmp_path):
    templates = tmp_path / "templates"
    templates.mkdir()
    (templates / "ingest.yaml").write_text(TEMPLATE)
    return templates


# --- environment overrides -------------------------------------------------------

BASE_CONFIG = {
    "process_name": "env_demo",
    "parameters": {"environment": "dev", "target": "/dev/out"},
    "settings": {"log_row_counts": True},
    "environments": {
        "prod": {
            "parameters": {"target": "/prod/out"},
            "settings": {"log_row_counts": False, "audit": {"enabled": True}},
        },
    },
}


def test_select_environment_runtime_wins():
    assert select_environment(BASE_CONFIG, {"environment": "prod"}) == "prod"
    assert select_environment(BASE_CONFIG, {}) == "dev"


def test_overrides_applied_for_matching_environment():
    merged = apply_environment_overrides(BASE_CONFIG, "prod")
    assert merged["parameters"]["target"] == "/prod/out"
    assert merged["parameters"]["environment"] == "dev"  # untouched default
    assert merged["settings"]["log_row_counts"] is False
    assert merged["settings"]["audit"] == {"enabled": True}


def test_unlisted_environment_is_unchanged():
    assert apply_environment_overrides(BASE_CONFIG, "test") is BASE_CONFIG


def test_environments_block_validation():
    from etl_framework.config_validator import validate_config

    config = {
        "process_name": "p",
        "environments": {"prod": {"steps": [], "parameters": {}}},
        "steps": [{"name": "r", "type": "read", "format": "parquet", "path": "/x", "alias": "a"}],
    }
    with pytest.raises(ValidationError, match="unsupported section"):
        validate_config(config)


def test_environment_overrides_end_to_end(spark, orders_df, tmp_path):
    source = str(tmp_path / "src")
    orders_df.write.parquet(source)
    config = f"""
process_name: env_e2e
parameters:
  environment: "dev"
  target_path: "{tmp_path / "dev_out"}"
environments:
  prod:
    parameters:
      target_path: "{tmp_path / "prod_out"}"
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: write_orders
    type: write
    input: orders
    format: parquet
    mode: overwrite
    path: "${{target_path}}"
"""
    run_pipeline(config_content=config, spark=spark, parameters={"environment": "prod"})
    assert spark.read.parquet(str(tmp_path / "prod_out")).count() == 4

    run_pipeline(config_content=config, spark=spark)
    assert spark.read.parquet(str(tmp_path / "dev_out")).count() == 4


# --- templates -------------------------------------------------------------------


def test_template_expansion(templates_dir):
    config = {
        "process_name": "tpl",
        "steps": [
            {"use_template": "ingest", "with": {"source_path": "/data/orders", "alias": "orders"}},
            {
                "name": "done",
                "type": "select",
                "input": "orders_checked",
                "columns": ["a"],
                "output": "final",
            },
        ],
    }
    expanded = expand_templates(config, str(templates_dir))
    names = [s["name"] for s in expanded["steps"]]
    assert names == ["read_orders", "check_orders", "done"]
    assert expanded["steps"][0]["path"] == "/data/orders"
    assert expanded["steps"][1]["output"] == "orders_checked"


def test_template_used_twice_with_different_args(templates_dir):
    config = {
        "steps": [
            {"use_template": "ingest", "with": {"source_path": "/a", "alias": "orders"}},
            {"use_template": "ingest", "with": {"source_path": "/b", "alias": "customers"}},
        ],
    }
    expanded = expand_templates(config, str(templates_dir))
    names = [s["name"] for s in expanded["steps"]]
    assert names == ["read_orders", "check_orders", "read_customers", "check_customers"]


def test_template_missing_argument(templates_dir):
    with pytest.raises(TemplateError, match=r"missing template parameter.*source_path"):
        TemplateLoader(str(templates_dir)).expand("ingest", {"alias": "x"})


def test_template_not_found(templates_dir):
    with pytest.raises(TemplateError, match="not found"):
        TemplateLoader(str(templates_dir)).expand("nope", {})


def test_template_without_base_path():
    config = {"steps": [{"use_template": "ingest", "with": {}}]}
    with pytest.raises(TemplateError, match="no template base path"):
        expand_templates(config, None)


def test_nested_templates_rejected(templates_dir):
    (templates_dir / "nested.yaml").write_text("steps:\n  - use_template: ingest\n    with: {}\n")
    with pytest.raises(TemplateError, match="cannot include other templates"):
        TemplateLoader(str(templates_dir)).expand("nested", {})


def test_template_leaves_runtime_parameters(templates_dir):
    (templates_dir / "dated.yaml").write_text("""
template_parameters: [alias]
steps:
  - name: filter_${alias}
    type: filter
    input: ${alias}
    condition: "d = '${business_date}'"
    output: ${alias}_filtered
""")
    steps = TemplateLoader(str(templates_dir)).expand("dated", {"alias": "orders"})
    # Template args substituted; runtime parameters left for the resolver.
    assert steps[0]["condition"] == "d = '${business_date}'"
    assert steps[0]["input"] == "orders"


def test_template_pipeline_end_to_end(spark, orders_df, tmp_path, templates_dir):
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir()
    source = str(tmp_path / "src")
    orders_df.write.parquet(source)
    config_file = configs_dir / "pipeline.yaml"
    config_file.write_text(f"""
process_name: tpl_e2e
steps:
  - use_template: ingest
    with:
      source_path: "{source}"
      alias: orders
  - name: write_orders
    type: write
    input: orders_checked
    format: parquet
    mode: overwrite
    path: "{tmp_path / "out"}"
""")
    # templates/ lives next to configs/ -> auto-detected.
    result = run_pipeline(config_path=str(config_file), spark=spark)
    assert result["status"] == "succeeded"
    assert [s["step_name"] for s in result["steps"]] == [
        "read_orders",
        "check_orders",
        "write_orders",
    ]
    assert spark.read.parquet(str(tmp_path / "out")).count() == 4


def test_cli_validates_templated_repo_example(capsys):
    assert validate_command(["configs/orders_daily_snapshot.yaml"]) == 0


# --- lineage ---------------------------------------------------------------------

LINEAGE_CONFIG = {
    "process_name": "lineage_demo",
    "steps": [
        {
            "name": "read_orders",
            "type": "read",
            "format": "delta",
            "path": "abfss://lake/silver/orders",
            "alias": "orders",
        },
        {
            "name": "filter",
            "type": "filter",
            "input": "orders",
            "condition": "1=1",
            "output": "filtered",
        },
        {
            "name": "write_gold",
            "type": "write",
            "input": "filtered",
            "format": "delta",
            "mode": "overwrite",
            "path": "abfss://lake/gold/orders",
        },
    ],
}


def test_extract_lineage():
    lineage = extract_lineage(LINEAGE_CONFIG)
    assert [s["dataset"] for s in lineage["sources"]] == ["abfss://lake/silver/orders"]
    assert [t["dataset"] for t in lineage["targets"]] == ["abfss://lake/gold/orders"]
    assert lineage["flow"][1] == {
        "step": "filter",
        "type": "filter",
        "inputs": ["orders"],
        "output": "filtered",
    }


def test_dry_run_includes_lineage():
    import yaml as yaml_lib

    result = run_pipeline(
        config_content=yaml_lib.safe_dump(LINEAGE_CONFIG),
        dry_run=True,
    )
    assert result["lineage"]["targets"][0]["dataset"] == "abfss://lake/gold/orders"


def test_openlineage_console_emitter(caplog):
    emitter = OpenLineageEmitter(
        {"enabled": True, "transport": "console", "namespace": "lake"},
        process_name="lineage_demo",
        run_id="run-ol",
    )
    lineage = extract_lineage(LINEAGE_CONFIG)
    with caplog.at_level("INFO", logger="etl_framework"):
        emitter.emit("START", lineage)
        emitter.emit("FAIL", lineage, error="boom")
    events = [
        json.loads(r.message.split("OpenLineage event: ", 1)[1])
        for r in caplog.records
        if "OpenLineage event" in r.message
    ]
    assert [e["eventType"] for e in events] == ["START", "FAIL"]
    start = events[0]
    assert start["job"] == {"namespace": "lake", "name": "lineage_demo"}
    assert start["run"]["runId"] == "run-ol"
    assert start["inputs"] == [{"namespace": "abfss://lake", "name": "/silver/orders"}]
    assert start["outputs"] == [{"namespace": "abfss://lake", "name": "/gold/orders"}]
    assert events[1]["run"]["facets"]["errorMessage"]["message"] == "boom"


def test_openlineage_http_emitter(monkeypatch):
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
            "url": "https://marquez.example.com/api/v1/lineage",
            "api_key": "tok",
        },
        process_name="lineage_demo",
        run_id="run-ol",
    )
    emitter.emit("COMPLETE", extract_lineage(LINEAGE_CONFIG))
    assert len(sent) == 1
    request = sent[0]
    assert request.get_header("Authorization") == "Bearer tok"
    body = json.loads(request.data.decode("utf-8"))
    assert body["eventType"] == "COMPLETE"


def test_lineage_failures_never_fail_the_run(monkeypatch, caplog):
    def broken_urlopen(request, timeout=None):
        raise OSError("network down")

    monkeypatch.setattr("urllib.request.urlopen", broken_urlopen)
    emitter = OpenLineageEmitter(
        {"enabled": True, "transport": "http", "url": "https://x/lineage"},
        process_name="p",
        run_id="r",
    )
    with caplog.at_level("WARNING", logger="etl_framework"):
        emitter.emit("START", extract_lineage(LINEAGE_CONFIG))
    assert any("Failed to emit OpenLineage" in r.message for r in caplog.records)


def test_run_summary_contains_lineage(spark, orders_df, tmp_path):
    source = str(tmp_path / "src")
    orders_df.write.parquet(source)
    config = f"""
process_name: summary_lineage
settings:
  lineage:
    enabled: true
    transport: console
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: write_orders
    type: write
    input: orders
    format: parquet
    mode: overwrite
    path: "{tmp_path / "out"}"
"""
    result = run_pipeline(config_content=config, spark=spark)
    assert result["lineage"]["sources"][0]["dataset"] == source
    assert result["lineage"]["targets"][0]["dataset"] == str(tmp_path / "out")
