"""The `run` command and the console/wheel-task entry point."""

import json
import subprocess
import sys

import pytest

from etl_framework import __version__
from etl_framework.cli import entrypoint, main

PIPELINE = """
process_name: cli_run_demo
parameters:
  business_date: null
  environment: dev
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{source}"
    alias: orders
  - name: filter_orders
    type: filter
    input: orders
    condition: "order_date = '${{business_date}}'"
    output: daily
  - name: write_orders
    type: write
    input: daily
    format: parquet
    mode: overwrite
    path: "{target}"
"""


@pytest.fixture()
def config_path(tmp_path):
    path = tmp_path / "pipeline.yaml"
    path.write_text(PIPELINE.format(source=tmp_path / "in", target=tmp_path / "out"))
    return str(path)


def test_dry_run_prints_the_plan_without_spark(config_path, capsys):
    assert main(["run", config_path, "--param", "business_date=2026-07-07", "--dry-run"]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == "dry_run"
    assert [step["step_name"] for step in summary["plan"]] == [
        "read_orders",
        "filter_orders",
        "write_orders",
    ]


def test_invalid_configuration_exits_2_before_executing(config_path, capsys):
    # business_date is required and not supplied.
    assert main(["run", config_path, "--dry-run"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "nothing was executed" in captured.err
    assert "business_date" in captured.err


def test_run_executes_and_writes_the_summary(spark, orders_df, tmp_path, config_path, capsys):
    orders_df.write.parquet(str(tmp_path / "in"))
    summary_path = tmp_path / "summary.json"
    code = main(
        [
            "run",
            config_path,
            "--params-json",
            '{"business_date": "1999-01-01", "environment": "test"}',
            "--param",
            "business_date=2026-07-07",  # --param wins over --params-json
            "--run-id",
            "orchestrator-42",
            "--output",
            str(summary_path),
        ]
    )
    assert code == 0
    summary = json.loads(summary_path.read_text())
    assert summary["status"] == "succeeded"
    assert summary["run_id"] == "orchestrator-42"
    assert summary["environment"] == "test"
    assert summary["parameters"]["business_date"] == "2026-07-07"
    assert spark.read.parquet(str(tmp_path / "out")).count() == 3
    assert "cli_run_demo: succeeded" in capsys.readouterr().out


def test_failing_step_exits_1(spark, config_path, capsys):
    # The source directory does not exist, so the read step fails.
    assert main(["run", config_path, "--param", "business_date=2026-07-07"]) == 1
    captured = capsys.readouterr()
    assert "Pipeline failed." in captured.err
    assert "step 'read_orders'" in captured.err


@pytest.mark.parametrize(
    "arguments",
    [
        ["--params-json", "{not json"],
        ["--params-json", '["a", "list"]'],
        ["--param", "business_date"],
    ],
)
def test_a_malformed_command_line_exits_2_like_any_usage_error(config_path, arguments, capsys):
    # Exit code 1 means "a step failed"; a command line that cannot be parsed
    # must not look like that to an orchestrator.
    with pytest.raises(SystemExit) as excinfo:
        main(["run", config_path, *arguments, "--dry-run"])
    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("error: invalid --param")


def test_stdout_of_a_real_run_is_one_json_document(spark, orders_df, tmp_path, config_path, capsys):
    # Log lines go to stderr, so the summary on stdout can be piped to a parser.
    orders_df.write.parquet(str(tmp_path / "in"))
    assert main(["run", config_path, "--param", "business_date=2026-07-07"]) == 0
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["status"] == "succeeded"
    assert '"event": "pipeline_started"' in captured.err
    assert '"event": "pipeline_finished"' in captured.err


def test_local_flag_runs_delta_off_databricks(spark, orders_df, tmp_path, capsys):
    # Without --local the command uses whatever Spark session is active,
    # which off Databricks has no Delta Lake support.
    orders_df.write.parquet(str(tmp_path / "in"))
    config = tmp_path / "delta.yaml"
    config.write_text(f"""
process_name: local_delta
steps:
  - name: read_orders
    type: read
    format: parquet
    path: "{tmp_path / "in"}"
    alias: orders
  - name: write_orders
    type: write
    input: orders
    format: delta
    mode: overwrite
    path: "{tmp_path / "out"}"
""")
    code = main(["run", str(config), "--local", str(tmp_path / "warehouse")])
    assert code == 0, capsys.readouterr().err
    assert spark.read.format("delta").load(str(tmp_path / "out")).count() == 4


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as excinfo:
        main(["--version"])
    assert excinfo.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_entrypoint_returns_on_success_and_raises_on_failure(config_path, monkeypatch, capsys):
    # A Databricks wheel task calls the function directly: success must not
    # raise, and failure must, because a returned exit code would be ignored.
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "etl-framework",
            "run",
            config_path,
            "--param",
            "business_date=2026-07-07",
            "--dry-run",
        ],
    )
    assert entrypoint() is None

    monkeypatch.setattr(sys, "argv", ["etl-framework", "run", config_path, "--dry-run"])
    with pytest.raises(SystemExit) as excinfo:
        entrypoint()
    assert excinfo.value.code == 2


def test_validate_and_docs_work_without_pyspark():
    # Simulate an environment where PySpark is not installed: importing it
    # raises ImportError. Validation, documentation and dry runs must still work.
    script = (
        "import sys\n"
        "sys.modules['pyspark'] = None\n"
        "sys.modules['delta'] = None\n"
        "from etl_framework.cli import main\n"
        "code = main(['validate', 'configs/customer_orders.yaml', "
        "'configs/orders_daily_snapshot.yaml'])\n"
        "code = code or main(['docs', 'configs/customer_orders.yaml'])\n"
        "code = code or main(['run', 'configs/customer_orders.yaml', "
        "'--param', 'business_date=2026-07-07', '--dry-run'])\n"
        "raise SystemExit(code)\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "OK   configs/customer_orders.yaml" in completed.stdout
