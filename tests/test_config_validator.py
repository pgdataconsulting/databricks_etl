import pytest

from etl_framework.config_validator import validate_config
from etl_framework.exceptions import ValidationError


def base_config(**overrides):
    config = {
        "process_name": "demo",
        "steps": [
            {"name": "read_a", "type": "read", "format": "parquet", "path": "/tmp/a", "alias": "a"},
            {
                "name": "filter_a",
                "type": "filter",
                "input": "a",
                "condition": "x > 0",
                "output": "b",
            },
            {
                "name": "write_b",
                "type": "write",
                "input": "b",
                "format": "parquet",
                "mode": "overwrite",
                "path": "/tmp/b",
            },
        ],
    }
    config.update(overrides)
    return config


def errors_of(config):
    with pytest.raises(ValidationError) as exc_info:
        validate_config(config)
    return "\n".join(exc_info.value.errors)


def test_valid_config_passes():
    validate_config(base_config())


def test_missing_process_name():
    config = base_config()
    del config["process_name"]
    assert "process_name" in errors_of(config)


def test_empty_steps():
    assert "non-empty list" in errors_of(base_config(steps=[]))


def test_unsupported_spec_version():
    assert "spec_version" in errors_of(base_config(spec_version="9.9"))


def test_duplicate_step_names():
    config = base_config()
    config["steps"][1]["name"] = "read_a"
    assert "unique" in errors_of(config)


def test_unsupported_operation_type():
    config = base_config()
    config["steps"][1]["type"] = "teleport"
    assert "unsupported operation type 'teleport'" in errors_of(config)


def test_missing_input_reference():
    config = base_config()
    config["steps"][1]["input"] = "missing_df"
    assert "missing_df" in errors_of(config)


def test_duplicate_output_requires_allow_overwrite():
    config = base_config()
    config["steps"][1]["output"] = "a"
    message = errors_of(config)
    assert "overwrite" in message

    config["steps"][1]["allow_overwrite"] = True
    # write step consumed "b"; repoint it at the overwritten alias.
    config["steps"][2]["input"] = "a"
    validate_config(config)


def test_missing_required_fields():
    config = base_config()
    del config["steps"][1]["condition"]
    assert "'condition'" in errors_of(config)


def test_invalid_join_type_and_cross_join_gate():
    config = base_config()
    config["steps"].insert(
        2,
        {
            "name": "join_bad",
            "type": "join",
            "left": "a",
            "right": "b",
            "join_type": "sideways",
            "condition": ["a.x = b.x"],
            "output": "c",
        },
    )
    assert "join_type" in errors_of(config)

    config["steps"][2]["join_type"] = "cross"
    assert "allow_cross" in errors_of(config)

    config["steps"][2]["allow_cross"] = True
    del config["steps"][2]["condition"]
    validate_config(config)


def test_join_missing_condition():
    config = base_config()
    config["steps"].insert(
        2,
        {
            "name": "join_no_cond",
            "type": "join",
            "left": "a",
            "right": "b",
            "join_type": "inner",
            "output": "c",
        },
    )
    assert "condition" in errors_of(config)


def test_write_requires_target():
    config = base_config()
    del config["steps"][2]["path"]
    assert "target" in errors_of(config)


def test_unsupported_write_mode():
    config = base_config()
    config["steps"][2]["mode"] = "upsert"
    assert "mode" in errors_of(config)


def test_sql_disabled_by_default():
    config = base_config()
    config["steps"].insert(
        2,
        {
            "name": "sql_step",
            "type": "sql",
            "query": "SELECT * FROM b",
            "output": "c",
        },
    )
    assert "allow_sql" in errors_of(config)

    config["settings"] = {"allow_sql": True}
    validate_config(config)


def test_expression_validation():
    config = base_config()
    config["steps"].insert(
        2,
        {
            "name": "bad_expr",
            "type": "expression",
            "input": "b",
            "expressions": {"not a column!": "1", "ok_col": ""},
            "output": "c",
        },
    )
    message = errors_of(config)
    assert "invalid output column name" in message
    assert "non-empty SQL string" in message


def test_read_format_validation():
    config = base_config()
    config["steps"][0]["format"] = "xml"
    assert "unsupported read format" in errors_of(config)
