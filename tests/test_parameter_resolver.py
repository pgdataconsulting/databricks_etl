import pytest

from etl_framework.exceptions import MissingParameterError
from etl_framework.parameter_resolver import (
    collect_parameter_references,
    merge_parameters,
    resolve_config,
    resolve_parameters,
)


def test_merge_runtime_overrides_defaults():
    merged = merge_parameters({"env": "dev", "date": None}, {"date": "2026-07-07"})
    assert merged == {"env": "dev", "date": "2026-07-07"}


def test_merge_ignores_none_runtime_values():
    merged = merge_parameters({"env": "dev"}, {"env": None})
    assert merged == {"env": "dev"}


def test_collect_references_nested():
    steps = [
        {"condition": "order_date = '${business_date}'"},
        {"columns": {"env": "'${environment}'"}},
    ]
    assert collect_parameter_references(steps) == {"business_date", "environment"}


def test_resolve_embedded_and_full_placeholders():
    params = {"date": "2026-07-07", "count": 5}
    assert resolve_parameters("d = '${date}'", params) == "d = '2026-07-07'"
    # A standalone placeholder keeps the parameter's native type.
    assert resolve_parameters("${count}", params) == 5


def test_resolve_config_missing_parameter_fails_fast():
    config = {
        "process_name": "demo",
        "parameters": {"business_date": None},
        "steps": [
            {
                "name": "f",
                "type": "filter",
                "input": "a",
                "condition": "d = '${business_date}'",
                "output": "b",
            },
        ],
    }
    with pytest.raises(MissingParameterError, match="business_date"):
        resolve_config(config, {})


def test_resolve_config_substitutes_steps():
    config = {
        "process_name": "demo",
        "parameters": {"business_date": None},
        "steps": [
            {
                "name": "f",
                "type": "filter",
                "input": "a",
                "condition": "d = '${business_date}'",
                "output": "b",
            },
        ],
    }
    resolved, merged = resolve_config(config, {"business_date": "2026-07-07"})
    assert resolved["steps"][0]["condition"] == "d = '2026-07-07'"
    assert merged["business_date"] == "2026-07-07"
