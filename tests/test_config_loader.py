import pytest

from etl_framework.config_loader import load_config
from etl_framework.exceptions import ConfigLoadError


def test_load_config_from_file(tmp_path):
    path = tmp_path / "pipeline.yaml"
    path.write_text("process_name: demo\nsteps: []\n")
    config = load_config(config_path=str(path))
    assert config["process_name"] == "demo"


def test_load_config_from_content():
    config = load_config(config_content="process_name: inline\nsteps: []\n")
    assert config["process_name"] == "inline"


def test_missing_file_raises():
    with pytest.raises(ConfigLoadError, match="does not exist"):
        load_config(config_path="/nonexistent/pipeline.yaml")


def test_invalid_yaml_raises(tmp_path):
    path = tmp_path / "broken.yaml"
    path.write_text("steps:\n  - name: a\n   type: broken_indent\n")
    with pytest.raises(ConfigLoadError, match="invalid YAML"):
        load_config(config_path=str(path))


def test_non_mapping_top_level_raises():
    with pytest.raises(ConfigLoadError, match="must be a mapping"):
        load_config(config_content="- just\n- a\n- list\n")


def test_requires_exactly_one_source():
    with pytest.raises(ConfigLoadError):
        load_config()
    with pytest.raises(ConfigLoadError):
        load_config(config_path="x.yaml", config_content="a: 1")
