import pytest
from click.testing import CliRunner
from unittest.mock import MagicMock, patch
import yaml

from drune.cli import cli, _load_and_validate_config, get_engine
from drune.models.pydantic_models import PipelineConfig
from drune.core.pipeline import Pipeline
from drune.core.engine import BaseEngine

# Fixture for CliRunner
@pytest.fixture
def runner():
    return CliRunner()

# Fixture for a dummy PipelineConfig
@pytest.fixture
def dummy_pipeline_config():
    return PipelineConfig(
        pipeline_name="test_pipeline",
        engine="pandas",
        pipeline_type="silver",
        source=MagicMock(),
        sink=MagicMock(),
        columns=[]
    )

# Test _load_and_validate_config
def test_load_and_validate_config_success(tmp_path, dummy_pipeline_config):
    config_data = {
        "pipeline_name": "test_pipeline",
        "engine": "pandas",
        "pipeline_type": "silver",
        "source": {"format": "csv", "path": "data.csv"},
        "sink": {"format": "csv", "mode": "overwrite", "path": "output.csv"},
        "columns": []
    }
    config_file = tmp_path / "test_config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    config = _load_and_validate_config(str(config_file))
    assert config.pipeline_name == "test_pipeline"
    assert config.engine == "pandas"

def test_load_and_validate_config_invalid_yaml(runner, tmp_path):
    config_file = tmp_path / "invalid_config.yaml"
    config_file.write_text("invalid: - yaml")
    with pytest.raises(ValueError, match="YAML validation error:"):
        _load_and_validate_config(str(config_file))

def test_load_and_validate_config_invalid_pydantic_model(runner, tmp_path):
    config_data = {"pipeline_name": "test_pipeline"} # Missing required fields
    config_file = tmp_path / "invalid_pydantic_config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    with pytest.raises(ValueError, match="YAML validation error:"):
        _load_and_validate_config(str(config_file))

# Test get_engine
def test_get_engine_pandas(dummy_pipeline_config):
    engine = get_engine("pandas", dummy_pipeline_config)
    assert engine is not None
    assert engine.config == dummy_pipeline_config

def test_get_engine_unsupported():
    with pytest.raises(ValueError, match="Engine 'unsupported' not supported."):
        get_engine("unsupported", MagicMock(spec=PipelineConfig))

# Test CLI commands
def test_cli_run_command_success(runner, tmp_path, dummy_pipeline_config):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(dummy_pipeline_config.model_dump(mode='json')))

    mock_engine_instance = MagicMock(spec=BaseEngine)
    mock_pipeline_instance = MagicMock(spec=Pipeline)

    with patch('declarative_data_framework.cli._load_and_validate_config', return_value=dummy_pipeline_config) as mock_load_config,
         patch('declarative_data_framework.cli.get_engine', return_value=mock_engine_instance) as mock_get_engine,
         patch('declarative_data_framework.cli.Pipeline', return_value=mock_pipeline_instance) as mock_pipeline_class:
    
        result = runner.invoke(cli, ["run", str(config_file)])
        assert result.exit_code == 0
        mock_load_config.assert_called_once_with(str(config_file))
        mock_get_engine.assert_called_once_with(dummy_pipeline_config.engine, dummy_pipeline_config)
        mock_pipeline_class.assert_called_once_with(dummy_pipeline_config, mock_engine_instance)
        mock_pipeline_instance.run.assert_called_once()
        assert f"Pipeline '{dummy_pipeline_config.pipeline_name}' executed successfully." in result.output

def test_cli_run_command_config_error(runner, tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("invalid: yaml")

    with patch('declarative_data_framework.cli._load_and_validate_config', side_effect=ValueError("Test config error")) as mock_load_config:
        result = runner.invoke(cli, ["run", str(config_file)])
        assert result.exit_code == 1
        mock_load_config.assert_called_once_with(str(config_file))
        assert "Test config error" in result.output

def test_cli_create_command_success(runner, tmp_path, dummy_pipeline_config):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(dummy_pipeline_config.model_dump(mode='json')))

    mock_engine_instance = MagicMock(spec=BaseEngine)
    mock_pipeline_instance = MagicMock(spec=Pipeline)

    with patch('declarative_data_framework.cli._load_and_validate_config', return_value=dummy_pipeline_config) as mock_load_config,
         patch('declarative_data_framework.cli.get_engine', return_value=mock_engine_instance) as mock_get_engine,
         patch('declarative_data_framework.cli.Pipeline', return_value=mock_pipeline_instance) as mock_pipeline_class:
        result = runner.invoke(cli, ["create", str(config_file)])
        assert result.exit_code == 0
        mock_load_config.assert_called_once_with(str(config_file))
        mock_get_engine.assert_called_once_with(dummy_pipeline_config.engine, dummy_pipeline_config)
        mock_pipeline_class.assert_called_once_with(dummy_pipeline_config, mock_engine_instance)
        mock_pipeline_instance.create.assert_called_once()
        assert f"Pipeline '{dummy_pipeline_config.pipeline_name}' resources created successfully." in result.output

def test_cli_update_command_success(runner, tmp_path, dummy_pipeline_config):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(dummy_pipeline_config.model_dump(mode='json')))

    mock_engine_instance = MagicMock(spec=BaseEngine)
    mock_pipeline_instance = MagicMock(spec=Pipeline)

    with patch('declarative_data_framework.cli._load_and_validate_config', return_value=dummy_pipeline_config) as mock_load_config,
         patch('declarative_data_framework.cli.get_engine', return_value=mock_engine_instance) as mock_get_engine,
         patch('declarative_data_framework.cli.Pipeline', return_value=mock_pipeline_instance) as mock_pipeline_class:
        result = runner.invoke(cli, ["update", str(config_file)])
        assert result.exit_code == 0
        mock_load_config.assert_called_once_with(str(config_file))
        mock_get_engine.assert_called_once_with(dummy_pipeline_config.engine, dummy_pipeline_config)
        mock_pipeline_class.assert_called_once_with(dummy_pipeline_config, mock_engine_instance)
        mock_pipeline_instance.update.assert_called_once()
        assert f"Pipeline '{dummy_pipeline_config.pipeline_name}' resources updated successfully." in result.output

def test_cli_test_command_success(runner, tmp_path, dummy_pipeline_config):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(dummy_pipeline_config.model_dump(mode='json')))

    mock_engine_instance = MagicMock(spec=BaseEngine)
    mock_pipeline_instance = MagicMock(spec=Pipeline)

    with patch('declarative_data_framework.cli._load_and_validate_config', return_value=dummy_pipeline_config) as mock_load_config,
         patch('declarative_data_framework.cli.get_engine', return_value=mock_engine_instance) as mock_get_engine,
         patch('declarative_data_framework.cli.Pipeline', return_value=mock_pipeline_instance) as mock_pipeline_class:
        result = runner.invoke(cli, ["test", str(config_file)])
        assert result.exit_code == 0
        mock_load_config.assert_called_once_with(str(config_file))
        mock_get_engine.assert_called_once_with(dummy_pipeline_config.engine, dummy_pipeline_config)
        mock_pipeline_class.assert_called_once_with(dummy_pipeline_config, mock_engine_instance)
        mock_pipeline_instance.test.assert_called_once()
        assert f"Pipeline '{dummy_pipeline_config.pipeline_name}' tests executed successfully." in result.output