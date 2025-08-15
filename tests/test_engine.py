import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from drune.core.engine import BaseEngine, register_engine, get_engine
from drune.models.pydantic_models import PipelineConfig, SourceConfig, SinkConfig, ColumnSpec, DefaultsConfig
from drune.engines.spark.spark_engine import SparkEngine
from drune.engines.pandas.pandas_engine import PandasEngine
import pandas as pd

# Fixture for a dummy PipelineConfig
@pytest.fixture
def dummy_pipeline_config():
    return PipelineConfig(
        pipeline_name="test_pipeline",
        engine="spark",
        pipeline_type="silver",
        source=SourceConfig(type="csv", path="data.csv"),
        sink=SinkConfig(type="csv", path="output.csv"),
        columns=[
            ColumnSpec(name="col1", type="string"),
            ColumnSpec(name="col2", type="int")
        ],
        defaults=DefaultsConfig()
    )

# Test register_engine and get_engine
def test_engine_registration():
    class MyEngine(BaseEngine):
        def __init__(self, config): pass
        def create_table(self): pass
        def update_table(self): pass
        def run(self): pass
        def test(self, config): pass

    register_engine("my_test_engine", MyEngine)
    retrieved_engine = get_engine("my_test_engine")
    assert retrieved_engine == MyEngine

    @register_engine("another_test_engine")
    class AnotherEngine(BaseEngine):
        def __init__(self, config): pass
        def create_table(self): pass
        def update_table(self): pass
        def run(self): pass
        def test(self, config): pass

    retrieved_another_engine = get_engine("another_test_engine")
    assert retrieved_another_engine == AnotherEngine

    # Test overwriting
    class OverwriteEngine(BaseEngine):
        def __init__(self, config): pass
        def create_table(self): pass
        def update_table(self): pass
        def run(self): pass
        def test(self, config): pass

    register_engine("my_test_engine", OverwriteEngine)
    retrieved_overwritten_engine = get_engine("my_test_engine")
    assert retrieved_overwritten_engine == OverwriteEngine

def test_get_non_existent_engine():
    assert get_engine("non_existent_engine") is None

# Test BaseEngine abstract methods
def test_base_engine_instantiation_fails():
    with pytest.raises(TypeError):
        BaseEngine(config=MagicMock(spec=PipelineConfig))

# Test SparkEngine
def test_spark_engine_initialization(spark, dummy_pipeline_config):
    with patch('pyspark.sql.SparkSession.builder.appName', return_value=spark.builder) as mock_app_name:
        engine = SparkEngine(dummy_pipeline_config)
        mock_app_name.assert_called_with(dummy_pipeline_config.pipeline_name)
        assert engine.spark == spark
        assert engine.config == dummy_pipeline_config

def test_spark_engine_run_method(spark, dummy_pipeline_config):
    engine = SparkEngine(dummy_pipeline_config)
    mock_step_instance = MagicMock()
    mock_get_step = MagicMock(return_value=MagicMock(return_value=mock_step_instance))

    with patch('declarative_data_framework.core.step.get_step', mock_get_step):
        # Add a dummy step to the config for testing run method
        dummy_pipeline_config.steps = [
            {"type": "reader", "params": {"param1": "value1"}},
            {"type": "transformer", "params": {"param2": "value2"}}
        ]
        engine.config = dummy_pipeline_config # Update engine's config

        engine.run()

        assert mock_get_step.call_count == 2
        assert mock_step_instance.execute.call_count == 2
        mock_get_step.assert_any_call("reader")
        mock_get_step.assert_any_call("transformer")

def test_spark_engine_run_method_unknown_step_type(spark, dummy_pipeline_config):
    engine = SparkEngine(dummy_pipeline_config)
    dummy_pipeline_config.steps = [{"type": "unknown_step", "params": {}}]
    engine.config = dummy_pipeline_config

    with pytest.raises(ValueError, match="Step type 'unknown_step' not found in registry."):
        engine.run()

# Test PandasEngine
def test_pandas_engine_initialization(dummy_pipeline_config):
    engine = PandasEngine(dummy_pipeline_config)
    assert engine.config == dummy_pipeline_config

def test_pandas_engine_run_method(dummy_pipeline_config):
    engine = PandasEngine(dummy_pipeline_config)
    mock_step_instance = MagicMock()
    mock_get_step = MagicMock(return_value=MagicMock(return_value=mock_step_instance))

    with patch('declarative_data_framework.core.step.get_step', mock_get_step):
        # Add a dummy step to the config for testing run method
        dummy_pipeline_config.steps = [
            {"type": "reader", "params": {"param1": "value1"}},
            {"type": "transformer", "params": {"param2": "value2"}}
        ]
        engine.config = dummy_pipeline_config # Update engine's config

        engine.run()

        assert mock_get_step.call_count == 2
        assert mock_step_instance.execute.call_count == 2
        mock_get_step.assert_any_call("reader")
        mock_get_step.assert_any_call("transformer")

def test_pandas_engine_run_method_unknown_step_type(dummy_pipeline_config):
    engine = PandasEngine(dummy_pipeline_config)
    dummy_pipeline_config.steps = [{"type": "unknown_step", "params": {}}]
    engine.config = dummy_pipeline_config

    with pytest.raises(ValueError, match="Step type 'unknown_step' not found in registry."):
        engine.run()

def test_pandas_engine_create_table_does_nothing(dummy_pipeline_config):
    engine = PandasEngine(dummy_pipeline_config)
    # Should not raise any error and simply pass
    engine.create_table(dummy_pipeline_config)

def test_pandas_engine_update_table_does_nothing(dummy_pipeline_config):
    engine = PandasEngine(dummy_pipeline_config)
    # Should not raise any error and simply pass
    engine.update_table(dummy_pipeline_config)
