# Testes para DuckDBEngine
import pytest
from drune.engines.duckdb import DuckDBEngine
from drune.models.pydantic_models import PipelineConfig

def test_duckdb_engine_instantiation():
    config = PipelineConfig(pipeline_name='test', engine='duckdb', pipeline_type='silver', source={}, sink={}, columns=[])
    engine = DuckDBEngine(config)
    assert engine.config.pipeline_name == 'test'
