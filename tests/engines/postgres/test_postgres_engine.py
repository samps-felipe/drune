# Testes para PostgresEngine
import pytest
from drune.engines.postgres import PostgresEngine
from drune.models.pydantic_models import PipelineConfig

def test_postgres_engine_instantiation():
    config = PipelineConfig(pipeline_name='test', engine='postgres', pipeline_type='silver', source={}, sink={}, columns=[])
    engine = PostgresEngine(config)
    assert engine.config.pipeline_name == 'test'
