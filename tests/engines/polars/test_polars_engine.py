# Testes para PolarsEngine
import pytest
from drune.engines.polars import PolarsEngine
from drune.models.pydantic_models import PipelineConfig

def test_polars_engine_instantiation():
    config = PipelineConfig(pipeline_name='test', engine='polars', pipeline_type='silver', source={}, sink={}, columns=[])
    engine = PolarsEngine(config)
    assert engine.config.pipeline_name == 'test'
