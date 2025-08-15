# Testes para PandasEngine
import pytest
from drune.engines.pandas.pandas_engine import PandasEngine
from drune.models.pydantic_models import PipelineConfig

def test_pandas_engine_instantiation():
    config = PipelineConfig(
        pipeline_name='test',
        engine='pandas',
        pipeline_type='silver',
        source={'format': 'csv', 'path': 'input.csv'},
        sink={'format': 'csv', 'mode': 'overwrite', 'path': 'output.csv'},
    columns=[{'name': 'col1', 'type': 'string'}]
    )
    engine = PandasEngine(config)
    assert engine.config.pipeline_name == 'test'
