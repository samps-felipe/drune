# Testes para SparkEngine
import pytest
from drune.engines.spark.spark_engine import SparkEngine
from drune.models.pydantic_models import PipelineConfig
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('test').master('local[*]').getOrCreate()

def test_spark_engine_instantiation(spark):
    config = PipelineConfig(pipeline_name='test', engine='spark', pipeline_type='silver', source={}, sink={}, columns=[])
    engine = SparkEngine(spark)
    assert engine.spark is not None
