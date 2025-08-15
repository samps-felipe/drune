# Testes de qualidade para Spark
import pytest
from drune.engines.spark.quality.rules import NotNullValidation
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('test').master('local[*]').getOrCreate()

def test_not_null_validation(spark):
    from pyspark.sql import Row
    df = spark.createDataFrame([Row(a=1), Row(a=None), Row(a=3)])
    rule = NotNullValidation()
    result = rule.apply(df, 'a')
    assert result is not None
