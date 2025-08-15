
# Fixtures globais para o projeto
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    """
    Cria uma sessão Spark para os testes.
    """
    return SparkSession.builder.appName('test-spark-session').master('local[*]').getOrCreate()
