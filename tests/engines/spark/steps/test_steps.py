# Testes de steps para Spark
import pytest
from drune.engines.spark.steps.reader import ReadStep

def test_read_step():
    step = ReadStep()
    assert hasattr(step, 'run')
