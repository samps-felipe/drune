# Testes de steps para Pandas
import pytest
from drune.engines.pandas.steps.reader import ReadStep

def test_read_step():
    class DummyEngine:
        def __init__(self):
            self.config = None
    step = ReadStep(engine=DummyEngine())
    assert hasattr(step, 'execute')
