# Testes para Engine genérica
import pytest
from drune.core.engine.engine import BaseEngine, register_engine, get_engine

def test_register_and_get_engine():
    class DummyEngine(BaseEngine):
        def run(self):
            return 'ok'
    register_engine('dummy', DummyEngine)
    assert get_engine('dummy') == DummyEngine
