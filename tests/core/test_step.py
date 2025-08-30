# Testes para BaseStep e steps genéricos
import pytest
from drune.core.steps.step import BaseStep, register_step, get_step

def test_register_and_get_step():
    class DummyStep(BaseStep):
        def run(self):
            return 'ok'
    register_step('dummy', DummyStep)
    assert get_step('dummy') == DummyStep
