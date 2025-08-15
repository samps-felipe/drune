# Testes para modelos pydantic
import pytest
from drune.models.pydantic_models import ValidationRule, ColumnSpec, RESERVED_COLUMN_NAMES
from pydantic import ValidationError

def test_validation_rule():
    rule = ValidationRule(rule='not_null')
    assert rule.rule == 'not_null'
    assert rule.on_fail == 'fail'

def test_column_spec_reserved_names():
    for reserved in RESERVED_COLUMN_NAMES:
        with pytest.raises(ValidationError):
            ColumnSpec(name=reserved, type='string')
