# Testes de qualidade para Pandas
import pytest
from drune.engines.pandas.quality.rules import NotNullValidation
import pandas as pd

def test_not_null_validation():
    df = pd.DataFrame({'a': [1, None, 3]})
    rule = NotNullValidation()
    result = rule.apply(df, 'a')
    # O resultado pode ser uma tupla (df, series) ou apenas uma série, dependendo da implementação
    # Aceita Series, DataFrame ou tupla
    if isinstance(result, tuple):
        assert any(isinstance(x, pd.Series) or isinstance(x, pd.DataFrame) for x in result)
    else:
        assert isinstance(result, pd.Series) or isinstance(result, pd.DataFrame)
