from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F
from typing import Tuple, Dict, Type

# This will hold the mapping from rule name to rule class
_validation_rule_registry: Dict[str, Type] = {}

def register_rule(name: str, cls: Type = None):
    """
    Registers or overwrites a validation rule class.
    Usage as decorator: @register_rule('rule_name')
    Usage as function: register_rule('rule_name', MyClass)
    """
    if cls is not None:
        _validation_rule_registry[name] = cls
        return cls
    def decorator(inner_cls: Type) -> Type:
        _validation_rule_registry[name] = inner_cls
        return inner_cls
    return decorator

def get_validationrule(name: str) -> Type:
    """Retrieves a validation rule class from the registry."""
    return _validation_rule_registry.get(name)


class BaseValidation(ABC):
    """Base class for all column validations."""
    def __init__(self, params: dict = None):
        # Universal constructor for all rules
        self.params = params or {}

    @abstractmethod
    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        """Applies the validation rule."""
        pass

class BaseTableValidation(ABC):
    """Base class for all table validations."""
    @abstractmethod
    def apply(self, df: DataFrame):
        """Applies the validation rule to the entire table."""
        pass
