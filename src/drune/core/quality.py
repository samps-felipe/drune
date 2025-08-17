from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F
from typing import Tuple, Dict, Type
from drune.utils.logger import get_logger
from drune.utils.parsers import parse_function


# This will hold the mapping from rule name to rule class
_constraint_rule_registry: Dict[str, Dict[str, Type]] = {}

def register_constraint(engine: str, name: str, cls: Type = None):
    """
    Registers or overwrites a constraint rule class.
    Usage as decorator: @register_rule('rule_name')
    Usage as function: register_rule('rule_name', MyClass)
    """
    if cls is not None:
        if engine not in _constraint_rule_registry:
            _constraint_rule_registry[engine] = {}
        _constraint_rule_registry[engine][name] = cls
        return cls
    
    def decorator(inner_cls: Type) -> Type:
        _constraint_rule_registry[engine][name] = inner_cls
        return inner_cls
    return decorator

def get_constraint_rule(engine: str, name: str) -> Type:
    """Retrieves a constraint rule class from the registry."""
    return _constraint_rule_registry[engine][name]

class BaseConstraint(ABC):
    """Base class for all column constraints."""
    def __init__(self, params: dict = None):
        # Universal constructor for all rules
        self.params = params or {}
        self.logger = get_logger(self.__class__.__name__)


    @abstractmethod
    def apply(self, df: DataFrame, column_name: str) -> Tuple[DataFrame, DataFrame]:
        """Applies the constraint rule."""
        pass
 

    # def _create_constraint_rule(self, rule_name: str, param: str):

    #     rule_class = get_constraint_rule(rule_name)

    #     if not rule_class:
    #         self.logger.warning(f"Validation rule '{rule_name}' is not registered. Skipping.")
    #         return None
    #     else:
    #         return rule_class(param)

# class BaseTableConstraint(ABC):
#     """Base class for all table constraints."""
#     @abstractmethod
#     def apply(self, df: DataFrame):
#         """Applies the constraint rule to the entire table."""
#         pass
