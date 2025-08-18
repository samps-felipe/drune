from abc import ABC, abstractmethod
import os
from typing import Dict, Type, List, Any, Tuple
from drune.core.engine import BaseEngine
from drune.models import ValidationResult
from drune.utils.exceptions import ConstraintError
from drune.utils.parsers import parse_function_string
from drune.utils.logger import get_logger
import uuid

# This will hold the mapping from rule name to rule class
_constraint_rule_registry: Dict[str, Dict[str, Type]] = {}

def register_constraint(engine: str, name: str, cls: Type = None):
    """
    Registers or overwrites a constraint rule class.
    Usage as decorator: @register_constraint('engine_name', 'rule_name')
    Usage as function: register_constraint('engine_name', 'rule_name', MyClass)
    """
    if cls is not None:
        if engine not in _constraint_rule_registry:
            _constraint_rule_registry[engine] = {}
        _constraint_rule_registry[engine][name] = cls
        return cls
    
    def decorator(inner_cls: Type) -> Type:
        if engine not in _constraint_rule_registry:
            _constraint_rule_registry[engine] = {}
        
        _constraint_rule_registry[engine][name] = inner_cls
    
        return inner_cls
    
    return decorator

def get_constraint_rule(engine: str, name: str) -> Type:
    """Retrieves a constraint rule class from the registry."""
    if engine not in _constraint_rule_registry:
        raise NotImplementedError(f"Engine '{engine}' not found in constraint rule registry.")
    
    if name not in _constraint_rule_registry[engine]:
        raise NotImplementedError(
            f"Constraint rule '{name}' not found for engine '{engine}'."
            f"Available rules: {list(_constraint_rule_registry[engine].keys())}")
    
    return _constraint_rule_registry[engine][name]

class BaseConstraintRule(ABC):
    """Base class for all column constraints."""
    def __init__(self, name: str):
        """Initializes the constraint rule."""
        self.name = name
        self.uuid = f'_{self.name}_{uuid.uuid4().hex}'
        self.logger = get_logger(f'{self.name}')

    @abstractmethod
    def apply(self, df: Any, params: Dict[str, Any]) -> Tuple[Any, Any]:
        """Applies the constraint rule."""
        pass

    def fail(self, df) -> Tuple[Any, Any]:
        pass

    def drop(self, df):
        pass

    def warn(sefl, df):
        pass
        
class DataQualityProcessor:
    """Processes data quality checks."""
    def __init__(self, engine: BaseEngine, log_path: str):
        self.logger = get_logger(self.__class__.__name__)

        os.makedirs(log_path, exist_ok=True)
        self.log_path = log_path

        self.engine = engine
        self.results_logger = []


    def apply_column_constraint(self, df, constraint_name, params) -> Tuple[str, Any]:
        """Applies a constraint to the DataFrame."""
        self.logger.info(f"Applying constraint '{constraint_name}'...")

        constraint_class = get_constraint_rule(self.engine.name, constraint_name)

        constraint_instance = constraint_class(constraint_name)
        result = constraint_instance.apply(df, params)

        return constraint_instance, result
        

    def apply_schema_constraints(self, df, schema):
        """Applies schema constraints to the DataFrame."""
        self.logger.info("Applying schema constraints...")

        results = {
            'fail': [],
            'warn': [],
            'drop': []
        }

        for column_spec in schema.columns:
            col_name = column_spec.rename

            for constraint in column_spec.constraints:
                func_list = parse_function_string(constraint.rule)
                
                for func in func_list:
                    func['params']['column_name']= col_name

                    constraint_instance, df = self.apply_column_constraint(df, func['function'], func['params'])

                    results[constraint.on_fail].append(constraint_instance)
        
        fails_list = []

        for result in results['warn']:
            df, fail_df = result.warn(df)
            fails_list.append(fail_df)


        for result in results['drop']:
            df, fail_df = result.drop(df)
            fails_list.append(fail_df)

        for result in results['fail']:
            df, fail_df = result.fail(df)
            fails_list.append(fail_df)
        
        self.engine.write_list(fails_list, self.log_path)

        if results['fail']:
            raise ConstraintError(f'The schema validation failed with {len(results["fail"])} errors.')

        return df
            