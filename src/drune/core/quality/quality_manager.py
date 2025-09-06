import os
from typing import Any, Tuple
from typing import Dict, Type

from drune.core.quality.constraint import BaseConstraint
from ..engine.engine import BaseEngine
from drune.utils.exceptions import ConstraintError
from drune.utils.parsers import parse_function_string
from drune.utils.logger import get_logger

class DataQualityManager:
    """Processes data quality checks."""
    _constraints: Dict[str, Dict[str, Type[BaseConstraint]]] = {}

    @classmethod
    def register(cls, engine: str, name: str):
        """
        Registers or overwrites a constraint class.
        Usage as decorator: @register('engine_name', 'constraint_name')
        """

        def decorator(constraint_class: Type[BaseConstraint]):
            if engine not in cls._constraints:
                cls._constraints[engine] = {}

            cls._constraints[engine][name] = constraint_class

            return constraint_class

        return decorator
    
    @classmethod
    def get_constraint(cls, engine: str, name: str) -> Type[BaseConstraint]:
        """Retrieves a constraint class by its name."""
        if engine not in cls._constraints:
            raise NotImplementedError(f"Engine '{engine}' not found in registry.")

        constraint_class = cls._constraints[engine][name]

        if not constraint_class:
            raise NotImplementedError(
                f"Constraint '{name}' not found for engine '{engine}'."
                f"Available constraints: {list(cls._constraints[engine].keys())}")
        
        return constraint_class(name)


    def __init__(self, engine: BaseEngine, project_dir, log_path):
        self.logger = get_logger(self.__class__.__name__)

        log_path = os.path.join(project_dir, log_path)

        os.makedirs(log_path, exist_ok=True)

        self.log_path = log_path

        self.engine = engine
        self.results_logger = []


    def apply_column_constraint(self, df, constraint_name, params) -> Tuple[str, Any]:
        """Applies a constraint to the DataFrame."""
        self.logger.info(f"Applying constraint '{constraint_name}'...")

        constraint_instance = DataQualityManager.get_constraint(self.engine.name, constraint_name)

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
            col_name = column_spec.name

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
            