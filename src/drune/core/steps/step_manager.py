from typing import Dict, Optional, Type
from .step import BaseStep
from drune.utils.logger import get_logger

from drune.core.engine import BaseEngine


class StepManager:
    """
    Manages the registration and retrieval of step classes.
    """
    _steps: Dict[str, Dict[str, Type[BaseStep]]] = {}

    @classmethod
    def register(cls, engine: str, name: str):
        """
        Registers or overwrites a step class.
        Usage as decorator: @register('engine_name', 'step_name')
        """

        def decorator(step_class: Type[BaseStep]):
            if engine not in cls._steps:
                cls._steps[engine] = {}

            cls._steps[engine][name] = step_class

            return step_class

        return decorator
    
    @classmethod
    def get_step(cls, engine: str, name: str, params: Dict = None) -> Type[BaseStep]:
        """Retrieves a step class by its name."""
        if engine not in cls._steps:
            raise NotImplementedError(f"Engine '{engine}' not found in registry.")

        step_class = cls._steps[engine].get(name)

        if not step_class:
            raise NotImplementedError(
                f"Step '{name}' not found for engine '{engine}'."
                f"Available steps: {list(cls._steps[engine].keys())}")
        
        return step_class(**params) if params else step_class()
    
    def __init__(self, engine: BaseEngine):
        self.engine = engine

        self.steps = []
        self.reset()

        self.logger = get_logger(__class__.__name__)
    
    def reset(self):
        self._current_step_index = -1

    def load(self, steps: list):
        self.steps = steps
        self._current_step_index = -1
    
    def get_current_step(self):
        if self._current_step_index < 0 or self._current_step_index >= len(self.steps):
            return None
        
        return self.steps[self._current_step_index]
    
    def get_next_step(self):
        if self._current_step_index + 1 >= len(self.steps):
            return None
        
        return self.steps[self._current_step_index + 1]
    
    def run(self, sources, target, stop_at: Optional[str] = None):
        """
        Executes the pipeline from the current state.

        Args:
            stop_at: The name of the step to stop after. If None, runs all steps.
        """
        if self._current_step_index > -1:
            self.logger.info(f'Returning from breaking point: {self.get_current_step().name}')

        start_index = self._current_step_index + 1

        for i in range(start_index, len(self.steps)):
            step = self.steps[i]
            self.logger.info(f'Executing step: {step.name}')

            step_instance = StepManager.get_step(self.engine.name, step.type, {'name': step.name})
            target = step_instance.execute(sources, target, **step.params)

            self._current_step_index += 1

            if stop_at and step.name == stop_at:
                self.logger.info(f"Pipeline execution stopped at step: '{stop_at}'")
                return target            

        self.logger.info("Pipeline execution completed.")
        return target
    
