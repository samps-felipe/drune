from abc import ABC, abstractmethod
from typing import Dict, Type, Any, Tuple
from drune.core.state import PipelineState
from drune.utils.logger import get_logger

# This will hold the mapping from step name to step class
_step_registry: Dict[str, Type] = {}

def register_step(name: str, cls: Type = None):
    """
    Registers or overwrites a step class.
    Can be used as a decorator: @register_step('step_name')
    or as a function: register_step('step_name', MyClass)
    """
    if cls is not None:
        _step_registry[name] = cls
        return cls
    
    def decorator(inner_cls: Type) -> Type:
        _step_registry[name] = inner_cls
        return inner_cls
    
    return decorator

def get_step(name: str) -> Type['BaseStep']:
    """Retrieves a step class from the registry."""
    step_class = _step_registry.get(name)
    if not step_class:
        raise ValueError(f"Step type '{name}' not found in registry.")
    
    return step_class

class BaseStep(ABC):
    """
    Abstract base class that defines the contract for any pipeline step.
    """
    def __init__(self, name: str, params: dict = {}):
        self.name = name
        self.params = params
        self.logger = get_logger(self.name)

    @abstractmethod
    def execute(self, previous: PipelineState = None, **kwargs):
        """
        Executes the logic of the step.
        It receives the result of the previous step and optional parameters.
        And it returns the result of the step and optional metadata.
        """
        pass
