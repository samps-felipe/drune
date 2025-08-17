from abc import ABC, abstractmethod
from typing import Dict, Type
from drune.models import ProjectModel

# This will hold the mapping from engine name to engine class
_engine_registry: Dict[str, Type] = {}

def register_engine(name: str, cls: Type = None):
    """
    Registers or overwrites a engine class.
    Usage as decorator: @register_engine('engine_name')
    Usage as function: register_engine('engine_name', MyClass)
    """
    if cls is not None:
        _engine_registry[name] = cls
        return cls
    
    def decorator(inner_cls: Type) -> Type:
        _engine_registry[name] = inner_cls
        return inner_cls
    
    return decorator

def get_engine(name: str) -> Type:
    """Retrieves a engine from the registry."""
    return _engine_registry[name]

class BaseEngine(ABC):

    @abstractmethod
    def __init__(self, config: ProjectModel):
        """Initializes the engine with the given pipeline configuration."""
        pass

    # @abstractmethod
    # def run(self):
    #     """Executes the pipeline by dynamically running the steps defined in the config."""
    #     pass
    
    # @abstractmethod
    # def create_table(self):
    #     """Creates the table schema at the destination."""
    #     pass

    # @abstractmethod
    # def update_table(self):
    #     """Applies schema or metadata changes to an existing table."""
    #     pass
