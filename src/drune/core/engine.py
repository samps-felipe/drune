from abc import ABC, abstractmethod
from typing import Any, Dict, Type
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
    """Retrieves an engine from the registry."""
    try:
        return _engine_registry[name]
    except KeyError:
        raise NotImplementedError(
        f"Engine '{name}' not implemented. "
        f"Available engines: {list(_engine_registry.keys())}")

class BaseEngine(ABC):

    @abstractmethod
    def __init__(self, config: ProjectModel):
        """Initializes the engine with the given pipeline configuration."""
        pass

    @abstractmethod
    def read_file(self, source) -> Any:
        """Reads a file from the given source configuration."""
        pass

    @abstractmethod
    def read_table(self, table_name: str) -> Any:
        """Reads a table from the database using the given table name."""
        pass

    @abstractmethod
    def execute_query(self, query: str) -> Any:
        """Executes a SQL query and returns the result."""
        pass

    @abstractmethod
    def write(self, data: Any, target: Any):
        """Writes data to the specified target configuration."""
        pass

    @abstractmethod
    def apply_schema(self, df, schema) -> Any:
        """Applies the schema to the DataFrame."""
        pass
