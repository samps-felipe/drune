from abc import ABC, abstractmethod
from typing import Dict, Type, Any, List

# This will hold the mapping from Metadata engine name to Metadata engine class
_metadata_registry: Dict[str, Type["BaseMetadataEngine"]] = {}

def register_metadata(name: str, cls: Type["BaseMetadataEngine"] = None):
    """
    Registers or overwrites a metadata engine class.

    Can be used as a decorator:
        @register_metadata('my_engine')
        class MyMetadataEngine(BaseMetadataEngine):
            ...

    Or as a function:
        register_metadata('my_engine', MyMetadataEngine)
    """
    if cls is not None:
        _metadata_registry[name] = cls
        return cls

    def decorator(inner_cls: Type["BaseMetadataEngine"]) -> Type["BaseMetadataEngine"]:
        _metadata_registry[name] = inner_cls
        return inner_cls

    return decorator

def get_metadata(name: str) -> Type["BaseMetadataEngine"]:
    """Retrieves a metadata engine from the registry."""
    if name not in _metadata_registry:
        raise NotImplementedError(
            f"Metadata engine '{name}' not found. "
            f"Available metadata engines: {list(_metadata_registry.keys())}")
    
    return _metadata_registry[name]

class BaseMetadataEngine(ABC):
    """
    Abstract base class that defines the contract for any metadata engine.
    Any new metadata engine (e.g., HiveMetadataEngine, OpenMetadataEngine) must implement these methods.
    """

    @abstractmethod
    def __init__(self, **kwargs: Any) -> None:
        """Initializes the metadata engine with the given configuration."""
        pass

    @abstractmethod
    def create_table(self, table_name: str, schema: Dict[str, Any]) -> None:
        """Creates the table schema at the destination."""
        pass

    @abstractmethod
    def get_table(self, table_name: str) -> Dict[str, Any]:
        """Retrieves metadata for a specific table."""
        pass

    @abstractmethod
    def update_table(self, table_name: str, schema: Dict[str, Any]) -> None:
        """Applies schema or metadata changes to an existing table."""
        pass

    @abstractmethod
    def list_tables(self, database: str = None, schema: str = None) -> List[str]:
        """Lists all tables in a database or schema."""
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Checks if a table exists."""
        pass
