from abc import ABC, abstractmethod
from typing import Any

class BaseEngine(ABC):
    """Abstract Base Class for all data processing engines."""

    @abstractmethod
    def __init__(self, **kwargs):
        """Initializes the engine with the given pipeline configuration."""
        pass

    @abstractmethod
    def read_file(self, source) -> Any:
        """Reads a file from the given source configuration."""
        raise NotImplementedError

    @abstractmethod
    def read_table(self, table_name: str) -> Any:
        """Reads a table from the database using the given table name."""
        raise NotImplementedError

    @abstractmethod
    def execute_query(self, query: str) -> Any:
        """Executes a SQL query and returns the result."""
        raise NotImplementedError

    @abstractmethod
    def write(self, data: Any, target: Any, safe: bool = True):
        """Writes data to the specified target configuration."""
        raise NotImplementedError

    @abstractmethod
    def apply_schema(self, df, schema) -> Any:
        """Applies the schema to the DataFrame."""
        raise NotImplementedError
    
    @abstractmethod
    def rollback(self, target: Any):
        """Roll back the last write operation if it was done in safe mode."""
        raise NotImplementedError