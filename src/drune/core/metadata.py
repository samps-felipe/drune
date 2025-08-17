from abc import ABC, abstractmethod
from typing import Dict, Type

# This will hold the mapping from Metadata name to Metadata class
_Metadata_registry: Dict[str, Type] = {}

def register_Metadata(name: str, cls: Type = None):
    """
    Registers or overwrites a Metadata class.
    Usage as decorator: @register_Metadata('Metadata_name')
    Usage as function: register_Metadata('Metadata_name', MyClass)
    """
    if cls is not None:
        _Metadata_registry[name] = cls
        return cls
    
    def decorator(inner_cls: Type) -> Type:
        _Metadata_registry[name] = inner_cls
        return inner_cls
    
    return decorator

def get_Metadata(name: str) -> Type:
    """Retrieves a Metadata from the registry."""
    return _Metadata_registry[name]

class BaseMetadata(ABC):
    """
    Classe Abstrata que define o contrato para qualquer motor de processamento (Metadata).
    Qualquer novo Metadata (ex: PandasMetadata) deve implementar estes métodos.
    """
    @abstractmethod
    def __init__(self):
        """Initializes the Metadata with the given pipeline configuration."""
        pass