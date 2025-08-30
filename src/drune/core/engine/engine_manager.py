from typing import Dict, Type
from .engine import BaseEngine

class EngineManager:
    """Manages registration and retrieval of processing engines."""

    _engines: Dict[str, Type[BaseEngine]] = {}

    @classmethod
    def register(cls, name: str):
        """A decorator to register a new engine class."""

        def decorator(engine_class: Type[BaseEngine]):
            cls._engines[name] = engine_class
            return engine_class

        return decorator

    @classmethod
    def get_engine(cls, name: str, config: Dict) -> BaseEngine:
        """Get an instance of a registered engine by name."""
        if name not in cls._engines:
            try:
                __import__(f"drune.engines.{name}")
            except ImportError:
                raise ValueError(f"Unsupported engine: {name}. Could not import drune.engines.{name}.")

        engine_class = cls._engines.get(name)
        
        if not engine_class:
            raise ValueError(f"Engine '{name}' is not registered. Available: {list(cls._engines.keys())}")

        return engine_class(**config)