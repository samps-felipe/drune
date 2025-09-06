from abc import ABC, abstractmethod
import os
from typing import Dict, Type, List, Any, Tuple
from ..engine.engine import BaseEngine
from ..models import ValidationResult
from drune.utils.exceptions import ConstraintError
from drune.utils.logger import get_logger

class BaseConstraint(ABC):
    """Base class for all column constraints."""
    def __init__(self, name: str):
        """Initializes the constraint rule."""
        self.name = name
        self.logger = get_logger(f'{self.name}')

    @abstractmethod
    def apply(self, df: Any, params: Dict[str, Any]) -> Tuple[Any, Any]:
        """Applies the constraint rule."""
        pass
    
    @abstractmethod
    def fail(self, df) -> Tuple[Any, Any]:
        pass
    
    @abstractmethod
    def drop(self, df):
        pass

    @abstractmethod
    def warn(self, df):
        pass