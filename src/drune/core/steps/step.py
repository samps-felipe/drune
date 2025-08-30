from abc import ABC, abstractmethod
from typing import Any, Dict
from drune.utils.logger import get_logger


class BaseStep(ABC):
    """
    Abstract base class that defines the contract for any pipeline step.
    """
    def __init__(self, name: str, params: dict = {}):
        self.name = name
        self.params = params
        self.logger = get_logger(self.name)

    @abstractmethod
    def execute(self, sources: Dict[str, Any], target: Any, **kwargs):
        """
        Executes the logic of the step.
        It receives the result of the previous step and optional parameters.
        And it returns the result of the step and optional metadata.
        """
        pass
